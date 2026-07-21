package db

import (
	"context"
	"fmt"
	"time"

	"github.com/samucap/poly-asian-data/internal/services"
)

// TagAggregate is a metrics-only row (legacy aggregate-only updates).
type TagAggregate struct {
	ID           string
	TotalVol     float64
	TotalVol24hr float64
	TotalLiq     float64
	TotalMarkets int
}

// ResetTagCatalogResult counts rows touched by ResetTagCatalog.
type ResetTagCatalogResult struct {
	SportsCleared int64
	TagsDeleted   int64
	WatermarkGone bool
}

// ResetTagCatalog wipes the tags table safely for a clean API re-seed.
//
// Order (single transaction):
//  1. NULL sports.primary_tag_id (FK → tags)
//  2. NULL tags.parent_tag_id (self-FK)
//  3. DELETE FROM tags
//  4. DELETE sync_state row for WatermarkTopMarketsTagCatalog (force next LoadTagCatalog API path)
//
// Does not run every cycle — call explicitly (e.g. catalog-markets --reset-tags).
func ResetTagCatalog(ctx context.Context, conn DBInterface) (ResetTagCatalogResult, error) {
	var out ResetTagCatalogResult
	if conn == nil {
		return out, ErrNilDB
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return out, fmt.Errorf("db: reset tag catalog begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `UPDATE sports SET primary_tag_id = NULL WHERE primary_tag_id IS NOT NULL`)
	if err != nil {
		return out, fmt.Errorf("db: reset tag catalog clear sports.primary_tag_id: %w", err)
	}
	out.SportsCleared = tag.RowsAffected()

	if _, err := tx.Exec(ctx, `UPDATE tags SET parent_tag_id = NULL WHERE parent_tag_id IS NOT NULL`); err != nil {
		return out, fmt.Errorf("db: reset tag catalog clear parent_tag_id: %w", err)
	}

	tag, err = tx.Exec(ctx, `DELETE FROM tags`)
	if err != nil {
		return out, fmt.Errorf("db: reset tag catalog delete tags: %w", err)
	}
	out.TagsDeleted = tag.RowsAffected()

	tag, err = tx.Exec(ctx, `DELETE FROM sync_state WHERE sync_type = $1`, WatermarkTopMarketsTagCatalog)
	if err != nil {
		return out, fmt.Errorf("db: reset tag catalog clear watermark: %w", err)
	}
	out.WatermarkGone = tag.RowsAffected() > 0

	if err := tx.Commit(ctx); err != nil {
		return out, fmt.Errorf("db: reset tag catalog commit: %w", err)
	}
	return out, nil
}

// FetchTopCategoryTags loads tags with the given parent_tag_id and the newest updated_at.
// On query failure returns (nil, zero time, err).
func FetchTopCategoryTags(ctx context.Context, conn DBInterface, parentTagID string) ([]*services.PlyMktTag, time.Time, error) {
	return fetchTagsWhere(ctx, conn, `SELECT id, label, slug, force_show, force_hide, parent_tag_id, updated_at
		FROM tags WHERE parent_tag_id = $1`, parentTagID)
}

// FetchTagSubtree loads the full descendant tree under rootParentID (recursive).
// Needed for sports chains like mlb → baseball → Sports → Categories (depth 3+).
// maxUpdated is the newest updated_at among returned rows.
func FetchTagSubtree(ctx context.Context, conn DBInterface, rootParentID string) ([]*services.PlyMktTag, time.Time, error) {
	if conn == nil {
		return nil, time.Time{}, ErrNilDB
	}
	if rootParentID == "" {
		return nil, time.Time{}, fmt.Errorf("db: rootParentID is required")
	}
	// Recursive CTE with depth cap to avoid cycles; sports trees are shallow.
	query := `
		WITH RECURSIVE tree AS (
			SELECT id, label, slug, force_show, force_hide, parent_tag_id, updated_at, 1 AS depth
			FROM tags
			WHERE parent_tag_id = $1
			UNION ALL
			SELECT t.id, t.label, t.slug, t.force_show, t.force_hide, t.parent_tag_id, t.updated_at, tree.depth + 1
			FROM tags t
			INNER JOIN tree ON t.parent_tag_id = tree.id
			WHERE tree.depth < 8
		)
		SELECT id, label, slug, force_show, force_hide, parent_tag_id, updated_at
		FROM tree`
	return fetchTagsWhere(ctx, conn, query, rootParentID)
}

func fetchTagsWhere(ctx context.Context, conn DBInterface, query string, arg string) ([]*services.PlyMktTag, time.Time, error) {
	if conn == nil {
		return nil, time.Time{}, ErrNilDB
	}

	rows, err := conn.Query(ctx, query, arg)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer rows.Close()

	var tags []*services.PlyMktTag
	var maxUpdatedAt time.Time

	for rows.Next() {
		var t services.PlyMktTag
		var parentID *string
		var updatedAt time.Time
		if err := rows.Scan(&t.ID, &t.Label, &t.Slug, &t.ForceShow, &t.ForceHide, &parentID, &updatedAt); err != nil {
			return nil, time.Time{}, err
		}
		if parentID != nil {
			t.ParentTagID = *parentID
		}
		if updatedAt.After(maxUpdatedAt) {
			maxUpdatedAt = updatedAt
		}
		tags = append(tags, &t)
	}
	if err := rows.Err(); err != nil {
		return nil, time.Time{}, err
	}

	return tags, maxUpdatedAt, nil
}

// UpdateTags upserts tag definitions, hierarchy, and cycle aggregates in one write.
// Empty parent_tag_id is stored as NULL. Metrics are fully replaced for each row
// (idle tags should be passed with zeros so stale totals do not linger).
func UpdateTags(ctx context.Context, conn DBInterface, tags []*services.PlyMktTag) error {
	if len(tags) == 0 {
		return nil
	}
	if conn == nil {
		return ErrNilDB
	}

	// parent_tag_id: writer is authoritative when non-empty (catalog-markets overlays
	// sports hierarchy as Gamma parent tag ids before calling this). Never use
	// tags.sport_id (UUID) as hierarchy — that is only a sports-table FK for leagues/teams.
	// label/slug: never replace a non-empty value with empty/null from a sparse writer.
	sql := `
		INSERT INTO tags (
			id, label, slug, force_show, force_hide, parent_tag_id,
			total_vol, total_vol_24hr, total_liq, total_markets
		) VALUES (
			$1, $2, $3, $4, $5, NULLIF($6, ''),
			$7, $8, $9, $10
		)
		ON CONFLICT (id) DO UPDATE SET
			label = CASE
				WHEN EXCLUDED.label IS NOT NULL AND EXCLUDED.label <> '' THEN EXCLUDED.label
				ELSE tags.label
			END,
			slug = CASE
				WHEN EXCLUDED.slug IS NOT NULL AND EXCLUDED.slug <> '' THEN EXCLUDED.slug
				ELSE tags.slug
			END,
			force_show = EXCLUDED.force_show,
			force_hide = EXCLUDED.force_hide,
			parent_tag_id = CASE
				WHEN EXCLUDED.parent_tag_id IS NOT NULL THEN EXCLUDED.parent_tag_id
				ELSE tags.parent_tag_id
			END,
			total_vol = EXCLUDED.total_vol,
			total_vol_24hr = EXCLUDED.total_vol_24hr,
			total_liq = EXCLUDED.total_liq,
			total_markets = EXCLUDED.total_markets,
			updated_at = NOW()
	`

	rows := make([][]any, 0, len(tags))
	for _, t := range tags {
		if t == nil || t.ID == "" {
			continue
		}
		rows = append(rows, []any{
			t.ID,
			t.Label,
			t.Slug,
			t.ForceShow,
			t.ForceHide,
			t.ParentTagID,
			t.TotalVol,
			t.TotalVol24hr,
			t.TotalLiq,
			t.TotalMarkets,
		})
	}
	if len(rows) == 0 {
		return nil
	}
	return BatchExec(ctx, conn, sql, rows)
}

// UpdateTagAggregates writes volume/liquidity rollups onto tags by id (metrics only).
// Prefer UpdateTags when definitions/hierarchy must be written in the same pass.
func UpdateTagAggregates(ctx context.Context, conn DBInterface, aggregates []TagAggregate) error {
	if len(aggregates) == 0 {
		return nil
	}
	if conn == nil {
		return ErrNilDB
	}

	sql := `
		UPDATE tags SET
			total_vol = $2,
			total_vol_24hr = $3,
			total_liq = $4,
			total_markets = $5,
			updated_at = NOW()
		WHERE id = $1
	`

	rows := make([][]any, 0, len(aggregates))
	for _, a := range aggregates {
		if a.ID == "" {
			continue
		}
		rows = append(rows, []any{
			a.ID,
			a.TotalVol,
			a.TotalVol24hr,
			a.TotalLiq,
			a.TotalMarkets,
		})
	}
	if len(rows) == 0 {
		return nil
	}
	return BatchExec(ctx, conn, sql, rows)
}

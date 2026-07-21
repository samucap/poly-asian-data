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

// FetchTopCategoryTags loads tags with the given parent_tag_id and the newest updated_at.
// On query failure returns (nil, zero time, err).
func FetchTopCategoryTags(ctx context.Context, conn DBInterface, parentTagID string) ([]*services.PlyMktTag, time.Time, error) {
	return fetchTagsWhere(ctx, conn, `SELECT id, label, slug, force_show, force_hide, parent_tag_id, updated_at
		FROM tags WHERE parent_tag_id = $1`, parentTagID)
}

// FetchTagSubtree loads all tags under rootParentID: direct children (top-level)
// and their children (subtags). Used for hierarchical catalog loads.
// maxUpdated is the newest updated_at among returned rows.
func FetchTagSubtree(ctx context.Context, conn DBInterface, rootParentID string) ([]*services.PlyMktTag, time.Time, error) {
	if conn == nil {
		return nil, time.Time{}, ErrNilDB
	}
	if rootParentID == "" {
		return nil, time.Time{}, fmt.Errorf("db: rootParentID is required")
	}
	// Depth-1 and depth-2 under root (matches fetchCategories related-tags walk).
	query := `
		SELECT id, label, slug, force_show, force_hide, parent_tag_id, updated_at
		FROM tags
		WHERE parent_tag_id = $1
		   OR parent_tag_id IN (SELECT id FROM tags WHERE parent_tag_id = $1)`
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

	sql := `
		INSERT INTO tags (
			id, label, slug, force_show, force_hide, parent_tag_id,
			total_vol, total_vol_24hr, total_liq, total_markets
		) VALUES (
			$1, $2, $3, $4, $5, NULLIF($6, ''),
			$7, $8, $9, $10
		)
		ON CONFLICT (id) DO UPDATE SET
			label = EXCLUDED.label,
			slug = EXCLUDED.slug,
			force_show = EXCLUDED.force_show,
			force_hide = EXCLUDED.force_hide,
			parent_tag_id = EXCLUDED.parent_tag_id,
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

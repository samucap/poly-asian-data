package db

import (
	"context"
	"fmt"
	"time"

	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/samucap/poly-asian-data/internal/sportstags"
)

// LeagueTagRow is the minimum league data needed to rebuild sports parent edges.
type LeagueTagRow struct {
	Sport   string
	RawTags string
}

// TeamHierarchyRow is the minimum team data for team→league tag matching.
type TeamHierarchyRow struct {
	Name         string
	League       string
	Abbreviation string
	Alias        string
}

// FetchLeagueTagRows loads sport key + raw_tags CSV from leagues for hierarchy rebuild.
func FetchLeagueTagRows(ctx context.Context, conn DBInterface) ([]LeagueTagRow, error) {
	if conn == nil {
		return nil, ErrNilDB
	}
	rows, err := conn.Query(ctx, `SELECT sport, COALESCE(raw_tags, '') FROM leagues`)
	if err != nil {
		return nil, fmt.Errorf("db: fetch league tag rows: %w", err)
	}
	defer rows.Close()

	var out []LeagueTagRow
	for rows.Next() {
		var r LeagueTagRow
		if err := rows.Scan(&r.Sport, &r.RawTags); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// FetchTeamsForHierarchy loads teams for name→tag matching.
func FetchTeamsForHierarchy(ctx context.Context, conn DBInterface) ([]TeamHierarchyRow, error) {
	if conn == nil {
		return nil, ErrNilDB
	}
	rows, err := conn.Query(ctx, `
		SELECT COALESCE(name, ''), COALESCE(league, ''),
		       COALESCE(abbreviation, ''), COALESCE(alias, '')
		FROM teams
	`)
	if err != nil {
		return nil, fmt.Errorf("db: fetch teams for hierarchy: %w", err)
	}
	defer rows.Close()

	var out []TeamHierarchyRow
	for rows.Next() {
		var r TeamHierarchyRow
		if err := rows.Scan(&r.Name, &r.League, &r.Abbreviation, &r.Alias); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// FetchSportsScopedTags loads tags under the Sports top (id=1 and descendants) for matching.
// Falls back to a broad id/label/slug set when the sports tree is still empty.
func FetchSportsScopedTags(ctx context.Context, conn DBInterface) ([]sportstags.TagRef, error) {
	if conn == nil {
		return nil, ErrNilDB
	}
	// Include Sports itself and all descendants (depth cap); also include any tag that is a
	// direct child of Sports even if parent was wiped temporarily.
	query := `
		WITH RECURSIVE tree AS (
			SELECT id, label, slug, 0 AS depth
			FROM tags
			WHERE id = $1
			UNION ALL
			SELECT t.id, t.label, t.slug, tree.depth + 1
			FROM tags t
			INNER JOIN tree ON t.parent_tag_id = tree.id
			WHERE tree.depth < 8
		)
		SELECT id, COALESCE(label, ''), COALESCE(slug, '') FROM tree
		WHERE id <> $1
	`
	rows, err := conn.Query(ctx, query, sportstags.TagIDSports)
	if err != nil {
		return nil, fmt.Errorf("db: fetch sports-scoped tags: %w", err)
	}
	defer rows.Close()

	var out []sportstags.TagRef
	for rows.Next() {
		var t sportstags.TagRef
		if err := rows.Scan(&t.ID, &t.Label, &t.Slug); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// If tree is sparse (pre-hierarchy or wipe), also load tags with non-empty slug that
	// are not top categories under Categories root only — keep it simple: all tags with slug.
	if len(out) < 50 {
		rows2, err := conn.Query(ctx, `
			SELECT id, COALESCE(label, ''), COALESCE(slug, '')
			FROM tags
			WHERE COALESCE(slug, '') <> '' OR COALESCE(label, '') <> ''
		`)
		if err != nil {
			return out, nil // prefer partial tree over failure
		}
		defer rows2.Close()
		seen := map[string]bool{}
		for _, t := range out {
			seen[t.ID] = true
		}
		for rows2.Next() {
			var t sportstags.TagRef
			if err := rows2.Scan(&t.ID, &t.Label, &t.Slug); err != nil {
				return out, err
			}
			if seen[t.ID] {
				continue
			}
			seen[t.ID] = true
			out = append(out, t)
		}
		_ = rows2.Err()
	}
	return out, nil
}

// SportsParentEdges rebuilds child→parent Gamma tag map from leagues + teams + static anchors.
// League/family edges are authoritative; team edges are merged in (team→league title).
func SportsParentEdges(ctx context.Context, conn DBInterface) (map[string]string, error) {
	rows, err := FetchLeagueTagRows(ctx, conn)
	if err != nil {
		return nil, err
	}
	leagues := make([]services.PlyMktSport, 0, len(rows))
	keyTags := make([]sportstags.LeagueKeyTags, 0, len(rows))
	for _, r := range rows {
		leagues = append(leagues, services.PlyMktSport{Sport: r.Sport, Tags: r.RawTags})
		keyTags = append(keyTags, sportstags.LeagueKeyTags{Sport: r.Sport, RawTags: r.RawTags})
	}
	leagueEdges := sportstags.EdgesFromLeagues(leagues)

	// Team → league title (best-effort; skip if teams/tags unavailable).
	teamEdges, err := buildTeamParentEdges(ctx, conn, keyTags)
	if err != nil {
		// Non-fatal: return league edges only.
		return leagueEdges, nil
	}
	// League/family edges win on key collision (merge team first, then league overwrites).
	return sportstags.MergeEdges(teamEdges, leagueEdges), nil
}

func buildTeamParentEdges(ctx context.Context, conn DBInterface, keyTags []sportstags.LeagueKeyTags) (map[string]string, error) {
	titles := sportstags.LeagueTitleByKey(keyTags)
	if len(titles) == 0 {
		return nil, nil
	}
	teams, err := FetchTeamsForHierarchy(ctx, conn)
	if err != nil {
		return nil, err
	}
	if len(teams) == 0 {
		return nil, nil
	}
	tagRefs, err := FetchSportsScopedTags(ctx, conn)
	if err != nil {
		return nil, err
	}
	if len(tagRefs) == 0 {
		return nil, nil
	}
	teamRows := make([]sportstags.TeamRow, 0, len(teams))
	for _, t := range teams {
		teamRows = append(teamRows, sportstags.TeamRow{
			Name:         t.Name,
			League:       t.League,
			Abbreviation: t.Abbreviation,
			Alias:        t.Alias,
		})
	}
	matches := sportstags.MatchTeamsToTags(teamRows, tagRefs, titles)
	return sportstags.TeamParentEdges(matches), nil
}

// FetchTagsByIDs loads tag rows for the given Gamma ids (definitions + current parent).
func FetchTagsByIDs(ctx context.Context, conn DBInterface, ids []string) ([]*services.PlyMktTag, error) {
	if conn == nil {
		return nil, ErrNilDB
	}
	if len(ids) == 0 {
		return nil, nil
	}
	// Dedup
	seen := map[string]bool{}
	uniq := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		uniq = append(uniq, id)
	}
	rows, err := conn.Query(ctx, `
		SELECT id, label, slug, force_show, force_hide, parent_tag_id, updated_at
		FROM tags WHERE id = ANY($1)
	`, uniq)
	if err != nil {
		return nil, fmt.Errorf("db: fetch tags by ids: %w", err)
	}
	defer rows.Close()

	var tags []*services.PlyMktTag
	for rows.Next() {
		var t services.PlyMktTag
		var parentID *string
		var forceShow, forceHide *bool
		var updatedAt time.Time
		if err := rows.Scan(&t.ID, &t.Label, &t.Slug, &forceShow, &forceHide, &parentID, &updatedAt); err != nil {
			return nil, err
		}
		if forceShow != nil {
			t.ForceShow = *forceShow
		}
		if forceHide != nil {
			t.ForceHide = *forceHide
		}
		if parentID != nil {
			t.ParentTagID = *parentID
		}
		tags = append(tags, &t)
	}
	return tags, rows.Err()
}

// ApplySportsParentTags writes parent_tag_id for sports hierarchy edges (Gamma ids only).
// Does not touch tags.sport_id (UUID). Seeds well-known labels when missing.
// UPDATE-only for unknown leaves (must already exist from tag defs).
func ApplySportsParentTags(ctx context.Context, conn DBInterface, edges map[string]string) (int64, error) {
	if conn == nil {
		return 0, ErrNilDB
	}
	edges = sportstags.MergeStaticFamilyAnchors(edges)
	if len(edges) == 0 {
		return 0, nil
	}

	// Seed anchors with full label/slug so parent FKs resolve.
	var total int64
	for id := range edges {
		if err := seedKnownTag(ctx, conn, id); err != nil {
			return total, err
		}
	}
	for _, parent := range edges {
		if err := seedKnownTag(ctx, conn, parent); err != nil {
			return total, err
		}
	}
	// Sports top
	if err := seedKnownTag(ctx, conn, sportstags.TagIDSports); err != nil {
		return total, err
	}

	sql := `
		UPDATE tags SET
			parent_tag_id = $2,
			updated_at = NOW()
		WHERE id = $1
	`
	rows := make([][]any, 0, len(edges))
	for child, parent := range edges {
		if child == "" || parent == "" {
			continue
		}
		rows = append(rows, []any{child, parent})
	}
	if len(rows) == 0 {
		return 0, nil
	}
	if err := BatchExec(ctx, conn, sql, rows); err != nil {
		return 0, fmt.Errorf("db: apply sports parent tags: %w", err)
	}
	return int64(len(rows)), nil
}

func seedKnownTag(ctx context.Context, conn DBInterface, id string) error {
	if id == "" {
		return nil
	}
	label, slug := sportstags.KnownLabelSlug(id)
	if label == "" && slug == "" {
		return nil
	}
	_, err := conn.Exec(ctx, `
		INSERT INTO tags (id, label, slug)
		VALUES ($1, $2, $3)
		ON CONFLICT (id) DO UPDATE SET
			label = COALESCE(NULLIF(tags.label, ''), EXCLUDED.label),
			slug  = COALESCE(NULLIF(tags.slug, ''), EXCLUDED.slug)
	`, id, label, slug)
	return err
}

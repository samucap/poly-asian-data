package db

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// Catalog watermark keys (stored in sync_state.sync_type — not tags.updated_at).
const (
	// WatermarkTopMarketsTagCatalog is set when top tags under 102982 were last
	// refreshed from the related-tags API (definitions/hierarchy only).
	WatermarkTopMarketsTagCatalog = "top_markets.tag_catalog"
)

// DefaultTagCatalogTTL is how long a top-tag catalog may be reused from DB
// before re-fetching related-tags under the categories root.
const DefaultTagCatalogTTL = 24 * time.Hour

// GetWatermark returns last_sync_at for key from sync_state.
// ok is false when the row is missing or last_sync_at is null.
func GetWatermark(ctx context.Context, conn DBInterface, key string) (t time.Time, ok bool, err error) {
	if conn == nil {
		return time.Time{}, false, ErrNilDB
	}
	if key == "" {
		return time.Time{}, false, errors.New("db: watermark key is required")
	}
	var ts *time.Time
	err = conn.QueryRow(ctx, `
		SELECT last_sync_at FROM sync_state WHERE sync_type = $1
	`, key).Scan(&ts)
	if errors.Is(err, pgx.ErrNoRows) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	if ts == nil || ts.IsZero() {
		return time.Time{}, false, nil
	}
	return *ts, true, nil
}

// SetWatermark upserts last_sync_at for key (catalog refresh time, not metric aggregates).
func SetWatermark(ctx context.Context, conn DBInterface, key string, t time.Time) error {
	if conn == nil {
		return ErrNilDB
	}
	if key == "" {
		return errors.New("db: watermark key is required")
	}
	if t.IsZero() {
		t = time.Now().UTC()
	}
	_, err := conn.Exec(ctx, `
		INSERT INTO sync_state (sync_type, last_sync_at, status, total_items, last_cursor)
		VALUES ($1, $2, 'completed', 0, NULL)
		ON CONFLICT (sync_type) DO UPDATE SET
			last_sync_at = EXCLUDED.last_sync_at,
			status = 'completed'
	`, key, t)
	return err
}

// CatalogNeedsRefresh reports whether the tag catalog should be re-fetched from API.
// Uses the dedicated watermark only — never tags.updated_at (polluted by aggregates).
func CatalogNeedsRefresh(wm time.Time, hasWM bool, now time.Time, ttl time.Duration) bool {
	if ttl <= 0 {
		ttl = DefaultTagCatalogTTL
	}
	if !hasWM || wm.IsZero() {
		return true
	}
	return now.Sub(wm) >= ttl
}

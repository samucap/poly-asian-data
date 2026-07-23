package db

import (
	"context"
	"fmt"
	"time"
)

// MarketResolutionRow is one WS market_resolved update for plymkt_markets.
type MarketResolutionRow struct {
	GammaID        string
	ConditionID    string
	WinningAssetID string
	WinningOutcome string
	ResolvedAt     time.Time
}

// EnsureMarketResolutionColumns adds winning_* / resolved_at if missing.
func EnsureMarketResolutionColumns(ctx context.Context, conn DBInterface) error {
	if conn == nil {
		return ErrNilDB
	}
	stmts := []string{
		`ALTER TABLE plymkt_markets ADD COLUMN IF NOT EXISTS winning_asset_id TEXT`,
		`ALTER TABLE plymkt_markets ADD COLUMN IF NOT EXISTS winning_outcome TEXT`,
		`ALTER TABLE plymkt_markets ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ`,
	}
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("db: ensure market resolution cols: %w", err)
		}
	}
	return nil
}

// ApplyMarketResolutions batch-updates plymkt_markets for resolved board markets.
// Best-effort: 0 rows updated is not an error (market may not be in DB yet).
func ApplyMarketResolutions(ctx context.Context, conn DBInterface, rows []MarketResolutionRow) (updated int, err error) {
	if conn == nil {
		return 0, ErrNilDB
	}
	if len(rows) == 0 {
		return 0, nil
	}
	const sql = `
		UPDATE plymkt_markets SET
			closed = true,
			active = false,
			closed_time = COALESCE(NULLIF(closed_time, ''), $1),
			resolved_at = COALESCE(resolved_at, $2::timestamptz),
			winning_asset_id = COALESCE(NULLIF($3, ''), winning_asset_id),
			winning_outcome = COALESCE(NULLIF($4, ''), winning_outcome),
			updated_at = NOW()
		WHERE ($5 <> '' AND condition_id = $5)
		   OR ($6 <> '' AND id = $6)`
	for _, r := range rows {
		ts := r.ResolvedAt
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		tsText := ts.UTC().Format(time.RFC3339)
		tag, e := conn.Exec(ctx, sql,
			tsText, ts.UTC(),
			r.WinningAssetID, r.WinningOutcome,
			r.ConditionID, r.GammaID,
		)
		if e != nil {
			return updated, fmt.Errorf("db: apply market resolution: %w", e)
		}
		if tag.RowsAffected() > 0 {
			updated++
		}
	}
	return updated, nil
}

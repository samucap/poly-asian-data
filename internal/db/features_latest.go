package db

import (
	"context"
	"fmt"
	"time"
)

// FeaturesLatestRow is the current feature vector for one token (M6 hot path).
type FeaturesLatestRow struct {
	TokenID         string
	ConditionID     string
	MarketID        string
	BestBid         float64
	BestAsk         float64
	Mid             float64
	LastTradePrice  float64
	Spread          float64
	Imbalance       float64
	BidDepth        float64
	AskDepth        float64
	UpdatedAt       time.Time
}

// EnsureFeaturesLatestTable creates features_latest if missing.
func EnsureFeaturesLatestTable(ctx context.Context, conn DBInterface) error {
	if conn == nil {
		return ErrNilDB
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS features_latest (
			token_id TEXT PRIMARY KEY,
			condition_id TEXT,
			market_id TEXT,
			best_bid DOUBLE PRECISION,
			best_ask DOUBLE PRECISION,
			mid DOUBLE PRECISION,
			last_trade_price DOUBLE PRECISION,
			spread DOUBLE PRECISION,
			imbalance DOUBLE PRECISION,
			bid_depth DOUBLE PRECISION,
			ask_depth DOUBLE PRECISION,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`CREATE INDEX IF NOT EXISTS idx_features_latest_condition ON features_latest (condition_id)`,
		`CREATE INDEX IF NOT EXISTS idx_features_latest_updated ON features_latest (updated_at DESC)`,
	}
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("db: ensure features_latest: %w", err)
		}
	}
	return nil
}

// UpsertFeaturesLatest batch-upserts current feature rows (dirty flush only).
func UpsertFeaturesLatest(ctx context.Context, conn DBInterface, rows []FeaturesLatestRow) error {
	if conn == nil {
		return ErrNilDB
	}
	if len(rows) == 0 {
		return nil
	}
	args := make([][]any, 0, len(rows))
	for _, r := range rows {
		if r.TokenID == "" {
			continue
		}
		ts := r.UpdatedAt
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		args = append(args, []any{
			r.TokenID, nullStr(r.ConditionID), nullStr(r.MarketID),
			r.BestBid, r.BestAsk, r.Mid, r.LastTradePrice, r.Spread,
			r.Imbalance, r.BidDepth, r.AskDepth, ts,
		})
	}
	if len(args) == 0 {
		return nil
	}
	const sql = `
		INSERT INTO features_latest (
			token_id, condition_id, market_id,
			best_bid, best_ask, mid, last_trade_price, spread,
			imbalance, bid_depth, ask_depth, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (token_id) DO UPDATE SET
			condition_id = EXCLUDED.condition_id,
			market_id = EXCLUDED.market_id,
			best_bid = EXCLUDED.best_bid,
			best_ask = EXCLUDED.best_ask,
			mid = EXCLUDED.mid,
			last_trade_price = EXCLUDED.last_trade_price,
			spread = EXCLUDED.spread,
			imbalance = EXCLUDED.imbalance,
			bid_depth = EXCLUDED.bid_depth,
			ask_depth = EXCLUDED.ask_depth,
			updated_at = EXCLUDED.updated_at`
	if err := BatchExec(ctx, conn, sql, args); err != nil {
		return fmt.Errorf("db: upsert features_latest: %w", err)
	}
	return nil
}

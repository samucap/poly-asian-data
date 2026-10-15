package db

import (
	"context"
	"fmt"
	"time"
)

// OIHistoryRow is one open-interest sample.
type OIHistoryRow struct {
	Time        time.Time
	ConditionID string
	OIValue     float64
	Source      string
}

// EnsureM3FeatureTables creates oi_history and extends edge_board for M3 (idempotent).
func EnsureM3FeatureTables(ctx context.Context, conn DBInterface) error {
	if conn == nil {
		return ErrNilDB
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS oi_history (
			time TIMESTAMPTZ NOT NULL,
			condition_id TEXT NOT NULL,
			oi_value DOUBLE PRECISION NOT NULL,
			source TEXT DEFAULT 'data-api'
		)`,
		// Migrate pre-M3 tables created without source (schema.sql / older DDL).
		`ALTER TABLE oi_history ADD COLUMN IF NOT EXISTS source TEXT DEFAULT 'data-api'`,
		// Best-effort hypertable (ignore if extension missing / already hypertable).
		// Callers may run scripts/sql/m3_features.sql for full Timescale setup.
		`CREATE INDEX IF NOT EXISTS idx_oi_history_cid_time ON oi_history (condition_id, time DESC)`,
	}
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("db: ensure m3 feature tables: %w", err)
		}
	}
	// Try hypertable conversion (no-op failure is OK).
	_, _ = conn.Exec(ctx, `SELECT create_hypertable('oi_history', 'time', if_not_exists => TRUE)`)

	// Extend edge_board with M3 columns (idempotent).
	alters := []string{
		`ALTER TABLE edge_board ADD COLUMN IF NOT EXISTS cost_bps DOUBLE PRECISION`,
		`ALTER TABLE edge_board ADD COLUMN IF NOT EXISTS capacity_usd DOUBLE PRECISION`,
		`ALTER TABLE edge_board ADD COLUMN IF NOT EXISTS urgency DOUBLE PRECISION`,
		`ALTER TABLE edge_board ADD COLUMN IF NOT EXISTS key_features JSONB`,
		`ALTER TABLE edge_board ADD COLUMN IF NOT EXISTS risk_flags TEXT[] NOT NULL DEFAULT '{}'`,
		`ALTER TABLE edge_board ADD COLUMN IF NOT EXISTS strategy_tags TEXT[] NOT NULL DEFAULT '{}'`,
		`ALTER TABLE edge_board ADD COLUMN IF NOT EXISTS features_asof TIMESTAMPTZ`,
		`ALTER TABLE edge_board ADD COLUMN IF NOT EXISTS fair_value DOUBLE PRECISION`,
		`ALTER TABLE edge_board ADD COLUMN IF NOT EXISTS model_edge_bps DOUBLE PRECISION`,
		`ALTER TABLE edge_board ADD COLUMN IF NOT EXISTS fv_source TEXT`,
	}
	for _, s := range alters {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("db: ensure edge_board m3 columns: %w", err)
		}
	}
	return nil
}

// InsertOIHistory appends OI samples (batched).
func InsertOIHistory(ctx context.Context, conn DBInterface, rows []OIHistoryRow) error {
	if conn == nil {
		return ErrNilDB
	}
	if len(rows) == 0 {
		return nil
	}
	args := make([][]any, 0, len(rows))
	for _, r := range rows {
		if r.ConditionID == "" {
			continue
		}
		src := r.Source
		if src == "" {
			src = "data-api"
		}
		ts := r.Time
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		args = append(args, []any{ts, r.ConditionID, r.OIValue, src})
	}
	const sql = `INSERT INTO oi_history (time, condition_id, oi_value, source) VALUES ($1, $2, $3, $4)`
	if err := BatchExec(ctx, conn, sql, args); err != nil {
		return fmt.Errorf("db: insert oi_history: %w", err)
	}
	return nil
}

// InsertOrderbookSnapshots writes orderbook_snapshots rows (batched).
// Requires unique (time, market_id, token_id) for upsert; plain insert if no conflict target.
func InsertOrderbookSnapshots(ctx context.Context, conn DBInterface, rows []OrderbookSnapshotRow) error {
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
		ts := r.Time
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		args = append(args, []any{
			ts, r.MarketID, r.TokenID, r.BestBid, r.BestAsk,
			r.Imbalance, r.TotalBidDepth, r.TotalAskDepth, r.DepthJSON, r.RawJSON,
		})
	}
	const upsertSQL = `
		INSERT INTO orderbook_snapshots (
			time, market_id, token_id, best_bid, best_ask,
			imbalance, total_bid_depth, total_ask_depth, depth_json, raw_response_json
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (time, market_id, token_id) DO UPDATE SET
			best_bid = EXCLUDED.best_bid,
			best_ask = EXCLUDED.best_ask,
			imbalance = EXCLUDED.imbalance,
			total_bid_depth = EXCLUDED.total_bid_depth,
			total_ask_depth = EXCLUDED.total_ask_depth,
			depth_json = EXCLUDED.depth_json,
			raw_response_json = EXCLUDED.raw_response_json`
	if err := BatchExec(ctx, conn, upsertSQL, args); err != nil {
		// Schema without unique constraint: fall back to plain insert batch.
		const insertSQL = `
			INSERT INTO orderbook_snapshots (
				time, market_id, token_id, best_bid, best_ask,
				imbalance, total_bid_depth, total_ask_depth, depth_json, raw_response_json
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`
		if err2 := BatchExec(ctx, conn, insertSQL, args); err2 != nil {
			return fmt.Errorf("db: insert orderbook_snapshots: %w", err)
		}
	}
	return nil
}

// OrderbookSnapshotRow is a DB write row for orderbook_snapshots.
type OrderbookSnapshotRow struct {
	Time          time.Time
	MarketID      string
	TokenID       string
	BestBid       float64
	BestAsk       float64
	Imbalance     float64
	TotalBidDepth float64
	TotalAskDepth float64
	DepthJSON     []byte
	RawJSON       []byte
}

// LatestBook is the most recent book snapshot for a token.
type LatestBook struct {
	TokenID       string
	MarketID      string
	Time          time.Time
	BestBid       float64
	BestAsk       float64
	Imbalance     float64
	TotalBidDepth float64
	TotalAskDepth float64
}

// LoadLatestBooks returns latest snapshot per token_id for the given tokens.
func LoadLatestBooks(ctx context.Context, conn DBInterface, tokenIDs []string) (map[string]LatestBook, error) {
	out := map[string]LatestBook{}
	if conn == nil {
		return out, ErrNilDB
	}
	if len(tokenIDs) == 0 {
		return out, nil
	}
	rows, err := conn.Query(ctx, `
		SELECT DISTINCT ON (token_id)
			token_id, COALESCE(market_id,''), time,
			COALESCE(best_bid,0), COALESCE(best_ask,0),
			COALESCE(imbalance,0), COALESCE(total_bid_depth,0), COALESCE(total_ask_depth,0)
		FROM orderbook_snapshots
		WHERE token_id = ANY($1)
		ORDER BY token_id, time DESC
	`, tokenIDs)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var b LatestBook
		if err := rows.Scan(&b.TokenID, &b.MarketID, &b.Time, &b.BestBid, &b.BestAsk, &b.Imbalance, &b.TotalBidDepth, &b.TotalAskDepth); err != nil {
			return out, err
		}
		out[b.TokenID] = b
	}
	return out, rows.Err()
}

// LoadLatestOI returns latest OI per condition_id.
func LoadLatestOI(ctx context.Context, conn DBInterface, conditionIDs []string) (map[string]float64, error) {
	out := map[string]float64{}
	if conn == nil {
		return out, ErrNilDB
	}
	if len(conditionIDs) == 0 {
		return out, nil
	}
	rows, err := conn.Query(ctx, `
		SELECT DISTINCT ON (condition_id)
			condition_id, oi_value
		FROM oi_history
		WHERE condition_id = ANY($1)
		ORDER BY condition_id, time DESC
	`, conditionIDs)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var v float64
		if err := rows.Scan(&id, &v); err != nil {
			return out, err
		}
		out[id] = v
	}
	return out, rows.Err()
}

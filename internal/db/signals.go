package db

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// SignalRow is one multi-dimensional paper intent (M6).
type SignalRow struct {
	Time              time.Time
	SignalID          string
	Event             string
	SupersedesID      string
	Strategy          string
	StrategyVersionID *int64
	BoardRunID        string
	BoardRank         int
	Mode              string

	ConditionID    string
	MarketID       string
	TokenID        string
	Outcome        string
	NegRisk        bool
	NegRiskGroupID string

	Side        string
	Action      string
	TimeInForce string

	EdgeBps         float64
	OpportunityBps  float64
	ModelEdgeBps    *float64
	CostBps         float64
	CostBreakdown   map[string]float64
	FairValue       *float64
	FVSource        string
	ScorePath       string

	Conviction  float64
	HorizonSec  int
	HalfLifeSec int
	Urgency     float64

	SizeUSD     float64
	SizeShares  float64
	CapacityUSD float64
	KellyFrac   *float64
	RiskFlags   []string

	Mid              float64
	BestBid          float64
	BestAsk          float64
	SpreadBps        float64
	Imbalance        float64
	BidDepth         float64
	AskDepth         float64
	LastTradePrice   float64
	BookAgeMs        int
	FeatureAgeMs     int

	Features map[string]any
	Factors  map[string]float64
	Tags     []string
	Reason   map[string]any
}

// EnsureSignalsTable creates signals hypertable-ish table if missing.
func EnsureSignalsTable(ctx context.Context, conn DBInterface) error {
	if conn == nil {
		return ErrNilDB
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS signals (
			time TIMESTAMPTZ NOT NULL,
			signal_id UUID NOT NULL,
			event TEXT NOT NULL DEFAULT 'open',
			supersedes_id UUID,
			strategy TEXT NOT NULL DEFAULT 'default',
			strategy_version_id BIGINT,
			board_run_id TEXT,
			board_rank INT,
			mode TEXT NOT NULL DEFAULT 'paper',
			condition_id TEXT NOT NULL,
			market_id TEXT,
			token_id TEXT NOT NULL,
			outcome TEXT DEFAULT 'YES',
			neg_risk BOOLEAN NOT NULL DEFAULT FALSE,
			neg_risk_group_id TEXT,
			side TEXT NOT NULL,
			action TEXT NOT NULL DEFAULT 'ENTER',
			time_in_force TEXT DEFAULT 'IOC_PAPER',
			edge_bps DOUBLE PRECISION,
			opportunity_bps DOUBLE PRECISION,
			model_edge_bps DOUBLE PRECISION,
			cost_bps DOUBLE PRECISION,
			cost_breakdown JSONB,
			fair_value DOUBLE PRECISION,
			fv_source TEXT,
			score_path TEXT,
			conviction DOUBLE PRECISION,
			horizon_sec INT,
			half_life_sec INT,
			urgency DOUBLE PRECISION,
			size_usd DOUBLE PRECISION,
			size_shares DOUBLE PRECISION,
			capacity_usd DOUBLE PRECISION,
			kelly_frac DOUBLE PRECISION,
			risk_flags TEXT[] NOT NULL DEFAULT '{}',
			mid DOUBLE PRECISION,
			best_bid DOUBLE PRECISION,
			best_ask DOUBLE PRECISION,
			spread_bps DOUBLE PRECISION,
			imbalance DOUBLE PRECISION,
			bid_depth DOUBLE PRECISION,
			ask_depth DOUBLE PRECISION,
			last_trade_price DOUBLE PRECISION,
			book_age_ms INT,
			feature_age_ms INT,
			features JSONB,
			factors JSONB,
			tags TEXT[] NOT NULL DEFAULT '{}',
			reason JSONB NOT NULL DEFAULT '{}',
			portfolio_heat DOUBLE PRECISION,
			correlation_group TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_signals_strategy_time ON signals (strategy, time DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_signals_condition_time ON signals (condition_id, time DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_signals_version_time ON signals (strategy_version_id, time DESC) WHERE strategy_version_id IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_signals_signal_id ON signals (signal_id)`,
	}
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("db: ensure signals: %w", err)
		}
	}
	// Best-effort hypertable (ignore if extension missing / already hypertable).
	_, _ = conn.Exec(ctx, `SELECT create_hypertable('signals', 'time', if_not_exists => TRUE)`)
	return nil
}

// InsertSignals appends paper signal rows (batched). Low write rate expected (debounced).
func InsertSignals(ctx context.Context, conn DBInterface, rows []SignalRow) error {
	if conn == nil {
		return ErrNilDB
	}
	if len(rows) == 0 {
		return nil
	}
	args := make([][]any, 0, len(rows))
	for _, r := range rows {
		if r.SignalID == "" || r.ConditionID == "" || r.TokenID == "" || r.Side == "" {
			continue
		}
		ts := r.Time
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		event := r.Event
		if event == "" {
			event = "open"
		}
		mode := r.Mode
		if mode == "" {
			mode = "paper"
		}
		strategy := r.Strategy
		if strategy == "" {
			strategy = "default"
		}
		action := r.Action
		if action == "" {
			action = "ENTER"
		}
		tif := r.TimeInForce
		if tif == "" {
			tif = "IOC_PAPER"
		}
		outcome := r.Outcome
		if outcome == "" {
			outcome = "YES"
		}
		flags := r.RiskFlags
		if flags == nil {
			flags = []string{}
		}
		tags := r.Tags
		if tags == nil {
			tags = []string{}
		}
		costJSON, _ := json.Marshal(r.CostBreakdown)
		if r.CostBreakdown == nil {
			costJSON = []byte("{}")
		}
		featJSON, _ := json.Marshal(r.Features)
		if r.Features == nil {
			featJSON = []byte("{}")
		}
		factJSON, _ := json.Marshal(r.Factors)
		if r.Factors == nil {
			factJSON = []byte("{}")
		}
		reasonJSON, _ := json.Marshal(r.Reason)
		if r.Reason == nil {
			reasonJSON = []byte("{}")
		}
		var supersedes any
		if r.SupersedesID != "" {
			supersedes = r.SupersedesID
		}
		args = append(args, []any{
			ts, r.SignalID, event, supersedes, strategy, r.StrategyVersionID,
			nullStr(r.BoardRunID), nullInt(r.BoardRank), mode,
			r.ConditionID, nullStr(r.MarketID), r.TokenID, outcome, r.NegRisk, nullStr(r.NegRiskGroupID),
			r.Side, action, tif,
			r.EdgeBps, r.OpportunityBps, r.ModelEdgeBps, r.CostBps, costJSON,
			r.FairValue, nullStr(r.FVSource), nullStr(r.ScorePath),
			r.Conviction, nullInt(r.HorizonSec), nullInt(r.HalfLifeSec), r.Urgency,
			r.SizeUSD, r.SizeShares, r.CapacityUSD, r.KellyFrac, flags,
			r.Mid, r.BestBid, r.BestAsk, r.SpreadBps, r.Imbalance, r.BidDepth, r.AskDepth, r.LastTradePrice,
			nullInt(r.BookAgeMs), nullInt(r.FeatureAgeMs),
			featJSON, factJSON, tags, reasonJSON,
		})
	}
	if len(args) == 0 {
		return nil
	}
	const sql = `
		INSERT INTO signals (
			time, signal_id, event, supersedes_id, strategy, strategy_version_id,
			board_run_id, board_rank, mode,
			condition_id, market_id, token_id, outcome, neg_risk, neg_risk_group_id,
			side, action, time_in_force,
			edge_bps, opportunity_bps, model_edge_bps, cost_bps, cost_breakdown,
			fair_value, fv_source, score_path,
			conviction, horizon_sec, half_life_sec, urgency,
			size_usd, size_shares, capacity_usd, kelly_frac, risk_flags,
			mid, best_bid, best_ask, spread_bps, imbalance, bid_depth, ask_depth, last_trade_price,
			book_age_ms, feature_age_ms,
			features, factors, tags, reason
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
			$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,
			$36,$37,$38,$39,$40,$41,$42,$43,$44,$45,$46,$47,$48,$49
		)`
	if err := BatchExec(ctx, conn, sql, args); err != nil {
		return fmt.Errorf("db: insert signals: %w", err)
	}
	return nil
}

func nullInt(n int) any {
	if n == 0 {
		return nil
	}
	return n
}

package db

import (
	"context"
	"fmt"
	"time"
)

// LabelRow is one PIT label for edge-eval storage.
type LabelRow struct {
	DecisionTime       time.Time
	Horizon            string
	ConditionID        string
	SelectionSet       string
	RunID              string
	Hit                *bool
	AfterCostReturnBps *float64
	Category           string
	NegRisk            *bool
	FVSource           string
	TTRBucket          string
	MidAtT             *float64
	EdgeBpsAtT         *float64
}

// EnsureLabelRowsTable creates label_rows (M4) idempotently.
func EnsureLabelRowsTable(ctx context.Context, conn DBInterface) error {
	if conn == nil {
		return ErrNilDB
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS label_rows (
			decision_time   TIMESTAMPTZ NOT NULL,
			horizon         TEXT        NOT NULL,
			condition_id    TEXT        NOT NULL,
			selection_set   TEXT        NOT NULL DEFAULT 'board_at_t',
			run_id          TEXT,
			hit             BOOLEAN,
			after_cost_return_bps DOUBLE PRECISION,
			category        TEXT,
			neg_risk        BOOLEAN,
			fv_source       TEXT,
			ttr_bucket      TEXT,
			mid_at_t        DOUBLE PRECISION,
			edge_bps_at_t   DOUBLE PRECISION,
			PRIMARY KEY (decision_time, horizon, condition_id, selection_set)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_label_rows_horizon_time
			ON label_rows (horizon, decision_time DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_label_rows_strata
			ON label_rows (category, neg_risk, fv_source)`,
		`CREATE INDEX IF NOT EXISTS idx_label_rows_run
			ON label_rows (run_id) WHERE run_id IS NOT NULL`,
	}
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("db: ensure label_rows: %w", err)
		}
	}
	return nil
}

// InsertLabelRows upserts labels (batched).
func InsertLabelRows(ctx context.Context, conn DBInterface, rows []LabelRow) error {
	if conn == nil {
		return ErrNilDB
	}
	if len(rows) == 0 {
		return nil
	}
	args := make([][]any, 0, len(rows))
	for _, r := range rows {
		if r.ConditionID == "" || r.Horizon == "" || r.DecisionTime.IsZero() {
			continue
		}
		sel := r.SelectionSet
		if sel == "" {
			sel = "board_at_t"
		}
		args = append(args, []any{
			r.DecisionTime, r.Horizon, r.ConditionID, sel, r.RunID,
			r.Hit, r.AfterCostReturnBps, r.Category, r.NegRisk, r.FVSource,
			r.TTRBucket, r.MidAtT, r.EdgeBpsAtT,
		})
	}
	if len(args) == 0 {
		return nil
	}
	const sql = `
		INSERT INTO label_rows (
			decision_time, horizon, condition_id, selection_set, run_id,
			hit, after_cost_return_bps, category, neg_risk, fv_source,
			ttr_bucket, mid_at_t, edge_bps_at_t
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		ON CONFLICT (decision_time, horizon, condition_id, selection_set) DO UPDATE SET
			run_id = EXCLUDED.run_id,
			hit = EXCLUDED.hit,
			after_cost_return_bps = EXCLUDED.after_cost_return_bps,
			category = EXCLUDED.category,
			neg_risk = EXCLUDED.neg_risk,
			fv_source = EXCLUDED.fv_source,
			ttr_bucket = EXCLUDED.ttr_bucket,
			mid_at_t = EXCLUDED.mid_at_t,
			edge_bps_at_t = EXCLUDED.edge_bps_at_t`
	if err := BatchExec(ctx, conn, sql, args); err != nil {
		return fmt.Errorf("db: insert label_rows: %w", err)
	}
	return nil
}

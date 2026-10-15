package db

import (
	"context"
	"fmt"
	"time"
)

// AllocationRow is one whole-board capital-allocation decision (audit).
type AllocationRow struct {
	Time          time.Time
	RunID         string
	Strategy      string
	ConditionID   string
	TokenID       string
	Rank          int
	EdgeBps       float64
	Conviction    float64
	Taken         bool
	RejectReason  string
	BoardRunID    string
	SelectedAt    time.Time
}

// EnsureAllocationsTable creates allocations if missing.
func EnsureAllocationsTable(ctx context.Context, conn DBInterface) error {
	if conn == nil {
		return ErrNilDB
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS allocations (
			time            TIMESTAMPTZ NOT NULL,
			run_id          TEXT,
			strategy        TEXT NOT NULL DEFAULT 'default',
			condition_id    TEXT NOT NULL,
			token_id        TEXT,
			rank            INTEGER,
			edge_bps        DOUBLE PRECISION,
			conviction      DOUBLE PRECISION,
			taken           BOOLEAN NOT NULL,
			reject_reason   TEXT,
			board_run_id    TEXT,
			selected_at     TIMESTAMPTZ,
			PRIMARY KEY (time, strategy, condition_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_allocations_time ON allocations (strategy, time DESC)`,
	}
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("db: ensure allocations: %w", err)
		}
	}
	return nil
}

// InsertAllocations appends allocation rows (best-effort audit; ignore empty).
func InsertAllocations(ctx context.Context, conn DBInterface, rows []AllocationRow) error {
	if conn == nil {
		return ErrNilDB
	}
	if len(rows) == 0 {
		return nil
	}
	if err := EnsureAllocationsTable(ctx, conn); err != nil {
		return err
	}
	args := make([][]any, 0, len(rows))
	for _, r := range rows {
		if r.ConditionID == "" {
			continue
		}
		ts := r.Time
		if ts.IsZero() {
			ts = time.Now().UTC()
		}
		sel := r.SelectedAt
		if sel.IsZero() {
			sel = ts
		}
		args = append(args, []any{
			ts, nullStr(r.RunID), r.Strategy, r.ConditionID, nullStr(r.TokenID),
			r.Rank, r.EdgeBps, r.Conviction, r.Taken, nullStr(r.RejectReason),
			nullStr(r.BoardRunID), sel,
		})
	}
	if len(args) == 0 {
		return nil
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("db: allocations begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, a := range args {
		_, err := tx.Exec(ctx, `
			INSERT INTO allocations (
				time, run_id, strategy, condition_id, token_id, rank,
				edge_bps, conviction, taken, reject_reason, board_run_id, selected_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
			ON CONFLICT (time, strategy, condition_id) DO NOTHING
		`, a...)
		if err != nil {
			return fmt.Errorf("db: allocation insert: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("db: allocations commit: %w", err)
	}
	return nil
}

package db

import (
	"context"
	"fmt"
	"time"
)

// EdgeBoardSnapshotRow is one lean historical board row (M4 L2/L3/L4/L7).
// Bound to published board N — never full universe.
type EdgeBoardSnapshotRow struct {
	SelectedAt     time.Time
	Strategy       string
	RunID          string
	ConditionID    string
	TokenID        string
	Rank           int
	EdgeBps        *float64
	Volume24hr     float64
	Category       string
	NegRisk        bool
	NegRiskGroupID string
	RelatedLegs    []string
}

// EnsureEdgeBoardSnapshotsTable creates append-only board history for PIT eval.
func EnsureEdgeBoardSnapshotsTable(ctx context.Context, conn DBInterface) error {
	if conn == nil {
		return ErrNilDB
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS edge_board_snapshots (
			selected_at         TIMESTAMPTZ NOT NULL,
			strategy            TEXT NOT NULL DEFAULT 'default',
			run_id              TEXT,
			condition_id        TEXT NOT NULL,
			token_id            TEXT,
			rank                INTEGER NOT NULL,
			edge_bps            DOUBLE PRECISION,
			volume_24hr         DOUBLE PRECISION,
			category            TEXT,
			neg_risk            BOOLEAN NOT NULL DEFAULT FALSE,
			neg_risk_group_id   TEXT,
			related_legs        TEXT[] NOT NULL DEFAULT '{}',
			PRIMARY KEY (strategy, selected_at, condition_id)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_edge_board_snapshots_time
			ON edge_board_snapshots (strategy, selected_at DESC)`,
	}
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return fmt.Errorf("db: edge_board_snapshots: %w", err)
		}
	}
	return nil
}

// InsertEdgeBoardSnapshots batch-inserts lean snapshot rows (O(board)).
// Uses ON CONFLICT DO NOTHING for idempotent re-runs of same selected_at.
func InsertEdgeBoardSnapshots(ctx context.Context, conn DBInterface, rows []EdgeBoardSnapshotRow) error {
	if conn == nil {
		return ErrNilDB
	}
	if len(rows) == 0 {
		return nil
	}
	if err := EnsureEdgeBoardSnapshotsTable(ctx, conn); err != nil {
		return err
	}
	// Multi-row via single tx; board N ≈ 50 — fine for hot path.
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("db: snapshots begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for _, r := range rows {
		if r.ConditionID == "" {
			continue
		}
		st := r.Strategy
		if st == "" {
			st = "default"
		}
		sel := r.SelectedAt
		if sel.IsZero() {
			sel = time.Now().UTC()
		}
		legs := r.RelatedLegs
		if legs == nil {
			legs = []string{}
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO edge_board_snapshots (
				selected_at, strategy, run_id, condition_id, token_id, rank,
				edge_bps, volume_24hr, category, neg_risk, neg_risk_group_id, related_legs
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NULLIF($11,''),$12)
			ON CONFLICT (strategy, selected_at, condition_id) DO NOTHING
		`,
			sel, st, nullStr(r.RunID), r.ConditionID, nullStr(r.TokenID), r.Rank,
			r.EdgeBps, r.Volume24hr, nullStr(r.Category), r.NegRisk, r.NegRiskGroupID, legs,
		)
		if err != nil {
			return fmt.Errorf("db: snapshot insert %s: %w", r.ConditionID, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("db: snapshots commit: %w", err)
	}
	return nil
}

// SnapshotRowsFromBoard maps live board rows to lean history rows.
func SnapshotRowsFromBoard(rows []EdgeBoardRow) []EdgeBoardSnapshotRow {
	out := make([]EdgeBoardSnapshotRow, 0, len(rows))
	for _, r := range rows {
		tok := ""
		if len(r.ClobTokenIDs) > 0 {
			tok = r.ClobTokenIDs[0]
		}
		out = append(out, EdgeBoardSnapshotRow{
			SelectedAt:     r.SelectedAt,
			Strategy:       r.Strategy,
			RunID:          r.RunID,
			ConditionID:    r.ConditionID,
			TokenID:        tok,
			Rank:           r.Rank,
			EdgeBps:        r.EdgeBps,
			Volume24hr:     r.Volume24hr,
			Category:       r.Category,
			NegRisk:        r.NegRisk,
			NegRiskGroupID: r.NegRiskGroupID,
			RelatedLegs:    r.RelatedLegs,
		})
	}
	return out
}

// LoadEdgeBoardSnapshots loads all snapshot rows for strategy in [from, to] (bulk).
func LoadEdgeBoardSnapshots(ctx context.Context, conn DBInterface, strategy string, from, to time.Time) ([]EdgeBoardSnapshotRow, error) {
	if conn == nil {
		return nil, ErrNilDB
	}
	if strategy == "" {
		strategy = "default"
	}
	if err := EnsureEdgeBoardSnapshotsTable(ctx, conn); err != nil {
		return nil, err
	}
	rows, err := conn.Query(ctx, `
		SELECT selected_at, strategy, COALESCE(run_id,''), condition_id, COALESCE(token_id,''),
		       rank, edge_bps, COALESCE(volume_24hr,0), COALESCE(category,''),
		       neg_risk, COALESCE(neg_risk_group_id,''), COALESCE(related_legs, '{}')
		FROM edge_board_snapshots
		WHERE strategy = $1 AND selected_at >= $2 AND selected_at <= $3
		ORDER BY selected_at ASC, rank ASC
	`, strategy, from.UTC(), to.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EdgeBoardSnapshotRow
	for rows.Next() {
		var r EdgeBoardSnapshotRow
		var legs []string
		if err := rows.Scan(
			&r.SelectedAt, &r.Strategy, &r.RunID, &r.ConditionID, &r.TokenID,
			&r.Rank, &r.EdgeBps, &r.Volume24hr, &r.Category,
			&r.NegRisk, &r.NegRiskGroupID, &legs,
		); err != nil {
			return nil, err
		}
		r.RelatedLegs = legs
		out = append(out, r)
	}
	return out, rows.Err()
}

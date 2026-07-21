package db

import (
	"context"
	"fmt"
	"time"
)

// EdgeBoardRow is one market on the current edge board.
type EdgeBoardRow struct {
	Strategy          string
	ConditionID       string
	MarketID          string
	QuestionShort     string
	Category          string
	ClobTokenIDs      []string
	Rank              int
	Score             float64
	EdgeBps           *float64
	StrategyVersionID *int64
	NegRisk           bool
	NegRiskGroupID    string
	RelatedLegs       []string
	Volume24hr        float64
	Liquidity         float64
	Spread            float64
	SelectedAt        time.Time
	RunID             string
}

// EnsureEdgeBoardTable creates edge_board if missing (idempotent for existing DBs).
func EnsureEdgeBoardTable(ctx context.Context, conn DBInterface) error {
	if conn == nil {
		return ErrNilDB
	}
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS edge_board (
    strategy              TEXT NOT NULL DEFAULT 'default',
    condition_id          TEXT NOT NULL,
    market_id             TEXT,
    question_short        TEXT,
    category              TEXT,
    clob_token_ids        TEXT[] NOT NULL DEFAULT '{}',
    rank                  INTEGER NOT NULL,
    score                 DOUBLE PRECISION,
    edge_bps              DOUBLE PRECISION,
    strategy_version_id   BIGINT,
    neg_risk              BOOLEAN NOT NULL DEFAULT FALSE,
    neg_risk_group_id     TEXT,
    related_legs          TEXT[] NOT NULL DEFAULT '{}',
    volume_24hr           DOUBLE PRECISION,
    liquidity             DOUBLE PRECISION,
    spread                DOUBLE PRECISION,
    selected_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    run_id                TEXT,
    PRIMARY KEY (strategy, condition_id)
)`,
		`CREATE INDEX IF NOT EXISTS idx_edge_board_rank ON edge_board (strategy, rank)`,
		`CREATE INDEX IF NOT EXISTS idx_edge_board_group ON edge_board (neg_risk_group_id) WHERE neg_risk_group_id IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_edge_board_selected ON edge_board (selected_at DESC)`,
	}
	for _, s := range stmts {
		if _, err := conn.Exec(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

// ReplaceEdgeBoard deletes the strategy's board and inserts the new set in one transaction.
func ReplaceEdgeBoard(ctx context.Context, conn DBInterface, strategy string, rows []EdgeBoardRow) error {
	if conn == nil {
		return ErrNilDB
	}
	if strategy == "" {
		strategy = "default"
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("db: edge_board begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `DELETE FROM edge_board WHERE strategy = $1`, strategy); err != nil {
		return fmt.Errorf("db: edge_board delete: %w", err)
	}

	for _, r := range rows {
		if r.ConditionID == "" {
			continue
		}
		tokens := r.ClobTokenIDs
		if tokens == nil {
			tokens = []string{}
		}
		legs := r.RelatedLegs
		if legs == nil {
			legs = []string{}
		}
		st := r.Strategy
		if st == "" {
			st = strategy
		}
		sel := r.SelectedAt
		if sel.IsZero() {
			sel = time.Now().UTC()
		}
		_, err := tx.Exec(ctx, `
			INSERT INTO edge_board (
				strategy, condition_id, market_id, question_short, category,
				clob_token_ids, rank, score, edge_bps, strategy_version_id,
				neg_risk, neg_risk_group_id, related_legs,
				volume_24hr, liquidity, spread, selected_at, run_id
			) VALUES (
				$1,$2,$3,$4,$5,
				$6,$7,$8,$9,$10,
				$11,NULLIF($12,''),$13,
				$14,$15,$16,$17,$18
			)`,
			st, r.ConditionID, nullStr(r.MarketID), nullStr(r.QuestionShort), nullStr(r.Category),
			tokens, r.Rank, r.Score, r.EdgeBps, r.StrategyVersionID,
			r.NegRisk, r.NegRiskGroupID, legs,
			r.Volume24hr, r.Liquidity, r.Spread, sel, nullStr(r.RunID),
		)
		if err != nil {
			return fmt.Errorf("db: edge_board insert %s: %w", r.ConditionID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("db: edge_board commit: %w", err)
	}
	return nil
}

// LoadEdgeBoardConditionIDs returns condition_ids currently on the board (sticky set).
func LoadEdgeBoardConditionIDs(ctx context.Context, conn DBInterface, strategy string) ([]string, error) {
	if conn == nil {
		return nil, ErrNilDB
	}
	if strategy == "" {
		strategy = "default"
	}
	rows, err := conn.Query(ctx, `
		SELECT condition_id FROM edge_board WHERE strategy = $1 ORDER BY rank
	`, strategy)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if id != "" {
			out = append(out, id)
		}
	}
	return out, rows.Err()
}

// LoadEdgeBoard returns full board rows ordered by rank.
func LoadEdgeBoard(ctx context.Context, conn DBInterface, strategy string) ([]EdgeBoardRow, error) {
	if conn == nil {
		return nil, ErrNilDB
	}
	if strategy == "" {
		strategy = "default"
	}
	rows, err := conn.Query(ctx, `
		SELECT strategy, condition_id, COALESCE(market_id,''), COALESCE(question_short,''), COALESCE(category,''),
			clob_token_ids, rank, COALESCE(score,0), edge_bps, strategy_version_id,
			neg_risk, COALESCE(neg_risk_group_id,''), related_legs,
			COALESCE(volume_24hr,0), COALESCE(liquidity,0), COALESCE(spread,0), selected_at, COALESCE(run_id,'')
		FROM edge_board WHERE strategy = $1 ORDER BY rank
	`, strategy)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []EdgeBoardRow
	for rows.Next() {
		var r EdgeBoardRow
		var tokens, legs []string
		if err := rows.Scan(
			&r.Strategy, &r.ConditionID, &r.MarketID, &r.QuestionShort, &r.Category,
			&tokens, &r.Rank, &r.Score, &r.EdgeBps, &r.StrategyVersionID,
			&r.NegRisk, &r.NegRiskGroupID, &legs,
			&r.Volume24hr, &r.Liquidity, &r.Spread, &r.SelectedAt, &r.RunID,
		); err != nil {
			return nil, err
		}
		r.ClobTokenIDs = tokens
		r.RelatedLegs = legs
		out = append(out, r)
	}
	return out, rows.Err()
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

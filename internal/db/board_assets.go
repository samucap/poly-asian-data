package db

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// BoardAssetMeta describes one board market for WS subscribe + paper signals.
type BoardAssetMeta struct {
	ConditionID       string
	MarketID          string
	QuestionShort     string
	Category          string
	TokenIDs          []string // all CLOB tokens on the board row
	PrimaryTokenID    string   // YES / first token
	Rank              int
	Score             float64
	EdgeBps           *float64
	CostBps           *float64
	CapacityUSD       *float64
	Urgency           *float64
	FairValue         *float64
	ModelEdgeBps      *float64
	FVSource          string
	NegRisk           bool
	NegRiskGroupID    string
	RelatedLegs       []string // condition_ids
	StrategyTags      []string
	RiskFlags         []string
	KeyFeatures       map[string]any
	StrategyVersionID *int64
	RunID             string
}

// BoardSubscribeSet is the capped token set for market WS + meta index.
type BoardSubscribeSet struct {
	// TokenIDs ordered for subscribe (primaries first by rank, then siblings).
	TokenIDs []string
	// ByToken maps token_id → board meta (primary token points at owning row).
	ByToken map[string]BoardAssetMeta
	// ByCondition maps condition_id → board meta for markets on the board.
	ByCondition map[string]BoardAssetMeta
	// Primaries are YES/first tokens for signal evaluation (board rows only).
	Primaries []BoardAssetMeta
}

// LoadBoardSubscribeSet loads edge_board tokens ∪ related-leg tokens, hard-capped.
// Preference: primary tokens of top ranks, then other tokens on board rows, then
// off-board related-leg tokens resolved via plymkt_markets.
func LoadBoardSubscribeSet(ctx context.Context, conn DBInterface, strategy string, maxAssets int) (*BoardSubscribeSet, error) {
	if conn == nil {
		return nil, ErrNilDB
	}
	if strategy == "" {
		strategy = "default"
	}
	if maxAssets <= 0 {
		maxAssets = 180
	}

	rows, err := conn.Query(ctx, `
		SELECT condition_id, COALESCE(market_id,''), COALESCE(question_short,''), COALESCE(category,''),
			clob_token_ids, rank, COALESCE(score,0), edge_bps, cost_bps, capacity_usd, urgency,
			fair_value, model_edge_bps, COALESCE(fv_source,''),
			neg_risk, COALESCE(neg_risk_group_id,''), COALESCE(related_legs, '{}'),
			COALESCE(strategy_tags, '{}'), COALESCE(risk_flags, '{}'),
			key_features, strategy_version_id, COALESCE(run_id,'')
		FROM edge_board
		WHERE strategy = $1
		ORDER BY rank ASC
	`, strategy)
	if err != nil {
		return nil, fmt.Errorf("db: load edge_board assets: %w", err)
	}
	defer rows.Close()

	var board []BoardAssetMeta
	legConds := make(map[string]struct{})
	for rows.Next() {
		var m BoardAssetMeta
		var tokens []string
		var legs []string
		var tags []string
		var flags []string
		var kf []byte
		if err := rows.Scan(
			&m.ConditionID, &m.MarketID, &m.QuestionShort, &m.Category,
			&tokens, &m.Rank, &m.Score, &m.EdgeBps, &m.CostBps, &m.CapacityUSD, &m.Urgency,
			&m.FairValue, &m.ModelEdgeBps, &m.FVSource,
			&m.NegRisk, &m.NegRiskGroupID, &legs,
			&tags, &flags, &kf, &m.StrategyVersionID, &m.RunID,
		); err != nil {
			return nil, err
		}
		m.TokenIDs = cleanTokens(tokens)
		if len(m.TokenIDs) > 0 {
			m.PrimaryTokenID = m.TokenIDs[0]
		}
		m.RelatedLegs = legs
		m.StrategyTags = tags
		m.RiskFlags = flags
		if len(kf) > 0 {
			_ = json.Unmarshal(kf, &m.KeyFeatures)
		}
		if m.ConditionID == "" || m.PrimaryTokenID == "" {
			continue
		}
		board = append(board, m)
		for _, leg := range legs {
			if leg != "" && leg != m.ConditionID {
				legConds[leg] = struct{}{}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Remove legs already on board.
	onBoard := make(map[string]struct{}, len(board))
	for _, m := range board {
		onBoard[m.ConditionID] = struct{}{}
	}
	var missingLegs []string
	for leg := range legConds {
		if _, ok := onBoard[leg]; !ok {
			missingLegs = append(missingLegs, leg)
		}
	}

	// Resolve off-board related legs → tokens (batch).
	legTokens, err := loadTokensForConditions(ctx, conn, missingLegs)
	if err != nil {
		return nil, err
	}

	out := &BoardSubscribeSet{
		ByToken:     make(map[string]BoardAssetMeta),
		ByCondition: make(map[string]BoardAssetMeta, len(board)),
	}
	seen := make(map[string]struct{})

	addToken := func(tid string, meta BoardAssetMeta) {
		if tid == "" {
			return
		}
		if _, ok := seen[tid]; ok {
			return
		}
		if len(out.TokenIDs) >= maxAssets {
			return
		}
		seen[tid] = struct{}{}
		out.TokenIDs = append(out.TokenIDs, tid)
		out.ByToken[tid] = meta
	}

	// Pass 1: primary tokens by rank.
	for _, m := range board {
		out.ByCondition[m.ConditionID] = m
		out.Primaries = append(out.Primaries, m)
		addToken(m.PrimaryTokenID, m)
	}
	// Pass 2: remaining tokens on board rows.
	for _, m := range board {
		for _, tid := range m.TokenIDs {
			if tid == m.PrimaryTokenID {
				continue
			}
			addToken(tid, m)
		}
	}
	// Pass 3: off-board group siblings.
	for cond, toks := range legTokens {
		meta := BoardAssetMeta{
			ConditionID: cond,
			TokenIDs:    toks,
		}
		if len(toks) > 0 {
			meta.PrimaryTokenID = toks[0]
		}
		for _, tid := range toks {
			addToken(tid, meta)
		}
	}

	return out, nil
}

func cleanTokens(in []string) []string {
	out := make([]string, 0, len(in))
	for _, t := range in {
		t = strings.TrimSpace(t)
		if t != "" {
			out = append(out, t)
		}
	}
	return out
}

func loadTokensForConditions(ctx context.Context, conn DBInterface, conditionIDs []string) (map[string][]string, error) {
	out := make(map[string][]string)
	if len(conditionIDs) == 0 {
		return out, nil
	}
	rows, err := conn.Query(ctx, `
		SELECT condition_id, COALESCE(clob_token_ids, '')
		FROM plymkt_markets
		WHERE condition_id = ANY($1)
		  AND clob_token_ids IS NOT NULL AND clob_token_ids <> ''
	`, conditionIDs)
	if err != nil {
		// Table may be missing in unit tests without DB — surface error to caller.
		return out, fmt.Errorf("db: load leg tokens: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid, raw string
		if err := rows.Scan(&cid, &raw); err != nil {
			return out, err
		}
		toks := parseClobTokenIDs(raw)
		if len(toks) > 0 {
			out[cid] = toks
		}
	}
	return out, rows.Err()
}

// parseClobTokenIDs handles JSON array text or postgres-array-ish dumps.
func parseClobTokenIDs(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	if raw[0] == '[' {
		var arr []string
		if err := json.Unmarshal([]byte(raw), &arr); err == nil {
			return cleanTokens(arr)
		}
	}
	// Single token or comma-separated.
	parts := strings.Split(raw, ",")
	return cleanTokens(parts)
}

// CapTokenIDs is a pure helper for tests: prefer order, hard max.
func CapTokenIDs(ordered []string, max int) []string {
	if max <= 0 || len(ordered) <= max {
		return append([]string(nil), ordered...)
	}
	return append([]string(nil), ordered[:max]...)
}

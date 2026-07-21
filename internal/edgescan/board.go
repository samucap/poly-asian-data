// Package edgescan builds the bounded edge board from filtered Gamma events
// (M2: activity-score parity bridge; M3 will replace score with edge_bps).
package edgescan

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/samucap/poly-asian-data/internal/artifacts"
	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/marketranking"
	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/samucap/poly-asian-data/internal/tagagg"
)

const (
	// SchemaVersion is edge_board artifact schema_version (M3 cost-aware board).
	SchemaVersion = "2.0"
	// ArtifactPipeline is artifacts/ subdirectory.
	ArtifactPipeline = "edge_board"
	// DefaultStrategy partitions edge_board rows.
	DefaultStrategy = "default"
)

// Candidate is a tradable market with event-level context for board building.
type Candidate struct {
	Market         *services.PlyMktMarket
	EventID        string
	Category       string
	NegRisk        bool
	NegRiskGroupID string
	NegRiskFeeBips int
}

// BuildOptions controls Stage-1 rank and final board size.
type BuildOptions struct {
	Filter     marketranking.MarketFilter
	BoardMaxN  int
	// StickyConditionIDs from prior board; members still in pool get a small score boost
	// and are preferred when cutting to BoardMaxN after rank (re-include if dropped by MaxN).
	StickyConditionIDs []string
	Strategy           string
	RunID              string
	Now                time.Time
}

// BoardBuildResult is the ranked board plus drop diagnostics.
type BoardBuildResult struct {
	Rows           []db.EdgeBoardRow
	Stage1Count    int
	PoolCount      int
	DroppedSummary map[string]int
}

// FlattenCandidates extracts tradable markets from events with neg-risk context.
func FlattenCandidates(events []*services.PlyMktEvent) []Candidate {
	var out []Candidate
	for _, e := range events {
		if e == nil {
			continue
		}
		nr := e.NegRisk || e.EnableNegRisk
		group := e.NegRiskMarketID
		fee := e.NegRiskFeeBips
		// Category slug from first non-catch-all top-like tag slug if present.
		cat := firstCategorySlug(e)
		for i := range e.Markets {
			m := e.Markets[i]
			if m == nil || !tagagg.IsTradable(m) {
				continue
			}
			m.EventID = e.ID
			if m.Category == "" {
				m.Category = cat
			}
			out = append(out, Candidate{
				Market:         m,
				EventID:        e.ID,
				Category:       m.Category,
				NegRisk:        nr,
				NegRiskGroupID: group,
				NegRiskFeeBips: fee,
			})
		}
	}
	return out
}

func firstCategorySlug(e *services.PlyMktEvent) string {
	for _, t := range e.Tags {
		if t == nil || t.ID == "" || tagagg.IsCatchAllTag(t) {
			continue
		}
		if t.Slug != "" {
			return t.Slug
		}
		if t.Label != "" {
			return strings.ToLower(strings.ReplaceAll(t.Label, " ", "-"))
		}
	}
	return ""
}

// BuildBoard materializes Stage-1 activity rows without edge scoring (tests / diagnostics).
// Production uses SelectStage1 + BuildEdgeBoard (or RunCycle). Prefer those.
func BuildBoard(pool []Candidate, opts BuildOptions) BoardBuildResult {
	if opts.Strategy == "" {
		opts.Strategy = DefaultStrategy
	}
	if opts.Now.IsZero() {
		opts.Now = time.Now().UTC()
	}
	if opts.BoardMaxN <= 0 {
		opts.BoardMaxN = 50
	}
	if opts.Filter.MaxN <= 0 {
		opts.Filter.MaxN = 200
	}
	// Cap Stage-1 to BoardMaxN for the diagnostic path so tests stay small.
	if opts.Filter.MaxN > opts.BoardMaxN {
		opts.Filter.MaxN = opts.BoardMaxN
	}

	stage1 := SelectStage1(pool, opts)
	groupMembers := groupMemberIndex(stage1.ByCond)
	rows := make([]db.EdgeBoardRow, 0, len(stage1.Candidates))
	for i, c := range stage1.Candidates {
		if c.Market == nil {
			continue
		}
		m := c.Market
		tokens := parseTokenIDs(m.ClobTokenIds)
		legs := relatedLegs(m.ConditionID, c.NegRiskGroupID, groupMembers)
		liq := marketLiquidity(m)
		vol24 := m.Volume24hr
		if m.Volume24hrClob != 0 {
			vol24 = m.Volume24hrClob
		}
		rows = append(rows, db.EdgeBoardRow{
			Strategy:       opts.Strategy,
			ConditionID:    m.ConditionID,
			MarketID:       m.ID,
			QuestionShort:  shortQuestion(m.Question, 120),
			Category:       c.Category,
			ClobTokenIDs:   tokens,
			Rank:           i + 1,
			Score:          m.ComputedScore,
			EdgeBps:        nil,
			NegRisk:        c.NegRisk,
			NegRiskGroupID: c.NegRiskGroupID,
			RelatedLegs:    legs,
			Volume24hr:     vol24,
			Liquidity:      liq,
			Spread:         m.Spread,
			SelectedAt:     opts.Now,
			RunID:          opts.RunID,
		})
	}
	dropped := copyDropped(stage1.DroppedSummary)
	dropped["board_cap"] = 0
	return BoardBuildResult{
		Rows:           rows,
		Stage1Count:    stage1.Stage1Count,
		PoolCount:      stage1.PoolCount,
		DroppedSummary: dropped,
	}
}

func selectBoard(ranked []*services.PlyMktMarket, sticky map[string]bool, maxN int) []*services.PlyMktMarket {
	if maxN <= 0 || len(ranked) <= maxN {
		return ranked
	}
	// First pass: all sticky in ranked order, then fill with non-sticky.
	var out []*services.PlyMktMarket
	seen := map[string]bool{}
	for _, m := range ranked {
		if m == nil || seen[m.ConditionID] {
			continue
		}
		if sticky[m.ConditionID] {
			out = append(out, m)
			seen[m.ConditionID] = true
			if len(out) >= maxN {
				return out
			}
		}
	}
	for _, m := range ranked {
		if m == nil || seen[m.ConditionID] {
			continue
		}
		out = append(out, m)
		seen[m.ConditionID] = true
		if len(out) >= maxN {
			break
		}
	}
	// Preserve score order overall
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].ComputedScore > out[j].ComputedScore
	})
	// Re-assign is fine; ranks set by caller after this sort
	return out
}

func relatedLegs(self, group string, members map[string][]string) []string {
	if group == "" {
		return []string{}
	}
	var out []string
	for _, id := range members[group] {
		if id != self {
			out = append(out, id)
		}
	}
	return out
}

func passesHardFilter(m *services.PlyMktMarket, f marketranking.MarketFilter) bool {
	if m == nil {
		return false
	}
	liq := marketLiquidity(m)
	if m.Volume24hr < f.MinVolume24hr {
		return false
	}
	if liq < f.MinLiquidity {
		return false
	}
	if m.Spread > f.MaxSpread {
		return false
	}
	// MinVolatility 0 disables the check.
	if f.MinVolatility > 0 {
		if abs(m.OneDayPriceChange) < f.MinVolatility {
			return false
		}
	}
	return true
}

func marketLiquidity(m *services.PlyMktMarket) float64 {
	if m.LiquidityClob != 0 {
		return m.LiquidityClob
	}
	if m.LiquidityNum != 0 {
		return m.LiquidityNum
	}
	if m.Liquidity != "" {
		if v, err := strconv.ParseFloat(m.Liquidity, 64); err == nil {
			return v
		}
	}
	return 0
}

func parseTokenIDs(raw string) []string {
	if raw == "" {
		return []string{}
	}
	var ids []string
	if err := json.Unmarshal([]byte(raw), &ids); err != nil || ids == nil {
		return []string{}
	}
	return ids
}

func shortQuestion(q string, max int) string {
	q = strings.TrimSpace(q)
	if max <= 0 || utf8.RuneCountInString(q) <= max {
		return q
	}
	runes := []rune(q)
	return string(runes[:max-1]) + "…"
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// --- Artifact ---

// EdgeBoardV1 is the versioned edge-scan board artifact.
type EdgeBoardV1 struct {
	artifacts.Envelope
	Strategy       string         `json:"strategy"`
	BoardStats     BoardStats     `json:"board_stats"`
	Board          []BoardEntry   `json:"board"`
	DroppedSummary map[string]int `json:"dropped_summary"`
}

// BoardStats summarizes the board for agents.
type BoardStats struct {
	NCandidates      int     `json:"n_candidates"`
	NStage1          int     `json:"n_stage1"`
	NBoard           int     `json:"n_board"`
	MedianScore      float64 `json:"median_score"`
	MedianEdgeBps    float64 `json:"median_edge_bps,omitempty"`
	TotalCapacity    float64 `json:"total_capacity_usd"`
	TotalLiquidity   float64 `json:"total_liquidity"`
	CycleMS          int64   `json:"cycle_ms,omitempty"`
	EnrichMS         int64   `json:"enrich_ms,omitempty"`
	EnrichCoverage   float64 `json:"enrich_coverage,omitempty"`
	FeatureAgeP95MS  int64   `json:"feature_age_p95_ms,omitempty"`
	EdgeBpsNullFrac  float64 `json:"edge_bps_null_frac,omitempty"`
}

// BoardEntry is one self-contained board row for agents / WS.
type BoardEntry struct {
	Rank           int            `json:"rank"`
	ConditionID    string         `json:"condition_id"`
	MarketID       string         `json:"market_id,omitempty"`
	QuestionShort  string         `json:"question_short"`
	Category       string         `json:"category,omitempty"`
	TokenIDs       []string       `json:"token_ids"`
	Score          float64        `json:"score"` // Stage-1 activity diagnostic
	EdgeBps        *float64       `json:"edge_bps"`
	CostBps        *float64       `json:"cost_bps,omitempty"`
	CapacityUSD    *float64       `json:"capacity_usd,omitempty"`
	Urgency        *float64       `json:"urgency,omitempty"`
	KeyFeatures    map[string]any `json:"key_features,omitempty"`
	RiskFlags      []string       `json:"risk_flags,omitempty"`
	StrategyTags   []string       `json:"strategy_tags,omitempty"`
	FairValue      *float64       `json:"fair_value,omitempty"`
	ModelEdgeBps   *float64       `json:"model_edge_bps,omitempty"`
	FVSource       string         `json:"fv_source,omitempty"`
	NegRisk        bool           `json:"neg_risk"`
	NegRiskGroupID string         `json:"neg_risk_group_id,omitempty"`
	RelatedLegs    []string       `json:"related_legs"`
	Volume24hr     float64        `json:"volume_24hr"`
	Liquidity      float64        `json:"liquidity"`
	Spread         float64        `json:"spread"`
}

// BuildArtifact builds a validated edge_board document (schema 2.0).
func BuildArtifact(rows []db.EdgeBoardRow, poolN, stage1N int, dropped map[string]int, status string, errs []artifacts.ErrorItem) (EdgeBoardV1, error) {
	return BuildArtifactWithStats(rows, poolN, stage1N, dropped, status, errs, BoardStats{})
}

// BuildArtifactWithStats allows callers to attach lag / enrich metrics.
func BuildArtifactWithStats(rows []db.EdgeBoardRow, poolN, stage1N int, dropped map[string]int, status string, errs []artifacts.ErrorItem, extra BoardStats) (EdgeBoardV1, error) {
	entries := make([]BoardEntry, 0, len(rows))
	var scores []float64
	var edges []float64
	var liqSum, capSum float64
	var nullEdge int
	for _, r := range rows {
		var kf map[string]any
		if len(r.KeyFeatures) > 0 {
			_ = json.Unmarshal(r.KeyFeatures, &kf)
		}
		entries = append(entries, BoardEntry{
			Rank:           r.Rank,
			ConditionID:    r.ConditionID,
			MarketID:       r.MarketID,
			QuestionShort:  r.QuestionShort,
			Category:       r.Category,
			TokenIDs:       r.ClobTokenIDs,
			Score:          r.Score,
			EdgeBps:        r.EdgeBps,
			CostBps:        r.CostBps,
			CapacityUSD:    r.CapacityUSD,
			Urgency:        r.Urgency,
			KeyFeatures:    kf,
			RiskFlags:      r.RiskFlags,
			StrategyTags:   r.StrategyTags,
			FairValue:      r.FairValue,
			ModelEdgeBps:   r.ModelEdgeBps,
			FVSource:       r.FVSource,
			NegRisk:        r.NegRisk,
			NegRiskGroupID: r.NegRiskGroupID,
			RelatedLegs:    r.RelatedLegs,
			Volume24hr:     r.Volume24hr,
			Liquidity:      r.Liquidity,
			Spread:         r.Spread,
		})
		scores = append(scores, r.Score)
		liqSum += r.Liquidity
		if r.CapacityUSD != nil {
			capSum += *r.CapacityUSD
		}
		if r.EdgeBps != nil {
			edges = append(edges, *r.EdgeBps)
		} else {
			nullEdge++
		}
	}
	sort.Float64s(scores)
	sort.Float64s(edges)
	med := median(scores)
	medEdge := median(edges)
	if dropped == nil {
		dropped = map[string]int{}
	}
	if errs == nil {
		errs = []artifacts.ErrorItem{}
	}

	hashIn := struct {
		Board []BoardEntry `json:"board"`
	}{Board: entries}
	h, err := artifacts.HashCanonicalJSON(hashIn)
	if err != nil {
		return EdgeBoardV1{}, err
	}
	env := artifacts.NewEnvelope(SchemaVersion, h)
	if status != "" {
		env.Status = status
	}
	env.Errors = errs
	strategy := DefaultStrategy
	if len(rows) > 0 && rows[0].Strategy != "" {
		strategy = rows[0].Strategy
	}

	nullFrac := 0.0
	if len(rows) > 0 {
		nullFrac = float64(nullEdge) / float64(len(rows))
	}
	stats := BoardStats{
		NCandidates:     poolN,
		NStage1:         stage1N,
		NBoard:          len(entries),
		MedianScore:     med,
		MedianEdgeBps:   medEdge,
		TotalCapacity:   capSum,
		TotalLiquidity:  liqSum,
		CycleMS:         extra.CycleMS,
		EnrichMS:        extra.EnrichMS,
		EnrichCoverage:  extra.EnrichCoverage,
		FeatureAgeP95MS: extra.FeatureAgeP95MS,
		EdgeBpsNullFrac: nullFrac,
	}

	doc := EdgeBoardV1{
		Envelope:       env,
		Strategy:       strategy,
		BoardStats:     stats,
		Board:          entries,
		DroppedSummary: dropped,
	}
	return doc, nil
}

func median(xs []float64) float64 {
	n := len(xs)
	if n == 0 {
		return 0
	}
	if n%2 == 1 {
		return xs[n/2]
	}
	return (xs[n/2-1] + xs[n/2]) / 2
}

// ValidateEdgeBoardV1 checks required fields (schema 1.0 legacy or 2.0 M3).
func ValidateEdgeBoardV1(doc map[string]any) error {
	required := []string{
		"schema_version", "pipeline_version", "run_id", "generated_at",
		"input_hash", "code_commit", "status", "errors",
		"strategy", "board_stats", "board", "dropped_summary",
	}
	for _, k := range required {
		if _, ok := doc[k]; !ok {
			return fmt.Errorf("edge_board: missing %q", k)
		}
	}
	sv, _ := doc["schema_version"].(string)
	if sv != "1.0" && sv != "2.0" {
		return fmt.Errorf("edge_board: schema_version must be 1.0 or 2.0, got %q", sv)
	}
	return nil
}

// WriteArtifact validates and writes edge_board artifact.
func WriteArtifact(doc EdgeBoardV1, root string) (artifacts.WriteResult, error) {
	b, err := json.Marshal(doc)
	if err != nil {
		return artifacts.WriteResult{}, err
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return artifacts.WriteResult{}, err
	}
	if err := ValidateEdgeBoardV1(m); err != nil {
		return artifacts.WriteResult{}, err
	}
	return artifacts.WriteJSON(doc.RunID, ArtifactPipeline, doc, artifacts.WriteOptions{
		Root:        root,
		WriteLatest: true,
	})
}

package risk

import (
	"math"
	"sort"
)

// Rankable is a signal candidate for opportunity-aware budget allocation.
type Rankable struct {
	Index      int
	EdgeBps    float64
	Conviction float64
	Urgency    float64
	Score      float64
}

// OpportunityScore ranks a candidate for scarce budget (higher = better).
// Uses |edge_bps| and conviction; optional urgency boost.
func OpportunityScore(cfg Config, edgeBps, conviction, urgency float64) float64 {
	ew := cfg.OpportunityEdgeWeight
	if ew <= 0 {
		ew = 1
	}
	cw := cfg.OpportunityConvictionWeight
	if cw <= 0 {
		cw = 1
	}
	conv := conviction
	if conv < 0 {
		conv = 0
	}
	if conv > 1 {
		conv = 1
	}
	// Normalize edge: 100 bps → 1.0 unit
	edgeUnit := math.Abs(edgeBps) / 100.0
	score := ew*edgeUnit + cw*conv
	if urgency > 0 {
		score += 0.1 * urgency
	}
	return score
}

// SortByOpportunity sorts rankables descending by Score (stable on Index).
func SortByOpportunity(items []Rankable) {
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].Score == items[j].Score {
			return items[i].Index < items[j].Index
		}
		return items[i].Score > items[j].Score
	})
}

// BuildRankOrder returns original indices in opportunity-descending order.
func BuildRankOrder(cfg Config, edges, convictions, urgencies []float64) []int {
	n := len(edges)
	items := make([]Rankable, n)
	for i := 0; i < n; i++ {
		c, u := 0.0, 0.0
		if i < len(convictions) {
			c = convictions[i]
		}
		if i < len(urgencies) {
			u = urgencies[i]
		}
		items[i] = Rankable{
			Index:      i,
			EdgeBps:    edges[i],
			Conviction: c,
			Urgency:    u,
			Score:      OpportunityScore(cfg, edges[i], c, u),
		}
	}
	SortByOpportunity(items)
	out := make([]int, n)
	for i, it := range items {
		out[i] = it.Index
	}
	return out
}

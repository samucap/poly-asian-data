// Package alloc is capital allocation over a board (whole listen set), not a firehose of intents.
//
// Run considers every name passed in, drops those that fail worthiness, ranks the rest
// with risk.OpportunityScore, and takes the top K under max_positions / implied gross.
package alloc

import (
	"math"
	"time"

	"github.com/samucap/poly-asian-data/internal/risk"
)

// ReasonUnworthy / ReasonOutranked / ReasonAlreadyOpen are reject codes.
const (
	ReasonUnworthy    = "unworthy"
	ReasonOutranked   = "outranked"
	ReasonAlreadyOpen = "already_open"
)

// Name is one board market at a decision time (live books or a snapshot row).
type Name struct {
	Time              time.Time
	ConditionID       string
	TokenID           string
	MarketID          string
	Side              string
	EdgeBps           float64
	Conviction        float64
	Urgency           float64
	SpreadBps         float64
	Mid               float64
	CostBps           float64
	SizeUSD           float64
	CapacityUSD       float64
	FVSource          string
	ScorePath         string
	HorizonSec        int
	Strategy          string
	StrategyVersionID *int64
}

// Worthiness is the eligibility bar (not attention / who we watch).
type Worthiness struct {
	MinEdgeBps   float64
	MaxSpreadBps float64
	RequireFV    bool
}

// Rejected is a name that did not receive capital.
type Rejected struct {
	Name   Name
	Reason string
}

// Input is one allocation decision over the full current board.
type Input struct {
	Names []Name
	Risk  risk.Config
	Worth Worthiness
	Open  []string // condition_ids already in the book; replace_delta is not implemented
}

// Result is taken (ranked) vs rejected with reasons.
type Result struct {
	Taken    []Name
	Rejected []Rejected
}

// Run allocates capital across the whole board set.
func Run(in Input) Result {
	cfg := in.Risk
	cfg.Normalize()
	open := map[string]struct{}{}
	for _, id := range in.Open {
		if id != "" {
			open[id] = struct{}{}
		}
	}

	var (
		eligible []Name
		out      Result
	)
	for _, n := range in.Names {
		if why := unworthy(n, in.Worth); why != "" {
			out.Rejected = append(out.Rejected, Rejected{Name: n, Reason: why})
			continue
		}
		if n.ConditionID != "" {
			if _, ok := open[n.ConditionID]; ok {
				out.Rejected = append(out.Rejected, Rejected{Name: n, Reason: ReasonAlreadyOpen})
				continue
			}
		}
		eligible = append(eligible, n)
	}

	eligible = dedupeBest(cfg, eligible)

	maxN := cfg.MaxPositions
	if maxN <= 0 {
		maxN = len(eligible)
	}
	maxGross := cfg.MaxGrossUSD
	maxPos := cfg.MaxPositionUSD
	if maxPos <= 0 {
		maxPos = 500
	}

	if len(eligible) == 0 {
		return out
	}
	edges := make([]float64, len(eligible))
	convs := make([]float64, len(eligible))
	urgs := make([]float64, len(eligible))
	for i, n := range eligible {
		edges[i] = n.EdgeBps
		convs[i] = n.Conviction
		urgs[i] = n.Urgency
	}
	order := risk.BuildRankOrder(cfg, edges, convs, urgs)

	gross := 0.0
	for _, i := range order {
		n := eligible[i]
		if len(out.Taken) >= maxN {
			out.Rejected = append(out.Rejected, Rejected{Name: n, Reason: ReasonOutranked})
			continue
		}
		add := n.SizeUSD
		if add <= 0 {
			add = maxPos
		}
		if maxGross > 0 && gross+add > maxGross+1e-9 {
			out.Rejected = append(out.Rejected, Rejected{Name: n, Reason: ReasonOutranked})
			continue
		}
		out.Taken = append(out.Taken, n)
		gross += add
	}
	return out
}

func unworthy(n Name, w Worthiness) string {
	if w.MinEdgeBps > 0 && math.Abs(n.EdgeBps) < w.MinEdgeBps {
		return ReasonUnworthy
	}
	if w.MaxSpreadBps > 0 && n.SpreadBps > w.MaxSpreadBps {
		return ReasonUnworthy
	}
	if w.RequireFV {
		if n.FVSource == "" || n.FVSource == "none" || n.ScorePath == "proxy" {
			return ReasonUnworthy
		}
	}
	return ""
}

func dedupeBest(cfg risk.Config, batch []Name) []Name {
	best := map[string]int{}
	for i, n := range batch {
		id := n.ConditionID
		if id == "" {
			id = n.TokenID
		}
		if id == "" {
			id = n.TokenID + "#" + n.Time.UTC().Format(time.RFC3339Nano)
		}
		prev, ok := best[id]
		if !ok {
			best[id] = i
			continue
		}
		cur := risk.OpportunityScore(cfg, n.EdgeBps, n.Conviction, n.Urgency)
		old := risk.OpportunityScore(cfg, batch[prev].EdgeBps, batch[prev].Conviction, batch[prev].Urgency)
		if cur > old {
			best[id] = i
		}
	}
	out := make([]Name, 0, len(best))
	keep := map[int]struct{}{}
	for _, idx := range best {
		keep[idx] = struct{}{}
	}
	for i, n := range batch {
		if _, ok := keep[i]; ok {
			out = append(out, n)
		}
	}
	return out
}



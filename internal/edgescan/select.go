package edgescan

import "sort"

// scoredCand is a Stage-1 candidate with edge score (package-internal).
type scoredCand struct {
	c   Candidate
	res edgeScoreView
	act float64 // stage-1 activity score
}

// edgeScoreView is the subset of edge.ScoreResult needed for ranking/materialize.
type edgeScoreView struct {
	EdgeBps        float64
	OpportunityBps float64
	CostTotalBps   float64
	CapacityUSD    float64
	Urgency        float64
	RiskFlags      []string
	StrategyTags   []string
	KeyFeatures    map[string]any
	FairValue      *float64
	ModelEdgeBps   *float64
	FVSource       string
	HasBook        bool
	HalfSpreadBps  float64
	Mid            float64
	Drop           bool
}

// selectTopNByEdge keeps sticky members when cutting to maxN, then reorders by edge desc.
// scored must already be sorted by EdgeBps desc.
func selectTopNByEdge(scored []scoredCand, sticky map[string]bool, maxN int) []scoredCand {
	if maxN <= 0 || len(scored) <= maxN {
		return scored
	}
	out := make([]scoredCand, 0, maxN)
	seen := make(map[string]bool, maxN)

	for _, s := range scored {
		if s.c.Market == nil {
			continue
		}
		id := s.c.Market.ConditionID
		if !sticky[id] || seen[id] {
			continue
		}
		out = append(out, s)
		seen[id] = true
		if len(out) >= maxN {
			return sortScoredByEdge(out)
		}
	}
	for _, s := range scored {
		if s.c.Market == nil {
			continue
		}
		id := s.c.Market.ConditionID
		if seen[id] {
			continue
		}
		out = append(out, s)
		seen[id] = true
		if len(out) >= maxN {
			break
		}
	}
	return sortScoredByEdge(out)
}

func sortScoredByEdge(in []scoredCand) []scoredCand {
	out := append([]scoredCand(nil), in...)
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].res.EdgeBps == out[j].res.EdgeBps {
			return out[i].act > out[j].act
		}
		return out[i].res.EdgeBps > out[j].res.EdgeBps
	})
	return out
}

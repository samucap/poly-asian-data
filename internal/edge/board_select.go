package edge

import "sort"

// BoardCandidate is one market ready for board selection (live or offline PIT).
// Shared by edge-scan and edge-eval so promote eval matches production policy.
type BoardCandidate struct {
	Features FeatureVector
	// Optional FV path inputs
	GroupMids      map[string]float64 // condition_id → mid at decision time
	KnownGroupSize int
	NegRiskFeeBips float64
	TakerFeeBps    float64
	// TieBreak is Stage-1 activity (or offline volume proxy) when EdgeBps ties.
	TieBreak float64
}

// BoardSelectResult is one kept board row after policy filters + Score.
type BoardSelectResult struct {
	Candidate BoardCandidate
	Result    ScoreResult
}

// SelectBoard applies production board policy (shared live edge-scan + offline eval):
//  1. drop extreme mid when DropExtremePrice
//  2. optional FV chain (neg-risk complement, …)
//  3. edge.Score
//  4. drop Score.Drop
//  5. sort by EdgeBps desc, TieBreak desc
//  6. cut to n (if n <= 0, default 50; pass len(cands) to keep all survivors)
//
// Live sticky retention is applied by edgescan after this helper (not part of promote eval).
func SelectBoard(cands []BoardCandidate, w Weights, n int) []BoardSelectResult {
	if w.Name == "" {
		w = DefaultWeights()
	}
	if n <= 0 {
		n = 50
	}
	chain := DefaultFVChainFromWeights(w)

	var kept []BoardSelectResult
	for _, c := range cands {
		f := c.Features
		if f.Mid == 0 && !f.MissingBook {
			f.FillBookDerived(w.MinDepthShares)
		}
		// Board policy: extreme mid (same as edgescan stage2).
		if w.DropExtremePrice && !f.MissingBook && IsExtremeMid(f.Mid, w.ExtremeLo, w.ExtremeHi) {
			continue
		}

		in := ScoreInput{
			Features:      f,
			TakerFeeBps:   c.TakerFeeBps,
			NegRiskFeeBps: c.NegRiskFeeBips,
		}

		if w.FVEnabled && chain.Enabled {
			known := c.KnownGroupSize
			if known <= 0 && len(c.GroupMids) > 0 {
				known = len(c.GroupMids)
			}
			q := chain.Resolve(FairValueInput{
				ConditionID:    f.ConditionID,
				Features:       f,
				GroupMids:      c.GroupMids,
				KnownGroupSize: known,
				NegRiskFeeBips: c.NegRiskFeeBips,
			})
			if q != nil {
				fv := q.FairValue
				in.FairValue = &fv
				in.FVSource = q.Source
			}
		}

		res := Score(in, w)
		if res.Drop {
			continue
		}
		c.Features = f
		kept = append(kept, BoardSelectResult{Candidate: c, Result: res})
	}

	sort.SliceStable(kept, func(i, j int) bool {
		if kept[i].Result.EdgeBps == kept[j].Result.EdgeBps {
			return kept[i].Candidate.TieBreak > kept[j].Candidate.TieBreak
		}
		return kept[i].Result.EdgeBps > kept[j].Result.EdgeBps
	})
	if n > len(kept) {
		n = len(kept)
	}
	return kept[:n]
}

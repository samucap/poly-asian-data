package signals

import "github.com/samucap/poly-asian-data/internal/alloc"

// NameFromPaper maps a live scored candidate onto the capital allocator.
func NameFromPaper(sig *PaperSignal) alloc.Name {
	if sig == nil {
		return alloc.Name{}
	}
	return alloc.Name{
		Time:              sig.Time,
		ConditionID:       sig.ConditionID,
		TokenID:           sig.TokenID,
		MarketID:          sig.MarketID,
		Side:              sig.Side,
		EdgeBps:           sig.EdgeBps,
		Conviction:        sig.Conviction,
		Urgency:           sig.Urgency,
		SpreadBps:         sig.SpreadBps,
		Mid:               sig.Mid,
		CostBps:           sig.CostBps,
		SizeUSD:           sig.SizeUSD,
		CapacityUSD:       sig.CapacityUSD,
		FVSource:          sig.FVSource,
		ScorePath:         sig.ScorePath,
		HorizonSec:        sig.HorizonSec,
		Strategy:          sig.Strategy,
		StrategyVersionID: sig.StrategyVersionID,
	}
}

// SelectTaken returns paper signals in alloc Taken order (whole-board top-K).
func SelectTaken(cands []*PaperSignal, res alloc.Result) []*PaperSignal {
	byCond := map[string]*PaperSignal{}
	for _, s := range cands {
		if s == nil {
			continue
		}
		byCond[s.ConditionID] = s
	}
	out := make([]*PaperSignal, 0, len(res.Taken))
	for _, n := range res.Taken {
		if s, ok := byCond[n.ConditionID]; ok {
			if s.Reason == nil {
				s.Reason = map[string]any{}
			}
			s.Reason["source"] = "board_alloc"
			s.Event = EventOpen
			out = append(out, s)
		}
	}
	return out
}

package signals

// BuildFactors maps board + live book into named factor scores (no new alpha).
func BuildFactors(board BoardSnap, book BookSnap, opportunityBps, costBps, netEdge float64) map[string]float64 {
	f := map[string]float64{
		"opportunity_bps": opportunityBps,
		"cost_bps":        costBps,
		"net_edge_bps":    netEdge,
		"imbalance":       book.Imbalance,
	}
	if board.EdgeBps != nil {
		f["board_edge_bps"] = *board.EdgeBps
	}
	if board.ModelEdgeBps != nil {
		f["model_edge_bps"] = *board.ModelEdgeBps
	}
	if board.FairValue != nil && book.Mid > 0 {
		f["fv_residual_bps"] = (*board.FairValue - book.Mid) * 10_000
	}
	if board.Urgency != nil {
		f["urgency"] = *board.Urgency
	}
	f["rank_score"] = board.Score
	if book.Mid > 0 && book.BestAsk > book.BestBid {
		f["spread_cost_bps"] = 10_000 * ((book.BestAsk - book.BestBid) / 2) / book.Mid
	}
	// Lift a few key_features if present.
	if board.KeyFeatures != nil {
		if v, ok := asFloat(board.KeyFeatures["abs_ret_5m"]); ok {
			f["abs_ret_5m"] = v
		}
		if v, ok := asFloat(board.KeyFeatures["volume_24hr"]); ok {
			f["volume_24hr"] = v
		}
	}
	return f
}

func asFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	default:
		return 0, false
	}
}

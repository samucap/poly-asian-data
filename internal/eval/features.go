package eval

import (
	"time"

	"github.com/samucap/poly-asian-data/internal/edge"
)

// CandidateFeatureNames are microstructure inputs for edge.Score (no circular rank/edge_bps).
var CandidateFeatureNames = []string{
	"mid", "best_bid", "best_ask", "spread_bps", "imbalance",
	"abs_ret_1m", "abs_ret_5m", "abs_ret_1h", "one_day_abs",
	"ttr_hours", "bid_depth", "ask_depth", "book_age_sec",
	"volume_proxy", // SeriesActivityProxy — not live volume_24hr
}

// Policy parity levels (surface + promote gate).
const (
	// PolicyParityScanBoard = extreme + Score + FV chain (same SelectBoard as architecture).
	PolicyParityScanBoard = "scan_board_v1"
	// PolicyParityScoreProxy = Score only without full policy (legacy; should not promote).
	PolicyParityScoreProxy = "score_proxy_v1"
)

// MetaAsOf is market meta safe for PIT at T (end_date known in advance; group from board snapshot).
type MetaAsOf struct {
	ConditionID    string
	TokenID        string
	Category       string
	NegRisk        bool
	NegRiskGroupID string
	RelatedLegs    []string // other condition_ids in group (from board when known)
	EndDate        time.Time
}

// FeaturesAsOf builds edge.FeatureVector at T from local series (+ optional book).
// Does not use current Gamma volume_24hr / one_day_price_change.
func FeaturesAsOf(
	t time.Time,
	meta MetaAsOf,
	prices PriceSeries,
	book *BookPoint,
	minDepth float64,
) (edge.FeatureVector, bool) {
	mid, ok := prices.MidAsOf(t)
	if !ok || mid <= 0 {
		return edge.FeatureVector{}, false
	}
	pts := prices.PointCount(t, 24*time.Hour)
	path := prices.PathActivity(t, 24*time.Hour)
	f := edge.FeatureVector{
		ConditionID:      meta.ConditionID,
		TokenID:          meta.TokenID,
		Mid:              mid,
		AbsRet1m:         prices.AbsRet(t, time.Minute),
		AbsRet5m:         prices.AbsRet(t, 5*time.Minute),
		AbsRet1h:         prices.AbsRet(t, time.Hour),
		OneDayAbs:        prices.AbsRet(t, 24*time.Hour),
		NegRisk:          meta.NegRisk,
		NegRiskGroupID:   meta.NegRiskGroupID,
		VolumeProxy:      edge.SeriesActivityProxy(pts, path),
		FeaturesAsOfUnix: t.Unix(),
	}
	// Volume24hr left 0 offline — activityFamily uses VolumeProxy.
	if !meta.EndDate.IsZero() {
		f.TTRHours = meta.EndDate.Sub(t).Hours()
	}
	if book != nil && book.BestBid > 0 && book.BestAsk > 0 && book.BestAsk >= book.BestBid {
		f.BestBid = book.BestBid
		f.BestAsk = book.BestAsk
		f.BidDepth = book.TotalBidDepth
		f.AskDepth = book.TotalAskDepth
		f.BookAgeSec = t.Sub(book.Time).Seconds()
		f.FillBookDerived(minDepth)
	} else {
		f.MissingBook = true
		f.Mid = mid
	}
	return f, true
}

// VolumeProxy24h is PIT series sample count (volume_top_n baseline).
func VolumeProxy24h(ps PriceSeries, t time.Time) float64 {
	return float64(ps.PointCount(t, 24*time.Hour))
}

// ActivityProxy24h is PIT sum|Δmid| (activity_stage1 baseline).
func ActivityProxy24h(ps PriceSeries, t time.Time) float64 {
	return ps.PathActivity(t, 24*time.Hour)
}

// GroupMidsAtT builds condition_id → mid for a neg-risk group from price series.
func GroupMidsAtT(
	t time.Time,
	self MetaAsOf,
	byCond map[string]MetaAsOf,
	prices map[string]PriceSeries,
) map[string]float64 {
	if self.NegRiskGroupID == "" && len(self.RelatedLegs) == 0 {
		return nil
	}
	out := map[string]float64{}
	// self
	if ps, ok := prices[self.TokenID]; ok {
		if mid, ok := ps.MidAsOf(t); ok && mid > 0 {
			out[self.ConditionID] = mid
		}
	}
	for _, cid := range self.RelatedLegs {
		m, ok := byCond[cid]
		if !ok {
			continue
		}
		ps, ok := prices[m.TokenID]
		if !ok {
			continue
		}
		if mid, ok := ps.MidAsOf(t); ok && mid > 0 {
			out[cid] = mid
		}
	}
	// also any universe member with same group id
	if self.NegRiskGroupID != "" {
		for cid, m := range byCond {
			if m.NegRiskGroupID != self.NegRiskGroupID || cid == self.ConditionID {
				continue
			}
			if _, exists := out[cid]; exists {
				continue
			}
			ps, ok := prices[m.TokenID]
			if !ok {
				continue
			}
			if mid, ok := ps.MidAsOf(t); ok && mid > 0 {
				out[cid] = mid
			}
		}
	}
	if len(out) < 2 {
		return nil
	}
	return out
}

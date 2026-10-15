package eval

import (
	"math"

	"github.com/samucap/poly-asian-data/internal/edge"
)

// FillParams are numeric costs applied to mid→horizon moves.
// Prefer CostFromScore / CostFromFeatures so rank and labels share TCA.
type FillParams struct {
	FeeBps        float64
	SlipBps       float64
	IncludeSpread bool
	HalfSpreadBps float64
	// ImpactBps and NegRiskFeeBps align with edge.CostBreakdown when set.
	ImpactBps     float64
	NegRiskFeeBps float64
	// TotalOverride when set replaces the sum (use ScoreResult.Cost.TotalCostBps).
	TotalOverride *float64
}

// DefaultFillParams matches configs/eval/default.yaml mid_fee_slip.
func DefaultFillParams() FillParams {
	return FillParams{
		FeeBps:        10,
		SlipBps:       5,
		IncludeSpread: true,
	}
}

// FillParamsFromModel maps the documented FillModel to numeric params.
func FillParamsFromModel(m FillModel, halfSpreadBps float64) FillParams {
	return FillParams{
		FeeBps:        m.FeeBps,
		SlipBps:       m.SlipBps,
		IncludeSpread: m.IncludeSpread,
		HalfSpreadBps: halfSpreadBps,
	}
}

// FillParamsFromCost uses live edge.ComputeCost stack (same as Score).
func FillParamsFromCost(c edge.CostBreakdown) FillParams {
	tot := c.TotalCostBps
	return FillParams{
		FeeBps:        c.FeeBps,
		HalfSpreadBps: c.HalfSpreadBps,
		ImpactBps:     c.ImpactBps,
		NegRiskFeeBps: c.NegRiskFeeBps,
		IncludeSpread: true,
		TotalOverride: &tot,
	}
}

// TotalCostBps is fee + slip + half-spread + impact + neg-risk (or TotalOverride).
func (p FillParams) TotalCostBps() float64 {
	if p.TotalOverride != nil {
		return *p.TotalOverride
	}
	c := math.Max(0, p.FeeBps) + math.Max(0, p.SlipBps) + math.Max(0, p.ImpactBps) + math.Max(0, p.NegRiskFeeBps)
	if p.IncludeSpread {
		c += math.Max(0, p.HalfSpreadBps)
	}
	return c
}

// RawMoveBps is (mid_h − mid_t) × 10_000 — same scale as model_edge.
func RawMoveBps(midT, midH float64) float64 {
	if midT <= 0 || midH <= 0 {
		return math.NaN()
	}
	return (midH - midT) * 10_000
}

// Action model constants (eval measurement; not live signals).
const (
	ActionLongYes      = "long_yes"
	ActionSignFromEdge = "sign_from_edge"
)

// AfterCostReturnBps applies fill costs to a long-YES mid→horizon move.
func AfterCostReturnBps(midT, midH float64, p FillParams) float64 {
	raw := RawMoveBps(midT, midH)
	if math.IsNaN(raw) {
		return raw
	}
	return raw - p.TotalCostBps()
}

// AfterCostReturnBpsAction applies action_model:
//   - long_yes: long YES mid move − costs
//   - sign_from_edge: long YES if edgeBpsAtT ≥ 0, else long NO (flip raw move) − costs
func AfterCostReturnBpsAction(midT, midH float64, p FillParams, actionModel string, edgeBpsAtT float64) float64 {
	raw := RawMoveBps(midT, midH)
	if math.IsNaN(raw) {
		return raw
	}
	switch actionModel {
	case ActionSignFromEdge:
		if edgeBpsAtT < 0 {
			raw = -raw
		}
	case ActionLongYes, "":
		// long YES
	default:
		// unknown → long YES
	}
	return raw - p.TotalCostBps()
}

// Hit is true when after-cost return is strictly positive.
func Hit(afterCostBps float64) bool {
	if math.IsNaN(afterCostBps) {
		return false
	}
	return afterCostBps > 0
}

// HalfSpreadBpsFromBook converts bid/ask into half-spread bps.
func HalfSpreadBpsFromBook(bestBid, bestAsk float64) float64 {
	if bestBid <= 0 || bestAsk <= 0 || bestAsk < bestBid {
		return 0
	}
	mid := (bestBid + bestAsk) / 2
	if mid <= 0 {
		return 0
	}
	return 10_000 * ((bestAsk - bestBid) / 2) / mid
}

// HalfSpreadBpsFromSpread uses absolute spread (probability units).
func HalfSpreadBpsFromSpread(spread, mid float64) float64 {
	if spread <= 0 || mid <= 0 {
		return 0
	}
	return 10_000 * (spread / 2) / mid
}

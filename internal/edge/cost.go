// Package edge provides pure, deterministic cost-aware opportunity scoring
// for M3 board ranking. No I/O — unit-testable; features arrive precomputed.
package edge

import "math"

// CostInput is everything needed to estimate execution cost in bps.
type CostInput struct {
	BestBid  float64
	BestAsk  float64
	BidDepth float64
	AskDepth float64
	// Levels optional ask-side levels for impact walk.
	Levels []BookLevel
	// SizeUSD is the notional used for impact / capacity estimates.
	SizeUSD float64
	// TakerFeeBps from market metadata or strategy default.
	TakerFeeBps float64
	// NegRiskFeeBps from event (0 if not neg-risk).
	NegRiskFeeBps float64
	// ImpactCapBps used by CapacityUSD.
	ImpactCapBps float64
	// ImpactCoeff scales simple sqrt impact when no depth walk.
	ImpactCoeff float64
}

// BookLevel is one price/size level for impact walk.
type BookLevel struct {
	Price float64
	Size  float64 // shares
}

// CostBreakdown is the explicit TCA stack (never bury costs in opaque scores).
type CostBreakdown struct {
	Mid           float64 `json:"mid"`
	HalfSpreadBps float64 `json:"half_spread_bps"`
	FeeBps        float64 `json:"fee_bps"`
	ImpactBps     float64 `json:"impact_bps"`
	NegRiskFeeBps float64 `json:"neg_risk_fee_bps"`
	TotalCostBps  float64 `json:"total_cost_bps"`
	CapacityUSD   float64 `json:"capacity_usd"`
	HasBook       bool    `json:"has_book"`
	SpreadInvalid bool    `json:"spread_invalid"`
}

// ComputeCost returns a full cost stack. Missing/invalid book → HasBook false.
func ComputeCost(in CostInput) CostBreakdown {
	out := CostBreakdown{
		FeeBps:        math.Max(0, in.TakerFeeBps),
		NegRiskFeeBps: math.Max(0, in.NegRiskFeeBps),
	}
	if in.BestBid <= 0 || in.BestAsk <= 0 || in.BestAsk < in.BestBid {
		out.SpreadInvalid = in.BestBid > 0 || in.BestAsk > 0
		out.TotalCostBps = out.FeeBps + out.NegRiskFeeBps
		return out
	}
	mid := (in.BestBid + in.BestAsk) / 2
	if mid <= 0 {
		out.SpreadInvalid = true
		out.TotalCostBps = out.FeeBps + out.NegRiskFeeBps
		return out
	}
	out.HasBook = true
	out.Mid = mid
	spread := in.BestAsk - in.BestBid
	out.HalfSpreadBps = 10_000 * (spread / 2) / mid

	size := in.SizeUSD
	if size <= 0 {
		size = 100
	}
	coeff := in.ImpactCoeff
	if coeff <= 0 {
		coeff = 50
	}
	if len(in.Levels) > 0 {
		out.ImpactBps = impactWalkBps(mid, in.BestAsk, in.Levels, size)
	} else {
		depthUSD := in.AskDepth * mid
		if depthUSD < 1e-9 {
			depthUSD = 1e-9
		}
		out.ImpactBps = coeff * math.Sqrt(size/depthUSD)
	}

	out.TotalCostBps = out.HalfSpreadBps + out.FeeBps + out.ImpactBps + out.NegRiskFeeBps

	capBps := in.ImpactCapBps
	if capBps <= 0 {
		capBps = 25
	}
	out.CapacityUSD = capacityUSD(in, mid, capBps, coeff)
	return out
}

func impactWalkBps(mid, bestAsk float64, levels []BookLevel, sizeUSD float64) float64 {
	if mid <= 0 || sizeUSD <= 0 {
		return 0
	}
	remaining := sizeUSD
	var sumPXSize, sumSize float64
	for _, lv := range levels {
		if lv.Price <= 0 || lv.Size <= 0 {
			continue
		}
		if bestAsk > 0 && lv.Price+1e-12 < bestAsk {
			continue
		}
		levelUSD := lv.Price * lv.Size
		takeUSD := math.Min(remaining, levelUSD)
		takeShares := takeUSD / lv.Price
		sumPXSize += lv.Price * takeShares
		sumSize += takeShares
		remaining -= takeUSD
		if remaining <= 1e-9 {
			break
		}
	}
	if sumSize <= 0 {
		return 0
	}
	vwap := sumPXSize / sumSize
	return 10_000 * (vwap - mid) / mid
}

func capacityUSD(in CostInput, mid, capBps, coeff float64) float64 {
	if mid <= 0 {
		return 0
	}
	depthUSD := in.AskDepth * mid
	if depthUSD < 1 {
		depthUSD = 1
	}
	if coeff <= 0 {
		return depthUSD
	}
	ratio := capBps / coeff
	if ratio < 0 {
		return 0
	}
	size := depthUSD * ratio * ratio
	maxVis := in.AskDepth * mid
	if maxVis > 0 && size > maxVis {
		size = maxVis
	}
	return size
}

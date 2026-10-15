package edge

import (
	"math"
	"sort"
)

// ScoreInput is one market's scoring bundle.
type ScoreInput struct {
	Features FeatureVector
	// FairValue is optional (probability/price in [0,1]). Nil → proxy opportunity path.
	FairValue *float64
	// FVSource labels the provider (e.g. "neg_risk_complement", "external_btc").
	FVSource string
	// TakerFeeBps overrides weights default when set (>0 or explicit 0 with market meta).
	TakerFeeBps float64
	// NegRiskFeeBps from event.
	NegRiskFeeBps float64
	// Levels for impact walk (optional).
	Levels []BookLevel
}

// ScoreResult is the cost-aware edge output for board ranking.
type ScoreResult struct {
	EdgeBps        float64        `json:"edge_bps"`
	OpportunityBps float64        `json:"opportunity_bps"`
	Cost           CostBreakdown  `json:"cost"`
	ModelEdgeBps   *float64       `json:"model_edge_bps,omitempty"`
	FairValue      *float64       `json:"fair_value,omitempty"`
	FVSource       string         `json:"fv_source,omitempty"`
	Urgency        float64        `json:"urgency"`
	RiskFlags      []string       `json:"risk_flags"`
	StrategyTags   []string       `json:"strategy_tags"`
	KeyFeatures    map[string]any `json:"key_features"`
	Drop           bool           `json:"drop"`
	Path           string         `json:"path"` // "proxy" | "fair_value"
}

// Scored pairs input with result for ranking helpers.
type Scored struct {
	Input  ScoreInput
	Result ScoreResult
}

// Score computes edge_bps = opportunity − cost (proxy) or (FV−mid)×1e4 − cost − buffer (FV path).
func Score(in ScoreInput, w Weights) ScoreResult {
	if w.Name == "" {
		w = DefaultWeights()
	}
	f := in.Features
	if f.Mid == 0 && !f.MissingBook {
		f.FillBookDerived(w.MinDepthShares)
	}
	if w.MaxFeatureAgeSec > 0 && f.FeatureAgeSec > w.MaxFeatureAgeSec {
		f.StaleFeatures = true
	}

	fee := in.TakerFeeBps
	if fee < 0 {
		fee = 0
	}
	if fee == 0 && w.DefaultTakerFeeBps > 0 {
		fee = w.DefaultTakerFeeBps
	}

	cost := ComputeCost(CostInput{
		BestBid:       f.BestBid,
		BestAsk:       f.BestAsk,
		BidDepth:      f.BidDepth,
		AskDepth:      f.AskDepth,
		Levels:        in.Levels,
		SizeUSD:       w.ProbeSizeUSD,
		TakerFeeBps:   fee,
		NegRiskFeeBps: in.NegRiskFeeBps,
		ImpactCapBps:  w.ImpactCapBps,
		ImpactCoeff:   w.ImpactCoeff,
	})

	res := ScoreResult{
		Cost:         cost,
		RiskFlags:    f.RiskFlags(w),
		KeyFeatures:  f.KeyFeatures(),
		StrategyTags: strategyTags(f),
		Urgency:      urgency(f),
		FairValue:    in.FairValue,
		FVSource:     in.FVSource,
	}

	if f.MissingBook || !cost.HasBook {
		if w.DropMissingBook {
			res.Drop = true
		}
		res.OpportunityBps = opportunityBps(f, w)
		res.EdgeBps = res.OpportunityBps - cost.TotalCostBps - 500
		res.Path = "proxy"
		return res
	}

	// Extreme mid is a board eligibility policy (edgescan), not applied here.

	if in.FairValue != nil {
		fv := *in.FairValue
		modelEdge := (fv - cost.Mid) * 10_000
		buf := w.ModelBufferBps
		if buf < 0 {
			buf = 0
		}
		net := modelEdge - cost.TotalCostBps - buf
		res.ModelEdgeBps = &net
		res.EdgeBps = net
		// OpportunityBps on FV path = raw model edge only (no multi-family mix).
		res.OpportunityBps = modelEdge
		res.Path = "fair_value"
	} else {
		res.OpportunityBps = opportunityBps(f, w)
		res.EdgeBps = res.OpportunityBps - cost.TotalCostBps
		res.Path = "proxy"
	}

	if f.StaleFeatures {
		res.EdgeBps -= w.StalePenaltyBps
	}
	return res
}

// RankByEdge scores inputs and returns them sorted by EdgeBps descending (drops filtered).
func RankByEdge(inputs []ScoreInput, w Weights) []Scored {
	out := make([]Scored, 0, len(inputs))
	for _, in := range inputs {
		r := Score(in, w)
		if r.Drop {
			continue
		}
		out = append(out, Scored{Input: in, Result: r})
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].Result.EdgeBps > out[j].Result.EdgeBps
	})
	return out
}

func opportunityBps(f FeatureVector, w Weights) float64 {
	vol := volFamily(f, w)
	oi := oiFamily(f, w)
	imb := imbalanceFamily(f)
	ttr := ttrFamily(f)
	// Incomplete groups: do not invent residual opportunity (flag only via RiskFlags).
	nr := 0.0
	if !f.NegRiskIncomplete {
		nr = math.Min(w.OICapBps, math.Abs(f.NegRiskResidualBps))
	}
	flow := flowFamily(f)
	act := activityFamily(f)

	return w.WVol*vol +
		w.WOI*oi +
		w.WImbalance*imb +
		w.WTTR*ttr +
		w.WNegRisk*nr +
		w.WFlow*flow +
		w.WActivity*act
}

func volFamily(f FeatureVector, w Weights) float64 {
	r := f.AbsRet5m
	if f.AbsRet1m > r {
		r = f.AbsRet1m
	}
	if r <= 0 {
		r = f.OneDayAbs
	}
	scale := w.VolScale
	if scale <= 0 {
		scale = 1
	}
	cap := w.VolCapBps
	if cap <= 0 {
		cap = 500
	}
	return math.Min(cap, math.Abs(r)*10_000*scale)
}

func oiFamily(f FeatureVector, w Weights) float64 {
	base := math.Abs(f.OI)
	if base < 1 {
		base = 1
	}
	rel := math.Abs(f.DOI1h) / base
	if math.Abs(f.DOI24h) > 0 {
		rel = math.Max(rel, math.Abs(f.DOI24h)/(base*4))
	}
	scale := w.OIScale
	if scale <= 0 {
		scale = 100
	}
	cap := w.OICapBps
	if cap <= 0 {
		cap = 200
	}
	return math.Min(cap, rel*scale)
}

func imbalanceFamily(f FeatureVector) float64 {
	if f.Imbalance <= 0 {
		return 0
	}
	return math.Abs(f.Imbalance-0.5) * 200
}

func ttrFamily(f FeatureVector) float64 {
	h := f.TTRHours
	if h <= 0 {
		return 0
	}
	switch {
	case h < 24:
		return 20
	case h < 96:
		return 60
	case h <= 504:
		return 100
	case h <= 1080:
		return 50
	default:
		return 25
	}
}

func flowFamily(f FeatureVector) float64 {
	if f.BuyRatio5m <= 0 {
		return 0
	}
	return math.Abs(f.BuyRatio5m-0.5) * 200
}

func activityFamily(f FeatureVector) float64 {
	v := f.Volume24hr
	if v <= 0 {
		v = f.VolumeProxy
	}
	if v <= 0 {
		return 0
	}
	return math.Min(100, math.Log1p(v)/math.Log1p(1e6)*100)
}

// SeriesActivityProxy maps PIT series density + path movement into a volume-like
// scale for activityFamily (so offline weights remain meaningful).
// pointCount ≈ samples in window; pathAct = sum|Δmid| in window.
func SeriesActivityProxy(pointCount int, pathAct float64) float64 {
	// ~1k notional units per sample + path moves scaled to 1e5 (0.01 move → 1k).
	return float64(pointCount)*1000 + pathAct*100_000
}

func urgency(f FeatureVector) float64 {
	u := 0.0
	if f.TTRHours > 0 && f.TTRHours < 72 {
		u += 0.4
	} else if f.TTRHours <= 504 {
		u += 0.25
	}
	if f.AbsRet5m > 0.01 || f.AbsRet1m > 0.005 {
		u += 0.35
	}
	if math.Abs(f.DOI1h) > 0 && f.OI > 0 && math.Abs(f.DOI1h)/math.Max(f.OI, 1) > 0.05 {
		u += 0.25
	}
	if u > 1 {
		u = 1
	}
	return u
}

func strategyTags(f FeatureVector) []string {
	var tags []string
	if f.NegRisk {
		tags = append(tags, "neg_risk_leg")
	} else {
		tags = append(tags, "standalone")
	}
	if f.TTRHours > 0 && f.TTRHours < 6 {
		tags = append(tags, "near_resolution")
	}
	return tags
}

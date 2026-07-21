package edge

import "math"

// FeatureVector is the multi-family feature set used for M3 scoring.
// All fields are precomputed; this package does not fetch data.
type FeatureVector struct {
	ConditionID string `json:"condition_id,omitempty"`
	TokenID     string `json:"token_id,omitempty"`

	// Book / liquidity
	BestBid    float64 `json:"best_bid"`
	BestAsk    float64 `json:"best_ask"`
	Mid        float64 `json:"mid"`
	SpreadBps  float64 `json:"spread_bps"`
	Imbalance  float64 `json:"imbalance"` // bid/(bid+ask), 0.5 neutral
	BidDepth   float64 `json:"bid_depth"`
	AskDepth   float64 `json:"ask_depth"`
	BookAgeSec float64 `json:"book_age_sec"`

	// Volatility / path
	AbsRet1m  float64 `json:"abs_ret_1m"`
	AbsRet5m  float64 `json:"abs_ret_5m"`
	AbsRet1h  float64 `json:"abs_ret_1h"`
	OneDayAbs float64 `json:"one_day_abs"`

	// OI / positioning
	OI     float64 `json:"oi"`
	DOI1h  float64 `json:"doi_1h"`
	DOI24h float64 `json:"doi_24h"`

	// Time structure
	TTRHours float64 `json:"ttr_hours"`
	AgeDays  float64 `json:"age_days"`

	// Activity
	Volume24hr float64 `json:"volume_24hr"`

	// Trade flow (optional)
	BuyRatio5m float64 `json:"buy_ratio_5m"`

	// Neg-risk
	NegRisk            bool    `json:"neg_risk"`
	NegRiskGroupID     string  `json:"neg_risk_group_id,omitempty"`
	NegRiskResidualBps float64 `json:"neg_risk_residual_bps"`
	NegRiskIncomplete  bool    `json:"neg_risk_incomplete"`

	// Quality
	FeaturesAsOfUnix int64   `json:"features_asof_unix,omitempty"`
	FeatureAgeSec    float64 `json:"feature_age_sec"`
	MissingBook      bool    `json:"missing_book"`
	ThinBook         bool    `json:"thin_book"`
	StaleFeatures    bool    `json:"stale_features"`
}

// KeyFeatures returns a compact map for board embed (agent budget).
func (f FeatureVector) KeyFeatures() map[string]any {
	return map[string]any{
		"mid":                   round1(f.Mid),
		"spread_bps":            round1(f.SpreadBps),
		"imbalance":             round3(f.Imbalance),
		"abs_ret_5m":            round5(f.AbsRet5m),
		"one_day_abs":           round5(f.OneDayAbs),
		"doi_1h":                round2(f.DOI1h),
		"ttr_hours":             round2(f.TTRHours),
		"neg_risk_residual_bps": round1(f.NegRiskResidualBps),
		"volume_24hr":           round1(f.Volume24hr),
		"book_age_sec":          round1(f.BookAgeSec),
		"feature_age_sec":       round1(f.FeatureAgeSec),
	}
}

// RiskFlags derives discrete quality / risk labels.
func (f FeatureVector) RiskFlags(cfg Weights) []string {
	var flags []string
	if f.MissingBook {
		flags = append(flags, "missing_book")
	}
	if f.ThinBook {
		flags = append(flags, "thin_book")
	}
	if f.StaleFeatures {
		flags = append(flags, "stale_features")
	}
	if f.SpreadBps > cfg.WideSpreadBps {
		flags = append(flags, "wide_spread")
	}
	if f.Mid > 0 && (f.Mid < 0.02 || f.Mid > 0.98) {
		flags = append(flags, "extreme_price")
	}
	if f.NegRiskIncomplete {
		flags = append(flags, "neg_risk_incomplete_group")
	}
	if f.BookAgeSec > cfg.MaxBookAgeSec && cfg.MaxBookAgeSec > 0 {
		flags = append(flags, "stale_book")
	}
	return flags
}

// FillBookDerived sets Mid, SpreadBps, MissingBook, ThinBook from bid/ask/depth.
func (f *FeatureVector) FillBookDerived(minDepth float64) {
	if f.BestBid <= 0 || f.BestAsk <= 0 || f.BestAsk < f.BestBid {
		f.MissingBook = true
		return
	}
	f.MissingBook = false
	f.Mid = (f.BestBid + f.BestAsk) / 2
	if f.Mid > 0 {
		f.SpreadBps = 10_000 * (f.BestAsk - f.BestBid) / f.Mid
	}
	total := f.BidDepth + f.AskDepth
	if total > 0 && f.Imbalance == 0 {
		f.Imbalance = f.BidDepth / total
	}
	if minDepth <= 0 {
		minDepth = 10
	}
	if total < minDepth {
		f.ThinBook = true
	}
}

// NegRiskResidualBpsFromMids returns |sum(mids) - 1| in bps.
func NegRiskResidualBpsFromMids(mids []float64) float64 {
	if len(mids) < 2 {
		return 0
	}
	var sum float64
	for _, m := range mids {
		sum += m
	}
	return math.Abs(sum-1) * 10_000
}

func round1(v float64) float64 { return math.Round(v*10) / 10 }
func round2(v float64) float64 { return math.Round(v*100) / 100 }
func round3(v float64) float64 { return math.Round(v*1000) / 1000 }
func round5(v float64) float64 { return math.Round(v*1e5) / 1e5 }

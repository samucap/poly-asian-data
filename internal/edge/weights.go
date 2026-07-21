package edge

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Weights controls opportunity mix, cost defaults, and quality gates.
type Weights struct {
	Name string `yaml:"name" json:"name"`

	WVol       float64 `yaml:"w_vol" json:"w_vol"`
	WOI        float64 `yaml:"w_oi" json:"w_oi"`
	WImbalance float64 `yaml:"w_imbalance" json:"w_imbalance"`
	WTTR       float64 `yaml:"w_ttr" json:"w_ttr"`
	WNegRisk   float64 `yaml:"w_neg_risk" json:"w_neg_risk"`
	WFlow      float64 `yaml:"w_flow" json:"w_flow"`
	WActivity  float64 `yaml:"w_activity" json:"w_activity"`

	DefaultTakerFeeBps float64 `yaml:"default_taker_fee_bps" json:"default_taker_fee_bps"`
	ImpactCapBps       float64 `yaml:"impact_cap_bps" json:"impact_cap_bps"`
	ImpactCoeff        float64 `yaml:"impact_coeff" json:"impact_coeff"`
	ProbeSizeUSD       float64 `yaml:"probe_size_usd" json:"probe_size_usd"`
	ModelBufferBps     float64 `yaml:"model_buffer_bps" json:"model_buffer_bps"`

	MaxFeatureAgeSec float64 `yaml:"max_feature_age_sec" json:"max_feature_age_sec"`
	MaxBookAgeSec    float64 `yaml:"max_book_age_sec" json:"max_book_age_sec"`
	WideSpreadBps    float64 `yaml:"wide_spread_bps" json:"wide_spread_bps"`
	MinDepthShares   float64 `yaml:"min_depth_shares" json:"min_depth_shares"`
	DropMissingBook  bool    `yaml:"drop_missing_book" json:"drop_missing_book"`
	StalePenaltyBps  float64 `yaml:"stale_penalty_bps" json:"stale_penalty_bps"`

	VolScale  float64 `yaml:"vol_scale" json:"vol_scale"`
	VolCapBps float64 `yaml:"vol_cap_bps" json:"vol_cap_bps"`
	OIScale   float64 `yaml:"oi_scale" json:"oi_scale"`
	OICapBps  float64 `yaml:"oi_cap_bps" json:"oi_cap_bps"`
}

// DefaultWeights is the M3 baseline screen (cost-aware opportunity, not FV).
func DefaultWeights() Weights {
	return Weights{
		Name:               "default",
		WVol:               0.30,
		WOI:                0.15,
		WImbalance:         0.10,
		WTTR:               0.15,
		WNegRisk:           0.15,
		WFlow:              0.05,
		WActivity:          0.10,
		DefaultTakerFeeBps: 0,
		ImpactCapBps:       25,
		ImpactCoeff:        50,
		ProbeSizeUSD:       100,
		ModelBufferBps:     20,
		MaxFeatureAgeSec:   300,
		MaxBookAgeSec:      300,
		WideSpreadBps:      300,
		MinDepthShares:     10,
		DropMissingBook:    false,
		StalePenaltyBps:    50,
		VolScale:           1.0,
		VolCapBps:          500,
		OIScale:            100,
		OICapBps:           200,
	}
}

// LoadWeightsFile reads YAML strategy weights from path.
func LoadWeightsFile(path string) (Weights, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Weights{}, fmt.Errorf("edge: read weights: %w", err)
	}
	return ParseWeightsYAML(b)
}

// ParseWeightsYAML merges YAML over defaults.
func ParseWeightsYAML(b []byte) (Weights, error) {
	w := DefaultWeights()
	if len(b) == 0 {
		return w, nil
	}
	if err := yaml.Unmarshal(b, &w); err != nil {
		return Weights{}, fmt.Errorf("edge: parse weights: %w", err)
	}
	if w.Name == "" {
		w.Name = "default"
	}
	return w, nil
}

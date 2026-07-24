package eval

import (
	"fmt"
	"math"
	"time"
)

// EvalSurface is the promote/hold/kill artifact (eval_surface_v1 payload body).
// Compatible with asians AO shape: ok, metrics, gates_passed.
type EvalSurface struct {
	SchemaVersion   string      `json:"schema_version"`
	PipelineVersion string      `json:"pipeline_version,omitempty"`
	RunID           string      `json:"run_id"`
	GeneratedAt     string      `json:"generated_at"`
	InputHash       string      `json:"input_hash,omitempty"`
	CodeCommit      string      `json:"code_commit,omitempty"`
	Status          string      `json:"status"`
	Errors          []ErrorItem `json:"errors"`
	// Protocol
	OK          bool              `json:"ok"` // true only if all required gates pass
	GatesPassed []string          `json:"gates_passed"`
	GatesFailed []string          `json:"gates_failed"`
	GateDetails map[string]string `json:"gate_details,omitempty"`
	// Scope
	StrategyVersionID *int64 `json:"strategy_version_id,omitempty"`
	StrategyName      string `json:"strategy_name,omitempty"`
	// PolicyID is the offline candidate ranker (edge.SelectBoard / edge.Score).
	PolicyID string `json:"policy_id,omitempty"`
	// PolicyParity: scan_board_v1 = full board policy; score_proxy_v1 = incomplete.
	PolicyParity string `json:"policy_parity,omitempty"`
	// WeightsHash is sha256 hex of strategy weights YAML (M5 lineage).
	WeightsHash string `json:"weights_hash,omitempty"`
	// WeightsPath optional source path.
	WeightsPath string `json:"weights_path,omitempty"`
	// BookCoverage fraction of candidate features with books at T (diag).
	BookCoverage float64 `json:"book_coverage,omitempty"`
	// FVCoverage fraction of candidate labels/rows with FV path (diag).
	FVCoverage float64 `json:"fv_coverage,omitempty"`
	// UniverseNote documents non-PIT universe membership.
	UniverseNote string   `json:"universe_note,omitempty"`
	SelectionSet string   `json:"selection_set"` // board_at_t | stage1_at_t | ...
	Horizons     []string `json:"horizons"`
	// Label / fill contract
	LabelProtocol LabelProtocol `json:"label_protocol"`
	FillModel     FillModel     `json:"fill_model"`
	// Metrics
	Metrics EvalMetrics `json:"metrics"`
	// ActionModel: long_yes | sign_from_edge (how candidate labels were built).
	ActionModel string `json:"action_model,omitempty"`
	// Feature list used by the strategy under test (for forbidden-feature gate)
	FeatureNames []string `json:"feature_names,omitempty"`
	// BaselineNotes documents PIT proxies for volume/activity baselines.
	BaselineNotes string `json:"baseline_notes,omitempty"`
	// PromoteEligible: protocol ok + after_cost > 0 + policy_parity=scan_board_v1.
	// M5 must promote only when promote_eligible is true.
	// Forced false when DataQuality.BlockPromote (synthetic share too high).
	PromoteEligible bool `json:"promote_eligible"`
	// DataQuality documents venue vs synthetic fill (dev gap-fill). Optional.
	DataQuality *DataQuality `json:"data_quality,omitempty"`
}

// DataQuality is eval-time price/book source mix (development synthetic fill).
type DataQuality struct {
	PriceSourceMix  map[string]int     `json:"price_source_mix,omitempty"`
	BookSourceMix   map[string]int     `json:"book_source_mix,omitempty"`
	SynthPriceShare float64            `json:"synth_price_share"`
	SynthBookShare  float64            `json:"synth_book_share"`
	FillMode        string             `json:"fill_mode,omitempty"`
	SynthMaxGap     string             `json:"synth_max_gap,omitempty"`
	SynthHoldMax    string             `json:"synth_hold_max,omitempty"`
	Warning         string             `json:"warning,omitempty"`
	SignificantSynth bool              `json:"significant_synth"`
	BlockPromote    bool               `json:"block_promote"`
}

// ErrorItem mirrors artifacts.ErrorItem without importing cycles.
type ErrorItem struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Component string `json:"component,omitempty"`
}

// LabelProtocol documents how labels were built (PIT contract).
type LabelProtocol struct {
	PointInTime        bool     `json:"point_in_time"`
	AsOfField          string   `json:"as_of_field"` // e.g. features_asof / selected_at
	Horizons           []string `json:"horizons"`
	NoFutureInFeatures bool     `json:"no_future_in_features"`
	ResolvedHandling   string   `json:"resolved_handling,omitempty"` // e.g. drop_if_resolved_before_horizon
}

// FillModel documents cost assumptions (anti-vanity).
type FillModel struct {
	Name          string  `json:"name"`  // e.g. mid_fee_slip
	Entry         string  `json:"entry"` // mid | ask | vwap
	FeeBps        float64 `json:"fee_bps"`
	SlipBps       float64 `json:"slip_bps"`
	IncludeSpread bool    `json:"include_half_spread"`
	Notes         string  `json:"notes,omitempty"`
}

// EvalMetrics holds overall + stratified + baseline comparisons.
type EvalMetrics struct {
	N                int                       `json:"n"`
	Overall          HorizonMetrics            `json:"overall"`
	ByStratum        map[string]HorizonMetrics `json:"by_stratum,omitempty"`         // key: "category=sports" etc.
	ByHorizon        map[string]HorizonMetrics `json:"by_horizon,omitempty"`         // 5m / 1h / 1d
	Portfolio        *PortfolioMetrics         `json:"portfolio,omitempty"`          // equity-curve Sharpe / max DD
	Baselines        map[string]HorizonMetrics `json:"baselines,omitempty"`          // volume_top_n, ...
	DeltaVsBaselines map[string]float64        `json:"delta_vs_baselines,omitempty"` // primary horizon after_cost_return_bps
	PrimaryHorizon   string                    `json:"primary_horizon"`
}

// HorizonMetrics is one horizon's outcome stats (after costs when filled).
type HorizonMetrics struct {
	Horizon               string  `json:"horizon,omitempty"`
	N                     int     `json:"n"`
	HitRate               float64 `json:"hit_rate"`              // fraction of positive after-cost outcomes
	AfterCostReturnBps    float64 `json:"after_cost_return_bps"` // mean
	AfterCostReturnMedBps float64 `json:"after_cost_return_med_bps,omitempty"`
	MaxDrawdownBps        float64 `json:"max_drawdown_bps,omitempty"`
	// WinRate is an alias some consumers expect; must equal HitRate when set.
	WinRate float64 `json:"win_rate,omitempty"`
}

// GateConfig thresholds for Validate / EvaluateGates.
type GateConfig struct {
	MinSample             int
	RequireBeatsVolume    bool
	RequireBeatsActivity  bool
	MinDeltaVsBaselineBps float64 // e.g. 0 = must be strictly better mean after-cost
	PrimaryHorizon        string
}

// DefaultGateConfig is promote-eligible defaults (strict, not vanity).
func DefaultGateConfig() GateConfig {
	return GateConfig{
		MinSample:             DefaultMinSample,
		RequireBeatsVolume:    true,
		RequireBeatsActivity:  true,
		MinDeltaVsBaselineBps: 0,
		PrimaryHorizon:        "1h",
	}
}

// EvaluateGates fills OK / GatesPassed / GatesFailed from surface content + cfg.
// Does not mutate Metrics; only gate fields and OK.
func EvaluateGates(s *EvalSurface, cfg GateConfig) {
	if s == nil {
		return
	}
	if cfg.MinSample <= 0 {
		cfg = DefaultGateConfig()
	}
	if s.GateDetails == nil {
		s.GateDetails = map[string]string{}
	}
	var passed, failed []string

	pass := func(id, detail string) {
		passed = append(passed, id)
		if detail != "" {
			s.GateDetails[id] = detail
		}
	}
	fail := func(id, detail string) {
		failed = append(failed, id)
		if detail != "" {
			s.GateDetails[id] = detail
		}
	}

	// PIT
	if s.LabelProtocol.PointInTime && s.LabelProtocol.NoFutureInFeatures && s.LabelProtocol.AsOfField != "" {
		pass(GatePITLabels, "point_in_time + no_future_in_features + as_of_field")
		pass(GateNoLookahead, "label protocol asserts no future in features")
	} else {
		fail(GatePITLabels, "label_protocol incomplete")
		fail(GateNoLookahead, "cannot certify no lookahead")
	}

	// Forbidden features
	if bad := CheckForbiddenFeatures(s.FeatureNames); len(bad) > 0 {
		fail(GateNoForbiddenFeatures, fmt.Sprintf("forbidden: %v", bad))
	} else {
		pass(GateNoForbiddenFeatures, "feature_names clean or empty")
	}

	// Selection documented
	switch s.SelectionSet {
	case SelectionBoard, SelectionStage1, SelectionPool:
		pass(GateSelectionDocumented, s.SelectionSet)
	default:
		fail(GateSelectionDocumented, "selection_set missing or unknown")
	}

	// Fill model
	if s.FillModel.Name != "" && (s.FillModel.FeeBps > 0 || s.FillModel.SlipBps > 0 || s.FillModel.IncludeSpread) {
		pass(GateFillModelDocumented, s.FillModel.Name)
		pass(GateAfterCostReported, "fill model non-trivial")
	} else if s.FillModel.Name != "" {
		pass(GateFillModelDocumented, s.FillModel.Name+" (zero fee/slip — weak)")
		// still require after-cost field presence
		if !math.IsNaN(s.Metrics.Overall.AfterCostReturnBps) {
			pass(GateAfterCostReported, "after_cost_return_bps present")
		} else {
			fail(GateAfterCostReported, "missing after_cost_return_bps")
		}
	} else {
		fail(GateFillModelDocumented, "fill_model.name empty")
		fail(GateAfterCostReported, "no fill model")
	}

	// Sample size
	n := s.Metrics.N
	if n <= 0 {
		n = s.Metrics.Overall.N
	}
	if n >= cfg.MinSample {
		pass(GateMinSample, fmt.Sprintf("n=%d", n))
	} else {
		fail(GateMinSample, fmt.Sprintf("n=%d < %d", n, cfg.MinSample))
	}

	// Stratification
	if len(s.Metrics.ByStratum) >= 2 {
		pass(GateStratified, fmt.Sprintf("%d strata", len(s.Metrics.ByStratum)))
	} else {
		fail(GateStratified, "need by_stratum with ≥2 keys for promote-eligible eval")
	}

	// Baselines
	missingBase := []string{}
	for _, b := range RequiredBaselines {
		if s.Metrics.Baselines == nil {
			missingBase = append(missingBase, b)
			continue
		}
		if _, ok := s.Metrics.Baselines[b]; !ok {
			missingBase = append(missingBase, b)
		}
	}
	if len(missingBase) == 0 {
		pass(GateBaselinesPresent, "all required baselines")
	} else {
		fail(GateBaselinesPresent, fmt.Sprintf("missing %v", missingBase))
	}

	// Beat baselines on primary horizon after-cost mean
	ph := cfg.PrimaryHorizon
	if s.Metrics.PrimaryHorizon != "" {
		ph = s.Metrics.PrimaryHorizon
	}
	_ = ph
	cand := s.Metrics.Overall.AfterCostReturnBps
	if s.Metrics.Baselines != nil {
		if vol, ok := s.Metrics.Baselines["volume_top_n"]; ok {
			d := cand - vol.AfterCostReturnBps
			if s.Metrics.DeltaVsBaselines == nil {
				s.Metrics.DeltaVsBaselines = map[string]float64{}
			}
			s.Metrics.DeltaVsBaselines["volume_top_n"] = d
			if !cfg.RequireBeatsVolume || d > cfg.MinDeltaVsBaselineBps {
				pass(GateBeatsVolumeBaseline, fmt.Sprintf("delta_bps=%.2f", d))
			} else {
				fail(GateBeatsVolumeBaseline, fmt.Sprintf("delta_bps=%.2f not > %.2f", d, cfg.MinDeltaVsBaselineBps))
			}
		} else if cfg.RequireBeatsVolume {
			fail(GateBeatsVolumeBaseline, "volume_top_n baseline missing")
		}
		if act, ok := s.Metrics.Baselines["activity_stage1"]; ok {
			d := cand - act.AfterCostReturnBps
			if s.Metrics.DeltaVsBaselines == nil {
				s.Metrics.DeltaVsBaselines = map[string]float64{}
			}
			s.Metrics.DeltaVsBaselines["activity_stage1"] = d
			if !cfg.RequireBeatsActivity || d > cfg.MinDeltaVsBaselineBps {
				pass(GateBeatsActivityBaseline, fmt.Sprintf("delta_bps=%.2f", d))
			} else {
				fail(GateBeatsActivityBaseline, fmt.Sprintf("delta_bps=%.2f not > %.2f", d, cfg.MinDeltaVsBaselineBps))
			}
		} else if cfg.RequireBeatsActivity {
			fail(GateBeatsActivityBaseline, "activity_stage1 baseline missing")
		}
	}

	// Policy parity (architecture: same SelectBoard path as production)
	if s.PolicyParity == PolicyParityScanBoard {
		pass(GatePolicyParity, s.PolicyParity)
	} else if s.PolicyParity == "" {
		fail(GatePolicyParity, "policy_parity not set")
	} else {
		fail(GatePolicyParity, s.PolicyParity+" (need "+PolicyParityScanBoard+")")
	}

	// Synthetic fill: fail gate when share too high for promote; significant → not OK for real actions.
	if s.DataQuality != nil && s.DataQuality.BlockPromote {
		fail(GateSynthShareOK, s.DataQuality.Warning)
	} else if s.DataQuality != nil && (s.DataQuality.SynthPriceShare > 0 || s.DataQuality.SynthBookShare > 0) {
		pass(GateSynthShareOK, fmt.Sprintf("price_synth=%.3f book_synth=%.3f under promote cap",
			s.DataQuality.SynthPriceShare, s.DataQuality.SynthBookShare))
	}

	s.GatesPassed = passed
	s.GatesFailed = failed
	s.OK = len(failed) == 0
	// Significant synthetic share always blocks "success" for real-action consumers.
	if s.DataQuality != nil && s.DataQuality.SignificantSynth {
		s.OK = false
	}

	// Promote is separate from ok: protocol may pass while after-cost ≤ 0.
	// Hard rule: DataQuality.BlockPromote forces promote_eligible=false.
	s.PromoteEligible = s.OK &&
		s.Metrics.Overall.AfterCostReturnBps > 0 &&
		n >= cfg.MinSample &&
		s.PolicyParity == PolicyParityScanBoard &&
		(s.DataQuality == nil || !s.DataQuality.BlockPromote)
	if s.PromoteEligible {
		s.GatesPassed = append(s.GatesPassed, GatePromoteEligible)
		s.GateDetails[GatePromoteEligible] = fmt.Sprintf("after_cost_bps=%.2f", s.Metrics.Overall.AfterCostReturnBps)
	} else {
		s.GatesFailed = append(s.GatesFailed, GatePromoteEligible)
		detail := fmt.Sprintf(
			"ok=%v after_cost=%.2f parity=%s n=%d",
			s.OK, s.Metrics.Overall.AfterCostReturnBps, s.PolicyParity, n,
		)
		if s.DataQuality != nil && s.DataQuality.BlockPromote {
			detail += "; synth_block=true"
		}
		s.GateDetails[GatePromoteEligible] = detail
	}

	if s.OK {
		s.Status = "success"
	} else if s.Status == "" {
		s.Status = "failed"
	}
}

// ValidateStructural checks required fields without running numeric baseline beats.
func ValidateStructural(s *EvalSurface) error {
	if s == nil {
		return fmt.Errorf("eval: nil surface")
	}
	if s.SchemaVersion != SchemaVersion {
		return fmt.Errorf("eval: schema_version want %s got %s", SchemaVersion, s.SchemaVersion)
	}
	if s.RunID == "" {
		return fmt.Errorf("eval: run_id required")
	}
	if s.GeneratedAt == "" {
		return fmt.Errorf("eval: generated_at required")
	}
	if _, err := time.Parse(time.RFC3339, s.GeneratedAt); err != nil {
		// allow RFC3339Nano
		if _, err2 := time.Parse(time.RFC3339Nano, s.GeneratedAt); err2 != nil {
			return fmt.Errorf("eval: generated_at parse: %w", err)
		}
	}
	if s.SelectionSet == "" {
		return fmt.Errorf("eval: selection_set required")
	}
	if len(s.Horizons) == 0 {
		return fmt.Errorf("eval: horizons required")
	}
	if !s.LabelProtocol.PointInTime {
		return fmt.Errorf("eval: label_protocol.point_in_time must be true")
	}
	if !s.LabelProtocol.NoFutureInFeatures {
		return fmt.Errorf("eval: label_protocol.no_future_in_features must be true")
	}
	if s.FillModel.Name == "" {
		return fmt.Errorf("eval: fill_model.name required")
	}
	return nil
}

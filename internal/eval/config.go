package eval

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the YAML eval protocol (configs/eval/default.yaml).
type Config struct {
	SchemaVersion       string        `yaml:"schema_version"`
	PrimaryHorizon      string        `yaml:"primary_horizon"`
	Horizons            []string      `yaml:"horizons"`
	DefaultSelectionSet string        `yaml:"default_selection_set"`
	// ActionModel: long_yes | sign_from_edge (default sign_from_edge for board economic claim).
	ActionModel       string        `yaml:"action_model"`
	LabelProtocol     LabelYAML     `yaml:"label_protocol"`
	FillModel         FillModelYAML `yaml:"fill_model"`
	Gates             GatesYAML     `yaml:"gates"`
	RequiredBaselines []string      `yaml:"required_baselines"`
	RequiredStrata    []string      `yaml:"required_strata"`
	ForbiddenFeatures []string      `yaml:"forbidden_features"`
}

// LabelYAML is the YAML form of LabelProtocol.
type LabelYAML struct {
	PointInTime        bool   `yaml:"point_in_time"`
	AsOfField          string `yaml:"as_of_field"`
	NoFutureInFeatures bool   `yaml:"no_future_in_features"`
	ResolvedHandling   string `yaml:"resolved_handling"`
}

// FillModelYAML is the YAML form of FillModel.
type FillModelYAML struct {
	Name          string  `yaml:"name"`
	Entry         string  `yaml:"entry"`
	FeeBps        float64 `yaml:"fee_bps"`
	SlipBps       float64 `yaml:"slip_bps"`
	IncludeSpread bool    `yaml:"include_half_spread"`
	Notes         string  `yaml:"notes"`
}

// GatesYAML maps to GateConfig.
type GatesYAML struct {
	MinSample             int     `yaml:"min_sample"`
	RequireBeatsVolume    bool    `yaml:"require_beats_volume_baseline"`
	RequireBeatsActivity  bool    `yaml:"require_beats_activity_baseline"`
	MinDeltaVsBaselineBps float64 `yaml:"min_delta_vs_baseline_bps"`
}

// DefaultConfig returns promote-eligible defaults without reading disk.
func DefaultConfig() Config {
	return Config{
		SchemaVersion:       SchemaVersion,
		PrimaryHorizon:      "1h",
		Horizons:            append([]string{}, DefaultHorizons...),
		DefaultSelectionSet: SelectionBoard,
		ActionModel:         ActionSignFromEdge,
		LabelProtocol: LabelYAML{
			PointInTime:        true,
			AsOfField:          "features_asof",
			NoFutureInFeatures: true,
			ResolvedHandling:   "drop_if_resolved_before_horizon",
		},
		FillModel: FillModelYAML{
			Name:          "mid_fee_slip",
			Entry:         "mid",
			FeeBps:        10,
			SlipBps:       5,
			IncludeSpread: true,
			Notes:         "Simple honest fill; VWAP/partials later",
		},
		Gates: GatesYAML{
			MinSample:             DefaultMinSample,
			RequireBeatsVolume:    true,
			RequireBeatsActivity:  true,
			MinDeltaVsBaselineBps: 0,
		},
		RequiredBaselines: append([]string{}, RequiredBaselines...),
		RequiredStrata:    append([]string{}, RequiredStrata...),
		ForbiddenFeatures: append([]string{}, ForbiddenFeatureNames...),
	}
}

// LoadConfig reads YAML from path. Empty path → DefaultConfig.
func LoadConfig(path string) (Config, error) {
	if path == "" {
		return DefaultConfig(), nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("eval config: read %s: %w", path, err)
	}
	cfg := DefaultConfig()
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return Config{}, fmt.Errorf("eval config: parse %s: %w", path, err)
	}
	if cfg.PrimaryHorizon == "" {
		cfg.PrimaryHorizon = "1h"
	}
	if len(cfg.Horizons) == 0 {
		cfg.Horizons = append([]string{}, DefaultHorizons...)
	}
	if cfg.DefaultSelectionSet == "" {
		cfg.DefaultSelectionSet = SelectionBoard
	}
	if cfg.ActionModel == "" {
		cfg.ActionModel = ActionSignFromEdge
	}
	if cfg.Gates.MinSample <= 0 {
		cfg.Gates.MinSample = DefaultMinSample
	}
	return cfg, nil
}

// GateConfig converts YAML gates + primary horizon.
func (c Config) GateConfig() GateConfig {
	return GateConfig{
		MinSample:             c.Gates.MinSample,
		RequireBeatsVolume:    c.Gates.RequireBeatsVolume,
		RequireBeatsActivity:  c.Gates.RequireBeatsActivity,
		MinDeltaVsBaselineBps: c.Gates.MinDeltaVsBaselineBps,
		PrimaryHorizon:        c.PrimaryHorizon,
	}
}

// FillModel converts YAML fill model.
func (c Config) ToFillModel() FillModel {
	return FillModel{
		Name:          c.FillModel.Name,
		Entry:         c.FillModel.Entry,
		FeeBps:        c.FillModel.FeeBps,
		SlipBps:       c.FillModel.SlipBps,
		IncludeSpread: c.FillModel.IncludeSpread,
		Notes:         c.FillModel.Notes,
	}
}

// LabelProtocol converts YAML label protocol.
func (c Config) ToLabelProtocol() LabelProtocol {
	return LabelProtocol{
		PointInTime:        c.LabelProtocol.PointInTime,
		AsOfField:          c.LabelProtocol.AsOfField,
		Horizons:           append([]string{}, c.Horizons...),
		NoFutureInFeatures: c.LabelProtocol.NoFutureInFeatures,
		ResolvedHandling:   c.LabelProtocol.ResolvedHandling,
	}
}

// ParseHorizon duration helper (5m, 1h, 1d).
func ParseHorizon(h string) (time.Duration, error) {
	switch h {
	case "5m":
		return 5 * time.Minute, nil
	case "1h":
		return time.Hour, nil
	case "1d":
		return 24 * time.Hour, nil
	default:
		d, err := time.ParseDuration(h)
		if err != nil {
			return 0, fmt.Errorf("eval: unknown horizon %q: %w", h, err)
		}
		return d, nil
	}
}

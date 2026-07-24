// Package risk implements a portfolio-aware paper risk manager (M8).
// It decides whether to accept a signal and at what size, preferring
// higher-opportunity intents when budget is scarce. No exchange orders (OMS).
// Knobs are YAML-configurable for an external auto-optimizer.
package risk

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Sizing modes.
const (
	SizingFracKelly  = "frac_kelly"
	SizingVolTarget  = "vol_target"
	SizingSignalSize = "signal_size"
)

// Config is portfolio budget + risk policy for paper trading decisions (AO-mutable).
type Config struct {
	StartingEquityUSD   float64 `yaml:"starting_equity_usd" json:"starting_equity_usd"`
	SizingMode          string  `yaml:"sizing_mode" json:"sizing_mode"`
	KellyFraction       float64 `yaml:"kelly_fraction" json:"kelly_fraction"`
	KellyP              float64 `yaml:"kelly_p" json:"kelly_p"`
	KellyB              float64 `yaml:"kelly_b" json:"kelly_b"`
	VolTargetAnn        float64 `yaml:"vol_target_ann" json:"vol_target_ann"`
	VolLookbackPeriods  int     `yaml:"vol_lookback_periods" json:"vol_lookback_periods"`
	MaxPositionUSD      float64 `yaml:"max_position_usd" json:"max_position_usd"`
	MaxGrossUSD         float64 `yaml:"max_gross_usd" json:"max_gross_usd"`
	MaxPositions        int     `yaml:"max_positions" json:"max_positions"`
	CapacityFrac        float64 `yaml:"capacity_frac" json:"capacity_frac"`
	MaxDailyDrawdownBps float64 `yaml:"max_daily_drawdown_bps" json:"max_daily_drawdown_bps"`
	MaxDrawdownBps      float64 `yaml:"max_drawdown_bps" json:"max_drawdown_bps"`
	DailyHaltPolicy     string  `yaml:"daily_halt_policy" json:"daily_halt_policy"`
	MinSizeUSD          float64 `yaml:"min_size_usd" json:"min_size_usd"`
	// Opportunity ranking weights (budget competition).
	OpportunityEdgeWeight       float64 `yaml:"opportunity_edge_weight" json:"opportunity_edge_weight"`
	OpportunityConvictionWeight float64 `yaml:"opportunity_conviction_weight" json:"opportunity_conviction_weight"`
	// BatchWindowMs: if >0, sim groups signals within window and ranks by opportunity.
	BatchWindowMs int `yaml:"batch_window_ms" json:"batch_window_ms"`
}

// DefaultConfig returns conservative paper defaults.
func DefaultConfig() Config {
	return Config{
		StartingEquityUSD:           10_000,
		SizingMode:                  SizingFracKelly,
		KellyFraction:               0.25,
		KellyP:                      0.55,
		KellyB:                      1.0,
		VolTargetAnn:                0.15,
		VolLookbackPeriods:          24,
		MaxPositionUSD:              500,
		MaxGrossUSD:                 5_000,
		MaxPositions:                20,
		CapacityFrac:                0.1,
		MaxDailyDrawdownBps:         300,
		MaxDrawdownBps:              1000,
		DailyHaltPolicy:             "reject_new",
		MinSizeUSD:                  1,
		OpportunityEdgeWeight:       1.0,
		OpportunityConvictionWeight: 1.0,
		// >0 ranks near-simultaneous signals by opportunity when budget is scarce.
		// 0 = strict time order (no re-rank).
		BatchWindowMs: 500,
	}
}

// LoadConfig reads YAML risk config; empty path → defaults.
func LoadConfig(path string) (Config, error) {
	cfg := DefaultConfig()
	if path == "" {
		return cfg, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("risk config: %w", err)
	}
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return cfg, fmt.Errorf("risk config yaml: %w", err)
	}
	cfg.Normalize()
	return cfg, nil
}

// Normalize fills zeros with defaults and clamps unsafe values.
func (c *Config) Normalize() {
	d := DefaultConfig()
	if c.StartingEquityUSD <= 0 {
		c.StartingEquityUSD = d.StartingEquityUSD
	}
	if c.SizingMode == "" {
		c.SizingMode = d.SizingMode
	}
	if c.KellyFraction <= 0 || c.KellyFraction > 1 {
		c.KellyFraction = d.KellyFraction
	}
	if c.KellyP <= 0 || c.KellyP >= 1 {
		c.KellyP = d.KellyP
	}
	if c.KellyB <= 0 {
		c.KellyB = d.KellyB
	}
	if c.VolTargetAnn <= 0 {
		c.VolTargetAnn = d.VolTargetAnn
	}
	if c.VolLookbackPeriods <= 0 {
		c.VolLookbackPeriods = d.VolLookbackPeriods
	}
	if c.MaxPositionUSD <= 0 {
		c.MaxPositionUSD = d.MaxPositionUSD
	}
	if c.MaxGrossUSD <= 0 {
		c.MaxGrossUSD = d.MaxGrossUSD
	}
	if c.MaxPositions <= 0 {
		c.MaxPositions = d.MaxPositions
	}
	if c.CapacityFrac <= 0 {
		c.CapacityFrac = d.CapacityFrac
	}
	if c.MaxDailyDrawdownBps <= 0 {
		c.MaxDailyDrawdownBps = d.MaxDailyDrawdownBps
	}
	if c.MaxDrawdownBps <= 0 {
		c.MaxDrawdownBps = d.MaxDrawdownBps
	}
	if c.MinSizeUSD <= 0 {
		c.MinSizeUSD = d.MinSizeUSD
	}
	if c.DailyHaltPolicy == "" {
		c.DailyHaltPolicy = d.DailyHaltPolicy
	}
	if c.OpportunityEdgeWeight < 0 {
		c.OpportunityEdgeWeight = d.OpportunityEdgeWeight
	}
	if c.OpportunityConvictionWeight < 0 {
		c.OpportunityConvictionWeight = d.OpportunityConvictionWeight
	}
}

// Valid reports whether config is usable.
func (c Config) Valid() bool {
	return c.StartingEquityUSD > 0 &&
		c.KellyFraction > 0 && c.KellyFraction <= 1 &&
		c.MaxPositionUSD > 0 && c.MaxGrossUSD > 0 &&
		c.MaxPositions > 0
}

// ConfigHash is sha256 of canonical YAML for AO attribution.
func ConfigHash(cfg Config) string {
	b, err := yaml.Marshal(cfg)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

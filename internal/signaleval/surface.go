package signaleval

import (
	"time"

	"github.com/samucap/poly-asian-data/internal/risk"
)

// Surface is the signal_eval_surface artifact body (M8).
type Surface struct {
	SchemaVersion   string         `json:"schema_version"`
	PipelineVersion string         `json:"pipeline_version,omitempty"`
	RunID           string         `json:"run_id"`
	GeneratedAt     string         `json:"generated_at"`
	Status          string         `json:"status"`
	OK              bool           `json:"ok"`
	// EconomicPass is true when total_pnl_usd > 0. Not M4 promote_eligible; AO scorecard only.
	EconomicPass    bool           `json:"economic_pass"`
	Purpose         string         `json:"purpose"`
	StrategyName    string         `json:"strategy_name,omitempty"`
	Window          WindowMeta     `json:"window"`
	Risk            risk.Config    `json:"risk"`
	ConfigHash      string         `json:"config_hash"`
	Metrics         Metrics        `json:"metrics"`
	RiskEvents      []risk.Event   `json:"risk_events,omitempty"`
	FillModel       FillModelDoc   `json:"fill_model"`
	GatesPassed     []string       `json:"gates_passed"`
	GatesFailed     []string       `json:"gates_failed"`
	Notes           string         `json:"notes"`
}

// WindowMeta documents the eval window.
type WindowMeta struct {
	From     string `json:"from,omitempty"`
	To       string `json:"to,omitempty"`
	NSignals int    `json:"n_signals"`
}

// FillModelDoc documents paper fill assumptions.
type FillModelDoc struct {
	Entry string `json:"entry"`
	Cost  string `json:"cost"`
	Exit  string `json:"exit"`
	Notes string `json:"notes,omitempty"`
}

// BuildSurface constructs the AO scorecard from a sim result.
func BuildSurface(runID, strategy string, res Result, from, to time.Time, minSample int) Surface {
	if minSample <= 0 {
		minSample = 3
	}
	s := Surface{
		SchemaVersion:   "signal_eval_surface_v1",
		PipelineVersion: "m8.0",
		RunID:           runID,
		GeneratedAt:     time.Now().UTC().Format(time.RFC3339Nano),
		Status:          "success",
		Purpose:         "Score paper signals under portfolio risk decisions for external AO; not M4 promote_eligible; no OMS. ok=protocol/sample; economic_pass=total_pnl_usd>0",
		StrategyName:    strategy,
		Window: WindowMeta{
			NSignals: res.Metrics.NSignals,
		},
		Risk:       res.RiskCfg,
		ConfigHash: res.ConfigHash,
		Metrics:    res.Metrics,
		RiskEvents: res.RiskEvents,
		FillModel: FillModelDoc{
			Entry: "signal_mid_or_price_asof",
			Cost:  "signal_cost_bps",
			Exit:  "mid_at_horizon",
			Notes: "paper only",
		},
		Notes: "Hard risk constraints first; opportunity ranking allocates scarce budget. Strategy research is external. Do not treat ok as economic success — use economic_pass and metrics.",
	}
	s.EconomicPass = res.Metrics.TotalPnLUSD > 0
	if !from.IsZero() {
		s.Window.From = from.UTC().Format(time.RFC3339)
	}
	if !to.IsZero() {
		s.Window.To = to.UTC().Format(time.RFC3339)
	}

	var passed, failed []string
	if res.RiskCfg.Valid() {
		passed = append(passed, "risk_config_valid")
	} else {
		failed = append(failed, "risk_config_valid")
	}
	passed = append(passed, "protocol")
	if res.Metrics.NSignals >= minSample {
		passed = append(passed, "min_sample")
	} else {
		failed = append(failed, "min_sample")
	}
	s.GatesPassed = passed
	s.GatesFailed = failed
	s.OK = len(failed) == 0
	if !s.OK {
		s.Status = "partial"
	}
	return s
}

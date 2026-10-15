package strategyreg

import (
	"encoding/json"
	"fmt"
	"os"
)

// SurfaceSummary is the minimal eval_surface fields needed for promote gate.
type SurfaceSummary struct {
	OK              bool   `json:"ok"`
	PromoteEligible bool   `json:"promote_eligible"`
	WeightsHash     string `json:"weights_hash"`
	RunID           string `json:"run_id"`
	StrategyName    string `json:"strategy_name"`
	Status          string `json:"status"`
}

// LoadSurfaceSummary reads an eval_surface JSON artifact from disk.
func LoadSurfaceSummary(path string) (SurfaceSummary, error) {
	if path == "" {
		return SurfaceSummary{}, fmt.Errorf("%w: surface path required", ErrGateRejected)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return SurfaceSummary{}, fmt.Errorf("strategyreg: read surface: %w", err)
	}
	var s SurfaceSummary
	if err := json.Unmarshal(b, &s); err != nil {
		return SurfaceSummary{}, fmt.Errorf("strategyreg: parse surface: %w", err)
	}
	return s, nil
}

// CheckPromoteGate enforces fail-closed promote rules.
//
//	ALLOW IFF promote_eligible && ok && non-empty matching weights_hash
//	AND (surface.strategy_name empty OR equals versionStrategy)
func CheckPromoteGate(surface SurfaceSummary, versionWeightsHash, versionStrategy string) error {
	if versionWeightsHash == "" {
		return fmt.Errorf("%w: version weights_hash empty", ErrGateRejected)
	}
	if surface.WeightsHash == "" {
		return fmt.Errorf("%w: surface weights_hash empty", ErrGateRejected)
	}
	if surface.WeightsHash != versionWeightsHash {
		return fmt.Errorf("%w: weights_hash mismatch surface=%s version=%s",
			ErrGateRejected, surface.WeightsHash, versionWeightsHash)
	}
	if surface.StrategyName != "" && versionStrategy != "" && surface.StrategyName != versionStrategy {
		return fmt.Errorf("%w: strategy_name mismatch surface=%s version=%s",
			ErrGateRejected, surface.StrategyName, versionStrategy)
	}
	if !surface.OK {
		return fmt.Errorf("%w: surface ok=false (protocol not admissible)", ErrGateRejected)
	}
	if !surface.PromoteEligible {
		return fmt.Errorf("%w: promote_eligible=false (after-cost/parity/sample)", ErrGateRejected)
	}
	return nil
}

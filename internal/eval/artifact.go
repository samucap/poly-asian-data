package eval

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/samucap/poly-asian-data/internal/artifacts"
)

// WriteSurface writes eval_surface JSON under artifacts/eval_surface/.
func WriteSurface(s *EvalSurface, root string) (artifacts.WriteResult, error) {
	if s == nil {
		return artifacts.WriteResult{}, fmt.Errorf("eval: nil surface")
	}
	if s.RunID == "" {
		return artifacts.WriteResult{}, fmt.Errorf("eval: run_id required")
	}
	if s.PipelineVersion == "" {
		s.PipelineVersion = artifacts.PipelineVersion
	}
	if s.CodeCommit == "" {
		s.CodeCommit = artifacts.CodeCommit
		if s.CodeCommit == "" {
			s.CodeCommit = "unknown"
		}
	}
	return artifacts.WriteJSON(s.RunID, ArtifactPipeline, s, artifacts.WriteOptions{
		Root:        root,
		Pipeline:    ArtifactPipeline,
		WriteLatest: true,
	})
}

// FinalizeSurface runs structural validation + gates and sets status/ok/promote_eligible.
func FinalizeSurface(s *EvalSurface, cfg Config) error {
	if err := ValidateStructural(s); err != nil {
		return err
	}
	if s.PolicyParity == "" {
		s.PolicyParity = PolicyParityScanBoard // default when using SelectBoard path
	}
	EvaluateGates(s, cfg.GateConfig())
	if s.OK {
		s.Status = artifacts.StatusSuccess
	} else if s.Metrics.N > 0 {
		s.Status = artifacts.StatusPartial
	} else {
		s.Status = artifacts.StatusFailed
	}
	return nil
}

// WeightsHashFile returns sha256 hex of file bytes (empty path → "").
func WeightsHashFile(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// WeightsHashBytes hashes raw YAML/JSON weights bytes.
func WeightsHashBytes(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

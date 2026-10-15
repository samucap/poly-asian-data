package strategyreg

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/edge"
	"github.com/samucap/poly-asian-data/internal/eval"
)

// RegisterOpts freezes a weights YAML into strategy_versions as draft.
type RegisterOpts struct {
	WeightsPath string
	Strategy    string // override family name; default from YAML name
	GitSHA      string
	Note        string
	Actor       string
	// Status defaults to draft; may be candidate.
	Status string
}

// Register reads YAML bytes, hashes them (same as edge-eval), stores params JSONB.
// weights_hash is sha256 of the YAML file bytes (not re-serialized JSON).
// Caller must EnsureStrategyTables first.
func Register(ctx context.Context, conn db.DBInterface, opts RegisterOpts) (Version, error) {
	if opts.WeightsPath == "" {
		return Version{}, fmt.Errorf("strategyreg: --weights path required")
	}
	b, err := os.ReadFile(opts.WeightsPath)
	if err != nil {
		return Version{}, fmt.Errorf("strategyreg: read weights: %w", err)
	}
	w, err := edge.ParseWeightsYAML(b)
	if err != nil {
		return Version{}, err
	}
	strategy := opts.Strategy
	if strategy == "" {
		strategy = w.Name
	}
	if strategy == "" {
		strategy = "default"
	}
	// Keep params name aligned with strategy family.
	w.Name = strategy

	hash := eval.WeightsHashBytes(b)
	if hash == "" {
		return Version{}, fmt.Errorf("strategyreg: empty weights file")
	}
	params, err := json.Marshal(w)
	if err != nil {
		return Version{}, fmt.Errorf("strategyreg: marshal params: %w", err)
	}
	status := opts.Status
	if status == "" {
		status = StatusDraft
	}
	switch status {
	case StatusDraft, StatusCandidate:
	default:
		return Version{}, fmt.Errorf("strategyreg: register status must be draft or candidate, got %q", status)
	}
	actor := opts.Actor
	if actor == "" {
		actor = "operator"
	}

	v, err := db.InsertStrategyVersion(ctx, conn, db.StrategyVersion{
		Strategy:    strategy,
		Status:      status,
		Params:      params,
		WeightsHash: hash,
		SourcePath:  opts.WeightsPath,
		GitSHA:      opts.GitSHA,
		Note:        opts.Note,
		CreatedBy:   actor,
	})
	if err != nil {
		return Version{}, err
	}

	if _, err := db.InsertStrategyPromotion(ctx, conn, db.StrategyPromotion{
		Strategy:    strategy,
		Action:      "register",
		ToVersionID: &v.ID,
		WeightsHash: hash,
		Reason:      opts.Note,
		Actor:       actor,
	}); err != nil {
		return v, fmt.Errorf("strategyreg: version id=%d inserted but audit failed: %w", v.ID, err)
	}
	return v, nil
}

// MarkCandidate sets status=candidate (shadow; not live).
func MarkCandidate(ctx context.Context, conn db.DBInterface, id int64, actor, reason string) error {
	v, err := db.GetStrategyVersion(ctx, conn, id)
	if err != nil {
		return err
	}
	if v.Status == StatusActive {
		return fmt.Errorf("strategyreg: cannot mark active version as candidate; demote first")
	}
	if err := db.SetStrategyStatus(ctx, conn, id, StatusCandidate); err != nil {
		return err
	}
	if actor == "" {
		actor = "operator"
	}
	if _, err := db.InsertStrategyPromotion(ctx, conn, db.StrategyPromotion{
		Strategy:    v.Strategy,
		Action:      "candidate",
		ToVersionID: &id,
		WeightsHash: v.WeightsHash,
		Reason:      reason,
		Actor:       actor,
	}); err != nil {
		return fmt.Errorf("strategyreg: candidate status set but audit failed: %w", err)
	}
	return nil
}

// ParamsToWeights unmarshals JSONB params into edge.Weights.
func ParamsToWeights(params json.RawMessage) (edge.Weights, error) {
	if len(params) == 0 {
		return edge.Weights{}, fmt.Errorf("strategyreg: empty params")
	}
	// Start from defaults so missing YAML keys stay filled if params are partial.
	w := edge.DefaultWeights()
	if err := json.Unmarshal(params, &w); err != nil {
		return edge.Weights{}, fmt.Errorf("strategyreg: unmarshal params: %w", err)
	}
	return w, nil
}

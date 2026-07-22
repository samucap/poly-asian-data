package strategyreg

import (
	"context"
	"fmt"

	"github.com/samucap/poly-asian-data/internal/db"
)

// PromoteOpts activates a version only when the bound surface passes the gate.
type PromoteOpts struct {
	VersionID   int64
	SurfacePath string
	Actor       string
	Reason      string
}

// Promote loads the version + surface, enforces promote_eligible ∧ weights_hash match, then activates.
func Promote(ctx context.Context, conn db.DBInterface, opts PromoteOpts) (Active, error) {
	if opts.VersionID <= 0 {
		return Active{}, fmt.Errorf("strategyreg: --id required")
	}
	if opts.SurfacePath == "" {
		return Active{}, fmt.Errorf("%w: --surface path required", ErrGateRejected)
	}

	v, err := db.GetStrategyVersion(ctx, conn, opts.VersionID)
	if err != nil {
		return Active{}, err
	}

	surface, err := LoadSurfaceSummary(opts.SurfacePath)
	if err != nil {
		return Active{}, err
	}
	if err := CheckPromoteGate(surface, v.WeightsHash, v.Strategy); err != nil {
		return Active{}, err
	}

	actor := opts.Actor
	if actor == "" {
		actor = "operator"
	}
	return db.ActivateStrategyVersion(ctx, conn, db.ActivateOpts{
		VersionID:       opts.VersionID,
		Actor:           actor,
		EvalRunID:       surface.RunID,
		WeightsHash:     v.WeightsHash,
		PromoteEligible: true,
		Reason:          opts.Reason,
		Action:          "promote",
	})
}

// Rollback restores the previous active version (no promote_eligible required).
func Rollback(ctx context.Context, conn db.DBInterface, strategy, actor, reason string) (Active, error) {
	if strategy == "" {
		strategy = "default"
	}
	return db.RollbackStrategyActive(ctx, conn, strategy, actor, reason)
}

// Retire marks a non-active version retired.
func Retire(ctx context.Context, conn db.DBInterface, id int64, actor, reason string) error {
	v, err := db.GetStrategyVersion(ctx, conn, id)
	if err != nil {
		return err
	}
	if v.Status == StatusActive {
		return fmt.Errorf("strategyreg: cannot retire active version; promote another or rollback first")
	}
	if err := db.SetStrategyStatus(ctx, conn, id, StatusRetired); err != nil {
		return err
	}
	if actor == "" {
		actor = "operator"
	}
	if _, err := db.InsertStrategyPromotion(ctx, conn, db.StrategyPromotion{
		Strategy:    v.Strategy,
		Action:      "retire",
		ToVersionID: &id,
		WeightsHash: v.WeightsHash,
		Reason:      reason,
		Actor:       actor,
	}); err != nil {
		return fmt.Errorf("strategyreg: retired but audit failed: %w", err)
	}
	return nil
}

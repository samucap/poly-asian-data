// Package strategyreg implements M5 hybrid strategy version control:
// register board-policy weights, promote only when eval_surface.promote_eligible,
// load active params for live edge-scan.
package strategyreg

import (
	"errors"

	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/edge"
)

// Re-export status constants for callers.
const (
	StatusDraft     = db.StrategyStatusDraft
	StatusCandidate = db.StrategyStatusCandidate
	StatusActive    = db.StrategyStatusActive
	StatusRetired   = db.StrategyStatusRetired
)

// Sentinel errors.
var (
	// ErrGateRejected means promote was refused (not eligible / hash mismatch / ok false).
	ErrGateRejected = errors.New("strategyreg: promote gate rejected")
	// ErrNotFound wraps missing version.
	ErrNotFound = db.ErrStrategyNotFound
	// ErrNoActive wraps missing active pointer.
	ErrNoActive = db.ErrNoActiveStrategy
	// ErrNoRollbackTarget wraps one-shot rollback exhausted.
	ErrNoRollbackTarget = db.ErrNoRollbackTarget
)

// Version is a registered weight freeze.
type Version = db.StrategyVersion

// Active is the live pointer.
type Active = db.StrategyActive

// LoadedActive is active version params ready for edge-scan / edge-eval.
type LoadedActive struct {
	Version Version
	Active  Active
	Weights edge.Weights
}

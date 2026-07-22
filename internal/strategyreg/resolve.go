package strategyreg

import (
	"context"
	"os"

	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/edge"
	"github.com/samucap/poly-asian-data/internal/eval"
)

// Resolve source labels for live weight loading.
const (
	SourceActive  = "active"
	SourceFile    = "file"
	SourceDefault = "default"
)

// ResolveOpts controls live weight resolution for edge-scan (and future M6).
type ResolveOpts struct {
	Strategy string
	// ExplicitPath: when non-empty, force file load (dev override; no version stamp).
	ExplicitPath string
	// FallbackPath used when no active version (default configs/strategies/default.yaml).
	FallbackPath string
}

// Resolved is the board-policy weights source for one cycle.
type Resolved struct {
	Weights   edge.Weights
	VersionID *int64
	Hash      string
	Source    string // active | file | default
	Path      string
	// OverrideDiffers is true when file hash ≠ active version hash (override path).
	OverrideDiffers bool
	ActiveVersionID int64
	ActiveHash      string
	// LoadNote is a non-fatal message for operators (DB miss, file miss, etc.).
	LoadNote string
}

// ResolveLive picks weights for production edge-scan:
//  1. ExplicitPath → file (version_id nil; may set OverrideDiffers)
//  2. strategy_active → DB params + version id
//  3. FallbackPath file → else DefaultWeights
//
// Caller must EnsureStrategyTables once at process start when using DB.
// DB/query failures fall through to file (edge-scan stays up); LoadNote explains.
func ResolveLive(ctx context.Context, conn db.DBInterface, opts ResolveOpts) Resolved {
	strategy := opts.Strategy
	if strategy == "" {
		strategy = "default"
	}
	fallback := opts.FallbackPath
	if fallback == "" {
		fallback = "configs/strategies/default.yaml"
	}

	if opts.ExplicitPath != "" {
		return resolveFile(ctx, conn, strategy, opts.ExplicitPath)
	}

	if conn != nil {
		la, ok, err := TryLoadActive(ctx, conn, strategy)
		if err != nil {
			r := resolveFile(ctx, conn, strategy, fallback)
			r.LoadNote = "strategy_active load failed; using file: " + err.Error()
			return r
		}
		if ok {
			id := la.Version.ID
			return Resolved{
				Weights:   la.Weights,
				VersionID: &id,
				Hash:      la.Version.WeightsHash,
				Source:    SourceActive,
				Path:      la.Version.SourcePath,
			}
		}
	}

	return resolveFile(ctx, conn, strategy, fallback)
}

func resolveFile(ctx context.Context, conn db.DBInterface, strategy, path string) Resolved {
	r := Resolved{Path: path, Source: SourceFile}
	w, err := edge.LoadWeightsFile(path)
	if err != nil {
		r.Weights = edge.DefaultWeights()
		r.Source = SourceDefault
		if os.IsNotExist(err) {
			r.LoadNote = "weights file missing; using DefaultWeights"
		} else {
			r.LoadNote = "weights file error; using DefaultWeights: " + err.Error()
		}
	} else {
		r.Weights = w
		if h, herr := eval.WeightsHashFile(path); herr == nil {
			r.Hash = h
		}
	}
	if conn != nil {
		if la, ok, _ := TryLoadActive(ctx, conn, strategy); ok {
			r.ActiveVersionID = la.Version.ID
			r.ActiveHash = la.Version.WeightsHash
			if r.Hash != "" && la.Version.WeightsHash != "" && r.Hash != la.Version.WeightsHash {
				r.OverrideDiffers = true
			}
		}
	}
	return r
}

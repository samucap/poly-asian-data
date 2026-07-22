package strategyreg

import (
	"context"
	"errors"

	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/edge"
)

// LoadActive returns active weights for a strategy family name.
func LoadActive(ctx context.Context, conn db.DBInterface, strategy string) (LoadedActive, error) {
	if strategy == "" {
		strategy = "default"
	}
	v, a, err := db.LoadActiveStrategyVersion(ctx, conn, strategy)
	if err != nil {
		return LoadedActive{}, err
	}
	w, err := ParamsToWeights(v.Params)
	if err != nil {
		return LoadedActive{}, err
	}
	if w.Name == "" {
		w.Name = strategy
	}
	return LoadedActive{Version: v, Active: a, Weights: w}, nil
}

// LoadVersion loads params for a specific version id (edge-eval --version-id).
func LoadVersion(ctx context.Context, conn db.DBInterface, id int64) (edge.Weights, Version, error) {
	v, err := db.GetStrategyVersion(ctx, conn, id)
	if err != nil {
		return edge.Weights{}, Version{}, err
	}
	w, err := ParamsToWeights(v.Params)
	if err != nil {
		return edge.Weights{}, v, err
	}
	return w, v, nil
}

// List returns recent versions.
func List(ctx context.Context, conn db.DBInterface, strategy string, limit int) ([]Version, error) {
	return db.ListStrategyVersions(ctx, conn, strategy, limit)
}

// Get returns one version.
func Get(ctx context.Context, conn db.DBInterface, id int64) (Version, error) {
	return db.GetStrategyVersion(ctx, conn, id)
}

// TryLoadActive is LoadActive that returns (zero, false, nil) when no active pointer.
func TryLoadActive(ctx context.Context, conn db.DBInterface, strategy string) (LoadedActive, bool, error) {
	la, err := LoadActive(ctx, conn, strategy)
	if err != nil {
		if errors.Is(err, db.ErrNoActiveStrategy) {
			return LoadedActive{}, false, nil
		}
		return LoadedActive{}, false, err
	}
	return la, true, nil
}

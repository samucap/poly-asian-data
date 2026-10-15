package db_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/stretchr/testify/require"
)

func TestStrategyVersions_ActivatePromoteRollback(t *testing.T) {
	pool := openTestPool(t)
	ctx := context.Background()
	require.NoError(t, db.EnsureStrategyTables(ctx, pool))

	// Isolate with unique strategy name.
	name := "test_m5_" + t.Name()
	// Clean prior runs if any
	_, _ = pool.Exec(ctx, `DELETE FROM strategy_promotions WHERE strategy = $1`, name)
	_, _ = pool.Exec(ctx, `DELETE FROM strategy_active WHERE strategy = $1`, name)
	_, _ = pool.Exec(ctx, `DELETE FROM strategy_versions WHERE strategy = $1`, name)

	params, err := json.Marshal(map[string]any{"name": name, "w_vol": 0.5})
	require.NoError(t, err)

	v1, err := db.InsertStrategyVersion(ctx, pool, db.StrategyVersion{
		Strategy: name, Status: db.StrategyStatusDraft,
		Params: params, WeightsHash: "hash_v1", SourcePath: "a.yaml", CreatedBy: "test",
	})
	require.NoError(t, err)
	v2, err := db.InsertStrategyVersion(ctx, pool, db.StrategyVersion{
		Strategy: name, Status: db.StrategyStatusDraft,
		Params: params, WeightsHash: "hash_v2", SourcePath: "b.yaml", CreatedBy: "test",
	})
	require.NoError(t, err)

	// Promote v1
	a, err := db.ActivateStrategyVersion(ctx, pool, db.ActivateOpts{
		VersionID: v1.ID, Actor: "test", WeightsHash: "hash_v1",
		PromoteEligible: true, Action: "promote",
	})
	require.NoError(t, err)
	require.Equal(t, v1.ID, a.VersionID)
	require.Nil(t, a.PrevVersionID)

	got, err := db.GetStrategyVersion(ctx, pool, v1.ID)
	require.NoError(t, err)
	require.Equal(t, db.StrategyStatusActive, got.Status)

	// Promote v2 — v1 retired, prev=v1
	a, err = db.ActivateStrategyVersion(ctx, pool, db.ActivateOpts{
		VersionID: v2.ID, Actor: "test", WeightsHash: "hash_v2",
		PromoteEligible: true, Action: "promote",
	})
	require.NoError(t, err)
	require.Equal(t, v2.ID, a.VersionID)
	require.NotNil(t, a.PrevVersionID)
	require.Equal(t, v1.ID, *a.PrevVersionID)

	got1, err := db.GetStrategyVersion(ctx, pool, v1.ID)
	require.NoError(t, err)
	require.Equal(t, db.StrategyStatusRetired, got1.Status)
	got2, err := db.GetStrategyVersion(ctx, pool, v2.ID)
	require.NoError(t, err)
	require.Equal(t, db.StrategyStatusActive, got2.Status)

	// Exactly one active
	var nActive int
	err = pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM strategy_versions WHERE strategy = $1 AND status = 'active'
	`, name).Scan(&nActive)
	require.NoError(t, err)
	require.Equal(t, 1, nActive)

	// Rollback once → v1 active, prev cleared
	a, err = db.RollbackStrategyActive(ctx, pool, name, "test", "incident")
	require.NoError(t, err)
	require.Equal(t, v1.ID, a.VersionID)
	require.Nil(t, a.PrevVersionID)

	// Second rollback hard-fails (not a toggle)
	_, err = db.RollbackStrategyActive(ctx, pool, name, "test", "again")
	require.Error(t, err)
	require.True(t, errors.Is(err, db.ErrNoRollbackTarget))
}

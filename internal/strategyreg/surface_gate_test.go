package strategyreg

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckPromoteGate(t *testing.T) {
	hash := "abc123"
	ok := SurfaceSummary{OK: true, PromoteEligible: true, WeightsHash: hash, RunID: "r1", StrategyName: "default"}

	require.NoError(t, CheckPromoteGate(ok, hash, "default"))
	require.NoError(t, CheckPromoteGate(SurfaceSummary{OK: true, PromoteEligible: true, WeightsHash: hash}, hash, "default"))

	err := CheckPromoteGate(SurfaceSummary{OK: true, PromoteEligible: false, WeightsHash: hash}, hash, "default")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrGateRejected))

	err = CheckPromoteGate(SurfaceSummary{OK: false, PromoteEligible: true, WeightsHash: hash}, hash, "default")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrGateRejected))

	err = CheckPromoteGate(SurfaceSummary{OK: true, PromoteEligible: true, WeightsHash: "other"}, hash, "default")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrGateRejected))

	err = CheckPromoteGate(SurfaceSummary{OK: true, PromoteEligible: true, WeightsHash: ""}, hash, "default")
	require.Error(t, err)

	err = CheckPromoteGate(ok, "", "default")
	require.Error(t, err)

	err = CheckPromoteGate(SurfaceSummary{OK: true, PromoteEligible: true, WeightsHash: hash, StrategyName: "other"}, hash, "default")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrGateRejected))
}

func TestLoadSurfaceSummary(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "surface.json")
	require.NoError(t, os.WriteFile(path, []byte(`{
		"ok": true,
		"promote_eligible": true,
		"weights_hash": "deadbeef",
		"run_id": "run-1",
		"strategy_name": "default"
	}`), 0o644))

	s, err := LoadSurfaceSummary(path)
	require.NoError(t, err)
	require.True(t, s.OK)
	require.True(t, s.PromoteEligible)
	require.Equal(t, "deadbeef", s.WeightsHash)
	require.Equal(t, "run-1", s.RunID)

	_, err = LoadSurfaceSummary("")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrGateRejected))
}

func TestParamsToWeights(t *testing.T) {
	w, err := ParamsToWeights([]byte(`{"name":"exp1","w_vol":0.5,"fv_enabled":true}`))
	require.NoError(t, err)
	require.Equal(t, "exp1", w.Name)
	require.Equal(t, 0.5, w.WVol)
	require.True(t, w.FVEnabled)
	// Defaults filled for missing fields
	require.Greater(t, w.ImpactCapBps, 0.0)
}

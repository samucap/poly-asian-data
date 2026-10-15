package eval

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIsForbiddenFeature(t *testing.T) {
	require.True(t, IsForbiddenFeature("edge_bps"))
	require.True(t, IsForbiddenFeature("ComputedScore"))
	require.True(t, IsForbiddenFeature("stage1_score"))
	require.True(t, IsForbiddenFeature("rank"))
	require.True(t, IsForbiddenFeature("score_vol"))
	require.False(t, IsForbiddenFeature("spread_bps"))
	require.False(t, IsForbiddenFeature("mid"))
	require.False(t, IsForbiddenFeature("volume_24hr"))
}

func TestCheckForbiddenFeatures(t *testing.T) {
	bad := CheckForbiddenFeatures([]string{"mid", "edge_bps", "mid", "spread"})
	require.Equal(t, []string{"edge_bps"}, bad)
}

func TestEvaluateGates_VanityWinRateFails(t *testing.T) {
	// High hit rate but no baselines, no strata, tiny n, forbidden feature
	s := &EvalSurface{
		SchemaVersion: SchemaVersion,
		RunID:         "t1",
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		Status:        "success",
		SelectionSet:  SelectionBoard,
		Horizons:      []string{"1h"},
		LabelProtocol: LabelProtocol{
			PointInTime:        true,
			AsOfField:          "selected_at",
			Horizons:           []string{"1h"},
			NoFutureInFeatures: true,
		},
		FillModel: FillModel{Name: "mid_only"}, // weak costs
		Metrics: EvalMetrics{
			N:              10,
			PrimaryHorizon: "1h",
			Overall: HorizonMetrics{
				N:                  10,
				HitRate:            0.9,
				WinRate:            0.9,
				AfterCostReturnBps: 50,
			},
		},
		FeatureNames: []string{"mid", "edge_bps"},
	}
	EvaluateGates(s, DefaultGateConfig())
	require.False(t, s.OK, "vanity surface must not pass")
	require.Contains(t, s.GatesFailed, GateNoForbiddenFeatures)
	require.Contains(t, s.GatesFailed, GateBaselinesPresent)
	require.Contains(t, s.GatesFailed, GateMinSample)
	require.Contains(t, s.GatesFailed, GateStratified)
}

func TestEvaluateGates_RobustPasses(t *testing.T) {
	s := robustSurface()
	EvaluateGates(s, DefaultGateConfig())
	require.True(t, s.OK, "gates_failed=%v details=%v", s.GatesFailed, s.GateDetails)
	require.Empty(t, s.GatesFailed)
	require.Contains(t, s.GatesPassed, GateBeatsVolumeBaseline)
	require.Contains(t, s.GatesPassed, GateBeatsActivityBaseline)
	require.NoError(t, ValidateStructural(s))
}

func TestEvaluateGates_DoesNotBeatBaseline(t *testing.T) {
	s := robustSurface()
	s.Metrics.Overall.AfterCostReturnBps = -10
	s.Metrics.Baselines["volume_top_n"] = HorizonMetrics{AfterCostReturnBps: 0, N: 100, HitRate: 0.5}
	EvaluateGates(s, DefaultGateConfig())
	require.False(t, s.OK)
	require.Contains(t, s.GatesFailed, GateBeatsVolumeBaseline)
}

func robustSurface() *EvalSurface {
	return &EvalSurface{
		SchemaVersion: SchemaVersion,
		RunID:         "t2",
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		Status:        "success",
		SelectionSet:  SelectionBoard,
		PolicyParity:  PolicyParityScanBoard,
		PolicyID:      PolicyIDSelectBoardV1,
		Horizons:      DefaultHorizons,
		LabelProtocol: LabelProtocol{
			PointInTime:        true,
			AsOfField:          "features_asof",
			Horizons:           DefaultHorizons,
			NoFutureInFeatures: true,
			ResolvedHandling:   "drop_if_resolved_before_horizon",
		},
		FillModel: FillModel{
			Name:          "mid_fee_slip",
			Entry:         "mid",
			FeeBps:        10,
			SlipBps:       5,
			IncludeSpread: true,
		},
		FeatureNames: CandidateFeatureNames,
		Metrics: EvalMetrics{
			N:              100,
			PrimaryHorizon: "1h",
			Overall: HorizonMetrics{
				Horizon:            "1h",
				N:                  100,
				HitRate:            0.55,
				AfterCostReturnBps: 12,
			},
			ByStratum: map[string]HorizonMetrics{
				"category=sports": {N: 40, HitRate: 0.5, AfterCostReturnBps: 5},
				"neg_risk=false":  {N: 60, HitRate: 0.58, AfterCostReturnBps: 15},
				"fv_source=proxy": {N: 70, HitRate: 0.52, AfterCostReturnBps: 8},
				"ttr_bucket=mid":  {N: 50, HitRate: 0.56, AfterCostReturnBps: 10},
			},
			Baselines: map[string]HorizonMetrics{
				"volume_top_n":    {N: 100, HitRate: 0.5, AfterCostReturnBps: 0},
				"activity_stage1": {N: 100, HitRate: 0.51, AfterCostReturnBps: 2},
				"random_board":    {N: 100, HitRate: 0.48, AfterCostReturnBps: -5},
			},
		},
	}
}

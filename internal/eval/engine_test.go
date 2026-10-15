package eval

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAggregateLabels(t *testing.T) {
	labels := []Label{
		{Horizon: "1h", Hit: true, AfterCostReturnBps: 10},
		{Horizon: "1h", Hit: false, AfterCostReturnBps: -5},
		{Horizon: "1h", Hit: true, AfterCostReturnBps: 20},
	}
	m := AggregateLabels(labels)
	require.Equal(t, 3, m.N)
	require.InDelta(t, 2.0/3.0, m.HitRate, 1e-9)
	require.InDelta(t, (10-5+20)/3.0, m.AfterCostReturnBps, 1e-9)
	require.InDelta(t, 10, m.AfterCostReturnMedBps, 1e-9)
}

func TestBuildMetrics_BaselinesAndStrata(t *testing.T) {
	now := time.Now().UTC()
	var labels []Label
	// candidate: better after-cost
	for i := 0; i < 60; i++ {
		labels = append(labels, Label{
			DecisionTime: now, Horizon: "1h", ConditionID: "c", Policy: "candidate",
			Hit: true, AfterCostReturnBps: 15,
			Category: "sports", NegRisk: false, FVSource: "proxy", TTRBucket: "mid",
		})
	}
	for i := 0; i < 60; i++ {
		labels = append(labels, Label{
			DecisionTime: now, Horizon: "1h", ConditionID: "v", Policy: "volume_top_n",
			AfterCostReturnBps: 0, Category: "sports",
		})
		labels = append(labels, Label{
			DecisionTime: now, Horizon: "1h", ConditionID: "a", Policy: "activity_stage1",
			AfterCostReturnBps: 2, Category: "sports",
		})
		labels = append(labels, Label{
			DecisionTime: now, Horizon: "1h", ConditionID: "r", Policy: "random_board",
			AfterCostReturnBps: -5, Category: "sports",
		})
	}
	m := BuildMetrics(labels, "1h")
	require.Equal(t, 60, m.N)
	require.InDelta(t, 15, m.Overall.AfterCostReturnBps, 1e-9)
	require.Contains(t, m.Baselines, "volume_top_n")
	require.InDelta(t, 15, m.DeltaVsBaselines["volume_top_n"], 1e-9)
	require.GreaterOrEqual(t, len(m.ByStratum), 2)

	cfg := DefaultConfig()
	s := NewSurface(cfg, m, CandidateFeatureNames, "test-run")
	s.PolicyParity = PolicyParityScanBoard
	EvaluateGates(s, cfg.GateConfig())
	require.True(t, s.OK, "failed=%v details=%v", s.GatesFailed, s.GateDetails)
	require.True(t, s.PromoteEligible)
	require.Empty(t, CheckForbiddenFeatures(CandidateFeatureNames))
}

func TestTTRBucket(t *testing.T) {
	require.Equal(t, "near", TTRBucket(12))
	require.Equal(t, "mid", TTRBucket(48))
	require.Equal(t, "far", TTRBucket(24*14))
}

func TestLoadConfig_Default(t *testing.T) {
	cfg, err := LoadConfig("")
	require.NoError(t, err)
	require.Equal(t, "1h", cfg.PrimaryHorizon)
	require.Equal(t, 50, cfg.Gates.MinSample)
}

func TestLoadConfig_File(t *testing.T) {
	cfg, err := LoadConfig("../../configs/eval/default.yaml")
	require.NoError(t, err)
	require.Equal(t, "1.0", cfg.SchemaVersion)
	require.Equal(t, "mid_fee_slip", cfg.FillModel.Name)
	require.True(t, cfg.LabelProtocol.PointInTime)
}

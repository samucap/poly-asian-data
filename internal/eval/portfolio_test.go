package eval

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBuildPortfolioMetrics_DrawdownAndSharpe(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Two periods: +10 mean, then -30 mean → drawdown 20 from peak after first
	labels := []Label{
		{DecisionTime: t0, Horizon: "1h", Policy: "candidate", AfterCostReturnBps: 10, Hit: true},
		{DecisionTime: t0, Horizon: "1h", Policy: "candidate", AfterCostReturnBps: 10, Hit: true},
		{DecisionTime: t0.Add(time.Hour), Horizon: "1h", Policy: "candidate", AfterCostReturnBps: -30, Hit: false},
		{DecisionTime: t0.Add(time.Hour), Horizon: "1h", Policy: "candidate", AfterCostReturnBps: -30, Hit: false},
	}
	p := BuildPortfolioMetrics(labels, "1h", "candidate", 24*365)
	require.Equal(t, 2, p.NPeriods)
	require.InDelta(t, -20, p.TotalReturnBps, 1e-9) // 10 + (-30)
	require.InDelta(t, 30, p.MaxDrawdownBps, 1e-9)   // peak 10 → equity -20 → dd 30
	require.NotZero(t, p.Sharpe)                     // non-zero vol
}

func TestBuildMetrics_ByHorizonAndPortfolio(t *testing.T) {
	t0 := time.Now().UTC()
	var labels []Label
	for i := 0; i < 60; i++ {
		labels = append(labels, Label{
			DecisionTime: t0.Add(time.Duration(i) * time.Hour), Horizon: "1h", Policy: "candidate",
			AfterCostReturnBps: 5, Hit: true, Category: "sports", FVSource: "proxy", TTRBucket: "mid",
		})
		labels = append(labels, Label{
			DecisionTime: t0.Add(time.Duration(i) * time.Hour), Horizon: "5m", Policy: "candidate",
			AfterCostReturnBps: 1, Hit: true, Category: "sports", FVSource: "proxy", TTRBucket: "mid",
		})
		labels = append(labels, Label{
			DecisionTime: t0, Horizon: "1h", Policy: "volume_top_n", AfterCostReturnBps: 0,
		})
		labels = append(labels, Label{
			DecisionTime: t0, Horizon: "1h", Policy: "activity_stage1", AfterCostReturnBps: 0,
		})
		labels = append(labels, Label{
			DecisionTime: t0, Horizon: "1h", Policy: "random_board", AfterCostReturnBps: -1,
		})
	}
	m := BuildMetrics(labels, "1h")
	require.Equal(t, 60, m.N)
	require.Contains(t, m.ByHorizon, "1h")
	require.Contains(t, m.ByHorizon, "5m")
	require.NotNil(t, m.Portfolio)
	require.Equal(t, 60, m.Portfolio.NPeriods)
	require.InDelta(t, m.Portfolio.MaxDrawdownBps, m.Overall.MaxDrawdownBps, 1e-9)
}

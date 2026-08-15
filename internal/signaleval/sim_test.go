package signaleval

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSimulateSyntheticTightBudget(t *testing.T) {
	sigs, prices := SyntheticFixture()
	cfg := SimConfig{Risk: TightBudgetRisk()}
	res := Simulate(cfg, sigs, prices)

	require.Greater(t, res.Metrics.NSignals, 0)
	require.Greater(t, res.Metrics.NAccepted, 0)
	// Simultaneous high/low: low should lose budget after high takes gross
	require.Greater(t, res.Metrics.NRejectedRisk, 0, "reject_reasons=%v", res.Metrics.RejectReasons)
	require.NotEmpty(t, res.ConfigHash)
	require.NotNil(t, res.Metrics.SelectionQuality)
	require.True(t, res.Metrics.SelectionQuality.PreferHigherEdge,
		"should prefer higher opportunity: %+v", res.Metrics.SelectionQuality)
	// First accepted trade should be high-edge condition
	require.Equal(t, "cond_high", res.Trades[0].ConditionID)
}

func TestSimulateAuctionOutranks(t *testing.T) {
	t0 := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	sigs := []SignalIn{
		{Time: t0, SignalID: "1", ConditionID: "weak", TokenID: "tw", Side: "BUY", EdgeBps: 10, Conviction: 0.2, SizeUSD: 80, CapacityUSD: 50_000, CostBps: 10, Mid: 0.5, HorizonSec: 3600},
		{Time: t0.Add(time.Second), SignalID: "2", ConditionID: "strong", TokenID: "ts", Side: "BUY", EdgeBps: 400, Conviction: 0.9, SizeUSD: 80, CapacityUSD: 50_000, CostBps: 10, Mid: 0.4, HorizonSec: 3600},
		{Time: t0.Add(2 * time.Second), SignalID: "3", ConditionID: "mid", TokenID: "tm", Side: "BUY", EdgeBps: 80, Conviction: 0.5, SizeUSD: 80, CapacityUSD: 50_000, CostBps: 10, Mid: 0.45, HorizonSec: 3600},
	}
	prices := PriceIndex{
		"tw": {{Time: t0, Mid: 0.5}, {Time: t0.Add(time.Hour), Mid: 0.5}},
		"ts": {{Time: t0, Mid: 0.4}, {Time: t0.Add(time.Hour), Mid: 0.45}},
		"tm": {{Time: t0, Mid: 0.45}, {Time: t0.Add(time.Hour), Mid: 0.45}},
	}
	cfg := TightBudgetRisk()
	cfg.AuctionEnabled = true
	cfg.AllocWindowSec = 60
	cfg.AuctionDedupe = "condition"
	cfg.MaxPositions = 1
	cfg.MaxGrossUSD = 500
	res := Simulate(SimConfig{Risk: cfg}, sigs, prices)
	require.Equal(t, 1, res.Metrics.NAccepted, "reject=%v trades=%d", res.Metrics.RejectReasons, len(res.Trades))
	require.Equal(t, "strong", res.Trades[0].ConditionID)
	require.Greater(t, res.Metrics.RejectReasons["outranked"], 0)
	require.True(t, res.Metrics.SelectionQuality.PreferHigherEdge)
}

func TestSpotCheckFills(t *testing.T) {
	t0 := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	sigs := []SignalIn{{Time: t0, SignalID: "s1", ConditionID: "c", TokenID: "tok", Mid: 0.40}}
	prices := PriceIndex{"tok": {{Time: t0, Mid: 0.40}}}
	got := SpotCheckFills(sigs, prices, 1)
	require.Len(t, got, 1)
	require.True(t, got[0].OK)
}

func TestApplyWorthiness(t *testing.T) {
	sigs := []SignalIn{
		{ConditionID: "a", EdgeBps: 10, SpreadBps: 50, ScorePath: "proxy"},
		{ConditionID: "b", EdgeBps: 80, SpreadBps: 40, FVSource: "neg_risk_complement", ScorePath: "fair_value"},
		{ConditionID: "c", EdgeBps: 90, SpreadBps: 400, ScorePath: "proxy"},
	}
	out := ApplyWorthiness(sigs, WorthinessFilter{MinEdgeBps: 25, MaxSpreadBps: 200})
	require.Len(t, out, 1)
	require.Equal(t, "b", out[0].ConditionID)
	fvOnly := ApplyWorthiness(sigs, WorthinessFilter{RequireFV: true})
	require.Len(t, fvOnly, 1)
	require.Equal(t, "b", fvOnly[0].ConditionID)
}

func TestRealizePnL(t *testing.T) {
	// BUY 10 shares @ 0.4 → exit 0.5, cost 1
	pnl := RealizePnL("BUY", 10, 0.4, 0.5, 1)
	require.InDelta(t, 0.0, pnl, 1e-9) // 10*0.1 - 1 = 0
	pnlS := RealizePnL("SELL", 10, 0.6, 0.5, 0)
	require.InDelta(t, 1.0, pnlS, 1e-9)
}

func TestBuildSurfaceOK(t *testing.T) {
	sigs, prices := SyntheticFixture()
	res := Simulate(SimConfig{Risk: TightBudgetRisk()}, sigs, prices)
	surf := BuildSurface("test-run", "default", res, sigs[0].Time, sigs[len(sigs)-1].Time, 3)
	require.True(t, surf.OK)
	require.Equal(t, "signal_eval_surface_v1", surf.SchemaVersion)
	require.Contains(t, surf.GatesPassed, "min_sample")
}

func TestEquityStatsDrawdown(t *testing.T) {
	t0 := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	pts := []EquityPoint{
		{Time: t0, Equity: 100},
		{Time: t0.Add(time.Hour), Equity: 110},
		{Time: t0.Add(2 * time.Hour), Equity: 90},
	}
	st := ComputeEquityStats(pts, 100, 24*365)
	require.InDelta(t, 1818.18, st.MaxDrawdownBps, 1) // (110-90)/110 * 1e4
}

func TestSparsePathNoExplosiveSharpe(t *testing.T) {
	// ~20 lumpy losses totaling ~$80 — event-based Sharpe used to annualize to ~-9.
	t0 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	eq := 10_000.0
	var pts []EquityPoint
	pts = append(pts, EquityPoint{Time: t0, Equity: eq})
	for i := 1; i <= 20; i++ {
		eq -= 4.0
		pts = append(pts, EquityPoint{Time: t0.Add(time.Duration(i) * 30 * time.Minute), Equity: eq})
	}
	st := ComputeEquityStats(pts, 10_000, 24*365)
	require.InDelta(t, -80, st.TotalReturnBps, 1) // -80 bps on 10k
	// Short window → no annualized Sharpe; raw mean still reported
	require.Equal(t, 0.0, st.Sharpe)
	require.Equal(t, "insufficient_periods", st.SharpeNote)
	require.Less(t, st.MeanPeriodReturn, 0.0)
	require.Greater(t, st.NPeriods, 0)
}

func TestLongPathCanReportSharpe(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	eq := 10_000.0
	var pts []EquityPoint
	// 80 hours alternating mild up/down with positive drift
	for i := 0; i < 80; i++ {
		if i%2 == 0 {
			eq *= 1.001
		} else {
			eq *= 0.9995
		}
		pts = append(pts, EquityPoint{Time: t0.Add(time.Duration(i) * time.Hour), Equity: eq})
	}
	st := ComputeEquityStats(pts, 10_000, 24*365)
	require.GreaterOrEqual(t, st.NPeriods, MinHoursForSharpe)
	require.Empty(t, st.SharpeNote)
	require.NotZero(t, st.Sharpe)
}

func TestResampleHourlyForwardFill(t *testing.T) {
	t0 := time.Date(2026, 6, 1, 10, 15, 0, 0, time.UTC)
	pts := []EquityPoint{
		{Time: t0, Equity: 100},
		{Time: t0.Add(2*time.Hour + 30*time.Minute), Equity: 110},
	}
	h := ResampleHourly(pts)
	require.GreaterOrEqual(t, len(h), 3) // 10:00, 11:00, 12:00
	require.Equal(t, 100.0, h[0].Equity)
	// hour 11 forward-filled
	require.Equal(t, 100.0, h[1].Equity)
	require.Equal(t, 110.0, h[len(h)-1].Equity)
}


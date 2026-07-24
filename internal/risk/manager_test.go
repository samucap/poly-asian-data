package risk

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAcceptKellyCaps(t *testing.T) {
	cfg := DefaultConfig()
	cfg.StartingEquityUSD = 10_000
	cfg.MaxPositionUSD = 200
	cfg.SizingMode = SizingFracKelly
	m := NewManager(cfg)
	d := m.Accept(SignalInput{
		Time: time.Now().UTC(), ConditionID: "c1", Side: "BUY",
		SizeUSD: 1000, Conviction: 1, CapacityUSD: 1e9, EdgeBps: 100,
	})
	require.True(t, d.Accept)
	require.LessOrEqual(t, d.SizeUSD, cfg.MaxPositionUSD)
	require.Greater(t, d.SizeUSD, 0.0)
	require.Greater(t, d.OpportunityScore, 0.0)
}

func TestMaxPositions(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxPositions = 1
	m := NewManager(cfg)
	now := time.Now().UTC()
	d1 := m.Accept(SignalInput{Time: now, ConditionID: "a", Side: "BUY", SizeUSD: 50, Conviction: 0.8})
	require.True(t, d1.Accept)
	m.OnOpen(Position{ConditionID: "a", Side: "BUY", SizeUSD: d1.SizeUSD, OpenedAt: now})
	d2 := m.Accept(SignalInput{Time: now, ConditionID: "b", Side: "BUY", SizeUSD: 50, Conviction: 0.8})
	require.False(t, d2.Accept)
	require.Equal(t, ReasonMaxPositions, d2.Reason)
}

func TestAlreadyOpenDistinctFromMaxPositions(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxPositions = 10
	m := NewManager(cfg)
	now := time.Now().UTC()
	d1 := m.Accept(SignalInput{Time: now, ConditionID: "a", Side: "BUY", SizeUSD: 50, Conviction: 0.8})
	require.True(t, d1.Accept)
	m.OnOpen(Position{ConditionID: "a", Side: "BUY", SizeUSD: d1.SizeUSD, OpenedAt: now})
	d2 := m.Accept(SignalInput{Time: now, ConditionID: "a", Side: "BUY", SizeUSD: 50, Conviction: 0.8})
	require.False(t, d2.Accept)
	require.Equal(t, ReasonAlreadyOpen, d2.Reason)
}

func TestVolTargetScalesWithPeriodReturns(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SizingMode = SizingVolTarget
	cfg.VolLookbackPeriods = 24
	cfg.MaxPositionUSD = 10_000
	cfg.MaxGrossUSD = 50_000
	m := NewManager(cfg)
	now := time.Now().UTC()
	// High realized vol → scale down
	for i := 0; i < 20; i++ {
		if i%2 == 0 {
			m.RecordPeriodReturn(0.05)
		} else {
			m.RecordPeriodReturn(-0.05)
		}
	}
	dVol := m.Accept(SignalInput{
		Time: now, ConditionID: "v1", Side: "BUY", SizeUSD: 500, Conviction: 1, CapacityUSD: 1e9,
	})
	require.True(t, dVol.Accept)

	m2 := NewManager(cfg)
	// Flat path → scale ~1
	for i := 0; i < 20; i++ {
		m2.RecordPeriodReturn(0.0001)
	}
	dFlat := m2.Accept(SignalInput{
		Time: now, ConditionID: "v2", Side: "BUY", SizeUSD: 500, Conviction: 1, CapacityUSD: 1e9,
	})
	require.True(t, dFlat.Accept)
	require.Less(t, dVol.SizeUSD, dFlat.SizeUSD, "high vol should size smaller than near-flat path")
}

func TestDailyDDHalt(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxDailyDrawdownBps = 100
	cfg.StartingEquityUSD = 10_000
	m := NewManager(cfg)
	now := time.Now().UTC()
	m.ApplyPnL(-200, now)
	require.Equal(t, StateHaltedDay, m.State)
	d := m.Accept(SignalInput{Time: now, ConditionID: "x", Side: "BUY", SizeUSD: 50, Conviction: 1})
	require.False(t, d.Accept)
	require.Equal(t, ReasonHaltedDay, d.Reason)
}

func TestMaxDDHalt(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxDrawdownBps = 500
	cfg.StartingEquityUSD = 10_000
	m := NewManager(cfg)
	now := time.Now().UTC()
	m.ApplyPnL(-600, now)
	require.Equal(t, StateHaltedDD, m.State)
	require.True(t, m.ShouldFlattenAll())
}

func TestHigherEdgeWinsRank(t *testing.T) {
	cfg := DefaultConfig()
	order := BuildRankOrder(cfg,
		[]float64{10, 200, 50},
		[]float64{0.5, 0.5, 0.5},
		[]float64{0, 0, 0},
	)
	require.Equal(t, []int{1, 2, 0}, order, "highest |edge| first")
}

func TestBudgetPrefersHighEdge(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxGrossUSD = 100
	cfg.MaxPositionUSD = 100
	cfg.SizingMode = SizingSignalSize
	cfg.MaxPositions = 10
	m := NewManager(cfg)
	now := time.Now().UTC()

	// Process in opportunity order: high edge first
	sigs := []SignalInput{
		{Time: now, ConditionID: "low", Side: "BUY", SizeUSD: 90, EdgeBps: 10, Conviction: 0.5},
		{Time: now, ConditionID: "high", Side: "BUY", SizeUSD: 90, EdgeBps: 300, Conviction: 0.5},
	}
	order := BuildRankOrder(cfg,
		[]float64{sigs[0].EdgeBps, sigs[1].EdgeBps},
		[]float64{sigs[0].Conviction, sigs[1].Conviction},
		nil,
	)
	var accepted []string
	for _, i := range order {
		d := m.Accept(sigs[i])
		if d.Accept {
			accepted = append(accepted, sigs[i].ConditionID)
			m.OnOpen(Position{ConditionID: sigs[i].ConditionID, Side: "BUY", SizeUSD: d.SizeUSD})
		}
	}
	require.Contains(t, accepted, "high")
	// low may be rejected for max_gross after high takes budget
	if len(accepted) == 1 {
		require.Equal(t, "high", accepted[0])
	}
}

func TestDayResume(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxDailyDrawdownBps = 50
	m := NewManager(cfg)
	day1 := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	m.ApplyPnL(-100, day1)
	require.Equal(t, StateHaltedDay, m.State)
	day2 := time.Date(2026, 7, 2, 1, 0, 0, 0, time.UTC)
	d := m.Accept(SignalInput{Time: day2, ConditionID: "c", Side: "BUY", SizeUSD: 40, Conviction: 0.9})
	require.True(t, d.Accept)
	require.Equal(t, StateOK, m.State)
}

package signals

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEvaluateEmitsOnPositiveEdge(t *testing.T) {
	e := NewEmitter(DefaultGateConfig())
	now := time.Now().UTC()
	fv := 0.55
	edge := 200.0
	board := BoardSnap{
		ConditionID:  "c1",
		MarketID:     "m1",
		TokenID:      "t1",
		Rank:         1,
		FairValue:    &fv,
		ModelEdgeBps: &edge,
		FVSource:     "test",
		CapacityUSD:  ptr(10_000),
	}
	book := BookSnap{
		BestBid: 0.48, BestAsk: 0.50, Mid: 0.49,
		BidDepth: 500, AskDepth: 500,
		UpdatedAt: now,
	}
	sig := e.Evaluate(now, "default", board, book)
	require.NotNil(t, sig)
	require.Equal(t, ModePaper, sig.Mode)
	require.Equal(t, SideBuy, sig.Side)
	require.Equal(t, ActionEnter, sig.Action)
	require.Greater(t, sig.EdgeBps, 0.0)
	require.Greater(t, sig.Conviction, 0.0)
	require.NotEmpty(t, sig.SignalID)
	require.NotEmpty(t, sig.Factors)
	require.Contains(t, sig.Reason, "gates")
}

func TestEvaluateDebounce(t *testing.T) {
	cfg := DefaultGateConfig()
	cfg.Cooldown = time.Minute
	e := NewEmitter(cfg)
	now := time.Now().UTC()
	fv := 0.55
	board := BoardSnap{ConditionID: "c1", TokenID: "t1", FairValue: &fv}
	book := BookSnap{BestBid: 0.48, BestAsk: 0.50, Mid: 0.49, BidDepth: 100, AskDepth: 100, UpdatedAt: now}
	s1 := e.Evaluate(now, "default", board, book)
	require.NotNil(t, s1)
	s2 := e.Evaluate(now.Add(time.Second), "default", board, book)
	require.Nil(t, s2)
}

func TestEvaluateRejectsWideSpread(t *testing.T) {
	cfg := DefaultGateConfig()
	cfg.MaxSpreadBps = 50
	e := NewEmitter(cfg)
	now := time.Now().UTC()
	fv := 0.80
	board := BoardSnap{ConditionID: "c1", TokenID: "t1", FairValue: &fv}
	book := BookSnap{BestBid: 0.40, BestAsk: 0.60, Mid: 0.50, BidDepth: 10, AskDepth: 10, UpdatedAt: now}
	require.Nil(t, e.Evaluate(now, "default", board, book))
}

func TestConvictionScalesSize(t *testing.T) {
	cfg := DefaultGateConfig()
	cfg.Cooldown = 0
	cfg.MaxNotionalUSD = 10_000
	cfg.ProbeSizeUSD = 100
	e := NewEmitter(cfg)
	now := time.Now().UTC()
	// Strong residual
	fvHi := 0.70
	boardHi := BoardSnap{ConditionID: "a", TokenID: "t", FairValue: &fvHi, CapacityUSD: ptr(1e9)}
	book := BookSnap{BestBid: 0.49, BestAsk: 0.51, Mid: 0.50, BidDepth: 1e6, AskDepth: 1e6, UpdatedAt: now}
	hi := e.Evaluate(now, "default", boardHi, book)
	require.NotNil(t, hi)

	e2 := NewEmitter(cfg)
	fvLo := 0.505
	boardLo := BoardSnap{ConditionID: "b", TokenID: "t2", FairValue: &fvLo, CapacityUSD: ptr(1e9)}
	lo := e2.Evaluate(now, "default", boardLo, book)
	require.NotNil(t, lo)
	require.Greater(t, hi.SizeUSD, lo.SizeUSD)
	require.Greater(t, hi.Conviction, lo.Conviction)
}

func TestBuildFactors(t *testing.T) {
	fv := 0.52
	f := BuildFactors(
		BoardSnap{FairValue: &fv, Score: 0.1, KeyFeatures: map[string]any{"abs_ret_5m": 0.01}},
		BookSnap{Imbalance: 0.2, BestBid: 0.49, BestAsk: 0.51, Mid: 0.5},
		100, 20, 80,
	)
	require.InDelta(t, 80, f["net_edge_bps"], 1e-9)
	require.InDelta(t, 0.2, f["imbalance"], 1e-9)
}

func ptr(f float64) *float64 { return &f }

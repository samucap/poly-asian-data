package eval

import (
	"testing"
	"time"

	"github.com/samucap/poly-asian-data/internal/edge"
	"github.com/stretchr/testify/require"
)

func TestPriceSeries_MidAsOf(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := PriceSeries{
		TokenID: "tok",
		Times:   []time.Time{t0, t0.Add(time.Hour), t0.Add(2 * time.Hour)},
		Prices:  []float64{0.4, 0.5, 0.6},
	}
	p, ok := s.MidAsOf(t0.Add(90 * time.Minute))
	require.True(t, ok)
	require.InDelta(t, 0.5, p, 1e-9)
}

func TestRunBacktest_SelectBoardPolicy(t *testing.T) {
	t0 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	tokenA, tokenB := "ta", "tb"
	var times []time.Time
	var pa, pb []float64
	for i := 0; i < 48; i++ {
		times = append(times, t0.Add(time.Duration(i)*time.Hour))
		pa = append(pa, 0.40+float64(i)*0.002)
		pb = append(pb, 0.50)
	}
	prices := map[string]PriceSeries{
		tokenA: {TokenID: tokenA, Times: times, Prices: pa},
		tokenB: {TokenID: tokenB, Times: times, Prices: pb},
	}

	w := edge.DefaultWeights()
	w.DropMissingBook = false
	w.DropExtremePrice = true
	w.FVEnabled = true

	var snaps []SnapshotAtT
	for i := 0; i < 12; i++ {
		tt := t0.Add(time.Duration(i*2) * time.Hour)
		var pts []MarketPoint
		for _, tok := range []struct {
			id, cat string
		}{{"ta", "crypto"}, {"tb", "sports"}} {
			ps := prices[tok.id]
			meta := MetaAsOf{ConditionID: tok.id, TokenID: tok.id, Category: tok.cat, EndDate: t0.Add(30 * 24 * time.Hour)}
			feat, ok := FeaturesAsOf(tt, meta, ps, nil, 10)
			require.True(t, ok)
			require.Greater(t, feat.VolumeProxy, 0.0)
			require.Equal(t, 0.0, feat.Volume24hr)
			mid, _ := ps.MidAsOf(tt)
			pts = append(pts, MarketPoint{
				ConditionID: tok.id, TokenID: tok.id, Category: tok.cat, Mid: mid,
				VolumeProxy: VolumeProxy24h(ps, tt), ActivityProxy: ActivityProxy24h(ps, tt),
				Features: feat,
			})
		}
		// group mids for FV (binary-like)
		pts[0].GroupMids = map[string]float64{"ta": pts[0].Mid, "tb": pts[1].Mid}
		pts[0].NegRisk = true
		pts[0].Features.NegRisk = true
		pts[1].GroupMids = map[string]float64{"ta": pts[0].Mid, "tb": pts[1].Mid}
		pts[1].NegRisk = true
		pts[1].Features.NegRisk = true
		snaps = append(snaps, SnapshotAtT{T: tt, Markets: pts})
	}

	bc := DefaultBacktestConfig()
	bc.BoardN = 1
	bc.Weights = w
	bc.Cfg.Horizons = []string{"1h"}
	labels, stats := RunBacktest(snaps, prices, bc)
	require.NotEmpty(t, labels)
	require.Greater(t, stats.BoardN, 0)

	var cand int
	for _, l := range labels {
		if l.Policy == "candidate" {
			cand++
		}
	}
	require.Greater(t, cand, 0)

	m := BuildMetrics(labels, "1h")
	require.Greater(t, m.N, 0)
	require.Contains(t, m.Baselines, "activity_stage1")
}

func TestFinalizeSurface_PromoteAndParity(t *testing.T) {
	cfg := DefaultConfig()
	s := robustSurface()
	s.PolicyParity = PolicyParityScanBoard
	require.NoError(t, FinalizeSurface(s, cfg))
	require.True(t, s.OK)
	require.True(t, s.PromoteEligible)
	require.Contains(t, s.GatesPassed, GatePolicyParity)
	require.Contains(t, s.GatesPassed, GatePromoteEligible)

	s2 := robustSurface()
	s2.PolicyParity = PolicyParityScanBoard
	s2.Metrics.Overall.AfterCostReturnBps = -5
	s2.Metrics.Baselines["volume_top_n"] = HorizonMetrics{AfterCostReturnBps: -50, N: 100}
	s2.Metrics.Baselines["activity_stage1"] = HorizonMetrics{AfterCostReturnBps: -40, N: 100}
	s2.Metrics.Baselines["random_board"] = HorizonMetrics{AfterCostReturnBps: -60, N: 100}
	require.NoError(t, FinalizeSurface(s2, cfg))
	require.True(t, s2.OK, "protocol ok with negative after-cost")
	require.False(t, s2.PromoteEligible)
	require.Contains(t, s2.GatesFailed, GatePromoteEligible)

	s3 := robustSurface()
	s3.PolicyParity = PolicyParityScoreProxy
	require.NoError(t, FinalizeSurface(s3, cfg))
	require.False(t, s3.OK, "proxy parity must fail protocol")
	require.False(t, s3.PromoteEligible)
	require.Contains(t, s3.GatesFailed, GatePolicyParity)
}

func TestDecisionTimes_Stride(t *testing.T) {
	end := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	bc := BacktestConfig{Lookback: 10 * time.Hour, Bucket: time.Hour, Stride: 2, End: end}
	ts := DecisionTimes(bc, time.Hour)
	require.NotEmpty(t, ts)
}

func TestFillParamsFromCost_MatchesTotal(t *testing.T) {
	c := edge.ComputeCost(edge.CostInput{
		BestBid: 0.49, BestAsk: 0.51, BidDepth: 100, AskDepth: 100,
		SizeUSD: 100, TakerFeeBps: 10, ImpactCoeff: 50,
	})
	fp := FillParamsFromCost(c)
	require.InDelta(t, c.TotalCostBps, fp.TotalCostBps(), 1e-9)
}

package edge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func baseFeatures(spreadWide bool) FeatureVector {
	bid, ask := 0.49, 0.51
	if spreadWide {
		bid, ask = 0.40, 0.60
	}
	f := FeatureVector{
		BestBid:    bid,
		BestAsk:    ask,
		BidDepth:   500,
		AskDepth:   500,
		Imbalance:  0.55,
		AbsRet5m:   0.02,
		OneDayAbs:  0.03,
		OI:         10000,
		DOI1h:      500,
		TTRHours:   200,
		Volume24hr: 50000,
	}
	f.FillBookDerived(10)
	return f
}

func TestScore_WideSpreadLowersEdge(t *testing.T) {
	w := DefaultWeights()
	tight := Score(ScoreInput{Features: baseFeatures(false)}, w)
	wide := Score(ScoreInput{Features: baseFeatures(true)}, w)
	require.Greater(t, tight.EdgeBps, wide.EdgeBps)
	require.Greater(t, wide.Cost.HalfSpreadBps, tight.Cost.HalfSpreadBps)
}

func TestScore_WeightFlipChangesOrder(t *testing.T) {
	a := baseFeatures(false)
	a.AbsRet5m = 0.05
	a.NegRiskResidualBps = 0

	b := baseFeatures(false)
	b.AbsRet5m = 0.001
	b.OneDayAbs = 0.001
	b.NegRiskResidualBps = 400
	b.NegRisk = true

	wVol := DefaultWeights()
	wVol.WVol = 0.9
	wVol.WNegRisk = 0.05
	wVol.WOI = 0.01
	wVol.WImbalance = 0.01
	wVol.WTTR = 0.01
	wVol.WFlow = 0.01
	wVol.WActivity = 0.01

	wNR := DefaultWeights()
	wNR.WVol = 0.05
	wNR.WNegRisk = 0.9
	wNR.WOI = 0.01
	wNR.WImbalance = 0.01
	wNR.WTTR = 0.01
	wNR.WFlow = 0.01
	wNR.WActivity = 0.01

	sa := Score(ScoreInput{Features: a}, wVol)
	sb := Score(ScoreInput{Features: b}, wVol)
	require.Greater(t, sa.EdgeBps, sb.EdgeBps, "vol-weighted should prefer high-vol A")

	sa2 := Score(ScoreInput{Features: a}, wNR)
	sb2 := Score(ScoreInput{Features: b}, wNR)
	require.Greater(t, sb2.EdgeBps, sa2.EdgeBps, "neg-risk-weighted should prefer high residual B")
}

func TestScore_FairValuePath(t *testing.T) {
	f := baseFeatures(false)
	fv := 0.60
	r := Score(ScoreInput{Features: f, FairValue: &fv, FVSource: "test"}, DefaultWeights())
	require.Equal(t, "fair_value", r.Path)
	require.NotNil(t, r.ModelEdgeBps)
	require.Greater(t, r.OpportunityBps, 900.0)
	require.Less(t, r.EdgeBps, r.OpportunityBps)
}

func TestScore_ProxyPath(t *testing.T) {
	r := Score(ScoreInput{Features: baseFeatures(false)}, DefaultWeights())
	require.Equal(t, "proxy", r.Path)
	require.Nil(t, r.ModelEdgeBps)
	require.NotEmpty(t, r.KeyFeatures)
	require.Contains(t, r.StrategyTags, "standalone")
}

func TestRankByEdge(t *testing.T) {
	low := baseFeatures(true)
	high := baseFeatures(false)
	high.AbsRet5m = 0.08
	ranked := RankByEdge([]ScoreInput{
		{Features: low},
		{Features: high},
	}, DefaultWeights())
	require.Len(t, ranked, 2)
	require.GreaterOrEqual(t, ranked[0].Result.EdgeBps, ranked[1].Result.EdgeBps)
}

func TestGroupResidual(t *testing.T) {
	bps, incomplete := GroupResidual([]GroupLeg{
		{ConditionID: "a", Mid: 0.40},
		{ConditionID: "b", Mid: 0.45},
		{ConditionID: "c", Mid: 0.20},
	})
	require.False(t, incomplete)
	require.InDelta(t, 500, bps, 1e-6)

	_, incomplete = GroupResidual([]GroupLeg{{Mid: 0.5}})
	require.True(t, incomplete)
}

func TestParseWeightsYAML(t *testing.T) {
	w, err := ParseWeightsYAML([]byte(`
name: aggressive_vol
w_vol: 0.8
w_oi: 0.05
impact_cap_bps: 40
`))
	require.NoError(t, err)
	require.Equal(t, "aggressive_vol", w.Name)
	require.InDelta(t, 0.8, w.WVol, 1e-9)
	require.InDelta(t, 40, w.ImpactCapBps, 1e-9)
	require.InDelta(t, 300, w.MaxFeatureAgeSec, 1e-9)
}

func TestNegRiskResidualBpsFromMids(t *testing.T) {
	require.InDelta(t, 0, NegRiskResidualBpsFromMids([]float64{0.5, 0.5}), 1e-9)
	require.InDelta(t, 1000, NegRiskResidualBpsFromMids([]float64{0.6, 0.5}), 1e-9)
}

func TestLoadWeightsFile_Default(t *testing.T) {
	w, err := LoadWeightsFile("configs/strategies/default.yaml")
	if err != nil {
		// Allow running tests from package dir
		w, err = LoadWeightsFile("../../configs/strategies/default.yaml")
	}
	require.NoError(t, err)
	require.Equal(t, "default", w.Name)
	require.InDelta(t, 0.30, w.WVol, 1e-9)
}

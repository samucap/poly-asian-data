package edge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNegRiskComplement_TwoLeg(t *testing.T) {
	p := NegRiskComplement{}
	// Leg A mid 0.40, B mid 0.55 → FV_A = 1-0.55 = 0.45
	q := p.Quote(FairValueInput{
		ConditionID: "a",
		GroupMids:   map[string]float64{"a": 0.40, "b": 0.55},
	})
	require.NotNil(t, q)
	require.InDelta(t, 0.45, q.FairValue, 1e-9)
	require.Equal(t, fvSourceNegRisk, q.Source)
	require.Greater(t, q.Confidence, 0.0)
}

func TestNegRiskComplement_ThreeLegSumOne(t *testing.T) {
	p := NegRiskComplement{}
	// mids 0.2, 0.3, 0.5 → FV for first = 1-0.3-0.5 = 0.2
	q := p.Quote(FairValueInput{
		ConditionID:    "a",
		GroupMids:      map[string]float64{"a": 0.2, "b": 0.3, "c": 0.5},
		KnownGroupSize: 3,
	})
	require.NotNil(t, q)
	require.InDelta(t, 0.2, q.FairValue, 1e-9)
}

func TestNegRiskComplement_SumUnderOne(t *testing.T) {
	p := NegRiskComplement{}
	// others sum 0.5 → FV = 0.5; self mid 0.3 → cheap
	q := p.Quote(FairValueInput{
		ConditionID: "a",
		GroupMids:   map[string]float64{"a": 0.30, "b": 0.25, "c": 0.25},
	})
	require.NotNil(t, q)
	require.InDelta(t, 0.50, q.FairValue, 1e-9)
}

func TestNegRiskComplement_OnlySelf_NoQuote(t *testing.T) {
	p := NegRiskComplement{}
	q := p.Quote(FairValueInput{
		ConditionID: "a",
		GroupMids:   map[string]float64{"a": 0.5},
	})
	require.Nil(t, q)
}

func TestNegRiskComplement_Clamp(t *testing.T) {
	p := NegRiskComplement{}
	// others sum 0 → FV clamped to lo? 1-0 = 1 → clamp 0.99
	// need at least one other mid > 0; use tiny other
	q := p.Quote(FairValueInput{
		ConditionID: "a",
		GroupMids:   map[string]float64{"a": 0.5, "b": 0.001},
	})
	require.NotNil(t, q)
	require.LessOrEqual(t, q.FairValue, fvClampHi)
	require.GreaterOrEqual(t, q.FairValue, fvClampLo)
}

func TestFVChain_FirstWins(t *testing.T) {
	chain := DefaultFVChain(0.4)
	q := chain.Resolve(FairValueInput{
		ConditionID: "a",
		GroupMids:   map[string]float64{"a": 0.4, "b": 0.55},
	})
	require.NotNil(t, q)
	require.Equal(t, fvSourceNegRisk, q.Source)
}

func TestFVChain_Disabled(t *testing.T) {
	chain := DefaultFVChain(0.4)
	chain.Enabled = false
	q := chain.Resolve(FairValueInput{
		ConditionID: "a",
		GroupMids:   map[string]float64{"a": 0.4, "b": 0.55},
	})
	require.Nil(t, q)
}

func TestFVChain_LowConfidenceRejected(t *testing.T) {
	// External alone with underlying would be 0.35 conf < 0.4
	chain := FVChain{
		Providers:     []FairValueProvider{ExternalUnderlying{}},
		MinConfidence: 0.4,
		Enabled:       true,
	}
	open := 100.0
	spot := 110.0
	q := chain.Resolve(FairValueInput{
		ConditionID: "x",
		Underlying:  &spot,
		WindowOpen:  &open,
	})
	require.Nil(t, q)
}

func TestScore_FVPathIdentity(t *testing.T) {
	f := baseFeatures(false)
	// mid ~0.50
	fv := 0.60
	w := DefaultWeights()
	w.ModelBufferBps = 20
	r := Score(ScoreInput{Features: f, FairValue: &fv, FVSource: "test"}, w)
	require.Equal(t, "fair_value", r.Path)
	require.NotNil(t, r.ModelEdgeBps)
	// raw = 1000 bps; edge = 1000 - cost - 20
	require.InDelta(t, *r.ModelEdgeBps, r.EdgeBps, 1e-9)
	expected := (0.60-r.Cost.Mid)*10_000 - r.Cost.TotalCostBps - 20
	require.InDelta(t, expected, r.EdgeBps, 1e-6)
}

func TestScore_BufferReducesEdge(t *testing.T) {
	f := baseFeatures(false)
	fv := 0.60
	w0 := DefaultWeights()
	w0.ModelBufferBps = 0
	w50 := DefaultWeights()
	w50.ModelBufferBps = 50
	r0 := Score(ScoreInput{Features: f, FairValue: &fv}, w0)
	r50 := Score(ScoreInput{Features: f, FairValue: &fv}, w50)
	require.InDelta(t, 50, r0.EdgeBps-r50.EdgeBps, 1e-6)
}

func TestDifferential_CheapVsRichSameFV(t *testing.T) {
	// Same FV 0.45; A mid 0.30 (cheap), B mid 0.50 (rich)
	w := DefaultWeights()
	w.ModelBufferBps = 0
	// Tight identical books except mid via bid/ask
	a := FeatureVector{BestBid: 0.29, BestAsk: 0.31, BidDepth: 500, AskDepth: 500}
	a.FillBookDerived(10)
	b := FeatureVector{BestBid: 0.49, BestAsk: 0.51, BidDepth: 500, AskDepth: 500}
	b.FillBookDerived(10)
	fv := 0.45
	sa := Score(ScoreInput{Features: a, FairValue: &fv}, w)
	sb := Score(ScoreInput{Features: b, FairValue: &fv}, w)
	require.Greater(t, sa.EdgeBps, sb.EdgeBps, "cheap vs FV should rank above rich")
}

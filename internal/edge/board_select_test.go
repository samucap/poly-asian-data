package edge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSelectBoard_DropsExtremeMid(t *testing.T) {
	w := DefaultWeights()
	w.DropExtremePrice = true
	w.DropMissingBook = false
	w.FVEnabled = false

	cands := []BoardCandidate{
		{Features: FeatureVector{ConditionID: "x", TokenID: "tx", Mid: 0.02, BestBid: 0.01, BestAsk: 0.03, MissingBook: false}, TieBreak: 1},
		{Features: FeatureVector{ConditionID: "y", TokenID: "ty", Mid: 0.50, BestBid: 0.49, BestAsk: 0.51, MissingBook: false, VolumeProxy: 1e5}, TieBreak: 1},
	}
	// Fill book derived
	for i := range cands {
		cands[i].Features.FillBookDerived(10)
	}
	out := SelectBoard(cands, w, 10)
	require.Len(t, out, 1)
	require.Equal(t, "y", out[0].Candidate.Features.ConditionID)
}

func TestSelectBoard_FVPathWhenGroupMids(t *testing.T) {
	w := DefaultWeights()
	w.DropExtremePrice = false
	w.DropMissingBook = false
	w.FVEnabled = true

	// Two-leg group: mids 0.4 + 0.5 → residual 0.1; FV for A = 1-0.5 = 0.5
	cands := []BoardCandidate{
		{
			Features: FeatureVector{
				ConditionID: "A", TokenID: "ta", Mid: 0.40, BestBid: 0.39, BestAsk: 0.41,
				NegRisk: true, VolumeProxy: 5e4,
			},
			GroupMids:      map[string]float64{"A": 0.40, "B": 0.50},
			KnownGroupSize: 2,
			TieBreak:       1,
		},
	}
	cands[0].Features.FillBookDerived(10)
	out := SelectBoard(cands, w, 5)
	require.Len(t, out, 1)
	require.Equal(t, "fair_value", out[0].Result.Path)
	require.Equal(t, "neg_risk_complement", out[0].Result.FVSource)
}

func TestSeriesActivityProxy_NonZero(t *testing.T) {
	require.Greater(t, SeriesActivityProxy(24, 0.05), 0.0)
	require.Greater(t, activityFamily(FeatureVector{VolumeProxy: SeriesActivityProxy(24, 0.05)}), 0.0)
}

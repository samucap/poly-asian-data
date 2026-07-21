package edge

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComputeCost_HalfSpreadAndFees(t *testing.T) {
	c := ComputeCost(CostInput{
		BestBid:       0.48,
		BestAsk:       0.52,
		BidDepth:      1000,
		AskDepth:      1000,
		SizeUSD:       100,
		TakerFeeBps:   10,
		NegRiskFeeBps: 5,
		ImpactCoeff:   50,
		ImpactCapBps:  25,
	})
	require.True(t, c.HasBook)
	require.InDelta(t, 0.50, c.Mid, 1e-9)
	// half-spread bps = 1e4 * (ask-bid)/(2*mid) = 1e4 * 0.04/1 = 400
	require.InDelta(t, 400, c.HalfSpreadBps, 1e-6)
	require.InDelta(t, 10, c.FeeBps, 1e-9)
	require.InDelta(t, 5, c.NegRiskFeeBps, 1e-9)
	require.Greater(t, c.TotalCostBps, c.HalfSpreadBps+c.FeeBps+c.NegRiskFeeBps-1)
	require.Greater(t, c.CapacityUSD, 0.0)
}

func TestComputeCost_MissingBook(t *testing.T) {
	c := ComputeCost(CostInput{TakerFeeBps: 15})
	require.False(t, c.HasBook)
	require.InDelta(t, 15, c.TotalCostBps, 1e-9)
}

func TestComputeCost_ImpactWalk(t *testing.T) {
	c := ComputeCost(CostInput{
		BestBid: 0.40,
		BestAsk: 0.42,
		Levels: []BookLevel{
			{Price: 0.42, Size: 100},
			{Price: 0.45, Size: 500},
		},
		SizeUSD:     50,
		ImpactCoeff: 50,
	})
	require.True(t, c.HasBook)
	require.GreaterOrEqual(t, c.ImpactBps, 0.0)
}

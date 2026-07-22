package eval

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAfterCostReturnBpsAction_SignFromEdge(t *testing.T) {
	// mid 0.50 → 0.60 = +1000 raw bps; costs 50
	p := FillParams{TotalOverride: ptrF(50)}
	long := AfterCostReturnBpsAction(0.5, 0.6, p, ActionLongYes, 10)
	require.InDelta(t, 950, long, 1e-6)

	// positive edge → same as long
	pos := AfterCostReturnBpsAction(0.5, 0.6, p, ActionSignFromEdge, 100)
	require.InDelta(t, 950, pos, 1e-6)

	// negative edge → flip raw → -1000 - 50
	neg := AfterCostReturnBpsAction(0.5, 0.6, p, ActionSignFromEdge, -50)
	require.InDelta(t, -1050, neg, 1e-6)
}

func ptrF(v float64) *float64 { return &v }

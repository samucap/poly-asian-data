package eval

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRawMoveBps(t *testing.T) {
	require.InDelta(t, 100, RawMoveBps(0.50, 0.51), 1e-9)
	require.InDelta(t, -200, RawMoveBps(0.50, 0.48), 1e-9)
	require.True(t, math.IsNaN(RawMoveBps(0, 0.5)))
}

func TestAfterCostReturnBps_FeeSlip(t *testing.T) {
	// +100 bps raw move, fee 10 + slip 5 = 15 → after cost 85
	p := FillParams{FeeBps: 10, SlipBps: 5, IncludeSpread: false}
	got := AfterCostReturnBps(0.50, 0.51, p)
	require.InDelta(t, 85, got, 1e-9)
	require.True(t, Hit(got))
}

func TestAfterCostReturnBps_WithHalfSpread(t *testing.T) {
	// mid 0.5, bid 0.49 ask 0.51 → half-spread = 200 bps absolute
	// wait: spread=0.02, half=0.01, half_spread_bps = 10000*0.01/0.5 = 200
	hs := HalfSpreadBpsFromBook(0.49, 0.51)
	require.InDelta(t, 200, hs, 1e-6)
	p := FillParams{FeeBps: 10, SlipBps: 5, IncludeSpread: true, HalfSpreadBps: hs}
	// raw +100 − 215 = −115
	got := AfterCostReturnBps(0.50, 0.51, p)
	require.InDelta(t, -115, got, 1e-6)
	require.False(t, Hit(got))
}

func TestHalfSpreadBpsFromSpread(t *testing.T) {
	// spread 0.02 at mid 0.5 → half 0.01 → 200 bps
	require.InDelta(t, 200, HalfSpreadBpsFromSpread(0.02, 0.5), 1e-9)
	require.Equal(t, 0.0, HalfSpreadBpsFromSpread(0, 0.5))
}

func TestVanityZeroCostBeatsFilled(t *testing.T) {
	// Same small move: zero-cost "vanity" looks great; honest fill+spread flips sign.
	raw := AfterCostReturnBps(0.50, 0.501, FillParams{}) // +10 bps
	p := DefaultFillParams()
	p.HalfSpreadBps = HalfSpreadBpsFromBook(0.49, 0.51) // 200 bps half-spread
	honest := AfterCostReturnBps(0.50, 0.501, p)
	require.True(t, Hit(raw))
	require.False(t, Hit(honest), "fee+slip+half-spread should wipe a 10 bps move")
}

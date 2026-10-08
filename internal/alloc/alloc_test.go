package alloc

import (
	"testing"
	"time"

	"github.com/samucap/poly-asian-data/internal/risk"
	"github.com/stretchr/testify/require"
)

func TestRun_TopKAndOutranked(t *testing.T) {
	t0 := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	cfg := risk.DefaultConfig()
	cfg.MaxPositions = 2
	cfg.MaxGrossUSD = 1e9
	in := Input{
		Risk: cfg,
		Names: []Name{
			{Time: t0, ConditionID: "weak", TokenID: "tw", EdgeBps: 10, Conviction: 0.1, SizeUSD: 80},
			{Time: t0, ConditionID: "strong", TokenID: "ts", EdgeBps: 400, Conviction: 0.9, SizeUSD: 80},
			{Time: t0, ConditionID: "mid", TokenID: "tm", EdgeBps: 80, Conviction: 0.5, SizeUSD: 80},
			{Time: t0, ConditionID: "also", TokenID: "ta", EdgeBps: 50, Conviction: 0.4, SizeUSD: 80},
			{Time: t0, ConditionID: "noise", TokenID: "tn", EdgeBps: 12, Conviction: 0.1, SizeUSD: 80},
		},
	}
	got := Run(in)
	require.Len(t, got.Taken, 2)
	require.Equal(t, "strong", got.Taken[0].ConditionID)
	require.Equal(t, "mid", got.Taken[1].ConditionID)
	nOut := 0
	for _, r := range got.Rejected {
		if r.Reason == ReasonOutranked {
			nOut++
		}
	}
	require.Equal(t, 3, nOut)
}

func TestRun_WorthinessDrops(t *testing.T) {
	t0 := time.Now().UTC()
	cfg := risk.DefaultConfig()
	cfg.MaxPositions = 10
	cfg.MaxGrossUSD = 1e9
	got := Run(Input{
		Risk: cfg,
		Worth: Worthiness{MinEdgeBps: 25, MaxSpreadBps: 200},
		Names: []Name{
			{Time: t0, ConditionID: "thin", EdgeBps: 10, SpreadBps: 50, SizeUSD: 50},
			{Time: t0, ConditionID: "wide", EdgeBps: 80, SpreadBps: 400, SizeUSD: 50},
			{Time: t0, ConditionID: "ok", EdgeBps: 80, SpreadBps: 40, SizeUSD: 50, TokenID: "tok"},
		},
	})
	require.Len(t, got.Taken, 1)
	require.Equal(t, "ok", got.Taken[0].ConditionID)
	require.Equal(t, ReasonUnworthy, got.Rejected[0].Reason)
}

func TestRun_EmptyBoard(t *testing.T) {
	got := Run(Input{Risk: risk.DefaultConfig()})
	require.Empty(t, got.Taken)
	require.Empty(t, got.Rejected)
}

func TestRun_AlreadyOpen(t *testing.T) {
	t0 := time.Now().UTC()
	cfg := risk.DefaultConfig()
	cfg.MaxPositions = 5
	cfg.MaxGrossUSD = 1e9
	got := Run(Input{
		Risk: cfg,
		Open: []string{"held"},
		Names: []Name{
			{Time: t0, ConditionID: "held", TokenID: "th", EdgeBps: 400, Conviction: 1, SizeUSD: 50},
			{Time: t0, ConditionID: "new", TokenID: "tn", EdgeBps: 100, Conviction: 0.5, SizeUSD: 50},
		},
	})
	require.Len(t, got.Taken, 1)
	require.Equal(t, "new", got.Taken[0].ConditionID)
	require.Equal(t, ReasonAlreadyOpen, got.Rejected[0].Reason)
}

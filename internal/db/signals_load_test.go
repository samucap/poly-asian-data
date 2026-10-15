package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNormalizeSignalOrder(t *testing.T) {
	require.Equal(t, SignalOrderNewest, NormalizeSignalOrder(""))
	require.Equal(t, SignalOrderNewest, NormalizeSignalOrder("newest"))
	require.Equal(t, SignalOrderOldest, NormalizeSignalOrder("oldest"))
	require.Equal(t, SignalOrderNewest, NormalizeSignalOrder("bogus"))
}

func TestReverseSignalRows_ChronologicalAfterNewestLoad(t *testing.T) {
	t0 := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	rows := []SignalRow{
		{Time: t0.Add(2 * time.Hour), SignalID: "late"},
		{Time: t0.Add(time.Hour), SignalID: "mid"},
		{Time: t0, SignalID: "early"},
	}
	reverseSignalRows(rows)
	require.Equal(t, "early", rows[0].SignalID)
	require.Equal(t, "mid", rows[1].SignalID)
	require.Equal(t, "late", rows[2].SignalID)
	require.True(t, rows[0].Time.Before(rows[1].Time))
}

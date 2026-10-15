package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGroupSnapshotsBySelectedAt(t *testing.T) {
	t0 := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)
	rows := []EdgeBoardSnapshotRow{
		{SelectedAt: t0, ConditionID: "a"},
		{SelectedAt: t0, ConditionID: "b"},
		{SelectedAt: t1, ConditionID: "a"},
	}
	g := GroupSnapshotsBySelectedAt(rows)
	require.Len(t, g, 2)
	require.Len(t, g[0], 2)
	require.Len(t, g[1], 1)
	require.Empty(t, GroupSnapshotsBySelectedAt(nil))
}

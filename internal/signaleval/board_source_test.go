package signaleval

import (
	"testing"
	"time"

	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/risk"
	"github.com/stretchr/testify/require"
)

func TestAllocateFromBoardSnapshots_WholeBoardNotFirehose(t *testing.T) {
	t0 := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	ptr := func(v float64) *float64 { return &v }
	// 3 board times × 4 names. One name never "fired" as a standalone ENTER —
	// it only exists on the snapshot (quiet).
	var rows []db.EdgeBoardSnapshotRow
	for i := 0; i < 3; i++ {
		ts := t0.Add(time.Duration(i) * time.Minute)
		rows = append(rows,
			db.EdgeBoardSnapshotRow{SelectedAt: ts, ConditionID: "strong", TokenID: "ts", Rank: 1, EdgeBps: ptr(400), Strategy: "default"},
			db.EdgeBoardSnapshotRow{SelectedAt: ts, ConditionID: "mid", TokenID: "tm", Rank: 2, EdgeBps: ptr(80), Strategy: "default"},
			db.EdgeBoardSnapshotRow{SelectedAt: ts, ConditionID: "quiet", TokenID: "tq", Rank: 3, EdgeBps: ptr(70), Strategy: "default"},
			db.EdgeBoardSnapshotRow{SelectedAt: ts, ConditionID: "weak", TokenID: "tw", Rank: 4, EdgeBps: ptr(10), Strategy: "default"},
		)
	}
	cfg := risk.DefaultConfig()
	cfg.MaxPositions = 3
	cfg.MaxGrossUSD = 1e9
	cfg.MaxPositionUSD = 80
	sigs, meta := AllocateFromBoardSnapshots(rows, cfg, WorthinessFilter{MinEdgeBps: 25})
	require.Equal(t, 3, meta.SnapshotTimes)
	require.Equal(t, 12, meta.NamesSeen)
	require.Len(t, sigs, 9) // 3 per time (weak fails worthiness)
	// quiet never existed as a firehose ENTER; it is still allocated because it is on the board.
	require.Equal(t, 3, countCond(sigs, "quiet"))
	require.Equal(t, 0, countCond(sigs, "weak"))
	require.Equal(t, 3, countCond(sigs, "strong"))
}

func countCond(sigs []SignalIn, id string) int {
	n := 0
	for _, s := range sigs {
		if s.ConditionID == id {
			n++
		}
	}
	return n
}

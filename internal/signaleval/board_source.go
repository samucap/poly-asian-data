package signaleval

import (
	"math"
	"time"

	"github.com/samucap/poly-asian-data/internal/alloc"
	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/risk"
)

const snapshotConvictionScale = 200.0

// BoardAllocMeta is diagnostics for --source board replay.
type BoardAllocMeta struct {
	SnapshotTimes int
	NamesSeen     int
	Taken         int
	Rejected      int
	Source        string
}

// AllocateFromBoardSnapshots runs capital allocation on each published board
// (every name at selected_at), not on a firehose of paper ENTERs.
func AllocateFromBoardSnapshots(rows []db.EdgeBoardSnapshotRow, rcfg risk.Config, worth WorthinessFilter) ([]SignalIn, BoardAllocMeta) {
	meta := BoardAllocMeta{Source: "board_snapshots"}
	groups := db.GroupSnapshotsBySelectedAt(rows)
	meta.SnapshotTimes = len(groups)
	out := make([]SignalIn, 0)
	horizon := 3600
	size := rcfg.MaxPositionUSD
	if size <= 0 {
		size = 25
	}
	for _, g := range groups {
		names := make([]alloc.Name, 0, len(g))
		for _, r := range g {
			n := snapshotToName(r, size, horizon)
			names = append(names, n)
			meta.NamesSeen++
		}
		res := alloc.Run(alloc.Input{Names: names, Risk: rcfg, Worth: alloc.Worthiness{
			MinEdgeBps: worth.MinEdgeBps, MaxSpreadBps: worth.MaxSpreadBps, RequireFV: worth.RequireFV,
		}})
		meta.Taken += len(res.Taken)
		meta.Rejected += len(res.Rejected)
		for _, n := range res.Taken {
			out = append(out, signalFromName(n))
		}
	}
	return out, meta
}

func signalFromName(n alloc.Name) SignalIn {
	side := n.Side
	if side == "" {
		if n.EdgeBps >= 0 {
			side = "BUY"
		} else {
			side = "SELL"
		}
	}
	return SignalIn{
		Time:              n.Time,
		SignalID:          n.ConditionID + ":" + n.Time.UTC().Format(time.RFC3339Nano),
		Strategy:          n.Strategy,
		StrategyVersionID: n.StrategyVersionID,
		ConditionID:       n.ConditionID,
		TokenID:           n.TokenID,
		MarketID:          n.MarketID,
		Side:              side,
		Action:            "ENTER",
		EdgeBps:           n.EdgeBps,
		Conviction:        n.Conviction,
		Urgency:           n.Urgency,
		SizeUSD:           n.SizeUSD,
		CapacityUSD:       n.CapacityUSD,
		CostBps:           n.CostBps,
		Mid:               n.Mid,
		HorizonSec:        n.HorizonSec,
		SpreadBps:         n.SpreadBps,
		FVSource:          n.FVSource,
		ScorePath:         n.ScorePath,
	}
}

func snapshotToName(r db.EdgeBoardSnapshotRow, sizeUSD float64, horizonSec int) alloc.Name {
	edge := 0.0
	if r.EdgeBps != nil {
		edge = *r.EdgeBps
	}
	conv := math.Abs(edge) / snapshotConvictionScale
	if conv > 1 {
		conv = 1
	}
	side := "BUY"
	if edge < 0 {
		side = "SELL"
	}
	return alloc.Name{
		Time:        r.SelectedAt,
		ConditionID: r.ConditionID,
		TokenID:     r.TokenID,
		Side:        side,
		EdgeBps:     edge,
		Conviction:  conv,
		SizeUSD:     sizeUSD,
		Strategy:    r.Strategy,
		HorizonSec:  horizonSec,
		ScorePath:   "board_snapshot",
	}
}

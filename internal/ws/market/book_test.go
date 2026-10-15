package market

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseBookAndApply(t *testing.T) {
	raw := []byte(`{
		"event_type": "book",
		"asset_id": "tok1",
		"market": "m1",
		"bids": [{"price": "0.48", "size": "100"}, {"price": "0.47", "size": "50"}],
		"asks": [{"price": "0.52", "size": "80"}, {"price": "0.53", "size": "20"}],
		"timestamp": "1750428146322"
	}`)
	evs, err := ParseMessage(raw)
	require.NoError(t, err)
	require.Len(t, evs, 1)
	require.Equal(t, EventBook, evs[0].Type)

	store := NewBookStore()
	require.True(t, store.Apply(evs[0]))
	b, ok := store.Snapshot("tok1")
	require.True(t, ok)
	require.InDelta(t, 0.48, b.BestBid, 1e-9)
	require.InDelta(t, 0.52, b.BestAsk, 1e-9)
	require.InDelta(t, 0.50, b.Mid(), 1e-9)
	require.True(t, b.Dirty)
}

func TestPriceChangeSizeZeroRemovesLevel(t *testing.T) {
	store := NewBookStore()
	store.Apply(ParsedEvent{
		Type: EventBook, AssetID: "t",
		Bids: []Level{{Price: 0.5, Size: 10}},
		Asks: []Level{{Price: 0.55, Size: 10}},
		Timestamp: time.Now().UTC(),
	})
	store.Apply(ParsedEvent{
		Type: EventPriceChange, AssetID: "t",
		Side: "BUY", Price: 0.5, Size: 0,
		Timestamp: time.Now().UTC(),
	})
	b, ok := store.Snapshot("t")
	require.True(t, ok)
	require.Equal(t, 0.0, b.BestBid)
}

func TestDiffSubscriptions(t *testing.T) {
	d := DiffSubscriptions([]string{"a", "b"}, []string{"b", "c"})
	require.Equal(t, []string{"c"}, d.Subscribe)
	require.Equal(t, []string{"a"}, d.Unsubscribe)
}

func TestTakeDirtyBatch(t *testing.T) {
	store := NewBookStore()
	store.Apply(ParsedEvent{
		Type: EventBook, AssetID: "a",
		Bids: []Level{{Price: 0.4, Size: 1}}, Asks: []Level{{Price: 0.6, Size: 1}},
	})
	store.Apply(ParsedEvent{
		Type: EventBook, AssetID: "b",
		Bids: []Level{{Price: 0.3, Size: 1}}, Asks: []Level{{Price: 0.7, Size: 1}},
	})
	require.Equal(t, 2, store.DirtyCount())
	got := store.TakeDirty(1)
	require.Len(t, got, 1)
	require.Equal(t, 1, store.DirtyCount())
}

func TestPeekDirtySurvivesUntilMarkFlushed(t *testing.T) {
	store := NewBookStore()
	store.Apply(ParsedEvent{
		Type: EventBook, AssetID: "a",
		Bids: []Level{{Price: 0.4, Size: 1}}, Asks: []Level{{Price: 0.6, Size: 1}},
	})
	require.Equal(t, 1, store.DirtyCount())
	peek := store.PeekDirty(10)
	require.Len(t, peek, 1)
	// Simulated upsert failure: still dirty
	require.Equal(t, 1, store.DirtyCount())
	peek2 := store.PeekDirty(10)
	require.Len(t, peek2, 1)
	store.MarkFlushed([]string{"a"})
	require.Equal(t, 0, store.DirtyCount())
	require.Empty(t, store.PeekDirty(10))
}

func TestParseLastTrade(t *testing.T) {
	raw := []byte(`{
		"asset_id": "x",
		"event_type": "last_trade_price",
		"market": "m",
		"price": "0.456",
		"side": "BUY",
		"size": "10",
		"timestamp": "1750428146322"
	}`)
	evs, err := ParseMessage(raw)
	require.NoError(t, err)
	require.Len(t, evs, 1)
	require.InDelta(t, 0.456, evs[0].LastTradePrice, 1e-9)
}

func TestCapAssets(t *testing.T) {
	require.Equal(t, []string{"a", "b"}, CapAssets([]string{"a", "b", "c"}, 2))
}

func TestParseMarketResolved(t *testing.T) {
	raw := []byte(`{
		"event_type": "market_resolved",
		"id": "1031769",
		"market": "0xabc",
		"winning_asset_id": "tokYES",
		"winning_outcome": "Yes",
		"assets_ids": ["tokYES", "tokNO"],
		"timestamp": "1766790415550"
	}`)
	evs, err := ParseMessage(raw)
	require.NoError(t, err)
	require.Len(t, evs, 1)
	require.Equal(t, EventMarketResolved, evs[0].Type)
	require.Equal(t, "0xabc", evs[0].MarketID)
	require.Equal(t, "tokYES", evs[0].WinningAssetID)
	require.Equal(t, "Yes", evs[0].WinningOutcome)
	require.Equal(t, []string{"tokYES", "tokNO"}, evs[0].ResolvedAssetIDs)
}

func TestResolveQueueDedupeAndTake(t *testing.T) {
	q := NewResolveQueue()
	t0 := time.Now().UTC()
	q.Enqueue(MarketResolution{ConditionID: "c1", WinningOutcome: "Yes", ResolvedAt: t0})
	q.Enqueue(MarketResolution{ConditionID: "c1", WinningOutcome: "No", ResolvedAt: t0.Add(time.Second)}) // newer wins
	q.Enqueue(MarketResolution{ConditionID: "c2", WinningOutcome: "Yes", ResolvedAt: t0})
	require.Equal(t, 2, q.Len())
	all := q.TakeAll()
	require.Len(t, all, 2)
	require.Equal(t, 0, q.Len())
	var c1 MarketResolution
	for _, r := range all {
		if r.ConditionID == "c1" {
			c1 = r
		}
	}
	require.Equal(t, "No", c1.WinningOutcome)
}

func TestResolveQueuePrefersNewerOnRequeue(t *testing.T) {
	q := NewResolveQueue()
	t0 := time.Now().UTC()
	q.Enqueue(MarketResolution{ConditionID: "c1", WinningOutcome: "Yes", WinningAssetID: "y", ResolvedAt: t0})
	// Simulate: drain for failed batch, concurrent newer event, then requeue older
	batch := q.TakeAll()
	require.Len(t, batch, 1)
	q.Enqueue(MarketResolution{
		ConditionID: "c1", WinningOutcome: "No", WinningAssetID: "n",
		ResolvedAt: t0.Add(time.Minute), AssetIDs: []string{"n", "y"},
	})
	// Stale requeue from failed DB write must not overwrite
	q.Enqueue(batch[0])
	all := q.TakeAll()
	require.Len(t, all, 1)
	require.Equal(t, "No", all[0].WinningOutcome)
	require.Equal(t, "n", all[0].WinningAssetID)
}

func TestRuntimeStatsObserve(t *testing.T) {
	s := NewRuntimeStats()
	s.ObserveMsg(EventBook)
	s.ObserveMsg(EventPriceChange)
	s.AddSignals(3)
	require.Contains(t, s.CompactTypes(), "book=1")
	require.Contains(t, s.Line(StatusInput{Sub: 1, Mem: 1}), "signals=3")
}

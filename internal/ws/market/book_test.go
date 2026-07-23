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
	q.Enqueue(MarketResolution{ConditionID: "c1", WinningOutcome: "Yes"})
	q.Enqueue(MarketResolution{ConditionID: "c1", WinningOutcome: "No"}) // overwrite
	q.Enqueue(MarketResolution{ConditionID: "c2", WinningOutcome: "Yes"})
	require.Equal(t, 2, q.Len())
	all := q.TakeAll()
	require.Len(t, all, 2)
	require.Equal(t, 0, q.Len())
}

func TestRuntimeStatsObserve(t *testing.T) {
	s := NewRuntimeStats()
	s.ObserveMsg(EventBook)
	s.ObserveMsg(EventPriceChange)
	s.AddSignals(3)
	require.Contains(t, s.CompactTypes(), "book=1")
	require.Contains(t, s.Line(StatusInput{Sub: 1, Mem: 1}), "signals=3")
}

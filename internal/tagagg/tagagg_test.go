package tagagg

import (
	"testing"

	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const rootID = "102982"

func sportsCatalog() map[string]*services.PlyMktTag {
	return map[string]*services.PlyMktTag{
		"1":   {ID: "1", Slug: "sports", Label: "Sports", ParentTagID: rootID},
		"2":   {ID: "2", Slug: "politics", Label: "Politics", ParentTagID: rootID},
		"nba": {ID: "nba", Slug: "nba", Label: "NBA", ParentTagID: "1"},
		"nfl": {ID: "nfl", Slug: "nfl", Label: "NFL", ParentTagID: "1"},
	}
}

func tradable(id, cond string, vol, vol24, liq float64) *services.PlyMktMarket {
	return &services.PlyMktMarket{
		ID:              id,
		ConditionID:     cond,
		EnableOrderBook: true,
		AcceptingOrders: true,
		Active:          true,
		Closed:          false,
		VolumeClob:      vol,
		Volume24hrClob:  vol24,
		LiquidityClob:   liq,
	}
}

func TestIsTradable(t *testing.T) {
	assert.True(t, IsTradable(tradable("a", "c", 1, 1, 1)))
	assert.False(t, IsTradable(&services.PlyMktMarket{
		EnableOrderBook: true, AcceptingOrders: true, Active: true, Closed: true,
	}))
	assert.False(t, IsTradable(&services.PlyMktMarket{
		EnableOrderBook: true, AcceptingOrders: true, Active: true, ClosedTime: "2020-01-01",
	}))
	assert.False(t, IsTradable(nil))
}

func TestResolveTopOf(t *testing.T) {
	cat := sportsCatalog()
	assert.Equal(t, "1", ResolveTopOf(cat["nba"], cat, rootID).ID)
	assert.Equal(t, "1", ResolveTopOf(cat["1"], cat, rootID).ID)
	assert.Nil(t, ResolveTopOf(&services.PlyMktTag{ID: "x", ParentTagID: "missing"}, cat, rootID))
}

func TestAggregate_TopAndSubtag(t *testing.T) {
	cat := sportsCatalog()
	m1 := tradable("m1", "c1", 100, 50, 10)
	m2 := tradable("m2", "c2", 200, 80, 20)
	events := []*services.PlyMktEvent{{
		ID:   "e1",
		Tags: []*services.PlyMktTag{{ID: "1", Slug: "sports"}, {ID: "nba", Slug: "nba"}},
		Markets: []*services.PlyMktMarket{m1, m2},
	}}

	res := Aggregate(events, cat, rootID)
	require.Contains(t, res.Tags, "1")
	require.Contains(t, res.Tags, "nba")
	// Second subtag not on event — idle zero.
	assert.Equal(t, 0, res.Tags["nfl"].TotalMarkets)

	assert.Equal(t, 300.0, res.Tags["1"].TotalVol)
	assert.Equal(t, 130.0, res.Tags["1"].TotalVol24hr)
	assert.Equal(t, 30.0, res.Tags["1"].TotalLiq)
	assert.Equal(t, 2, res.Tags["1"].TotalMarkets)

	// Primary leaf is nba (first non-top).
	assert.Equal(t, 300.0, res.Tags["nba"].TotalVol)
	assert.Equal(t, 2, res.Tags["nba"].TotalMarkets)

	assert.Equal(t, 2, res.PoolBySlug["sports"])
	assert.Equal(t, "sports", res.Markets[0].Category)
	assert.Equal(t, "sports", res.CondCategory["c1"])
}

func TestAggregate_MultiTop(t *testing.T) {
	cat := sportsCatalog()
	events := []*services.PlyMktEvent{{
		ID:      "e-cross",
		Tags:    []*services.PlyMktTag{{ID: "1"}, {ID: "2"}},
		Markets: []*services.PlyMktMarket{tradable("m", "c", 1000, 100, 50)},
	}}
	res := Aggregate(events, cat, rootID)
	assert.Equal(t, 1000.0, res.Tags["1"].TotalVol)
	assert.Equal(t, 1000.0, res.Tags["2"].TotalVol)
	assert.Equal(t, 1, res.Tags["1"].TotalMarkets)
	assert.Equal(t, 1, res.Tags["2"].TotalMarkets)
	// First top wins Category slug only.
	assert.Equal(t, "sports", res.Markets[0].Category)
}

func TestAggregate_PrimaryLeafOnlyAmongSubtags(t *testing.T) {
	cat := sportsCatalog()
	events := []*services.PlyMktEvent{{
		ID: "e1",
		Tags: []*services.PlyMktTag{
			{ID: "1"},
			{ID: "nba"},
			{ID: "nfl"},
		},
		Markets: []*services.PlyMktMarket{tradable("m", "c", 100, 50, 10)},
	}}
	res := Aggregate(events, cat, rootID)
	assert.Equal(t, 100.0, res.Tags["1"].TotalVol)
	assert.Equal(t, 100.0, res.Tags["nba"].TotalVol)
	assert.Equal(t, 0.0, res.Tags["nfl"].TotalVol)
	assert.Equal(t, 0, res.Tags["nfl"].TotalMarkets)
}

func TestAggregate_SubtagOnlyResolvesTop(t *testing.T) {
	cat := sportsCatalog()
	m := tradable("m1", "c1", 100, 50, 10)
	events := []*services.PlyMktEvent{{
		ID:      "e1",
		Tags:    []*services.PlyMktTag{{ID: "nba", Slug: "nba"}},
		Markets: []*services.PlyMktMarket{m},
	}}

	res := Aggregate(events, cat, rootID)
	assert.Equal(t, 100.0, res.Tags["1"].TotalVol)
	assert.Equal(t, 100.0, res.Tags["nba"].TotalVol)
	assert.Equal(t, "sports", res.Markets[0].Category)
	assert.Equal(t, 0, res.UnresolvedMarkets)
}

func TestAggregate_DiscoverUnknownUnderTop(t *testing.T) {
	cat := sportsCatalog()
	events := []*services.PlyMktEvent{{
		ID: "e1",
		Tags: []*services.PlyMktTag{
			{ID: "1", Slug: "sports"},
			{ID: "playoffs", Slug: "playoffs", Label: "Playoffs"},
		},
		Markets: []*services.PlyMktMarket{tradable("m", "c", 40, 20, 5)},
	}}
	res := Aggregate(events, cat, rootID)
	require.Contains(t, res.Tags, "playoffs")
	assert.Equal(t, "1", res.Tags["playoffs"].ParentTagID)
	assert.Equal(t, 40.0, res.Tags["playoffs"].TotalVol)
	assert.Equal(t, 40.0, res.Tags["1"].TotalVol)
}

func TestAggregate_Unresolved(t *testing.T) {
	cat := sportsCatalog()
	events := []*services.PlyMktEvent{{
		ID:      "e1",
		Tags:    []*services.PlyMktTag{{ID: "totally-unknown", Slug: "x"}},
		Markets: []*services.PlyMktMarket{tradable("m", "c", 100, 50, 10)},
	}}
	// Unknown attaches under root → becomes a top (ParentTagID=root), so it IS resolved.
	// True unresolved: empty tags.
	eventsEmpty := []*services.PlyMktEvent{{
		ID:      "e2",
		Tags:    nil,
		Markets: []*services.PlyMktMarket{tradable("m2", "c2", 10, 5, 1)},
	}}
	res := Aggregate(eventsEmpty, cat, rootID)
	assert.Equal(t, 1, res.UnresolvedMarkets)
	assert.Equal(t, "", res.Markets[0].Category)

	res2 := Aggregate(events, cat, rootID)
	// Discovered under root counts as top category.
	require.Contains(t, res2.Tags, "totally-unknown")
	assert.Equal(t, rootID, res2.Tags["totally-unknown"].ParentTagID)
	assert.Equal(t, 100.0, res2.Tags["totally-unknown"].TotalVol)
	assert.Equal(t, 0, res2.UnresolvedMarkets)
}

func TestAggregate_SkipsNonTradable(t *testing.T) {
	cat := sportsCatalog()
	closed := &services.PlyMktMarket{
		ID: "x", ConditionID: "cx",
		EnableOrderBook: true, AcceptingOrders: true, Active: true, Closed: true,
		VolumeClob: 999, Volume24hrClob: 999, LiquidityClob: 999,
	}
	open := tradable("o", "co", 10, 5, 1)
	events := []*services.PlyMktEvent{{
		ID:      "e1",
		Tags:    []*services.PlyMktTag{{ID: "1"}},
		Markets: []*services.PlyMktMarket{closed, open},
	}}
	res := Aggregate(events, cat, rootID)
	assert.Equal(t, 1, res.Tags["1"].TotalMarkets)
	assert.Equal(t, 10.0, res.Tags["1"].TotalVol)
	assert.Equal(t, 2, len(res.Markets))
	assert.Equal(t, 1, len(res.Tradable))
}

func TestAggregate_SeedNotMutated(t *testing.T) {
	cat := sportsCatalog()
	// Poison seed metrics to prove we don't read/write them as running totals.
	cat["1"].TotalVol = 999
	events := []*services.PlyMktEvent{{
		ID:      "e1",
		Tags:    []*services.PlyMktTag{{ID: "1"}},
		Markets: []*services.PlyMktMarket{tradable("m", "c", 10, 5, 1)},
	}}
	res := Aggregate(events, cat, rootID)
	assert.Equal(t, 10.0, res.Tags["1"].TotalVol)
	assert.Equal(t, 999.0, cat["1"].TotalVol, "seed pointer must not be mutated")

	// Second pass still starts from zero on working copies.
	res2 := Aggregate(events, cat, rootID)
	assert.Equal(t, 10.0, res2.Tags["1"].TotalVol)
	assert.Equal(t, 999.0, cat["1"].TotalVol)
}

func TestAggregate_MultiCategoryIsolation(t *testing.T) {
	cat := sportsCatalog()
	events := []*services.PlyMktEvent{
		{
			ID:      "e-sports",
			Tags:    []*services.PlyMktTag{{ID: "1"}},
			Markets: []*services.PlyMktMarket{tradable("ms", "cs", 1000, 100, 50)},
		},
		{
			ID:      "e-pol",
			Tags:    []*services.PlyMktTag{{ID: "2"}},
			Markets: []*services.PlyMktMarket{tradable("mp", "cp", 500, 40, 25)},
		},
	}
	res := Aggregate(events, cat, rootID)
	assert.Equal(t, 1000.0, res.Tags["1"].TotalVol)
	assert.Equal(t, 100.0, res.Tags["1"].TotalVol24hr)
	assert.Equal(t, 500.0, res.Tags["2"].TotalVol)
	assert.Equal(t, 40.0, res.Tags["2"].TotalVol24hr)
}

func TestSortedByVol24AndTagsForUpdate(t *testing.T) {
	tags := map[string]*services.PlyMktTag{
		"1": {ID: "1", Slug: "sports", TotalVol24hr: 10, TotalMarkets: 1},
		"2": {ID: "2", Slug: "politics", TotalVol24hr: 50, TotalMarkets: 2},
	}
	sorted := SortedByVol24(tags)
	require.Len(t, sorted, 2)
	assert.Equal(t, "2", sorted[0].ID)
	assert.Equal(t, "1", sorted[1].ID)

	slice := TagsForUpdate(tags)
	assert.Len(t, slice, 2)
}

func TestCatalogToMap_SkipsRoot(t *testing.T) {
	tags := []*services.PlyMktTag{
		{ID: rootID, Slug: "categories"},
		{ID: "1", Slug: "sports"},
	}
	m := CatalogToMap(tags, rootID)
	assert.NotContains(t, m, rootID)
	assert.Contains(t, m, "1")
}

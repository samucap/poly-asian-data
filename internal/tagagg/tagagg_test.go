package tagagg

import (
	"testing"

	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sportsTop() map[string]*services.PlyMktTag {
	return map[string]*services.PlyMktTag{
		"1": {ID: "1", Slug: "sports", Label: "Sports", ParentTagID: "102982"},
		"2": {ID: "2", Slug: "politics", Label: "Politics", ParentTagID: "102982"},
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

func TestTopCategory(t *testing.T) {
	top := sportsTop()
	got := TopCategory([]*services.PlyMktTag{
		{ID: "nba", Slug: "nba"},
		{ID: "1", Slug: "sports"},
	}, top)
	require.NotNil(t, got)
	assert.Equal(t, "1", got.ID)

	assert.Nil(t, TopCategory([]*services.PlyMktTag{{ID: "nba"}}, top))
	assert.Nil(t, TopCategory(nil, top))
}

func TestAggregate_TopTagSumsOnce(t *testing.T) {
	top := sportsTop()
	m1 := tradable("m1", "c1", 100, 50, 10)
	m2 := tradable("m2", "c2", 200, 80, 20)
	events := []*services.PlyMktEvent{{
		ID:   "e1",
		Tags: []*services.PlyMktTag{{ID: "1", Slug: "sports"}, {ID: "nba", Slug: "nba"}},
		Markets: []*services.PlyMktMarket{m1, m2},
	}}

	res := Aggregate(events, top)
	require.Contains(t, res.Categories, "1")
	// Subtag must not appear as a category total.
	assert.NotContains(t, res.Categories, "nba")

	cat := res.Categories["1"]
	assert.Equal(t, 300.0, cat.TotalVol)
	assert.Equal(t, 130.0, cat.TotalVol24hr)
	assert.Equal(t, 30.0, cat.TotalLiq)
	assert.Equal(t, 2, cat.TotalMarkets)
	assert.Equal(t, 2, res.PoolBySlug["sports"])
	assert.Equal(t, 2, len(res.Tradable))
	assert.Equal(t, 2, len(res.Markets))
	assert.Equal(t, "sports", res.Markets[0].Category)
	assert.Equal(t, "sports", res.CondCategory["c1"])
}

func TestAggregate_SubtagOnlyNoCategoryTotals(t *testing.T) {
	top := sportsTop()
	m := tradable("m1", "c1", 100, 50, 10)
	events := []*services.PlyMktEvent{{
		ID:      "e1",
		Tags:    []*services.PlyMktTag{{ID: "nba", Slug: "nba"}},
		Markets: []*services.PlyMktMarket{m},
	}}

	res := Aggregate(events, top)
	assert.Empty(t, res.Categories, "no top tag ⇒ no category totals (and no subtag totals)")
	assert.Equal(t, 1, res.PoolBySlug[""])
	assert.Equal(t, 1, len(res.Tradable))
	assert.Equal(t, "", res.Markets[0].Category)
}

func TestAggregate_MultiCategoryIsolation(t *testing.T) {
	top := sportsTop()
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
	res := Aggregate(events, top)
	require.Contains(t, res.Categories, "1")
	require.Contains(t, res.Categories, "2")
	assert.Equal(t, 1000.0, res.Categories["1"].TotalVol)
	assert.Equal(t, 100.0, res.Categories["1"].TotalVol24hr)
	assert.Equal(t, 500.0, res.Categories["2"].TotalVol)
	assert.Equal(t, 40.0, res.Categories["2"].TotalVol24hr)
}

func TestAggregate_SkipsNonTradable(t *testing.T) {
	top := sportsTop()
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
	res := Aggregate(events, top)
	assert.Equal(t, 1, res.Categories["1"].TotalMarkets)
	assert.Equal(t, 10.0, res.Categories["1"].TotalVol)
	assert.Equal(t, 2, len(res.Markets))
	assert.Equal(t, 1, len(res.Tradable))
}

func TestToTagAggregatesAndSorted(t *testing.T) {
	cats := map[string]*services.PlyMktTag{
		"1": {ID: "1", Slug: "sports", TotalVol24hr: 10, TotalMarkets: 1},
		"2": {ID: "2", Slug: "politics", TotalVol24hr: 50, TotalMarkets: 2},
	}
	top := sportsTop()
	aggs := ToTagAggregates(cats, top)
	assert.Len(t, aggs, 2)

	sorted := SortedByVol24(cats)
	require.Len(t, sorted, 2)
	assert.Equal(t, "2", sorted[0].ID)
	assert.Equal(t, "1", sorted[1].ID)
}

// Idle top tags (no markets this cycle) must be written as zeros so DB does not
// keep prior total_vol / total_vol_24hr / total_liq / total_markets.
func TestToTagAggregates_ZerosIdleTopTags(t *testing.T) {
	top := sportsTop() // sports=1, politics=2
	// Only sports got volume this scan.
	cats := map[string]*services.PlyMktTag{
		"1": {ID: "1", Slug: "sports", TotalVol: 100, TotalVol24hr: 50, TotalLiq: 10, TotalMarkets: 2},
	}
	aggs := ToTagAggregates(cats, top)
	require.Len(t, aggs, 2)

	byID := map[string]db.TagAggregate{}
	for _, a := range aggs {
		byID[a.ID] = a
	}
	require.Contains(t, byID, "1")
	require.Contains(t, byID, "2")
	assert.Equal(t, 100.0, byID["1"].TotalVol)
	assert.Equal(t, 50.0, byID["1"].TotalVol24hr)
	assert.Equal(t, 2, byID["1"].TotalMarkets)
	// politics idle → zeros
	assert.Equal(t, 0.0, byID["2"].TotalVol)
	assert.Equal(t, 0.0, byID["2"].TotalVol24hr)
	assert.Equal(t, 0.0, byID["2"].TotalLiq)
	assert.Equal(t, 0, byID["2"].TotalMarkets)
}

func TestTopByID_SkipsRoot(t *testing.T) {
	tags := []*services.PlyMktTag{
		{ID: "102982", Slug: "categories"},
		{ID: "1", Slug: "sports"},
	}
	m := TopByID(tags, "102982")
	assert.NotContains(t, m, "102982")
	assert.Contains(t, m, "1")
}

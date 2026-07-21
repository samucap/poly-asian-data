package edgescan

import (
	"testing"
	"time"

	"github.com/samucap/poly-asian-data/internal/marketranking"
	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tradable(id, cond string, vol24, liq, spread float64) *services.PlyMktMarket {
	return &services.PlyMktMarket{
		ID:              id,
		ConditionID:     cond,
		Question:        "Will something happen " + id + "?",
		Active:          true,
		Closed:          false,
		EnableOrderBook: true,
		AcceptingOrders: true,
		Volume24hr:      vol24,
		LiquidityClob:   liq,
		Spread:          spread,
		ClobTokenIds:    `["yes-` + id + `","no-` + id + `"]`,
		OneDayPriceChange: 0.02,
	}
}

func TestFlattenAndBuildBoard(t *testing.T) {
	ev := &services.PlyMktEvent{
		ID:              "e1",
		NegRisk:         true,
		NegRiskMarketID: "grp1",
		Tags:            []*services.PlyMktTag{{ID: "1", Slug: "sports"}},
		Markets: []*services.PlyMktMarket{
			tradable("m1", "c1", 100_000, 50_000, 0.02),
			tradable("m2", "c2", 80_000, 40_000, 0.02),
			tradable("m3", "c3", 1_000, 500, 0.02), // filtered by min vol
		},
	}
	// second market same group for related_legs
	ev.Markets[1].EventID = "e1"

	pool := FlattenCandidates([]*services.PlyMktEvent{ev})
	// Flatten does not apply volume filters — all tradable markets included.
	require.Len(t, pool, 3)

	w := marketranking.DefaultScoreWeights()
	w.UseEnvWeights = false
	// RankMarkets uses DefaultScoreWeights with env - OK for test

	res := BuildBoard(pool, BuildOptions{
		Filter: marketranking.MarketFilter{
			MinVolume24hr: 30_000,
			MinLiquidity:  15_000,
			MaxSpread:     0.05,
			MinVolatility: 0,
			MaxN:          10,
		},
		BoardMaxN: 10,
		Strategy:  "default",
		Now:       time.Now().UTC(),
	})
	require.NotEmpty(t, res.Rows)
	assert.Equal(t, 1, res.Rows[0].Rank)
	assert.Equal(t, "c1", res.Rows[0].ConditionID)
	assert.True(t, res.Rows[0].NegRisk)
	assert.Equal(t, "grp1", res.Rows[0].NegRiskGroupID)
	// related legs: c2 for c1
	assert.Contains(t, res.Rows[0].RelatedLegs, "c2")
	assert.NotEmpty(t, res.Rows[0].ClobTokenIDs)
	assert.Equal(t, "sports", res.Rows[0].Category)

	doc, err := BuildArtifact(res.Rows, res.PoolCount, res.Stage1Count, res.DroppedSummary, "success", nil)
	require.NoError(t, err)
	assert.Equal(t, "2.0", doc.SchemaVersion)
	assert.Equal(t, len(res.Rows), doc.BoardStats.NBoard)
	_, err = WriteArtifact(doc, t.TempDir())
	require.NoError(t, err)
}

func TestStickyPreferredOnCap(t *testing.T) {
	var mkts []*services.PlyMktMarket
	var pool []Candidate
	for i := 0; i < 5; i++ {
		id := string(rune('a' + i))
		m := tradable(id, "cond-"+id, float64(100_000-i*10_000), 50_000, 0.02)
		mkts = append(mkts, m)
		pool = append(pool, Candidate{Market: m, Category: "x"})
	}
	// sticky is lower score market
	res := BuildBoard(pool, BuildOptions{
		Filter: marketranking.MarketFilter{
			MinVolume24hr: 0, MinLiquidity: 0, MaxSpread: 1, MinVolatility: 0, MaxN: 10,
		},
		BoardMaxN:          2,
		StickyConditionIDs: []string{"cond-d"},
		Strategy:           "default",
	})
	require.Len(t, res.Rows, 2)
	ids := []string{res.Rows[0].ConditionID, res.Rows[1].ConditionID}
	assert.Contains(t, ids, "cond-d")
}

func TestShortQuestion(t *testing.T) {
	assert.Equal(t, "hi", shortQuestion("hi", 10))
	s := shortQuestion("abcdefghijKLM", 10)
	assert.True(t, len([]rune(s)) <= 10)
}

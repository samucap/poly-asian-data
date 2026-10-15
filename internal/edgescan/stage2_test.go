package edgescan

import (
	"testing"
	"time"

	"github.com/samucap/poly-asian-data/internal/edge"
	"github.com/samucap/poly-asian-data/internal/enrich"
	"github.com/samucap/poly-asian-data/internal/marketranking"
	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSelectStage1AndBuildEdgeBoard(t *testing.T) {
	ev := &services.PlyMktEvent{
		ID:              "e1",
		NegRisk:         true,
		NegRiskMarketID: "grp1",
		NegRiskFeeBips:  0,
		Tags:            []*services.PlyMktTag{{ID: "1", Slug: "sports"}},
		Markets: []*services.PlyMktMarket{
			tradable("m1", "c1", 100_000, 50_000, 0.02),
			tradable("m2", "c2", 80_000, 40_000, 0.02),
		},
	}
	pool := FlattenCandidates([]*services.PlyMktEvent{ev})
	stage1 := SelectStage1(pool, BuildOptions{
		Filter: marketranking.MarketFilter{
			MinVolume24hr: 30_000,
			MinLiquidity:  15_000,
			MaxSpread:     0.05,
			MaxN:          10,
		},
		Now: time.Now().UTC(),
	})
	require.Equal(t, 2, stage1.Stage1Count)

	// Tight book on c1, wide on c2 → c1 should rank higher after costs
	books := BookIndex{
		"yes-m1": {TokenID: "yes-m1", BestBid: 0.49, BestAsk: 0.51, TotalBidDepth: 500, TotalAskDepth: 500, Time: time.Now().UTC(), Imbalance: 0.5},
		"yes-m2": {TokenID: "yes-m2", BestBid: 0.30, BestAsk: 0.70, TotalBidDepth: 50, TotalAskDepth: 50, Time: time.Now().UTC(), Imbalance: 0.5},
	}
	// tradable() uses ClobTokenIds `["yes-id","no-id"]` with market id
	// m1 id → yes-m1
	build := BuildEdgeBoard(stage1, EdgeBuildOptions{
		BuildOptions: BuildOptions{
			BoardMaxN: 10,
			Strategy:  "default",
			Now:       time.Now().UTC(),
		},
		Weights: edge.DefaultWeights(),
		Books:   books,
	})
	require.NotEmpty(t, build.Rows)
	require.NotNil(t, build.Rows[0].EdgeBps)
	// tight market should outrank wide
	assert.Equal(t, "c1", build.Rows[0].ConditionID)
	assert.NotEmpty(t, build.Rows[0].KeyFeatures)
	assert.NotNil(t, build.Rows[0].CostBps)

	doc, err := BuildArtifact(build.Rows, build.PoolCount, build.Stage1Count, build.DroppedSummary, "success", nil)
	require.NoError(t, err)
	assert.Equal(t, "2.0", doc.SchemaVersion)
	assert.NotNil(t, doc.Board[0].EdgeBps)
}

func TestCollectTokenIDs(t *testing.T) {
	cands := []Candidate{
		{Market: tradable("m1", "c1", 1, 1, 0.01)},
		{Market: tradable("m2", "c2", 1, 1, 0.01)},
	}
	// Primary token only (YES) — halves /books fan-out.
	ids := CollectPrimaryTokenIDs(cands)
	require.Len(t, ids, 2)
	require.Equal(t, []string{"yes-m1", "yes-m2"}, ids)
}

func TestBuildEdgeBoard_ZeroBooksNoPublish(t *testing.T) {
	ev := &services.PlyMktEvent{
		ID: "e1",
		Markets: []*services.PlyMktMarket{
			tradable("m1", "c1", 100_000, 50_000, 0.02),
		},
	}
	pool := FlattenCandidates([]*services.PlyMktEvent{ev})
	stage1 := SelectStage1(pool, BuildOptions{
		Filter: marketranking.MarketFilter{MinVolume24hr: 1, MinLiquidity: 1, MaxSpread: 1, MaxN: 10},
		Now:    time.Now().UTC(),
	})
	build := BuildEdgeBoard(stage1, EdgeBuildOptions{
		BuildOptions:        BuildOptions{BoardMaxN: 5, Now: time.Now().UTC()},
		Weights:             edge.DefaultWeights(),
		Books:               BookIndex{},
		PublishRequireBooks: true,
	})
	require.Empty(t, build.Rows)
}

func TestBookIndexFromEnrich(t *testing.T) {
	// smoke: enrich snapshot type maps
	var _ enrich.BookSnapshot
}

func TestBuildEdgeBoard_FVComplementQuotesAndCostRanks(t *testing.T) {
	// Complement FV implies model raw edge = (1 - Σ mids)×1e4 for every fully observed leg.
	// Rank within group is therefore driven by cost (spread), not relative mid.
	// sum mids ≈ 0.15+0.40+0.35 = 0.90 → raw residual ≈ +1000 bps for each.
	now := time.Now().UTC()
	ev := &services.PlyMktEvent{
		ID: "e1", NegRisk: true, NegRiskMarketID: "g1",
		Markets: []*services.PlyMktMarket{
			tradable("mA", "cA", 100_000, 50_000, 0.02),
			tradable("mB", "cB", 100_000, 50_000, 0.02),
			tradable("mC", "cC", 100_000, 50_000, 0.02),
		},
	}
	pool := FlattenCandidates([]*services.PlyMktEvent{ev})
	for i := range pool {
		pool[i].NegRisk = true
		pool[i].NegRiskGroupID = "g1"
	}
	stage1 := SelectStage1(pool, BuildOptions{
		Filter: marketranking.MarketFilter{MinVolume24hr: 1, MinLiquidity: 1, MaxSpread: 1, MaxN: 10},
		Now:    now,
	})
	require.Equal(t, 3, stage1.Stage1Count)

	// A tight book, B wide book → A ranks higher after same residual edge
	books := BookIndex{
		"yes-mA": {TokenID: "yes-mA", BestBid: 0.145, BestAsk: 0.155, TotalBidDepth: 2000, TotalAskDepth: 2000, Time: now, Imbalance: 0.5},
		"yes-mB": {TokenID: "yes-mB", BestBid: 0.30, BestAsk: 0.50, TotalBidDepth: 100, TotalAskDepth: 100, Time: now, Imbalance: 0.5},
		"yes-mC": {TokenID: "yes-mC", BestBid: 0.34, BestAsk: 0.36, TotalBidDepth: 500, TotalAskDepth: 500, Time: now, Imbalance: 0.5},
	}

	w := edge.DefaultWeights()
	w.ModelBufferBps = 0
	w.DropExtremePrice = true
	w.FVEnabled = true
	build := BuildEdgeBoard(stage1, EdgeBuildOptions{
		BuildOptions:     BuildOptions{BoardMaxN: 10, Strategy: "default", Now: now},
		Weights:          w,
		Books:            books,
		EnrichCandidates: stage1.Candidates,
	})
	require.NotEmpty(t, build.Rows)
	require.Greater(t, build.FVCoverage, 0.0)

	var rankA, rankB int
	for _, r := range build.Rows {
		require.NotNil(t, r.FairValue, "condition %s", r.ConditionID)
		require.Equal(t, "neg_risk_complement", r.FVSource)
		require.NotNil(t, r.ModelEdgeBps)
		require.NotNil(t, r.EdgeBps)
		require.InDelta(t, *r.ModelEdgeBps, *r.EdgeBps, 1e-6)
		if r.ConditionID == "cA" {
			rankA = r.Rank
		}
		if r.ConditionID == "cB" {
			rankB = r.Rank
		}
	}
	require.NotZero(t, rankA)
	require.NotZero(t, rankB)
	require.Less(t, rankA, rankB, "tighter book A should rank above wide book B at same residual")
}

func TestBuildEdgeBoard_DropsExtremePrice(t *testing.T) {
	now := time.Now().UTC()
	ev := &services.PlyMktEvent{
		ID: "e1",
		Markets: []*services.PlyMktMarket{
			tradable("m1", "c1", 100_000, 50_000, 0.01),
		},
	}
	pool := FlattenCandidates([]*services.PlyMktEvent{ev})
	stage1 := SelectStage1(pool, BuildOptions{
		Filter: marketranking.MarketFilter{MinVolume24hr: 1, MinLiquidity: 1, MaxSpread: 1, MaxN: 10},
		Now:    now,
	})
	books := BookIndex{
		"yes-m1": {TokenID: "yes-m1", BestBid: 0.98, BestAsk: 0.99, TotalBidDepth: 500, TotalAskDepth: 500, Time: now},
	}
	w := edge.DefaultWeights()
	w.DropExtremePrice = true
	build := BuildEdgeBoard(stage1, EdgeBuildOptions{
		BuildOptions: BuildOptions{BoardMaxN: 10, Now: now},
		Weights:      w,
		Books:        books,
	})
	require.Empty(t, build.Rows)
	require.Greater(t, build.DroppedSummary["extreme_price"], 0)
}

func TestExpandGroupCandidates(t *testing.T) {
	now := time.Now().UTC()
	ev := &services.PlyMktEvent{
		ID: "e1", NegRisk: true, NegRiskMarketID: "g1",
		Markets: []*services.PlyMktMarket{
			tradable("m1", "c1", 100_000, 50_000, 0.02),
			tradable("m2", "c2", 80_000, 40_000, 0.02),
			tradable("m3", "c3", 1_000, 500, 0.02), // filtered from stage1 by vol
		},
	}
	pool := FlattenCandidates([]*services.PlyMktEvent{ev})
	for i := range pool {
		pool[i].NegRisk = true
		pool[i].NegRiskGroupID = "g1"
	}
	stage1 := SelectStage1(pool, BuildOptions{
		Filter: marketranking.MarketFilter{MinVolume24hr: 30_000, MinLiquidity: 15_000, MaxSpread: 0.05, MaxN: 10},
		Now:    now,
	})
	require.Equal(t, 2, stage1.Stage1Count)
	// ByCond still has all pool for expand from Flatten... actually SelectStage1 ByCond is full pool
	require.Contains(t, stage1.ByCond, "c3")
	expanded, added := ExpandGroupCandidates(stage1, 10)
	require.GreaterOrEqual(t, added, 1)
	require.GreaterOrEqual(t, len(expanded), 3)
}

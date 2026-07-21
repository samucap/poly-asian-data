package catalog

import (
	"testing"
	"time"

	"github.com/samucap/poly-asian-data/internal/artifacts"
	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/samucap/poly-asian-data/internal/tagagg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildCatalogV1_NegRiskAndDiff(t *testing.T) {
	end := time.Now().Add(10 * 24 * time.Hour)
	ev := &services.PlyMktEvent{
		ID:              "ev1",
		NegRisk:         true,
		NegRiskMarketID: "nr-group-1",
		NegRiskFeeBips:  30,
		Markets: []*services.PlyMktMarket{
			{
				ID:              "m1",
				ConditionID:     "c1",
				Question:        "Will X?",
				Active:          true,
				Closed:          false,
				ClobTokenIds:    `["tYes","tNo"]`,
				Volume24hr:      50_000,
				LiquidityClob:   20_000,
				EndDate:         end,
				EnableOrderBook: true,
				AcceptingOrders: true,
			},
		},
	}
	agg := tagagg.Aggregate([]*services.PlyMktEvent{ev}, nil, CategoriesRootTagID)
	doc, err := BuildCatalogV1([]*services.PlyMktEvent{ev}, agg, nil, artifacts.StatusSuccess, nil)
	require.NoError(t, err)
	require.NoError(t, mustValidate(doc))

	require.Len(t, doc.Markets, 1)
	assert.True(t, doc.Markets[0].NegRisk)
	assert.Equal(t, "nr-group-1", doc.Markets[0].NegRiskGroupID)
	assert.Equal(t, 30, doc.Markets[0].NegRiskFeeBips)
	assert.True(t, doc.Markets[0].IsNegRiskLeg)
	assert.Equal(t, "medium", doc.Markets[0].LiquidityTier)
	assert.Equal(t, 1, doc.UniverseStats.NegRiskMarkets)
	assert.Equal(t, 1, doc.UniverseStats.NegRiskGroups)
	assert.Equal(t, []string{"tYes", "tNo"}, doc.Markets[0].ClobTokenIDs)

	// Second build with closed market → status_changed
	ev2 := *ev
	m2 := *ev.Markets[0]
	m2.Closed = true
	m2.Active = false
	m2.AcceptingOrders = false
	ev2.Markets = []*services.PlyMktMarket{&m2}
	agg2 := tagagg.Aggregate([]*services.PlyMktEvent{&ev2}, nil, CategoriesRootTagID)
	doc2, err := BuildCatalogV1([]*services.PlyMktEvent{&ev2}, agg2, &doc, artifacts.StatusSuccess, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, doc2.DiffFromPrevious.StatusChanged)
}

func TestBuildCatalogV1_AddedRemoved(t *testing.T) {
	mk := func(id string) *services.PlyMktEvent {
		return &services.PlyMktEvent{
			ID: "e-" + id,
			Markets: []*services.PlyMktMarket{{
				ID: id, ConditionID: "cond-" + id, Active: true,
				EnableOrderBook: true, AcceptingOrders: true,
				ClobTokenIds: `[]`,
			}},
		}
	}
	e1 := mk("a")
	agg1 := tagagg.Aggregate([]*services.PlyMktEvent{e1}, nil, CategoriesRootTagID)
	d1, err := BuildCatalogV1([]*services.PlyMktEvent{e1}, agg1, nil, artifacts.StatusSuccess, nil)
	require.NoError(t, err)

	e2 := mk("b")
	agg2 := tagagg.Aggregate([]*services.PlyMktEvent{e2}, nil, CategoriesRootTagID)
	d2, err := BuildCatalogV1([]*services.PlyMktEvent{e2}, agg2, &d1, artifacts.StatusSuccess, nil)
	require.NoError(t, err)
	assert.Contains(t, d2.DiffFromPrevious.Added, "cond-b")
	assert.Contains(t, d2.DiffFromPrevious.Removed, "cond-a")
}

func TestLiquidityTier(t *testing.T) {
	assert.Equal(t, "high", liquidityTier(100_000))
	assert.Equal(t, "medium", liquidityTier(15_000))
	assert.Equal(t, "low", liquidityTier(1))
	assert.Equal(t, "none", liquidityTier(0))
}

func mustValidate(doc CatalogV1) error {
	_, err := artifacts.ValidateAndMarshalCatalog(doc)
	return err
}

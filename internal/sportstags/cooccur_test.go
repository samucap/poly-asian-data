package sportstags

import (
	"testing"

	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/stretchr/testify/assert"
)

func TestInferTeamParentsFromEvents(t *testing.T) {
	leagueEdges := map[string]string{
		"745":           TagIDBasketball, // nba
		TagIDBasketball: TagIDSports,
		"100381":        TagIDBaseball, // mlb
		TagIDBaseball:   TagIDSports,
	}
	events := []*services.PlyMktEvent{
		{
			ID: "e1",
			Tags: []*services.PlyMktTag{
				{ID: "1", Slug: "sports"},
				{ID: "745", Slug: "nba"},
				{ID: "101774", Slug: "los-angeles-lakers", Label: "Los Angeles Lakers"},
			},
		},
		{
			ID: "e2",
			Tags: []*services.PlyMktTag{
				{ID: "745", Slug: "nba"},
				{ID: "101774", Slug: "los-angeles-lakers"},
				{ID: "104713", Slug: "golden-state-warriors"},
			},
		},
		{
			ID: "e3",
			Tags: []*services.PlyMktTag{
				{ID: "100381", Slug: "mlb"},
				{ID: "999001", Slug: "new-york-yankees"},
			},
		},
	}
	got := InferTeamParentsFromEvents(events, leagueEdges)
	assert.Equal(t, "745", got["101774"])
	assert.Equal(t, "745", got["104713"])
	assert.Equal(t, "100381", got["999001"])
	assert.NotContains(t, got, "745") // league not reparented as team
}

func TestInferTeamParentsFromEvents_AmbiguousLeaguesSkipped(t *testing.T) {
	leagueEdges := map[string]string{
		"745": TagIDBasketball,
		"450": TagIDFootball,
		TagIDBasketball: TagIDSports,
		TagIDFootball:   TagIDSports,
	}
	events := []*services.PlyMktEvent{
		{
			Tags: []*services.PlyMktTag{
				{ID: "745"}, {ID: "450"}, {ID: "teamX"},
			},
		},
	}
	got := InferTeamParentsFromEvents(events, leagueEdges)
	assert.NotContains(t, got, "teamX")
}

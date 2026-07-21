package sportstags

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeKey(t *testing.T) {
	assert.Equal(t, "los-angeles-lakers", NormalizeKey("Los Angeles Lakers"))
	assert.Equal(t, "real-madrid", NormalizeKey("real madrid"))
	assert.Equal(t, "", NormalizeKey("  "))
}

func TestMatchTeamsToTags_Unique(t *testing.T) {
	// Ensure FamilyTagIDs has basketball for filter
	FamilyTagIDs = map[string]bool{TagIDBasketball: true, TagIDSoccer: true}
	tags := []TagRef{
		{ID: "101774", Label: "Los Angeles Lakers", Slug: "los-angeles-lakers"},
		{ID: "104713", Label: "Golden State Warriors", Slug: "golden-state-warriors"},
		{ID: "745", Label: "NBA", Slug: "nba"},
		{ID: TagIDBasketball, Label: "Basketball", Slug: "basketball"},
		{ID: "1243", Label: "real madrid", Slug: "real-madrid"},
		{ID: "82", Label: "Premier League", Slug: "premier-league"},
	}
	teams := []TeamRow{
		{Name: "Los Angeles Lakers", League: "nba", Abbreviation: "LAL"},
		{Name: "Golden State Warriors", League: "nba", Abbreviation: "GSW"},
		{Name: "Real Madrid", League: "epl"},
	}
	titles := map[string]string{"nba": "745", "epl": "82"}
	matches := MatchTeamsToTags(teams, tags, titles)
	require.Len(t, matches, 3)
	edges := TeamParentEdges(matches)
	assert.Equal(t, "745", edges["101774"])
	assert.Equal(t, "745", edges["104713"])
	assert.Equal(t, "82", edges["1243"])
}

func TestMatchTeamsToTags_AmbiguousSkipped(t *testing.T) {
	tags := []TagRef{
		{ID: "a1", Label: "Manchester", Slug: "manchester"},
		{ID: "a2", Label: "Manchester", Slug: "manchester-utd"},
	}
	teams := []TeamRow{{Name: "Manchester", League: "epl"}}
	titles := map[string]string{"epl": "82"}
	assert.Empty(t, MatchTeamsToTags(teams, tags, titles))
}

func TestLeagueTitleByKey(t *testing.T) {
	m := LeagueTitleByKey([]LeagueKeyTags{
		{Sport: "epl", RawTags: "1,82,306,100639,100350"},
		{Sport: "nba", RawTags: "1,745,100639"},
	})
	assert.Equal(t, "82", m["epl"])
	assert.Equal(t, "745", m["nba"])
}

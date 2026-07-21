package sportstags

import (
	"testing"

	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildFromSportsMetadata_RealCSVs(t *testing.T) {
	rows := []SportsRow{
		{Sport: "mlb", Tags: "1,100639,100381"},
		{Sport: "lol", Tags: "1,64,65,100639"},
		{Sport: "nba", Tags: "1,745,100639"},
		{Sport: "nfl", Tags: "1,450,100639"},
		{Sport: "epl", Tags: "1,82,306,100639,100350"},
		{Sport: "nhl", Tags: "1,899,100639"},
		{Sport: "mls", Tags: "1,100639,100350,100100"},
		// enough rows for soccer/esports/basketball to pass auto-family if only these —
		// SeedFamilyIDs always includes soccer/baseball/etc.
	}
	// pad soccer frequency with more soccer leagues
	for i, key := range []string{"lal", "bun", "fl1", "ucl"} {
		_ = i
		rows = append(rows, SportsRow{Sport: key, Tags: "1,100350,100639,9" + string(rune('0'+i))})
	}

	edges, meta := BuildFromSportsMetadata(rows, DefaultFamilyMinFreq)
	require.Greater(t, meta.EdgeCount, 0)

	assert.Equal(t, TagIDBaseball, edges["100381"]) // mlb inject
	assert.Equal(t, TagIDEsports, edges["65"])
	assert.Equal(t, TagIDBasketball, edges["745"]) // nba inject
	assert.Equal(t, TagIDFootball, edges["450"])   // nfl inject
	assert.Equal(t, TagIDSoccer, edges["82"])
	assert.Equal(t, TagIDSoccer, edges["306"])
	assert.Equal(t, TagIDHockey, edges["899"])
	assert.Equal(t, TagIDSports, edges[TagIDBaseball])
	assert.Equal(t, TagIDSports, edges[TagIDSoccer])
	assert.Equal(t, TagIDSports, edges[TagIDBasketball])
	assert.NotContains(t, edges, TagIDGames)
}

func TestAutoFamilies_HighFreq(t *testing.T) {
	var rows []SportsRow
	for i := 0; i < 6; i++ {
		rows = append(rows, SportsRow{Sport: "l" + string(rune('a'+i)), Tags: "1,100350,999" + string(rune('0'+i))})
	}
	fams := AutoFamilies(rows, 5)
	assert.True(t, fams[TagIDSoccer])
	assert.True(t, fams[TagIDBaseball]) // seed
	assert.False(t, fams["9990"])        // appears once
}

func TestBuildParentEdges_EPL(t *testing.T) {
	fams := map[string]bool{TagIDSoccer: true}
	edges := BuildParentEdges("1,82,306,100639,100350", TagIDSoccer, fams)
	assert.Equal(t, TagIDSoccer, edges["82"])
	assert.Equal(t, TagIDSoccer, edges["306"])
	assert.Equal(t, TagIDSports, edges[TagIDSoccer])
}

func TestLeagueTitleTag(t *testing.T) {
	fams := map[string]bool{TagIDSoccer: true, TagIDBasketball: true}
	assert.Equal(t, "82", LeagueTitleTag("1,82,306,100639,100350", TagIDSoccer, fams))
	assert.Equal(t, "745", LeagueTitleTag("1,745,100639", TagIDBasketball, fams))
}

func TestEdgesFromLeagues_AndApplyParents(t *testing.T) {
	leagues := []services.PlyMktSport{
		{Sport: "mlb", Tags: "1,100639,100381"},
		{Sport: "lol", Tags: "1,64,65,100639"},
		{Sport: "nba", Tags: "1,745,100639"},
	}
	edges := EdgesFromLeagues(leagues)
	assert.Equal(t, TagIDBaseball, edges["100381"])
	assert.Equal(t, TagIDEsports, edges["65"])
	assert.Equal(t, TagIDBasketball, edges["745"])

	tags := map[string]*services.PlyMktTag{
		"100381":      {ID: "100381", Label: "MLB", Slug: "mlb"},
		"65":          {ID: "65", Label: "LoL", Slug: "lol"},
		"745":         {ID: "745", Label: "NBA", Slug: "nba"},
		TagIDEsports:  {ID: TagIDEsports, Label: "Esports", ParentTagID: "wrong"},
		TagIDBaseball: {ID: TagIDBaseball, Label: "baseball", ParentTagID: ""},
	}
	n := ApplyParents(tags, edges)
	require.Greater(t, n, 0)
	assert.Equal(t, TagIDBaseball, tags["100381"].ParentTagID)
	assert.Equal(t, TagIDEsports, tags["65"].ParentTagID)
	assert.Equal(t, TagIDBasketball, tags["745"].ParentTagID)
}

func TestKnownLabelSlug(t *testing.T) {
	l, s := KnownLabelSlug(TagIDSoccer)
	assert.Equal(t, "Soccer", l)
	assert.Equal(t, "soccer", s)
}

func TestAllTagIDsFromSports(t *testing.T) {
	ids := AllTagIDsFromSports([]SportsRow{{Sport: "mlb", Tags: "1,100639,100381"}})
	assert.Contains(t, ids, "100381")
	assert.Contains(t, ids, TagIDSports)
	assert.Contains(t, ids, TagIDBaseball) // inject
	assert.NotContains(t, ids, TagIDGames)
}

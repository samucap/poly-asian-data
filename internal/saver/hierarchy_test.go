package saver

import (
	"testing"

	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/samucap/poly-asian-data/internal/sportstags"
	"github.com/stretchr/testify/assert"
)

// Saver-level checks that league→sport slug mapping still works for FKs.
// Tag parent edges live in package sportstags (authoritative).

func TestFindSportForLeague_Defaults(t *testing.T) {
	s := &Saver{sportCache: map[string]*CachedSport{}}
	assert.Equal(t, "esports", s.findSportForLeague(&services.PlyMktSport{Sport: "lol", Tags: "1,64,65"}))
	assert.Equal(t, "baseball", s.findSportForLeague(&services.PlyMktSport{Sport: "mlb", Tags: "1,100381"}))
	assert.Equal(t, "basketball", s.findSportForLeague(&services.PlyMktSport{Sport: "nba", Tags: "1,745"}))
}

func TestSportstagsHierarchy_MLB_LoL_NBA(t *testing.T) {
	edges := sportstags.EdgesFromLeagues([]services.PlyMktSport{
		{Sport: "mlb", Tags: "1,100639,100381"},
		{Sport: "lol", Tags: "1,64,65,100639"},
		{Sport: "nba", Tags: "1,745,100639"},
		{Sport: "epl", Tags: "1,82,306,100639,100350"},
	})
	assert.Equal(t, sportstags.TagIDBaseball, edges["100381"])
	assert.Equal(t, sportstags.TagIDEsports, edges["65"])
	assert.Equal(t, sportstags.TagIDBasketball, edges["745"])
	assert.Equal(t, sportstags.TagIDSoccer, edges["82"])
	assert.Equal(t, sportstags.TagIDSports, edges[sportstags.TagIDBaseball])
	assert.Equal(t, sportstags.TagIDSports, edges[sportstags.TagIDEsports])
	assert.Equal(t, sportstags.TagIDSports, edges[sportstags.TagIDBasketball])
	assert.Equal(t, sportstags.TagIDSports, edges[sportstags.TagIDSoccer])
}

// Package sportstags is the authoritative sports tag hierarchy: Gamma tag id → parent tag id.
//
// Source of truth: Gamma GET /sports (league key + tags CSV) and GET /teams (name match).
// Target tree: Categories → Sports (1) → sport family → league title → team.
//
// Hierarchy is pure parent_tag_id (text Gamma IDs). It does not use sports table UUIDs.
package sportstags

import (
	"strings"

	"github.com/samucap/poly-asian-data/internal/services"
)

// Well-known Gamma tag IDs.
const (
	TagIDSports     = "1"      // top category Sports (parent = categories root)
	TagIDEsports    = "64"     // family under Sports
	TagIDBaseball   = "678"    // family under Sports
	TagIDSoccer     = "100350" // family under Sports
	TagIDBasketball = "28"     // family under Sports
	TagIDFootball   = "10"     // American football family under Sports
	TagIDHockey     = "100088" // family under Sports
	TagIDTennis     = "864"    // family under Sports
	TagIDCricket    = "517"    // family under Sports
	TagIDGames      = "100639" // meta "Games" — never a parent/child in sport chains

	// DefaultFamilyMinFreq: tags appearing in this many /sports rows become auto-families.
	DefaultFamilyMinFreq = 5
)

// Static family edges always true regardless of /sports CSV completeness.
var staticFamilyParents = map[string]string{
	TagIDEsports:    TagIDSports,
	TagIDBaseball:   TagIDSports,
	TagIDSoccer:     TagIDSports,
	TagIDBasketball: TagIDSports,
	TagIDFootball:   TagIDSports,
	TagIDHockey:     TagIDSports,
	TagIDTennis:     TagIDSports,
	TagIDCricket:    TagIDSports,
}

// SeedFamilyIDs are always treated as families (even if below frequency threshold).
var SeedFamilyIDs = map[string]bool{
	TagIDEsports:    true,
	TagIDBaseball:   true,
	TagIDSoccer:     true,
	TagIDBasketball: true,
	TagIDFootball:   true,
	TagIDHockey:     true,
	TagIDTennis:     true,
	TagIDCricket:    true,
}

// FamilyTagIDs is the working set of mid-layer parents under Sports (1).
// Populated by AutoFamilies / BuildFromSportsMetadata; SeedFamilyIDs always included.
// Callers may read this after BuildFromSportsMetadata for IsOwned checks.
var FamilyTagIDs = map[string]bool{
	TagIDEsports:    true,
	TagIDBaseball:   true,
	TagIDSoccer:     true,
	TagIDBasketball: true,
	TagIDFootball:   true,
	TagIDHockey:     true,
	TagIDTennis:     true,
	TagIDCricket:    true,
}

// LeagueKeyToFamilyTagID injects a family when the league CSV omits it (orphan majors).
// Only used when auto-family is not present in that row's tags.
var LeagueKeyToFamilyTagID = map[string]string{
	"mlb":  TagIDBaseball,
	"kbo":  TagIDBaseball,
	"npb":  TagIDBaseball,
	"nba":  TagIDBasketball,
	"wnba": TagIDBasketball,
	"ncaab": TagIDBasketball,
	"cbb":  TagIDBasketball,
	"nfl":  TagIDFootball,
	"cfb":  TagIDFootball,
	"cfl":  TagIDFootball,
	"nhl":  TagIDHockey,
	// slug-based soccer keys without family still covered when CSV has 100350;
	// inject for sparse CSVs if needed later.
}

// LeagueKeyToSportSlug maps league key → sport slug for sports-table FKs (not tag parents).
var LeagueKeyToSportSlug = map[string]string{
	"acn": "soccer", "bl2": "soccer", "scop": "soccer", "fr2": "soccer", "itsb": "soccer",
	"epl": "soccer", "lal": "soccer", "bun": "soccer", "fl1": "soccer", "sea": "soccer",
	"mls": "soccer", "ucl": "soccer", "uel": "soccer",
	"nba": "basketball", "wnba": "basketball", "ncaab": "basketball", "cbb": "basketball",
	"nhl": "hockey", "cfb": "football", "nfl": "football", "cfl": "football",
	"mlb": "baseball", "kbo": "baseball", "npb": "baseball",
	"lol": "esports", "lol-wild-rift": "esports", "val": "esports",
	"cs2": "esports", "csgo": "esports", "dota2": "esports", "dota": "esports",
	"starcraft2": "esports", "es2": "esports", "bnd": "esports", "codmw": "esports",
	"bpl": "cricket", "cpl": "cricket", "wtc": "cricket", "odc": "cricket",
	"ecc": "cricket", "weth": "cricket", "eth": "cricket",
	"atp": "tennis", "wta": "tennis", "wttmen": "tennis",
}

// SportSlugToFamilyTagID maps sport slug → family tag (for sports.primary_tag_id helpers).
var SportSlugToFamilyTagID = map[string]string{
	"esports":    TagIDEsports,
	"baseball":   TagIDBaseball,
	"soccer":     TagIDSoccer,
	"basketball": TagIDBasketball,
	"football":   TagIDFootball,
	"hockey":     TagIDHockey,
	"tennis":     TagIDTennis,
	"cricket":    TagIDCricket,
}

// SportsRow is the minimum /sports metadata needed for hierarchy.
type SportsRow struct {
	Sport string // league key
	Tags  string // CSV of Gamma tag ids
}

// BuildMeta describes a BuildFromSportsMetadata run.
type BuildMeta struct {
	SportsRows     int
	AutoFamilies   int
	InjectHits     int
	NoFamilyLeagues int
	UniqueTagIDs   int
	EdgeCount      int
}

// KnownLabelSlug returns hardcoded label/slug for well-known sport tags.
func KnownLabelSlug(id string) (label, slug string) {
	switch id {
	case TagIDSports:
		return "Sports", "sports"
	case TagIDEsports:
		return "Esports", "esports"
	case TagIDBaseball:
		return "baseball", "baseball"
	case TagIDSoccer:
		return "Soccer", "soccer"
	case TagIDBasketball:
		return "Basketball", "basketball"
	case TagIDFootball:
		return "football", "football"
	case TagIDHockey:
		return "Hockey", "hockey"
	case TagIDTennis:
		return "Tennis", "tennis"
	case TagIDCricket:
		return "Cricket", "cricket"
	case TagIDGames:
		return "Games", "games"
	default:
		return "", ""
	}
}

// AutoFamilies returns tag ids that appear in >= minFreq sports CSVs (excluding Sports/Games),
// merged with SeedFamilyIDs.
func AutoFamilies(rows []SportsRow, minFreq int) map[string]bool {
	if minFreq <= 0 {
		minFreq = DefaultFamilyMinFreq
	}
	freq := map[string]int{}
	for _, r := range rows {
		for _, id := range splitTagCSV(r.Tags) {
			if id == TagIDSports || id == TagIDGames {
				continue
			}
			freq[id]++
		}
	}
	out := map[string]bool{}
	for id, n := range freq {
		if n >= minFreq {
			out[id] = true
		}
	}
	for id := range SeedFamilyIDs {
		out[id] = true
	}
	return out
}

// FamilyTagForLeague resolves family for one league using auto-family set + inject.
// families may be nil (falls back to SeedFamilyIDs / FamilyTagIDs).
func FamilyTagForLeague(leagueKey, tagsCSV string, families map[string]bool) string {
	if families == nil {
		families = FamilyTagIDs
	}
	// Prefer family id present in CSV.
	for _, id := range splitTagCSV(tagsCSV) {
		if families[id] {
			return id
		}
	}
	// Inject for orphan majors (CSV omits family).
	key := strings.ToLower(strings.TrimSpace(leagueKey))
	if fam := LeagueKeyToFamilyTagID[key]; fam != "" {
		return fam
	}
	// Legacy slug map → family id.
	if slug, ok := LeagueKeyToSportSlug[key]; ok {
		if fam := SportSlugToFamilyTagID[slug]; fam != "" {
			return fam
		}
	}
	return ""
}

// BuildParentEdges derives child→parent edges from a tags CSV + family id.
func BuildParentEdges(tagsCSV, familyTagID string, families map[string]bool) map[string]string {
	if families == nil {
		families = FamilyTagIDs
	}
	ids := splitTagCSV(tagsCSV)

	family := familyTagID
	if family == "" {
		for _, id := range ids {
			if families[id] {
				family = id
				break
			}
		}
	}

	var leaves []string
	for _, id := range ids {
		if id == TagIDSports || id == family {
			continue
		}
		if families[id] && id != family {
			continue
		}
		leaves = append(leaves, id)
	}

	edges := map[string]string{}
	if family != "" {
		edges[family] = TagIDSports
	}
	for _, leaf := range leaves {
		if family != "" {
			edges[leaf] = family
		} else {
			edges[leaf] = TagIDSports
		}
	}
	return edges
}

// LeagueTitleTagIDs returns non-family leaf tag ids from a league CSV.
func LeagueTitleTagIDs(tagsCSV, familyTagID string, families map[string]bool) []string {
	if families == nil {
		families = FamilyTagIDs
	}
	ids := splitTagCSV(tagsCSV)
	family := familyTagID
	if family == "" {
		for _, id := range ids {
			if families[id] {
				family = id
				break
			}
		}
	}
	var leaves []string
	for _, id := range ids {
		if id == TagIDSports || id == family {
			continue
		}
		if families[id] {
			continue
		}
		leaves = append(leaves, id)
	}
	return leaves
}

// LeagueTitleTag picks the first league leaf in CSV order.
func LeagueTitleTag(tagsCSV, familyTagID string, families map[string]bool) string {
	leaves := LeagueTitleTagIDs(tagsCSV, familyTagID, families)
	if len(leaves) == 0 {
		return ""
	}
	return leaves[0]
}

// BuildFromSportsMetadata builds the full child→parent map from /sports rows.
// Updates package FamilyTagIDs to the auto-family set used for this build.
func BuildFromSportsMetadata(rows []SportsRow, minFreq int) (edges map[string]string, meta BuildMeta) {
	if minFreq <= 0 {
		minFreq = DefaultFamilyMinFreq
	}
	families := AutoFamilies(rows, minFreq)
	// Publish for IsOwned / callers during this process.
	FamilyTagIDs = families

	meta.SportsRows = len(rows)
	meta.AutoFamilies = len(families)

	edges = map[string]string{}
	seenIDs := map[string]bool{}
	for _, r := range rows {
		for _, id := range splitTagCSV(r.Tags) {
			seenIDs[id] = true
		}
		fam := FamilyTagForLeague(r.Sport, r.Tags, families)
		if fam != "" {
			if inject := LeagueKeyToFamilyTagID[strings.ToLower(strings.TrimSpace(r.Sport))]; inject != "" && fam == inject {
				// count inject when CSV lacked family (fam from inject only)
				inCSV := false
				for _, id := range splitTagCSV(r.Tags) {
					if id == inject {
						inCSV = true
						break
					}
				}
				if !inCSV {
					meta.InjectHits++
				}
			}
		} else {
			meta.NoFamilyLeagues++
		}
		for child, parent := range BuildParentEdges(r.Tags, fam, families) {
			edges[child] = parent
			seenIDs[child] = true
			seenIDs[parent] = true
		}
	}
	edges = MergeStaticFamilyAnchors(edges)
	meta.UniqueTagIDs = len(seenIDs)
	meta.EdgeCount = len(edges)
	return edges, meta
}

// EdgesFromLeagues builds edges from PlyMktSport rows (DB / processor shape).
func EdgesFromLeagues(leagues []services.PlyMktSport) map[string]string {
	rows := make([]SportsRow, 0, len(leagues))
	for _, l := range leagues {
		rows = append(rows, SportsRow{Sport: l.Sport, Tags: l.Tags})
	}
	edges, _ := BuildFromSportsMetadata(rows, DefaultFamilyMinFreq)
	return edges
}

// MergeStaticFamilyAnchors adds seed family→Sports when missing.
func MergeStaticFamilyAnchors(edges map[string]string) map[string]string {
	if edges == nil {
		edges = map[string]string{}
	}
	for child, parent := range staticFamilyParents {
		if _, exists := edges[child]; !exists {
			edges[child] = parent
		}
	}
	return edges
}

// MergeEdges merges b into a copy of a; b wins on key collision.
func MergeEdges(a, b map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range a {
		if k != "" && v != "" {
			out[k] = v
		}
	}
	for k, v := range b {
		if k != "" && v != "" {
			out[k] = v
		}
	}
	return out
}

// ApplyParents sets ParentTagID on tags from edges. Sports hierarchy wins.
func ApplyParents(tags map[string]*services.PlyMktTag, edges map[string]string) int {
	if tags == nil || len(edges) == 0 {
		return 0
	}
	n := 0
	for child, parent := range edges {
		if child == "" || parent == "" {
			continue
		}
		t := tags[child]
		if t == nil {
			label, slug := KnownLabelSlug(child)
			if label == "" && slug == "" {
				continue
			}
			t = &services.PlyMktTag{ID: child, Label: label, Slug: slug}
			tags[child] = t
		}
		if t.ParentTagID != parent {
			t.ParentTagID = parent
			n++
		}
	}
	if _, ok := tags[TagIDSports]; !ok {
		label, slug := KnownLabelSlug(TagIDSports)
		tags[TagIDSports] = &services.PlyMktTag{ID: TagIDSports, Label: label, Slug: slug}
	}
	return n
}

// IsOwned reports whether tagID is a sports-hierarchy node.
func IsOwned(tagID string, edges map[string]string) bool {
	if tagID == "" {
		return false
	}
	if tagID == TagIDSports || FamilyTagIDs[tagID] {
		return true
	}
	if edges != nil {
		if _, ok := edges[tagID]; ok {
			return true
		}
		// Also owned if used as a parent in the edge map.
		for _, p := range edges {
			if p == tagID {
				return true
			}
		}
	}
	return false
}

// ParentOf returns the authoritative parent for a sports-owned tag.
func ParentOf(tagID string, edges map[string]string) (string, bool) {
	if edges != nil {
		if p, ok := edges[tagID]; ok && p != "" {
			return p, true
		}
	}
	if p, ok := staticFamilyParents[tagID]; ok {
		return p, true
	}
	return "", false
}

// AllTagIDsFromSports returns unique tag ids referenced by sports rows (incl. Sports, excl. Games).
func AllTagIDsFromSports(rows []SportsRow) []string {
	seen := map[string]bool{}
	var ids []string
	add := func(id string) {
		if id == "" || id == TagIDGames || seen[id] {
			return
		}
		seen[id] = true
		ids = append(ids, id)
	}
	add(TagIDSports)
	for _, r := range rows {
		for _, id := range splitTagCSV(r.Tags) {
			add(id)
		}
		if fam := LeagueKeyToFamilyTagID[strings.ToLower(strings.TrimSpace(r.Sport))]; fam != "" {
			add(fam)
		}
	}
	for id := range SeedFamilyIDs {
		add(id)
	}
	return ids
}

func splitTagCSV(tagsCSV string) []string {
	raw := strings.Split(tagsCSV, ",")
	seen := map[string]bool{}
	var ids []string
	for _, id := range raw {
		id = strings.TrimSpace(id)
		if id == "" || id == TagIDGames || seen[id] {
			continue
		}
		seen[id] = true
		ids = append(ids, id)
	}
	return ids
}

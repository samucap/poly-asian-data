package main

import (
	"encoding/json"
	"fmt"
	"github.com/samucap/poly-asian-data/internal/services"
	"os"
	"slices"
	"strings"
)

// Simplified struct for local unmarshalling since we can't import main
type Sport struct {
	ID          string // UUID
	Tag         *services.PlyMktTag
	Slug        string
	RelatedTags []*services.PlyMktTag
	Leagues     []string
}

func main() {
	// 1. Load Tags
	tagsData, err := os.ReadFile("tags_sample.json")
	if err != nil {
		panic(err)
	}
	var tags []*services.PlyMktTag
	if err := json.Unmarshal(tagsData, &tags); err != nil {
		panic(err)
	}
	fmt.Printf("Loaded %d tags\n", len(tags))

	tagsMap := make(map[string]*services.PlyMktTag)
	for _, t := range tags {
		tagsMap[t.ID] = t
	}
	// Inject missing tags for "shl" test case
	if _, ok := tagsMap["899"]; !ok {
		t := &services.PlyMktTag{ID: "899", Label: "Mock Tag 899", Slug: "mock-899"}
		tagsMap["899"] = t
		tags = append(tags, t)
	}
	if _, ok := tagsMap["102906"]; !ok {
		t := &services.PlyMktTag{ID: "102906", Label: "Mock Tag 102906", Slug: "mock-102906"}
		tagsMap["102906"] = t
		tags = append(tags, t)
	}

	// 2. Load Leagues
	leaguesData, err := os.ReadFile("sports_sample.json")
	if err != nil {
		panic(err)
	}
	var leagues []*services.PlyMktSport
	if err := json.Unmarshal(leaguesData, &leagues); err != nil {
		panic(err)
	}
	fmt.Printf("Loaded %d leagues\n", len(leagues))

	// 3. Setup Sports Cats
	slugs := []string{
		"football", "basketball", "hockey", "tennis", "esports", "baseball",
		"soccer", "cricket", "rugby", "golf", "ufc", "formula1", "chess",
		"boxing", "pickleball",
	}
	sportsCats := make(map[string]*Sport)
	for _, tag := range tags {
		if slices.Contains(slugs, tag.Slug) {
			sportsCats[tag.Slug] = &Sport{
				Tag:  tag,
				Slug: tag.Slug,
			}
			fmt.Printf("Initialized Sport Category: %s (Tag ID: %s)\n", tag.Slug, tag.ID)
		}
	}

	gamesTag := "100639"

	// 4. Run Logic
	for _, league := range leagues {
		sportSlug := findSportTags(league, sportsCats)
		if sportSlug == "" {
			// fmt.Printf("Skipping league %s (no sport found)\n", league.Sport)
			continue
		}
		sport := sportsCats[sportSlug]

		// Debug output for SHL (Hockey)
		if league.Sport == "shl" {
			fmt.Printf("Processing SHL. Sport: %s (ID: %s). League Tags: %s\n", sportSlug, sport.Tag.ID, league.Tags)
		}

		// Chain parent tags (Top-down: Sport -> Tag1 -> Tag2 ...)
		currTags := strings.Split(league.Tags, ",")
		cleaned := make([]string, 0, len(currTags)+1)

		// 1. Sport is always the root/parent
		cleaned = append(cleaned, sport.Tag.ID)

		// 2. Append other tags, filtering out duplicates/ignored
		for _, id := range currTags {
			id = strings.TrimSpace(id)
			if id != "" && id != sport.Tag.ID && id != "1" && id != gamesTag {
				cleaned = append(cleaned, id)
			}
		}

		if league.Sport == "shl" {
			fmt.Printf("Cleaned Tag Chain: %v\n", cleaned)
		}

		for i := 0; i < len(cleaned)-1; i++ {
			parentID := cleaned[i]
			childID := cleaned[i+1]

			// Ensure both child and parent exist in our map
			if t, ok := tagsMap[childID]; ok {
				// Determine if we are changing it
				oldParent := t.ParentTagID
				t.ParentTagID = parentID
				if league.Sport == "shl" {
					fmt.Printf("  Set ParentTagID for Tag %s (%s) to %s (Old: %s)\n", t.ID, t.Label, t.ParentTagID, oldParent)
				}
			} else {
				if league.Sport == "shl" {
					fmt.Printf("  Tag ID %s not found in Tags Map!\n", childID)
				}
			}
		}
	}
}

// Copied helper
func findSportTags(league *services.PlyMktSport, sportsCats map[string]*Sport) string {
	defaults := map[string]string{
		"acn": "soccer", "bl2": "soccer", "scop": "soccer", "fr2": "soccer", "itsb": "soccer",
		"nba": "basketball", "wnba": "basketball", "ncaab": "basketball", "cbb": "basketball",
		"nhl": "hockey", "cfb": "football", "nfl": "football", "mlb": "baseball",
		"csgo": "esports", "starcraft2": "esports", "es2": "esports", "bnd": "esports",
		"bpl": "cricket", "cpl": "cricket", "wtc": "cricket", "odc": "cricket",
		"ecc": "cricket", "weth": "cricket", "eth": "cricket",
	}
	tagIDs := strings.Split(league.Tags, ",")
	for slug, cat := range sportsCats {
		for _, tagID := range tagIDs {
			if tagID == cat.Tag.ID {
				return slug
			}
		}
		if strings.Contains(league.Resolution, cat.Tag.Slug) {
			return slug
		}
		if defaultSlug, ok := defaults[league.Sport]; ok && defaultSlug == slug {
			return slug
		}
	}
	return ""
}

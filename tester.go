package main

import (
	"fmt"
	"net/http"
	"net/url"
	"encoding/json"
	"strconv"
	"slices"
	"strings"
	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/google/uuid"
)

type Sport struct {
	ID string
	Tag *services.PlyMktTag
	Slug string
	RelatedTags []*services.PlyMktTag
	Leagues []string
	Defaults []string
}

func main() {
	slugs := []string {
		"football",
		"basketball",
		"hockey",
		"tennis",
		"esports",
		"baseball",
		"soccer",
		"cricket",
		"rugby",
		"golf",
		"ufc",
		"f1",
		"chess",
		"boxing",
		"pickleball",
	}
	gamesTag := "100639"
	sportsCats := map[string]*Sport {}

	tags, err := fetchTags()
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, tag := range tags {
		if slices.Contains(slugs, tag.Slug) {
			sportsCats[tag.Slug] = &Sport{
				ID: uuid.New().String(),
				Tag: tag,
				Slug: tag.Slug,
			}
		}
	}

	gamesEvs, err := fetchEvents(gamesTag)
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, ev := range gamesEvs {
		evTags := ev.Tags
		if slug := getSportFromEvTags(evTags, slugs); slug != "" {
			related := slices.DeleteFunc(ev.Tags, func(t *services.PlyMktTag) bool {
				return t.ID == "1" || t.ID == "100639" || t.Slug == slug || slices.ContainsFunc(sportsCats[slug].RelatedTags, func(tag *services.PlyMktTag) bool {
					return tag.ID == t.ID
				})
			})
			if len(related) > 0 {
				sportsCats[slug].RelatedTags = append(sportsCats[slug].RelatedTags, related...)
			}
		}
	}

	leagues, err := fetchLeagues()
	if err != nil {
		fmt.Println(err)
		return
	}
	
	//var unknownSports []*services.PlyMktSport
	for _, league := range leagues {
		if slug := findSportTags(league, sportsCats); slug != "" {
			if !slices.Contains(sportsCats[slug].Leagues, league.Sport) {
				sportsCats[slug].Leagues = append(sportsCats[slug].Leagues, league.Sport)
			}
			currTags := strings.Split(league.Tags, ",")
			currTags = slices.DeleteFunc(currTags, func(t string) bool {
				return t == "" || t == sportsCats[slug].Tag.ID || t == "1" || t == "100639" || slices.ContainsFunc(sportsCats[slug].RelatedTags, func(tag *services.PlyMktTag) bool {
					return tag.ID == t
				})
			})
			for _, tag := range currTags {
				if found := findTag(tag, tags); found != nil {
					sportsCats[slug].RelatedTags = append(sportsCats[slug].RelatedTags, found)
				} else {
					fmt.Printf("unknown tag ==== %+v\n", tag)
				}
			}
		} else {
			

		}
	}

	for _, cat := range sportsCats {
		fmt.Printf("%+v %d\n", cat, len(cat.RelatedTags))
		if cat.Slug == "esports" {
			for _, tag := range cat.RelatedTags {
				fmt.Printf("%+v\n", tag)
			}
		}
	}

	// fetch teams and try to match team.League to a sport if in sportsCats[slug].Leagues or sportsCats[slug].RelatedTags
	teams, err := fetchTeams()
	if err != nil {
		fmt.Println(err)
		return
	}

	for _, team := range teams {
		if currSport := findTeamSport(team, sportsCats); currSport != "" {
			team.SportID = currSport
		} else {
			fmt.Printf("unknown team ==== %+v\n", team)
		}
	}

	fmt.Printf("teams ==== %+v\n", teams[0])
	
}

func getSportFromEvTags(evTags []*services.PlyMktTag, slugs []string) string {
	for _, tag := range evTags {
		if slices.Contains(slugs, tag.Slug) {
			return tag.Slug
		}
	}
	return ""
}

func findTag(id string, tags []*services.PlyMktTag) *services.PlyMktTag {
	for _, tag := range tags {
		if tag.ID == id {
			return tag
		}
	}
	return nil
}

func findTeamSport(team *services.PlyMktTeam, sportsCats map[string]*Sport) string {
	defaults := map[string]string{
		"acn":        "soccer",
		"bl2":        "soccer",
		"scop":       "soccer",
		"fr2":        "soccer",
		"itsb":       "soccer",
		"nba":        "basketball",
		"wnba":       "basketball",
		"ncaab":      "basketball",
		"cbb":        "basketball",
		"nhl":        "hockey",
		"cfb":        "football",
		"nfl":        "football",
		"mlb":        "baseball",
		"csgo":       "esports",
		"starcraft2": "esports",
		"es2":        "esports",
		"bnd":        "esports",
		"bpl":        "cricket",
		"cpl":        "cricket",
		"wtc":        "cricket",
		"odc":        "cricket",
		"ecc":        "cricket",
		"weth":       "cricket",
		"eth":        "cricket",
	}
	for _, cat := range sportsCats {
		if slices.Contains(cat.Leagues, team.League) {
			return cat.Slug
		}
		if defaultSlug, ok := defaults[team.League]; ok && defaultSlug == cat.Slug {
			return cat.Slug
		}
		if slices.ContainsFunc(cat.RelatedTags, func(tag *services.PlyMktTag) bool {
			return tag.Slug == team.League
		}) {
			return cat.Slug
		}
	}

	// Fallback: extract sport from logo URL (e.g., "team_logos/soccer/scop/...")
	if team.Logo != "" {
		parts := strings.Split(team.Logo, "/")
		for i, part := range parts {
			if part == "team_logos" && i+1 < len(parts) {
				sportFromLogo := parts[i+1]
				// Check if this sport exists in our categories
				if _, ok := sportsCats[sportFromLogo]; ok {
					return sportFromLogo
				}
			}
		}
	}

	return ""
}

func findSportTags(sport *services.PlyMktSport, cats map[string]*Sport) string {
	// Defaults: map league names to sport slugs
	defaults := map[string]string{
		"acn":        "soccer",
		"bl2":        "soccer",
		"scop":       "soccer",
		"fr2":        "soccer",
		"itsb":       "soccer",
		"nba":        "basketball",
		"wnba":       "basketball",
		"ncaab":      "basketball",
		"cbb":        "basketball",
		"nhl":        "hockey",
		"cfb":        "football",
		"nfl":        "football",
		"mlb":        "baseball",
		"csgo":       "esports",
		"starcraft2": "esports",
		"es2":        "esports",
		"bnd":        "esports",
		"bpl":        "cricket",
		"cpl":        "cricket",
		"wtc":        "cricket",
		"odc":        "cricket",
		"ecc":        "cricket",
		"weth":       "cricket",
		"eth":        "cricket",
	}

	// Split tags once for exact matching
	sportTagIDs := strings.Split(sport.Tags, ",")

	for _, cat := range cats {
		// Use exact ID matching instead of substring matching
		if slices.Contains(sportTagIDs, cat.Tag.ID) ||
			strings.Contains(sport.Resolution, cat.Tag.Slug) {
			return cat.Tag.Slug
		}
	}

	// Check defaults using exact league name match
	if slug, ok := defaults[sport.Sport]; ok {
		return slug
	}

	return ""
}

func fetchLeagues() ([]*services.PlyMktSport, error) {
	baseURL := "https://gamma-api.polymarket.com/sports"
	resp, err := http.Get(baseURL)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	defer resp.Body.Close()

	var sports []*services.PlyMktSport
	if err := json.NewDecoder(resp.Body).Decode(&sports); err != nil {
		fmt.Println(err)
		return nil, err
	}

	return sports, nil
}

func fetchTags() ([]*services.PlyMktTag, error) {
	baseURL := "https://gamma-api.polymarket.com/tags"
	limit := 300
	offset := 0

	var fullTags []*services.PlyMktTag
	for {
		params := url.Values{}
		params.Add("limit", strconv.Itoa(limit))
		if offset > 0 {
			params.Add("offset", strconv.Itoa(offset))
		}

		reqURL := baseURL + "?" + params.Encode()
		fmt.Println("fetching", reqURL)

		resp, err := http.Get(reqURL)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		defer resp.Body.Close()

		var tags []*services.PlyMktTag
		if err := json.NewDecoder(resp.Body).Decode(&tags); err != nil {
			fmt.Println(err)
			return nil, err
		}

		if len(tags) == 0 {
			break
		}

		fullTags = append(fullTags, tags...)
		offset += limit
	}

	return fullTags, nil
}

func fetchTeams() ([]*services.PlyMktTeam, error) {
	baseURL := "https://gamma-api.polymarket.com/teams"
	limit := 500
	offset := 0

	var fullTeams []*services.PlyMktTeam
	for {
		params := url.Values{}
		params.Add("limit", strconv.Itoa(limit))
		if offset > 0 {
			params.Add("offset", strconv.Itoa(offset))
		}

		reqURL := baseURL + "?" + params.Encode()
		fmt.Println("fetching", reqURL)

		resp, err := http.Get(reqURL)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		defer resp.Body.Close()

		var teams []*services.PlyMktTeam
		if err := json.NewDecoder(resp.Body).Decode(&teams); err != nil {
			fmt.Println(err)
			return nil, err
		}

		if len(teams) == 0 {
			break
		}

		fullTeams = append(fullTeams, teams...)
		offset += limit
	}
	
	return fullTeams, nil
}

func fetchEvents(tagID string) ([]*services.PlyMktEvent, error) {
	baseURL := "https://gamma-api.polymarket.com/events"
	limit := 500
	offset := 0

	var fullEvents []*services.PlyMktEvent
	params := url.Values{}
	params.Add("limit", strconv.Itoa(limit))
	params.Add("offset", "0")
	params.Add("tag_id", tagID)
	params.Add("active", "true")
	params.Add("closed", "false")
	params.Add("include_chat", "false")
	params.Add("order", "id")
	params.Add("ascending", "false")
	for {
		reqURL := baseURL + "?" + params.Encode()
		fmt.Println("fetching", reqURL)

		resp, err := http.Get(reqURL)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		defer resp.Body.Close()

		var events []*services.PlyMktEvent
		if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
			fmt.Println(err)
			return nil, err
		}

		if len(events) == 0 {
			break
		}

		fullEvents = append(fullEvents, events...)
		offset += limit
		params.Set("offset", strconv.Itoa(offset))
	}
	
	return fullEvents, nil
}
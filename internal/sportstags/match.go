package sportstags

import (
	"regexp"
	"strings"
)

var nonAlnumDash = regexp.MustCompile(`[^a-z0-9-]+`)

// TeamRow is the minimum team data needed for tag matching.
type TeamRow struct {
	Name         string
	League       string
	Abbreviation string
	Alias        string
}

// TagRef is a tag id + labels used for matching.
type TagRef struct {
	ID    string
	Label string
	Slug  string
}

// TeamTagMatch is a unique team-tag → league-title edge.
type TeamTagMatch struct {
	TeamTagID      string
	LeagueKey      string
	LeagueTitleTag string
}

// NormalizeKey lowercases, trims, maps spaces/_ to '-', strips other non-alnum.
func NormalizeKey(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return ""
	}
	s = strings.ReplaceAll(s, "_", "-")
	s = strings.ReplaceAll(s, " ", "-")
	s = nonAlnumDash.ReplaceAllString(s, "")
	for strings.Contains(s, "--") {
		s = strings.ReplaceAll(s, "--", "-")
	}
	return strings.Trim(s, "-")
}

// TeamParentEdges builds teamTagID → leagueTitleTagID from matches.
func TeamParentEdges(matches []TeamTagMatch) map[string]string {
	edges := map[string]string{}
	for _, m := range matches {
		if m.TeamTagID == "" || m.LeagueTitleTag == "" {
			continue
		}
		if m.TeamTagID == TagIDSports || m.TeamTagID == TagIDGames || FamilyTagIDs[m.TeamTagID] {
			continue
		}
		if m.TeamTagID == m.LeagueTitleTag {
			continue
		}
		edges[m.TeamTagID] = m.LeagueTitleTag
	}
	return edges
}

// MatchTeamsToTags pairs teams to unique tags by normalized name/slug/abbrev.
func MatchTeamsToTags(teams []TeamRow, tags []TagRef, leagueTitleByKey map[string]string) []TeamTagMatch {
	if len(teams) == 0 || len(tags) == 0 || len(leagueTitleByKey) == 0 {
		return nil
	}

	byKey := map[string][]string{}
	add := func(key, id string) {
		if key == "" || id == "" {
			return
		}
		if id == TagIDSports || id == TagIDGames || FamilyTagIDs[id] {
			return
		}
		for _, existing := range byKey[key] {
			if existing == id {
				return
			}
		}
		byKey[key] = append(byKey[key], id)
	}
	for _, t := range tags {
		if t.ID == "" {
			continue
		}
		add(NormalizeKey(t.Slug), t.ID)
		add(NormalizeKey(t.Label), t.ID)
	}

	var out []TeamTagMatch
	seenTeamTag := map[string]bool{}
	for _, team := range teams {
		leagueKey := strings.ToLower(strings.TrimSpace(team.League))
		if leagueKey == "" {
			continue
		}
		title, ok := leagueTitleByKey[leagueKey]
		if !ok || title == "" {
			continue
		}

		candidates := map[string]bool{}
		for _, raw := range []string{team.Name, team.Abbreviation, team.Alias} {
			key := NormalizeKey(raw)
			if key == "" {
				continue
			}
			for _, id := range byKey[key] {
				candidates[id] = true
			}
		}
		if len(candidates) != 1 {
			continue
		}
		var teamTagID string
		for id := range candidates {
			teamTagID = id
		}
		if teamTagID == title || seenTeamTag[teamTagID] {
			continue
		}
		if FamilyTagIDs[teamTagID] || teamTagID == TagIDSports {
			continue
		}
		seenTeamTag[teamTagID] = true
		out = append(out, TeamTagMatch{
			TeamTagID:      teamTagID,
			LeagueKey:      leagueKey,
			LeagueTitleTag: title,
		})
	}
	return out
}

// LeagueKeyTags is sport key + raw_tags CSV for title resolution.
type LeagueKeyTags struct {
	Sport   string
	RawTags string
}

// LeagueTitleByKey builds league key → canonical title tag from league rows.
func LeagueTitleByKey(leagues []LeagueKeyTags) map[string]string {
	rows := make([]SportsRow, 0, len(leagues))
	for _, l := range leagues {
		rows = append(rows, SportsRow{Sport: l.Sport, Tags: l.RawTags})
	}
	families := AutoFamilies(rows, DefaultFamilyMinFreq)
	out := map[string]string{}
	for _, l := range leagues {
		key := strings.ToLower(strings.TrimSpace(l.Sport))
		if key == "" {
			continue
		}
		family := FamilyTagForLeague(key, l.RawTags, families)
		title := LeagueTitleTag(l.RawTags, family, families)
		if title != "" {
			out[key] = title
		}
	}
	return out
}

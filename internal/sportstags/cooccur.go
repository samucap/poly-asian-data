package sportstags

import (
	"github.com/samucap/poly-asian-data/internal/services"
)

// InferTeamParentsFromEvents derives teamTag → leagueTitle edges from event co-occurrence.
//
// Polymarket /teams often omits major pro leagues (NBA/MLB/soccer clubs). Event tags
// still carry both league brand tags and team tags on the same event.
//
// Rule: when an event has exactly one known league-title tag (edge child whose parent is a
// sport family), every other non-meta tag on that event votes for that league as parent.
// A tag is assigned parent P only if P is the unique majority vote (strict: one clear winner).
func InferTeamParentsFromEvents(events []*services.PlyMktEvent, leagueEdges map[string]string) map[string]string {
	if len(events) == 0 || len(leagueEdges) == 0 {
		return nil
	}

	// League title tags: children in sports edges whose parent is a family mid-layer.
	leagueTitles := map[string]bool{}
	for child, parent := range leagueEdges {
		if child == "" || parent == "" {
			continue
		}
		if FamilyTagIDs[parent] {
			leagueTitles[child] = true
		}
	}
	if len(leagueTitles) == 0 {
		return nil
	}

	// votes[tagID][leagueTitleID] = count
	votes := map[string]map[string]int{}

	for _, e := range events {
		if e == nil {
			continue
		}
		var leaguesOnEvent []string
		var others []string
		seenL := map[string]bool{}
		seenO := map[string]bool{}
		for _, t := range e.Tags {
			if t == nil || t.ID == "" {
				continue
			}
			id := t.ID
			if id == TagIDSports || id == TagIDGames || FamilyTagIDs[id] {
				continue
			}
			if leagueTitles[id] {
				if !seenL[id] {
					seenL[id] = true
					leaguesOnEvent = append(leaguesOnEvent, id)
				}
				continue
			}
			// Skip tags that are already sport-hierarchy owned as league leaves of another family? 
			// If id is itself a league title, already handled. Other edge keys that parent to family
			// are league titles. Tags that parent to Sports directly in edges are also league-like
			// without family — treat as league titles too.
			if p, ok := leagueEdges[id]; ok && (FamilyTagIDs[p] || p == TagIDSports) {
				if !seenL[id] {
					seenL[id] = true
					leaguesOnEvent = append(leaguesOnEvent, id)
				}
				continue
			}
			if !seenO[id] {
				seenO[id] = true
				others = append(others, id)
			}
		}
		if len(leaguesOnEvent) != 1 || len(others) == 0 {
			continue
		}
		league := leaguesOnEvent[0]
		for _, o := range others {
			if o == league {
				continue
			}
			if votes[o] == nil {
				votes[o] = map[string]int{}
			}
			votes[o][league]++
		}
	}

	out := map[string]string{}
	for tagID, byLeague := range votes {
		best, bestN, second := "", 0, 0
		for league, n := range byLeague {
			if n > bestN {
				second = bestN
				bestN = n
				best = league
			} else if n > second {
				second = n
			}
		}
		// Require a clear winner (strict majority over second).
		if best != "" && bestN > second && bestN >= 1 {
			// Never parent family/meta.
			if FamilyTagIDs[tagID] || tagID == TagIDSports || tagID == TagIDGames {
				continue
			}
			if leagueTitles[tagID] {
				continue // league title is not a team
			}
			out[tagID] = best
		}
	}
	return out
}

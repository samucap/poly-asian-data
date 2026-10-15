package edgescan

// ExpandGroupCandidates adds in-pool sibling legs for neg-risk Stage-1 candidates
// so group mids can support fair-value complement. No Gamma HTTP.
//
// Complexity: O(|ByCond| + |Stage1|) via one group→members index.
// Returns expanded candidate list (Stage-1 first, then siblings) and count added.
func ExpandGroupCandidates(stage1 Stage1Result, expandCap int) (cands []Candidate, added int) {
	if expandCap < 0 {
		expandCap = 0
	}

	// groupID → members in pool
	byGroup := make(map[string][]Candidate, 64)
	for _, c := range stage1.ByCond {
		if c.Market == nil || c.NegRiskGroupID == "" {
			continue
		}
		byGroup[c.NegRiskGroupID] = append(byGroup[c.NegRiskGroupID], c)
	}

	seen := make(map[string]bool, len(stage1.Candidates)+expandCap)
	cands = make([]Candidate, 0, len(stage1.Candidates)+expandCap)
	for _, c := range stage1.Candidates {
		if c.Market == nil || c.Market.ConditionID == "" {
			continue
		}
		id := c.Market.ConditionID
		if seen[id] {
			continue
		}
		seen[id] = true
		cands = append(cands, c)
	}

	// Groups touched by Stage-1
	groupsNeeded := make(map[string]struct{}, 32)
	for _, c := range stage1.Candidates {
		if c.Market == nil || !c.NegRisk || c.NegRiskGroupID == "" {
			continue
		}
		groupsNeeded[c.NegRiskGroupID] = struct{}{}
	}

	for gid := range groupsNeeded {
		if added >= expandCap {
			break
		}
		for _, sib := range byGroup[gid] {
			if added >= expandCap {
				break
			}
			if sib.Market == nil {
				continue
			}
			id := sib.Market.ConditionID
			if seen[id] {
				continue
			}
			seen[id] = true
			cands = append(cands, sib)
			added++
		}
	}
	return cands, added
}

// BuildGroupMidIndex maps groupID → (conditionID → mid) from books.
func BuildGroupMidIndex(cands []Candidate, books BookIndex) map[string]map[string]float64 {
	byGroup := map[string]map[string]float64{}
	for _, c := range cands {
		if c.Market == nil || c.NegRiskGroupID == "" {
			continue
		}
		toks := parseTokenIDs(c.Market.ClobTokenIds)
		mid := midFromBooks(toks, books)
		if mid <= 0 {
			continue
		}
		g := c.NegRiskGroupID
		if byGroup[g] == nil {
			byGroup[g] = map[string]float64{}
		}
		byGroup[g][c.Market.ConditionID] = mid
	}
	return byGroup
}

// GroupMidsFor returns mids map for a candidate's group (may be nil).
func GroupMidsFor(c Candidate, byGroup map[string]map[string]float64) map[string]float64 {
	if c.NegRiskGroupID == "" {
		return nil
	}
	return byGroup[c.NegRiskGroupID]
}

// GroupKnownSize returns how many pool members share the candidate's neg-risk group.
func GroupKnownSize(c Candidate, byCond map[string]Candidate) int {
	if c.NegRiskGroupID == "" {
		return 0
	}
	n := 0
	for _, o := range byCond {
		if o.NegRiskGroupID == c.NegRiskGroupID {
			n++
		}
	}
	return n
}

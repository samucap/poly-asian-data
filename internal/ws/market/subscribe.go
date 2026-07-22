package market

// DiffSubscriptions returns assets to add/remove given desired vs currently subscribed sets.
// Order of desired is preserved for Subscribe; Unsubscribe is sorted by appearance in current.
func DiffSubscriptions(current, desired []string) SubDiff {
	cur := make(map[string]struct{}, len(current))
	for _, id := range current {
		if id != "" {
			cur[id] = struct{}{}
		}
	}
	des := make(map[string]struct{}, len(desired))
	var sub []string
	for _, id := range desired {
		if id == "" {
			continue
		}
		des[id] = struct{}{}
		if _, ok := cur[id]; !ok {
			sub = append(sub, id)
		}
	}
	var unsub []string
	for _, id := range current {
		if id == "" {
			continue
		}
		if _, ok := des[id]; !ok {
			unsub = append(unsub, id)
		}
	}
	return SubDiff{Subscribe: sub, Unsubscribe: unsub}
}

// CapAssets hard-caps ordered asset list.
func CapAssets(ids []string, max int) []string {
	if max <= 0 || len(ids) <= max {
		out := make([]string, len(ids))
		copy(out, ids)
		return out
	}
	out := make([]string, max)
	copy(out, ids[:max])
	return out
}

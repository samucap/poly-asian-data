// Package tagagg classifies Polymarket events into category tags and aggregates
// tradable market metrics for treemap-style per-tag stats.
//
// Each cycle recomputes totals from the current event scan. Catalog seed entries
// are treated as read-only definitions; metrics accumulate on working copies only.
package tagagg

import (
	"sort"
	"strings"

	"github.com/samucap/poly-asian-data/internal/services"
)

// Catch-all / meta tag IDs that must not act as hierarchy parents or top categories.
// Gamma tag 100215 "All" is related to the categories root but only has ~1 active event;
// BFS-related parenting under it previously inflated volume/market attribution.
const catchAllTagID = "100215"

// Result holds per-tag totals and classified market lists from one scan pass.
type Result struct {
	// Tags: working copies (defs + cycle totals). Includes idle catalog tags at zero
	// and any tags discovered on events this cycle. Safe for db.UpdateTags.
	Tags map[string]*services.PlyMktTag
	// Markets: value copies of all markets (Category set); suitable for DB save.
	Markets []services.PlyMktMarket
	// Tradable: pointers into event markets used for ranking (same objects as e.Markets[i]).
	Tradable []*services.PlyMktMarket
	// PoolBySlug: tradable market count per category slug ("" = uncategorized).
	PoolBySlug map[string]int
	// CondCategory: conditionID → category slug for selected-set reporting.
	CondCategory map[string]string
	// CondMarket: conditionID → pointer into Markets (for OI merge into save rows).
	CondMarket map[string]*services.PlyMktMarket
	// UnresolvedMarkets: tradable markets with no top and no usable leaf path.
	UnresolvedMarkets int
	// EventCountByTag: unique events that contributed metrics to each tag id this cycle.
	// Compare to services.PlyMktTag.ActiveEventsCount from Gamma for validation.
	EventCountByTag map[string]int
}

// IsTradable reports whether a market is eligible for ranking and category metrics.
// Gates: order book enabled, accepting orders, active, not closed, no closedTime.
func IsTradable(m *services.PlyMktMarket) bool {
	if m == nil {
		return false
	}
	return m.EnableOrderBook && m.AcceptingOrders && m.Active && !m.Closed && m.ClosedTime == ""
}

// IsCatchAllTag reports meta/catch-all tags that are not real category parents.
// Example: id 100215 slug "all" — Gamma activeEventsCount≈1 but related-tags BFS
// previously hung most of the tree under it.
func IsCatchAllTag(t *services.PlyMktTag) bool {
	if t == nil || t.ID == "" {
		return false
	}
	if t.ID == catchAllTagID {
		return true
	}
	slug := strings.ToLower(strings.TrimSpace(t.Slug))
	if slug == "all" {
		return true
	}
	return false
}

// IsTopCategory reports whether t is a direct child of the categories root
// and is not a catch-all meta tag.
func IsTopCategory(t *services.PlyMktTag, rootID string) bool {
	return t != nil && t.ID != "" && rootID != "" && t.ParentTagID == rootID && !IsCatchAllTag(t)
}

// ResolveTopOf walks parent_tag_id links until a real top category (parent == rootID,
// not catch-all). Catch-all nodes are skipped so volume does not collapse into "All".
func ResolveTopOf(t *services.PlyMktTag, byID map[string]*services.PlyMktTag, rootID string) *services.PlyMktTag {
	if t == nil || byID == nil || rootID == "" {
		return nil
	}
	seen := map[string]bool{}
	cur := t
	for cur != nil && cur.ID != "" && !seen[cur.ID] {
		seen[cur.ID] = true
		if IsCatchAllTag(cur) {
			if cur.ParentTagID == "" || cur.ParentTagID == rootID {
				return nil
			}
			cur = byID[cur.ParentTagID]
			continue
		}
		if cur.ParentTagID == rootID {
			return cur
		}
		if cur.ParentTagID == "" {
			return nil
		}
		cur = byID[cur.ParentTagID]
	}
	return nil
}

// Aggregate classifies events and attributes each tradable market's metrics to tags.
//
// Catalog seed entries are not mutated; totals live on working copies in Result.Tags.
// Aggregates are fully recomputed every call (per cycle), not accumulated across cycles.
//
// Attribution per tradable market (each tag id at most once):
//  1. Every real top category on the event / via parent walk (multi-label).
//  2. Every non-catch-all tag that appears directly on the event.
//  3. One primary non-top leaf (first non-top, non-catch-all event tag).
//  4. Ancestors of that leaf up to a real top — catch-all nodes are never credited
//     and never used as discovery parents (see IsCatchAllTag).
//
// Unknown event tags are discovered and parented under the first real top when possible.
func Aggregate(events []*services.PlyMktEvent, catalog map[string]*services.PlyMktTag, rootID string) Result {
	res := Result{
		Tags:            make(map[string]*services.PlyMktTag),
		PoolBySlug:      make(map[string]int),
		CondCategory:    make(map[string]string),
		CondMarket:      make(map[string]*services.PlyMktMarket),
		EventCountByTag: make(map[string]int),
	}

	// Working copies of the seed catalog (zero metrics). Idle tags stay at zero for write-back.
	for id, t := range catalog {
		if t == nil || id == "" || id == rootID {
			continue
		}
		res.Tags[id] = zeroCopy(t)
	}

	if len(events) == 0 {
		return res
	}

	for _, e := range events {
		if e == nil {
			continue
		}

		// --- Discover unknown event tags, then collect tops (multi-label). ---
		// Multiple tops on one event mean the market belongs to multiple top categories.
		// Attribute metrics to each top (multi-label), not "first top wins" exclusivity.
		// Outer treemap cells can therefore sum to more than global volume when events
		// are cross-tagged; that reflects real multi-category membership.
		topsByID := map[string]*services.PlyMktTag{}
		var firstTop *services.PlyMktTag

		// Pass 1: ensure known tags exist; collect real tops from known tags / parent walk.
		for _, et := range e.Tags {
			if et == nil || et.ID == "" || et.ID == rootID || IsCatchAllTag(et) {
				continue
			}
			w, ok := res.Tags[et.ID]
			if !ok {
				continue // discover in pass 2 after firstTop is known
			}
			if IsCatchAllTag(w) {
				continue
			}
			if IsTopCategory(w, rootID) {
				topsByID[w.ID] = w
				if firstTop == nil {
					firstTop = w
				}
				continue
			}
			if top := ResolveTopOf(w, res.Tags, rootID); top != nil {
				topsByID[top.ID] = top
				if firstTop == nil {
					firstTop = top
				}
			}
		}

		// Pass 2: discover missing tags under first real top (never under catch-all).
		for _, et := range e.Tags {
			if et == nil || et.ID == "" || et.ID == rootID {
				continue
			}
			if _, ok := res.Tags[et.ID]; ok {
				continue
			}
			parentID := rootID
			if firstTop != nil && !IsCatchAllTag(firstTop) {
				parentID = firstTop.ID
			}
			// Catch-all on the event: keep as catalog node under root, never as top parent.
			if IsCatchAllTag(et) {
				parentID = rootID
			}
			discovered := zeroCopy(et)
			discovered.ParentTagID = parentID
			discovered.TotalVol = 0
			discovered.TotalVol24hr = 0
			discovered.TotalLiq = 0
			discovered.TotalMarkets = 0
			res.Tags[discovered.ID] = discovered

			if IsCatchAllTag(discovered) {
				continue
			}
			if IsTopCategory(discovered, rootID) {
				topsByID[discovered.ID] = discovered
				if firstTop == nil {
					firstTop = discovered
				}
			} else if top := ResolveTopOf(discovered, res.Tags, rootID); top != nil {
				topsByID[top.ID] = top
				if firstTop == nil {
					firstTop = top
				}
			}
		}

		// Primary leaf: first non-top, non-catch-all event tag in event order.
		var primaryLeaf *services.PlyMktTag
		for _, et := range e.Tags {
			if et == nil || et.ID == "" || et.ID == rootID || IsCatchAllTag(et) {
				continue
			}
			w := res.Tags[et.ID]
			if w == nil || IsTopCategory(w, rootID) || IsCatchAllTag(w) {
				continue
			}
			primaryLeaf = w
			break
		}

		// Tags to credit for this event (union of tops, direct tags, leaf path).
		eventCreditIDs := map[string]bool{}

		catSlug := ""
		if firstTop != nil {
			catSlug = firstTop.Slug
		}

		for i := range e.Markets {
			m := e.Markets[i]
			if m == nil {
				continue
			}
			m.EventID = e.ID
			m.Category = catSlug

			res.Markets = append(res.Markets, *m)

			if !IsTradable(m) {
				continue
			}

			res.Tradable = append(res.Tradable, m)
			res.PoolBySlug[catSlug]++
			if m.ConditionID != "" {
				res.CondCategory[m.ConditionID] = catSlug
			}

			if len(topsByID) == 0 && primaryLeaf == nil {
				// Still try direct non-catch-all tags on the event.
				hasDirect := false
				for _, et := range e.Tags {
					if et != nil && et.ID != "" && et.ID != rootID && !IsCatchAllTag(et) {
						hasDirect = true
						break
					}
				}
				if !hasDirect {
					res.UnresolvedMarkets++
					continue
				}
			}

			credited := map[string]bool{}
			credit := func(t *services.PlyMktTag) {
				if t == nil || t.ID == "" || t.ID == rootID || credited[t.ID] {
					return
				}
				// Never roll metrics into catch-all "All" (100215) via hierarchy.
				// Direct event membership of catch-all is also ignored for aggregates
				// (Gamma reports ~1 active event; hierarchy inflation was the bug).
				if IsCatchAllTag(t) {
					return
				}
				credited[t.ID] = true
				eventCreditIDs[t.ID] = true
				w := res.Tags[t.ID]
				if w == nil {
					w = zeroCopy(t)
					res.Tags[t.ID] = w
				}
				w.TotalVol += volumeClob(m)
				w.TotalVol24hr += volume24hrClob(m)
				w.TotalLiq += liquidityClob(m)
				w.TotalMarkets++
			}

			for _, top := range topsByID {
				credit(top)
			}
			// Direct membership: every non-catch-all tag on the event.
			for _, et := range e.Tags {
				if et == nil || et.ID == "" || et.ID == rootID {
					continue
				}
				if w := res.Tags[et.ID]; w != nil {
					credit(w)
				}
			}
			if primaryLeaf != nil {
				credit(primaryLeaf)
				// Roll up ancestors to a real top; skip catch-all nodes.
				cur := res.Tags[primaryLeaf.ParentTagID]
				seen := map[string]bool{primaryLeaf.ID: true}
				for cur != nil && cur.ID != "" && cur.ID != rootID && !seen[cur.ID] {
					seen[cur.ID] = true
					if IsCatchAllTag(cur) {
						if cur.ParentTagID == "" || cur.ParentTagID == rootID {
							break
						}
						cur = res.Tags[cur.ParentTagID]
						continue
					}
					credit(cur)
					if cur.ParentTagID == rootID || cur.ParentTagID == "" {
						break
					}
					cur = res.Tags[cur.ParentTagID]
				}
			}
		}

		// Unique event attribution (once per tag per event).
		for tid := range eventCreditIDs {
			res.EventCountByTag[tid]++
		}
	}

	// Build CondMarket after the slice is final so pointers stay valid (no realloc).
	for i := range res.Markets {
		if cid := res.Markets[i].ConditionID; cid != "" {
			res.CondMarket[cid] = &res.Markets[i]
		}
	}
	return res
}

func zeroCopy(t *services.PlyMktTag) *services.PlyMktTag {
	if t == nil {
		return nil
	}
	cp := *t
	cp.TotalVol = 0
	cp.TotalVol24hr = 0
	cp.TotalLiq = 0
	cp.TotalMarkets = 0
	return &cp
}

func volumeClob(m *services.PlyMktMarket) float64 {
	if m.VolumeClob != 0 {
		return m.VolumeClob
	}
	return m.VolumeNum
}

func volume24hrClob(m *services.PlyMktMarket) float64 {
	if m.Volume24hrClob != 0 {
		return m.Volume24hrClob
	}
	return m.Volume24hr
}

func liquidityClob(m *services.PlyMktMarket) float64 {
	if m.LiquidityClob != 0 {
		return m.LiquidityClob
	}
	return m.LiquidityNum
}

// TagsForUpdate returns working tags as a slice for db.UpdateTags (stable enough for batch).
func TagsForUpdate(tags map[string]*services.PlyMktTag) []*services.PlyMktTag {
	if len(tags) == 0 {
		return nil
	}
	out := make([]*services.PlyMktTag, 0, len(tags))
	for _, t := range tags {
		if t == nil || t.ID == "" {
			continue
		}
		out = append(out, t)
	}
	return out
}

// SortedByVol24 returns tags with activity sorted by TotalVol24hr descending (stable id tie-break).
func SortedByVol24(tags map[string]*services.PlyMktTag) []*services.PlyMktTag {
	out := make([]*services.PlyMktTag, 0, len(tags))
	for _, t := range tags {
		if t != nil && t.TotalMarkets > 0 {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TotalVol24hr == out[j].TotalVol24hr {
			return out[i].ID < out[j].ID
		}
		return out[i].TotalVol24hr > out[j].TotalVol24hr
	})
	return out
}

// CatalogToMap builds a classification map from tags, skipping nil/empty/root.
func CatalogToMap(tags []*services.PlyMktTag, skipRootID string) map[string]*services.PlyMktTag {
	out := make(map[string]*services.PlyMktTag, len(tags))
	for _, t := range tags {
		if t == nil || t.ID == "" || t.ID == skipRootID {
			continue
		}
		out[t.ID] = t
	}
	return out
}

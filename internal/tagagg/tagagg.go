// Package tagagg classifies Polymarket events into top-level category tags and
// aggregates tradable market metrics onto those categories only (no subtag fan-out).
package tagagg

import (
	"sort"

	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/services"
)

// Result holds category totals and classified market lists from one scan pass.
type Result struct {
	// Categories: top-tag id → working copy with TotalVol / TotalVol24hr / TotalLiq / TotalMarkets.
	Categories map[string]*services.PlyMktTag
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
}

// TopCategory returns the first event tag that is a known top-level category, or nil.
func TopCategory(eventTags []*services.PlyMktTag, topByID map[string]*services.PlyMktTag) *services.PlyMktTag {
	if topByID == nil {
		return nil
	}
	for _, tag := range eventTags {
		if tag == nil || tag.ID == "" {
			continue
		}
		if t, ok := topByID[tag.ID]; ok && t != nil {
			return t
		}
	}
	return nil
}

// IsTradable reports whether a market is eligible for ranking and category metrics.
// Gates: order book enabled, accepting orders, active, not closed, no closedTime.
func IsTradable(m *services.PlyMktMarket) bool {
	if m == nil {
		return false
	}
	return m.EnableOrderBook && m.AcceptingOrders && m.Active && !m.Closed && m.ClosedTime == ""
}

// Aggregate classifies each event once and adds each tradable market's metrics
// once to that top category only. Subtags never receive totals.
func Aggregate(events []*services.PlyMktEvent, topByID map[string]*services.PlyMktTag) Result {
	res := Result{
		Categories:   make(map[string]*services.PlyMktTag),
		PoolBySlug:   make(map[string]int),
		CondCategory: make(map[string]string),
		CondMarket:   make(map[string]*services.PlyMktMarket),
	}
	if len(events) == 0 {
		return res
	}

	for _, e := range events {
		if e == nil {
			continue
		}
		top := TopCategory(e.Tags, topByID)
		catSlug := ""
		var cat *services.PlyMktTag
		if top != nil {
			catSlug = top.Slug
			cat = res.Categories[top.ID]
			if cat == nil {
				// Working copy so we do not mutate the catalog map entries.
				cp := *top
				cp.TotalVol = 0
				cp.TotalVol24hr = 0
				cp.TotalLiq = 0
				cp.TotalMarkets = 0
				cat = &cp
				res.Categories[top.ID] = cat
			}
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

			if cat == nil {
				continue
			}
			cat.TotalVol += volumeClob(m)
			cat.TotalVol24hr += volume24hrClob(m)
			cat.TotalLiq += liquidityClob(m)
			cat.TotalMarkets++
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

// ToTagAggregates builds DB update rows for every known top tag in topByID.
// Categories present in cats keep their cycle totals; top tags with no tradable
// markets this cycle are written as zeros so stale totals do not linger.
// If topByID is nil/empty, only cats with non-empty ids are emitted.
func ToTagAggregates(cats map[string]*services.PlyMktTag, topByID map[string]*services.PlyMktTag) []db.TagAggregate {
	if len(topByID) == 0 {
		if len(cats) == 0 {
			return nil
		}
		out := make([]db.TagAggregate, 0, len(cats))
		for _, t := range cats {
			if t == nil || t.ID == "" {
				continue
			}
			out = append(out, db.TagAggregate{
				ID:           t.ID,
				TotalVol:     t.TotalVol,
				TotalVol24hr: t.TotalVol24hr,
				TotalLiq:     t.TotalLiq,
				TotalMarkets: t.TotalMarkets,
			})
		}
		return out
	}

	out := make([]db.TagAggregate, 0, len(topByID))
	for id, top := range topByID {
		if id == "" || top == nil {
			continue
		}
		if t, ok := cats[id]; ok && t != nil {
			out = append(out, db.TagAggregate{
				ID:           id,
				TotalVol:     t.TotalVol,
				TotalVol24hr: t.TotalVol24hr,
				TotalLiq:     t.TotalLiq,
				TotalMarkets: t.TotalMarkets,
			})
			continue
		}
		// No markets attributed this cycle — reset aggregates.
		out = append(out, db.TagAggregate{ID: id})
	}
	return out
}

// SortedByVol24 returns categories sorted by TotalVol24hr descending (stable id tie-break).
func SortedByVol24(cats map[string]*services.PlyMktTag) []*services.PlyMktTag {
	out := make([]*services.PlyMktTag, 0, len(cats))
	for _, t := range cats {
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

// TopByID builds a classification map from top-category tags, skipping nil/empty/root.
func TopByID(tags []*services.PlyMktTag, skipRootID string) map[string]*services.PlyMktTag {
	out := make(map[string]*services.PlyMktTag, len(tags))
	for _, t := range tags {
		if t == nil || t.ID == "" || t.ID == skipRootID {
			continue
		}
		out[t.ID] = t
	}
	return out
}

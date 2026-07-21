package catalog

import (
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/samucap/poly-asian-data/internal/artifacts"
	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/samucap/poly-asian-data/internal/tagagg"
)

// CatalogV1 is the versioned catalog pipeline artifact.
type CatalogV1 struct {
	artifacts.Envelope
	UniverseStats    UniverseStats `json:"universe_stats"`
	Markets          []MarketRow   `json:"markets"`
	Tags             []TagRow      `json:"tags"`
	DiffFromPrevious Diff          `json:"diff_from_previous"`
}

// UniverseStats is aggregate catalog summary.
type UniverseStats struct {
	TotalMarkets     int            `json:"total_markets"`
	Tradable         int            `json:"tradable"`
	TotalActive      int            `json:"total_active"`
	NegRiskMarkets   int            `json:"neg_risk_markets"`
	NegRiskGroups    int            `json:"neg_risk_groups"`
	ByTag            map[string]int `json:"by_tag"`
	ByLiquidityTier map[string]int `json:"by_liquidity_tier"`
}

// MarketRow is a compact structured market for the catalog artifact.
type MarketRow struct {
	MarketID         string   `json:"market_id"`
	ConditionID      string   `json:"condition_id"`
	EventID          string   `json:"event_id,omitempty"`
	Question         string   `json:"question,omitempty"`
	Slug             string   `json:"slug,omitempty"`
	Active           bool     `json:"active"`
	Closed           bool     `json:"closed"`
	Archived         bool     `json:"archived"`
	ClobTokenIDs     []string `json:"clob_token_ids"`
	Category         string   `json:"category,omitempty"`
	EndDate          string   `json:"end_date,omitempty"`
	DaysToResolution *float64 `json:"days_to_resolution,omitempty"`
	LiquidityTier   string   `json:"liquidity_tier"`
	Volume24hr       float64  `json:"volume_24hr"`
	Liquidity        float64  `json:"liquidity"`
	NegRisk          bool     `json:"neg_risk"`
	NegRiskGroupID   string   `json:"neg_risk_group_id,omitempty"`
	NegRiskFeeBips   int      `json:"neg_risk_fee_bips,omitempty"`
	IsNegRiskLeg     bool     `json:"is_neg_risk_leg"`
	Tradable         bool     `json:"tradable"`
}

// TagRow is a tag summary for the catalog artifact.
type TagRow struct {
	ID                 string  `json:"id"`
	Label              string  `json:"label,omitempty"`
	Slug               string  `json:"slug,omitempty"`
	ParentID           string  `json:"parent_id,omitempty"`
	MarketCount        int     `json:"market_count"`
	VolumeRank         int     `json:"volume_rank"`
	TotalVol24         float64 `json:"total_vol_24hr,omitempty"`
	ActiveEventsCount  int     `json:"active_events_count,omitempty"`
	AttributedEvents   int     `json:"attributed_events,omitempty"`
	IsCatchAll         bool    `json:"is_catch_all,omitempty"`
}

// Diff is catalog change vs previous latest artifact.
type Diff struct {
	Added         []string            `json:"added"`
	Removed       []string            `json:"removed"`
	StatusChanged []StatusChange      `json:"status_changed"`
}

// StatusChange records active/closed flips for a condition.
type StatusChange struct {
	ConditionID string `json:"condition_id"`
	Field       string `json:"field"`
	From        any    `json:"from"`
	To          any    `json:"to"`
}

// eventNegRisk carries event-level neg-risk onto markets.
type eventNegRisk struct {
	NegRisk     bool
	GroupID     string
	FeeBips     int
}

// BuildCatalogV1 builds a catalog artifact from a full event scan + aggregate result.
func BuildCatalogV1(
	events []*services.PlyMktEvent,
	agg tagagg.Result,
	prev *CatalogV1,
	status string,
	errs []artifacts.ErrorItem,
) (CatalogV1, error) {
	// Map event_id → neg risk from scan (authoritative for groups).
	byEvent := map[string]eventNegRisk{}
	for _, e := range events {
		if e == nil || e.ID == "" {
			continue
		}
		byEvent[e.ID] = eventNegRisk{
			NegRisk: e.NegRisk || e.EnableNegRisk,
			GroupID: e.NegRiskMarketID,
			FeeBips: e.NegRiskFeeBips,
		}
	}

	now := time.Now()
	markets := make([]MarketRow, 0, len(agg.Markets))
	activeN := 0
	negRiskMarkets := 0
	groups := map[string]struct{}{}
	byTier := map[string]int{}
	byTag := map[string]int{}

	for i := range agg.Markets {
		m := &agg.Markets[i]
		nr := byEvent[m.EventID]
		liq := marketLiquidity(m)
		tier := liquidityTier(liq)
		byTier[tier]++

		vol24 := m.Volume24hr
		if m.Volume24hrClob != 0 {
			vol24 = m.Volume24hrClob
		}

		tokens := parseTokenIDs(m.ClobTokenIds)
		tradable := tagagg.IsTradable(m)
		row := MarketRow{
			MarketID:       m.ID,
			ConditionID:    m.ConditionID,
			EventID:        m.EventID,
			Question:       m.Question,
			Slug:           m.Slug,
			Active:         m.Active,
			Closed:         m.Closed,
			Archived:       m.Archived,
			ClobTokenIDs:   tokens,
			Category:       m.Category,
			LiquidityTier:  tier,
			Volume24hr:     vol24,
			Liquidity:      liq,
			NegRisk:        nr.NegRisk,
			NegRiskGroupID: nr.GroupID,
			NegRiskFeeBips: nr.FeeBips,
			IsNegRiskLeg:   nr.NegRisk || nr.GroupID != "",
			Tradable:       tradable,
		}
		if !m.EndDate.IsZero() {
			row.EndDate = m.EndDate.UTC().Format(time.RFC3339)
			d := m.EndDate.Sub(now).Hours() / 24.0
			row.DaysToResolution = &d
		}
		if m.Active && !m.Closed {
			activeN++
		}
		if row.IsNegRiskLeg {
			negRiskMarkets++
			if row.NegRiskGroupID != "" {
				groups[row.NegRiskGroupID] = struct{}{}
			}
		}
		if m.Category != "" {
			byTag[m.Category]++
		} else {
			byTag["uncategorized"]++
		}
		markets = append(markets, row)
	}

	// Stable market order by condition_id for cleaner diffs/hashes of structure.
	sort.Slice(markets, func(i, j int) bool {
		return markets[i].ConditionID < markets[j].ConditionID
	})

	tags := buildTagRows(agg)
	diff := Diff{Added: []string{}, Removed: []string{}, StatusChanged: []StatusChange{}}
	if prev != nil {
		diff = computeDiff(prev.Markets, markets)
	}

	// Input hash over stable content (exclude envelope times).
	hashIn := struct {
		Markets []MarketRow `json:"markets"`
		Tags    []TagRow    `json:"tags"`
	}{Markets: markets, Tags: tags}
	inputHash, err := artifacts.HashCanonicalJSON(hashIn)
	if err != nil {
		return CatalogV1{}, err
	}

	env := artifacts.NewEnvelope(SchemaVersion, inputHash)
	if status != "" {
		env.Status = status
	}
	if errs == nil {
		errs = []artifacts.ErrorItem{}
	}
	env.Errors = errs

	doc := CatalogV1{
		Envelope: env,
		UniverseStats: UniverseStats{
			TotalMarkets:     len(markets),
			Tradable:         len(agg.Tradable),
			TotalActive:      activeN,
			NegRiskMarkets:   negRiskMarkets,
			NegRiskGroups:    len(groups),
			ByTag:            byTag,
			ByLiquidityTier: byTier,
		},
		Markets:          markets,
		Tags:             tags,
		DiffFromPrevious: diff,
	}
	return doc, nil
}

func buildTagRows(agg tagagg.Result) []TagRow {
	sorted := tagagg.SortedByVol24(agg.Tags)
	var all []*services.PlyMktTag
	seen := map[string]bool{}
	for _, t := range sorted {
		if t == nil || t.ID == "" {
			continue
		}
		all = append(all, t)
		seen[t.ID] = true
	}
	for id, t := range agg.Tags {
		if t == nil || id == "" || seen[id] {
			continue
		}
		all = append(all, t)
	}
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].TotalVol24hr == all[j].TotalVol24hr {
			return all[i].ID < all[j].ID
		}
		return all[i].TotalVol24hr > all[j].TotalVol24hr
	})
	out := make([]TagRow, 0, len(all))
	for i, t := range all {
		attrEv := 0
		if agg.EventCountByTag != nil {
			attrEv = agg.EventCountByTag[t.ID]
		}
		out = append(out, TagRow{
			ID:                t.ID,
			Label:             t.Label,
			Slug:              t.Slug,
			ParentID:          t.ParentTagID,
			MarketCount:       t.TotalMarkets,
			VolumeRank:        i + 1,
			TotalVol24:        t.TotalVol24hr,
			ActiveEventsCount: t.ActiveEventsCount,
			AttributedEvents:  attrEv,
			IsCatchAll:        tagagg.IsCatchAllTag(t),
		})
	}
	return out
}

func computeDiff(prev, cur []MarketRow) Diff {
	prevBy := map[string]MarketRow{}
	for _, m := range prev {
		if m.ConditionID == "" {
			continue
		}
		prevBy[m.ConditionID] = m
	}
	curBy := map[string]MarketRow{}
	for _, m := range cur {
		if m.ConditionID == "" {
			continue
		}
		curBy[m.ConditionID] = m
	}
	d := Diff{Added: []string{}, Removed: []string{}, StatusChanged: []StatusChange{}}
	for id := range curBy {
		if _, ok := prevBy[id]; !ok {
			d.Added = append(d.Added, id)
		}
	}
	for id := range prevBy {
		if _, ok := curBy[id]; !ok {
			d.Removed = append(d.Removed, id)
		}
	}
	for id, c := range curBy {
		p, ok := prevBy[id]
		if !ok {
			continue
		}
		if p.Active != c.Active {
			d.StatusChanged = append(d.StatusChanged, StatusChange{ConditionID: id, Field: "active", From: p.Active, To: c.Active})
		}
		if p.Closed != c.Closed {
			d.StatusChanged = append(d.StatusChanged, StatusChange{ConditionID: id, Field: "closed", From: p.Closed, To: c.Closed})
		}
		if p.Tradable != c.Tradable {
			d.StatusChanged = append(d.StatusChanged, StatusChange{ConditionID: id, Field: "tradable", From: p.Tradable, To: c.Tradable})
		}
	}
	sort.Strings(d.Added)
	sort.Strings(d.Removed)
	sort.Slice(d.StatusChanged, func(i, j int) bool {
		if d.StatusChanged[i].ConditionID == d.StatusChanged[j].ConditionID {
			return d.StatusChanged[i].Field < d.StatusChanged[j].Field
		}
		return d.StatusChanged[i].ConditionID < d.StatusChanged[j].ConditionID
	})
	return d
}

func marketLiquidity(m *services.PlyMktMarket) float64 {
	if m.LiquidityClob != 0 {
		return m.LiquidityClob
	}
	if m.LiquidityNum != 0 {
		return m.LiquidityNum
	}
	if m.Liquidity != "" {
		if v, err := strconv.ParseFloat(m.Liquidity, 64); err == nil {
			return v
		}
	}
	return 0
}

func liquidityTier(liq float64) string {
	switch {
	case liq >= 100_000:
		return "high"
	case liq >= 15_000:
		return "medium"
	case liq > 0:
		return "low"
	default:
		return "none"
	}
}

func parseTokenIDs(raw string) []string {
	if raw == "" {
		return []string{}
	}
	var ids []string
	if err := json.Unmarshal([]byte(raw), &ids); err != nil {
		return []string{}
	}
	if ids == nil {
		return []string{}
	}
	return ids
}

// LoadPreviousCatalog reads artifacts/catalog/latest.json if present.
func LoadPreviousCatalog(root string) (*CatalogV1, error) {
	path := artifacts.LatestPath(root, ArtifactPipeline)
	var prev CatalogV1
	if err := artifacts.ReadJSONFile(path, &prev); err != nil {
		return nil, err
	}
	return &prev, nil
}

// WriteCatalogArtifact validates and writes catalog_v1 to disk.
func WriteCatalogArtifact(doc CatalogV1, root string) (artifacts.WriteResult, error) {
	if _, err := artifacts.ValidateAndMarshalCatalog(doc); err != nil {
		return artifacts.WriteResult{}, err
	}
	return artifacts.WriteJSON(doc.RunID, ArtifactPipeline, doc, artifacts.WriteOptions{
		Root:        root,
		WriteLatest: true,
	})
}

// RoundDays is a small helper for tests.
func RoundDays(d float64) float64 {
	return math.Round(d*1000) / 1000
}

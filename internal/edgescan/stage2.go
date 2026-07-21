package edgescan

import (
	"encoding/json"
	"math"
	"sort"
	"time"

	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/edge"
	"github.com/samucap/poly-asian-data/internal/enrich"
	"github.com/samucap/poly-asian-data/internal/marketranking"
	"github.com/samucap/poly-asian-data/internal/services"
)

// Stage1Result is the activity-budget candidate set (not a published board).
type Stage1Result struct {
	Candidates     []Candidate
	PoolCount      int
	Stage1Count    int
	DroppedSummary map[string]int
	// ByCond indexes all pool candidates (for related legs / sticky).
	ByCond map[string]Candidate
}

// SelectStage1 filters and ranks by activity up to Filter.MaxN (budget gate only).
func SelectStage1(pool []Candidate, opts BuildOptions) Stage1Result {
	dropped := map[string]int{
		"not_tradable_or_empty_pool": 0,
		"stage1_filter":              0,
		"stage1_cap":                 0,
	}
	if opts.Now.IsZero() {
		opts.Now = time.Now().UTC()
	}
	if opts.Filter.MaxN <= 0 {
		opts.Filter.MaxN = 200
	}
	if len(pool) == 0 {
		dropped["not_tradable_or_empty_pool"] = 1
		return Stage1Result{DroppedSummary: dropped, ByCond: map[string]Candidate{}}
	}

	byCond := map[string]Candidate{}
	markets := make([]*services.PlyMktMarket, 0, len(pool))
	for _, c := range pool {
		if c.Market == nil || c.Market.ConditionID == "" {
			continue
		}
		byCond[c.Market.ConditionID] = c
		markets = append(markets, c.Market)
	}

	ranked := marketranking.RankMarkets(markets, opts.Filter)
	stage1Count := len(ranked)
	dropped["stage1_filter"] = len(markets) - stage1Count

	// Sticky inject if still pass hard filter
	stickySet := map[string]bool{}
	for _, id := range opts.StickyConditionIDs {
		if id != "" {
			stickySet[id] = true
		}
	}
	inRanked := map[string]bool{}
	for _, m := range ranked {
		if m != nil {
			inRanked[m.ConditionID] = true
		}
	}
	for id := range stickySet {
		if inRanked[id] {
			continue
		}
		c, ok := byCond[id]
		if !ok || c.Market == nil {
			continue
		}
		if passesHardFilter(c.Market, opts.Filter) {
			c.Market.ComputedScore += 1e-6
			ranked = append(ranked, c.Market)
			inRanked[id] = true
		}
	}
	sort.SliceStable(ranked, func(i, j int) bool {
		return ranked[i].ComputedScore > ranked[j].ComputedScore
	})
	if opts.Filter.MaxN > 0 && len(ranked) > opts.Filter.MaxN {
		// Prefer sticky when trimming stage-1 budget
		ranked = selectBoard(ranked, stickySet, opts.Filter.MaxN)
		dropped["stage1_cap"] = stage1Count - len(ranked)
		if dropped["stage1_cap"] < 0 {
			dropped["stage1_cap"] = 0
		}
	}

	cands := make([]Candidate, 0, len(ranked))
	for _, m := range ranked {
		if m == nil {
			continue
		}
		if c, ok := byCond[m.ConditionID]; ok {
			cands = append(cands, c)
		}
	}
	return Stage1Result{
		Candidates:     cands,
		PoolCount:      len(pool),
		Stage1Count:    len(cands),
		DroppedSummary: dropped,
		ByCond:         byCond,
	}
}

// BookIndex maps token_id → book snapshot (this cycle or DB).
type BookIndex map[string]enrich.BookSnapshot

// OIIndex maps condition_id → OI value.
type OIIndex map[string]float64

// EdgeBuildOptions controls Stage-2 cost-aware board.
type EdgeBuildOptions struct {
	BuildOptions
	Weights edge.Weights
	Books   BookIndex
	OI      OIIndex
	// Now for feature age.
	// If PublishRequireBooks and zero books, return empty with error hint.
	PublishRequireBooks bool
}

// BuildEdgeBoard ranks Stage-1 candidates by edge_bps and cuts to BoardMaxN.
// Does not use activity score for final order (score field kept as Stage-1 diagnostic).
func BuildEdgeBoard(stage1 Stage1Result, opts EdgeBuildOptions) BoardBuildResult {
	dropped := map[string]int{}
	for k, v := range stage1.DroppedSummary {
		dropped[k] = v
	}
	dropped["missing_book"] = 0
	dropped["edge_drop"] = 0
	dropped["board_cap"] = 0

	if opts.Strategy == "" {
		opts.Strategy = DefaultStrategy
	}
	if opts.Now.IsZero() {
		opts.Now = time.Now().UTC()
	}
	if opts.BoardMaxN <= 0 {
		opts.BoardMaxN = 50
	}
	w := opts.Weights
	if w.Name == "" {
		w = edge.DefaultWeights()
	}

	// Neg-risk group mids from books
	groupMids := map[string][]edge.GroupLeg{}
	for _, c := range stage1.Candidates {
		if c.Market == nil || !c.NegRisk || c.NegRiskGroupID == "" {
			continue
		}
		toks := parseTokenIDs(c.Market.ClobTokenIds)
		mid := midFromBooks(toks, opts.Books)
		if mid > 0 {
			groupMids[c.NegRiskGroupID] = append(groupMids[c.NegRiskGroupID], edge.GroupLeg{
				ConditionID: c.Market.ConditionID,
				Mid:         mid,
			})
		}
	}
	groupResidual := map[string]float64{}
	groupIncomplete := map[string]bool{}
	for g, legs := range groupMids {
		bps, incomplete := edge.GroupResidual(legs)
		groupResidual[g] = bps
		groupIncomplete[g] = incomplete
	}

	type scoredCand struct {
		c   Candidate
		res edge.ScoreResult
		act float64 // stage-1 activity score
	}
	var scored []scoredCand
	booksHit := 0

	for _, c := range stage1.Candidates {
		if c.Market == nil {
			continue
		}
		fv := featureFromCandidate(c, opts.Books, opts.OI, groupResidual, groupIncomplete, opts.Now, w)
		if !fv.MissingBook {
			booksHit++
		}
		in := edge.ScoreInput{
			Features:      fv,
			TakerFeeBps:   takerFeeBps(c.Market),
			NegRiskFeeBps: float64(c.NegRiskFeeBips),
		}
		res := edge.Score(in, w)
		if res.Drop {
			dropped["edge_drop"]++
			continue
		}
		scored = append(scored, scoredCand{c: c, res: res, act: c.Market.ComputedScore})
	}

	if booksHit == 0 && opts.PublishRequireBooks {
		dropped["missing_book"] = len(stage1.Candidates)
		return BoardBuildResult{
			Rows:           nil,
			Stage1Count:    stage1.Stage1Count,
			PoolCount:      stage1.PoolCount,
			DroppedSummary: dropped,
		}
	}
	dropped["missing_book"] = max(0, len(stage1.Candidates)-booksHit)

	sort.SliceStable(scored, func(i, j int) bool {
		if scored[i].res.EdgeBps == scored[j].res.EdgeBps {
			return scored[i].act > scored[j].act
		}
		return scored[i].res.EdgeBps > scored[j].res.EdgeBps
	})

	// Sticky preference among edge-scored set
	stickySet := map[string]bool{}
	for _, id := range opts.StickyConditionIDs {
		if id != "" {
			stickySet[id] = true
		}
	}
	// Partition: sticky first in edge order, then rest
	var ordered []scoredCand
	seen := map[string]bool{}
	for _, s := range scored {
		id := s.c.Market.ConditionID
		if stickySet[id] {
			ordered = append(ordered, s)
			seen[id] = true
		}
	}
	for _, s := range scored {
		id := s.c.Market.ConditionID
		if !seen[id] {
			ordered = append(ordered, s)
			seen[id] = true
		}
	}
	// Re-sort by edge within full set but ensure sticky not cut first:
	// take BoardMaxN with sticky preference like selectBoard
	if len(ordered) > opts.BoardMaxN {
		var picked []scoredCand
		seen2 := map[string]bool{}
		for _, s := range ordered {
			id := s.c.Market.ConditionID
			if stickySet[id] {
				picked = append(picked, s)
				seen2[id] = true
				if len(picked) >= opts.BoardMaxN {
					break
				}
			}
		}
		if len(picked) < opts.BoardMaxN {
			for _, s := range scored { // original edge order
				id := s.c.Market.ConditionID
				if seen2[id] {
					continue
				}
				picked = append(picked, s)
				seen2[id] = true
				if len(picked) >= opts.BoardMaxN {
					break
				}
			}
		}
		dropped["board_cap"] = len(scored) - len(picked)
		// final order by edge_bps
		sort.SliceStable(picked, func(i, j int) bool {
			return picked[i].res.EdgeBps > picked[j].res.EdgeBps
		})
		ordered = picked
	}

	// Related legs
	groupMembers := map[string][]string{}
	for cond, c := range stage1.ByCond {
		if c.NegRiskGroupID == "" {
			continue
		}
		groupMembers[c.NegRiskGroupID] = append(groupMembers[c.NegRiskGroupID], cond)
	}
	for g := range groupMembers {
		sort.Strings(groupMembers[g])
	}

	rows := make([]db.EdgeBoardRow, 0, len(ordered))
	for i, s := range ordered {
		m := s.c.Market
		tokens := parseTokenIDs(m.ClobTokenIds)
		legs := relatedLegs(m.ConditionID, s.c.NegRiskGroupID, groupMembers)
		liq := marketLiquidity(m)
		vol24 := m.Volume24hr
		if m.Volume24hrClob != 0 {
			vol24 = m.Volume24hrClob
		}
		eb := s.res.EdgeBps
		cost := s.res.Cost.TotalCostBps
		cap := s.res.Cost.CapacityUSD
		urg := s.res.Urgency
		kf, _ := json.Marshal(s.res.KeyFeatures)
		flags := s.res.RiskFlags
		if flags == nil {
			flags = []string{}
		}
		tags := s.res.StrategyTags
		if tags == nil {
			tags = []string{}
		}
		var featuresAsOf *time.Time
		if s.res.KeyFeatures != nil {
			t := opts.Now
			featuresAsOf = &t
		}
		row := db.EdgeBoardRow{
			Strategy:       opts.Strategy,
			ConditionID:    m.ConditionID,
			MarketID:       m.ID,
			QuestionShort:  shortQuestion(m.Question, 120),
			Category:       s.c.Category,
			ClobTokenIDs:   tokens,
			Rank:           i + 1,
			Score:          s.act,
			EdgeBps:        &eb,
			CostBps:        &cost,
			CapacityUSD:    &cap,
			Urgency:        &urg,
			KeyFeatures:    kf,
			RiskFlags:      flags,
			StrategyTags:   tags,
			FeaturesAsOf:   featuresAsOf,
			FairValue:      s.res.FairValue,
			ModelEdgeBps:   s.res.ModelEdgeBps,
			FVSource:       s.res.FVSource,
			NegRisk:        s.c.NegRisk,
			NegRiskGroupID: s.c.NegRiskGroupID,
			RelatedLegs:    legs,
			Volume24hr:     vol24,
			Liquidity:      liq,
			Spread:         m.Spread,
			SelectedAt:     opts.Now,
			RunID:          opts.RunID,
		}
		// Prefer live book mid spread if present
		if s.res.Cost.HasBook && s.res.Cost.Mid > 0 {
			// approximate full spread from half
			row.Spread = (s.res.Cost.HalfSpreadBps * 2 / 10_000) * s.res.Cost.Mid
		}
		rows = append(rows, row)
	}

	return BoardBuildResult{
		Rows:           rows,
		Stage1Count:    stage1.Stage1Count,
		PoolCount:      stage1.PoolCount,
		DroppedSummary: dropped,
	}
}

func featureFromCandidate(
	c Candidate,
	books BookIndex,
	oi OIIndex,
	groupResidual map[string]float64,
	groupIncomplete map[string]bool,
	now time.Time,
	w edge.Weights,
) edge.FeatureVector {
	m := c.Market
	fv := edge.FeatureVector{
		ConditionID:    m.ConditionID,
		Volume24hr:     m.Volume24hr,
		OneDayAbs:      math.Abs(m.OneDayPriceChange),
		NegRisk:        c.NegRisk,
		NegRiskGroupID: c.NegRiskGroupID,
	}
	if m.Volume24hrClob != 0 {
		fv.Volume24hr = m.Volume24hrClob
	}
	if !m.EndDate.IsZero() {
		fv.TTRHours = m.EndDate.Sub(now).Hours()
		if fv.TTRHours < 0 {
			fv.TTRHours = 0
		}
	}
	if !m.StartDate.IsZero() {
		fv.AgeDays = now.Sub(m.StartDate).Hours() / 24
	}
	if oi != nil {
		if v, ok := oi[m.ConditionID]; ok {
			fv.OI = v
		}
	}
	if c.NegRisk && c.NegRiskGroupID != "" {
		fv.NegRiskResidualBps = groupResidual[c.NegRiskGroupID]
		fv.NegRiskIncomplete = groupIncomplete[c.NegRiskGroupID]
	}

	toks := parseTokenIDs(m.ClobTokenIds)
	// Prefer first token (YES) book for binary markets
	var snap *enrich.BookSnapshot
	for _, tid := range toks {
		if b, ok := books[tid]; ok {
			bb := b
			snap = &bb
			fv.TokenID = tid
			break
		}
	}
	if snap == nil {
		fv.MissingBook = true
		fv.FillBookDerived(w.MinDepthShares)
		return fv
	}
	fv.BestBid = snap.BestBid
	fv.BestAsk = snap.BestAsk
	fv.BidDepth = snap.TotalBidDepth
	fv.AskDepth = snap.TotalAskDepth
	fv.Imbalance = snap.Imbalance
	fv.BookAgeSec = now.Sub(snap.Time).Seconds()
	if fv.BookAgeSec < 0 {
		fv.BookAgeSec = 0
	}
	fv.FeatureAgeSec = fv.BookAgeSec
	fv.FeaturesAsOfUnix = snap.Time.Unix()
	fv.FillBookDerived(w.MinDepthShares)
	return fv
}

func midFromBooks(tokenIDs []string, books BookIndex) float64 {
	for _, tid := range tokenIDs {
		b, ok := books[tid]
		if !ok || b.BestBid <= 0 || b.BestAsk <= 0 {
			continue
		}
		return (b.BestBid + b.BestAsk) / 2
	}
	return 0
}

func takerFeeBps(m *services.PlyMktMarket) float64 {
	if m == nil {
		return 0
	}
	// TakerBaseFee on Gamma is often in raw fee units; treat as bps if small.
	if m.TakerBaseFee > 0 && m.TakerBaseFee < 10_000 {
		return float64(m.TakerBaseFee)
	}
	return 0
}

// CollectTokenIDs returns unique CLOB token IDs for Stage-1 candidates (YES+NO).
func CollectTokenIDs(cands []Candidate) []string {
	seen := map[string]bool{}
	var out []string
	for _, c := range cands {
		if c.Market == nil {
			continue
		}
		for _, tid := range parseTokenIDs(c.Market.ClobTokenIds) {
			if tid == "" || seen[tid] {
				continue
			}
			seen[tid] = true
			out = append(out, tid)
		}
	}
	return out
}

// CollectConditionIDs returns unique condition IDs.
func CollectConditionIDs(cands []Candidate) []string {
	seen := map[string]bool{}
	var out []string
	for _, c := range cands {
		if c.Market == nil || c.Market.ConditionID == "" || seen[c.Market.ConditionID] {
			continue
		}
		seen[c.Market.ConditionID] = true
		out = append(out, c.Market.ConditionID)
	}
	return out
}

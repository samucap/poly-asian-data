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

	stickySet := stickyMap(opts.StickyConditionIDs)
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
		ranked = selectBoard(ranked, stickySet, opts.Filter.MaxN)
		dropped["stage1_cap"] = max(0, stage1Count-len(ranked))
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
	Weights             edge.Weights
	// StrategyVersionID stamps edge_board.strategy_version_id (M5 lineage).
	StrategyVersionID *int64
	Books             BookIndex
	OI                OIIndex
	PublishRequireBooks bool
	// FV chain; zero value → DefaultFVChain from weights.
	FV edge.FVChain
	// EnrichCandidates optional expanded set for group mids (defaults to Stage-1).
	EnrichCandidates []Candidate
}

// BoardBuildResult extended with FV coverage diagnostics.
// (fields added on existing struct via this package — see BoardBuildResult in board.go)

// BuildEdgeBoard ranks Stage-1 candidates via edge.SelectBoard (same pure policy as edge-eval),
// then applies live-only sticky retention when cutting to BoardMaxN.
//
// policy_parity=scan_board_v1: scoring/rank keys come from SelectBoard; sticky is operational
// retention only and is not part of offline promote eval.
func BuildEdgeBoard(stage1 Stage1Result, opts EdgeBuildOptions) BoardBuildResult {
	dropped := copyDropped(stage1.DroppedSummary)
	dropped["missing_book"] = 0
	dropped["edge_drop"] = 0
	dropped["board_cap"] = 0
	dropped["extreme_price"] = 0

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

	// Enrich set for group mids (Stage-1 + expanded siblings).
	enrichSet := opts.EnrichCandidates
	if len(enrichSet) == 0 {
		enrichSet = stage1.Candidates
	}
	groupMidsByGroup := BuildGroupMidIndex(enrichSet, opts.Books)
	groupResidual, groupIncomplete := negRiskResiduals(enrichSet, opts.Books)

	boardCands := make([]edge.BoardCandidate, 0, len(stage1.Candidates))
	byCondFeat := make(map[string]Candidate, len(stage1.Candidates))
	booksHit := 0
	for _, c := range stage1.Candidates {
		if c.Market == nil {
			continue
		}
		feat := featureFromCandidate(c, opts.Books, opts.OI, groupResidual, groupIncomplete, opts.Now, w)
		if !feat.MissingBook {
			booksHit++
		}
		// Pre-count extremes for diagnostics (SelectBoard also drops them).
		if w.DropExtremePrice && !feat.MissingBook && edge.IsExtremeMid(feat.Mid, w.ExtremeLo, w.ExtremeHi) {
			dropped["extreme_price"]++
		}
		gMids := GroupMidsFor(c, groupMidsByGroup)
		known := GroupKnownSize(c, stage1.ByCond)
		if gMids != nil && len(gMids) > known {
			known = len(gMids)
		}
		act := 0.0
		if c.Market != nil {
			act = c.Market.ComputedScore
		}
		boardCands = append(boardCands, edge.BoardCandidate{
			Features:       feat,
			GroupMids:      gMids,
			KnownGroupSize: known,
			NegRiskFeeBips: float64(c.NegRiskFeeBips),
			TakerFeeBps:    takerFeeBps(c.Market),
			TieBreak:       act,
		})
		byCondFeat[c.Market.ConditionID] = c
	}

	if booksHit == 0 && opts.PublishRequireBooks {
		dropped["missing_book"] = len(stage1.Candidates)
		return BoardBuildResult{
			Stage1Count:    stage1.Stage1Count,
			PoolCount:      stage1.PoolCount,
			DroppedSummary: dropped,
			FVHits:         0,
			FVCoverage:     0,
		}
	}
	dropped["missing_book"] = max(0, len(stage1.Candidates)-booksHit)

	// Score/rank all survivors via shared policy (n = full set; sticky cut next).
	ranked := edge.SelectBoard(boardCands, w, len(boardCands))
	// Candidates not returned = extreme drop and/or Score.Drop.
	dropped["edge_drop"] = max(0, len(boardCands)-len(ranked))

	scored := make([]scoredCand, 0, len(ranked))
	fvHits := 0
	for _, r := range ranked {
		id := r.Candidate.Features.ConditionID
		c, ok := byCondFeat[id]
		if !ok {
			continue
		}
		if r.Result.FairValue != nil {
			fvHits++
		}
		act := r.Candidate.TieBreak
		scored = append(scored, scoredCand{c: c, act: act, res: r.Result})
	}

	before := len(scored)
	scored = selectTopNByEdge(scored, stickyMap(opts.StickyConditionIDs), opts.BoardMaxN)
	dropped["board_cap"] = max(0, before-len(scored))

	boardFV := 0
	for _, s := range scored {
		if s.res.FairValue != nil {
			boardFV++
		}
	}
	fvCov := 0.0
	if len(scored) > 0 {
		fvCov = float64(boardFV) / float64(len(scored))
	}

	groupMembers := groupMemberIndex(stage1.ByCond)
	rows := materializeRows(scored, groupMembers, opts)
	return BoardBuildResult{
		Rows:           rows,
		Stage1Count:    stage1.Stage1Count,
		PoolCount:      stage1.PoolCount,
		DroppedSummary: dropped,
		FVHits:         fvHits,
		FVCoverage:     fvCov,
	}
}

func materializeRows(ordered []scoredCand, groupMembers map[string][]string, opts EdgeBuildOptions) []db.EdgeBoardRow {
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
		// Annotate residual-homogeneous complement on board for agents.
		if s.res.Path == "fair_value" && s.res.FVSource == "neg_risk_complement" {
			flags = append(flags, "fv_group_residual")
		}
		tags := s.res.StrategyTags
		if tags == nil {
			tags = []string{}
		}
		t := opts.Now
		row := db.EdgeBoardRow{
			Strategy:          opts.Strategy,
			ConditionID:       m.ConditionID,
			MarketID:          m.ID,
			QuestionShort:     shortQuestion(m.Question, 120),
			Category:          s.c.Category,
			ClobTokenIDs:      tokens,
			Rank:              i + 1,
			Score:             s.act,
			EdgeBps:           &eb,
			StrategyVersionID: opts.StrategyVersionID,
			CostBps:           &cost,
			CapacityUSD:       &cap,
			Urgency:           &urg,
			KeyFeatures:       kf,
			RiskFlags:         flags,
			StrategyTags:      tags,
			FeaturesAsOf:      &t,
			FairValue:         s.res.FairValue,
			ModelEdgeBps:      s.res.ModelEdgeBps,
			FVSource:          s.res.FVSource,
			NegRisk:           s.c.NegRisk,
			NegRiskGroupID:    s.c.NegRiskGroupID,
			RelatedLegs:       legs,
			Volume24hr:        vol24,
			Liquidity:         liq,
			Spread:            m.Spread,
			SelectedAt:        opts.Now,
			RunID:             opts.RunID,
		}
		if s.res.Cost.HasBook && s.res.Cost.Mid > 0 {
			row.Spread = (s.res.Cost.HalfSpreadBps * 2 / 10_000) * s.res.Cost.Mid
		}
		rows = append(rows, row)
	}
	return rows
}

func negRiskResiduals(cands []Candidate, books BookIndex) (residual map[string]float64, incomplete map[string]bool) {
	groupMids := map[string][]edge.GroupLeg{}
	for _, c := range cands {
		if c.Market == nil || !c.NegRisk || c.NegRiskGroupID == "" {
			continue
		}
		toks := parseTokenIDs(c.Market.ClobTokenIds)
		mid := midFromBooks(toks, books)
		if mid > 0 {
			groupMids[c.NegRiskGroupID] = append(groupMids[c.NegRiskGroupID], edge.GroupLeg{
				ConditionID: c.Market.ConditionID,
				Mid:         mid,
			})
		}
	}
	residual = map[string]float64{}
	incomplete = map[string]bool{}
	for g, legs := range groupMids {
		bps, inc := edge.GroupResidual(legs)
		residual[g] = bps
		incomplete[g] = inc
	}
	return residual, incomplete
}

func groupMemberIndex(byCond map[string]Candidate) map[string][]string {
	groupMembers := map[string][]string{}
	for cond, c := range byCond {
		if c.NegRiskGroupID == "" {
			continue
		}
		groupMembers[c.NegRiskGroupID] = append(groupMembers[c.NegRiskGroupID], cond)
	}
	for g := range groupMembers {
		sort.Strings(groupMembers[g])
	}
	return groupMembers
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

	// Primary token only (first CLOB id = YES on binary markets).
	toks := parseTokenIDs(m.ClobTokenIds)
	var snap *enrich.BookSnapshot
	if len(toks) > 0 {
		if b, ok := books[toks[0]]; ok {
			bb := b
			snap = &bb
			fv.TokenID = toks[0]
		}
	}
	if snap == nil {
		// Fallback: any token in index (legacy enrich of both legs).
		for _, tid := range toks {
			if b, ok := books[tid]; ok {
				bb := b
				snap = &bb
				fv.TokenID = tid
				break
			}
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
	// Prefer strategy default (0 here); only use market meta when clearly bps-scale.
	if m.TakerBaseFee > 0 && m.TakerBaseFee <= 500 {
		return float64(m.TakerBaseFee)
	}
	return 0
}

// CollectPrimaryTokenIDs returns the first CLOB token per market (YES leg for binaries).
// This halves /books fan-out vs collecting both outcomes.
func CollectPrimaryTokenIDs(cands []Candidate) []string {
	seen := map[string]bool{}
	var out []string
	for _, c := range cands {
		if c.Market == nil {
			continue
		}
		toks := parseTokenIDs(c.Market.ClobTokenIds)
		if len(toks) == 0 {
			continue
		}
		tid := toks[0]
		if tid == "" || seen[tid] {
			continue
		}
		seen[tid] = true
		out = append(out, tid)
	}
	return out
}

// CollectTokenIDs is an alias for CollectPrimaryTokenIDs (prefer primary-token enrich).
func CollectTokenIDs(cands []Candidate) []string {
	return CollectPrimaryTokenIDs(cands)
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

func stickyMap(ids []string) map[string]bool {
	m := map[string]bool{}
	for _, id := range ids {
		if id != "" {
			m[id] = true
		}
	}
	return m
}

func copyDropped(in map[string]int) map[string]int {
	out := map[string]int{}
	for k, v := range in {
		out[k] = v
	}
	return out
}

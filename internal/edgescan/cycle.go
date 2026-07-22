package edgescan

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/samucap/poly-asian-data/internal/artifacts"
	"github.com/samucap/poly-asian-data/internal/catalog"
	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/edge"
	"github.com/samucap/poly-asian-data/internal/enrich"
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/marketranking"
)

// CycleDeps is everything RunCycle needs (no global env reads inside the cycle).
type CycleDeps struct {
	DB            db.DBInterface
	HTTP          *http.Client
	Cfg           *config.Config
	Logger        *slog.Logger
	Strategy      string
	Weights       edge.Weights
	ArtifactsRoot string
	EdgeScan      config.EdgeScanConfig

	EnrichBooks         bool
	EnrichOI            bool
	PublishOnEnrichFail bool
	// OIMaxConditions caps concurrent OI fan-out (default 80).
	OIMaxConditions int
	// OIConcurrency bounds parallel OI GETs (default 16).
	OIConcurrency int
	// BookBatchSize for CLOB /books (default 50).
	BookBatchSize int

	// M4 warm stage: batch prices_history for board ∪ top-volume tokens.
	// Default true when unset via cmd; set false to disable.
	EnrichPrices bool
	// PriceTokenCap hard cap on tokens for EnsurePrices (default 300).
	PriceTokenCap int
	// PriceFidelityMin CLOB fidelity minutes (default 60).
	PriceFidelityMin int
	// PriceLookback cold lookback (default 30d).
	PriceLookback time.Duration
	// PriceWarmMaxAge skip incremental fetch if last point fresher (default 30m).
	PriceWarmMaxAge time.Duration
	// ForceColdPrices force full lookback even when series exist.
	ForceColdPrices bool
}

// CycleResult is the outcome of one edge-scan cycle.
type CycleResult struct {
	OK             bool
	Status         string
	Rows           []db.EdgeBoardRow
	PoolCount      int
	Stage1Count    int
	BoardCount     int
	EnrichCoverage float64
	EnrichMS       int64
	CycleMS        int64
	RunID          string
	Errors         []artifacts.ErrorItem
	// M4 warm prices stage (best-effort; does not fail board).
	PricesMS     int64
	PricesPoints int
	PricesTokens int
}

// RunCycle executes: keyset → Stage-1 budget → parallel enrich → edge rank → persist board.
// Does not publish activity-only boards. On enrich/empty failure, leaves prior DB board intact.
func RunCycle(ctx context.Context, deps CycleDeps) CycleResult {
	log := deps.Logger
	if log == nil {
		log = slog.Default()
	}
	start := time.Now()
	res := CycleResult{Status: artifacts.StatusSuccess}

	strategy := deps.Strategy
	if strategy == "" {
		strategy = DefaultStrategy
	}
	esc := deps.EdgeScan
	oiMax := deps.OIMaxConditions
	if oiMax <= 0 {
		oiMax = 80
	}
	oiConc := deps.OIConcurrency
	if oiConc <= 0 {
		oiConc = 16
	}
	bookBatch := deps.BookBatchSize
	if bookBatch <= 0 {
		bookBatch = 50
	}

	// Sticky from prior published board
	var sticky []string
	if esc.Sticky && deps.DB != nil {
		ids, err := db.LoadEdgeBoardConditionIDs(ctx, deps.DB, strategy)
		if err != nil {
			log.Warn("load sticky board failed (continuing)", "error", err)
		} else {
			sticky = ids
		}
	}

	// 1. Keyset
	stage := time.Now()
	now := time.Now().UTC()
	kf := catalog.KeysetFilters{
		VolumeMin:      esc.KeysetVolumeMin,
		LiquidityMin:   esc.KeysetLiquidityMin,
		LimitThreshold: esc.KeysetEventCap,
	}
	if esc.EndDateMinOffset > 0 {
		kf.EndDateMin = now.Add(-esc.EndDateMinOffset)
	}
	if esc.EndDateMaxOffset > 0 {
		kf.EndDateMax = now.Add(esc.EndDateMaxOffset)
	}
	events, err := catalog.FetchEventsKeyset(ctx, deps.HTTP, deps.Cfg, kf)
	if err != nil {
		log.Error("keyset fetch failed", "error", err)
		res.OK = false
		res.Status = artifacts.StatusFailed
		res.CycleMS = time.Since(start).Milliseconds()
		return res
	}
	log.Info("stage complete",
		"stage", "events_keyset",
		"duration", logging.FormatDuration(time.Since(stage)),
		"events", logging.FormatCount(int64(len(events))),
	)

	// 2. Stage-1 budget (not published)
	stage = time.Now()
	pool := FlattenCandidates(events)
	filter := marketranking.MarketFilter{
		MinVolume24hr: esc.MinVolume24hr,
		MinLiquidity:  esc.MinLiquidity,
		MaxSpread:     esc.MaxSpread,
		MinVolatility: esc.MinVolatility,
		MaxN:          esc.Stage1MaxN,
	}
	stage1 := SelectStage1(pool, BuildOptions{
		Filter:             filter,
		StickyConditionIDs: sticky,
		Strategy:           strategy,
		Now:                now,
	})
	res.PoolCount = stage1.PoolCount
	res.Stage1Count = stage1.Stage1Count
	log.Info("stage complete",
		"stage", "stage1_budget",
		"duration", logging.FormatDuration(time.Since(stage)),
		"pool", logging.FormatCount(int64(stage1.PoolCount)),
		"stage1", logging.FormatCount(int64(stage1.Stage1Count)),
	)

	if stage1.Stage1Count == 0 {
		res.Errors = append(res.Errors, artifacts.ErrorItem{
			Code: "empty_stage1", Message: "no markets passed Stage-1 filters", Component: "rank",
		})
		writeFailArtifact(deps, stage1, res.Status, res.Errors, start, 0, 0)
		res.OK = false
		res.CycleMS = time.Since(start).Milliseconds()
		return res
	}

	// 3. Expand in-pool neg-risk siblings (bounded), then parallel enrich
	stage = time.Now()
	expandCap := deps.Weights.GroupExpandCap
	if expandCap <= 0 {
		expandCap = 100
	}
	// Books: Stage-1 + expanded siblings (needed for group FV mids).
	// OI: Stage-1 only — expanded legs are not ranked; skip wasted GETs.
	enrichCands, tokensExpanded := ExpandGroupCandidates(stage1, expandCap)
	tokenIDs := CollectPrimaryTokenIDs(enrichCands)
	condIDs := CollectConditionIDs(stage1.Candidates)
	if len(condIDs) > oiMax {
		condIDs = condIDs[:oiMax]
	}
	if tokensExpanded > 0 {
		log.Info("group expand", "added_candidates", tokensExpanded, "enrich_cands", len(enrichCands), "primary_tokens", len(tokenIDs))
	}

	bookIdx := BookIndex{}
	oiIdx := OIIndex{}
	var booksErr, oiErr error
	var snaps []enrich.BookSnapshot
	var oiPts []enrich.OIPoint

	// Parallel books ∥ OI (independent I/O).
	var wg sync.WaitGroup
	if deps.EnrichBooks && len(tokenIDs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			snaps, booksErr = enrich.FetchBooks(ctx, deps.HTTP, tokenIDs, bookBatch)
		}()
	}
	if deps.EnrichOI && len(condIDs) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			oiPts, oiErr = enrich.FetchOI(ctx, deps.HTTP, condIDs, oiConc)
		}()
	}
	wg.Wait()

	if booksErr != nil {
		log.Warn("enrich books failed", "error", booksErr)
		res.Errors = append(res.Errors, artifacts.ErrorItem{
			Code: "enrich_books_failed", Message: booksErr.Error(), Component: "enrich",
		})
		res.Status = artifacts.StatusPartial
	}
	if oiErr != nil {
		log.Warn("enrich oi failed", "error", oiErr)
		res.Errors = append(res.Errors, artifacts.ErrorItem{
			Code: "enrich_oi_failed", Message: oiErr.Error(), Component: "enrich",
		})
		res.Status = artifacts.StatusPartial
	}

	if len(snaps) > 0 {
		dbRows := make([]db.OrderbookSnapshotRow, 0, len(snaps))
		for _, s := range snaps {
			bookIdx[s.TokenID] = s
			dbRows = append(dbRows, db.OrderbookSnapshotRow{
				Time: s.Time, MarketID: s.MarketID, TokenID: s.TokenID,
				BestBid: s.BestBid, BestAsk: s.BestAsk, Imbalance: s.Imbalance,
				TotalBidDepth: s.TotalBidDepth, TotalAskDepth: s.TotalAskDepth,
				DepthJSON: s.DepthJSON, RawJSON: s.RawJSON,
			})
		}
		if deps.DB != nil {
			if err := db.InsertOrderbookSnapshots(ctx, deps.DB, dbRows); err != nil {
				log.Warn("persist orderbook_snapshots failed", "error", err)
			}
		}
		log.Info("enriched books", "tokens_req", len(tokenIDs), "snaps", len(snaps))
	}
	if len(oiPts) > 0 {
		oiRows := make([]db.OIHistoryRow, 0, len(oiPts))
		for _, p := range oiPts {
			if p.ConditionID == "" {
				continue
			}
			oiIdx[p.ConditionID] = p.Value
			oiRows = append(oiRows, db.OIHistoryRow{
				Time: p.Time, ConditionID: p.ConditionID, OIValue: p.Value, Source: "data-api",
			})
		}
		if deps.DB != nil {
			if err := db.InsertOIHistory(ctx, deps.DB, oiRows); err != nil {
				log.Warn("persist oi_history failed", "error", err)
			}
		}
		log.Info("enriched oi", "conditions", len(condIDs), "points", len(oiPts))
	}

	res.EnrichMS = time.Since(stage).Milliseconds()
	if len(tokenIDs) > 0 {
		res.EnrichCoverage = float64(len(bookIdx)) / float64(len(tokenIDs))
	}
	log.Info("stage complete",
		"stage", "enrich",
		"duration", logging.FormatDuration(time.Since(stage)),
		"enrich_coverage", res.EnrichCoverage,
		"books", len(bookIdx),
		"oi", len(oiIdx),
	)

	if len(bookIdx) == 0 && !deps.PublishOnEnrichFail && deps.EnrichBooks {
		res.Errors = append(res.Errors, artifacts.ErrorItem{
			Code: "enrich_failed", Message: "no books fetched; keeping prior board", Component: "enrich",
		})
		log.Error("enrich produced zero books; not overwriting edge_board")
		writeFailArtifact(deps, stage1, artifacts.StatusFailed, res.Errors, start, res.EnrichMS, res.EnrichCoverage)
		res.OK = false
		res.Status = artifacts.StatusFailed
		res.CycleMS = time.Since(start).Milliseconds()
		return res
	}

	// 4. Edge rank (single published board)
	stage = time.Now()
	build := BuildEdgeBoard(stage1, EdgeBuildOptions{
		BuildOptions: BuildOptions{
			Filter:             filter,
			BoardMaxN:          esc.BoardMaxN,
			StickyConditionIDs: sticky,
			Strategy:           strategy,
			Now:                now,
		},
		Weights:             deps.Weights,
		Books:               bookIdx,
		OI:                  oiIdx,
		PublishRequireBooks: !deps.PublishOnEnrichFail,
		EnrichCandidates:    enrichCands,
	})
	log.Info("stage complete",
		"stage", "edge_rank",
		"duration", logging.FormatDuration(time.Since(stage)),
		"board", logging.FormatCount(int64(len(build.Rows))),
		"fv_coverage", build.FVCoverage,
		"fv_hits", build.FVHits,
	)

	if len(build.Rows) == 0 {
		res.Errors = append(res.Errors, artifacts.ErrorItem{
			Code: "empty_board", Message: "no markets after edge ranking", Component: "edge",
		})
		log.Error("empty edge board; not overwriting prior board")
		writeFailArtifact(deps, stage1, artifacts.StatusFailed, res.Errors, start, res.EnrichMS, res.EnrichCoverage)
		res.OK = false
		res.Status = artifacts.StatusFailed
		res.CycleMS = time.Since(start).Milliseconds()
		return res
	}

	// 5. Artifact + persist
	extra := BoardStats{
		CycleMS:        time.Since(start).Milliseconds(),
		EnrichMS:       res.EnrichMS,
		EnrichCoverage: res.EnrichCoverage,
		FVCoverage:     build.FVCoverage,
		FVHits:         build.FVHits,
		TokensExpanded: tokensExpanded,
	}
	doc, err := BuildArtifactWithStats(build.Rows, build.PoolCount, build.Stage1Count, build.DroppedSummary, res.Status, res.Errors, extra)
	if err != nil {
		log.Error("build edge_board artifact failed", "error", err)
		res.OK = false
		res.CycleMS = time.Since(start).Milliseconds()
		return res
	}
	for i := range build.Rows {
		build.Rows[i].RunID = doc.RunID
	}
	doc.BoardStats.NBoard = len(build.Rows)
	doc.BoardStats.CycleMS = time.Since(start).Milliseconds()
	res.RunID = doc.RunID
	res.Rows = build.Rows
	res.BoardCount = len(build.Rows)

	stage = time.Now()
	if deps.DB != nil {
		if err := db.ReplaceEdgeBoard(ctx, deps.DB, strategy, build.Rows); err != nil {
			log.Error("replace edge_board failed", "error", err)
			res.Errors = append(res.Errors, artifacts.ErrorItem{
				Code: "edge_board_write_failed", Message: err.Error(), Component: "db",
			})
			res.Status = artifacts.StatusFailed
			doc.Status = res.Status
			doc.Errors = res.Errors
		}
	}
	log.Info("stage complete", "stage", "save_board", "duration", logging.FormatDuration(time.Since(stage)))

	stage = time.Now()
	if deps.ArtifactsRoot != "" {
		wr, err := WriteArtifact(doc, deps.ArtifactsRoot)
		if err != nil {
			log.Error("write edge_board artifact failed", "error", err)
			if res.Status != artifacts.StatusFailed {
				res.Status = artifacts.StatusPartial
			}
		} else {
			log.Info("edge_board artifact written",
				"path", wr.RunPath,
				"latest", wr.LatestPath,
				"run_id", doc.RunID,
				"board_n", len(build.Rows),
				"schema", doc.SchemaVersion,
			)
		}
	}
	log.Info("stage complete", "stage", "artifact", "duration", logging.FormatDuration(time.Since(stage)))

	for i, r := range build.Rows {
		if i >= 5 {
			break
		}
		edgeVal := 0.0
		if r.EdgeBps != nil {
			edgeVal = *r.EdgeBps
		}
		log.Info("board row",
			"rank", r.Rank,
			"condition_id", r.ConditionID,
			"edge_bps", edgeVal,
			"score_stage1", r.Score,
			"category", r.Category,
			"neg_risk", r.NegRisk,
			"vol24", r.Volume24hr,
		)
	}

	// 6. WARM: prices_history for board ∪ top-volume (M4 labels path).
	// Best-effort: never flips board status to failed.
	if deps.EnrichPrices && deps.DB != nil {
		stage = time.Now()
		priceRes := ensureBoardPrices(ctx, deps, build.Rows)
		res.PricesMS = time.Since(stage).Milliseconds()
		res.PricesPoints = priceRes.PointsWritten
		res.PricesTokens = priceRes.TokensWritten
		if len(priceRes.Errors) > 0 {
			for _, e := range priceRes.Errors {
				res.Errors = append(res.Errors, artifacts.ErrorItem{
					Code: "ensure_prices_partial", Message: e, Component: "enrich",
				})
			}
			if res.Status == artifacts.StatusSuccess {
				res.Status = artifacts.StatusPartial
			}
		}
		log.Info("stage complete",
			"stage", "ensure_prices",
			"duration", logging.FormatDuration(time.Since(stage)),
			"points", priceRes.PointsWritten,
			"tokens_written", priceRes.TokensWritten,
			"cold", priceRes.ColdTokens,
			"warm", priceRes.WarmTokens,
			"skipped_fresh", priceRes.SkippedFresh,
		)
	}

	res.CycleMS = time.Since(start).Milliseconds()
	res.OK = res.Status != artifacts.StatusFailed
	log.Info("cycle complete",
		"status", res.Status,
		"duration", logging.FormatDuration(time.Since(start)),
		"events", logging.FormatCount(int64(len(events))),
		"pool", logging.FormatCount(int64(build.PoolCount)),
		"stage1", logging.FormatCount(int64(build.Stage1Count)),
		"board", logging.FormatCount(int64(len(build.Rows))),
		"enrich_coverage", res.EnrichCoverage,
		"prices_points", res.PricesPoints,
		"strategy", strategy,
	)
	return res
}

// ensureBoardPrices builds the token set (board ∪ top volume) and calls enrich.EnsurePrices.
func ensureBoardPrices(ctx context.Context, deps CycleDeps, board []db.EdgeBoardRow) enrich.EnsurePricesResult {
	log := deps.Logger
	if log == nil {
		log = slog.Default()
	}
	capN := deps.PriceTokenCap
	if capN <= 0 {
		capN = enrich.DefaultPriceTokenCap
	}
	// Board tokens from published rows
	boardMeta := make([]db.EvalMarketMeta, 0, len(board))
	for _, r := range board {
		tok := ""
		if len(r.ClobTokenIDs) > 0 {
			tok = r.ClobTokenIDs[0]
		}
		if tok == "" {
			continue
		}
		boardMeta = append(boardMeta, db.EvalMarketMeta{
			ConditionID: r.ConditionID,
			MarketID:    r.MarketID,
			TokenID:     tok,
			Category:    r.Category,
			NegRisk:     r.NegRisk,
			Volume24hr:  r.Volume24hr,
			Spread:      r.Spread,
		})
	}
	// Top volume fill-up
	var top []db.EvalMarketMeta
	if deps.DB != nil {
		var err error
		top, err = db.LoadTopVolumeMarkets(ctx, deps.DB, capN)
		if err != nil {
			log.Warn("load top volume for prices failed", "error", err)
		}
	}
	tokens := enrich.SelectPriceTokens(boardMeta, top, capN)
	tm := map[string]string{}
	for _, t := range tokens {
		if t.MarketID != "" {
			tm[t.TokenID] = t.MarketID
		}
	}
	lookback := deps.PriceLookback
	if lookback <= 0 && deps.Cfg != nil && deps.Cfg.TopMarkets.PriceLookback > 0 {
		lookback = deps.Cfg.TopMarkets.PriceLookback
	}
	if lookback <= 0 {
		lookback = enrich.DefaultPriceLookback
	}
	return enrich.EnsurePrices(ctx, deps.DB, deps.HTTP, tokens, enrich.EnsurePricesConfig{
		Lookback:    lookback,
		FidelityMin: deps.PriceFidelityMin,
		WarmMaxAge:  deps.PriceWarmMaxAge,
		ForceCold:   deps.ForceColdPrices,
		Logger:      log,
		TokenMarket: tm,
	})
}

func writeFailArtifact(
	deps CycleDeps,
	stage1 Stage1Result,
	status string,
	errs []artifacts.ErrorItem,
	start time.Time,
	enrichMS int64,
	coverage float64,
) {
	if deps.ArtifactsRoot == "" {
		return
	}
	doc, err := BuildArtifactWithStats(nil, stage1.PoolCount, stage1.Stage1Count, stage1.DroppedSummary, status, errs, BoardStats{
		CycleMS:        time.Since(start).Milliseconds(),
		EnrichMS:       enrichMS,
		EnrichCoverage: coverage,
	})
	if err != nil {
		return
	}
	if deps.Strategy != "" {
		doc.Strategy = deps.Strategy
	}
	if _, err := WriteArtifact(doc, deps.ArtifactsRoot); err != nil && deps.Logger != nil {
		deps.Logger.Warn("failed artifact write", "error", err)
	}
}

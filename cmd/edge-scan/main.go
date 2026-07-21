// Command edge-scan builds a bounded edge board from filtered Gamma events.
//
// M3: Stage-1 activity budget gate → REST enrich (books/OI) → cost-aware edge_bps board.
// Stage-1 activity is never published alone; only the edge-ranked board is written.
//
// Flags:
//
//	--once       one cycle then exit
//	--interval   override EDGE_REFRESH_INTERVAL
//	--artifacts  artifact root (default artifacts)
//	--strategy   edge_board strategy partition (default from env EDGE_STRATEGY)
//	--weights    path to strategy YAML (default configs/strategies/default.yaml)
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/samucap/poly-asian-data/internal/artifacts"
	"github.com/samucap/poly-asian-data/internal/catalog"
	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/edge"
	"github.com/samucap/poly-asian-data/internal/edgescan"
	"github.com/samucap/poly-asian-data/internal/enrich"
	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/marketranking"
	"github.com/samucap/poly-asian-data/internal/pipeline"
)

func main() {
	once := flag.Bool("once", false, "run one edge-scan cycle and exit")
	intervalFlag := flag.String("interval", "", "override EDGE_REFRESH_INTERVAL (e.g. 2m)")
	artifactsRoot := flag.String("artifacts", artifacts.DefaultRoot, "artifact output root")
	strategyFlag := flag.String("strategy", "", "edge_board strategy name (default EDGE_STRATEGY or default)")
	weightsFlag := flag.String("weights", "", "path to strategy weights YAML")
	flag.Parse()

	logging.Init(os.Getenv("ENV"))
	cfg, err := config.Load()
	if err != nil {
		logging.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	logging.Init(cfg.ENV)
	logger := logging.Logger

	esc := cfg.EdgeScan
	fetchInterval := esc.RefreshInterval
	if fetchInterval <= 0 {
		fetchInterval = 2 * time.Minute
	}
	if *intervalFlag != "" {
		d, err := time.ParseDuration(*intervalFlag)
		if err != nil {
			logger.Error("invalid --interval", "error", err)
			os.Exit(1)
		}
		fetchInterval = d
	}
	strategy := esc.Strategy
	if *strategyFlag != "" {
		strategy = *strategyFlag
	}
	if strategy == "" {
		strategy = edgescan.DefaultStrategy
	}
	fetcher.PaginateDelay = esc.PaginateDelay

	weightsPath := *weightsFlag
	if weightsPath == "" {
		weightsPath = os.Getenv("EDGE_WEIGHTS_PATH")
	}
	if weightsPath == "" {
		weightsPath = filepath.Join("configs", "strategies", "default.yaml")
	}
	weights, err := edge.LoadWeightsFile(weightsPath)
	if err != nil {
		logger.Warn("weights file not loaded; using defaults", "path", weightsPath, "error", err)
		weights = edge.DefaultWeights()
	} else {
		logger.Info("loaded strategy weights", "path", weightsPath, "name", weights.Name)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	factory, err := pipeline.NewFactory(ctx, cfg, logger)
	if err != nil {
		logger.Error("failed to create pipeline factory", slog.Any("error", err))
		os.Exit(1)
	}
	defer factory.Close()

	pipe, err := factory.Create(ctx, pipeline.Options{Name: "edge-scan"})
	if err != nil {
		logger.Error("failed to create pipeline", slog.Any("error", err))
		os.Exit(1)
	}
	defer pipe.Stop()

	dbPool := factory.DB()
	if err := db.EnsureEdgeBoardTable(ctx, dbPool); err != nil {
		logger.Error("ensure edge_board table failed", "error", err)
		os.Exit(1)
	}

	httpClient := fetcher.NewSecureHTTPClient()
	enrichBooks := envBool("EDGE_ENRICH_BOOKS", true)
	enrichOI := envBool("EDGE_ENRICH_OI", true)
	publishOnFail := envBool("EDGE_PUBLISH_ON_ENRICH_FAIL", false)

	runCycle := func() (ok bool) {
		logger.Info("edge-scan cycle starting", "strategy", strategy, "schema", edgescan.SchemaVersion)
		start := time.Now()
		status := artifacts.StatusSuccess
		var cycleErrs []artifacts.ErrorItem
		var enrichMS int64
		var enrichCoverage float64

		var sticky []string
		if esc.Sticky {
			ids, err := db.LoadEdgeBoardConditionIDs(ctx, dbPool, strategy)
			if err != nil {
				logger.Warn("load sticky board failed (continuing)", "error", err)
			} else {
				sticky = ids
			}
		}

		// 1. Filtered keyset
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
		events, err := catalog.FetchEventsKeyset(ctx, httpClient, cfg, kf)
		if err != nil {
			logger.Error("keyset fetch failed", "error", err)
			return false
		}
		logger.Info("stage complete",
			"stage", "events_keyset",
			"duration", logging.FormatDuration(time.Since(stage)),
			"events", logging.FormatCount(int64(len(events))),
		)

		// 2. Stage-1 activity budget (NOT published)
		stage = time.Now()
		pool := edgescan.FlattenCandidates(events)
		filter := marketranking.MarketFilter{
			MinVolume24hr: esc.MinVolume24hr,
			MinLiquidity:  esc.MinLiquidity,
			MaxSpread:     esc.MaxSpread,
			MinVolatility: esc.MinVolatility,
			MaxN:          esc.Stage1MaxN,
		}
		stage1 := edgescan.SelectStage1(pool, edgescan.BuildOptions{
			Filter:             filter,
			StickyConditionIDs: sticky,
			Strategy:           strategy,
			Now:                now,
		})
		logger.Info("stage complete",
			"stage", "stage1_budget",
			"duration", logging.FormatDuration(time.Since(stage)),
			"pool", logging.FormatCount(int64(stage1.PoolCount)),
			"stage1", logging.FormatCount(int64(stage1.Stage1Count)),
		)

		if stage1.Stage1Count == 0 {
			cycleErrs = append(cycleErrs, artifacts.ErrorItem{
				Code: "empty_stage1", Message: "no markets passed Stage-1 filters", Component: "rank",
			})
			// Do not overwrite board with activity-only empty rank.
			writeFailedArtifact(logger, *artifactsRoot, strategy, stage1, status, cycleErrs, start, 0, 0)
			return false
		}

		// 3. Enrich Stage-1 only (books + OI)
		stage = time.Now()
		bookIdx := edgescan.BookIndex{}
		oiIdx := edgescan.OIIndex{}
		tokenIDs := edgescan.CollectTokenIDs(stage1.Candidates)
		condIDs := edgescan.CollectConditionIDs(stage1.Candidates)

		if enrichBooks && len(tokenIDs) > 0 {
			snaps, err := enrich.FetchBooks(ctx, httpClient, tokenIDs, 50)
			if err != nil {
				logger.Warn("enrich books failed", "error", err)
				cycleErrs = append(cycleErrs, artifacts.ErrorItem{
					Code: "enrich_books_failed", Message: err.Error(), Component: "enrich",
				})
				status = artifacts.StatusPartial
			} else {
				var dbRows []db.OrderbookSnapshotRow
				for _, s := range snaps {
					bookIdx[s.TokenID] = s
					dbRows = append(dbRows, db.OrderbookSnapshotRow{
						Time: s.Time, MarketID: s.MarketID, TokenID: s.TokenID,
						BestBid: s.BestBid, BestAsk: s.BestAsk, Imbalance: s.Imbalance,
						TotalBidDepth: s.TotalBidDepth, TotalAskDepth: s.TotalAskDepth,
						DepthJSON: s.DepthJSON, RawJSON: s.RawJSON,
					})
				}
				if err := db.InsertOrderbookSnapshots(ctx, dbPool, dbRows); err != nil {
					logger.Warn("persist orderbook_snapshots failed", "error", err)
				}
				logger.Info("enriched books", "tokens_req", len(tokenIDs), "snaps", len(snaps))
			}
		}

		if enrichOI && len(condIDs) > 0 {
			// Cap OI fan-out: only first 80 conditions to protect cycle time
			oiIDs := condIDs
			if len(oiIDs) > 80 {
				oiIDs = oiIDs[:80]
			}
			pts, err := enrich.FetchOI(ctx, httpClient, oiIDs)
			if err != nil {
				logger.Warn("enrich oi failed", "error", err)
				cycleErrs = append(cycleErrs, artifacts.ErrorItem{
					Code: "enrich_oi_failed", Message: err.Error(), Component: "enrich",
				})
				status = artifacts.StatusPartial
			} else {
				var oiRows []db.OIHistoryRow
				for _, p := range pts {
					if p.ConditionID == "" {
						continue
					}
					oiIdx[p.ConditionID] = p.Value
					oiRows = append(oiRows, db.OIHistoryRow{
						Time: p.Time, ConditionID: p.ConditionID, OIValue: p.Value, Source: "data-api",
					})
				}
				if err := db.InsertOIHistory(ctx, dbPool, oiRows); err != nil {
					logger.Warn("persist oi_history failed", "error", err)
				}
				logger.Info("enriched oi", "conditions", len(oiIDs), "points", len(pts))
			}
		}

		enrichMS = time.Since(stage).Milliseconds()
		if len(tokenIDs) > 0 {
			enrichCoverage = float64(len(bookIdx)) / float64(len(tokenIDs))
		}
		logger.Info("stage complete",
			"stage", "enrich",
			"duration", logging.FormatDuration(time.Since(stage)),
			"enrich_coverage", enrichCoverage,
			"books", len(bookIdx),
			"oi", len(oiIdx),
		)

		// Prefer stale good board over activity-only if zero books and policy forbids publish
		if len(bookIdx) == 0 && !publishOnFail && enrichBooks {
			cycleErrs = append(cycleErrs, artifacts.ErrorItem{
				Code: "enrich_failed", Message: "no books fetched; keeping prior board", Component: "enrich",
			})
			logger.Error("enrich produced zero books; not overwriting edge_board")
			writeFailedArtifact(logger, *artifactsRoot, strategy, stage1, artifacts.StatusFailed, cycleErrs, start, enrichMS, enrichCoverage)
			return false
		}

		// 4. Stage-2: rank by edge_bps (single published board)
		stage = time.Now()
		build := edgescan.BuildEdgeBoard(stage1, edgescan.EdgeBuildOptions{
			BuildOptions: edgescan.BuildOptions{
				Filter:             filter,
				BoardMaxN:          esc.BoardMaxN,
				StickyConditionIDs: sticky,
				Strategy:           strategy,
				Now:                now,
			},
			Weights:             weights,
			Books:               bookIdx,
			OI:                  oiIdx,
			PublishRequireBooks: !publishOnFail,
		})
		logger.Info("stage complete",
			"stage", "edge_rank",
			"duration", logging.FormatDuration(time.Since(stage)),
			"board", logging.FormatCount(int64(len(build.Rows))),
		)

		if len(build.Rows) == 0 {
			cycleErrs = append(cycleErrs, artifacts.ErrorItem{
				Code: "empty_board", Message: "no markets after edge ranking", Component: "edge",
			})
			logger.Error("empty edge board; not overwriting prior board")
			writeFailedArtifact(logger, *artifactsRoot, strategy, stage1, artifacts.StatusFailed, cycleErrs, start, enrichMS, enrichCoverage)
			return false
		}

		// 5. Artifact + persist
		extra := edgescan.BoardStats{
			CycleMS:        time.Since(start).Milliseconds(),
			EnrichMS:       enrichMS,
			EnrichCoverage: enrichCoverage,
		}
		doc, err := edgescan.BuildArtifactWithStats(build.Rows, build.PoolCount, build.Stage1Count, build.DroppedSummary, status, cycleErrs, extra)
		if err != nil {
			logger.Error("build edge_board artifact failed", "error", err)
			return false
		}
		for i := range build.Rows {
			build.Rows[i].RunID = doc.RunID
		}
		doc.BoardStats.NBoard = len(build.Rows)
		doc.BoardStats.CycleMS = time.Since(start).Milliseconds()

		stage = time.Now()
		if err := db.ReplaceEdgeBoard(ctx, dbPool, strategy, build.Rows); err != nil {
			logger.Error("replace edge_board failed", "error", err)
			cycleErrs = append(cycleErrs, artifacts.ErrorItem{
				Code: "edge_board_write_failed", Message: err.Error(), Component: "db",
			})
			status = artifacts.StatusFailed
			doc.Status = status
			doc.Errors = cycleErrs
		}
		logger.Info("stage complete", "stage", "save_board", "duration", logging.FormatDuration(time.Since(stage)))

		stage = time.Now()
		wr, err := edgescan.WriteArtifact(doc, *artifactsRoot)
		if err != nil {
			logger.Error("write edge_board artifact failed", "error", err)
			if status != artifacts.StatusFailed {
				status = artifacts.StatusPartial
			}
		} else {
			logger.Info("edge_board artifact written",
				"path", wr.RunPath,
				"latest", wr.LatestPath,
				"run_id", doc.RunID,
				"board_n", len(build.Rows),
				"schema", doc.SchemaVersion,
			)
		}
		logger.Info("stage complete", "stage", "artifact", "duration", logging.FormatDuration(time.Since(stage)))

		for i, r := range build.Rows {
			if i >= 5 {
				break
			}
			edgeVal := 0.0
			if r.EdgeBps != nil {
				edgeVal = *r.EdgeBps
			}
			logger.Info("board row",
				"rank", r.Rank,
				"condition_id", r.ConditionID,
				"edge_bps", edgeVal,
				"score_stage1", r.Score,
				"category", r.Category,
				"neg_risk", r.NegRisk,
				"vol24", r.Volume24hr,
			)
		}

		logger.Info("cycle complete",
			"status", status,
			"duration", logging.FormatDuration(time.Since(start)),
			"events", logging.FormatCount(int64(len(events))),
			"pool", logging.FormatCount(int64(build.PoolCount)),
			"stage1", logging.FormatCount(int64(build.Stage1Count)),
			"board", logging.FormatCount(int64(len(build.Rows))),
			"enrich_coverage", enrichCoverage,
			"strategy", strategy,
		)
		if !*once {
			logger.Info("next cycle",
				"next_in", logging.FormatDuration(fetchInterval),
				"next_at", time.Now().Add(fetchInterval).Format("15:04:05"),
			)
		}
		return status != artifacts.StatusFailed
	}

	if *once {
		if !runCycle() {
			os.Exit(1)
		}
		return
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	runCycle()
	ticker := time.NewTicker(fetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("edge-scan shutting down")
			return
		case <-ticker.C:
			runCycle()
		case <-quit:
			logger.Info("stop signal")
			cancel()
			return
		}
	}
}

func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	switch v {
	case "1", "true", "TRUE", "yes", "YES":
		return true
	case "0", "false", "FALSE", "no", "NO":
		return false
	default:
		return def
	}
}

func writeFailedArtifact(
	logger *slog.Logger,
	root, strategy string,
	stage1 edgescan.Stage1Result,
	status string,
	errs []artifacts.ErrorItem,
	start time.Time,
	enrichMS int64,
	coverage float64,
) {
	doc, err := edgescan.BuildArtifactWithStats(nil, stage1.PoolCount, stage1.Stage1Count, stage1.DroppedSummary, status, errs, edgescan.BoardStats{
		CycleMS:        time.Since(start).Milliseconds(),
		EnrichMS:       enrichMS,
		EnrichCoverage: coverage,
	})
	if err != nil {
		return
	}
	doc.Strategy = strategy
	if _, err := edgescan.WriteArtifact(doc, root); err != nil {
		logger.Warn("failed artifact write", "error", err)
	}
}

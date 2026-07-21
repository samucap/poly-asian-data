// Command edge-scan builds a bounded edge board from filtered Gamma events.
//
// M2: activity-score parity bridge (marketranking). M3 will replace score with cost-aware edge_bps.
// Does not scan the full open universe (unlike catalog-markets).
//
// Flags:
//
//	--once       one cycle then exit
//	--interval   override EDGE_REFRESH_INTERVAL
//	--artifacts  artifact root (default artifacts)
//	--strategy   edge_board strategy partition (default from env EDGE_STRATEGY)
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/samucap/poly-asian-data/internal/artifacts"
	"github.com/samucap/poly-asian-data/internal/catalog"
	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/edgescan"
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	factory, err := pipeline.NewFactory(ctx, cfg, logger)
	if err != nil {
		logger.Error("failed to create pipeline factory", slog.Any("error", err))
		os.Exit(1)
	}
	defer factory.Close()

	// Pipeline reserved for M2+ OI/enrich on board; ensure factory DB works.
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

	runCycle := func() (ok bool) {
		logger.Info("edge-scan cycle starting", "strategy", strategy)
		start := time.Now()
		status := artifacts.StatusSuccess
		var cycleErrs []artifacts.ErrorItem

		// Sticky set from previous board
		var sticky []string
		if esc.Sticky {
			ids, err := db.LoadEdgeBoardConditionIDs(ctx, dbPool, strategy)
			if err != nil {
				logger.Warn("load sticky board failed (continuing)", "error", err)
			} else {
				sticky = ids
			}
		}

		// 1. Filtered keyset (bounded universe)
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
			"volume_min", esc.KeysetVolumeMin,
			"liquidity_min", esc.KeysetLiquidityMin,
		)

		// 2. Flatten + Stage-1 rank + board cut
		stage = time.Now()
		pool := edgescan.FlattenCandidates(events)
		filter := marketranking.MarketFilter{
			MinVolume24hr: esc.MinVolume24hr,
			MinLiquidity:  esc.MinLiquidity,
			MaxSpread:     esc.MaxSpread,
			MinVolatility: esc.MinVolatility,
			MaxN:          esc.Stage1MaxN,
		}
		// Provisional run_id for board rows (artifact overwrites with envelope run_id)
		build := edgescan.BuildBoard(pool, edgescan.BuildOptions{
			Filter:             filter,
			BoardMaxN:          esc.BoardMaxN,
			StickyConditionIDs: sticky,
			Strategy:           strategy,
			Now:                now,
		})
		logger.Info("stage complete",
			"stage", "rank_board",
			"duration", logging.FormatDuration(time.Since(stage)),
			"pool", logging.FormatCount(int64(build.PoolCount)),
			"stage1", logging.FormatCount(int64(build.Stage1Count)),
			"board", logging.FormatCount(int64(len(build.Rows))),
		)

		if len(build.Rows) == 0 {
			cycleErrs = append(cycleErrs, artifacts.ErrorItem{
				Code: "empty_board", Message: "no markets passed Stage-1 filters", Component: "rank",
			})
			status = artifacts.StatusPartial
		}

		// 3. Artifact first to get run_id, then stamp rows
		doc, err := edgescan.BuildArtifact(build.Rows, build.PoolCount, build.Stage1Count, build.DroppedSummary, status, cycleErrs)
		if err != nil {
			logger.Error("build edge_board artifact failed", "error", err)
			return false
		}
		for i := range build.Rows {
			build.Rows[i].RunID = doc.RunID
		}
		doc.BoardStats.NBoard = len(build.Rows)

		// 4. Persist board
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

		// 5. Write artifact
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
			)
		}
		logger.Info("stage complete", "stage", "artifact", "duration", logging.FormatDuration(time.Since(stage)))

		// Top of board log (agent-friendly)
		for i, r := range build.Rows {
			if i >= 5 {
				break
			}
			logger.Info("board row",
				"rank", r.Rank,
				"condition_id", r.ConditionID,
				"score", r.Score,
				"category", r.Category,
				"neg_risk", r.NegRisk,
				"vol24", r.Volume24hr,
				"tokens", len(r.ClobTokenIDs),
			)
		}

		logger.Info("cycle complete",
			"status", status,
			"duration", logging.FormatDuration(time.Since(start)),
			"events", logging.FormatCount(int64(len(events))),
			"pool", logging.FormatCount(int64(build.PoolCount)),
			"board", logging.FormatCount(int64(len(build.Rows))),
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

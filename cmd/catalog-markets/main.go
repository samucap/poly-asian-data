// Command catalog-markets runs the full open-universe Polymarket catalog sync:
// tag catalog, Gamma events keyset, tag aggregates, plymkt_markets save, and
// a schema-valid catalog_v1 artifact (artifacts/catalog/{run_id}.json).
//
// No ranking, MaxN, or OI/trades/prices/orderbook enrichment (see edge-scan later).
// cmd/top-markets remains as the combined reference implementation.
//
// Flags:
//
//	--once        run a single cycle and exit (0 success / partial, 1 hard failure)
//	--interval    override CATALOG_REFRESH_INTERVAL (e.g. 10m)
//	--artifacts   artifact output root directory (default: artifacts)
//	--reset-tags  wipe tags (FK-safe) + catalog watermark, run sports-sync,
//	              then one catalog rebuild (use with --once for one-shot)
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/samucap/poly-asian-data/internal/artifacts"
	"github.com/samucap/poly-asian-data/internal/catalog"
	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/pipeline"
	"github.com/samucap/poly-asian-data/internal/saver"
	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/samucap/poly-asian-data/internal/tagagg"
)

func main() {
	once := flag.Bool("once", false, "run one catalog cycle and exit")
	intervalFlag := flag.String("interval", "", "override CATALOG_REFRESH_INTERVAL (e.g. 10m)")
	artifactsRoot := flag.String("artifacts", artifacts.DefaultRoot, "artifact output root directory")
	resetTags := flag.Bool("reset-tags", false, "FK-safe wipe of tags + clear catalog watermark before run (use with --once)")
	flag.Parse()

	logging.Init(os.Getenv("ENV"))

	cfg, err := config.Load()
	if err != nil {
		logging.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	logging.Init(cfg.ENV)
	logger := logging.Logger

	fetchInterval := cfg.Catalog.RefreshInterval
	if fetchInterval <= 0 {
		fetchInterval = 10 * time.Minute
	}
	if *intervalFlag != "" {
		d, err := time.ParseDuration(*intervalFlag)
		if err != nil {
			logger.Error("invalid --interval", "value", *intervalFlag, "error", err)
			os.Exit(1)
		}
		fetchInterval = d
	}
	fetcher.PaginateDelay = cfg.Catalog.PaginateDelay

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	factory, err := pipeline.NewFactory(ctx, cfg, logger)
	if err != nil {
		logger.Error("failed to create pipeline factory", slog.Any("error", err))
		os.Exit(1)
	}
	defer factory.Close()

	pipe, err := factory.Create(ctx, pipeline.Options{Name: "catalog-markets"})
	if err != nil {
		logger.Error("failed to create pipeline", slog.Any("error", err))
		os.Exit(1)
	}
	defer pipe.Stop()

	dbPool := factory.DB()
	httpClient := fetcher.NewSecureHTTPClient()

	if *resetTags {
		logger.Warn("resetting tag catalog (null sports.primary_tag_id, delete all tags, clear watermark)")
		res, err := db.ResetTagCatalog(ctx, dbPool)
		if err != nil {
			logger.Error("tag catalog reset failed", "error", err)
			os.Exit(1)
		}
		logger.Info("tag catalog reset complete",
			"sports_cleared", res.SportsCleared,
			"tags_deleted", res.TagsDeleted,
			"watermark_cleared", res.WatermarkGone,
		)

		// Re-seed sports + primary_tag_id FKs before category catalog rebuild.
		logger.Info("running sports-sync after tag reset")
		if err := pipe.SyncSportsTags(); err != nil {
			logger.Error("sports-sync after reset failed", "error", err)
			os.Exit(1)
		}
		logger.Info("sports-sync after reset complete")

		// Always run one catalog rebuild after wipe so DB is not left empty.
		if !*once {
			logger.Info("--reset-tags without --once: running a single catalog cycle then continuing as daemon")
		}
	}

	runCycle := func() (ok bool) {
		logger.Info("catalog cycle starting")
		start := time.Now()
		var cycleErrs []artifacts.ErrorItem
		status := artifacts.StatusSuccess

		// 0. Tag catalog
		stage := time.Now()
		tagRes := catalog.LoadTagCatalog(ctx, logger, dbPool, httpClient, cfg, start)
		catMap := tagRes.Catalog
		logger.Info("stage complete",
			"stage", "catalog",
			"duration", logging.FormatDuration(time.Since(stage)),
			"tags", logging.FormatCount(int64(len(catMap))),
			"tags_source", string(tagRes.Source),
		)
		if tagRes.Source == catalog.TagSourceEmpty {
			cycleErrs = append(cycleErrs, artifacts.ErrorItem{
				Code: "tag_catalog_empty", Message: "tag catalog empty from API and DB", Component: "catalog",
			})
			status = artifacts.StatusPartial
		}

		// 1. Full open-universe keyset
		stage = time.Now()
		events, err := catalog.FetchOpenEventsKeyset(ctx, httpClient, cfg)
		if err != nil {
			logger.Error("failed to fetch events (keyset)", "error", err)
			cycleErrs = append(cycleErrs, artifacts.ErrorItem{
				Code: "events_keyset_failed", Message: err.Error(), Component: "events_keyset",
			})
			// Hard failure: no artifact markets, exit 1 on --once
			logCycleHealth(logger, artifacts.StatusFailed, 0, 0, 0, time.Since(start), "", string(tagRes.Source), cycleErrs)
			return false
		}
		logger.Info("stage complete",
			"stage", "events_keyset",
			"duration", logging.FormatDuration(time.Since(stage)),
			"events", logging.FormatCount(int64(len(events))),
		)

		// 2. Aggregate
		stage = time.Now()
		var agg tagagg.Result
		if len(catMap) == 0 {
			logger.Warn("tag catalog map empty; cycle continues without tag path resolution")
			agg = tagagg.Aggregate(events, nil, catalog.CategoriesRootTagID)
		} else {
			agg = tagagg.Aggregate(events, catMap, catalog.CategoriesRootTagID)
			if agg.UnresolvedMarkets > 0 {
				logger.Info("unresolved markets (no tag path)",
					"unresolved_markets", logging.FormatCount(int64(agg.UnresolvedMarkets)),
				)
			}
		}
		logger.Info("stage complete",
			"stage", "aggregate",
			"duration", logging.FormatDuration(time.Since(stage)),
			"tradable", logging.FormatCount(int64(len(agg.Tradable))),
			"markets", logging.FormatCount(int64(len(agg.Markets))),
		)

		// Validate attribution vs Gamma activeEventsCount (flags hierarchy bugs like tag "All").
		checks := tagagg.ValidateAgainstActiveEvents(agg, 1)
		suspicious := tagagg.SuspiciousOnly(checks)
		if len(suspicious) > 0 {
			for i, c := range suspicious {
				if i >= 10 {
					break
				}
				logger.Warn("tag attribution mismatch vs activeEventsCount",
					"tag_id", c.TagID,
					"slug", c.Slug,
					"active_events_count", c.ActiveEventsCount,
					"attributed_events", c.AttributedEvents,
					"attributed_markets", c.AttributedMarkets,
					"reason", c.Reason,
				)
			}
			cycleErrs = append(cycleErrs, artifacts.ErrorItem{
				Code:      "tag_attribution_mismatch",
				Message:   "one or more tags diverge from Gamma activeEventsCount",
				Component: "aggregate",
			})
			if status == artifacts.StatusSuccess {
				status = artifacts.StatusPartial
			}
		}

		// 3. Update tags
		stage = time.Now()
		if len(catMap) > 0 {
			toWrite := tagagg.TagsForUpdate(agg.Tags)
			toWrite = append([]*services.PlyMktTag{{
				ID:    catalog.CategoriesRootTagID,
				Label: "Categories",
				Slug:  "categories",
			}}, toWrite...)
			if err := db.UpdateTags(ctx, dbPool, toWrite); err != nil {
				logger.Error("failed to update tags", "error", err)
				cycleErrs = append(cycleErrs, artifacts.ErrorItem{
					Code: "update_tags_failed", Message: err.Error(), Component: "update_tags",
				})
				status = artifacts.StatusPartial
			}
		}
		logger.Info("stage complete", "stage", "update_tags", "duration", logging.FormatDuration(time.Since(stage)))

		// 4. Save markets
		stage = time.Now()
		if len(agg.Markets) > 0 {
			if err := pipe.SubmitSave(ctx, &saver.Record{
				TableName: "plymkt_markets",
				Data:      agg.Markets,
				ItemCount: len(agg.Markets),
			}); err != nil {
				logger.Error("failed to submit markets save", "error", err)
				cycleErrs = append(cycleErrs, artifacts.ErrorItem{
					Code: "save_markets_failed", Message: err.Error(), Component: "save_markets",
				})
				status = artifacts.StatusPartial
			}
		}
		pipe.WaitUntilIdle(ctx, 500*time.Millisecond)
		logger.Info("stage complete", "stage", "save_markets", "duration", logging.FormatDuration(time.Since(stage)))

		// 5. catalog_v1 artifact
		stage = time.Now()
		var prev *catalog.CatalogV1
		if p, err := catalog.LoadPreviousCatalog(*artifactsRoot); err == nil {
			prev = p
		}
		doc, err := catalog.BuildCatalogV1(events, agg, prev, status, cycleErrs)
		artifactPath := ""
		if err != nil {
			logger.Error("failed to build catalog artifact", "error", err)
			cycleErrs = append(cycleErrs, artifacts.ErrorItem{
				Code: "artifact_build_failed", Message: err.Error(), Component: "artifact",
			})
			status = artifacts.StatusPartial
		} else {
			wr, werr := catalog.WriteCatalogArtifact(doc, *artifactsRoot)
			if werr != nil {
				logger.Error("failed to write catalog artifact", "error", werr)
				cycleErrs = append(cycleErrs, artifacts.ErrorItem{
					Code: "artifact_write_failed", Message: werr.Error(), Component: "artifact",
				})
				status = artifacts.StatusPartial
			} else {
				artifactPath = wr.RunPath
				logger.Info("catalog artifact written",
					"path", wr.RunPath,
					"latest", wr.LatestPath,
					"run_id", doc.RunID,
					"status", doc.Status,
				)
			}
		}
		logger.Info("stage complete", "stage", "artifact", "duration", logging.FormatDuration(time.Since(stage)))

		duration := time.Since(start)
		logCycleHealth(logger, status, len(events), len(agg.Markets), len(agg.Tradable), duration, artifactPath, string(tagRes.Source), cycleErrs)
		if !*once {
			logger.Info("next cycle",
				"next_in", logging.FormatDuration(fetchInterval),
				"next_at", time.Now().Add(fetchInterval).Format("15:04:05"),
			)
		}
		pipe.LogStageReport("cycle", pipe.Stats())
		logCatalogOverview(logger, events, agg)
		// success or partial both ok for daemon; --once treats partial as 0 (data written)
		return status != artifacts.StatusFailed
	}

	// After --reset-tags, always rebuild once before optional daemon loop.
	if *once || *resetTags {
		if !runCycle() {
			os.Exit(1)
		}
		if *once {
			return
		}
		// reset-tags without --once: fall through to daemon after first rebuild
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	if !*resetTags {
		// Normal daemon: immediate first cycle (reset path already ran one above).
		runCycle()
	}

	ticker := time.NewTicker(fetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Shutting down catalog-markets loop...")
			return
		case <-ticker.C:
			runCycle()
		case <-quit:
			logger.Info("Received stop signal. Stopping...")
			cancel()
			return
		}
	}
}

func logCycleHealth(
	logger *slog.Logger,
	status string,
	events, markets, tradable int,
	duration time.Duration,
	artifactPath, tagsSource string,
	errs []artifacts.ErrorItem,
) {
	logger.Info("cycle complete",
		"status", status,
		"duration", logging.FormatDuration(duration),
		"events", logging.FormatCount(int64(events)),
		"markets", logging.FormatCount(int64(markets)),
		"tradable", logging.FormatCount(int64(tradable)),
		"tags_source", tagsSource,
		"artifact", artifactPath,
		"error_count", len(errs),
	)
}

func logCatalogOverview(
	logger *slog.Logger,
	events []*services.PlyMktEvent,
	agg tagagg.Result,
) {
	logger.Info("catalog snapshot",
		"events", logging.FormatCount(int64(len(events))),
		"markets_seen", logging.FormatCount(int64(len(agg.Markets))),
		"tradable_pool", logging.FormatCount(int64(len(agg.Tradable))),
	)

	type slugRow struct {
		slug  string
		count int
	}
	var rows []slugRow
	for slug, n := range agg.PoolBySlug {
		label := slug
		if label == "" {
			label = "uncategorized"
		}
		rows = append(rows, slugRow{label, n})
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].count > rows[j].count
	})
	const maxPoolLines = 15
	for i, r := range rows {
		if i >= maxPoolLines {
			break
		}
		logger.Info("category pool",
			"slug", r.slug,
			"tradable", logging.FormatCount(int64(r.count)),
		)
	}

	sorted := tagagg.SortedByVol24(agg.Tags)
	const maxTagLines = 15
	for i, c := range sorted {
		if i >= maxTagLines {
			break
		}
		slug := c.Slug
		if slug == "" {
			slug = c.ID
		}
		logger.Info("top tag by volume",
			"slug", slug,
			"vol24hr", logging.FormatFloat(c.TotalVol24hr),
			"vol", logging.FormatFloat(c.TotalVol),
			"liq", logging.FormatFloat(c.TotalLiq),
			"markets", logging.FormatCount(int64(c.TotalMarkets)),
		)
	}
}


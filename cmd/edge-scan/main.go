// Command edge-scan builds a bounded edge board from filtered Gamma events.
//
// M3: Stage-1 activity budget → REST enrich (books/OI) → cost-aware edge_bps board.
// M5: loads active strategy_versions when --weights not set; stamps strategy_version_id.
// Orchestration lives in edgescan.RunCycle; this file is flags + loop only.
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
	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/edgescan"
	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/pipeline"
	"github.com/samucap/poly-asian-data/internal/strategyreg"
)

func main() {
	once := flag.Bool("once", false, "run one edge-scan cycle and exit")
	intervalFlag := flag.String("interval", "", "override EDGE_REFRESH_INTERVAL (e.g. 2m)")
	artifactsRoot := flag.String("artifacts", artifacts.DefaultRoot, "artifact output root")
	strategyFlag := flag.String("strategy", "", "edge_board strategy name")
	weightsFlag := flag.String("weights", "", "path to strategy weights YAML (overrides strategy_active)")
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

	// Explicit file override: --weights or EDGE_WEIGHTS_PATH.
	explicitPath := *weightsFlag
	if explicitPath == "" {
		explicitPath = os.Getenv("EDGE_WEIGHTS_PATH")
	}
	fallbackPath := filepath.Join("configs", "strategies", "default.yaml")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	factory, err := pipeline.NewFactory(ctx, cfg, logger)
	if err != nil {
		logger.Error("failed to create pipeline factory", slog.Any("error", err))
		os.Exit(1)
	}
	defer factory.Close()

	dbPool := factory.DB()
	if err := db.EnsureEdgeBoardTable(ctx, dbPool); err != nil {
		logger.Error("ensure edge_board table failed", "error", err)
		os.Exit(1)
	}
	if err := db.EnsureStrategyTables(ctx, dbPool); err != nil {
		logger.Warn("ensure strategy tables", "error", err)
	}

	deps := edgescan.CycleDeps{
		DB:                  dbPool,
		HTTP:                fetcher.NewSecureHTTPClient(),
		Cfg:                 cfg,
		Logger:              logger,
		Strategy:            strategy,
		ArtifactsRoot:       *artifactsRoot,
		EdgeScan:            esc,
		EnrichBooks:         envBool("EDGE_ENRICH_BOOKS", true),
		EnrichOI:            envBool("EDGE_ENRICH_OI", true),
		PublishOnEnrichFail: envBool("EDGE_PUBLISH_ON_ENRICH_FAIL", false),
		OIMaxConditions:     80,
		OIConcurrency:       16,
		BookBatchSize:       50,
		EnrichPrices:        envBool("EDGE_ENRICH_PRICES", true),
		PriceTokenCap:       300,
		PriceFidelityMin:    60,
		PriceLookback:       cfg.TopMarkets.PriceLookback,
		PriceWarmMaxAge:     30 * time.Minute,
		ForceColdPrices:     envBool("EDGE_PRICES_FORCE_COLD", false),
	}

	runOnce := func() bool {
		res := strategyreg.ResolveLive(ctx, dbPool, strategyreg.ResolveOpts{
			Strategy:     strategy,
			ExplicitPath: explicitPath,
			FallbackPath: fallbackPath,
		})
		deps.Weights = res.Weights
		deps.StrategyVersionID = res.VersionID
		if res.LoadNote != "" {
			logger.Warn("weights resolve note", "note", res.LoadNote)
		}
		if res.OverrideDiffers {
			logger.Warn("using --weights override; strategy_active differs",
				"file", res.Path,
				"active_version_id", res.ActiveVersionID,
				"file_hash", truncateHash(res.Hash),
				"active_hash", truncateHash(res.ActiveHash),
			)
		}
		logger.Info("edge-scan cycle starting",
			"strategy", strategy,
			"schema", edgescan.SchemaVersion,
			"strategy_version_id", res.VersionID,
			"weights_source", res.Source,
			"weights_path", res.Path,
			"weights_hash", truncateHash(res.Hash),
		)
		r := edgescan.RunCycle(ctx, deps)
		return r.OK
	}

	if *once {
		if !runOnce() {
			os.Exit(1)
		}
		return
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	runOnce()
	ticker := time.NewTicker(fetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("edge-scan shutting down")
			return
		case <-ticker.C:
			logger.Info("next cycle",
				"next_in", logging.FormatDuration(fetchInterval),
				"next_at", time.Now().Add(fetchInterval).Format("15:04:05"),
			)
			runOnce()
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

func truncateHash(h string) string {
	if len(h) <= 12 {
		return h
	}
	return h[:12] + "…"
}

// Command edge-scan builds a bounded edge board from filtered Gamma events.
//
// M3: Stage-1 activity budget → REST enrich (books/OI) → cost-aware edge_bps board.
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
	"github.com/samucap/poly-asian-data/internal/edge"
	"github.com/samucap/poly-asian-data/internal/edgescan"
	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/pipeline"
)

func main() {
	once := flag.Bool("once", false, "run one edge-scan cycle and exit")
	intervalFlag := flag.String("interval", "", "override EDGE_REFRESH_INTERVAL (e.g. 2m)")
	artifactsRoot := flag.String("artifacts", artifacts.DefaultRoot, "artifact output root")
	strategyFlag := flag.String("strategy", "", "edge_board strategy name")
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

	// Pipeline factory for shared Postgres pool (no full pipeline jobs).
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

	deps := edgescan.CycleDeps{
		DB:                  dbPool,
		HTTP:                fetcher.NewSecureHTTPClient(),
		Cfg:                 cfg,
		Logger:              logger,
		Strategy:            strategy,
		Weights:             weights,
		ArtifactsRoot:       *artifactsRoot,
		EdgeScan:            esc,
		EnrichBooks:         envBool("EDGE_ENRICH_BOOKS", true),
		EnrichOI:            envBool("EDGE_ENRICH_OI", true),
		PublishOnEnrichFail: envBool("EDGE_PUBLISH_ON_ENRICH_FAIL", false),
		OIMaxConditions:     80,
		OIConcurrency:       16,
		BookBatchSize:       50,
		// M4: post-board prices_history (board ∪ top volume); never blocks board publish.
		EnrichPrices:     envBool("EDGE_ENRICH_PRICES", true),
		PriceTokenCap:    300,
		PriceFidelityMin: 60,
		PriceLookback:    cfg.TopMarkets.PriceLookback,
		PriceWarmMaxAge:  30 * time.Minute,
		ForceColdPrices:  envBool("EDGE_PRICES_FORCE_COLD", false),
	}

	runOnce := func() bool {
		logger.Info("edge-scan cycle starting", "strategy", strategy, "schema", edgescan.SchemaVersion)
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

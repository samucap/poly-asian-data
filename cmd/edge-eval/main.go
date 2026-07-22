// Command edge-eval runs bias-aware offline evaluation (M4).
//
// Default mode is DB-only: reads prices_history (+ optional books) and writes
// artifacts/eval_surface/{run_id}.json. Does not call CLOB in the T-loop.
//
//	go run ./cmd/edge-eval --once
//	go run ./cmd/edge-eval --once --lookback 720h --stride 2 --board-n 50
//	go run ./cmd/edge-eval --once --persist-labels
//
// Optional one-shot price backfill (same helper as edge-scan warm stage):
//
//	go run ./cmd/edge-eval --backfill-prices --once
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
	"github.com/samucap/poly-asian-data/internal/enrich"
	"github.com/samucap/poly-asian-data/internal/eval"
	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/pipeline"
)

func main() {
	once := flag.Bool("once", true, "run one eval and exit (default true)")
	artifactsRoot := flag.String("artifacts", artifacts.DefaultRoot, "artifact output root")
	configPath := flag.String("config", "configs/eval/default.yaml", "eval protocol YAML")
	lookback := flag.Duration("lookback", 30*24*time.Hour, "decision lookback window")
	stride := flag.Int("stride", 2, "take every Nth hour bucket")
	boardN := flag.Int("board-n", 50, "board size for candidate and baselines")
	universeCap := flag.Int("universe-cap", 500, "max markets per decision time")
	strategy := flag.String("strategy", "default", "edge_board strategy for token hints")
	persistLabels := flag.Bool("persist-labels", false, "upsert label_rows")
	backfillPrices := flag.Bool("backfill-prices", false, "one-shot EnsurePrices before eval (HTTP); prefer edge-scan")
	priceTokenCap := flag.Int("price-tokens", 300, "token cap for --backfill-prices")
	seed := flag.Int64("seed", 42, "RNG seed for random_board baseline")
	weightsFlag := flag.String("weights", "", "strategy weights YAML (default configs/strategies/default.yaml)")
	noBooks := flag.Bool("no-books", false, "prices-only costs (skip orderbook_snapshots load)")
	flag.Parse()

	logging.Init(os.Getenv("ENV"))
	cfg, err := config.Load()
	if err != nil {
		logging.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	logging.Init(cfg.ENV)
	logger := logging.Logger

	evalCfg, err := eval.LoadConfig(*configPath)
	if err != nil {
		logger.Warn("eval config load failed; using defaults", "error", err)
		evalCfg = eval.DefaultConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-quit
		cancel()
	}()

	factory, err := pipeline.NewFactory(ctx, cfg, logger)
	if err != nil {
		logger.Error("pipeline factory", slog.Any("error", err))
		os.Exit(1)
	}
	defer factory.Close()
	dbPool := factory.DB()

	if err := db.EnsureLabelRowsTable(ctx, dbPool); err != nil {
		logger.Warn("ensure label_rows", "error", err)
	}

	if *backfillPrices {
		logger.Info("backfill-prices starting", "cap", *priceTokenCap)
		board, _ := db.LoadEdgeBoardTokens(ctx, dbPool, *strategy)
		top, err := db.LoadTopVolumeMarkets(ctx, dbPool, *priceTokenCap)
		if err != nil {
			logger.Error("load top volume for backfill", "error", err)
			os.Exit(1)
		}
		tokens := enrich.SelectPriceTokens(board, top, *priceTokenCap)
		tm := map[string]string{}
		for _, t := range tokens {
			tm[t.TokenID] = t.MarketID
		}
		pr := enrich.EnsurePrices(ctx, dbPool, fetcher.NewSecureHTTPClient(), tokens, enrich.EnsurePricesConfig{
			Lookback:    *lookback,
			FidelityMin: 60,
			ForceCold:   true,
			Logger:      logger,
			TokenMarket: tm,
		})
		logger.Info("backfill-prices done",
			"points", pr.PointsWritten,
			"tokens", pr.TokensWritten,
			"errors", len(pr.Errors),
		)
		if len(pr.Errors) > 0 {
			for _, e := range pr.Errors {
				logger.Warn("backfill error", "err", e)
			}
		}
	}

	weightsPath := *weightsFlag
	if weightsPath == "" {
		weightsPath = filepath.Join("configs", "strategies", "default.yaml")
	}
	weights, err := edge.LoadWeightsFile(weightsPath)
	if err != nil {
		logger.Warn("weights not loaded; using defaults", "path", weightsPath, "error", err)
		weights = edge.DefaultWeights()
	} else {
		logger.Info("loaded strategy weights", "path", weightsPath, "name", weights.Name)
	}

	bc := eval.DefaultBacktestConfig()
	bc.Cfg = evalCfg
	bc.Lookback = *lookback
	bc.Stride = *stride
	bc.BoardN = *boardN
	bc.UniverseCap = *universeCap
	bc.Seed = *seed
	bc.End = time.Now().UTC()
	bc.Weights = weights

	useBooks := !*noBooks
	logger.Info("edge-eval starting",
		"lookback", lookback.String(),
		"stride", *stride,
		"board_n", *boardN,
		"primary_horizon", evalCfg.PrimaryHorizon,
		"policy", eval.PolicyIDSelectBoardV1,
		"weights", weights.Name,
		"use_books", useBooks,
	)

	res, err := eval.RunDBOnly(ctx, eval.RunnerOpts{
		DB:            dbPool,
		Logger:        logger,
		Cfg:           evalCfg,
		Backtest:      bc,
		ArtifactsRoot: *artifactsRoot,
		Strategy:      *strategy,
		PersistLabels: *persistLabels,
		UseBooks:      &useBooks,
		WeightsPath:   weightsPath,
	})
	if err != nil {
		logger.Error("edge-eval failed", "error", err)
		os.Exit(1)
	}

	s := res.Surface
	logger.Info("edge-eval complete",
		"ok", s.OK,
		"promote_eligible", s.PromoteEligible,
		"policy_parity", s.PolicyParity,
		"status", s.Status,
		"n", s.Metrics.N,
		"after_cost_bps", s.Metrics.Overall.AfterCostReturnBps,
		"policy", s.PolicyID,
		"fv_coverage", s.FVCoverage,
		"book_coverage", s.BookCoverage,
		"weights_hash", s.WeightsHash,
		"gates_failed", s.GatesFailed,
		"snaps", res.NSnaps,
		"tokens", res.NTokens,
		"duration", res.Duration.String(),
		"artifact", res.Write.LatestPath,
	)
	if !*once {
		// reserved for future continuous mode
	}
	// Exit 0 even when ok=false (honest thin sample is success of the tool).
	// Exit 1 only on hard errors above.
}

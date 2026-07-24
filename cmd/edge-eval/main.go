// Command edge-eval runs bias-aware offline board-policy evaluation.
//
// Default mode is DB-only: reads prices_history (+ optional books) and writes
// artifacts/eval_surface/{run_id}.json. Does not call CLOB in the T-loop.
//
//	go run ./cmd/edge-eval --once
//	go run ./cmd/edge-eval --once --lookback 720h --stride 2 --board-n 50
//	go run ./cmd/edge-eval --once --persist-labels
//
// Optional one-shot price backfill (ops escape only — prefer edge-scan WARM for series):
//
//	go run ./cmd/edge-eval --backfill-prices --once
package main

import (
	"context"
	"flag"
	"fmt"
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
	"github.com/samucap/poly-asian-data/internal/datagap"
	"github.com/samucap/poly-asian-data/internal/enrich"
	"github.com/samucap/poly-asian-data/internal/eval"
	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/pipeline"
	"github.com/samucap/poly-asian-data/internal/strategyreg"
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
	backfillPrices := flag.Bool("backfill-prices", false, "ops escape: one-shot HTTP price backfill before eval; default is DB-only (prefer edge-scan WARM)")
	priceTokenCap := flag.Int("price-tokens", 300, "token cap for --backfill-prices")
	seed := flag.Int64("seed", 42, "RNG seed for random_board baseline")
	weightsFlag := flag.String("weights", "", "strategy weights YAML (default configs/strategies/default.yaml)")
	versionIDFlag := flag.Int64("version-id", 0, "load board-policy weights from strategy_versions.id (M5)")
	noBooks := flag.Bool("no-books", false, "prices-only costs (skip orderbook_snapshots load)")
	synthFill := flag.Bool("synth-fill", false, "dev: interpolate/hold sparse prices + synth books; labeled; blocks promote if share high")
	synthMaxGap := flag.Duration("synth-max-gap", 48*time.Hour, "max anchor distance for linear price interp")
	synthHoldMax := flag.Duration("synth-hold-max", 6*time.Hour, "max flat hold from a single price anchor")
	synthSpreadBps := flag.Float64("synth-default-spread-bps", 50, "full spread bps for synthetic books when no recent book")
	synthPromoteMax := flag.Float64("synth-promote-max-share", 0.05, "synth share above this blocks promote_eligible")
	synthSignificant := flag.Float64("synth-significant-share", 0.20, "synth share at/above this is ALERT and ok=false")
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
	if err := db.EnsureStrategyTables(ctx, dbPool); err != nil {
		logger.Warn("ensure strategy tables", "error", err)
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
	var weights edge.Weights
	var strategyVersionID *int64
	var weightsHash string
	if *versionIDFlag > 0 {
		w, v, err := strategyreg.LoadVersion(ctx, dbPool, *versionIDFlag)
		if err != nil {
			logger.Error("load strategy version", "id", *versionIDFlag, "error", err)
			os.Exit(1)
		}
		weights = w
		id := v.ID
		strategyVersionID = &id
		weightsHash = v.WeightsHash
		weightsPath = v.SourcePath
		logger.Info("loaded strategy version",
			"id", v.ID,
			"strategy", v.Strategy,
			"weights_hash", v.WeightsHash,
			"name", weights.Name,
		)
	} else {
		var err error
		weights, err = edge.LoadWeightsFile(weightsPath)
		if err != nil {
			logger.Warn("weights not loaded; using defaults", "path", weightsPath, "error", err)
			weights = edge.DefaultWeights()
		} else {
			logger.Info("loaded strategy weights", "path", weightsPath, "name", weights.Name)
		}
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
		"strategy_version_id", strategyVersionID,
		"use_books", useBooks,
	)

	synthOpts := datagap.DefaultOpts()
	synthOpts.MaxGap = *synthMaxGap
	synthOpts.HoldMax = *synthHoldMax
	synthOpts.DefaultHalfSpreadBps = *synthSpreadBps

	res, err := eval.RunDBOnly(ctx, eval.RunnerOpts{
		DB:                    dbPool,
		Logger:                logger,
		Cfg:                   evalCfg,
		Backtest:              bc,
		ArtifactsRoot:         *artifactsRoot,
		Strategy:              *strategy,
		PersistLabels:         *persistLabels,
		UseBooks:              &useBooks,
		WeightsPath:           weightsPath,
		StrategyVersionID:     strategyVersionID,
		WeightsHash:           weightsHash,
		SynthFill:             *synthFill,
		Synth:                 synthOpts,
		SynthPromoteMaxShare:  *synthPromoteMax,
		SynthSignificantShare: *synthSignificant,
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
		"strategy_version_id", s.StrategyVersionID,
		"gates_failed", s.GatesFailed,
		"snaps", res.NSnaps,
		"tokens", res.NTokens,
		"duration", res.Duration.String(),
		"artifact", res.Write.LatestPath,
	)
	if s != nil {
		printBoardBacktestSummary(*s, res.Write.LatestPath)
	}
	if !*once {
		// reserved for future continuous mode
	}
	// Exit 0 even when ok=false (honest thin sample is success of the tool).
	// Exit 1 only on hard errors above.
}

// printBoardBacktestSummary is a one-screen human readout for board-policy backtest.
// Labels ≠ paper signals; portfolio metrics are backtest path risk, not live P&L.
func printBoardBacktestSummary(s eval.EvalSurface, artifact string) {
	o := s.Metrics.Overall
	var totRet, maxDD, sharpe float64
	var nPer int
	if s.Metrics.Portfolio != nil {
		totRet = s.Metrics.Portfolio.TotalReturnBps
		maxDD = s.Metrics.Portfolio.MaxDrawdownBps
		sharpe = s.Metrics.Portfolio.Sharpe
		nPer = s.Metrics.Portfolio.NPeriods
	} else {
		maxDD = o.MaxDrawdownBps
	}
	deltaVol := 0.0
	deltaRnd := 0.0
	if s.Metrics.DeltaVsBaselines != nil {
		deltaVol = s.Metrics.DeltaVsBaselines["volume_top_n"]
		deltaRnd = s.Metrics.DeltaVsBaselines["random_board"]
	}
	name := s.StrategyName
	if name == "" {
		name = s.WeightsPath
	}
	if name == "" {
		name = "(default weights)"
	}
	dqLine := "venue_only"
	if s.DataQuality != nil {
		dqLine = fmt.Sprintf("price_synth=%.1f%% book_synth=%.1f%% significant=%v block_promote=%v",
			100*s.DataQuality.SynthPriceShare, 100*s.DataQuality.SynthBookShare,
			s.DataQuality.SignificantSynth, s.DataQuality.BlockPromote)
		if s.DataQuality.Warning != "" {
			dqLine += "\n  warn: " + s.DataQuality.Warning
		}
	}
	fmt.Fprintf(os.Stdout, `
======== BOARD-POLICY BACKTEST (edge-eval) ========
weights:          %s
labels (n):       %d
after_cost_bps:   %.2f          # mean label economy after costs
hit_rate:         %.3f
total_return_bps: %.2f          # backtest path (~%.2f%%)
max_dd_bps:       %.2f          # backtest path max drawdown
sharpe:           %.2f          # backtest path only (not live)
portfolio_periods:%d
vs volume_top_n:  %+.2f bps     # delta after_cost vs liquid baseline
vs random_board:  %+.2f bps
ok:               %v
promote_eligible: %v            # false if synth share high
data_quality:     %s
book_coverage:    %.2f  fv_coverage: %.2f
artifact:         %s
======================================================
`,
		name,
		s.Metrics.N,
		o.AfterCostReturnBps,
		o.HitRate,
		totRet, totRet/100.0,
		maxDD,
		sharpe,
		nPer,
		deltaVol,
		deltaRnd,
		s.OK,
		s.PromoteEligible,
		dqLine,
		s.BookCoverage, s.FVCoverage,
		artifact,
	)
}

// Command signal-eval runs M8 paper portfolio risk + fill simulation.
// Scores signals + deciding logic for an external auto-optimizer.
// No live orders (OMS). Not M4 board eval / promote_eligible.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"crypto/rand"
	"encoding/hex"

	"github.com/samucap/poly-asian-data/internal/artifacts"
	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/pipeline"
	"github.com/samucap/poly-asian-data/internal/risk"
	"github.com/samucap/poly-asian-data/internal/signaleval"
)

func main() {
	artifactsRoot := flag.String("artifacts", artifacts.DefaultRoot, "artifact root")
	riskPath := flag.String("risk", "configs/risk/default.yaml", "risk policy YAML")
	lookback := flag.Duration("lookback", 7*24*time.Hour, "signal window lookback")
	strategy := flag.String("strategy", "default", "strategy name filter")
	versionID := flag.Int64("version-id", 0, "optional strategy_version_id filter")
	synthetic := flag.Bool("synthetic", false, "use built-in fixture (no DB signals required)")
	minSample := flag.Int("min-sample", 3, "min signals for ok gate")
	startingEquity := flag.Float64("starting-equity", 0, "override risk starting_equity_usd (0=config)")
	flag.Parse()

	logging.Init(os.Getenv("ENV"))
	logger := logging.Logger

	rcfg, err := risk.LoadConfig(*riskPath)
	if err != nil {
		logger.Warn("risk config; using defaults", "error", err)
		rcfg = risk.DefaultConfig()
	}
	if *startingEquity > 0 {
		rcfg.StartingEquityUSD = *startingEquity
	}
	rcfg.Normalize()

	var (
		sigs    []signaleval.SignalIn
		prices  signaleval.PriceIndex
		from, to time.Time
	)

	if *synthetic {
		sigs, prices = signaleval.SyntheticFixture()
		// Apply tight budget demo if default risk leaves all accepted — keep user config
		// but batch window helps ranking when simultaneous.
		if rcfg.BatchWindowMs == 0 {
			rcfg.BatchWindowMs = 1000
		}
		if len(sigs) > 0 {
			from, to = sigs[0].Time, sigs[len(sigs)-1].Time
		}
		logger.Info("synthetic fixture", "n_signals", len(sigs))
	} else {
		cfg, err := config.Load()
		if err != nil {
			logger.Error("config", "error", err)
			os.Exit(1)
		}
		logging.Init(cfg.ENV)
		logger = logging.Logger

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-sigCh
			cancel()
		}()

		factory, err := pipeline.NewFactory(ctx, cfg, logger)
		if err != nil {
			logger.Error("factory", "error", err)
			os.Exit(1)
		}
		defer factory.Close()
		pool := factory.DB()
		_ = db.EnsureSignalsTable(ctx, pool)

		to = time.Now().UTC()
		from = to.Add(-*lookback)
		fil := db.SignalFilter{
			Strategy: *strategy,
			Mode:     "paper",
			From:     from,
			To:       to,
		}
		if *versionID > 0 {
			v := *versionID
			fil.StrategyVersionID = &v
		}
		rows, err := db.LoadSignals(ctx, pool, fil)
		if err != nil {
			logger.Error("load signals", "error", err)
			os.Exit(1)
		}
		sigs = rowsToSignals(rows)
		// Prices: load series for tokens
		toks := uniqueTokens(sigs)
		if len(toks) > 0 {
			series, err := db.LoadPriceSeries(ctx, pool, toks, from.Add(-2*time.Hour), to.Add(48*time.Hour))
			if err != nil {
				logger.Warn("load prices", "error", err)
			} else {
				prices = priceIndexFromDB(series)
			}
		}
		logger.Info("loaded signals", "n", len(sigs), "tokens", len(toks))
	}

	res := signaleval.Simulate(signaleval.SimConfig{Risk: rcfg}, sigs, prices)
	runID := newRunID()
	surf := signaleval.BuildSurface(runID, *strategy, res, from, to, *minSample)

	wr, err := artifacts.WriteJSON(runID, "signal_eval_surface", surf, artifacts.WriteOptions{
		Root: *artifactsRoot, WriteLatest: true,
	})
	if err != nil {
		logger.Error("write artifact", "error", err)
		os.Exit(1)
	}

	summary, _ := json.Marshal(map[string]any{
		"ok":                     surf.OK,
		"n_signals":              res.Metrics.NSignals,
		"n_accepted":             res.Metrics.NAccepted,
		"n_rejected":             res.Metrics.NRejectedRisk,
		"hit_rate":               res.Metrics.HitRate,
		"total_pnl_usd":          res.Metrics.TotalPnLUSD,
		"total_return_bps":       res.Metrics.TotalReturnBps,
		"sharpe":                 res.Metrics.Sharpe,
		"sharpe_note":            res.Metrics.SharpeNote,
		"n_periods":              res.Metrics.NPeriods,
		"mean_period_return":     res.Metrics.MeanPeriodReturn,
		"max_drawdown_bps":       res.Metrics.MaxDrawdownBps,
		"max_daily_drawdown_bps": res.Metrics.MaxDailyDrawdownBps,
		"config_hash":            res.ConfigHash[:12] + "…",
		"path":                   wr.LatestPath,
	})
	fmt.Println(string(summary))
	if !surf.OK {
		os.Exit(2)
	}
}

func rowsToSignals(rows []db.SignalRow) []signaleval.SignalIn {
	out := make([]signaleval.SignalIn, 0, len(rows))
	for _, r := range rows {
		out = append(out, signaleval.SignalIn{
			Time: r.Time, SignalID: r.SignalID, Strategy: r.Strategy,
			StrategyVersionID: r.StrategyVersionID,
			ConditionID: r.ConditionID, TokenID: r.TokenID, MarketID: r.MarketID,
			Side: r.Side, Action: r.Action, EdgeBps: r.EdgeBps, Conviction: r.Conviction,
			Urgency: r.Urgency, SizeUSD: r.SizeUSD, CapacityUSD: r.CapacityUSD,
			CostBps: r.CostBps, Mid: r.Mid, HorizonSec: r.HorizonSec, KellyFrac: r.KellyFrac,
		})
	}
	return out
}

func uniqueTokens(sigs []signaleval.SignalIn) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, s := range sigs {
		if s.TokenID == "" {
			continue
		}
		if _, ok := seen[s.TokenID]; ok {
			continue
		}
		seen[s.TokenID] = struct{}{}
		out = append(out, s.TokenID)
	}
	return out
}

func priceIndexFromDB(series map[string][]db.PricePoint) signaleval.PriceIndex {
	out := make(signaleval.PriceIndex, len(series))
	for tok, pts := range series {
		for _, p := range pts {
			out[tok] = append(out[tok], signaleval.PricePoint{Time: p.Timestamp, Mid: p.Price})
		}
	}
	return out
}

func newRunID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

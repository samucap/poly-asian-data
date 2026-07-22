// Command ws-listener streams Polymarket market WS for the edge board only (M6).
// Maintains books in memory, flushes features_latest (and optional snapshots) with
// write-minimized dirty batches, and emits multi-dimensional paper signals.
// No live order placement.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/pipeline"
	"github.com/samucap/poly-asian-data/internal/signals"
	"github.com/samucap/poly-asian-data/internal/ws/market"
)

func main() {
	once := flag.Bool("once", false, "run briefly then exit (smoke)")
	onceDur := flag.Duration("once-duration", 20*time.Second, "how long --once listens")
	strategyFlag := flag.String("strategy", "default", "edge_board strategy partition")
	boardPoll := flag.Duration("board-poll", 12*time.Second, "refresh board asset set")
	maxAssets := flag.Int("max-assets", 180, "hard cap on subscribed assets")
	wsURL := flag.String("ws-url", market.DefaultWSURL, "market channel URL")
	featuresFlush := flag.Duration("features-flush", 3*time.Second, "dirty features_latest batch interval")
	snapshotFlush := flag.Duration("snapshot-flush", 15*time.Second, "dirty orderbook_snapshots batch (0=off)")
	noSnapshots := flag.Bool("no-snapshots", false, "disable orderbook_snapshots writes")
	signalEvery := flag.Duration("signal-every", 2*time.Second, "paper signal eval interval (memory)")
	signalCooldown := flag.Duration("signal-cooldown", 60*time.Second, "debounce per condition")
	minEdgeBps := flag.Float64("min-edge-bps", 0, "min |net edge| to emit paper signal")
	maxSpreadBps := flag.Float64("max-spread-bps", 500, "reject books wider than this")
	probeSize := flag.Float64("probe-size-usd", 25, "advisory size probe")
	flag.Parse()

	logging.Init(os.Getenv("ENV"))
	cfg, err := config.Load()
	if err != nil {
		logging.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	logging.Init(cfg.ENV)
	logger := logging.Logger

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("shutdown signal")
		cancel()
	}()

	factory, err := pipeline.NewFactory(ctx, cfg, logger)
	if err != nil {
		logger.Error("pipeline factory", "error", err)
		os.Exit(1)
	}
	defer factory.Close()
	pool := factory.DB()

	if err := db.EnsureFeaturesLatestTable(ctx, pool); err != nil {
		logger.Error("ensure features_latest", "error", err)
		os.Exit(1)
	}
	if err := db.EnsureSignalsTable(ctx, pool); err != nil {
		logger.Error("ensure signals", "error", err)
		os.Exit(1)
	}
	if err := db.EnsureEdgeBoardTable(ctx, pool); err != nil {
		logger.Warn("ensure edge_board", "error", err)
	}

	store := market.NewBookStore()
	gate := signals.DefaultGateConfig()
	gate.MinEdgeBps = *minEdgeBps
	gate.MaxSpreadBps = *maxSpreadBps
	gate.Cooldown = *signalCooldown
	gate.ProbeSizeUSD = *probeSize
	emitter := signals.NewEmitter(gate)

	var boardMeta *db.BoardSubscribeSet

	client := market.NewClient(*wsURL, logger)
	client.OnEvent = func(ev market.ParsedEvent) {
		store.Apply(ev)
	}

	// Board poll loop
	go func() {
		t := time.NewTicker(*boardPoll)
		defer t.Stop()
		refresh := func() {
			set, err := db.LoadBoardSubscribeSet(ctx, pool, *strategyFlag, *maxAssets)
			if err != nil {
				logger.Warn("load board assets", "error", err)
				return
			}
			boardMeta = set
			if err := client.SetDesired(ctx, set.TokenIDs); err != nil {
				logger.Warn("set desired assets", "error", err)
			}
			logger.Info("board assets",
				"strategy", *strategyFlag,
				"tokens", len(set.TokenIDs),
				"primaries", len(set.Primaries),
				"subscribed", len(client.Subscribed()),
			)
		}
		refresh()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				refresh()
			}
		}
	}()

	// Features flush (dirty only)
	go func() {
		if *featuresFlush <= 0 {
			return
		}
		t := time.NewTicker(*featuresFlush)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				dirty := store.TakeDirty(100)
				if len(dirty) == 0 {
					continue
				}
				rows := make([]db.FeaturesLatestRow, 0, len(dirty))
				now := time.Now().UTC()
				for _, b := range dirty {
					cond, mkt := "", b.MarketID
					if boardMeta != nil {
						if m, ok := boardMeta.ByToken[b.TokenID]; ok {
							cond = m.ConditionID
							if mkt == "" {
								mkt = m.MarketID
							}
						}
					}
					mid := b.Mid()
					rows = append(rows, db.FeaturesLatestRow{
						TokenID:        b.TokenID,
						ConditionID:    cond,
						MarketID:       mkt,
						BestBid:        b.BestBid,
						BestAsk:        b.BestAsk,
						Mid:            mid,
						LastTradePrice: b.LastTradePrice,
						Spread:         b.Spread(),
						Imbalance:      b.Imbalance,
						BidDepth:       b.BidDepth,
						AskDepth:       b.AskDepth,
						UpdatedAt:      pickTime(b.UpdatedAt, now),
					})
				}
				if err := db.UpsertFeaturesLatest(ctx, pool, rows); err != nil {
					logger.Warn("flush features_latest", "error", err, "n", len(rows))
					// re-dirty would require re-mark; accept loss until next change
					continue
				}
				logger.Debug("flushed features_latest", "n", len(rows), "dirty_left", store.DirtyCount())
			}
		}
	}()

	// Snapshot flush (optional, slower)
	if !*noSnapshots && *snapshotFlush > 0 {
		go func() {
			t := time.NewTicker(*snapshotFlush)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					// Independent of features Dirty — only mid/BBA change since last snap.
					changed := store.TakeChangedForSnapshot(100)
					if len(changed) == 0 {
						continue
					}
					now := time.Now().UTC()
					rows := make([]db.OrderbookSnapshotRow, 0, len(changed))
					for _, b := range changed {
						if !b.ValidBook() {
							continue
						}
						mkt := b.MarketID
						if mkt == "" {
							mkt = "unknown"
						}
						rows = append(rows, db.OrderbookSnapshotRow{
							Time:          pickTime(b.UpdatedAt, now),
							MarketID:      mkt,
							TokenID:       b.TokenID,
							BestBid:       b.BestBid,
							BestAsk:       b.BestAsk,
							Imbalance:     b.Imbalance,
							TotalBidDepth: b.BidDepth,
							TotalAskDepth: b.AskDepth,
							// no DepthJSON / RawJSON — minimize write size
						})
					}
					if len(rows) == 0 {
						continue
					}
					if err := db.InsertOrderbookSnapshots(ctx, pool, rows); err != nil {
						logger.Warn("flush snapshots", "error", err, "n", len(rows))
						continue
					}
					logger.Debug("flushed snapshots", "n", len(rows))
				}
			}
		}()
	}

	// Paper signals from memory
	go func() {
		if *signalEvery <= 0 {
			return
		}
		t := time.NewTicker(*signalEvery)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if boardMeta == nil {
					continue
				}
				now := time.Now().UTC()
				var out []db.SignalRow
				for _, p := range boardMeta.Primaries {
					bk, ok := store.Snapshot(p.PrimaryTokenID)
					if !ok || !bk.ValidBook() {
						continue
					}
					board := signals.BoardSnap{
						ConditionID:       p.ConditionID,
						MarketID:          p.MarketID,
						TokenID:           p.PrimaryTokenID,
						Rank:              p.Rank,
						Score:             p.Score,
						EdgeBps:           p.EdgeBps,
						CostBps:           p.CostBps,
						CapacityUSD:       p.CapacityUSD,
						Urgency:           p.Urgency,
						FairValue:         p.FairValue,
						ModelEdgeBps:      p.ModelEdgeBps,
						FVSource:          p.FVSource,
						NegRisk:           p.NegRisk,
						NegRiskGroupID:    p.NegRiskGroupID,
						StrategyTags:      p.StrategyTags,
						RiskFlags:         p.RiskFlags,
						KeyFeatures:       p.KeyFeatures,
						StrategyVersionID: p.StrategyVersionID,
						RunID:             p.RunID,
					}
					book := signals.BookSnap{
						BestBid: bk.BestBid, BestAsk: bk.BestAsk, Mid: bk.Mid(),
						LastTradePrice: bk.LastTradePrice, Imbalance: bk.Imbalance,
						BidDepth: bk.BidDepth, AskDepth: bk.AskDepth, UpdatedAt: bk.UpdatedAt,
					}
					sig := emitter.Evaluate(now, *strategyFlag, board, book)
					if sig == nil {
						continue
					}
					out = append(out, paperToRow(sig))
				}
				if len(out) == 0 {
					continue
				}
				if err := db.InsertSignals(ctx, pool, out); err != nil {
					logger.Warn("insert signals", "error", err, "n", len(out))
					continue
				}
				logger.Info("paper signals", "n", len(out))
			}
		}
	}()

	// Metrics ticker
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				logger.Info("ws-listener stats",
					"books", store.Len(),
					"dirty", store.DirtyCount(),
					"subscribed", len(client.Subscribed()),
				)
			}
		}
	}()

	runCtx := ctx
	if *once {
		var cxl context.CancelFunc
		runCtx, cxl = context.WithTimeout(ctx, *onceDur)
		defer cxl()
		logger.Info("ws-listener --once", "duration", *onceDur)
	}

	logger.Info("ws-listener starting",
		"url", *wsURL,
		"strategy", *strategyFlag,
		"max_assets", *maxAssets,
		"features_flush", *featuresFlush,
		"snapshot_flush", *snapshotFlush,
		"no_snapshots", *noSnapshots,
	)
	if err := client.Run(runCtx); err != nil && runCtx.Err() == nil {
		logger.Error("ws client exited", "error", err)
		os.Exit(1)
	}
	logger.Info("ws-listener stopped", slog.Any("books", store.Len()))
}

func pickTime(a, fallback time.Time) time.Time {
	if a.IsZero() {
		return fallback
	}
	return a
}

func paperToRow(s *signals.PaperSignal) db.SignalRow {
	return db.SignalRow{
		Time: s.Time, SignalID: s.SignalID, Event: s.Event, SupersedesID: s.SupersedesID,
		Strategy: s.Strategy, StrategyVersionID: s.StrategyVersionID,
		BoardRunID: s.BoardRunID, BoardRank: s.BoardRank, Mode: s.Mode,
		ConditionID: s.ConditionID, MarketID: s.MarketID, TokenID: s.TokenID,
		Outcome: s.Outcome, NegRisk: s.NegRisk, NegRiskGroupID: s.NegRiskGroupID,
		Side: s.Side, Action: s.Action, TimeInForce: s.TimeInForce,
		EdgeBps: s.EdgeBps, OpportunityBps: s.OpportunityBps, ModelEdgeBps: s.ModelEdgeBps,
		CostBps: s.CostBps, CostBreakdown: s.CostBreakdown,
		FairValue: s.FairValue, FVSource: s.FVSource, ScorePath: s.ScorePath,
		Conviction: s.Conviction, HorizonSec: s.HorizonSec, HalfLifeSec: s.HalfLifeSec, Urgency: s.Urgency,
		SizeUSD: s.SizeUSD, SizeShares: s.SizeShares, CapacityUSD: s.CapacityUSD, KellyFrac: s.KellyFrac,
		RiskFlags: s.RiskFlags,
		Mid: s.Mid, BestBid: s.BestBid, BestAsk: s.BestAsk, SpreadBps: s.SpreadBps,
		Imbalance: s.Imbalance, BidDepth: s.BidDepth, AskDepth: s.AskDepth,
		LastTradePrice: s.LastTradePrice, BookAgeMs: s.BookAgeMs, FeatureAgeMs: s.FeatureAgeMs,
		Features: s.Features, Factors: s.Factors, Tags: s.Tags, Reason: s.Reason,
	}
}

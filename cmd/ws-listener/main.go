// Command ws-listener streams Polymarket market WS for the edge board only (M6).
// Maintains books in memory, flushes features_latest (and optional snapshots) with
// write-minimized dirty batches, and emits multi-dimensional paper signals.
// market_resolved is queued in memory and batch-written to plymkt_markets every few minutes.
// No live order placement.
package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"sync"
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
	statusInterval := flag.Duration("status-interval", 2*time.Second, "TTY status rewrite interval")
	statusLogEvery := flag.Duration("status-log-every", 30*time.Second, "structured status log when not a TTY")
	noStatusLine := flag.Bool("no-status-line", false, "disable in-place TTY status line")
	resolveFlush := flag.Duration("resolve-flush", 7*time.Minute, "batch market_resolved → plymkt_markets (lean WS path)")
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
	if err := db.EnsureMarketResolutionColumns(ctx, pool); err != nil {
		logger.Warn("ensure market resolution columns", "error", err)
	}

	store := market.NewBookStore()
	stats := market.NewRuntimeStats()
	resolveQ := market.NewResolveQueue()

	// resolvedConds: skip paper signals; exclude from board subscribe when possible
	var resolvedMu sync.RWMutex
	resolvedConds := map[string]struct{}{}
	resolvedAssets := map[string]struct{}{}

	gate := signals.DefaultGateConfig()
	gate.MinEdgeBps = *minEdgeBps
	gate.MaxSpreadBps = *maxSpreadBps
	gate.Cooldown = *signalCooldown
	gate.ProbeSizeUSD = *probeSize
	emitter := signals.NewEmitter(gate)

	var boardMeta *db.BoardSubscribeSet
	var boardMu sync.RWMutex

	client := market.NewClient(*wsURL, logger)
	client.OnReconnect = func() { stats.IncReconnect() }
	client.OnEvent = func(ev market.ParsedEvent) {
		stats.ObserveMsg(ev.Type)
		switch ev.Type {
		case market.EventMarketResolved:
			// Hot path: memory only — unsub + queue for batch DB
			r := market.MarketResolution{
				GammaID:        ev.MarketGammaID,
				ConditionID:    ev.MarketID,
				WinningAssetID: ev.WinningAssetID,
				WinningOutcome: ev.WinningOutcome,
				ResolvedAt:     ev.Timestamp,
				AssetIDs:       append([]string(nil), ev.ResolvedAssetIDs...),
			}
			resolveQ.Enqueue(r)
			stats.AddResolveQueued(1)

			// If WS omitted asset IDs, resolve tokens from board meta.
			if len(r.AssetIDs) == 0 && r.ConditionID != "" {
				boardMu.RLock()
				bm := boardMeta
				boardMu.RUnlock()
				if bm != nil {
					if m, ok := bm.ByCondition[r.ConditionID]; ok {
						r.AssetIDs = append([]string(nil), m.TokenIDs...)
						if m.PrimaryTokenID != "" {
							found := false
							for _, a := range r.AssetIDs {
								if a == m.PrimaryTokenID {
									found = true
									break
								}
							}
							if !found {
								r.AssetIDs = append(r.AssetIDs, m.PrimaryTokenID)
							}
						}
					}
				}
			}

			resolvedMu.Lock()
			if r.ConditionID != "" {
				resolvedConds[r.ConditionID] = struct{}{}
			}
			for _, a := range r.AssetIDs {
				if a != "" {
					resolvedAssets[a] = struct{}{}
				}
			}
			resolvedMu.Unlock()

			if len(r.AssetIDs) > 0 {
				store.RemoveTokens(r.AssetIDs)
				_ = client.RemoveAssets(ctx, r.AssetIDs)
			}
			// Rare high-value line (not per signal spam)
			logger.Info("market_resolved queued",
				"condition_id", r.ConditionID,
				"gamma_id", r.GammaID,
				"winning_outcome", r.WinningOutcome,
				"assets", len(r.AssetIDs),
				"queue", resolveQ.Len(),
			)
			return
		case market.EventNewMarket, market.EventTickSizeChange:
			// Count only — no universe expand, no DB
			return
		default:
			store.Apply(ev)
		}
	}

	// Board poll
	go func() {
		t := time.NewTicker(*boardPoll)
		defer t.Stop()
		var lastN int
		refresh := func() {
			set, err := db.LoadBoardSubscribeSet(ctx, pool, *strategyFlag, *maxAssets)
			if err != nil {
				logger.Warn("load board assets", "error", err)
				return
			}
			// Filter resolved assets out of subscribe set; prune maps for markets
			// no longer on the board (bounded memory for long-running listeners).
			boardTok := make(map[string]struct{}, len(set.TokenIDs))
			for _, tid := range set.TokenIDs {
				boardTok[tid] = struct{}{}
			}
			resolvedMu.Lock()
			for c := range resolvedConds {
				if _, ok := set.ByCondition[c]; !ok {
					delete(resolvedConds, c)
				}
			}
			for a := range resolvedAssets {
				if _, ok := boardTok[a]; !ok {
					if _, ok2 := set.ByToken[a]; !ok2 {
						delete(resolvedAssets, a)
					}
				}
			}
			filtered := make([]string, 0, len(set.TokenIDs))
			for _, tid := range set.TokenIDs {
				if _, bad := resolvedAssets[tid]; bad {
					continue
				}
				filtered = append(filtered, tid)
			}
			var primaries []db.BoardAssetMeta
			for _, p := range set.Primaries {
				if _, bad := resolvedConds[p.ConditionID]; bad {
					continue
				}
				primaries = append(primaries, p)
			}
			resolvedMu.Unlock()

			set.TokenIDs = filtered
			set.Primaries = primaries
			boardMu.Lock()
			boardMeta = set
			boardMu.Unlock()

			if err := client.SetDesired(ctx, set.TokenIDs); err != nil {
				logger.Warn("set desired assets", "error", err)
			}
			// Log only when subscription size changes (not every poll)
			if n := len(set.TokenIDs); n != lastN {
				logger.Info("board subscribe set",
					"strategy", *strategyFlag,
					"tokens", n,
					"primaries", len(set.Primaries),
					"subscribed", len(client.Subscribed()),
				)
				lastN = n
			}
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

	// Features flush
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
				// Peek then mark flushed only after successful upsert (durable SoR).
				dirty := store.PeekDirty(100)
				if len(dirty) == 0 {
					continue
				}
				rows := make([]db.FeaturesLatestRow, 0, len(dirty))
				ids := make([]string, 0, len(dirty))
				now := time.Now().UTC()
				boardMu.RLock()
				bm := boardMeta
				boardMu.RUnlock()
				for _, b := range dirty {
					cond, mkt := "", b.MarketID
					if bm != nil {
						if m, ok := bm.ByToken[b.TokenID]; ok {
							cond = m.ConditionID
							if mkt == "" {
								mkt = m.MarketID
							}
						}
					}
					rows = append(rows, db.FeaturesLatestRow{
						TokenID: b.TokenID, ConditionID: cond, MarketID: mkt,
						BestBid: b.BestBid, BestAsk: b.BestAsk, Mid: b.Mid(),
						LastTradePrice: b.LastTradePrice, Spread: b.Spread(),
						Imbalance: b.Imbalance, BidDepth: b.BidDepth, AskDepth: b.AskDepth,
						UpdatedAt: pickTime(b.UpdatedAt, now),
					})
					ids = append(ids, b.TokenID)
				}
				if err := db.UpsertFeaturesLatest(ctx, pool, rows); err != nil {
					logger.Warn("flush features_latest", "error", err, "n", len(rows))
					continue
				}
				store.MarkFlushed(ids)
				stats.AddFeatFlush(len(rows))
			}
		}
	}()

	// Snapshot flush
	if !*noSnapshots && *snapshotFlush > 0 {
		go func() {
			t := time.NewTicker(*snapshotFlush)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
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
							Time: pickTime(b.UpdatedAt, now), MarketID: mkt, TokenID: b.TokenID,
							BestBid: b.BestBid, BestAsk: b.BestAsk, Imbalance: b.Imbalance,
							TotalBidDepth: b.BidDepth, TotalAskDepth: b.AskDepth,
						})
					}
					if len(rows) == 0 {
						continue
					}
					if err := db.InsertOrderbookSnapshots(ctx, pool, rows); err != nil {
						logger.Warn("flush snapshots", "error", err, "n", len(rows))
						continue
					}
					stats.AddSnapFlush(len(rows))
				}
			}
		}()
	}

	// Batch market_resolved → DB (5–10m; default 7m) — lean hot path
	go func() {
		interval := *resolveFlush
		if interval <= 0 {
			interval = 7 * time.Minute
		}
		t := time.NewTicker(interval)
		defer t.Stop()
		flush := func() {
			pending := resolveQ.TakeAll()
			if len(pending) == 0 {
				return
			}
			rows := make([]db.MarketResolutionRow, 0, len(pending))
			for _, r := range pending {
				rows = append(rows, db.MarketResolutionRow{
					GammaID: r.GammaID, ConditionID: r.ConditionID,
					WinningAssetID: r.WinningAssetID, WinningOutcome: r.WinningOutcome,
					ResolvedAt: r.ResolvedAt,
				})
			}
			n, err := db.ApplyMarketResolutions(ctx, pool, rows)
			if err != nil {
				logger.Warn("batch market_resolved", "error", err, "queued", len(rows))
				// re-queue on failure so we don't lose backtest labels
				for _, r := range pending {
					resolveQ.Enqueue(r)
				}
				return
			}
			stats.AddResolveFlushed(len(rows))
			logger.Info("batch market_resolved applied", "batch", len(rows), "rows_updated", n)
		}
		for {
			select {
			case <-ctx.Done():
				flush() // final drain
				return
			case <-t.C:
				flush()
			}
		}
	}()

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
				boardMu.RLock()
				bm := boardMeta
				boardMu.RUnlock()
				if bm == nil {
					continue
				}
				now := time.Now().UTC()
				var out []db.SignalRow
				for _, p := range bm.Primaries {
					resolvedMu.RLock()
					_, dead := resolvedConds[p.ConditionID]
					resolvedMu.RUnlock()
					if dead {
						continue
					}
					bk, ok := store.Snapshot(p.PrimaryTokenID)
					if !ok || !bk.ValidBook() {
						continue
					}
					board := signals.BoardSnap{
						ConditionID: p.ConditionID, MarketID: p.MarketID, TokenID: p.PrimaryTokenID,
						Rank: p.Rank, Score: p.Score, EdgeBps: p.EdgeBps, CostBps: p.CostBps,
						CapacityUSD: p.CapacityUSD, Urgency: p.Urgency, FairValue: p.FairValue,
						ModelEdgeBps: p.ModelEdgeBps, FVSource: p.FVSource, NegRisk: p.NegRisk,
						NegRiskGroupID: p.NegRiskGroupID, StrategyTags: p.StrategyTags,
						RiskFlags: p.RiskFlags, KeyFeatures: p.KeyFeatures,
						StrategyVersionID: p.StrategyVersionID, RunID: p.RunID,
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
					// Do not Commit — allow re-emit on next interval after transient DB failure.
					continue
				}
				// Debounce only after durable insert.
				for _, row := range out {
					emitter.Commit(&signals.PaperSignal{
						Time: row.Time, SignalID: row.SignalID, ConditionID: row.ConditionID,
						Side: row.Side, EdgeBps: row.EdgeBps,
					})
				}
				stats.AddSignals(len(out))
				// No per-batch Info spam — status line carries aggregates
			}
		}
	}()

	// Status: TTY in-place or periodic structured log
	useTTY := !*noStatusLine && market.IsTTY(os.Stderr)
	go func() {
		interval := *statusInterval
		if !useTTY {
			interval = *statusLogEvery
			if interval <= 0 {
				interval = 30 * time.Second
			}
		} else if interval <= 0 {
			interval = 2 * time.Second
		}
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				if useTTY {
					market.FinishStatusLine()
				}
				return
			case <-t.C:
				in := market.StatusInput{
					Sub:     len(client.Subscribed()),
					Mem:     store.Len(),
					Pending: store.DirtyCount(),
				}
				if useTTY {
					market.WriteStatusLine(stats.Line(in))
				} else {
					logger.Info("ws_status", stats.Attrs(in)...)
				}
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
		"resolve_flush", *resolveFlush,
		"status_tty", useTTY,
	)
	if err := client.Run(runCtx); err != nil && runCtx.Err() == nil {
		logger.Error("ws client exited", "error", err)
		os.Exit(1)
	}
	if useTTY {
		market.FinishStatusLine()
	}
	logger.Info("ws-listener stopped", stats.Attrs(market.StatusInput{
		Sub: len(client.Subscribed()), Mem: store.Len(), Pending: store.DirtyCount(),
	})...)
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

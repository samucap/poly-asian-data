package pipeline

import (
	"context"
	"log/slog"
	"time"
)

// RunWhaleSync runs the whale tracking loop (discovery + live data tickers).
func (p *Pipeline) RunWhaleSync(ctx context.Context) {
	p.logger.Info("Starting Whale Sync Pipeline...")

	discoveryTicker := time.NewTicker(5 * time.Minute)
	liveDataTicker := time.NewTicker(15 * time.Minute)
	defer discoveryTicker.Stop()
	defer liveDataTicker.Stop()

	p.runDiscovery(ctx)
	p.WaitUntilIdle(ctx, 2*time.Second)
	p.logCycleComplete("initial_discovery")

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Whale Sync context done, stopping...")
			return
		case <-discoveryTicker.C:
			p.runDiscovery(ctx)
			p.WaitUntilIdle(ctx, 2*time.Second)
			_ = p.saverPool.SetSyncStatus(ctx, "plymkt_events", "completed")
			p.logCycleComplete("discovery")
		case <-liveDataTicker.C:
			p.runLiveDataSync(ctx)
			p.WaitUntilIdle(ctx, 2*time.Second)
			_ = p.saverPool.SetSyncStatus(ctx, "prices_history", "completed")
			p.logCycleComplete("live_data")
		}
	}
}

func (p *Pipeline) runAccountSync(ctx context.Context) bool {
	p.logger.Info("Running Account Sync Phase...")
	_ = p.saverPool.SetSyncStatus(ctx, "accounts", "running")

	syncCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	startIds := make(map[string]string)
	cursor, err := p.saverPool.GetSyncCursor(syncCtx, "accounts")
	if err != nil {
		p.logger.Warn("failed to load accounts cursor, starting fresh", slog.Any("error", err))
	} else if cursor != "" {
		startIds["accounts"] = cursor
		p.logger.Info("Resuming accounts sync from cursor", slog.String("cursor", cursor))
	}

	reqs, err := p.plyMktSvc.GetSubgraphReqs(syncCtx, []string{"accounts"}, startIds)
	if err != nil {
		p.logger.Error("failed to get account sync reqs", slog.Any("error", err))
		return false
	}

	if err := p.RunBatch(syncCtx, "accounts", reqs); err != nil {
		p.logger.Warn("account sync batch incomplete", slog.Any("error", err))
		return false
	}

	if err := p.saverPool.MarkSyncComplete(syncCtx, "accounts"); err != nil {
		p.logger.Warn("failed to mark accounts sync complete", slog.Any("error", err))
	}
	return true
}

func (p *Pipeline) runDiscovery(ctx context.Context) {
	p.logger.Info("Running Discovery Phase...")
	_ = p.saverPool.SetSyncStatus(ctx, "plymkt_events", "running")
	_ = p.saverPool.SetSyncStatus(ctx, "fpmms", "running")

	reqs, err := p.plyMktSvc.GetDiscoveryReqs(ctx)
	if err != nil {
		p.logger.Error("failed to get discovery reqs", slog.Any("error", err))
		return
	}
	if err := p.RunBatch(ctx, "discovery", reqs); err != nil {
		p.logger.Error("discovery batch failed", slog.Any("error", err))
	}
}

func (p *Pipeline) runFillsSync(ctx context.Context) {
	p.logger.Info("Running Fills Sync Phase (Targeted Active Markets)...")
	_ = p.saverPool.SetSyncStatus(ctx, "enriched_order_filled_events", "running")

	marketIDs, err := p.saverPool.GetActiveMarketIDs(ctx, 100)
	if err != nil {
		p.logger.Error("failed to get active market IDs", slog.Any("error", err))
		return
	}
	if len(marketIDs) == 0 {
		p.logger.Info("No active markets found in DB yet. Skipping fills sync.")
		return
	}

	reqs, err := p.plyMktSvc.GetMarketFillsReqs(ctx, marketIDs)
	if err != nil {
		p.logger.Error("failed to get market fills reqs", slog.Any("error", err))
		return
	}
	if err := p.RunBatch(ctx, "fills", reqs); err != nil {
		p.logger.Error("fills batch failed", slog.Any("error", err))
	}
}

func (p *Pipeline) runWhalePositionsSync(ctx context.Context) {
	p.logger.Info("Running Whale Positions Sync Phase...")
	_ = p.saverPool.SetSyncStatus(ctx, "position_snapshots", "running")

	whaleIDs, err := p.saverPool.GetWhaleIDs(ctx, 100)
	if err != nil {
		p.logger.Error("failed to get top whale IDs", slog.Any("error", err))
		return
	}
	if len(whaleIDs) == 0 {
		p.logger.Info("No whales found in DB yet. Skipping position sync.")
		return
	}

	reqs, err := p.plyMktSvc.GetWhalePositionsReqs(ctx, whaleIDs)
	if err != nil {
		p.logger.Error("failed to get whale position reqs", slog.Any("error", err))
		return
	}
	if err := p.RunBatch(ctx, "whale_positions", reqs); err != nil {
		p.logger.Error("whale positions batch failed", slog.Any("error", err))
	}
}

func (p *Pipeline) runWhaleFillsSync(ctx context.Context) {
	p.logger.Info("Running Whale Fills (History) Sync Phase...")
	_ = p.saverPool.SetSyncStatus(ctx, "enriched_order_filled_events", "running")

	whaleIDs, err := p.saverPool.GetWhaleIDs(ctx, 100)
	if err != nil {
		p.logger.Error("failed to get top whale IDs for fills", slog.Any("error", err))
		return
	}
	if len(whaleIDs) == 0 {
		return
	}

	reqs, err := p.plyMktSvc.GetWhaleFillsReqs(ctx, whaleIDs)
	if err != nil {
		p.logger.Error("failed to get whale fills reqs", slog.Any("error", err))
		return
	}
	if err := p.RunBatch(ctx, "whale_fills", reqs); err != nil {
		p.logger.Error("whale fills batch failed", slog.Any("error", err))
	}
}

func (p *Pipeline) runLiveDataSync(ctx context.Context) {
	p.logger.Info("Running Live Data Sync Phase (Prices History)")
	_ = p.saverPool.SetSyncStatus(ctx, "prices_history", "running")

	tokenIDs, err := p.saverPool.GetActiveTokenIDs(ctx)
	if err != nil {
		p.logger.Error("failed to get active token IDs", slog.Any("error", err))
		return
	}
	if len(tokenIDs) > 200 {
		tokenIDs = tokenIDs[:200]
	}

	p.logger.Info("fetching price history for tokens", slog.Int("count", len(tokenIDs)))

	for _, tokenID := range tokenIDs {
		req, err := p.plyMktSvc.GetPriceHistoryReq(tokenID, 60, 0)
		if err != nil {
			p.logger.Error("failed to build price history req", slog.String("tokenID", tokenID), slog.Any("error", err))
			continue
		}
		if err := p.SubmitFetch(ctx, req); err != nil {
			p.logger.Warn("failed to submit price history req", slog.String("tokenID", tokenID), slog.Any("error", err))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
}

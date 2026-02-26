// Package pipeline orchestrates the multi-stage data processing pipeline.
// It creates and connects the fetcher, processor, and saver stages.
package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/processor"
	"github.com/samucap/poly-asian-data/internal/saver"
	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/samucap/poly-asian-data/internal/workerpool"
)

// =============================================================================
// Error Definitions
// =============================================================================

var (
	ErrPipelineStopped = errors.New("pipeline has been stopped")
	ErrInvalidConfig   = errors.New("invalid pipeline configuration")
)

// =============================================================================
// Stats Types
// =============================================================================

// Stats contains pipeline statistics.
type Stats struct {
	StartedAt      time.Time
	UptimeDuration time.Duration
	Fetcher        workerpool.StatsSnapshot
	Processor      workerpool.StatsSnapshot
	Saver          saver.StatsSnapshot
}

// =============================================================================
// Pipeline
// =============================================================================

// Pipeline orchestrates fetcher -> processor -> saver data flow.
// Each stage is connected via channels, with each stage's workers
// directly sending output to the downstream channel.
type Pipeline struct {
	fetcherPool   *fetcher.Fetcher
	processorPool *processor.Processor
	saverPool     *saver.Saver
	logger        *slog.Logger
	startedAt     time.Time
	cfg           *config.Config

	// Lifecycle
	ctx     context.Context
	cancel  context.CancelFunc
	stopped atomic.Bool

	// Services
	plyMktSvc *services.PlyMktService
}

// New creates a new pipeline with the given configuration.
func New(ctx context.Context, logger *slog.Logger, cfg *config.Config) (*Pipeline, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	pipelineCtx, cancel := context.WithCancel(ctx)
	logger = logger.With(slog.String("component", "pipeline"))
	logger.Info("Initializing Pipeline...")

	// Create all stages
	fetcherPool, err := fetcher.New(pipelineCtx, cfg, cfg.FetcherCfg.NumWorkers, cfg.FetcherCfg.Qsize)
	if err != nil {
		cancel()
		return nil, err
	}

	processorPool, err := processor.New(pipelineCtx, cfg, cfg.ProcessorCfg.NumWorkers, cfg.ProcessorCfg.Qsize)
	if err != nil {
		cancel()
		return nil, err
	}

	saverLogger := logger.With(slog.String("component", "saver"))
	saverPool, err := saver.New(pipelineCtx, saverLogger, cfg, cfg.SaverCfg.NumWorkers, cfg.SaverCfg.Qsize)
	if err != nil {
		cancel()
		return nil, err
	}

	p := &Pipeline{
		fetcherPool:   fetcherPool,
		processorPool: processorPool,
		saverPool:     saverPool,
		logger:        logger,
		startedAt:     time.Now(),
		cfg:           cfg,
		ctx:           pipelineCtx,
		cancel:        cancel,
		plyMktSvc: &services.PlyMktService{
			Cfg:    cfg,
			Logger: logger,
			Ctx:    pipelineCtx,
		},
	}

	// Connect stages:
	// - Processor subscribes to fetcher output
	// - Pipeline routes processor output to saver and handles pagination
	go processorPool.SubscribeToFetcher(pipelineCtx, fetcherPool.Outputs())
	go p.routeProcessorOutput(pipelineCtx)

	// - Drain Saver output to prevent deadlock (workers block on full output queue)
	go func() {
		for range saverPool.Outputs() {
			// discard results, maybe log errors if needed?
			// The saver worker logs failures already.
		}
	}()

	return p, nil
}

func (p *Pipeline) RunSportsTagsSync() {
	p.logger.Info("Starting PolyMarket Sync...")

	reqs, err := p.plyMktSvc.GetSportsReqs(p.ctx)
	if err != nil {
		p.logger.Error("failed to get sports reqs", slog.Any("error", err))
		return
	}

	for _, req := range reqs {
		p.logger.Info("Submitting request", slog.String("url", req.URL))
		if err := p.fetcherPool.SubmitAndThenWait(req); err != nil {
			p.logger.Error("failed to submit req", slog.Any("error", err))
			return
		}
	}

	p.logger.Info("Initial requests submitted. Waiting for pipeline completion...")
	// Revert to 5s for debugging, 60s is too long for feedback loop
	p.WaitUntilIdle(p.ctx, 5*time.Second)
	p.logger.Debug("WaitUntilIdle returned. Printing final report...")
	p.PrintFinalReport()
	p.StopNow()
}

// RunWhaleSync starts the pipeline for the whale tracking sync loop.
func (p *Pipeline) RunWhaleSync(ctx context.Context) {
	p.logger.Info("Starting Whale Sync Pipeline...")

	// Tickers
	discoveryTicker := time.NewTicker(5 * time.Minute)
	liveDataTicker := time.NewTicker(15 * time.Minute)

	defer func() {
		discoveryTicker.Stop()
		liveDataTicker.Stop()
	}()

	// 1. Initial Discovery Run
	p.runDiscovery()
	p.WaitUntilIdle(ctx, 2*time.Second)
	p.logCycleComplete("initial_discovery")

	// Main Loop
	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Whale Sync context done, stopping...")
			return

		case <-discoveryTicker.C:
			p.runDiscovery()
			p.WaitUntilIdle(ctx, 2*time.Second)
			p.saverPool.SetSyncStatus(ctx, "plymkt_events", "completed")
			p.logCycleComplete("discovery")

		case <-liveDataTicker.C:
			p.runLiveDataSync()
			p.WaitUntilIdle(ctx, 2*time.Second)
			p.saverPool.SetSyncStatus(ctx, "prices_history", "completed")
			p.logCycleComplete("live_data")
		}
	}
}

func (p *Pipeline) runAccountSync() bool {
	p.logger.Info("Running Account Sync Phase...")
	p.saverPool.SetSyncStatus(p.ctx, "accounts", "running")

	// Use a timeout context to prevent one sync from blocking forever
	syncCtx, cancel := context.WithTimeout(p.ctx, 2*time.Minute)
	defer cancel()

	targets := []string{"accounts"}

	// Load cursor from DB for incremental sync
	startIds := make(map[string]string)
	cursor, err := p.saverPool.GetSyncCursor(syncCtx, "accounts")
	if err != nil {
		p.logger.Warn("failed to load accounts cursor, starting fresh", slog.Any("error", err))
	} else if cursor != "" {
		startIds["accounts"] = cursor
		p.logger.Info("Resuming accounts sync from cursor", slog.String("cursor", cursor))
	}

	reqs, err := p.plyMktSvc.GetSubgraphReqs(syncCtx, targets, startIds)
	if err != nil {
		p.logger.Error("failed to get account sync reqs", slog.Any("error", err))
		return false
	}

	p.logger.Info("Dispatched Account Sync requests", slog.Int("count", len(reqs)))

	const maxErrors = 3
	errorCount := 0

	for _, req := range reqs {
		// Check if context timed out
		select {
		case <-syncCtx.Done():
			p.logger.Warn("Account sync timed out, will retry after other syncs",
				slog.Int("errors", errorCount),
			)
			return false
		default:
		}

		if err := p.fetcherPool.SubmitAndThenWait(req); err != nil {
			p.logger.Error("failed to submit account req", slog.String("url", req.URL), slog.Any("error", err))
			errorCount++
			if errorCount >= maxErrors {
				p.logger.Warn("Account sync hit error limit, will retry after other syncs",
					slog.Int("errorCount", errorCount),
					slog.Int("maxErrors", maxErrors),
				)
				return false
			}
		} else {
			// Reset error count on success
			errorCount = 0
		}
	}

	// Mark sync as complete
	if err := p.saverPool.MarkSyncComplete(syncCtx, "accounts"); err != nil {
		p.logger.Warn("failed to mark accounts sync complete", slog.Any("error", err))
	} else {
		// Explicitly set status to completed if MarkSyncComplete doesn't do it (it does, but just ensuring)
		// MarkSyncComplete sets "completed".
	}

	return true
}

func (p *Pipeline) logCycleComplete(phase string) {
	stats := p.Stats()
	p.logger.Info("cycle complete",
		slog.String("phase", phase),
		slog.Int64("fetched", stats.Fetcher.Completed),
		slog.Int64("processed", stats.Processor.Completed),
		slog.Int64("saved", stats.Saver.RecordsSaved),
	)
}

func (p *Pipeline) runDiscovery() {
	p.logger.Info("Running Discovery Phase...")
	p.saverPool.SetSyncStatus(p.ctx, "plymkt_events", "running")
	p.saverPool.SetSyncStatus(p.ctx, "fpmms", "running") // Also fpmms just in case

	// 1. Fetch Active Events (Discovery)
	reqs, err := p.plyMktSvc.GetDiscoveryReqs(p.ctx)
	if err != nil {
		p.logger.Error("failed to get discovery reqs", slog.Any("error", err))
		return
	}

	p.logger.Info("Dispatched Discovery requests (Assignments)", slog.Int("count", len(reqs)))
	for _, req := range reqs {
		if err := p.fetcherPool.SubmitAndThenWait(req); err != nil {
			p.logger.Error("failed to submit discovery req", slog.String("url", req.URL), slog.Any("error", err))
		}
	}
}

func (p *Pipeline) runFillsSync() {
	p.logger.Info("Running Fills Sync Phase (Targeted Active Markets)...")
	p.saverPool.SetSyncStatus(p.ctx, "enriched_order_filled_events", "running")

	// 1. Get Active Markets from DB
	marketIDs, err := p.saverPool.GetActiveMarketIDs(p.ctx, 100)
	if err != nil {
		p.logger.Error("failed to get active market IDs", slog.Any("error", err))
		return
	}

	if len(marketIDs) == 0 {
		p.logger.Info("No active markets found in DB yet. Skipping fills sync.")
		return
	}
	p.logger.Info("Fetched Active Market IDs", slog.Int("count", len(marketIDs)))

	// 2. Build Targeted Requests
	reqs, err := p.plyMktSvc.GetMarketFillsReqs(p.ctx, marketIDs)
	if err != nil {
		p.logger.Error("failed to get market fills reqs", slog.Any("error", err))
		return
	}

	p.logger.Info("Dispatched Market Fills requests", slog.Int("count", len(reqs)))
	for _, req := range reqs {
		if err := p.fetcherPool.SubmitAndThenWait(req); err != nil {
			p.logger.Error("failed to submit fills req", slog.String("url", req.URL), slog.Any("error", err))
		}
	}
}

func (p *Pipeline) runWhalePositionsSync() {
	p.logger.Info("Running Whale Positions Sync Phase...")
	p.saverPool.SetSyncStatus(p.ctx, "position_snapshots", "running")

	// 1. Get Top Whales from DB
	// User requested "top 100 accounts"
	whaleIDs, err := p.saverPool.GetWhaleIDs(p.ctx, 100)
	if err != nil {
		p.logger.Error("failed to get top whale IDs", slog.Any("error", err))
		return
	}

	if len(whaleIDs) == 0 {
		p.logger.Info("No whales found in DB yet. Skipping position sync.")
		return
	}
	p.logger.Info("Fetched Top Whale IDs", slog.Int("count", len(whaleIDs)))

	// 2. Build Targeted Requests
	reqs, err := p.plyMktSvc.GetWhalePositionsReqs(p.ctx, whaleIDs)
	if err != nil {
		p.logger.Error("failed to get whale position reqs", slog.Any("error", err))
		return
	}

	p.logger.Info("Dispatched Whale Positions requests", slog.Int("count", len(reqs)))
	for _, req := range reqs {
		if err := p.fetcherPool.SubmitAndThenWait(req); err != nil {
			p.logger.Error("failed to submit position req", slog.String("url", req.URL), slog.Any("error", err))
		}
	}
}

func (p *Pipeline) runWhaleFillsSync() {
	p.logger.Info("Running Whale Fills (History) Sync Phase...")
	p.saverPool.SetSyncStatus(p.ctx, "enriched_order_filled_events", "running")

	// 1. Get Top Whales from DB
	whaleIDs, err := p.saverPool.GetWhaleIDs(p.ctx, 100)
	if err != nil {
		p.logger.Error("failed to get top whale IDs for fills", slog.Any("error", err))
		return
	}

	if len(whaleIDs) == 0 {
		return
	}

	// 2. Build Targeted Fills Requests (Maker & Taker)
	reqs, err := p.plyMktSvc.GetWhaleFillsReqs(p.ctx, whaleIDs)
	if err != nil {
		p.logger.Error("failed to get whale fills reqs", slog.Any("error", err))
		return
	}

	p.logger.Info("Dispatched Whale Fills requests", slog.Int("count", len(reqs)))
	for _, req := range reqs {
		if err := p.fetcherPool.SubmitAndThenWait(req); err != nil {
			p.logger.Error("failed to submit whale fill req", slog.String("url", req.URL), slog.Any("error", err))
		}
	}
}

func (p *Pipeline) runLiveDataSync() {
	p.logger.Info("Running Live Data Sync Phase (Prices History)")
	p.saverPool.SetSyncStatus(p.ctx, "prices_history", "running")

	// 1. Get Active Tokens
	tokenIDs, err := p.saverPool.GetActiveTokenIDs(p.ctx)
	if err != nil {
		p.logger.Error("failed to get active active token IDs", slog.Any("error", err))
		return
	}
	// Limit just in case
	if len(tokenIDs) > 200 {
		tokenIDs = tokenIDs[:200]
	}

	p.logger.Info("fetching price history for tokens", slog.Int("count", len(tokenIDs)))

	// 2. Fetch History for each
	for _, tokenID := range tokenIDs {
		// Fidelity 1 (1 min), StartTs 0 (full history or rely on API default/limit)
		// Optimization: Check last fetch time? For now, nice to have, but simple loop first.
		req, err := p.plyMktSvc.GetPriceHistoryReq(tokenID, 60, 0)
		if err != nil {
			p.logger.Error("failed to build price history req", slog.String("tokenID", tokenID), slog.Any("error", err))
			continue
		}

		if err := p.fetcherPool.SubmitAndThenWait(req); err != nil {
			// Don't log error if queue full, generic submit logs it?
			// SubmitAndThenWait might return error.
			// Just log warning to avoid spamming error level if it's transient
			p.logger.Warn("failed to submit price history req", slog.String("tokenID", tokenID), slog.Any("error", err))
		}

		// Rate limit spacing
		time.Sleep(200 * time.Millisecond)
	}
}

// RunSubgraphSync starts the pipeline for subgraph data sync.
// Supports incremental syncing by loading cursors from DB.
func (p *Pipeline) RunSubgraphSync(ctx context.Context) {
	p.RunSubgraphSyncWithOpts(ctx, false)
}

// RunSubgraphSyncWithOpts starts the pipeline for subgraph data sync with options.
// If fullSync is true, ignores saved cursors and starts from beginning.
func (p *Pipeline) RunSubgraphSyncWithOpts(ctx context.Context, fullSync bool) {
	p.logger.Info("Starting Subgraph Sync...", slog.Bool("fullSync", fullSync))

	targets := []string{"fpmms"}

	// Load cursors from DB for incremental sync
	var startIds map[string]string
	if !fullSync {
		cursors, err := p.saverPool.GetAllSyncCursors(ctx)
		if err != nil {
			p.logger.Warn("failed to load sync cursors, starting fresh", slog.Any("error", err))
		} else if len(cursors) > 0 {
			startIds = cursors
			p.logger.Info("Loaded sync cursors for incremental sync",
				slog.Int("count", len(cursors)),
				slog.Any("cursors", cursors),
			)
		}
	} else {
		// Full sync - reset cursors
		for _, target := range targets {
			if err := p.saverPool.ResetSyncCursor(ctx, target); err != nil {
				p.logger.Warn("failed to reset cursor", slog.String("target", target), slog.Any("error", err))
			}
		}
	}

	reqs, err := p.plyMktSvc.GetSubgraphReqs(ctx, targets, startIds)
	if err != nil {
		p.logger.Error("failed to get subgraph reqs", slog.Any("error", err))
		return
	}

	p.logger.Info("Submitting subgraph requests...", slog.Int("count", len(reqs)))
	for _, req := range reqs {
		if err := p.fetcherPool.SubmitAndThenWait(req); err != nil {
			p.logger.Error("failed to submit req", slog.Any("error", err))
			return
		}
	}

	p.logger.Info("Initial subgraph requests submitted. Waiting for pipeline completion...")
	p.WaitUntilIdle(p.ctx, 5*time.Second)

	// Mark syncs as complete
	for _, target := range targets {
		if err := p.saverPool.MarkSyncComplete(ctx, target); err != nil {
			p.logger.Warn("failed to mark sync complete", slog.String("target", target), slog.Any("error", err))
		}
	}

	p.PrintFinalReport()
	p.StopNow()
}

// WaitUntilIdle blocks until the pipeline is idle for the specified duration.
func (p *Pipeline) WaitUntilIdle(ctx context.Context, stableDuration time.Duration) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	stableSince := time.Time{}
	isStable := false

	// Strategic Logging:
	// 1. Log when entering IDLE state.
	// 2. Log when leaving IDLE state (Reset).
	// 3. Log periodically (e.g. every 60s) if active, to show progress.
	lastLogTime := time.Time{}
	logInterval := 60 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			//p.logger.Info("DEBUG: Ticker fired. Checking idle status...")
			stats := p.Stats()

			// We must use the WorkerPool stats for Saver pending count,
			// because stats.Saver uses custom stats where Saved tracks rows vs Submitted tracks batches (mismatch).
			// p.saverPool.Stats() returns the underlying workerpool stats (batches).
			saverStats := p.saverPool.Stats().Snapshot()

			// Check if all stages are idle (Submitted == Completed + Failed)
			fPending := stats.Fetcher.Submitted - (stats.Fetcher.Completed + stats.Fetcher.Failed)
			pPending := stats.Processor.Submitted - (stats.Processor.Completed + stats.Processor.Failed)
			sPending := saverStats.Submitted - (saverStats.Completed + saverStats.Failed)

			idle := fPending == 0 && pPending == 0 && sPending == 0

			// Determine if we should log
			shouldLog := false

			if idle && !isStable {
				// Transition to IDLE
				shouldLog = true
			} else if !idle && isStable {
				// Transition to ACTIVE (Reset)
				shouldLog = true
			} else if !idle && time.Since(lastLogTime) >= logInterval {
				// Heartbeat while active
				shouldLog = true
			}

			if shouldLog {
				p.logger.Info("Pipeline Status",
					// Fetcher
					slog.Int64("fetcher_submitted", stats.Fetcher.Submitted),
					slog.Int64("fetcher_completed", stats.Fetcher.Completed),
					slog.Int64("fetcher_failed", stats.Fetcher.Failed),
					slog.Int64("fetcher_pending", fPending),
					// Processor
					slog.Int64("processor_submitted", stats.Processor.Submitted),
					slog.Int64("processor_completed", stats.Processor.Completed),
					slog.Int64("processor_failed", stats.Processor.Failed),
					slog.Int64("processor_pending", pPending),
					// Saver
					slog.Int64("saver_submitted", saverStats.Submitted),
					slog.Int64("saver_completed", saverStats.Completed),
					slog.Int64("saver_failed", saverStats.Failed),
					slog.Int64("saver_pending", sPending),
					// State
					slog.Bool("idle", idle),
					slog.Bool("stable", isStable),
					slog.Duration("stable_duration", time.Since(stableSince)),
				)
				lastLogTime = time.Now()
			}

			if idle {
				if !isStable {
					p.logger.Info("Pipeline state became IDLE. Starting stability timer.")
					stableSince = time.Now()
					isStable = true
				} else {
					if time.Since(stableSince) >= stableDuration {
						return // Stable for required duration
					}
				}
			} else {
				if isStable {
					p.logger.Info("Pipeline stability RESET. Activity detected.")
				}
				isStable = false
			}
		}
	}
}

// PrintFinalReport logs a summary of the pipeline execution.
func (p *Pipeline) PrintFinalReport() {
	stats := p.Stats()
	p.logger.Info("==================================================")
	p.logger.Info("           PIPELINE END REPORT                  ")
	p.logger.Info("==================================================")
	p.logger.Info("Execution Time", slog.Duration("duration", stats.UptimeDuration))
	p.logger.Info("--------------------------------------------------")
	p.logger.Info("Fetcher Stats:")
	p.logger.Info("  Submitted :", slog.Int64("count", stats.Fetcher.Submitted))
	p.logger.Info("  Completed :", slog.Int64("count", stats.Fetcher.Completed))
	p.logger.Info("  Failed    :", slog.Int64("count", stats.Fetcher.Failed))
	p.logger.Info("--------------------------------------------------")
	p.logger.Info("Processor Stats:")
	p.logger.Info("  Submitted :", slog.Int64("count", stats.Processor.Submitted))
	p.logger.Info("  Completed :", slog.Int64("count", stats.Processor.Completed))
	p.logger.Info("  Failed    :", slog.Int64("count", stats.Processor.Failed))
	p.logger.Info("--------------------------------------------------")
	p.logger.Info("Saver Stats:")
	p.logger.Info("  Submitted :", slog.Int64("count", stats.Saver.RecordsSubmitted))
	p.logger.Info("  Saved     :", slog.Int64("count", stats.Saver.RecordsSaved))
	p.logger.Info("  Failed    :", slog.Int64("count", stats.Saver.RecordsFailed))
	p.logger.Info("  Rows      :", slog.Int64("count", stats.Saver.RowsAffected))
	p.logger.Info("==================================================")
}

// routeProcessorOutput reads from processor output and routes data to saver
// and pagination requests back to fetcher.
func (p *Pipeline) routeProcessorOutput(ctx context.Context) {
	for {
		select {
		case result, ok := <-p.processorPool.Outputs():
			if !ok {
				return // Channel closed
			}
			if result.Err != nil {
				continue // Skip failed processing
			}
			output := result.Value
			if output == nil {
				p.logger.Warn("processor returned nil output with no error")
				continue
			}

			if output.SaverPayloads != nil {
				// 1. Route Saver Payloads
				for _, payload := range output.SaverPayloads {
					record := &saver.Record{
						ID:        output.ID, // Use original ID or generate new? Using batch ID is fine.
						TableName: payload.TableName,
						Data:      payload.Data,
						// ItemCount is for the whole batch, but here we split.
						// Ideally we track count per payload but let's use batch count for approximation or 0.
						ItemCount:   output.ItemCount,
						ProcessedAt: output.ProcessedAt,
					}

					// If queue full or other error, handle async to avoid blocking the reader
					// Log warning if it's not just queue full?
					// workerpool.Submit returns ErrQueueFull.
					go func() {
						payloadCopy := record // Escape closure capture issue
						if err := p.saverPool.SubmitAndThenWait(payloadCopy); err != nil {
							p.logger.Warn("failed to submit to saver (async)", slog.String("error", err.Error()))
						}
					}()
				}
			}

			// 1b. Update sync cursor for incremental sync
			if output.SyncType != "" && output.LastCursor != "" {
				go func(syncType, cursor string, itemCount int) {
					if err := p.saverPool.SetSyncCursor(ctx, syncType, cursor, itemCount); err != nil {
						p.logger.Warn("failed to update sync cursor",
							slog.String("syncType", syncType),
							slog.String("cursor", cursor),
							slog.Any("error", err),
						)
					}
				}(output.SyncType, output.LastCursor, output.ItemCount)
			}

			// 2. Route Derived Requests (e.g. Events for Sports)
			for _, req := range output.DerivedRequests {
				if req == nil {
					p.logger.Warn("processor returned nil derived request")
					continue
				}
				reqCopy := req
				// Launch in goroutine to avoid blocking the router loop (deadlock prevention)
				go func(r *fetcher.Request) {
					if err := p.fetcherPool.SubmitAndThenWait(r); err != nil {
						p.logger.Warn("failed to submit derived request (async)",
							slog.String("url", r.URL),
							slog.String("error", err.Error()),
						)
					}
				}(reqCopy)
			}

			// 3. Check if pagination should continue
			var nextReq *fetcher.Request
			if output.NextPageRequest != nil {
				nextReq = output.NextPageRequest
			} else {
				nextReq = p.fetcherPool.BuildNextPageRequest(output.OriginalRequest, output.ItemCount)
			}

			if nextReq != nil {
				// Launch in goroutine
				go func(r *fetcher.Request) {
					if err := p.fetcherPool.SubmitAndThenWait(r); err != nil {
						p.logger.Warn("failed to submit next page request (async)",
							slog.String("url", r.URL),
							slog.String("error", err.Error()),
						)
					}
				}(nextReq)
			}
		case <-ctx.Done():
			return
		}
	}
}

// Stats returns current pipeline statistics.
func (p *Pipeline) Stats() Stats {
	return Stats{
		StartedAt:      p.startedAt,
		UptimeDuration: time.Since(p.startedAt),
		Fetcher:        p.fetcherPool.Stats().Snapshot(),
		Processor:      p.processorPool.Stats().Snapshot(),
		Saver:          p.saverPool.SaverStats().Snapshot(),
	}
}

// IsStopped returns true if the pipeline has been stopped.
func (p *Pipeline) IsStopped() bool {
	return p.stopped.Load()
}

// Stop gracefully shuts down the pipeline.
func (p *Pipeline) Stop() {
	if p.stopped.Swap(true) {
		return // Already stopped
	}

	p.logger.Info("pipeline stopping...")

	// Stop in order: fetcher -> processor -> saver
	p.fetcherPool.Stop()
	p.processorPool.Stop()
	p.saverPool.Stop()
	p.saverPool.Close()
	p.cancel()

	stats := p.Stats()
	p.logger.Info("pipeline stopped",
		slog.Int64("fetched", stats.Fetcher.Completed),
		slog.Int64("processed", stats.Processor.Completed),
		slog.Int64("saved", stats.Saver.RecordsSaved),
		slog.Duration("uptime", stats.UptimeDuration),
	)
}

// StopNow immediately stops the pipeline.
func (p *Pipeline) StopNow() {
	if p.stopped.Swap(true) {
		return
	}

	p.cancel()
	p.fetcherPool.StopNow()
	p.processorPool.StopNow()
	p.saverPool.StopNow()
	p.saverPool.Close()
}

// RunTopSync starts the pipeline for Top Traders/Holders sync.
func (p *Pipeline) RunTopSync(ctx context.Context) {
	p.logger.Info("Starting Top Sync Pipeline (Leaderboard & Holders)...")

	// 1. Fetch Leaderboard
	// We might want multiple windows? e.g. "all", "month", "week"?
	// Let's fetch "all" and "month" for now as key metrics.
	windows := []string{"all", "month", "week"}
	for _, w := range windows {
		params := services.PlyMktLeaderboardParams{
			TimePeriod: w,
			Limit:      100, // Top 100
		}
		reqs, err := p.plyMktSvc.GetLeaderboardReqs(ctx, params)
		if err != nil {
			p.logger.Error("failed to get leaderboard reqs", slog.String("window", w), slog.Any("error", err))
			continue
		}
		p.logger.Info("Dispatched Leaderboard requests", slog.String("window", w), slog.Int("count", len(reqs)))
		for _, req := range reqs {
			if err := p.fetcherPool.SubmitAndThenWait(req); err != nil {
				p.logger.Error("failed to submit leaderboard req", slog.Any("error", err))
			}
		}
	}

	// 2. Fetch Holders for Active Markets
	// Similar strategy to Fills Sync: get top active markets
	marketIDs, err := p.saverPool.GetActiveMarketIDs(ctx, 100)
	if err != nil {
		p.logger.Error("failed to get active market IDs for holders", slog.Any("error", err))
	} else if len(marketIDs) > 0 {
		p.logger.Info("Fetching Holders for Active Markets", slog.Int("count", len(marketIDs)))
		reqs, err := p.plyMktSvc.GetHoldersReqs(ctx, marketIDs)
		if err != nil {
			p.logger.Error("failed to get holders reqs", slog.Any("error", err))
		} else {
			p.logger.Info("Dispatched Holders requests", slog.Int("count", len(reqs)))
			for _, req := range reqs {
				if err := p.fetcherPool.SubmitAndThenWait(req); err != nil {
					p.logger.Error("failed to submit holders req", slog.Any("error", err))
				}
			}
		}
	} else {
		p.logger.Info("No active markets found, skipping holders sync")
	}

	p.logger.Info("Top Sync requests submitted. Waiting for pipeline completion...")
	p.WaitUntilIdle(ctx, 5*time.Second)
	p.PrintFinalReport()
	p.StopNow()
}

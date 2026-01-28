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
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/processor"
	"github.com/samucap/poly-asian-data/internal/saver"
	"github.com/samucap/poly-asian-data/internal/workerpool"
	"github.com/samucap/poly-asian-data/internal/services"
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
}

// New creates a new pipeline with the given configuration.
func New(ctx context.Context, cfg *config.Config) (*Pipeline, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	pipelineCtx, cancel := context.WithCancel(ctx)
	logger := logging.Logger.With(slog.String("component", "pipeline"))
	logger.Info("Initializing Pipeline...")

	// Create all stages
	fetcherPool, err := fetcher.New(pipelineCtx, cfg, cfg.FetcherCfg.NumWorkers, cfg.FetcherCfg.Qsize)
	if err != nil {
		cancel()
		return nil, err
	}

	processorPool, err := processor.New(pipelineCtx, cfg.ProcessorCfg.NumWorkers, cfg.ProcessorCfg.Qsize)
	if err != nil {
		cancel()
		return nil, err
	}

	saverPool, err := saver.New(pipelineCtx, cfg, cfg.SaverCfg.NumWorkers, cfg.SaverCfg.Qsize)
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

// RunSportsTagsSync starts the pipeline and submits initial requests.
func (p *Pipeline) RunSportsTagsSync() {
	p.logger.Info("Starting PolyMarket Sync...")
	
	plyMktSvc := &services.PlyMktService{
		Cfg: p.cfg,
		Logger: p.logger,
		Ctx: p.ctx,
	}

	reqs, err := plyMktSvc.GetSportsReqs(p.ctx)
	if err != nil {
		p.logger.Error("failed to get sports reqs", slog.Any("error", err))
		return
	}

	p.logger.Info("DEBUG: Submitting initial requests...")
	for i, req := range reqs {
		p.logger.Info("DEBUG: Submitting request", slog.Int("index", i), slog.String("url", req.URL))
		if err := p.fetcherPool.SubmitAndThenWait(req); err != nil {
			p.logger.Error("failed to submit req", slog.Any("error", err))
			return
		}
	}

	p.logger.Info("Initial requests submitted. Waiting for pipeline completion...")
	// Revert to 5s for debugging, 60s is too long for feedback loop
	p.WaitUntilIdle(p.ctx, 5*time.Second)
	p.logger.Info("DEBUG: WaitUntilIdle returned. Printing final report...")
	p.PrintFinalReport()
	p.StopNow()
}

// WaitUntilIdle blocks until the pipeline is idle for the specified duration.
func (p *Pipeline) WaitUntilIdle(ctx context.Context, stableDuration time.Duration) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	stableSince := time.Time{}
	isStable := false

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			//p.logger.Info("DEBUG: Ticker fired. Checking idle status...")
			stats := p.Stats()
			
			// Check if all stages are idle (Submitted == Completed + Failed)
			// Note: We check Pending (Implied)
			// Fetcher
			fPending := stats.Fetcher.Submitted - (stats.Fetcher.Completed + stats.Fetcher.Failed)
			// Processor
			pPending := stats.Processor.Submitted - (stats.Processor.Completed + stats.Processor.Failed)
			// Saver
			sPending := stats.Saver.RecordsSubmitted - (stats.Saver.RecordsSaved + stats.Saver.RecordsFailed)

			idle := fPending == 0 && pPending == 0 && sPending == 0

			if idle {
				if !isStable {
					stableSince = time.Now()
					isStable = true
				} else {
					if time.Since(stableSince) >= stableDuration {
						p.cancel()
						p.StopNow()
						return
					}
				}
			} else {
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

			// 1. Route Saver Payloads
			for _, payload := range output.SaverPayloads {
				record := &saver.Record{
					ID:          output.ID, // Use original ID or generate new? Using batch ID is fine.
					TableName:   payload.TableName,
					Data:        payload.Data,
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

			// 2. Route Derived Requests (e.g. Events for Sports)
			for _, req := range output.DerivedRequests {
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

			// 3. Check if pagination should continue (fetcher handles the logic)
			// Only for the Original Request
			if nextReq := p.fetcherPool.BuildNextPageRequest(output.OriginalRequest, output.ItemCount); nextReq != nil {
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

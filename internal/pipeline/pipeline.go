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
	saverPool     *saver.Pool
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

	// TODO: Re-enable saver when implemented
	//saverPool, err := saver.New(pipelineCtx, cfg.SaverCfg)
	//if err != nil {
	//	cancel()
	//	return nil, err
	//}

	// Connect processor to fetcher output (type-safe transformation)
	go processorPool.SubscribeToFetcher(pipelineCtx, fetcherPool.Outputs())

	return &Pipeline{
		fetcherPool:   fetcherPool,
		processorPool: processorPool,
		// saverPool:     saverPool,
		logger:        logger,
		startedAt:     time.Now(),
		cfg:           cfg,
		ctx:           pipelineCtx,
		cancel:        cancel,
	}, nil
}

// SyncPlyMkt starts the pipeline and waits for it to complete.
func (p *Pipeline) SyncPlyMkt() {
	p.logger.Info("Starting PolyMarket Sync...")
	
	// TODO: Instantiate PlyMktService properly
	// For now, just log that we're starting
	plyMktSvc := &services.PlyMktService{
		Cfg: p.cfg,
		Logger: p.logger,
		Ctx: p.ctx,
	}

	reqs, err := plyMktSvc.GetSportsReqs(p.ctx)
	if err != nil {
		p.logger.Error("failed to get sports reqs", err)
		return
	}

	for _, req := range reqs {
		input := p.fetcherPool.MakeInputObj(req)
		if err := p.fetcherPool.SubmitWait(input); err != nil {
			p.logger.Error("failed to submit req", slog.Any("error", err))
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
		// Saver:          p.saverPool.Stats().Snapshot(),
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
	// _ = p.saverPool.Stop()
	p.cancel()

	stats := p.Stats()
	p.logger.Info("pipeline stopped",
		slog.Int64("fetched", stats.Fetcher.Completed),
		slog.Int64("processed", stats.Processor.Completed),
		// slog.Int64("saved", stats.Saver.RecordsSaved),
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
	// p.saverPool.StopNow()
}

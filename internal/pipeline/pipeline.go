// Package pipeline orchestrates the multi-stage data processing pipeline.
// It creates and connects the fetcher, processor, and saver stages.
package pipeline

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/processor"
	"github.com/samucap/poly-asian-data/internal/saver"
)

// =============================================================================
// Error Definitions
// =============================================================================

var (
	ErrPipelineStopped = errors.New("pipeline has been stopped")
	ErrInvalidConfig   = errors.New("invalid pipeline configuration")
)

// =============================================================================
// Configuration
// =============================================================================

// Config holds the configuration for the pipeline.
type Config struct {
	FetcherConfig   fetcher.Config
	ProcessorConfig processor.Config
	SaverConfig     saver.Config
	Logger          *slog.Logger
}

// Validate checks the configuration.
func (c *Config) Validate() error {
	if err := c.FetcherConfig.Validate(); err != nil {
		return err
	}
	if err := c.ProcessorConfig.Validate(); err != nil {
		return err
	}
	if err := c.SaverConfig.Validate(); err != nil {
		return err
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return nil
}

// =============================================================================
// Statistics
// =============================================================================

// Stats contains pipeline-level statistics.
type Stats struct {
	StartedAt      time.Time
	UptimeDuration time.Duration
	TotalItemsIn   int64
	TotalItemsOut  int64
	TotalErrors    int64
	FetcherStats   fetcher.StatsSnapshot
	ProcessorStats processor.StatsSnapshot
	SaverStats     saver.StatsSnapshot
}

// =============================================================================
// Pipeline
// =============================================================================

// Pipeline orchestrates fetcher -> processor -> saver data flow.
type Pipeline struct {
	fetcherPool   *fetcher.Pool
	processorPool *processor.Pool
	saverPool     *saver.Pool
	logger        *slog.Logger

	// Stats
	totalItemsIn  atomic.Int64
	totalItemsOut atomic.Int64
	totalErrors   atomic.Int64
	startedAt     time.Time

	// Channels for converting between stages
	processorInput chan *processor.Input
	saverInput     chan *saver.Record

	// Lifecycle
	ctx     context.Context
	cancel  context.CancelFunc
	stopped atomic.Bool
}

// New creates a new pipeline with the given configuration.
func New(ctx context.Context, config Config) (*Pipeline, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	pipelineCtx, cancel := context.WithCancel(ctx)
	logger := config.Logger.With(slog.String("component", "pipeline"))

	// Create fetcher pool
	fetcherPool, err := fetcher.NewPool(pipelineCtx, config.FetcherConfig)
	if err != nil {
		cancel()
		return nil, err
	}

	// Create processor pool
	processorPool, err := processor.NewPool(pipelineCtx, config.ProcessorConfig)
	if err != nil {
		fetcherPool.Stop()
		cancel()
		return nil, err
	}

	// Create saver pool
	saverPool, err := saver.NewPool(pipelineCtx, config.SaverConfig)
	if err != nil {
		processorPool.Stop()
		fetcherPool.Stop()
		cancel()
		return nil, err
	}

	p := &Pipeline{
		fetcherPool:    fetcherPool,
		processorPool:  processorPool,
		saverPool:      saverPool,
		logger:         logger,
		startedAt:      time.Now(),
		processorInput: make(chan *processor.Input, config.ProcessorConfig.QueueSize),
		saverInput:     make(chan *saver.Record, config.SaverConfig.QueueSize),
		ctx:            pipelineCtx,
		cancel:         cancel,
	}

	// Start the pipeline: connect stages via channels
	p.start()

	logger.Info("pipeline started",
		slog.Int("fetcher_workers", fetcherPool.NumWorkers()),
		slog.Int("processor_workers", processorPool.NumWorkers()),
		slog.Int("saver_workers", saverPool.NumWorkers()),
	)

	return p, nil
}

// start connects the pipeline stages.
func (p *Pipeline) start() {
	// Processor listens to processorInput channel
	p.processorPool.StartListening(p.processorInput)

	// Saver listens to saverInput channel
	p.saverPool.StartListening(p.saverInput)

	// Goroutine: Convert fetcher responses -> processor inputs
	go p.fetcherToProcessor()

	// Goroutine: Convert processor outputs -> saver records
	go p.processorToSaver()

	// Goroutine: Collect saver results for stats
	go p.collectSaverResults()
}

// fetcherToProcessor converts fetcher responses to processor inputs.
func (p *Pipeline) fetcherToProcessor() {
	defer close(p.processorInput)

	for {
		select {
		case <-p.ctx.Done():
			return
		case resp, ok := <-p.fetcherPool.Responses():
			if !ok {
				return
			}

			if resp.Err != nil {
				p.totalErrors.Add(1)
				continue
			}

			p.processorInput <- &processor.Input{
				ID:        resp.RequestID,
				SourceURL: resp.URL,
				Data:      resp.Body,
				Metadata:  resp.Metadata,
				FetchedAt: time.Now(),
			}
		}
	}
}

// processorToSaver converts processor outputs to saver records.
func (p *Pipeline) processorToSaver() {
	defer close(p.saverInput)

	for {
		select {
		case <-p.ctx.Done():
			return
		case output, ok := <-p.processorPool.Outputs():
			if !ok {
				return
			}

			if output.Err != nil {
				p.totalErrors.Add(1)
				continue
			}

			p.saverInput <- &saver.Record{
				ID:          output.InputID,
				SourceURL:   output.SourceURL,
				Data:        output.Data,
				Metadata:    output.Metadata,
				FetchedAt:   output.FetchedAt,
				ProcessedAt: output.ProcessedAt,
			}
		}
	}
}

// collectSaverResults tracks save results for stats.
func (p *Pipeline) collectSaverResults() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case result, ok := <-p.saverPool.Results():
			if !ok {
				return
			}

			if result.Err != nil {
				p.totalErrors.Add(1)
			} else {
				p.totalItemsOut.Add(1)
			}
		}
	}
}

// SubmitURL submits a URL to be fetched.
func (p *Pipeline) SubmitURL(id, url string, metadata map[string]any) error {
	if p.stopped.Load() {
		return ErrPipelineStopped
	}

	p.totalItemsIn.Add(1)

	return p.fetcherPool.Submit(&fetcher.Request{
		ID:       id,
		URL:      url,
		Metadata: metadata,
	})
}

// Stats returns current pipeline statistics.
func (p *Pipeline) Stats() Stats {
	return Stats{
		StartedAt:      p.startedAt,
		UptimeDuration: time.Since(p.startedAt),
		TotalItemsIn:   p.totalItemsIn.Load(),
		TotalItemsOut:  p.totalItemsOut.Load(),
		TotalErrors:    p.totalErrors.Load(),
		FetcherStats:   p.fetcherPool.Stats().Snapshot(),
		ProcessorStats: p.processorPool.Stats().Snapshot(),
		SaverStats:     p.saverPool.Stats().Snapshot(),
	}
}

// WorkerCounts returns the number of workers per stage.
type WorkerCounts struct {
	Fetcher   int
	Processor int
	Saver     int
	Total     int
}

// WorkerCounts returns worker counts.
func (p *Pipeline) WorkerCounts() WorkerCounts {
	return WorkerCounts{
		Fetcher:   p.fetcherPool.NumWorkers(),
		Processor: p.processorPool.NumWorkers(),
		Saver:     p.saverPool.NumWorkers(),
		Total:     p.fetcherPool.NumWorkers() + p.processorPool.NumWorkers() + p.saverPool.NumWorkers(),
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
	p.cancel()

	stats := p.Stats()
	p.logger.Info("pipeline stopped",
		slog.Int64("total_in", stats.TotalItemsIn),
		slog.Int64("total_out", stats.TotalItemsOut),
		slog.Int64("total_errors", stats.TotalErrors),
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
}

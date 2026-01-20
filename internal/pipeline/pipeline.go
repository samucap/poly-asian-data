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
// Configuration
// =============================================================================

// Config holds the configuration for the pipeline.
type Config struct {
	NumWorkers int
	QueueSize  int
	SaverCfg   saver.Config
	Logger     *slog.Logger
}

// Validate checks the configuration.
func (c *Config) Validate() error {
	if c.NumWorkers < 1 {
		return errors.New("NumWorkers must be >= 1")
	}
	if c.QueueSize < 1 {
		return errors.New("QueueSize must be >= 1")
	}
	if err := c.SaverCfg.Validate(); err != nil {
		return err
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return nil
}

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

// WorkerCounts contains worker counts for each stage.
type WorkerCounts struct {
	Fetcher   int
	Processor int
	Saver     int
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
	numWorkers    int

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
	logger.Info("Initializing Pipeline...")

	// Create all stages
	fetcherPool, err := fetcher.New(pipelineCtx, config.NumWorkers, config.QueueSize)
	if err != nil {
		cancel()
		return nil, err
	}

	processorPool, err := processor.New(pipelineCtx, config.NumWorkers, config.QueueSize)
	if err != nil {
		cancel()
		return nil, err
	}

	saverPool, err := saver.New(pipelineCtx, config.SaverCfg)
	if err != nil {
		cancel()
		return nil, err
	}

	// Connect processor to fetcher output (type-safe transformation)
	go processorPool.SubscribeToFetcher(pipelineCtx, fetcherPool.Outputs())

	p := &Pipeline{
		fetcherPool:   fetcherPool,
		processorPool: processorPool,
		saverPool:     saverPool,
		logger:        logger,
		startedAt:     time.Now(),
		numWorkers:    config.NumWorkers,
		ctx:           pipelineCtx,
		cancel:        cancel,
	}

	urls := []string{
		"google.com",
		"espn.com",
		"x.com",
	}

	go func(){
		for _, url := range urls {
			input := workerpool.Input[*fetcher.Request]{
				Data: &fetcher.Request{
					URL: url,
					Method: "GET",
					Headers: map[string]string{
						"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
						"Content-Type": "application/json",
					},
				},
				SubmittedAt: time.Now(),
			}
			p.fetcherPool.SubmitWait(input)
		}
	}()

	return p, nil
}

// Stats returns current pipeline statistics.
func (p *Pipeline) Stats() Stats {
	return Stats{
		StartedAt:      p.startedAt,
		UptimeDuration: time.Since(p.startedAt),
		Fetcher:        p.fetcherPool.Stats().Snapshot(),
		Processor:      p.processorPool.Stats().Snapshot(),
		Saver:          p.saverPool.Stats().Snapshot(),
	}
}

// WorkerCounts returns worker counts.
func (p *Pipeline) WorkerCounts() WorkerCounts {
	return WorkerCounts{
		Fetcher:   p.numWorkers,
		Processor: p.numWorkers,
		Saver:     p.saverPool.NumWorkers(),
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
	_ = p.saverPool.Stop()
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
}

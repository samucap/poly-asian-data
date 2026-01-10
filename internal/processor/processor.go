// Package processor provides a worker pool for processing data payloads.
// Uses the generic workerpool.Pool for worker management.
package processor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/samucap/poly-asian-data/internal/workerpool"
)

// =============================================================================
// Error Definitions
// =============================================================================

var (
	ErrPoolStopped   = errors.New("processor pool has been stopped")
	ErrInvalidConfig = errors.New("invalid processor configuration")
)

// =============================================================================
// Type Definitions
// =============================================================================

// Input represents data to be processed.
type Input struct {
	ID        string
	SourceURL string
	Data      []byte
	Metadata  map[string]any
	FetchedAt time.Time
}

// Output represents processed data.
type Output struct {
	InputID     string
	SourceURL   string
	Data        any
	Metadata    map[string]any
	Duration    time.Duration
	Err         error
	FetchedAt   time.Time
	ProcessedAt time.Time
}

// ProcessFunc is the function signature for processing data.
type ProcessFunc func(ctx context.Context, input *Input) (any, error)

// Stats contains atomic counters for processor statistics.
type Stats struct {
	ItemsSubmitted atomic.Int64
	ItemsProcessed atomic.Int64
	ItemsFailed    atomic.Int64
	BytesProcessed atomic.Int64
	TotalDuration  atomic.Int64
}

// Snapshot returns a point-in-time copy.
func (s *Stats) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		ItemsSubmitted: s.ItemsSubmitted.Load(),
		ItemsProcessed: s.ItemsProcessed.Load(),
		ItemsFailed:    s.ItemsFailed.Load(),
		BytesProcessed: s.BytesProcessed.Load(),
		TotalDuration:  time.Duration(s.TotalDuration.Load()),
	}
}

// StatsSnapshot is an immutable snapshot.
type StatsSnapshot struct {
	ItemsSubmitted int64
	ItemsProcessed int64
	ItemsFailed    int64
	BytesProcessed int64
	TotalDuration  time.Duration
}

// =============================================================================
// Configuration
// =============================================================================

// Config holds the configuration for a processor pool.
type Config struct {
	NumWorkers     int
	QueueSize      int
	ProcessTimeout time.Duration
	ProcessFunc    ProcessFunc
	Logger         *slog.Logger
}

// Validate checks the configuration.
func (c *Config) Validate() error {
	if c.NumWorkers < 1 {
		return fmt.Errorf("%w: NumWorkers must be >= 1", ErrInvalidConfig)
	}
	if c.QueueSize < 1 {
		return fmt.Errorf("%w: QueueSize must be >= 1", ErrInvalidConfig)
	}
	if c.ProcessFunc == nil {
		return fmt.Errorf("%w: ProcessFunc is required", ErrInvalidConfig)
	}
	if c.ProcessTimeout == 0 {
		c.ProcessTimeout = 60 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return nil
}

// =============================================================================
// Pool Implementation
// =============================================================================

// Pool is a processor pool using generic workerpool.Pool.
type Pool struct {
	pool    *workerpool.Pool[*Input, *Output]
	config  Config
	stats   Stats
	logger  *slog.Logger
	ctx     context.Context
	cancel  context.CancelFunc
	stopped atomic.Bool

	// Cached outputs channel
	outputs chan *Output

	// Source listener
	sourceWg sync.WaitGroup
}

// NewPool creates a new processor pool.
func NewPool(ctx context.Context, config Config) (*Pool, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	poolCtx, cancel := context.WithCancel(ctx)

	p := &Pool{
		config:  config,
		logger:  config.Logger.With(slog.String("component", "processor")),
		ctx:     poolCtx,
		cancel:  cancel,
		outputs: make(chan *Output, config.QueueSize),
	}

	pool, err := workerpool.NewPool(poolCtx, workerpool.Config{
		Name:       "processor",
		NumWorkers: config.NumWorkers,
		QueueSize:  config.QueueSize,
		Logger:     config.Logger,
	}, p.process)

	if err != nil {
		cancel()
		return nil, err
	}

	p.pool = pool

	// Start result forwarder
	go p.forwardResults()

	return p, nil
}

// StartListening starts goroutines to listen on a source channel and process items.
// This connects the processor to an upstream stage (e.g., fetcher responses).
// The converter function transforms source items into processor inputs.
func (p *Pool) StartListening(source <-chan *Input) {
	p.sourceWg.Add(1)
	go func() {
		defer p.sourceWg.Done()
		for {
			select {
			case <-p.ctx.Done():
				return
			case input, ok := <-source:
				if !ok {
					return
				}
				p.stats.ItemsSubmitted.Add(1)
				if err := p.pool.SubmitWait(input); err != nil {
					p.logger.Error("failed to submit input",
						slog.String("id", input.ID),
						slog.String("error", err.Error()),
					)
				}
			}
		}
	}()
}

// forwardResults forwards pool outputs to the cached outputs channel.
func (p *Pool) forwardResults() {
	defer close(p.outputs)
	for result := range p.pool.Outputs() {
		// Always forward - even on error, result.Value contains an Output with Err set
		if result.Value != nil {
			p.outputs <- result.Value
		}
	}
}

// process is the worker function.
func (p *Pool) process(ctx context.Context, input *Input) (*Output, error) {
	start := time.Now()

	p.logger.Info("processing data",
		slog.String("input_id", input.ID),
		slog.String("url", input.SourceURL),
	)

	processCtx, cancel := context.WithTimeout(ctx, p.config.ProcessTimeout)
	defer cancel()

	result, err := p.config.ProcessFunc(processCtx, input)

	duration := time.Since(start)
	p.stats.TotalDuration.Add(int64(duration))
	p.stats.BytesProcessed.Add(int64(len(input.Data)))

	if err != nil {
		p.stats.ItemsFailed.Add(1)
		p.logger.Error("processing failed",
			slog.String("input_id", input.ID),
			slog.String("error", err.Error()),
		)
		return &Output{
			InputID:     input.ID,
			SourceURL:   input.SourceURL,
			Metadata:    input.Metadata,
			Duration:    duration,
			Err:         err,
			FetchedAt:   input.FetchedAt,
			ProcessedAt: time.Now(),
		}, err
	}

	p.stats.ItemsProcessed.Add(1)
	p.logger.Info("processed data",
		slog.String("input_id", input.ID),
		slog.Duration("duration", duration),
	)

	return &Output{
		InputID:     input.ID,
		SourceURL:   input.SourceURL,
		Data:        result,
		Metadata:    input.Metadata,
		Duration:    duration,
		FetchedAt:   input.FetchedAt,
		ProcessedAt: time.Now(),
	}, nil
}

// Submit adds an input to the pool directly.
func (p *Pool) Submit(input *Input) error {
	if input == nil {
		return errors.New("input cannot be nil")
	}
	if p.stopped.Load() {
		return ErrPoolStopped
	}
	p.stats.ItemsSubmitted.Add(1)
	return p.pool.SubmitWait(input)
}

// Outputs returns the cached output channel.
func (p *Pool) Outputs() <-chan *Output {
	return p.outputs
}

// Stats returns statistics.
func (p *Pool) Stats() *Stats {
	return &p.stats
}

// NumWorkers returns worker count.
func (p *Pool) NumWorkers() int {
	return p.config.NumWorkers
}

// Stop gracefully shuts down.
func (p *Pool) Stop() {
	p.stopped.Store(true)
	p.sourceWg.Wait() // Wait for source listeners to finish
	p.pool.Stop()
	p.cancel()

	stats := p.stats.Snapshot()
	p.logger.Info("processor pool stopped",
		slog.Int64("items_processed", stats.ItemsProcessed),
		slog.Int64("items_failed", stats.ItemsFailed),
	)
}

// StopNow immediately stops.
func (p *Pool) StopNow() {
	p.stopped.Store(true)
	p.pool.StopNow()
	p.cancel()
}

// =============================================================================
// Placeholder Processors
// =============================================================================

// PassthroughProcessor passes data through unchanged.
func PassthroughProcessor(ctx context.Context, input *Input) (any, error) {
	return input.Data, nil
}

// JSONProcessor is a placeholder for JSON parsing.
func JSONProcessor(ctx context.Context, input *Input) (any, error) {
	return input.Data, nil
}

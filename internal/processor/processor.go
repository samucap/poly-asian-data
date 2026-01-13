// Package processor provides a worker pool for processing data payloads.
// Uses the generic workerpool.Pool for worker management.
package processor

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/samucap/poly-asian-data/internal/logging"
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
type Processor struct {
	*workerpool.Pool[*Input, *Output]
	stats Stats
}

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

// New creates and initializes a processor pool.
// Validates config and sets up resources (logger, input/output channels).
func New(ctx context.Context, numWorkers, qSize int) (*Processor, error) {
	logger := logging.Logger.With(
		slog.String("component", "processor"),
	)

	pool, err := workerpool.NewPool[*Input, *Output](ctx, "processor", numWorkers, qSize)
	if err != nil {
		return nil, err
	}

	p := &Processor{
		Pool: pool,
	}

	logger.Info("processor initialized",
		slog.Int("workers", numWorkers),
		slog.Int("queue_size", qSize),
	)

	return p, nil
}

// =============================================================================
// ProcessorJob Interface - Work Function
// =============================================================================

// Process is the work function that transforms input data.
// This is the domain-specific operation for the processor stage.
// func (p *Pool) Process(ctx context.Context, input *Input) (*Output, error) {
// 	start := time.Now()

// 	p.logger.Info("processing data",
// 		slog.String("input_id", input.ID),
// 		slog.String("url", input.SourceURL),
// 	)

// 	processCtx, cancel := context.WithTimeout(ctx, p.config.ProcessTimeout)
// 	defer cancel()

// 	result, err := p.config.ProcessFunc(processCtx, input)

// 	duration := time.Since(start)
// 	p.stats.TotalDuration.Add(int64(duration))
// 	p.stats.BytesProcessed.Add(int64(len(input.Data)))

// 	if err != nil {
// 		p.stats.ItemsFailed.Add(1)
// 		p.logger.Error("processing failed",
// 			slog.String("input_id", input.ID),
// 			slog.String("error", err.Error()),
// 		)
// 		return &Output{
// 			InputID:     input.ID,
// 			SourceURL:   input.SourceURL,
// 			Metadata:    input.Metadata,
// 			Duration:    duration,
// 			Err:         err,
// 			FetchedAt:   input.FetchedAt,
// 			ProcessedAt: time.Now(),
// 		}, err
// 	}

// 	p.stats.ItemsProcessed.Add(1)
// 	p.logger.Info("processed data",
// 		slog.String("input_id", input.ID),
// 		slog.Duration("duration", duration),
// 	)

// 	return &Output{
// 		InputID:     input.ID,
// 		SourceURL:   input.SourceURL,
// 		Data:        result,
// 		Metadata:    input.Metadata,
// 		Duration:    duration,
// 		FetchedAt:   input.FetchedAt,
// 		ProcessedAt: time.Now(),
// 	}, nil
// }

// Stats returns statistics.
func (p *Processor) ProcessorStats() *Stats {
	return &p.stats
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

func (p *Processor) WorkerTask(ctx context.Context, input *Input) (*Output, error) {
	time.Sleep(10 * time.Millisecond) // Placeholder delay
	return &Output{
		InputID:     input.ID,
		SourceURL:   input.SourceURL,
		Data:        "processorSuccess",
		ProcessedAt: time.Now(),
	}, nil
}

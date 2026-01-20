// Package processor provides a worker pool for processing data payloads.
// Uses the generic workerpool.Pool for worker management.
package processor

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/samucap/poly-asian-data/internal/fetcher"
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
	*workerpool.Pool[*fetcher.Response, *Output]
	stats Stats
}

// Output represents processed data.
type Output struct {
	ID          string
	WorkerID    int
	Data        any
	Duration    time.Duration
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

	// Create processor first so we can pass its method to the pool
	p := &Processor{}

	pool, err := workerpool.NewPool[*fetcher.Response, *Output](ctx, "processor", numWorkers, qSize, logger, p.workerTask)
	if err != nil {
		return nil, err
	}

	p.Pool = pool

	logger.Info("processor initialized",
		slog.Int("workers", numWorkers),
		slog.Int("queue_size", qSize),
	)

	return p, nil
}

// SubscribeToFetcher connects to the fetcher's output channel and transforms
// fetcher.Response -> processor.Input for processing.
func (p *Processor) SubscribeToFetcher(ctx context.Context, upstream <-chan workerpool.Result[*fetcher.Response]) {
	for {
		select {
		case result, ok := <-upstream:
			if !ok {
				return // Channel closed
			}
			if result.Err != nil {
				continue // Skip failed fetches
			}
			// Wrap the response in Input[T]
			input := workerpool.Input[*fetcher.Response]{
				Data:        result.Value,
				SubmittedAt: time.Now(),
			}
			_ = p.SubmitWait(input)
		case <-ctx.Done():
			return
		}
	}
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
func PassthroughProcessor(ctx context.Context, input *fetcher.Response) (any, error) {
	return input.Data, nil
}

// JSONProcessor is a placeholder for JSON parsing.
func JSONProcessor(ctx context.Context, input *fetcher.Response) (any, error) {
	return input.Data, nil
}

func (p *Processor) workerTask(ctx context.Context, input *fetcher.Response) (*Output, error) {
	time.Sleep(10 * time.Millisecond) // Placeholder delay
	return &Output{
		Data:        "processorSuccess",
		ProcessedAt: time.Now(),
	}, nil
}

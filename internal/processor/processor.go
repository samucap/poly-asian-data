// Package processor provides a worker pool for processing data payloads.
// Uses the generic workerpool.Pool for worker management.
package processor

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/samucap/poly-asian-data/internal/workerpool"
)

// =============================================================================
// Error Definitions
// =============================================================================

var (
	ErrPoolStopped   = errors.New("processor pool has been stopped")
	ErrInvalidConfig = errors.New("invalid processor configuration")
	ErrUnknownType   = errors.New("unknown response type")
)

// =============================================================================
// Type Definitions
// =============================================================================

type Processor struct {
	*workerpool.Pool[*fetcher.Response, *Output]
	stats  Stats
	logger *slog.Logger
}

// Output represents processed data.
type Output struct {
	ID              string
	WorkerID        int
	Data            any
	ItemCount       int
	Duration        time.Duration
	ProcessedAt     time.Time
	OriginalRequest *fetcher.Request // for pagination handling by fetcher
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
func New(ctx context.Context, numWorkers, qSize int) (*Processor, error) {
	logger := logging.Logger.With(
		slog.String("component", "processor"),
	)

	// Create processor first so we can pass its method to the pool
	p := &Processor{
		logger: logger,
	}

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
			// Submit the response directly - SubmitWait wraps it internally
			_ = p.SubmitWait(result.Value)
		case <-ctx.Done():
			return
		}
	}
}

// =============================================================================
// Worker Task - Type Dispatch and Processing
// =============================================================================

func (p *Processor) workerTask(ctx context.Context, resp *fetcher.Response) (*Output, error) {
	start := time.Now()

	if resp == nil || resp.Request == nil {
		return nil, errors.New("nil response or request")
	}

	p.logger.Info("processing response",
		slog.String("url", resp.URL),
		slog.Int("bytes", len(resp.Data)),
	)

	// Type dispatch based on URL path
	var data any
	var itemCount int
	var err error

	switch {
	case strings.Contains(resp.URL, "/sports"):
		data, itemCount, err = p.processSports(resp.Data)
	case strings.Contains(resp.URL, "/teams"):
		data, itemCount, err = p.processTeams(resp.Data)
	default:
		// Fallback: just count items if it's a JSON array
		data, itemCount, err = p.processGenericArray(resp.Data)
	}

	if err != nil {
		p.stats.ItemsFailed.Add(1)
		p.logger.Error("processing failed",
			slog.String("url", resp.URL),
			slog.String("error", err.Error()),
		)
		return nil, err
	}

	p.stats.ItemsProcessed.Add(int64(itemCount))
	p.stats.BytesProcessed.Add(int64(len(resp.Data)))
	p.stats.TotalDuration.Add(int64(time.Since(start)))

	p.logger.Info("processed response",
		slog.String("url", resp.URL),
		slog.Int("itemCount", itemCount),
		slog.Duration("duration", time.Since(start)),
	)

	return &Output{
		Data:            data,
		ItemCount:       itemCount,
		Duration:        time.Since(start),
		ProcessedAt:     time.Now(),
		OriginalRequest: resp.Request,
	}, nil
}

// =============================================================================
// Type-Specific Processors
// =============================================================================

func (p *Processor) processSports(data []byte) (any, int, error) {
	var sports []services.PlyMktSport
	if err := json.Unmarshal(data, &sports); err != nil {
		return nil, 0, err
	}
	return sports, len(sports), nil
}

func (p *Processor) processTeams(data []byte) (any, int, error) {
	var teams []services.PlyMktTeam
	if err := json.Unmarshal(data, &teams); err != nil {
		return nil, 0, err
	}
	return teams, len(teams), nil
}

func (p *Processor) processGenericArray(data []byte) (any, int, error) {
	var items []json.RawMessage
	if err := json.Unmarshal(data, &items); err != nil {
		// Not an array, treat as single item
		return data, 1, nil
	}
	return items, len(items), nil
}


// Stats returns statistics.
func (p *Processor) ProcessorStats() *Stats {
	return &p.stats
}

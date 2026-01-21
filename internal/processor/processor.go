// Package processor provides a worker pool for processing data payloads.
// Uses the generic workerpool.Pool for worker management.
package processor

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/url"
	"strconv"
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
	stats       Stats
	logger      *slog.Logger
	fetcherPool *fetcher.Fetcher // Reference to fetcher for pagination
}

// Output represents processed data.
type Output struct {
	ID          string
	WorkerID    int
	Data        any
	ItemCount   int
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
func New(ctx context.Context, numWorkers, qSize int, fetcherPool *fetcher.Fetcher) (*Processor, error) {
	logger := logging.Logger.With(
		slog.String("component", "processor"),
	)

	// Create processor first so we can pass its method to the pool
	p := &Processor{
		logger:      logger,
		fetcherPool: fetcherPool,
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

	// Handle pagination: if we got a full page, request next page
	if err := p.handlePagination(ctx, resp, itemCount); err != nil {
		p.logger.Warn("failed to request next page",
			slog.String("url", resp.URL),
			slog.String("error", err.Error()),
		)
	}

	return &Output{
		Data:        data,
		ItemCount:   itemCount,
		Duration:    time.Since(start),
		ProcessedAt: time.Now(),
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

// =============================================================================
// Pagination Handler
// =============================================================================

func (p *Processor) handlePagination(ctx context.Context, resp *fetcher.Response, itemCount int) error {
	if resp.Request == nil || resp.Request.Params == nil {
		return nil
	}

	// Check if pagination params exist
	limitStr := resp.Request.Params.Get("limit")
	offsetStr := resp.Request.Params.Get("offset")
	if limitStr == "" || offsetStr == "" {
		return nil // Not a paginated request
	}

	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		return err
	}

	// If we got fewer items than the limit, we've reached the last page
	if itemCount < limit {
		p.logger.Info("reached last page",
			slog.String("url", resp.URL),
			slog.Int("itemCount", itemCount),
			slog.Int("limit", limit),
		)
		return nil
	}

	// Full page - request next page
	offset, err := strconv.Atoi(offsetStr)
	if err != nil {
		return err
	}

	newOffset := strconv.Itoa(offset + limit)

	// Deep copy Params to avoid mutation issues
	newParams := make(url.Values)
	for k, v := range resp.Request.Params {
		newParams[k] = append([]string{}, v...)
	}
	newParams.Set("offset", newOffset)

	// Rebuild URL with new offset
	parsedURL, err := url.Parse(resp.URL)
	if err != nil {
		return err
	}
	parsedURL.RawQuery = newParams.Encode()

	nextReq := &fetcher.Request{
		URL:     parsedURL.String(),
		Method:  resp.Request.Method,
		Headers: resp.Request.Headers,
		Params:  newParams,
	}

	p.logger.Info("requesting next page",
		slog.String("url", nextReq.URL),
		slog.Int("newOffset", offset+limit),
	)

	// Use non-blocking Submit to avoid deadlock
	if err := p.fetcherPool.Submit(p.fetcherPool.MakeInputObj(nextReq)); err != nil {
		if errors.Is(err, workerpool.ErrQueueFull) {
			// Queue is full, try with wait
			return p.fetcherPool.SubmitWait(p.fetcherPool.MakeInputObj(nextReq))
		}
		return err
	}

	return nil
}

// Stats returns statistics.
func (p *Processor) ProcessorStats() *Stats {
	return &p.stats
}

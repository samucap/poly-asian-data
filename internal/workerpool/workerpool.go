// Package workerpool provides a generic, reusable worker pool implementation
// and concurrency utilities for building data processing pipelines.
//
// The Pool[T, R] type is the core abstraction - it manages a pool of workers
// that read from an input channel, process items using a provided function,
// and write results to an output channel.
//
// Key patterns implemented:
// - Fan-Out: Multiple workers reading from a single input channel
// - Fan-In: All workers writing to a single output channel
// - Bounded Parallelism: Configurable number of concurrent workers
//
// Security features:
// - Race condition prevention via atomic operations
// - Resource leak prevention with context cancellation
// - No shared mutable state without proper locking
package workerpool

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
	"fmt"
)

// =============================================================================
// Error Definitions
// =============================================================================

var (
	// ErrPoolStopped indicates the worker pool has been stopped.
	ErrPoolStopped = errors.New("worker pool has been stopped")

	// ErrInvalidConfig indicates invalid pool configuration.
	ErrInvalidConfig = errors.New("invalid worker pool configuration")

	// ErrNilInput indicates a nil input was submitted.
	ErrNilInput = errors.New("input cannot be nil")

	// ErrQueueFull indicates the input queue is at capacity.
	ErrQueueFull = errors.New("input queue is full")
)

// =============================================================================
// Generic Worker Pool
// =============================================================================

// Result represents the outcome of processing an item.
type Result[R any] struct {
	// Value is the processed result (valid only if Err is nil).
	Value R

	// Err contains any error that occurred during processing.
	Err error

	// Duration is the time taken to process.
	Duration time.Duration
}

// Stats contains atomic counters for pool statistics.
// All fields are safe for concurrent access.
type Stats struct {
	// Submitted is the total number of items submitted.
	Submitted atomic.Int64

	// Completed is the total number of successfully completed items.
	Completed atomic.Int64

	// Failed is the total number of failed items.
	Failed atomic.Int64

	// InProgress is the current number of items being processed.
	InProgress atomic.Int64

	// TotalDuration tracks cumulative processing time (in nanoseconds).
	TotalDuration atomic.Int64
}

// Snapshot returns a point-in-time copy of the statistics.
func (s *Stats) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		Submitted:     s.Submitted.Load(),
		Completed:     s.Completed.Load(),
		Failed:        s.Failed.Load(),
		InProgress:    s.InProgress.Load(),
		TotalDuration: time.Duration(s.TotalDuration.Load()),
	}
}

// StatsSnapshot is an immutable snapshot of pool statistics.
type StatsSnapshot struct {
	Submitted     int64
	Completed     int64
	Failed        int64
	InProgress    int64
	TotalDuration time.Duration
}

// AverageDuration returns the average processing duration.
func (s StatsSnapshot) AverageDuration() time.Duration {
	total := s.Completed + s.Failed
	if total == 0 {
		return 0
	}
	return s.TotalDuration / time.Duration(total)
}

// Pool is a generic worker pool for concurrent processing.
// T is the input type, R is the result type.
//
// Workers read from a shared input channel (fan-out) and write results
// to a shared output channel (fan-in).
type Pool[T, R any] struct {
	PoolType    string // "fetcher", "processor", "writer"
	stats       Stats
	logger      *slog.Logger
	numWorkers  int
	queueSize   int // input queue capacity
	InputQ      chan T
	OutputQ     chan Result[R]
	processFunc func(context.Context, T) (R, error)
	// Internal synchronization
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	stopped  atomic.Bool
	stopOnce sync.Once
}

type PoolIF[T, R any] interface {
	Testor()
	Outputs() chan Result[R]
	Inputs() chan T
	Stats() Stats
	Stop()
	StopNow()
	IsRunning() bool
	SubmitWait(ctx context.Context, input T) (R, error)
}

// NewPool creates a new worker pool with the given configuration.
// The processFunc is called for each input item to produce a result.
func NewPool[T, R any](ctx context.Context, pooltype string, numWorkers, qSize int, logger *slog.Logger, processFunc func(context.Context, T) (R, error)) (*Pool[T, R], error) {
	return NewPoolWithOutput[T, R](ctx, pooltype, numWorkers, qSize, logger, processFunc)
}

// NewPoolWithOutput creates a new worker pool that writes results to an external channel.
// If output is nil, an internal output channel is created (accessible via Outputs()).
func NewPoolWithOutput[T, R any](ctx context.Context, pooltype string, numWorkers, qSize int, logger *slog.Logger, processFunc func(context.Context, T) (R, error)) (*Pool[T, R], error) {
	logger.Info("Initializing pool",
		slog.String("type", pooltype),
		slog.Int("workers", numWorkers),
		slog.Int("queue_size", qSize),
	)
	poolCtx, cancel := context.WithCancel(ctx)
	p := &Pool[T, R]{
		PoolType:    pooltype,
		numWorkers:  numWorkers,
		InputQ:      make(chan T, qSize),
		OutputQ:     make(chan Result[R], qSize),
		processFunc: processFunc,
		logger:      logger,
		ctx:         poolCtx,
		cancel:      cancel,
	}

	// Start workers (fan-out pattern)
	for i := 0; i < numWorkers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}

	logger.Info("pool started",
		slog.Int("workers", numWorkers),
		slog.Int("queue_size", qSize),
	)

	return p, nil
}

// worker is a goroutine that processes input items.
// Multiple workers read from the same input channel (fan-out).
// All workers write to the same output channel (fan-in).
func (p *Pool[T, R]) worker(id int) {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return
		case input, ok := <-p.InputQ:
			if !ok {
				return
			}
			p.sendResult(p.processItem(input))
		}
	}
}

func (p *Pool[T, R]) sendResult(result Result[R]) {
	p.logger.Info("sending result",
		slog.String("result", fmt.Sprintf("%v", result.Value)),
		slog.String("type", p.PoolType),
		slog.Int64("in_progress", p.stats.InProgress.Load()),
		slog.Int64("submitted", p.stats.Submitted.Load()),
		slog.Int64("completed", p.stats.Completed.Load()),
		slog.Int64("failed", p.stats.Failed.Load()),
	)
	select {
	case p.OutputQ <- result:
		return
	case <-p.ctx.Done():
	}
}

// processItem runs the process function on an input item.
func (p *Pool[T, R]) processItem(input T) Result[R] {
	p.stats.InProgress.Add(1)
	defer p.stats.InProgress.Add(-1)

	start := time.Now()

	value, err := p.processFunc(p.ctx, input)

	duration := time.Since(start)
	p.stats.TotalDuration.Add(int64(duration))

	if err != nil {
		p.stats.Failed.Add(1)
		// Include value even on error - caller may have set useful data
		return Result[R]{
			Value:    value,
			Err:      err,
			Duration: duration,
		}
	}

	p.stats.Completed.Add(1)
	return Result[R]{
		Value:    value,
		Duration: duration,
	}
}

// Submit adds an input item to the pool's queue for processing.
// Returns an error if the pool is stopped or the queue is full.
func (p *Pool[T, R]) Submit(input T) error {
	if p.stopped.Load() {
		return ErrPoolStopped
	}

	p.stats.Submitted.Add(1)

	select {
	case p.InputQ <- input:
		return nil
	case <-p.ctx.Done():
		p.stats.Submitted.Add(-1)
		return ErrPoolStopped
	default:
		p.stats.Submitted.Add(-1)
		return ErrQueueFull
	}
}

// SubmitWait adds an input item, blocking until space is available.
// Returns an error only if the pool is stopped.
func (p *Pool[T, R]) SubmitWait(input T) error {
	if p.stopped.Load() {
		return ErrPoolStopped
	}

	p.stats.Submitted.Add(1)

	select {
	case p.InputQ <- input:
		return nil
	case <-p.ctx.Done():
		p.stats.Submitted.Add(-1)
		return ErrPoolStopped
	}
}

// Inputs returns the input channel for direct writing.
// Use with caution - prefer Submit/SubmitWait for proper stats tracking.
func (p *Pool[T, R]) Inputs() chan<- T {
	return p.InputQ
}

// Outputs returns the channel where results are published.
// Panics if an external output channel was provided to NewPoolWithOutput.
func (p *Pool[T, R]) Outputs() <-chan Result[R] {
	return p.OutputQ
}

// Stats returns the current statistics.
func (p *Pool[T, R]) Stats() *Stats {
	return &p.stats
}

// IsStopped returns true if the pool has been stopped.
func (p *Pool[T, R]) IsStopped() bool {
	return p.stopped.Load()
}

// Stop gracefully shuts down the pool.
// It stops accepting new work, waits for in-progress work to complete,
// then closes the output channel (if owned by this pool).
func (p *Pool[T, R]) Stop() {
	p.stopOnce.Do(func() {
		p.stopped.Store(true)
		close(p.InputQ)
		p.wg.Wait()
		p.cancel()

		// Only close output channel if we created it
		if p.OutputQ != nil {
			close(p.OutputQ)
		}

		stats := p.stats.Snapshot()
		p.logger.Info("pool stopped",
			slog.Int64("completed", stats.Completed),
			slog.Int64("failed", stats.Failed),
		)
	})
}

// StopNow immediately stops the pool, cancelling in-progress work.
func (p *Pool[T, R]) StopNow() {
	p.stopOnce.Do(func() {
		p.stopped.Store(true)
		p.cancel()
		close(p.InputQ)
		p.wg.Wait()

		// Only close output channel if we created it
		if p.OutputQ != nil {
			close(p.OutputQ)
		}
	})
}

// IsRunning returns true if the pool is currently running.
func (p *Pool[T, R]) IsRunning() bool {
	return p.IsStopped() == false
}

func (p *Pool[T, R]) Testor() {
	p.logger.Info("Testing Testing ==================================")
}

// =============================================================================
// Fan-In / Fan-Out Utilities
// =============================================================================

// FanIn merges multiple input channels into a single output channel.
// The output channel is closed when all input channels are closed.
func FanIn[T any](ctx context.Context, channels ...<-chan T) <-chan T {
	out := make(chan T)
	var wg sync.WaitGroup

	multiplex := func(ch <-chan T) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case val, ok := <-ch:
				if !ok {
					return
				}
				select {
				case out <- val:
				case <-ctx.Done():
					return
				}
			}
		}
	}

	wg.Add(len(channels))
	for _, ch := range channels {
		go multiplex(ch)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// FanOut distributes items from a single input channel to multiple output channels.
// Items are distributed round-robin. All output channels are closed when input is closed.
func FanOut[T any](ctx context.Context, input <-chan T, numOutputs int) []<-chan T {
	if numOutputs <= 0 {
		numOutputs = 1
	}

	outputs := make([]chan T, numOutputs)
	for i := range outputs {
		outputs[i] = make(chan T)
	}

	go func() {
		defer func() {
			for _, ch := range outputs {
				close(ch)
			}
		}()

		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-input:
				if !ok {
					return
				}
				select {
				case outputs[i%numOutputs] <- item:
					i++
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	result := make([]<-chan T, numOutputs)
	for i, ch := range outputs {
		result[i] = ch
	}
	return result
}

// =============================================================================
// Pipeline Utilities
// =============================================================================

// Generator creates a channel that emits items from a slice.
func Generator[T any](ctx context.Context, items ...T) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for _, item := range items {
			select {
			case <-ctx.Done():
				return
			case out <- item:
			}
		}
	}()
	return out
}

// Map applies a transformation function to each item in the input channel.
func Map[T, R any](ctx context.Context, input <-chan T, fn func(T) R) <-chan R {
	out := make(chan R)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-input:
				if !ok {
					return
				}
				select {
				case out <- fn(item):
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// Filter returns a channel containing only items that pass the predicate.
func Filter[T any](ctx context.Context, input <-chan T, predicate func(T) bool) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case item, ok := <-input:
				if !ok {
					return
				}
				if predicate(item) {
					select {
					case out <- item:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out
}

// Batch collects items into slices of the specified size.
func Batch[T any](ctx context.Context, input <-chan T, size int) <-chan []T {
	if size <= 0 {
		size = 1
	}

	out := make(chan []T)
	go func() {
		defer close(out)
		batch := make([]T, 0, size)

		for {
			select {
			case <-ctx.Done():
				if len(batch) > 0 {
					select {
					case out <- batch:
					case <-ctx.Done():
					}
				}
				return
			case item, ok := <-input:
				if !ok {
					if len(batch) > 0 {
						select {
						case out <- batch:
						case <-ctx.Done():
						}
					}
					return
				}
				batch = append(batch, item)
				if len(batch) >= size {
					select {
					case out <- batch:
						batch = make([]T, 0, size)
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out
}

// Collect drains a channel into a slice.
func Collect[T any](ctx context.Context, input <-chan T) []T {
	var result []T
	for {
		select {
		case <-ctx.Done():
			return result
		case item, ok := <-input:
			if !ok {
				return result
			}
			result = append(result, item)
		}
	}
}

// Parallel processes items with bounded concurrency.
func Parallel[T, R any](
	ctx context.Context,
	input <-chan T,
	maxConcurrency int,
	processor func(context.Context, T) (R, error),
) <-chan Result[R] {
	if maxConcurrency <= 0 {
		maxConcurrency = 1
	}

	out := make(chan Result[R])
	semaphore := make(chan struct{}, maxConcurrency)
	var wg sync.WaitGroup

	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				wg.Wait()
				return
			case item, ok := <-input:
				if !ok {
					wg.Wait()
					return
				}

				select {
				case semaphore <- struct{}{}:
				case <-ctx.Done():
					wg.Wait()
					return
				}

				wg.Add(1)
				go func(item T) {
					defer wg.Done()
					defer func() { <-semaphore }()

					start := time.Now()
					value, err := processor(ctx, item)

					select {
					case out <- Result[R]{Value: value, Err: err, Duration: time.Since(start)}:
					case <-ctx.Done():
					}
				}(item)
			}
		}
	}()

	return out
}

// =============================================================================
// Error Aggregation
// =============================================================================

// ErrorGroup collects errors from multiple goroutines safely.
type ErrorGroup struct {
	mu     sync.Mutex
	errors []error
}

// Add appends an error to the group. Nil errors are ignored.
func (eg *ErrorGroup) Add(err error) {
	if err == nil {
		return
	}
	eg.mu.Lock()
	eg.errors = append(eg.errors, err)
	eg.mu.Unlock()
}

// Errors returns all collected errors.
func (eg *ErrorGroup) Errors() []error {
	eg.mu.Lock()
	defer eg.mu.Unlock()
	result := make([]error, len(eg.errors))
	copy(result, eg.errors)
	return result
}

// HasErrors returns true if any errors were collected.
func (eg *ErrorGroup) HasErrors() bool {
	eg.mu.Lock()
	defer eg.mu.Unlock()
	return len(eg.errors) > 0
}

// Combined returns all errors as a single error.
func (eg *ErrorGroup) Combined() error {
	eg.mu.Lock()
	defer eg.mu.Unlock()
	if len(eg.errors) == 0 {
		return nil
	}
	return errors.Join(eg.errors...)
}

// Count returns the number of errors collected.
func (eg *ErrorGroup) Count() int {
	eg.mu.Lock()
	defer eg.mu.Unlock()
	return len(eg.errors)
}

// =============================================================================
// Counter Utility
// =============================================================================

// Counter is a thread-safe counter.
type Counter struct {
	value atomic.Int64
}

func (c *Counter) Add(delta int64) int64 { return c.value.Add(delta) }
func (c *Counter) Inc() int64            { return c.value.Add(1) }
func (c *Counter) Dec() int64            { return c.value.Add(-1) }
func (c *Counter) Load() int64           { return c.value.Load() }
func (c *Counter) Store(val int64)       { c.value.Store(val) }
func (c *Counter) Reset() int64          { return c.value.Swap(0) }

// =============================================================================
// Rate Limiter
// =============================================================================

// RateLimiter implements a token bucket rate limiter.
type RateLimiter struct {
	tokens   chan struct{}
	interval time.Duration
	stopCh   chan struct{}
	wg       sync.WaitGroup
	stopped  atomic.Bool
}

// NewRateLimiter creates a rate limiter allowing ratePerSecond operations per second.
func NewRateLimiter(ratePerSecond int) *RateLimiter {
	if ratePerSecond <= 0 {
		return nil
	}

	rl := &RateLimiter{
		tokens:   make(chan struct{}, ratePerSecond),
		interval: time.Second / time.Duration(ratePerSecond),
		stopCh:   make(chan struct{}),
	}

	for i := 0; i < ratePerSecond; i++ {
		rl.tokens <- struct{}{}
	}

	rl.wg.Add(1)
	go func() {
		defer rl.wg.Done()
		ticker := time.NewTicker(rl.interval)
		defer ticker.Stop()

		for {
			select {
			case <-rl.stopCh:
				return
			case <-ticker.C:
				select {
				case rl.tokens <- struct{}{}:
				default:
				}
			}
		}
	}()

	return rl
}

// Wait blocks until a token is available or context is cancelled.
func (rl *RateLimiter) Wait(ctx context.Context) error {
	if rl == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-rl.tokens:
		return nil
	}
}

// TryAcquire attempts to acquire a token without blocking.
func (rl *RateLimiter) TryAcquire() bool {
	if rl == nil {
		return true
	}
	select {
	case <-rl.tokens:
		return true
	default:
		return false
	}
}

// Stop stops the rate limiter.
func (rl *RateLimiter) Stop() {
	if rl == nil || rl.stopped.Swap(true) {
		return
	}
	close(rl.stopCh)
	rl.wg.Wait()
}
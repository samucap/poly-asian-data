// Package workerpool provides a generic, bounded worker pool for pipeline stages.
//
// Pool[T,R] manages N workers that:
//  1. pull jobs from a bounded input channel (fan-out)
//  2. run processFunc
//  3. push results to a bounded output channel (fan-in)
//
// Backpressure is provided by blocking SubmitWait when the input queue is full.
// Bridge connects an upstream Outputs() channel to a downstream SubmitWait.
package workerpool

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// =============================================================================
// Errors
// =============================================================================

var (
	ErrPoolStopped   = errors.New("worker pool has been stopped")
	ErrInvalidConfig = errors.New("invalid worker pool configuration")
	ErrQueueFull     = errors.New("input queue is full")
)

// =============================================================================
// Types
// =============================================================================

// Input wraps a job payload with tracking metadata.
type Input[T any] struct {
	ID          string
	WorkerID    int
	Data        T
	SubmittedAt time.Time
}

// Result is the outcome of processing one item.
type Result[R any] struct {
	Value    R
	Err      error
	Duration time.Duration
}

// Stats holds atomic counters (safe for concurrent access).
type Stats struct {
	Submitted     atomic.Int64
	Completed     atomic.Int64
	Failed        atomic.Int64
	InProgress    atomic.Int64
	TotalDuration atomic.Int64 // nanoseconds
}

// Snapshot returns a point-in-time copy.
func (s *Stats) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		Submitted:     s.Submitted.Load(),
		Completed:     s.Completed.Load(),
		Failed:        s.Failed.Load(),
		InProgress:    s.InProgress.Load(),
		TotalDuration: time.Duration(s.TotalDuration.Load()),
	}
}

// StatsSnapshot is an immutable stats view.
type StatsSnapshot struct {
	Submitted     int64
	Completed     int64
	Failed        int64
	InProgress    int64
	TotalDuration time.Duration
}

// AverageDuration returns mean processing time for finished items.
func (s StatsSnapshot) AverageDuration() time.Duration {
	total := s.Completed + s.Failed
	if total == 0 {
		return 0
	}
	return s.TotalDuration / time.Duration(total)
}

// Pending returns Submitted - Completed - Failed (items queued or in progress).
func (s StatsSnapshot) Pending() int64 {
	p := s.Submitted - s.Completed - s.Failed
	if p < 0 {
		return 0
	}
	return p
}

// Pool is a generic worker pool: T input, R result.
type Pool[T, R any] struct {
	Name        string
	stats       Stats
	logger      *slog.Logger
	numWorkers  int
	queueSize   int
	InputQ      chan Input[T]
	OutputQ     chan Result[R]
	processFunc func(context.Context, T) (R, error)
	ownsOutput  bool
	// discardOutput is for terminal stages (e.g. saver): processItem still updates
	// stats; results are not pushed to OutputQ (no Drain consumer required).
	discardOutput bool

	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	stopped  atomic.Bool
	stopOnce sync.Once
	idSeq    atomic.Uint64
}

// PoolOption configures optional pool behavior.
type PoolOption func(*poolOptions)

type poolOptions struct {
	discardOutput bool
}

// WithDiscardOutput makes the pool a terminal stage: workers still run processFunc
// and update Submitted/Completed/Failed/InProgress stats, but never send on OutputQ.
func WithDiscardOutput() PoolOption {
	return func(o *poolOptions) {
		o.discardOutput = true
	}
}

// Submitter is anything that accepts jobs with backpressure.
type Submitter[T any] interface {
	SubmitWait(ctx context.Context, data T) error
}

// NewPool creates and starts a worker pool.
func NewPool[T, R any](
	ctx context.Context,
	name string,
	numWorkers, queueSize int,
	logger *slog.Logger,
	processFunc func(context.Context, T) (R, error),
	opts ...PoolOption,
) (*Pool[T, R], error) {
	if numWorkers <= 0 {
		return nil, errors.Join(ErrInvalidConfig, errors.New("numWorkers must be > 0"))
	}
	if queueSize <= 0 {
		return nil, errors.Join(ErrInvalidConfig, errors.New("queueSize must be > 0"))
	}
	if processFunc == nil {
		return nil, errors.Join(ErrInvalidConfig, errors.New("processFunc is required"))
	}
	if logger == nil {
		logger = slog.Default()
	}
	if name == "" {
		name = "pool"
	}

	var po poolOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&po)
		}
	}

	logger = logger.With(slog.String("pool", name))
	poolCtx, cancel := context.WithCancel(ctx)

	// Output capacity is larger than input so workers can finish in-flight work and
	// free input slots even when the downstream consumer is briefly slower.
	// (Critical for cyclic pipelines: feedback re-enters an upstream stage.)
	outSize := queueSize*2 + numWorkers
	if outSize < queueSize {
		outSize = queueSize
	}

	p := &Pool[T, R]{
		Name:          name,
		numWorkers:    numWorkers,
		queueSize:     queueSize,
		InputQ:        make(chan Input[T], queueSize),
		processFunc:   processFunc,
		logger:        logger,
		ctx:           poolCtx,
		cancel:        cancel,
		discardOutput: po.discardOutput,
	}

	if po.discardOutput {
		// Terminal stage: no consumer needed; keep a closed-ready channel unused.
		p.OutputQ = nil
		p.ownsOutput = false
	} else {
		p.OutputQ = make(chan Result[R], outSize)
		p.ownsOutput = true
	}

	for i := 0; i < numWorkers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}

	logger.Debug("pool started",
		slog.Int("workers", numWorkers),
		slog.Int("queue_size", queueSize),
		slog.Bool("discard_output", po.discardOutput),
	)
	return p, nil
}

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
			input.WorkerID = id
			result := p.processItem(input)
			if !p.discardOutput {
				p.sendResult(result)
			}
		}
	}
}

func (p *Pool[T, R]) sendResult(result Result[R]) {
	if p.OutputQ == nil {
		return
	}
	select {
	case p.OutputQ <- result:
	case <-p.ctx.Done():
	}
}

func (p *Pool[T, R]) processItem(input Input[T]) Result[R] {
	p.stats.InProgress.Add(1)
	defer p.stats.InProgress.Add(-1)

	start := time.Now()
	value, err := p.processFunc(p.ctx, input.Data)
	duration := time.Since(start)
	p.stats.TotalDuration.Add(int64(duration))

	if err != nil {
		p.stats.Failed.Add(1)
		return Result[R]{Value: value, Err: err, Duration: duration}
	}
	p.stats.Completed.Add(1)
	return Result[R]{Value: value, Duration: duration}
}

func (p *Pool[T, R]) makeInput(data T) Input[T] {
	return Input[T]{
		ID:          formatID(p.idSeq.Add(1)),
		Data:        data,
		SubmittedAt: time.Now(),
	}
}

func formatID(n uint64) string {
	const digits = "0123456789"
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = digits[n%10]
		n /= 10
	}
	return string(buf[i:])
}

// Submit tries to enqueue without blocking. Returns ErrQueueFull if the queue is full.
func (p *Pool[T, R]) Submit(ctx context.Context, data T) error {
	if p.stopped.Load() {
		return ErrPoolStopped
	}
	input := p.makeInput(data)
	p.stats.Submitted.Add(1)

	select {
	case p.InputQ <- input:
		return nil
	case <-ctx.Done():
		p.stats.Submitted.Add(-1)
		return ctx.Err()
	case <-p.ctx.Done():
		p.stats.Submitted.Add(-1)
		return ErrPoolStopped
	default:
		p.stats.Submitted.Add(-1)
		return ErrQueueFull
	}
}

// SubmitWait blocks until the job is queued or ctx/pool is cancelled.
func (p *Pool[T, R]) SubmitWait(ctx context.Context, data T) error {
	if p.stopped.Load() {
		return ErrPoolStopped
	}
	input := p.makeInput(data)
	p.stats.Submitted.Add(1)

	select {
	case p.InputQ <- input:
		return nil
	case <-ctx.Done():
		p.stats.Submitted.Add(-1)
		return ctx.Err()
	case <-p.ctx.Done():
		p.stats.Submitted.Add(-1)
		return ErrPoolStopped
	}
}

// Outputs returns the result channel (receive-only).
// For discard-output (terminal) pools, returns nil.
func (p *Pool[T, R]) Outputs() <-chan Result[R] {
	return p.OutputQ
}

// DiscardsOutput reports whether this pool is a terminal stage (no OutputQ sends).
func (p *Pool[T, R]) DiscardsOutput() bool {
	return p.discardOutput
}

// Stats returns a pointer to live atomic stats.
func (p *Pool[T, R]) Stats() *Stats {
	return &p.stats
}

// Pending returns items submitted but not yet completed or failed.
func (p *Pool[T, R]) Pending() int64 {
	return p.stats.Snapshot().Pending()
}

// IsStopped reports whether the pool has been stopped.
func (p *Pool[T, R]) IsStopped() bool {
	return p.stopped.Load()
}

// IsRunning is the inverse of IsStopped.
func (p *Pool[T, R]) IsRunning() bool {
	return !p.stopped.Load()
}

// Stop gracefully shuts down: stop accepting, close input so workers drain, wait, close output.
func (p *Pool[T, R]) Stop() {
	p.stopOnce.Do(func() {
		p.stopped.Store(true)
		close(p.InputQ)
		p.wg.Wait()
		p.cancel()
		if p.ownsOutput && p.OutputQ != nil {
			close(p.OutputQ)
		}
		snap := p.stats.Snapshot()
		p.logger.Debug("pool stopped",
			slog.Int64("completed", snap.Completed),
			slog.Int64("failed", snap.Failed),
		)
	})
}

// StopNow cancels in-flight work and stops the pool.
func (p *Pool[T, R]) StopNow() {
	p.stopOnce.Do(func() {
		p.stopped.Store(true)
		p.cancel()
		close(p.InputQ)
		p.wg.Wait()
		if p.ownsOutput && p.OutputQ != nil {
			close(p.OutputQ)
		}
	})
}

// =============================================================================
// Bridge
// =============================================================================

// Bridge drains upstream results into dest.SubmitWait.
// Failed results (Err != nil) are skipped (optional onErr called).
// Blocks on SubmitWait — this is intentional backpressure.
// Returns when upstream is closed or ctx is done.
func Bridge[T any](
	ctx context.Context,
	upstream <-chan Result[T],
	dest Submitter[T],
	onErr func(error),
) {
	for {
		select {
		case <-ctx.Done():
			return
		case res, ok := <-upstream:
			if !ok {
				return
			}
			if res.Err != nil {
				if onErr != nil {
					onErr(res.Err)
				}
				continue
			}
			if err := dest.SubmitWait(ctx, res.Value); err != nil {
				if onErr != nil {
					onErr(err)
				}
				if errors.Is(err, ErrPoolStopped) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return
				}
			}
		}
	}
}

// StartBridge runs Bridge in a new goroutine.
func StartBridge[T any](
	ctx context.Context,
	upstream <-chan Result[T],
	dest Submitter[T],
	onErr func(error),
) {
	go Bridge(ctx, upstream, dest, onErr)
}

// Drain discards all values from ch until closed or ctx done.
// Required so workers never block forever on a full output queue.
func Drain[R any](ctx context.Context, ch <-chan Result[R]) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-ch:
			if !ok {
				return
			}
		}
	}
}

// StartDrain runs Drain in a new goroutine.
func StartDrain[R any](ctx context.Context, ch <-chan Result[R]) {
	go Drain(ctx, ch)
}

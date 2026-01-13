// Package saver provides a database client wrapper for saving processed data.
// Uses the generic workerpool.Pool for worker management.
package saver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/samucap/poly-asian-data/internal/processor"
	"github.com/samucap/poly-asian-data/internal/workerpool"
)

// =============================================================================
// Error Definitions
// =============================================================================

var (
	ErrPoolStopped   = errors.New("saver pool has been stopped")
	ErrInvalidConfig = errors.New("invalid saver configuration")
	ErrSaveFailed    = errors.New("save failed")
)

// =============================================================================
// Type Definitions
// =============================================================================

// Record represents data to be saved.
type Record struct {
	ID          string
	SourceURL   string
	Data        any
	Metadata    map[string]any
	FetchedAt   time.Time
	ProcessedAt time.Time
}

// SaveResult represents the outcome of a save operation.
type SaveResult struct {
	RecordID     string
	SourceURL    string
	Duration     time.Duration
	Err          error
	SavedAt      time.Time
	RowsAffected int64
}

// SaveFunc is the function signature for saving data.
type SaveFunc func(ctx context.Context, record *Record) (rowsAffected int64, err error)

// Stats contains atomic counters for saver statistics.
type Stats struct {
	RecordsSubmitted atomic.Int64
	RecordsSaved     atomic.Int64
	RecordsFailed    atomic.Int64
	RowsAffected     atomic.Int64
	TotalDuration    atomic.Int64
}

// Snapshot returns a point-in-time copy.
func (s *Stats) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		RecordsSubmitted: s.RecordsSubmitted.Load(),
		RecordsSaved:     s.RecordsSaved.Load(),
		RecordsFailed:    s.RecordsFailed.Load(),
		RowsAffected:     s.RowsAffected.Load(),
		TotalDuration:    time.Duration(s.TotalDuration.Load()),
	}
}

// StatsSnapshot is an immutable snapshot.
type StatsSnapshot struct {
	RecordsSubmitted int64
	RecordsSaved     int64
	RecordsFailed    int64
	RowsAffected     int64
	TotalDuration    time.Duration
}

// AverageDuration returns average save duration.
func (s StatsSnapshot) AverageDuration() time.Duration {
	total := s.RecordsSaved + s.RecordsFailed
	if total == 0 {
		return 0
	}
	return s.TotalDuration / time.Duration(total)
}

// =============================================================================
// Configuration
// =============================================================================

// Config holds the configuration for a saver pool.
type Config struct {
	NumWorkers  int
	QueueSize   int
	SaveTimeout time.Duration
	MaxRetries  int
	RetryDelay  time.Duration
	SaveFunc    SaveFunc
	Logger      *slog.Logger
}

// Validate checks the configuration.
func (c *Config) Validate() error {
	if c.NumWorkers < 1 {
		return fmt.Errorf("%w: NumWorkers must be >= 1", ErrInvalidConfig)
	}
	if c.QueueSize < 1 {
		return fmt.Errorf("%w: QueueSize must be >= 1", ErrInvalidConfig)
	}
	if c.SaveFunc == nil {
		return fmt.Errorf("%w: SaveFunc is required", ErrInvalidConfig)
	}
	if c.SaveTimeout == 0 {
		c.SaveTimeout = 30 * time.Second
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = 3
	}
	if c.RetryDelay == 0 {
		c.RetryDelay = 100 * time.Millisecond
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return nil
}

// =============================================================================
// Pool Implementation
// =============================================================================

// Pool is a saver that wraps workerpool.Pool.
// Only contains domain-specific state - lifecycle is managed by workerpool.
type Pool struct {
	pool    *workerpool.Pool[*Record, *SaveResult]
	config  Config
	stats   Stats
	logger  *slog.Logger
	input   chan *Record     // Owned internally
	results chan *SaveResult // Owned internally, for test access
}

// New creates and initializes a saver pool.
// Validates config and sets up resources (logger, input channel).
func New(ctx context.Context, config Config) (*Pool, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	logger := config.Logger.With(
		slog.String("component", "saver"),
	)

	p := &Pool{
		config:  config,
		logger:  logger,
		input:   make(chan *Record, config.QueueSize),
		results: make(chan *SaveResult, config.QueueSize),
	}

	logger.Info("saver initialized",
		slog.Int("workers", config.NumWorkers),
		slog.Int("queue_size", config.QueueSize),
	)

	return p, nil
}

// Subscribe connects to an upstream processor's output channel (fan-in).
// Converts processor.Output -> saver.Record and feeds to internal input channel.
func (s *Pool) Subscribe(upstream <-chan *processor.Output) {
	go func() {
		for output := range upstream {
			s.input <- &Record{
				ID:          output.InputID,
				SourceURL:   output.SourceURL,
				Data:        output.Data,
				Metadata:    output.Metadata,
				FetchedAt:   output.FetchedAt,
				ProcessedAt: output.ProcessedAt,
			}
		}
		close(s.input)
	}()
}



// =============================================================================
// Lifecycle Methods
// =============================================================================

// Name returns the pool's name.
func (s *Pool) Name() string {
	return "saver"
}

// Start begins the pool's work.
// Workers read from internal input channel.
func (s *Pool) Start(ctx context.Context) error {
	if s.pool != nil {
		return errors.New("pool is already running")
	}

	// Create the worker pool with the correct type parameters
	pool, err := workerpool.NewPool[*Record, *SaveResult](ctx, "saver", s.config.NumWorkers, s.config.QueueSize)
	if err != nil {
		return err
	}

	// Set the WorkerTask function
	pool.WorkerTask = s.Save

	s.pool = pool

	// Start input listener
	go s.listenInput(ctx)

	// Forward results from workerpool to internal results channel
	go func() {
		for result := range s.pool.Outputs() {
			s.results <- result.Value
		}
		close(s.results)
	}()

	s.logger.Info("saver started",
		slog.Int("workers", s.config.NumWorkers),
	)

	return nil
}

// IsRunning returns true if the pool is currently running.
func (s *Pool) IsRunning() bool {
	return s.pool != nil && !s.pool.IsStopped()
}

// listenInput reads from internal input channel and submits to worker pool.
func (s *Pool) listenInput(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case record, ok := <-s.input:
			if !ok {
				return
			}
			s.stats.RecordsSubmitted.Add(1)
			if err := s.pool.SubmitWait(record); err != nil {
				s.logger.Error("failed to submit record",
					slog.String("id", record.ID),
					slog.String("error", err.Error()),
				)
			}
		}
	}
}

// drainResults drains results (saver is final stage, results already logged in Save).
func (s *Pool) drainResults() {
	for range s.pool.Outputs() {
		// Results are logged in Save() function, stats already tracked
	}
}

// =============================================================================
// SaverJob Interface - Work Function
// =============================================================================

// Save is the work function that persists a record.
// This is the domain-specific operation for the saver stage.
func (s *Pool) Save(ctx context.Context, record *Record) (*SaveResult, error) {
	start := time.Now()

	s.logger.Info("saving data",
		slog.String("record_id", record.ID),
		slog.String("url", record.SourceURL),
	)

	var lastErr error
	for attempt := 0; attempt <= s.config.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := s.config.RetryDelay * time.Duration(1<<(attempt-1))
			select {
			case <-ctx.Done():
				return &SaveResult{
					RecordID:  record.ID,
					SourceURL: record.SourceURL,
					Duration:  time.Since(start),
					Err:       ctx.Err(),
					SavedAt:   time.Now(),
				}, ctx.Err()
			case <-time.After(delay):
			}
		}

		saveCtx, cancel := context.WithTimeout(ctx, s.config.SaveTimeout)
		rowsAffected, err := s.config.SaveFunc(saveCtx, record)
		cancel()

		if err == nil {
			duration := time.Since(start)
			s.stats.RecordsSaved.Add(1)
			s.stats.RowsAffected.Add(rowsAffected)
			s.stats.TotalDuration.Add(int64(duration))

			s.logger.Info("saved data",
				slog.String("record_id", record.ID),
				slog.Int64("rows_affected", rowsAffected),
			)

			return &SaveResult{
				RecordID:     record.ID,
				SourceURL:    record.SourceURL,
				Duration:     duration,
				SavedAt:      time.Now(),
				RowsAffected: rowsAffected,
			}, nil
		}
		lastErr = err
	}

	duration := time.Since(start)
	s.stats.RecordsFailed.Add(1)
	s.stats.TotalDuration.Add(int64(duration))

	s.logger.Error("save failed",
		slog.String("record_id", record.ID),
		slog.String("error", lastErr.Error()),
	)

	return &SaveResult{
		RecordID:  record.ID,
		SourceURL: record.SourceURL,
		Duration:  duration,
		Err:       fmt.Errorf("%w: %v", ErrSaveFailed, lastErr),
		SavedAt:   time.Now(),
	}, lastErr
}




// Submit adds a record directly to the pool's internal input channel.
// Primarily for testing. In production, use Subscribe() to connect upstream stages.
func (s *Pool) Submit(record *Record) error {
	if record == nil {
		return errors.New("record cannot be nil")
	}
	if s.pool == nil {
		return errors.New("pool not started: call Start() first")
	}
	if s.pool.IsStopped() {
		return ErrPoolStopped
	}
	s.input <- record
	return nil
}

// Results returns the output channel with save results.
// Primarily for testing. In production, results are logged.
func (s *Pool) Results() <-chan *SaveResult {
	return s.results
}

// Stats returns statistics.
func (s *Pool) Stats() *Stats {
	return &s.stats
}

// NumWorkers returns worker count.
func (s *Pool) NumWorkers() int {
	return s.config.NumWorkers
}

// =============================================================================
// Lifecycle Methods
// =============================================================================

// Stop gracefully shuts down the pool.
func (s *Pool) Stop() error {
	if s.pool == nil {
		return nil
	}

	s.pool.Stop()

	stats := s.stats.Snapshot()
	s.logger.Info("saver stopped",
		slog.Int64("records_saved", stats.RecordsSaved),
		slog.Int64("records_failed", stats.RecordsFailed),
	)

	return nil
}

// StopNow immediately stops the pool.
func (s *Pool) StopNow() error {
	if s.pool == nil {
		return nil
	}

	s.pool.StopNow()
	return nil
}

// =============================================================================
// Placeholder Save Functions
// =============================================================================

// NoOpSaveFunc is a placeholder that does nothing.
func NoOpSaveFunc(ctx context.Context, record *Record) (int64, error) {
	return 1, nil
}

// LoggingSaveFunc returns a save function that logs what would be saved.
func LoggingSaveFunc(logger *slog.Logger) SaveFunc {
	if logger == nil {
		logger = slog.Default()
	}
	return func(ctx context.Context, record *Record) (int64, error) {
		logger.Info("would save record",
			slog.String("id", record.ID),
			slog.String("url", record.SourceURL),
		)
		return 1, nil
	}
}

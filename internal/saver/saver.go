// Package saver provides a database client wrapper for saving processed data.
// Uses the generic workerpool.Pool for worker management.
package saver

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

// Pool is a saver pool using generic workerpool.Pool.
type Pool struct {
	pool    *workerpool.Pool[*Record, *SaveResult]
	config  Config
	stats   Stats
	logger  *slog.Logger
	ctx     context.Context
	cancel  context.CancelFunc
	stopped atomic.Bool

	// Cached results channel
	results chan *SaveResult

	// Source listener
	sourceWg sync.WaitGroup
}

// NewPool creates a new saver pool.
func NewPool(ctx context.Context, config Config) (*Pool, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	poolCtx, cancel := context.WithCancel(ctx)

	s := &Pool{
		config:  config,
		logger:  config.Logger.With(slog.String("component", "saver")),
		ctx:     poolCtx,
		cancel:  cancel,
		results: make(chan *SaveResult, config.QueueSize),
	}

	pool, err := workerpool.NewPool(poolCtx, workerpool.Config{
		Name:       "saver",
		NumWorkers: config.NumWorkers,
		QueueSize:  config.QueueSize,
		Logger:     config.Logger,
	}, s.save)

	if err != nil {
		cancel()
		return nil, err
	}

	s.pool = pool

	// Start result forwarder
	go s.forwardResults()

	return s, nil
}

// StartListening starts goroutines to listen on a source channel and save items.
// This connects the saver to an upstream stage (e.g., processor outputs).
func (s *Pool) StartListening(source <-chan *Record) {
	s.sourceWg.Add(1)
	go func() {
		defer s.sourceWg.Done()
		for {
			select {
			case <-s.ctx.Done():
				return
			case record, ok := <-source:
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
	}()
}

// forwardResults forwards pool outputs to the cached results channel.
func (s *Pool) forwardResults() {
	defer close(s.results)
	for result := range s.pool.Outputs() {
		if result.Value != nil {
			s.results <- result.Value
		}
	}
}

// save is the worker function with retry logic.
func (s *Pool) save(ctx context.Context, record *Record) (*SaveResult, error) {
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

// Submit adds a record to the pool directly.
func (s *Pool) Submit(record *Record) error {
	if record == nil {
		return errors.New("record cannot be nil")
	}
	if s.stopped.Load() {
		return ErrPoolStopped
	}
	s.stats.RecordsSubmitted.Add(1)
	return s.pool.SubmitWait(record)
}

// Results returns the cached results channel.
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

// Stop gracefully shuts down.
func (s *Pool) Stop() {
	s.stopped.Store(true)
	s.sourceWg.Wait() // Wait for source listeners to finish
	s.pool.Stop()
	s.cancel()

	stats := s.stats.Snapshot()
	s.logger.Info("saver pool stopped",
		slog.Int64("records_saved", stats.RecordsSaved),
		slog.Int64("records_failed", stats.RecordsFailed),
	)
}

// StopNow immediately stops.
func (s *Pool) StopNow() {
	s.stopped.Store(true)
	s.pool.StopNow()
	s.cancel()
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

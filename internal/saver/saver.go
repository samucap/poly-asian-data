// Package saver provides a database client wrapper for saving processed data.
// Uses the generic workerpool.Pool for worker management.
package saver

import (
	"context"
	"errors"
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
type Saver struct {
	logger *slog.Logger
}

// Record represents data to be saved.
type Record struct {
	ID          string
	Data        any
	ProcessedAt time.Time
}

// Result represents the outcome of a save operation.
type Result struct {
	ID           string
	RowsAffected int64
	Duration     time.Duration
}

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

func New(ctx context.Context, cfg *config.Config, numWorkers int, qsize int) (*Pool, error) {
	logger := logging.Logger.With(
		slog.String("component", "saver"),
	)

	s := &Saver{}
}

// SubscribeToProcessor subscribes to processor output and transforms for saving.
// TODO: This will be updated when pipeline fan-out routes to saver.
func (p *Pool) SubscribeToProcessor(ctx context.Context, upstream <-chan workerpool.Result[*processor.Output]) {
	for {
		select {
		case result, ok := <-upstream:
			if !ok {
				return
			}
			if result.Err != nil {
				continue
			}
			output := result.Value
			record := &Record{
				ID:          output.ID,
				Data:        output.Data,
				ProcessedAt: output.ProcessedAt,
			}
			_ = p.SubmitWait(record)
		case <-ctx.Done():
			return
		}
	}
}
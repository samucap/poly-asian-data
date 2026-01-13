package saver

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	// Initialize logging for tests
	logging.Init("dev")
}

// =============================================================================
// Test Helper
// =============================================================================

// newTestPool creates a saver pool with New() + Start() for testing.
func newTestPool(ctx context.Context, config Config) (*Pool, error) {
	pool, err := New(ctx, config)
	if err != nil {
		return nil, err
	}
	if err := pool.Start(ctx); err != nil {
		return nil, err
	}
	return pool, nil
}

// =============================================================================
// Configuration Tests
// =============================================================================

func TestConfig_Validate(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := Config{
			NumWorkers: 4,
			QueueSize:  100,
			SaveFunc:   NoOpSaveFunc,
			Logger:     slog.Default(),
		}
		err := cfg.Validate()
		assert.NoError(t, err)
		assert.Equal(t, 30*time.Second, cfg.SaveTimeout)
		assert.Equal(t, 3, cfg.MaxRetries)
	})

	t.Run("missing save func", func(t *testing.T) {
		cfg := Config{NumWorkers: 4, QueueSize: 10, Logger: slog.Default()}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "SaveFunc")
	})

	t.Run("zero workers", func(t *testing.T) {
		cfg := Config{NumWorkers: 0, QueueSize: 10, SaveFunc: NoOpSaveFunc, Logger: slog.Default()}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "NumWorkers")
	})

	t.Run("zero queue size", func(t *testing.T) {
		cfg := Config{NumWorkers: 4, QueueSize: 0, SaveFunc: NoOpSaveFunc, Logger: slog.Default()}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "QueueSize")
	})
}

// =============================================================================
// Pool Tests
// =============================================================================

func TestNewPool(t *testing.T) {
	t.Run("creates pool with valid config", func(t *testing.T) {
		ctx := context.Background()
		pool, err := newTestPool(ctx, Config{
			NumWorkers: 2,
			QueueSize:  10,
			SaveFunc:   NoOpSaveFunc,
			Logger:     slog.Default(),
		})
		require.NoError(t, err)
		require.NotNil(t, pool)
		assert.Equal(t, 2, pool.NumWorkers())
		pool.Stop()
	})

	t.Run("returns error with invalid config", func(t *testing.T) {
		ctx := context.Background()
		pool, err := newTestPool(ctx, Config{
			NumWorkers: 0,
			QueueSize:  10,
			Logger:     slog.Default(),
		})
		assert.Error(t, err)
		assert.Nil(t, pool)
	})
}

func TestPool_Submit(t *testing.T) {
	t.Run("submit nil record returns error", func(t *testing.T) {
		ctx := context.Background()
		pool, _ := newTestPool(ctx, Config{
			NumWorkers: 2,
			QueueSize:  10,
			SaveFunc:   NoOpSaveFunc,
			Logger:     slog.Default(),
		})
		defer pool.Stop()

		err := pool.Submit(nil)
		assert.Error(t, err)
	})

	t.Run("submit to stopped pool returns error", func(t *testing.T) {
		ctx := context.Background()
		pool, _ := newTestPool(ctx, Config{
			NumWorkers: 2,
			QueueSize:  10,
			SaveFunc:   NoOpSaveFunc,
			Logger:     slog.Default(),
		})
		pool.Stop()

		err := pool.Submit(&Record{ID: "test"})
		assert.Error(t, err)
		assert.Equal(t, ErrPoolStopped, err)
	})
}

// =============================================================================
// Save Tests
// =============================================================================

func TestPool_Save(t *testing.T) {
	t.Run("successful save", func(t *testing.T) {
		ctx := context.Background()

		var savedData any
		pool, err := newTestPool(ctx, Config{
			NumWorkers: 2,
			QueueSize:  10,
			SaveFunc: func(ctx context.Context, record *Record) (int64, error) {
				savedData = record.Data
				return 1, nil
			},
			Logger: slog.Default(),
		})
		require.NoError(t, err)
		defer pool.Stop()

		err = pool.Submit(&Record{
			ID:          "test-1",
			SourceURL:   "http://example.com",
			Data:        map[string]any{"key": "value"},
			ProcessedAt: time.Now(),
		})
		require.NoError(t, err)

		result := <-pool.Results()
		assert.NoError(t, result.Err)
		assert.Equal(t, "test-1", result.RecordID)
		assert.Equal(t, "http://example.com", result.SourceURL)
		assert.Equal(t, int64(1), result.RowsAffected)
		assert.Greater(t, result.Duration, time.Duration(0))
		assert.False(t, result.SavedAt.IsZero())

		assert.Equal(t, map[string]any{"key": "value"}, savedData)
	})

	t.Run("save failure with retry", func(t *testing.T) {
		var attempts atomic.Int32

		ctx := context.Background()
		pool, _ := newTestPool(ctx, Config{
			NumWorkers: 1,
			QueueSize:  10,
			MaxRetries: 2,
			RetryDelay: time.Millisecond * 10,
			SaveFunc: func(ctx context.Context, record *Record) (int64, error) {
				attempts.Add(1)
				return 0, errors.New("db error")
			},
			Logger: slog.Default(),
		})
		defer pool.Stop()

		_ = pool.Submit(&Record{ID: "fail-test"})

		// Wait for retries to complete
		time.Sleep(time.Millisecond * 200)
		assert.Equal(t, int32(3), attempts.Load()) // 1 + 2 retries

		stats := pool.Stats().Snapshot()
		assert.Equal(t, int64(1), stats.RecordsFailed)
	})

	t.Run("successful after retry", func(t *testing.T) {
		var attempts atomic.Int32

		ctx := context.Background()
		pool, _ := newTestPool(ctx, Config{
			NumWorkers: 1,
			QueueSize:  10,
			MaxRetries: 3,
			RetryDelay: time.Millisecond * 5,
			SaveFunc: func(ctx context.Context, record *Record) (int64, error) {
				if attempts.Add(1) < 3 {
					return 0, errors.New("temporary error")
				}
				return 1, nil
			},
			Logger: slog.Default(),
		})
		defer pool.Stop()

		_ = pool.Submit(&Record{ID: "retry-success"})

		result := <-pool.Results()
		assert.NoError(t, result.Err)
		assert.Equal(t, int32(3), attempts.Load())
	})
}

// =============================================================================
// Stats Tests
// =============================================================================

func TestPool_Stats(t *testing.T) {
	ctx := context.Background()

	pool, _ := newTestPool(ctx, Config{
		NumWorkers: 1,
		QueueSize:  10,
		SaveFunc: func(ctx context.Context, record *Record) (int64, error) {
			return 2, nil // 2 rows affected per save
		},
		Logger: slog.Default(),
	})
	defer pool.Stop()

	// Submit 5 records
	for i := 0; i < 5; i++ {
		_ = pool.Submit(&Record{ID: string(rune('a' + i))})
	}

	// Drain results
	for i := 0; i < 5; i++ {
		<-pool.Results()
	}

	stats := pool.Stats().Snapshot()
	assert.Equal(t, int64(5), stats.RecordsSubmitted)
	assert.Equal(t, int64(5), stats.RecordsSaved)
	assert.Equal(t, int64(0), stats.RecordsFailed)
	assert.Equal(t, int64(10), stats.RowsAffected) // 5 * 2
	assert.Greater(t, stats.TotalDuration, time.Duration(0))
}

func TestStatsSnapshot_AverageDuration(t *testing.T) {
	t.Run("with completed records", func(t *testing.T) {
		s := StatsSnapshot{
			RecordsSaved:  5,
			RecordsFailed: 5,
			TotalDuration: time.Second * 10,
		}
		assert.Equal(t, time.Second, s.AverageDuration())
	})

	t.Run("with no records", func(t *testing.T) {
		s := StatsSnapshot{}
		assert.Equal(t, time.Duration(0), s.AverageDuration())
	})
}

// =============================================================================
// Shutdown Tests
// =============================================================================

func TestPool_Stop(t *testing.T) {
	t.Run("graceful stop", func(t *testing.T) {
		ctx := context.Background()
		pool, _ := newTestPool(ctx, Config{
			NumWorkers: 1,
			QueueSize:  10,
			SaveFunc: func(ctx context.Context, record *Record) (int64, error) {
				time.Sleep(time.Millisecond * 50)
				return 1, nil
			},
			Logger: slog.Default(),
		})

		_ = pool.Submit(&Record{ID: "slow"})

		// Wait for save to start
		time.Sleep(time.Millisecond * 20)

		pool.Stop()

		// Result should be available
		result := <-pool.Results()
		assert.NoError(t, result.Err)
	})

	t.Run("stop is idempotent", func(t *testing.T) {
		ctx := context.Background()
		pool, _ := newTestPool(ctx, Config{
			NumWorkers: 1,
			QueueSize:  10,
			SaveFunc:   NoOpSaveFunc,
			Logger:     slog.Default(),
		})

		pool.Stop()
		pool.Stop()
		pool.Stop()
	})
}

// =============================================================================
// Placeholder Functions Tests
// =============================================================================

func TestNoOpSaveFunc(t *testing.T) {
	ctx := context.Background()
	record := &Record{ID: "test"}

	rows, err := NoOpSaveFunc(ctx, record)

	assert.NoError(t, err)
	assert.Equal(t, int64(1), rows)
}

func TestLoggingSaveFunc(t *testing.T) {
	ctx := context.Background()
	fn := LoggingSaveFunc(nil)

	rows, err := fn(ctx, &Record{ID: "test", SourceURL: "http://example.com"})

	assert.NoError(t, err)
	assert.Equal(t, int64(1), rows)
}

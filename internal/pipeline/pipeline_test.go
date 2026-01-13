package pipeline

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/saver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	// Initialize logging for tests
	logging.Init("dev")
}

// =============================================================================
// Configuration Tests
// =============================================================================

func TestConfig_Validate(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := Config{
			NumWorkers: 2,
			QueueSize:  10,
			SaverCfg: saver.Config{
				NumWorkers: 2,
				QueueSize:  10,
				SaveFunc:   saver.NoOpSaveFunc,
				Logger:     slog.Default(),
			},
			Logger: slog.Default(),
		}
		err := cfg.Validate()
		assert.NoError(t, err)
	})

	t.Run("invalid num workers", func(t *testing.T) {
		cfg := Config{
			NumWorkers: 0, // Invalid
			QueueSize:  10,
			SaverCfg: saver.Config{
				NumWorkers: 2,
				QueueSize:  10,
				SaveFunc:   saver.NoOpSaveFunc,
				Logger:     slog.Default(),
			},
			Logger: slog.Default(),
		}
		err := cfg.Validate()
		assert.Error(t, err)
	})

	t.Run("invalid queue size", func(t *testing.T) {
		cfg := Config{
			NumWorkers: 2,
			QueueSize:  0, // Invalid
			SaverCfg: saver.Config{
				NumWorkers: 2,
				QueueSize:  10,
				SaveFunc:   saver.NoOpSaveFunc,
				Logger:     slog.Default(),
			},
			Logger: slog.Default(),
		}
		err := cfg.Validate()
		assert.Error(t, err)
	})

	t.Run("invalid saver config", func(t *testing.T) {
		cfg := Config{
			NumWorkers: 2,
			QueueSize:  10,
			SaverCfg: saver.Config{
				NumWorkers: 2,
				QueueSize:  10,
				// Missing SaveFunc
			},
			Logger: slog.Default(),
		}
		err := cfg.Validate()
		assert.Error(t, err)
	})
}

// =============================================================================
// Pipeline Tests
// =============================================================================

func TestNew(t *testing.T) {
	t.Run("creates pipeline with valid config", func(t *testing.T) {
		ctx := context.Background()
		p, err := New(ctx, Config{
			NumWorkers: 2,
			QueueSize:  10,
			SaverCfg: saver.Config{
				NumWorkers: 2,
				QueueSize:  10,
				SaveFunc:   saver.NoOpSaveFunc,
				Logger:     slog.Default(),
			},
			Logger: slog.Default(),
		})
		require.NoError(t, err)
		require.NotNil(t, p)
		assert.False(t, p.IsStopped())
		p.Stop()
	})

	t.Run("returns error with invalid config", func(t *testing.T) {
		ctx := context.Background()
		p, err := New(ctx, Config{
			NumWorkers: 0, // Invalid
			QueueSize:  10,
		})
		assert.Error(t, err)
		assert.Nil(t, p)
	})
}

func TestPipeline_WorkerCounts(t *testing.T) {
	ctx := context.Background()
	p, _ := New(ctx, Config{
		NumWorkers: 3,
		QueueSize:  10,
		SaverCfg: saver.Config{
			NumWorkers: 2,
			QueueSize:  10,
			SaveFunc:   saver.NoOpSaveFunc,
			Logger:     slog.Default(),
		},
		Logger: slog.Default(),
	})
	defer p.Stop()

	counts := p.WorkerCounts()
	assert.Equal(t, 3, counts.Fetcher)
	assert.Equal(t, 3, counts.Processor)
	assert.Equal(t, 2, counts.Saver)
}

// =============================================================================
// Shutdown Tests
// =============================================================================

func TestPipeline_Stop(t *testing.T) {
	t.Run("graceful stop", func(t *testing.T) {
		ctx := context.Background()
		p, _ := New(ctx, Config{
			NumWorkers: 2,
			QueueSize:  10,
			SaverCfg: saver.Config{
				NumWorkers: 2,
				QueueSize:  10,
				SaveFunc:   saver.NoOpSaveFunc,
				Logger:     slog.Default(),
			},
			Logger: slog.Default(),
		})

		p.Stop()

		assert.True(t, p.IsStopped())
	})

	t.Run("stop is idempotent", func(t *testing.T) {
		ctx := context.Background()
		p, _ := New(ctx, Config{
			NumWorkers: 2,
			QueueSize:  10,
			SaverCfg: saver.Config{
				NumWorkers: 2,
				QueueSize:  10,
				SaveFunc:   saver.NoOpSaveFunc,
				Logger:     slog.Default(),
			},
			Logger: slog.Default(),
		})

		p.Stop()
		p.Stop()
		p.Stop()

		assert.True(t, p.IsStopped())
	})

	t.Run("submit to stopped pipeline returns error", func(t *testing.T) {
		ctx := context.Background()
		p, _ := New(ctx, Config{
			NumWorkers: 2,
			QueueSize:  10,
			SaverCfg: saver.Config{
				NumWorkers: 2,
				QueueSize:  10,
				SaveFunc:   saver.NoOpSaveFunc,
				Logger:     slog.Default(),
			},
			Logger: slog.Default(),
		})

		p.Stop()

		err := p.SubmitURL("test", "http://example.com", nil)
		assert.Error(t, err)
		assert.Equal(t, ErrPipelineStopped, err)
	})
}

// =============================================================================
// Stats Tests
// =============================================================================

func TestPipeline_Stats(t *testing.T) {
	ctx := context.Background()
	p, _ := New(ctx, Config{
		NumWorkers: 2,
		QueueSize:  10,
		SaverCfg: saver.Config{
			NumWorkers: 2,
			QueueSize:  10,
			SaveFunc:   saver.NoOpSaveFunc,
			Logger:     slog.Default(),
		},
		Logger: slog.Default(),
	})
	defer p.Stop()

	stats := p.Stats()

	assert.False(t, stats.StartedAt.IsZero())
	assert.Greater(t, stats.UptimeDuration, time.Duration(0))
}

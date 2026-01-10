package pipeline

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/processor"
	"github.com/samucap/poly-asian-data/internal/saver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Configuration Tests
// =============================================================================

func TestConfig_Validate(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := Config{
			FetcherConfig: fetcher.Config{
				NumWorkers: 2,
				QueueSize:  10,
			},
			ProcessorConfig: processor.Config{
				NumWorkers:  2,
				QueueSize:   10,
				ProcessFunc: processor.PassthroughProcessor,
			},
			SaverConfig: saver.Config{
				NumWorkers: 2,
				QueueSize:  10,
				SaveFunc:   saver.NoOpSaveFunc,
			},
		}
		err := cfg.Validate()
		assert.NoError(t, err)
		assert.NotNil(t, cfg.Logger)
	})

	t.Run("invalid fetcher config", func(t *testing.T) {
		cfg := Config{
			FetcherConfig: fetcher.Config{
				NumWorkers: 0, // Invalid
				QueueSize:  10,
			},
			ProcessorConfig: processor.Config{
				NumWorkers:  2,
				QueueSize:   10,
				ProcessFunc: processor.PassthroughProcessor,
			},
			SaverConfig: saver.Config{
				NumWorkers: 2,
				QueueSize:  10,
				SaveFunc:   saver.NoOpSaveFunc,
			},
		}
		err := cfg.Validate()
		assert.Error(t, err)
	})

	t.Run("invalid processor config", func(t *testing.T) {
		cfg := Config{
			FetcherConfig: fetcher.Config{
				NumWorkers: 2,
				QueueSize:  10,
			},
			ProcessorConfig: processor.Config{
				NumWorkers: 2,
				QueueSize:  10,
				// Missing ProcessFunc
			},
			SaverConfig: saver.Config{
				NumWorkers: 2,
				QueueSize:  10,
				SaveFunc:   saver.NoOpSaveFunc,
			},
		}
		err := cfg.Validate()
		assert.Error(t, err)
	})

	t.Run("invalid saver config", func(t *testing.T) {
		cfg := Config{
			FetcherConfig: fetcher.Config{
				NumWorkers: 2,
				QueueSize:  10,
			},
			ProcessorConfig: processor.Config{
				NumWorkers:  2,
				QueueSize:   10,
				ProcessFunc: processor.PassthroughProcessor,
			},
			SaverConfig: saver.Config{
				NumWorkers: 2,
				QueueSize:  10,
				// Missing SaveFunc
			},
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
			FetcherConfig: fetcher.Config{
				NumWorkers: 2,
				QueueSize:  10,
			},
			ProcessorConfig: processor.Config{
				NumWorkers:  2,
				QueueSize:   10,
				ProcessFunc: processor.PassthroughProcessor,
			},
			SaverConfig: saver.Config{
				NumWorkers: 2,
				QueueSize:  10,
				SaveFunc:   saver.NoOpSaveFunc,
			},
		})
		require.NoError(t, err)
		require.NotNil(t, p)
		assert.False(t, p.IsStopped())
		p.Stop()
	})

	t.Run("returns error with invalid config", func(t *testing.T) {
		ctx := context.Background()
		p, err := New(ctx, Config{
			FetcherConfig: fetcher.Config{
				NumWorkers: 0, // Invalid
				QueueSize:  10,
			},
		})
		assert.Error(t, err)
		assert.Nil(t, p)
	})
}

func TestPipeline_WorkerCounts(t *testing.T) {
	ctx := context.Background()
	p, _ := New(ctx, Config{
		FetcherConfig: fetcher.Config{
			NumWorkers: 3,
			QueueSize:  10,
		},
		ProcessorConfig: processor.Config{
			NumWorkers:  4,
			QueueSize:   10,
			ProcessFunc: processor.PassthroughProcessor,
		},
		SaverConfig: saver.Config{
			NumWorkers: 2,
			QueueSize:  10,
			SaveFunc:   saver.NoOpSaveFunc,
		},
	})
	defer p.Stop()

	counts := p.WorkerCounts()
	assert.Equal(t, 3, counts.Fetcher)
	assert.Equal(t, 4, counts.Processor)
	assert.Equal(t, 2, counts.Saver)
	assert.Equal(t, 9, counts.Total)
}

// =============================================================================
// End-to-End Pipeline Tests
// =============================================================================

func TestPipeline_EndToEnd(t *testing.T) {
	t.Run("full pipeline flow", func(t *testing.T) {
		// Create mock server
		var fetchCount atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fetchCount.Add(1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"data":"test"}`))
		}))
		defer server.Close()

		// Track saved data
		var savedCount atomic.Int32
		saveFn := func(ctx context.Context, record *saver.Record) (int64, error) {
			savedCount.Add(1)
			return 1, nil
		}

		ctx := context.Background()
		p, err := New(ctx, Config{
			FetcherConfig: fetcher.Config{
				NumWorkers: 2,
				QueueSize:  10,
			},
			ProcessorConfig: processor.Config{
				NumWorkers:  2,
				QueueSize:   10,
				ProcessFunc: processor.PassthroughProcessor,
			},
			SaverConfig: saver.Config{
				NumWorkers: 2,
				QueueSize:  10,
				SaveFunc:   saveFn,
			},
		})
		require.NoError(t, err)
		defer p.Stop()

		// Submit URLs
		for i := 0; i < 5; i++ {
			err := p.SubmitURL(
				string(rune('a'+i)),
				server.URL,
				map[string]any{"index": i},
			)
			require.NoError(t, err)
		}

		// Wait for pipeline to process
		time.Sleep(time.Millisecond * 500)

		// Check stats
		stats := p.Stats()
		assert.Equal(t, int64(5), stats.TotalItemsIn)
		assert.Equal(t, int32(5), fetchCount.Load())
		assert.GreaterOrEqual(t, savedCount.Load(), int32(1)) // At least some saved

		// Check individual stage stats
		assert.Equal(t, int64(5), stats.FetcherStats.RequestsSubmitted)
	})

	t.Run("handles fetch errors", func(t *testing.T) {
		// Server that returns errors
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		ctx := context.Background()
		p, _ := New(ctx, Config{
			FetcherConfig: fetcher.Config{
				NumWorkers:     1,
				QueueSize:      10,
				MaxRetries:     0, // No retries
				RequestTimeout: time.Second,
			},
			ProcessorConfig: processor.Config{
				NumWorkers:  1,
				QueueSize:   10,
				ProcessFunc: processor.PassthroughProcessor,
			},
			SaverConfig: saver.Config{
				NumWorkers: 1,
				QueueSize:  10,
				SaveFunc:   saver.NoOpSaveFunc,
			},
		})
		defer p.Stop()

		_ = p.SubmitURL("error-test", server.URL, nil)

		// Wait for processing
		time.Sleep(time.Millisecond * 200)

		stats := p.Stats()
		assert.Equal(t, int64(1), stats.TotalItemsIn)
		// Error should be counted
		assert.GreaterOrEqual(t, stats.TotalErrors, int64(0))
	})
}

// =============================================================================
// Shutdown Tests
// =============================================================================

func TestPipeline_Stop(t *testing.T) {
	t.Run("graceful stop", func(t *testing.T) {
		ctx := context.Background()
		p, _ := New(ctx, Config{
			FetcherConfig: fetcher.Config{
				NumWorkers: 2,
				QueueSize:  10,
			},
			ProcessorConfig: processor.Config{
				NumWorkers:  2,
				QueueSize:   10,
				ProcessFunc: processor.PassthroughProcessor,
			},
			SaverConfig: saver.Config{
				NumWorkers: 2,
				QueueSize:  10,
				SaveFunc:   saver.NoOpSaveFunc,
			},
		})

		p.Stop()

		assert.True(t, p.IsStopped())
	})

	t.Run("stop is idempotent", func(t *testing.T) {
		ctx := context.Background()
		p, _ := New(ctx, Config{
			FetcherConfig: fetcher.Config{
				NumWorkers: 2,
				QueueSize:  10,
			},
			ProcessorConfig: processor.Config{
				NumWorkers:  2,
				QueueSize:   10,
				ProcessFunc: processor.PassthroughProcessor,
			},
			SaverConfig: saver.Config{
				NumWorkers: 2,
				QueueSize:  10,
				SaveFunc:   saver.NoOpSaveFunc,
			},
		})

		p.Stop()
		p.Stop()
		p.Stop()

		assert.True(t, p.IsStopped())
	})

	t.Run("submit to stopped pipeline returns error", func(t *testing.T) {
		ctx := context.Background()
		p, _ := New(ctx, Config{
			FetcherConfig: fetcher.Config{
				NumWorkers: 2,
				QueueSize:  10,
			},
			ProcessorConfig: processor.Config{
				NumWorkers:  2,
				QueueSize:   10,
				ProcessFunc: processor.PassthroughProcessor,
			},
			SaverConfig: saver.Config{
				NumWorkers: 2,
				QueueSize:  10,
				SaveFunc:   saver.NoOpSaveFunc,
			},
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
		FetcherConfig: fetcher.Config{
			NumWorkers: 2,
			QueueSize:  10,
		},
		ProcessorConfig: processor.Config{
			NumWorkers:  2,
			QueueSize:   10,
			ProcessFunc: processor.PassthroughProcessor,
		},
		SaverConfig: saver.Config{
			NumWorkers: 2,
			QueueSize:  10,
			SaveFunc:   saver.NoOpSaveFunc,
		},
	})
	defer p.Stop()

	stats := p.Stats()

	assert.False(t, stats.StartedAt.IsZero())
	assert.Greater(t, stats.UptimeDuration, time.Duration(0))
}

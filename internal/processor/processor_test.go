package processor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Configuration Tests
// =============================================================================

func TestConfig_Validate(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := Config{
			NumWorkers:  4,
			QueueSize:   100,
			ProcessFunc: PassthroughProcessor,
		}
		err := cfg.Validate()
		assert.NoError(t, err)
		assert.Equal(t, 60*time.Second, cfg.ProcessTimeout)
		assert.NotNil(t, cfg.Logger)
	})

	t.Run("missing ProcessFunc", func(t *testing.T) {
		cfg := Config{NumWorkers: 4, QueueSize: 10}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ProcessFunc")
	})

	t.Run("zero workers", func(t *testing.T) {
		cfg := Config{NumWorkers: 0, QueueSize: 10, ProcessFunc: PassthroughProcessor}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "NumWorkers")
	})

	t.Run("zero queue size", func(t *testing.T) {
		cfg := Config{NumWorkers: 4, QueueSize: 0, ProcessFunc: PassthroughProcessor}
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
		pool, err := NewPool(ctx, Config{
			NumWorkers:  2,
			QueueSize:   10,
			ProcessFunc: PassthroughProcessor,
		})
		require.NoError(t, err)
		require.NotNil(t, pool)
		assert.Equal(t, 2, pool.NumWorkers())
		pool.Stop()
	})

	t.Run("returns error with invalid config", func(t *testing.T) {
		ctx := context.Background()
		pool, err := NewPool(ctx, Config{
			NumWorkers: 0,
			QueueSize:  10,
		})
		assert.Error(t, err)
		assert.Nil(t, pool)
	})
}

func TestPool_Submit(t *testing.T) {
	t.Run("submit nil input returns error", func(t *testing.T) {
		ctx := context.Background()
		pool, _ := NewPool(ctx, Config{
			NumWorkers:  2,
			QueueSize:   10,
			ProcessFunc: PassthroughProcessor,
		})
		defer pool.Stop()

		err := pool.Submit(nil)
		assert.Error(t, err)
	})

	t.Run("submit to stopped pool returns error", func(t *testing.T) {
		ctx := context.Background()
		pool, _ := NewPool(ctx, Config{
			NumWorkers:  2,
			QueueSize:   10,
			ProcessFunc: PassthroughProcessor,
		})
		pool.Stop()

		err := pool.Submit(&Input{ID: "test"})
		assert.Error(t, err)
		assert.Equal(t, ErrPoolStopped, err)
	})
}

// =============================================================================
// Processing Tests
// =============================================================================

func TestPool_Process(t *testing.T) {
	t.Run("successful processing", func(t *testing.T) {
		ctx := context.Background()
		pool, err := NewPool(ctx, Config{
			NumWorkers: 2,
			QueueSize:  10,
			ProcessFunc: func(ctx context.Context, input *Input) (any, error) {
				// Double the length of input data
				return len(input.Data) * 2, nil
			},
		})
		require.NoError(t, err)
		defer pool.Stop()

		err = pool.Submit(&Input{
			ID:        "test-1",
			SourceURL: "http://example.com",
			Data:      []byte("hello"),
			FetchedAt: time.Now(),
		})
		require.NoError(t, err)

		output := <-pool.Outputs()
		assert.NoError(t, output.Err)
		assert.Equal(t, "test-1", output.InputID)
		assert.Equal(t, "http://example.com", output.SourceURL)
		assert.Equal(t, 10, output.Data) // 5 * 2
		assert.Greater(t, output.Duration, time.Duration(0))
		assert.False(t, output.ProcessedAt.IsZero())
	})

	t.Run("processing failure", func(t *testing.T) {
		ctx := context.Background()
		pool, _ := NewPool(ctx, Config{
			NumWorkers: 1,
			QueueSize:  10,
			ProcessFunc: func(ctx context.Context, input *Input) (any, error) {
				return nil, errors.New("processing error")
			},
		})
		defer pool.Stop()

		_ = pool.Submit(&Input{ID: "fail-test"})

		output := <-pool.Outputs()
		assert.Error(t, output.Err)
		assert.Contains(t, output.Err.Error(), "processing error")
	})

	t.Run("metadata passes through", func(t *testing.T) {
		ctx := context.Background()
		pool, _ := NewPool(ctx, Config{
			NumWorkers:  1,
			QueueSize:   10,
			ProcessFunc: PassthroughProcessor,
		})
		defer pool.Stop()

		_ = pool.Submit(&Input{
			ID:       "meta-test",
			Metadata: map[string]any{"key": "value", "count": 42},
		})

		output := <-pool.Outputs()
		require.NoError(t, output.Err)
		assert.Equal(t, "value", output.Metadata["key"])
		assert.Equal(t, 42, output.Metadata["count"])
	})

	t.Run("fetchedAt passes through", func(t *testing.T) {
		ctx := context.Background()
		pool, _ := NewPool(ctx, Config{
			NumWorkers:  1,
			QueueSize:   10,
			ProcessFunc: PassthroughProcessor,
		})
		defer pool.Stop()

		fetchedAt := time.Now().Add(-time.Hour)
		_ = pool.Submit(&Input{
			ID:        "time-test",
			FetchedAt: fetchedAt,
		})

		output := <-pool.Outputs()
		require.NoError(t, output.Err)
		assert.Equal(t, fetchedAt, output.FetchedAt)
	})
}

// =============================================================================
// Stats Tests
// =============================================================================

func TestPool_Stats(t *testing.T) {
	ctx := context.Background()

	var processCount atomic.Int32
	pool, _ := NewPool(ctx, Config{
		NumWorkers: 2,
		QueueSize:  10,
		ProcessFunc: func(ctx context.Context, input *Input) (any, error) {
			if processCount.Add(1) == 3 {
				return nil, errors.New("fail on 3rd")
			}
			return input.Data, nil
		},
	})
	defer pool.Stop()

	// Submit 5 items
	for i := 0; i < 5; i++ {
		_ = pool.Submit(&Input{
			ID:   string(rune('a' + i)),
			Data: []byte("test"),
		})
	}

	// Drain outputs
	for i := 0; i < 5; i++ {
		<-pool.Outputs()
	}

	stats := pool.Stats().Snapshot()
	assert.Equal(t, int64(5), stats.ItemsSubmitted)
	assert.Equal(t, int64(4), stats.ItemsProcessed)
	assert.Equal(t, int64(1), stats.ItemsFailed)
	assert.Equal(t, int64(20), stats.BytesProcessed) // 5 * 4 bytes
	assert.Greater(t, stats.TotalDuration, time.Duration(0))
}

// =============================================================================
// Shutdown Tests
// =============================================================================

func TestPool_Stop(t *testing.T) {
	t.Run("graceful stop", func(t *testing.T) {
		ctx := context.Background()
		pool, _ := NewPool(ctx, Config{
			NumWorkers: 1,
			QueueSize:  10,
			ProcessFunc: func(ctx context.Context, input *Input) (any, error) {
				time.Sleep(time.Millisecond * 50)
				return input.Data, nil
			},
		})

		_ = pool.Submit(&Input{ID: "slow"})

		// Wait for processing to start
		time.Sleep(time.Millisecond * 20)

		pool.Stop()

		// Output should be available
		output := <-pool.Outputs()
		assert.NoError(t, output.Err)
	})

	t.Run("stop is idempotent", func(t *testing.T) {
		ctx := context.Background()
		pool, _ := NewPool(ctx, Config{
			NumWorkers:  1,
			QueueSize:   10,
			ProcessFunc: PassthroughProcessor,
		})

		pool.Stop()
		pool.Stop()
		pool.Stop()
	})

	t.Run("outputs closed after stop", func(t *testing.T) {
		ctx := context.Background()
		pool, _ := NewPool(ctx, Config{
			NumWorkers:  1,
			QueueSize:   10,
			ProcessFunc: PassthroughProcessor,
		})
		pool.Stop()

		_, ok := <-pool.Outputs()
		assert.False(t, ok)
	})
}

// =============================================================================
// Placeholder Processors Tests
// =============================================================================

func TestPassthroughProcessor(t *testing.T) {
	ctx := context.Background()
	input := &Input{Data: []byte("test data")}

	result, err := PassthroughProcessor(ctx, input)

	assert.NoError(t, err)
	assert.Equal(t, []byte("test data"), result)
}

func TestJSONProcessor(t *testing.T) {
	ctx := context.Background()
	input := &Input{Data: []byte(`{"key":"value"}`)}

	result, err := JSONProcessor(ctx, input)

	// Currently just passes through
	assert.NoError(t, err)
	assert.Equal(t, []byte(`{"key":"value"}`), result)
}

// =============================================================================
// Concurrency Tests
// =============================================================================

func TestPool_Concurrency(t *testing.T) {
	t.Run("processes multiple items concurrently", func(t *testing.T) {
		ctx := context.Background()

		var concurrent atomic.Int32
		var maxConcurrent atomic.Int32

		pool, _ := NewPool(ctx, Config{
			NumWorkers: 4,
			QueueSize:  20,
			ProcessFunc: func(ctx context.Context, input *Input) (any, error) {
				c := concurrent.Add(1)
				defer concurrent.Add(-1)

				for {
					old := maxConcurrent.Load()
					if c <= old || maxConcurrent.CompareAndSwap(old, c) {
						break
					}
				}

				time.Sleep(time.Millisecond * 20)
				return input.Data, nil
			},
		})
		defer pool.Stop()

		// Submit 10 items
		for i := 0; i < 10; i++ {
			_ = pool.Submit(&Input{ID: string(rune('a' + i))})
		}

		// Drain outputs
		for i := 0; i < 10; i++ {
			<-pool.Outputs()
		}

		// Should have had multiple concurrent workers
		assert.GreaterOrEqual(t, maxConcurrent.Load(), int32(2))
	})
}

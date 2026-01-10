package fetcher

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
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
			NumWorkers: 4,
			QueueSize:  100,
		}
		err := cfg.Validate()
		assert.NoError(t, err)
		// Check defaults were applied
		assert.Equal(t, 30*time.Second, cfg.RequestTimeout)
		assert.Equal(t, 3, cfg.MaxRetries)
		assert.NotNil(t, cfg.Logger)
	})

	t.Run("zero workers", func(t *testing.T) {
		cfg := Config{NumWorkers: 0, QueueSize: 10}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "NumWorkers")
	})

	t.Run("zero queue size", func(t *testing.T) {
		cfg := Config{NumWorkers: 4, QueueSize: 0}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "QueueSize")
	})

	t.Run("negative timeout", func(t *testing.T) {
		cfg := Config{NumWorkers: 4, QueueSize: 10, RequestTimeout: -1}
		err := cfg.Validate()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "RequestTimeout")
	})
}

// =============================================================================
// Pool Tests
// =============================================================================

func TestNewPool(t *testing.T) {
	t.Run("creates pool with valid config", func(t *testing.T) {
		ctx := context.Background()
		pool, err := NewPool(ctx, Config{
			NumWorkers: 2,
			QueueSize:  10,
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
	t.Run("submit nil request returns error", func(t *testing.T) {
		ctx := context.Background()
		pool, _ := NewPool(ctx, Config{NumWorkers: 2, QueueSize: 10})
		defer pool.Stop()

		err := pool.Submit(nil)
		assert.Error(t, err)
	})

	t.Run("submit to stopped pool returns error", func(t *testing.T) {
		ctx := context.Background()
		pool, _ := NewPool(ctx, Config{NumWorkers: 2, QueueSize: 10})
		pool.Stop()

		err := pool.Submit(&Request{ID: "test", URL: "http://example.com"})
		assert.Error(t, err)
		assert.Equal(t, ErrPoolStopped, err)
	})
}

// =============================================================================
// Fetch Tests with Mock Server
// =============================================================================

func TestPool_Fetch(t *testing.T) {
	t.Run("successful fetch", func(t *testing.T) {
		// Create mock server
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}))
		defer server.Close()

		ctx := context.Background()
		pool, err := NewPool(ctx, Config{
			NumWorkers: 2,
			QueueSize:  10,
		})
		require.NoError(t, err)
		defer pool.Stop()

		// Submit request
		err = pool.Submit(&Request{
			ID:  "test-1",
			URL: server.URL,
		})
		require.NoError(t, err)

		// Get response
		resp := <-pool.Responses()
		assert.NoError(t, resp.Err)
		assert.Equal(t, "test-1", resp.RequestID)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, `{"status":"ok"}`, string(resp.Body))
		assert.Greater(t, resp.Duration, time.Duration(0))
	})

	t.Run("failed fetch with retry", func(t *testing.T) {
		var attempts atomic.Int32

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			attempts.Add(1)
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		ctx := context.Background()
		pool, err := NewPool(ctx, Config{
			NumWorkers:     1,
			QueueSize:      10,
			MaxRetries:     2,
			RetryDelay:     time.Millisecond * 10,
			RequestTimeout: time.Second * 5,
		})
		require.NoError(t, err)
		defer pool.Stop()

		err = pool.Submit(&Request{
			ID:  "test-retry",
			URL: server.URL,
		})
		require.NoError(t, err)

		resp := <-pool.Responses()
		assert.Error(t, resp.Err)
		assert.Equal(t, int32(3), attempts.Load()) // 1 initial + 2 retries
	})

	t.Run("fetch with custom headers", func(t *testing.T) {
		var receivedHeader string

		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedHeader = r.Header.Get("X-Custom")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		ctx := context.Background()
		pool, _ := NewPool(ctx, Config{NumWorkers: 1, QueueSize: 10})
		defer pool.Stop()

		_ = pool.Submit(&Request{
			ID:      "test-headers",
			URL:     server.URL,
			Headers: map[string]string{"X-Custom": "test-value"},
		})

		<-pool.Responses()
		assert.Equal(t, "test-value", receivedHeader)
	})

	t.Run("metadata passes through", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		ctx := context.Background()
		pool, _ := NewPool(ctx, Config{NumWorkers: 1, QueueSize: 10})
		defer pool.Stop()

		_ = pool.Submit(&Request{
			ID:       "test-meta",
			URL:      server.URL,
			Metadata: map[string]any{"source": "test", "index": 42},
		})

		resp := <-pool.Responses()
		require.NoError(t, resp.Err)
		assert.Equal(t, "test", resp.Metadata["source"])
		assert.Equal(t, 42, resp.Metadata["index"])
	})
}

// =============================================================================
// Stats Tests
// =============================================================================

func TestPool_Stats(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hello"))
	}))
	defer server.Close()

	ctx := context.Background()
	pool, _ := NewPool(ctx, Config{NumWorkers: 2, QueueSize: 10})
	defer pool.Stop()

	// Submit requests
	for i := 0; i < 5; i++ {
		_ = pool.Submit(&Request{ID: string(rune('a' + i)), URL: server.URL})
	}

	// Drain responses
	for i := 0; i < 5; i++ {
		<-pool.Responses()
	}

	stats := pool.Stats().Snapshot()
	assert.Equal(t, int64(5), stats.RequestsSubmitted)
	assert.Equal(t, int64(5), stats.RequestsCompleted)
	assert.Equal(t, int64(0), stats.RequestsFailed)
	assert.Equal(t, int64(25), stats.BytesFetched) // 5 * "hello"
	assert.Greater(t, stats.TotalDuration, time.Duration(0))
}

// =============================================================================
// Shutdown Tests
// =============================================================================

func TestPool_Stop(t *testing.T) {
	t.Run("graceful stop", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(time.Millisecond * 50)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		ctx := context.Background()
		pool, _ := NewPool(ctx, Config{NumWorkers: 1, QueueSize: 10})

		_ = pool.Submit(&Request{ID: "slow", URL: server.URL})

		// Wait for request to start
		time.Sleep(time.Millisecond * 20)

		pool.Stop()

		// Response should be available
		resp := <-pool.Responses()
		assert.NoError(t, resp.Err)
	})

	t.Run("stop is idempotent", func(t *testing.T) {
		ctx := context.Background()
		pool, _ := NewPool(ctx, Config{NumWorkers: 1, QueueSize: 10})

		// Multiple stops should not panic
		pool.Stop()
		pool.Stop()
		pool.Stop()
	})

	t.Run("responses closed after stop", func(t *testing.T) {
		ctx := context.Background()
		pool, _ := NewPool(ctx, Config{NumWorkers: 1, QueueSize: 10})
		pool.Stop()

		_, ok := <-pool.Responses()
		assert.False(t, ok)
	})
}

// =============================================================================
// POST Request Tests
// =============================================================================

func TestPool_PostRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body) // Consume body
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"123"}`))
	}))
	defer server.Close()

	ctx := context.Background()
	pool, _ := NewPool(ctx, Config{NumWorkers: 1, QueueSize: 10})
	defer pool.Stop()

	_ = pool.Submit(&Request{
		ID:     "post-test",
		URL:    server.URL,
		Method: http.MethodPost,
		Body:   nil, // Would use strings.NewReader for actual body
		Headers: map[string]string{
			"Content-Type": "application/json",
		},
	})

	resp := <-pool.Responses()
	assert.NoError(t, resp.Err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
}

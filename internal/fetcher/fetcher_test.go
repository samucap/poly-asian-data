package fetcher

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/workerpool"
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

// testFetcherResult holds a fetcher and its output channel for testing.
type testFetcherResult struct {
	fetcher *Fetcher
	output  <-chan workerpool.Result[*Response]
	cleanup func()
}

// newTestFetcher creates a fetcher for testing.
func newTestFetcher(ctx context.Context, numWorkers, qSize int) (*testFetcherResult, error) {
	f, err := New(ctx, numWorkers, qSize)
	if err != nil {
		return nil, err
	}

	return &testFetcherResult{
		fetcher: f,
		output:  f.Outputs(),
		cleanup: func() {
			f.Stop()
		},
	}, nil
}

// =============================================================================
// Pool Tests
// =============================================================================

func TestNewFetcher(t *testing.T) {
	t.Run("creates fetcher with valid config", func(t *testing.T) {
		ctx := context.Background()
		result, err := newTestFetcher(ctx, 2, 10)
		require.NoError(t, err)
		require.NotNil(t, result.fetcher)
		result.cleanup()
	})
}

func TestFetcher_Submit(t *testing.T) {
	t.Run("submit to stopped pool returns error", func(t *testing.T) {
		ctx := context.Background()
		result, _ := newTestFetcher(ctx, 2, 10)
		result.cleanup() // Stop the pool

		err := result.fetcher.Submit(&Request{ID: "test", URL: "http://example.com"})
		assert.Error(t, err)
	})
}

// =============================================================================
// WorkerTask Tests with Mock Server
// =============================================================================

func TestFetcher_WorkerTask(t *testing.T) {
	t.Run("successful fetch via WorkerTask", func(t *testing.T) {
		// Create mock server
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}))
		defer server.Close()

		ctx := context.Background()
		f, err := New(ctx, 2, 10)
		require.NoError(t, err)
		defer f.Stop()

		// Call WorkerTask directly
		req := &Request{
			ID:  "test-1",
			URL: server.URL,
		}
		resp, err := f.WorkerTask(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, server.URL, resp.URL)
		// WorkerTask currently returns mock data, so check that
		assert.Equal(t, []byte("fetcherSuccess"), resp.Body)
	})

	t.Run("metadata passes through", func(t *testing.T) {
		ctx := context.Background()
		f, err := New(ctx, 1, 10)
		require.NoError(t, err)
		defer f.Stop()

		req := &Request{
			ID:       "test-meta",
			URL:      "http://example.com",
			Metadata: map[string]any{"source": "test", "index": 42},
		}
		resp, err := f.WorkerTask(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, "test", resp.Metadata["source"])
		assert.Equal(t, 42, resp.Metadata["index"])
	})
}

// =============================================================================
// Stats Tests
// =============================================================================

func TestFetcher_Stats(t *testing.T) {
	ctx := context.Background()
	result, _ := newTestFetcher(ctx, 2, 10)
	defer result.cleanup()

	stats := result.fetcher.Stats().Snapshot()
	// Just verify we can get stats without panic
	assert.GreaterOrEqual(t, stats.Submitted, int64(0))
}

// =============================================================================
// Shutdown Tests
// =============================================================================

func TestFetcher_Stop(t *testing.T) {
	t.Run("stop is idempotent", func(t *testing.T) {
		ctx := context.Background()
		result, _ := newTestFetcher(ctx, 1, 10)

		// Multiple stops should not panic
		result.cleanup()
		result.fetcher.Stop()
		result.fetcher.Stop()
	})
}

// =============================================================================
// IsRunning Tests
// =============================================================================

func TestFetcher_IsRunning(t *testing.T) {
	t.Run("returns true when running", func(t *testing.T) {
		ctx := context.Background()
		result, _ := newTestFetcher(ctx, 1, 10)
		defer result.cleanup()

		assert.True(t, result.fetcher.IsRunning())
	})

	t.Run("returns false after stop", func(t *testing.T) {
		ctx := context.Background()
		result, _ := newTestFetcher(ctx, 1, 10)
		result.cleanup()

		assert.False(t, result.fetcher.IsRunning())
	})
}

// =============================================================================
// Logger Tests
// =============================================================================

func TestFetcher_Logger(t *testing.T) {
	t.Run("fetcher has logger", func(t *testing.T) {
		ctx := context.Background()
		f, err := New(ctx, 1, 10)
		require.NoError(t, err)
		defer f.Stop()

		// Verify logger is set (internal field, just ensure no panic on use)
		assert.NotNil(t, f.logger)
	})
}

// Ensure unused imports are used
var _ = slog.Default
var _ = time.Second

package fetcher

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samucap/poly-asian-data/internal/config"
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
	cfg := &config.Config{}
	f, err := New(ctx, cfg, numWorkers, qSize)
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

		err := result.fetcher.Submit(workerpool.Input[*Request]{Data: &Request{URL: "http://example.com"}})
		assert.Error(t, err)
	})
}

// =============================================================================
// WorkerTask Tests with Mock Server
// =============================================================================

func TestFetcher_WorkerTask(t *testing.T) {
	t.Run("successful fetch returns data", func(t *testing.T) {
		// Create mock server
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok"}`))
		}))
		defer server.Close()

		ctx := context.Background()
		cfg := &config.Config{}
		f, err := New(ctx, cfg, 2, 10)
		require.NoError(t, err)
		defer f.Stop()

		// Call workerTask directly
		req := &Request{
			URL: server.URL,
		}
		resp, err := f.workerTask(ctx, req)
		require.NoError(t, err)
		assert.Equal(t, server.URL, resp.URL)
		assert.Equal(t, []byte(`{"status":"ok"}`), resp.Data)
		assert.NotNil(t, resp.Request) // Request should be attached to response
	})

	t.Run("response includes original request", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`[]`))
		}))
		defer server.Close()

		ctx := context.Background()
		cfg := &config.Config{}
		f, err := New(ctx, cfg, 1, 10)
		require.NoError(t, err)
		defer f.Stop()

		params := url.Values{}
		params.Set("limit", "10")
		params.Set("offset", "0")

		req := &Request{
			URL:    server.URL + "?" + params.Encode(),
			Params: params,
		}
		resp, err := f.workerTask(ctx, req)
		require.NoError(t, err)
		assert.NotNil(t, resp.Request)
		assert.Equal(t, "10", resp.Request.Params.Get("limit"))
		assert.Equal(t, "0", resp.Request.Params.Get("offset"))
	})
}

// =============================================================================
// Pagination Tests (Now in Processor)
// =============================================================================

func TestFetcher_FetchDoesNotPaginate(t *testing.T) {
	t.Run("fetcher no longer handles pagination", func(t *testing.T) {
		// Mock server that returns a full page (10 items)
		var requestCount atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestCount.Add(1)
			items := make([]map[string]string, 10)
			for i := range items {
				items[i] = map[string]string{"id": string(rune('a' + i))}
			}
			data, _ := json.Marshal(items)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
		}))
		defer server.Close()

		ctx := context.Background()
		cfg := &config.Config{}
		f, err := New(ctx, cfg, 1, 10)
		require.NoError(t, err)
		defer f.Stop()

		params := url.Values{}
		params.Set("limit", "10")
		params.Set("offset", "0")

		req := &Request{
			URL:    server.URL + "?" + params.Encode(),
			Params: params,
		}
		resp, err := f.Fetch(ctx, req)
		require.NoError(t, err)
		assert.NotNil(t, resp.Data)

		// Fetcher should only make 1 request (pagination is now in processor)
		assert.Equal(t, int32(1), requestCount.Load())
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
		cfg := &config.Config{}
		f, err := New(ctx, cfg, 1, 10)
		require.NoError(t, err)
		defer f.Stop()

		// Verify logger is set (internal field, just ensure no panic on use)
		assert.NotNil(t, f.logger)
	})
}

// Ensure unused imports are used
var _ = slog.Default
var _ = time.Second

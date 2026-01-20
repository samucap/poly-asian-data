// Package fetcher provides a secure HTTP client wrapper for fetching data.
// Uses the generic workerpool.Pool for worker management.
package fetcher

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/workerpool"
)

// =============================================================================
// Error Definitions
// =============================================================================

var (
	ErrPoolStopped   = errors.New("fetcher pool has been stopped")
	ErrInvalidConfig = errors.New("invalid fetcher configuration")
	ErrRequestFailed = errors.New("request failed")
)

// =============================================================================
// Type Definitions
// =============================================================================
type Fetcher struct {
	*workerpool.Pool[*Request, *Response]
	httpClient *http.Client
	logger     *slog.Logger
}

// Request represents a fetch request.
type Request struct {
	URL      string
	Method   string
	Headers  map[string]string
	Body     io.Reader
}

// Response represents the result of a fetch.
type Response struct {
	URL      string
	Data     []byte
	Duration time.Duration
	Err      error
}

// =============================================================================
// HTTP Client
// =============================================================================

func newSecureHTTPClient() *http.Client {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS13,
	}

	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		ForceAttemptHTTP2:   true,
	}

	return &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}
}

// =============================================================================
// Configuration
// =============================================================================

// New creates and initializes a fetcher pool.
// Validates config and sets up resources (HTTP client, logger, output channel).
func New(ctx context.Context, numWorkers, qSize int) (*Fetcher, error) {
	logger := logging.Logger.With(
		slog.String("component", "fetcher"),
	)

	// Create fetcher first so we can pass its method to the pool
	f := &Fetcher{
		httpClient: newSecureHTTPClient(),
		logger:     logger,
	}

	pool, err := workerpool.NewPool[*Request, *Response](ctx, "fetcher", numWorkers, qSize, logger, f.workerTask)
	if err != nil {
		return nil, err
	}

	f.Pool = pool
	return f, nil
}

func (f *Fetcher) workerTask(ctx context.Context, req *Request) (*Response, error) {
	f.logger.Info("fetching url",
		slog.String("url", req.URL),
	)

	time.Sleep(10 * time.Millisecond) // Placeholder delay
	return &Response{
		URL:      req.URL,
		Duration: 10 * time.Millisecond,
		Data:     []byte("fetcherSuccess"),
	}, nil
}

// GetPlyMktSubgraphs fetches Polymarket subgraph data.
func (f *Fetcher) GetPlyMktSubgraphs(ctx context.Context) (*Response, error) {
	input := workerpool.Input[*Request]{
		Data: &Request{
			URL:    "https://api.thegraph.com/subgraphs/name/polymarket/polymarket-matic",
			Method: "POST",
		},
		SubmittedAt: time.Now(),
	}

	// Submit to the pool
	if err := f.Submit(input); err != nil {
		return nil, err
	}

	// Wait for result from output channel
	select {
	case result := <-f.Outputs():
		if result.Err != nil {
			return nil, result.Err
		}
		return result.Value, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
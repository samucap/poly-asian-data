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
	"net/url"
	"time"
	"fmt"

	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/workerpool"
	"github.com/samucap/poly-asian-data/internal/config"
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
	cfg        *config.Config
}

// Request represents a fetch request.
type Request struct {
	URL      string
	Method   string
	Headers  map[string]string
	Body     io.Reader
	Params url.Values
}

// Response represents the result of a fetch.
type Response struct {
	URL      string
	Data     []byte
	Duration time.Duration
	Err      error
}

// =============================================================================
// Configuration
// =============================================================================

// New creates and initializes a fetcher pool.
// Validates config and sets up resources (HTTP client, logger, output channel).
func New(ctx context.Context, config *config.Config, numWorkers, qSize int) (*Fetcher, error) {
	logger := logging.Logger.With(
		slog.String("component", "fetcher"),
	)

	// Create fetcher first so we can pass its method to the pool
	f := &Fetcher{
		httpClient: newSecureHTTPClient(),
		cfg:        config,
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

	return f.Fetch(ctx, req)
}

func (f *Fetcher) doRequest(ctx context.Context, httpReq *http.Request) (*Response, error) {
	// TODO: 1 per host, prolly need to abstract this out
	httpClient := newSecureHTTPClient()
	httpResp, err := httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response: %w", err)
	}

	if httpResp.StatusCode >= 500 {
		return nil, fmt.Errorf("server error: %d", httpResp.StatusCode)
	}

	return &Response{
		Data: body,
	}, nil
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
		Timeout:   2 * time.Second,
	}
}

// =============================================================================
// FetcherJob Interface - Work Function
// =============================================================================

// Fetch is the work function that fetches data from a URL.
// This is the domain-specific operation for the fetcher stage.
func (f *Fetcher) Fetch(ctx context.Context, inputReqDetails *Request) (*Response, error) {
	const (
		maxRetries = 3
		retryDelay = 1 * time.Second
	)

	start := time.Now()
	method := inputReqDetails.Method
	if method == "" {
		method = http.MethodGet
	}

	f.logger.Info("fetching url",
		slog.String("url", inputReqDetails.URL),
	)

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			delay := retryDelay * time.Duration(1<<(attempt-1))
			select {
			case <-ctx.Done():
				return &Response{
					URL:      inputReqDetails.URL,
					Duration: time.Since(start),
					Err:      ctx.Err(),
				}, ctx.Err()
			case <-time.After(delay):
			}
		}

		httpReq, err := http.NewRequestWithContext(ctx, inputReqDetails.Method, inputReqDetails.URL, inputReqDetails.Body)
		if err != nil {
			return nil, fmt.Errorf("creating http request: %w", err)
		}

		for k, v := range inputReqDetails.Headers {
			httpReq.Header.Set(k, v)
		}

		resp, err := f.doRequest(ctx, httpReq)
		if err == nil {
			resp.Duration = time.Since(start)

			f.logger.Info("fetch completed",
				slog.String("url", inputReqDetails.URL),
				slog.Int("bytes", len(resp.Data)),
			)
			return resp, nil
		}
		lastErr = err
	}

	duration := time.Since(start)

	f.logger.Error("fetch failed",
		slog.String("url", inputReqDetails.URL),
		slog.String("error", lastErr.Error()),
	)

	return &Response{
		URL:      inputReqDetails.URL,
		Duration: duration,
		Err:      fmt.Errorf("%w: %v", ErrRequestFailed, lastErr),
	}, lastErr
}
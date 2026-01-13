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
	ID       string
	URL      string
	Method   string
	Headers  map[string]string
	Body     io.Reader
	Metadata map[string]any
}

// Response represents the result of a fetch.
type Response struct {
	URL        string
	StatusCode int
	Body       []byte
	Headers    http.Header
	Duration   time.Duration
	Err        error
	Metadata   map[string]any
}

// =============================================================================
// Configuration
// =============================================================================

// // Config holds fetcher configuration.
// type Config struct {
// 	RequestTimeout  time.Duration
// 	MaxRetries      int
// 	RetryDelay      time.Duration
// 	MaxIdleConns    int
// 	IdleConnTimeout time.Duration
// 	Logger          *slog.Logger
// }

// // Validate checks the configuration.
// func (c *Config) Validate() error {
// 	if c.NumWorkers < 1 {
// 		return fmt.Errorf("%w: NumWorkers must be >= 1", ErrInvalidConfig)
// 	}
// 	if c.QueueSize < 1 {
// 		return fmt.Errorf("%w: QueueSize must be >= 1", ErrInvalidConfig)
// 	}
// 	if c.RequestTimeout < 0 {
// 		return fmt.Errorf("%w: RequestTimeout cannot be negative", ErrInvalidConfig)
// 	}
// 	if c.RequestTimeout == 0 {
// 		c.RequestTimeout = 30 * time.Second
// 	}
// 	if c.MaxRetries == 0 {
// 		c.MaxRetries = 3
// 	}
// 	if c.RetryDelay == 0 {
// 		c.RetryDelay = 100 * time.Millisecond
// 	}
// 	if c.MaxIdleConns == 0 {
// 		c.MaxIdleConns = 100
// 	}
// 	if c.IdleConnTimeout == 0 {
// 		c.IdleConnTimeout = 90 * time.Second
// 	}
// 	if c.Logger == nil {
// 		c.Logger = slog.Default()
// 	}
// 	return nil
// }

// New creates and initializes a fetcher pool.
// Validates config and sets up resources (HTTP client, logger, output channel).
func New(ctx context.Context, numWorkers, qSize int) (*Fetcher, error) {
	logger := logging.Logger.With(
		slog.String("component", "fetcher"),
	)

	pool, err := workerpool.NewPool[*Request, *Response](ctx, "fetcher", numWorkers, qSize)
	if err != nil {
		return nil, err
	}

	f := &Fetcher{
		Pool:       pool,
		httpClient: newSecureHTTPClient(),
		logger:     logger,
	}

	return f, nil
}

// IsRunning returns true if the pool is currently running.
func (f *Fetcher) IsRunning() bool {
	return f.Pool != nil && !f.Pool.IsStopped()
}

// =============================================================================
// FetcherJob Interface - Work Function
// =============================================================================

// // Fetch is the work function that fetches data from a URL.
// // This is the domain-specific operation for the fetcher stage.
// func (f *Fetcher) Fetch(ctx context.Context, req *Request) (*Response, error) {
// 	start := time.Now()
// 	method := req.Method
// 	if method == "" {
// 		method = http.MethodGet
// 	}
//
// 	f.logger.Info("fetching url",
// 		slog.String("request_id", req.ID),
// 		slog.String("url", req.URL),
// 	)
//
// 	var lastErr error
// 	for attempt := 0; attempt <= f.config.MaxRetries; attempt++ {
// 		if attempt > 0 {
// 			f.stats.RetryCount.Add(1)
// 			delay := f.config.RetryDelay * time.Duration(1<<(attempt-1))
// 			select {
// 			case <-ctx.Done():
// 				return &Response{
// 					URL:      req.URL,
// 					Duration: time.Since(start),
// 					Err:      ctx.Err(),
// 					Metadata: req.Metadata,
// 				}, ctx.Err()
// 			case <-time.After(delay):
// 			}
// 		}
//
// 		resp, err := f.doRequest(ctx, req, method)
// 		if err == nil {
// 			resp.Duration = time.Since(start)
// 			f.stats.RequestsCompleted.Add(1)
// 			f.stats.BytesFetched.Add(int64(len(resp.Body)))
// 			f.stats.TotalDuration.Add(int64(resp.Duration))
//
// 			f.logger.Info("fetched from url",
// 				slog.String("request_id", req.ID),
// 				slog.String("url", req.URL),
// 				slog.Int("status", resp.StatusCode),
// 			)
// 			return resp, nil
// 		}
// 		lastErr = err
// 	}
//
// 	duration := time.Since(start)
// 	f.stats.RequestsFailed.Add(1)
// 	f.stats.TotalDuration.Add(int64(duration))
//
// 	f.logger.Error("fetch failed",
// 		slog.String("request_id", req.ID),
// 		slog.String("url", req.URL),
// 		slog.String("error", lastErr.Error()),
// 	)
//
// 	return &Response{
// 		URL:      req.URL,
// 		Duration: duration,
// 		Err:      fmt.Errorf("%w: %v", ErrRequestFailed, lastErr),
// 		Metadata: req.Metadata,
// 	}, lastErr
// }

// func (f *Fetcher) doRequest(ctx context.Context, req *Request, method string) (*Response, error) {
// 	httpReq, err := http.NewRequestWithContext(ctx, method, req.URL, req.Body)
// 	if err != nil {
// 		return nil, fmt.Errorf("creating request: %w", err)
// 	}
//
// 	for k, v := range req.Headers {
// 		httpReq.Header.Set(k, v)
// 	}
//
// 	httpResp, err := f.httpClient.Do(httpReq)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer httpResp.Body.Close()
//
// 	body, err := io.ReadAll(httpResp.Body)
// 	if err != nil {
// 		return nil, fmt.Errorf("reading response: %w", err)
// 	}
//
// 	if httpResp.StatusCode >= 500 {
// 		return nil, fmt.Errorf("server error: %d", httpResp.StatusCode)
// 	}
//
// 	return &Response{
// 		URL:        req.URL,
// 		StatusCode: httpResp.StatusCode,
// 		Body:       body,
// 		Headers:    httpResp.Header,
// 		Metadata:   req.Metadata,
// 	}, nil
// }

// =============================================================================
// HTTP Client
// =============================================================================

func newSecureHTTPClient() *http.Client {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
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

func (f *Fetcher) WorkerTask(ctx context.Context, input *Request) (*Response, error) {
	f.logger.Info("fetching url",
		slog.String("url", input.URL),
	)

	time.Sleep(10 * time.Millisecond) // Placeholder delay
	return &Response{
		URL:      input.URL,
		Duration: 10 * time.Millisecond,
		Body:     []byte("fetcherSuccess"),
		Err:      nil,
		Metadata: input.Metadata,
	}, nil
}

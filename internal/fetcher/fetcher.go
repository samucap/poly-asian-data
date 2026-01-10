// Package fetcher provides a secure HTTP client wrapper for fetching data.
// Uses the generic workerpool.Pool for worker management.
package fetcher

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

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
	RequestID  string
	URL        string
	StatusCode int
	Body       []byte
	Headers    http.Header
	Duration   time.Duration
	Err        error
	Metadata   map[string]any
}

// Stats contains atomic counters.
type Stats struct {
	RequestsSubmitted atomic.Int64
	RequestsCompleted atomic.Int64
	RequestsFailed    atomic.Int64
	BytesFetched      atomic.Int64
	TotalDuration     atomic.Int64
	RetryCount        atomic.Int64
}

// Snapshot returns a point-in-time copy.
func (s *Stats) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		RequestsSubmitted: s.RequestsSubmitted.Load(),
		RequestsCompleted: s.RequestsCompleted.Load(),
		RequestsFailed:    s.RequestsFailed.Load(),
		BytesFetched:      s.BytesFetched.Load(),
		TotalDuration:     time.Duration(s.TotalDuration.Load()),
		RetryCount:        s.RetryCount.Load(),
	}
}

// StatsSnapshot is an immutable snapshot.
type StatsSnapshot struct {
	RequestsSubmitted int64
	RequestsCompleted int64
	RequestsFailed    int64
	BytesFetched      int64
	TotalDuration     time.Duration
	RetryCount        int64
}

// =============================================================================
// Configuration
// =============================================================================

// Config holds fetcher configuration.
type Config struct {
	NumWorkers      int
	QueueSize       int
	RequestTimeout  time.Duration
	MaxRetries      int
	RetryDelay      time.Duration
	MaxIdleConns    int
	IdleConnTimeout time.Duration
	Logger          *slog.Logger
}

// Validate checks the configuration.
func (c *Config) Validate() error {
	if c.NumWorkers < 1 {
		return fmt.Errorf("%w: NumWorkers must be >= 1", ErrInvalidConfig)
	}
	if c.QueueSize < 1 {
		return fmt.Errorf("%w: QueueSize must be >= 1", ErrInvalidConfig)
	}
	if c.RequestTimeout < 0 {
		return fmt.Errorf("%w: RequestTimeout cannot be negative", ErrInvalidConfig)
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 30 * time.Second
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = 3
	}
	if c.RetryDelay == 0 {
		c.RetryDelay = 100 * time.Millisecond
	}
	if c.MaxIdleConns == 0 {
		c.MaxIdleConns = 100
	}
	if c.IdleConnTimeout == 0 {
		c.IdleConnTimeout = 90 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return nil
}

// =============================================================================
// Pool Implementation
// =============================================================================

// Pool is a fetcher pool using generic workerpool.Pool.
type Pool struct {
	pool    *workerpool.Pool[*Request, *Response]
	config  Config
	stats   Stats
	logger  *slog.Logger
	client  *http.Client
	ctx     context.Context
	cancel  context.CancelFunc
	stopped atomic.Bool

	// Cached responses channel
	responses chan *Response
}

// NewPool creates a new fetcher pool.
func NewPool(ctx context.Context, config Config) (*Pool, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}

	poolCtx, cancel := context.WithCancel(ctx)

	f := &Pool{
		config:    config,
		logger:    config.Logger.With(slog.String("component", "fetcher")),
		client:    newSecureHTTPClient(config),
		ctx:       poolCtx,
		cancel:    cancel,
		responses: make(chan *Response, config.QueueSize),
	}

	pool, err := workerpool.NewPool(poolCtx, workerpool.Config{
		Name:       "fetcher",
		NumWorkers: config.NumWorkers,
		QueueSize:  config.QueueSize,
		Logger:     config.Logger,
	}, f.fetch)

	if err != nil {
		cancel()
		return nil, err
	}

	f.pool = pool

	// Start result forwarder
	go f.forwardResults()

	return f, nil
}

// forwardResults forwards pool outputs to the cached responses channel.
func (f *Pool) forwardResults() {
	defer close(f.responses)
	for result := range f.pool.Outputs() {
		if result.Value != nil {
			f.responses <- result.Value
		} else if result.Err != nil {
			f.responses <- &Response{Err: result.Err, Duration: result.Duration}
		}
	}
}

// fetch is the process function.
func (f *Pool) fetch(ctx context.Context, req *Request) (*Response, error) {
	start := time.Now()
	method := req.Method
	if method == "" {
		method = http.MethodGet
	}

	f.logger.Info("fetching url",
		slog.String("request_id", req.ID),
		slog.String("url", req.URL),
	)

	var lastErr error
	for attempt := 0; attempt <= f.config.MaxRetries; attempt++ {
		if attempt > 0 {
			f.stats.RetryCount.Add(1)
			delay := f.config.RetryDelay * time.Duration(1<<(attempt-1))
			select {
			case <-ctx.Done():
				return &Response{
					RequestID: req.ID,
					URL:       req.URL,
					Duration:  time.Since(start),
					Err:       ctx.Err(),
					Metadata:  req.Metadata,
				}, ctx.Err()
			case <-time.After(delay):
			}
		}

		resp, err := f.doRequest(ctx, req, method)
		if err == nil {
			resp.Duration = time.Since(start)
			f.stats.RequestsCompleted.Add(1)
			f.stats.BytesFetched.Add(int64(len(resp.Body)))
			f.stats.TotalDuration.Add(int64(resp.Duration))

			f.logger.Info("fetched from url",
				slog.String("request_id", req.ID),
				slog.String("url", req.URL),
				slog.Int("status", resp.StatusCode),
			)
			return resp, nil
		}
		lastErr = err
	}

	duration := time.Since(start)
	f.stats.RequestsFailed.Add(1)
	f.stats.TotalDuration.Add(int64(duration))

	f.logger.Error("fetch failed",
		slog.String("request_id", req.ID),
		slog.String("url", req.URL),
		slog.String("error", lastErr.Error()),
	)

	return &Response{
		RequestID: req.ID,
		URL:       req.URL,
		Duration:  duration,
		Err:       fmt.Errorf("%w: %v", ErrRequestFailed, lastErr),
		Metadata:  req.Metadata,
	}, lastErr
}

func (f *Pool) doRequest(ctx context.Context, req *Request, method string) (*Response, error) {
	httpReq, err := http.NewRequestWithContext(ctx, method, req.URL, req.Body)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	for k, v := range req.Headers {
		httpReq.Header.Set(k, v)
	}

	httpResp, err := f.client.Do(httpReq)
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
		RequestID:  req.ID,
		URL:        req.URL,
		StatusCode: httpResp.StatusCode,
		Body:       body,
		Headers:    httpResp.Header,
		Metadata:   req.Metadata,
	}, nil
}

// Submit adds a request to the pool.
func (f *Pool) Submit(req *Request) error {
	if req == nil {
		return errors.New("request cannot be nil")
	}
	if f.stopped.Load() {
		return ErrPoolStopped
	}
	f.stats.RequestsSubmitted.Add(1)
	return f.pool.SubmitWait(req)
}

// Responses returns the cached responses channel.
func (f *Pool) Responses() <-chan *Response {
	return f.responses
}

// Stats returns statistics.
func (f *Pool) Stats() *Stats {
	return &f.stats
}

// NumWorkers returns worker count.
func (f *Pool) NumWorkers() int {
	return f.config.NumWorkers
}

// Stop gracefully shuts down.
func (f *Pool) Stop() {
	f.stopped.Store(true)
	f.pool.Stop()
	f.cancel()

	stats := f.stats.Snapshot()
	f.logger.Info("fetcher pool stopped",
		slog.Int64("requests_completed", stats.RequestsCompleted),
		slog.Int64("requests_failed", stats.RequestsFailed),
		slog.Int64("bytes_fetched", stats.BytesFetched),
	)
}

// StopNow immediately stops.
func (f *Pool) StopNow() {
	f.stopped.Store(true)
	f.pool.StopNow()
	f.cancel()
}

// =============================================================================
// HTTP Client
// =============================================================================

func newSecureHTTPClient(cfg Config) *http.Client {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        cfg.MaxIdleConns,
		MaxIdleConnsPerHost: cfg.MaxIdleConns,
		IdleConnTimeout:     cfg.IdleConnTimeout,
		TLSHandshakeTimeout: 10 * time.Second,
		ForceAttemptHTTP2:   true,
	}

	return &http.Client{
		Transport: transport,
		Timeout:   cfg.RequestTimeout,
	}
}

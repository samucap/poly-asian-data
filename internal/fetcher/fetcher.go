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
	"net/url"
	"strconv"
	"time"

	"github.com/samucap/poly-asian-data/internal/config"
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
	cfg        *config.Config
}

// Request represents a fetch request.
type Request struct {
	URL      string
	Method   string
	Headers  map[string]string
	Body     io.Reader
	Params   url.Values
}

// Response represents the result of a fetch.
type Response struct {
	URL      string
	Data     []byte
	Duration time.Duration
	Err      error
	Request  *Request // Original request (for processor to handle pagination)
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

// Fetch is the work function that fetches data from a URL.
// This is the domain-specific operation for the fetcher stage.
// Note: Pagination logic has been moved to the processor.
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
					Request:  inputReqDetails,
				}, ctx.Err()
			case <-time.After(delay):
			}
		}

		httpReq, err := http.NewRequestWithContext(ctx, method, inputReqDetails.URL, inputReqDetails.Body)
		if err != nil {
			return nil, fmt.Errorf("creating http request: %w", err)
		}

		for k, v := range inputReqDetails.Headers {
			httpReq.Header.Set(k, v)
		}

		resp, err := f.doRequest(ctx, httpReq)
		if err == nil {
			resp.URL = inputReqDetails.URL
			resp.Duration = time.Since(start)
			resp.Request = inputReqDetails

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
		Request:  inputReqDetails,
	}, lastErr
}

// =============================================================================
// Pagination
// =============================================================================

// BuildNextPageRequest returns the next page request if pagination should continue,
// or nil if we've reached the last page. The itemCount is needed to determine
// if the current page was full (more pages) or partial (last page).
func (f *Fetcher) BuildNextPageRequest(req *Request, itemCount int) *Request {
	if req == nil || req.Params == nil {
		return nil
	}

	// Check if pagination params exist
	limitStr := req.Params.Get("limit")
	offsetStr := req.Params.Get("offset")
	if limitStr == "" || offsetStr == "" {
		return nil // Not a paginated request
	}

	limit, err := strconv.Atoi(limitStr)
	if err != nil {
		return nil
	}

	// If we got fewer items than the limit, we've reached the last page
	if itemCount < limit {
		f.logger.Info("reached last page",
			slog.String("url", req.URL),
			slog.Int("itemCount", itemCount),
			slog.Int("limit", limit),
		)
		return nil
	}

	// Full page - build next page request
	offset, err := strconv.Atoi(offsetStr)
	if err != nil {
		return nil
	}

	newOffset := strconv.Itoa(offset + limit)

	// Deep copy Params to avoid mutation issues
	newParams := make(url.Values)
	for k, v := range req.Params {
		newParams[k] = append([]string{}, v...)
	}
	newParams.Set("offset", newOffset)

	// Rebuild URL with new offset
	parsedURL, err := url.Parse(req.URL)
	if err != nil {
		return nil
	}
	parsedURL.RawQuery = newParams.Encode()

	nextReq := &Request{
		URL:     parsedURL.String(),
		Method:  req.Method,
		Headers: req.Headers,
		Params:  newParams,
	}

	f.logger.Info("built next page request",
		slog.String("url", nextReq.URL),
		slog.Int("newOffset", offset+limit),
	)

	return nextReq
}
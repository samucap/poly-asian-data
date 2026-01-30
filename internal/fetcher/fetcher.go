// Package fetcher provides a secure HTTP client wrapper for fetching data.
// Uses the generic workerpool.Pool for worker management.
package fetcher

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
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
	Metadata map[string]string // Context for processor (e.g. SportSlug)
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
	logAttrs := []any{
		slog.String("url", req.URL),
	}
	if req.Metadata != nil {
		if entity, ok := req.Metadata["Entity"]; ok {
			logAttrs = append(logAttrs, slog.String("entity", entity))
		}
		if typ, ok := req.Metadata["Type"]; ok {
			logAttrs = append(logAttrs, slog.String("type", typ))
		}
	}

	f.logger.Info("fetching url", logAttrs...)

	return f.Fetch(ctx, req)
}

func (f *Fetcher) doRequest(ctx context.Context, httpReq *http.Request) (*Response, error) {
	// Use the shared http client. The Transport handles connection pooling per host.
	httpResp, err := f.httpClient.Do(httpReq)
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
		Timeout:   30 * time.Second,
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

			// Rewind body if possible for retry
			if inputReqDetails.Body != nil {
				if seeker, ok := inputReqDetails.Body.(io.Seeker); ok {
					seeker.Seek(0, io.SeekStart)
				}
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
		
		// Error occurred (timeout or status 500+)
		// Check for adaptive batch sizing if this is a GraphQL query
		if varsJSON, ok := inputReqDetails.Metadata["GraphqlVariables"]; ok {
			var vars map[string]any
			if jsonErr := json.Unmarshal([]byte(varsJSON), &vars); jsonErr == nil {
				// Check for 'first' parameter
				if firstVal, ok := vars["first"]; ok {
					// Handle float64 (json default) or int
					var currentLimit int
					switch v := firstVal.(type) {
					case float64:
						currentLimit = int(v)
					case int:
						currentLimit = v
					}

					// Only reduce if we have room (e.g., > 10 items)
					if currentLimit > 10 {
						// Reduce by 20%
						newLimit := int(float64(currentLimit) * 0.8)
						if newLimit < 1 {
							newLimit = 1
						}
						
						f.logger.Warn("reducing batch size due to error", 
							slog.String("error", err.Error()),
							slog.Int("oldLimit", currentLimit),
							slog.Int("newLimit", newLimit),
							slog.Int("attempt", attempt),
						)
						
						// Update variables
						vars["first"] = newLimit
						
						// Update Body and Metadata
						// We need to re-construct the full body ("query", "variables")
						// We can get the query from metadata or parsing original body?
						// Better to rely on metadata "GraphqlQuery" as standardized in plymkt/processor
						if query, qOk := inputReqDetails.Metadata["GraphqlQuery"]; qOk {
							newBodyData := map[string]any{
								"query": query,
								"variables": vars,
							}
							
							if newBodyBytes, mErr := json.Marshal(newBodyData); mErr == nil {
								// Update Request
								inputReqDetails.Body = bytes.NewReader(newBodyBytes)
								
								// Update Metadata for next retry/pagination preservation
								if newVarsBytes, vErr := json.Marshal(vars); vErr == nil {
									inputReqDetails.Metadata["GraphqlVariables"] = string(newVarsBytes)
								}
							}
						}
					}
				}
			}
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

	parsedURL, err := url.Parse(req.URL)
	if err != nil {
		f.logger.Error("failed to parse pagination url", slog.String("url", req.URL), slog.String("error", err.Error()))
		return nil
	}

	var newBody io.Reader
	// If it's a GraphQL request (indicated by GraphqlQuery metadata), regenerate the body
	// And crucially, DO NOT append query params to the URL (keep it clean)
	if query, ok := req.Metadata["GraphqlQuery"]; ok {
		// Construct JSON body with variables
		bodyData := map[string]any{
			"query": query,
			"variables": map[string]int{
				"first": limit,
				"skip":  offset + limit,
			},
		}
		b, err := json.Marshal(bodyData)
		if err == nil {
			newBody = bytes.NewReader(b)
		} else {
            f.logger.Error("failed to marshal graphql body", slog.String("error", err.Error()))
            return nil
        }
	} else {
		// For standard REST requests, update the URL query params
		parsedURL.RawQuery = newParams.Encode()
	}
		// For standard requests, we might need to reset the body if it was read?
		// But usually GET params handle pagination.
		// If it's a POST with a body that stays static, we'd need to re-read it.
		// `req.Body` is an io.Reader. If it was consumed, we can't reuse it easily unless we buffered it.
		// However, for standard REST pagination (GET), Body is usually nil.
		// If we are here, and it's NOT a GraphQL template, we assume standard GET or body is irrelevant/static?
		// Let's assume standard GET behavior for now unless template is present.
	
	// Create next request
	nextReq := &Request{
		URL:      parsedURL.String(),
		Method:   req.Method,
		Headers:  req.Headers,
		Params:   newParams,
		Metadata: req.Metadata, // Propagate metadata
		Body:     newBody,
	}

	f.logger.Info("built next page request",
		slog.String("url", nextReq.URL),
		slog.Int("newOffset", offset+limit),
	)

	return nextReq
}
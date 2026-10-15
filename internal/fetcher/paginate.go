package fetcher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/samucap/poly-asian-data/internal/logging"
)

// PaginateDelay is the optional wait between successive page requests.
// Default 0 (no artificial delay). Set from config at process start if needed
// (e.g. after HTTP 429s). Used by FetchPaginated and FetchPaginatedKeyset.
var PaginateDelay time.Duration

func waitPaginateDelay(ctx context.Context) error {
	if PaginateDelay <= 0 {
		return ctx.Err()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(PaginateDelay):
		return nil
	}
}

// FetchPaginated collects all pages of a JSON-array API before returning.
// limit must be ≤ the API's max page size (Gamma events/tags: 100). Stopping when
// len(page) < limit is only valid when limit is that real page size.
//
// On HTTP 422 with Gamma "offset too large" / keyset messaging, returns pages
// already collected rather than failing hard (once at least one page succeeded).
func FetchPaginated[T any](ctx context.Context, cl *http.Client, baseURL *url.URL, limit int, limitThreshold int) ([]*T, error) {
	if cl == nil {
		cl = NewSecureHTTPClient()
	}
	if baseURL == nil {
		return nil, fmt.Errorf("fetch paginated: baseURL is required")
	}

	var allResults []*T
	offset := 0

	for {
		if err := ctx.Err(); err != nil {
			return allResults, err
		}

		q := baseURL.Query()
		q.Set("limit", fmt.Sprintf("%d", limit))
		q.Set("offset", fmt.Sprintf("%d", offset))

		reqURL := *baseURL
		reqURL.RawQuery = q.Encode()

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
		if err != nil {
			return nil, err
		}

		resp, err := cl.Do(httpReq)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode != http.StatusOK {
			bodyBytes, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			// Gamma rejects deep offset pagination (≈2100+); use what we already have.
			// Message: "offset too large, use /events/keyset for deeper pagination"
			if resp.StatusCode == http.StatusUnprocessableEntity && len(allResults) > 0 &&
				(bytes.Contains(bodyBytes, []byte("offset too large")) ||
					bytes.Contains(bodyBytes, []byte("keyset"))) {
				logging.Warn("pagination offset limit reached; returning collected pages",
					"collected", len(allResults),
					"offset", offset,
					"body", string(bodyBytes),
				)
				return allResults, nil
			}
			return nil, fmt.Errorf("bad status: %s, body: %s, url: %s", resp.Status, string(bodyBytes), reqURL.String())
		}

		var pageData []*T
		if err := json.NewDecoder(resp.Body).Decode(&pageData); err != nil {
			resp.Body.Close()
			return nil, err
		}
		resp.Body.Close()

		if len(pageData) == 0 {
			break
		}

		allResults = append(allResults, pageData...)
		offset += len(pageData)

		if limitThreshold > 0 && len(allResults) >= limitThreshold {
			break
		}
		// Full page ⇒ more may exist; short page ⇒ last page.
		if len(pageData) < limit {
			break
		}

		if err := waitPaginateDelay(ctx); err != nil {
			return allResults, err
		}
	}

	return allResults, nil
}

// keysetPage is the Gamma /events/keyset (and similar) response envelope.
// next_cursor is present only when len(events) equals the effective limit (more pages may exist).
type keysetPage[T any] struct {
	Events     []*T   `json:"events"`
	NextCursor string `json:"next_cursor"`
}

// FetchPaginatedKeyset collects all pages of a keyset-paginated API.
// Callers must omit offset on baseURL (keyset endpoints reject it). This helper
// only sets limit and after_cursor (when continuing).
//
// Response: {"events":[...], "next_cursor":"..."} — next_cursor omitted on the last page.
// Stops when events is empty, next_cursor is empty, or limitThreshold is reached.
// limit must be ≤ API max (Gamma keyset: 500).
// Inter-page wait uses package PaginateDelay (default 0).
func FetchPaginatedKeyset[T any](ctx context.Context, cl *http.Client, baseURL *url.URL, limit int, limitThreshold int) ([]*T, error) {
	if cl == nil {
		cl = NewSecureHTTPClient()
	}
	if baseURL == nil {
		return nil, fmt.Errorf("fetch paginated keyset: baseURL is required")
	}
	if limit <= 0 {
		limit = 100
	}

	var allResults []*T
	cursor := ""

	for {
		if err := ctx.Err(); err != nil {
			return allResults, err
		}

		q := baseURL.Query()
		q.Set("limit", fmt.Sprintf("%d", limit))
		if cursor != "" {
			q.Set("after_cursor", cursor)
		} else {
			q.Del("after_cursor")
		}

		reqURL := *baseURL
		reqURL.RawQuery = q.Encode()

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL.String(), nil)
		if err != nil {
			return nil, err
		}

		resp, err := cl.Do(httpReq)
		if err != nil {
			return nil, err
		}

		bodyBytes, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("reading keyset page: %w", err)
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("bad status: %s, body: %s, url: %s", resp.Status, string(bodyBytes), reqURL.String())
		}

		var page keysetPage[T]
		if err := json.Unmarshal(bodyBytes, &page); err != nil {
			return nil, fmt.Errorf("decode keyset page: %w", err)
		}
		if len(page.Events) == 0 {
			break
		}

		allResults = append(allResults, page.Events...)

		if limitThreshold > 0 && len(allResults) >= limitThreshold {
			break
		}
		// Last page when next_cursor omitted (short page or exact full final page).
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor

		if err := waitPaginateDelay(ctx); err != nil {
			return allResults, err
		}
	}

	return allResults, nil
}

package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFetchPaginated_HonorsPageSize drives FetchPaginated against an httptest
// that caps pages at 100 (Gamma-like). Requesting limit=100 must walk all pages;
// an oversized limit stops early because len(page)=100 < limit=500.
func TestFetchPaginated_HonorsPageSize(t *testing.T) {
	const total = 250
	const apiMax = 100

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		if limit <= 0 {
			limit = apiMax
		}
		if limit > apiMax {
			limit = apiMax
		}
		var page []map[string]string
		for i := 0; i < limit && offset+i < total; i++ {
			page = append(page, map[string]string{"id": fmt.Sprintf("%d", offset+i)})
		}
		_ = json.NewEncoder(w).Encode(page)
	}))
	defer srv.Close()

	base, err := url.Parse(srv.URL)
	require.NoError(t, err)

	got, err := FetchPaginated[map[string]string](context.Background(), srv.Client(), base, apiMax, 0)
	require.NoError(t, err)
	assert.Equal(t, total, len(got), "with page size=API max, all pages must be collected")

	gotBug, err := FetchPaginated[map[string]string](context.Background(), srv.Client(), base, 500, 0)
	require.NoError(t, err)
	assert.Equal(t, apiMax, len(gotBug), "oversized limit incorrectly treats capped page as last page")
}

func TestFetchPaginated_EmptyStops(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("[]"))
	}))
	defer srv.Close()
	base, err := url.Parse(srv.URL)
	require.NoError(t, err)
	got, err := FetchPaginated[map[string]string](context.Background(), srv.Client(), base, 100, 0)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestFetchPaginated_OffsetTooLargeReturnsCollected(t *testing.T) {
	const pageSize = 100
	const maxOffset = 200

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		if offset >= maxOffset {
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte(`{"type":"validation error","error":"offset too large, use /events/keyset for deeper pagination"}`))
			return
		}
		var page []map[string]string
		for i := 0; i < pageSize; i++ {
			page = append(page, map[string]string{"id": fmt.Sprintf("%d", offset+i)})
		}
		_ = json.NewEncoder(w).Encode(page)
	}))
	defer srv.Close()

	base, err := url.Parse(srv.URL)
	require.NoError(t, err)
	got, err := FetchPaginated[map[string]string](context.Background(), srv.Client(), base, pageSize, 0)
	require.NoError(t, err)
	assert.Equal(t, maxOffset, len(got))
}

func TestNewSecureHTTPClient(t *testing.T) {
	cl := NewSecureHTTPClient()
	require.NotNil(t, cl)
	require.NotNil(t, cl.Transport)
	assert.Equal(t, 30*time.Second, cl.Timeout)
}

// TestFetchPaginatedKeyset_MultiPage exercises our keyset helper (not live Gamma):
// multi-page merge, stop without next_cursor, and no offset query param.
func TestFetchPaginatedKeyset_MultiPage(t *testing.T) {
	const pageSize = 2
	const total = 5
	pages := 0

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.URL.Query().Get("offset"), "keyset helper must not send offset")
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		assert.Equal(t, pageSize, limit)

		cursor := r.URL.Query().Get("after_cursor")
		start := 0
		switch cursor {
		case "":
			start = 0
		case "c1":
			start = pageSize
		case "c2":
			start = pageSize * 2
		default:
			t.Fatalf("unexpected after_cursor %q", cursor)
		}

		var events []map[string]string
		for i := 0; i < pageSize && start+i < total; i++ {
			events = append(events, map[string]string{"id": fmt.Sprintf("%d", start+i)})
		}
		pages++
		resp := map[string]any{"events": events}
		if len(events) == pageSize && start+pageSize < total {
			if start == 0 {
				resp["next_cursor"] = "c1"
			} else {
				resp["next_cursor"] = "c2"
			}
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	base, err := url.Parse(srv.URL)
	require.NoError(t, err)

	got, err := FetchPaginatedKeyset[map[string]string](context.Background(), srv.Client(), base, pageSize, 0)
	require.NoError(t, err)
	assert.Equal(t, total, len(got))
	assert.Equal(t, 3, pages)
	assert.Equal(t, "0", (*got[0])["id"])
	assert.Equal(t, "4", (*got[4])["id"])
}

func TestFetchPaginatedKeyset_ShortPageStops(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		assert.Empty(t, r.URL.Query().Get("offset"))
		// Full limit-sized page but no next_cursor ⇒ stop.
		_ = json.NewEncoder(w).Encode(map[string]any{
			"events": []map[string]string{{"id": "a"}, {"id": "b"}},
		})
	}))
	defer srv.Close()
	base, err := url.Parse(srv.URL)
	require.NoError(t, err)
	got, err := FetchPaginatedKeyset[map[string]string](context.Background(), srv.Client(), base, 2, 0)
	require.NoError(t, err)
	assert.Equal(t, 2, len(got))
	assert.Equal(t, 1, calls)
}

func TestFetchPaginatedKeyset_EmptyEventsStops(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"events": []any{}})
	}))
	defer srv.Close()
	base, err := url.Parse(srv.URL)
	require.NoError(t, err)
	got, err := FetchPaginatedKeyset[map[string]string](context.Background(), srv.Client(), base, 100, 0)
	require.NoError(t, err)
	assert.Empty(t, got)
}

package processor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	logging.Init("dev")
}

// =============================================================================
// Type Dispatch Tests
// =============================================================================

func TestProcessor_ProcessSports(t *testing.T) {
	t.Run("unmarshals sports correctly", func(t *testing.T) {
		ctx := context.Background()
		cfg := &config.Config{}
		fetcherPool, _ := fetcher.New(ctx, cfg, 1, 10)
		defer fetcherPool.Stop()

		p, err := New(ctx, 1, 10, fetcherPool)
		require.NoError(t, err)
		defer p.Stop()

		sportsData := []services.PlyMktSport{
			{Sport: "basketball", Image: "nba.png"},
			{Sport: "football", Image: "nfl.png"},
		}
		data, _ := json.Marshal(sportsData)

		result, count, err := p.processSports(data)
		require.NoError(t, err)
		assert.Equal(t, 2, count)

		sports := result.([]services.PlyMktSport)
		assert.Equal(t, "basketball", sports[0].Sport)
		assert.Equal(t, "football", sports[1].Sport)
	})
}

func TestProcessor_ProcessTeams(t *testing.T) {
	t.Run("unmarshals teams correctly", func(t *testing.T) {
		ctx := context.Background()
		cfg := &config.Config{}
		fetcherPool, _ := fetcher.New(ctx, cfg, 1, 10)
		defer fetcherPool.Stop()

		p, err := New(ctx, 1, 10, fetcherPool)
		require.NoError(t, err)
		defer p.Stop()

		teamsData := []services.PlyMktTeam{
			{ID: 1, Name: "Lakers", League: "NBA"},
			{ID: 2, Name: "Celtics", League: "NBA"},
			{ID: 3, Name: "Warriors", League: "NBA"},
		}
		data, _ := json.Marshal(teamsData)

		result, count, err := p.processTeams(data)
		require.NoError(t, err)
		assert.Equal(t, 3, count)

		teams := result.([]services.PlyMktTeam)
		assert.Equal(t, "Lakers", teams[0].Name)
		assert.Equal(t, "Celtics", teams[1].Name)
	})
}

// =============================================================================
// Pagination Tests
// =============================================================================

func TestProcessor_Pagination(t *testing.T) {
	t.Run("requests next page when itemCount equals limit", func(t *testing.T) {
		// Count how many requests the fetcher receives
		var fetchCount atomic.Int32

		// Create mock server
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fetchCount.Add(1)
			// Return 10 items (full page)
			teams := make([]services.PlyMktTeam, 10)
			for i := range teams {
				teams[i] = services.PlyMktTeam{ID: i, Name: "Team"}
			}
			data, _ := json.Marshal(teams)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(data)
		}))
		defer server.Close()

		ctx := context.Background()
		cfg := &config.Config{}
		fetcherPool, _ := fetcher.New(ctx, cfg, 1, 10)
		defer fetcherPool.Stop()

		p, err := New(ctx, 1, 10, fetcherPool)
		require.NoError(t, err)
		defer p.Stop()

		// Create response with pagination params
		params := url.Values{}
		params.Set("limit", "10")
		params.Set("offset", "0")

		resp := &fetcher.Response{
			URL:  server.URL + "/teams?" + params.Encode(),
			Data: []byte{}, // Will be populated by processor
			Request: &fetcher.Request{
				URL:    server.URL + "/teams?" + params.Encode(),
				Params: params,
			},
		}

		// Simulate processing a full page of teams
		teamsData := make([]services.PlyMktTeam, 10)
		for i := range teamsData {
			teamsData[i] = services.PlyMktTeam{ID: i, Name: "Team"}
		}
		data, _ := json.Marshal(teamsData)
		resp.Data = data

		// Process
		output, err := p.workerTask(ctx, resp)
		require.NoError(t, err)
		assert.Equal(t, 10, output.ItemCount)

		// Wait a bit for the next page request to be submitted
		time.Sleep(50 * time.Millisecond)

		// Check that a next page request was submitted to fetcher
		stats := fetcherPool.Stats().Snapshot()
		assert.GreaterOrEqual(t, stats.Submitted, int64(1))
	})

	t.Run("does not request next page when itemCount less than limit", func(t *testing.T) {
		ctx := context.Background()
		cfg := &config.Config{}
		fetcherPool, _ := fetcher.New(ctx, cfg, 1, 10)
		defer fetcherPool.Stop()

		p, err := New(ctx, 1, 10, fetcherPool)
		require.NoError(t, err)
		defer p.Stop()

		// Create response with pagination params
		params := url.Values{}
		params.Set("limit", "10")
		params.Set("offset", "50")

		resp := &fetcher.Response{
			URL: "http://example.com/teams?" + params.Encode(),
			Request: &fetcher.Request{
				URL:    "http://example.com/teams?" + params.Encode(),
				Params: params,
			},
		}

		// Simulate processing a partial page (7 items - less than limit)
		teamsData := make([]services.PlyMktTeam, 7)
		for i := range teamsData {
			teamsData[i] = services.PlyMktTeam{ID: i, Name: "Team"}
		}
		data, _ := json.Marshal(teamsData)
		resp.Data = data

		// Process
		output, err := p.workerTask(ctx, resp)
		require.NoError(t, err)
		assert.Equal(t, 7, output.ItemCount)

		// Wait a bit
		time.Sleep(50 * time.Millisecond)

		// Should NOT have submitted any requests (last page reached)
		stats := fetcherPool.Stats().Snapshot()
		assert.Equal(t, int64(0), stats.Submitted)
	})

	t.Run("correctly increments offset", func(t *testing.T) {
		ctx := context.Background()
		cfg := &config.Config{}
		fetcherPool, _ := fetcher.New(ctx, cfg, 1, 10)
		defer fetcherPool.Stop()

		p, err := New(ctx, 1, 10, fetcherPool)
		require.NoError(t, err)
		defer p.Stop()

		// Create response at offset 100
		params := url.Values{}
		params.Set("limit", "10")
		params.Set("offset", "100")

		resp := &fetcher.Response{
			URL: "http://example.com/teams?" + params.Encode(),
			Request: &fetcher.Request{
				URL:    "http://example.com/teams?" + params.Encode(),
				Params: params,
			},
		}

		// Full page
		teamsData := make([]services.PlyMktTeam, 10)
		data, _ := json.Marshal(teamsData)
		resp.Data = data

		// Process - should submit next page at offset 110
		_, err = p.workerTask(ctx, resp)
		require.NoError(t, err)

		// Wait for submit
		time.Sleep(50 * time.Millisecond)

		// Verify the request was submitted
		stats := fetcherPool.Stats().Snapshot()
		assert.Equal(t, int64(1), stats.Submitted)
	})
}

// =============================================================================
// Stats Tests
// =============================================================================

func TestProcessor_Stats(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{}
	fetcherPool, _ := fetcher.New(ctx, cfg, 1, 10)
	defer fetcherPool.Stop()

	p, err := New(ctx, 1, 10, fetcherPool)
	require.NoError(t, err)
	defer p.Stop()

	stats := p.ProcessorStats().Snapshot()
	assert.GreaterOrEqual(t, stats.ItemsProcessed, int64(0))
}
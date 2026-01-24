package processor

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"

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

		p, err := New(ctx, 1, 10)
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

		p, err := New(ctx, 1, 10)
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
	t.Run("output includes OriginalRequest for pagination", func(t *testing.T) {
		ctx := context.Background()

		p, err := New(ctx, 1, 10)
		require.NoError(t, err)
		defer p.Stop()

		// Create response with pagination params
		params := url.Values{}
		params.Set("limit", "10")
		params.Set("offset", "0")

		originalReq := &fetcher.Request{
			URL:    "http://example.com/teams?" + params.Encode(),
			Params: params,
		}

		resp := &fetcher.Response{
			URL:     "http://example.com/teams?" + params.Encode(),
			Request: originalReq,
		}

		// Simulate processing a page of teams
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

		// OriginalRequest should be set for fetcher to use for pagination
		require.NotNil(t, output.OriginalRequest)
		assert.Equal(t, originalReq, output.OriginalRequest)
		assert.Equal(t, "10", output.OriginalRequest.Params.Get("limit"))
		assert.Equal(t, "0", output.OriginalRequest.Params.Get("offset"))
	})
}

// =============================================================================
// Stats Tests
// =============================================================================

func TestProcessor_Stats(t *testing.T) {
	ctx := context.Background()

	p, err := New(ctx, 1, 10)
	require.NoError(t, err)
	defer p.Stop()

	stats := p.ProcessorStats().Snapshot()
	assert.GreaterOrEqual(t, stats.ItemsProcessed, int64(0))
}
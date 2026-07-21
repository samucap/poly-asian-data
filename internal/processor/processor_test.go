package processor

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"

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

		p, err := New(ctx, cfg, 1, 10)
		require.NoError(t, err)
		defer p.Stop()

		sportsData := []services.PlyMktSport{
			{Sport: "basketball", Image: "nba.png"},
			{Sport: "football", Image: "nfl.png"},
		}
		data, _ := json.Marshal(sportsData)

		resp := &fetcher.Response{
			URL:     "http://api.com/sports",
			Data:    data,
			Request: &fetcher.Request{},
		}

		// Use workerTask (which dispatches to processLeagues) or call directly if exported.
		// processLeagues is private (lowercase). We should test via workerTask dispatch.
		output, err := p.workerTask(ctx, resp)
		require.NoError(t, err)
		assert.Equal(t, 2, output.ItemCount)
		
		// leagues only; hierarchy applied later after tag defs
		require.Len(t, output.SaverPayloads, 1)
		assert.Equal(t, "leagues", output.SaverPayloads[0].TableName)
		leagues := output.SaverPayloads[0].Data.([]services.PlyMktSport)
		assert.Equal(t, "basketball", leagues[0].Sport)
		assert.Empty(t, output.DerivedRequests)
		ids := p.TakePendingSportTagIDs()
		assert.Contains(t, ids, "1")
		assert.Contains(t, ids, "64")
		pending := p.TakePendingLeagues()
		require.Len(t, pending, 2)
	})
}

func TestProcessor_ProcessTeams(t *testing.T) {
	t.Run("unmarshals teams correctly", func(t *testing.T) {
		ctx := context.Background()
		cfg := &config.Config{}

		p, err := New(ctx, cfg, 1, 10)
		require.NoError(t, err)
		defer p.Stop()

		teamsData := []services.PlyMktTeam{
			{ID: 1, Name: "Lakers", League: "NBA"},
			{ID: 2, Name: "Celtics", League: "NBA"},
			{ID: 3, Name: "Warriors", League: "NBA"},
		}
		data, _ := json.Marshal(teamsData)

		resp := &fetcher.Response{
			URL:     "http://api.com/teams",
			Data:    data,
			Request: &fetcher.Request{},
		}

		output, err := p.workerTask(ctx, resp)
		require.NoError(t, err)
		assert.Equal(t, 3, output.ItemCount)

		teams := output.SaverPayloads[0].Data.([]services.PlyMktTeam)
		assert.Equal(t, "Lakers", teams[0].Name)
	})
}

// =============================================================================
// Pagination Tests
// =============================================================================

func TestProcessor_Pagination(t *testing.T) {
	t.Run("output includes OriginalRequest for pagination", func(t *testing.T) {
		ctx := context.Background()
		cfg := &config.Config{}

		p, err := New(ctx, cfg, 1, 10)
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
	cfg := &config.Config{}

	p, err := New(ctx, cfg, 1, 10)
	require.NoError(t, err)
	defer p.Stop()

	stats := p.ProcessorStats().Snapshot()
	assert.GreaterOrEqual(t, stats.ItemsProcessed, int64(0))
}

func TestProcessor_ProcessPricesHistory(t *testing.T) {
	t.Run("unmarshals price history correctly", func(t *testing.T) {
		ctx := context.Background()
		cfg := &config.Config{}

		p, err := New(ctx, cfg, 1, 10)
		require.NoError(t, err)
		defer p.Stop()

		historyData := []services.PlyMktPriceHistory{
			{Timestamp: 1620000000, Price: 105},
			{Timestamp: 1620000060, Price: 110},
		}
		
		// Test Object Format wrapper
		wrapper := map[string]interface{}{
			"history": historyData,
		}
		data, _ := json.Marshal(wrapper)

		resp := &fetcher.Response{
			URL:     "http://api.com/prices-history?interval=1m",
			Data:    data,
			Request: &fetcher.Request{},
		}

		output, err := p.workerTask(ctx, resp)
		require.NoError(t, err)
		assert.Equal(t, 2, output.ItemCount)

		require.Len(t, output.SaverPayloads, 1)
		payload := output.SaverPayloads[0]
		assert.Equal(t, "prices_history", payload.TableName)
		items := payload.Data.([]services.PlyMktPriceHistory)
		assert.Equal(t, int64(1620000000), items[0].Timestamp)
	})
	
	t.Run("unmarshals plain array price history correctly", func(t *testing.T) {
		ctx := context.Background()
		cfg := &config.Config{}

		p, err := New(ctx, cfg, 1, 10)
		require.NoError(t, err)
		defer p.Stop()

		historyData := []services.PlyMktPriceHistory{
			{Timestamp: 1620000000, Price: 105},
		}
		data, _ := json.Marshal(historyData)

		resp := &fetcher.Response{
			URL:     "http://api.com/prices-history?interval=1m",
			Data:    data,
			Request: &fetcher.Request{},
		}

		output, err := p.workerTask(ctx, resp)
		require.NoError(t, err)
		assert.Equal(t, 1, output.ItemCount)
		items := output.SaverPayloads[0].Data.([]services.PlyMktPriceHistory)
		assert.Equal(t, int64(1620000000), items[0].Timestamp)
	})
}

func TestProcessor_ProcessOrderbook(t *testing.T) {
	t.Run("unmarshals orderbook and computes spread", func(t *testing.T) {
		ctx := context.Background()
		cfg := &config.Config{}
		
		p, err := New(ctx, cfg, 1, 10)
		require.NoError(t, err)
		defer p.Stop()
		
		obData := services.PlyMktOrderbook{
			TokenID: "123",
			Bids: []services.OrderbookItem{{Price: "0.40", Size: "100"}},
			Asks: []services.OrderbookItem{{Price: "0.60", Size: "100"}},
		}
		data, _ := json.Marshal(obData)
		
		resp := &fetcher.Response{
			URL: "http://api.com/book?token_id=123",
			Data: data,
			Request: &fetcher.Request{Metadata: map[string]string{"MarketID": "mkt1"}},
		}
		
		output, err := p.workerTask(ctx, resp)
		require.NoError(t, err)
		assert.Equal(t, 1, output.ItemCount)
		
		require.Len(t, output.SaverPayloads, 1)
		payload := output.SaverPayloads[0]
		assert.Equal(t, "orderbooks", payload.TableName)
		
		items, ok := payload.Data.([]services.PlyMktOrderbook)
		require.True(t, ok, "orderbook payload should be []PlyMktOrderbook")
		require.Len(t, items, 1)
		assert.Equal(t, "123", items[0].TokenID)
		assert.InDelta(t, 0.20, items[0].Spread, 0.0001) // 0.60 - 0.40
	})
}
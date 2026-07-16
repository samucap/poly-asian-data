package processor

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessor_TopMarkets(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{}
	p, err := New(ctx, cfg, 1, 10)
	require.NoError(t, err)
	defer p.Stop()

	t.Run("processTopMarketsOI", func(t *testing.T) {
		oiData := []oiResponse{
			{Market: "0x123", Value: 100500.5},
			{Market: "0x456", Value: 250.75},
		}
		data, _ := json.Marshal(oiData)

		resp := &fetcher.Response{
			URL:  "https://data-api.polymarket.com/oi",
			Data: data,
			Request: &fetcher.Request{
				Metadata: map[string]string{
					"Entity": "top_markets_oi",
				},
			},
		}

		output, err := p.workerTask(ctx, resp)
		require.NoError(t, err)
		assert.Equal(t, 2, output.ItemCount)
		require.Len(t, output.SaverPayloads, 1)
		assert.Equal(t, "plymkt_markets_oi", output.SaverPayloads[0].TableName)

		payload := output.SaverPayloads[0].Data.([]services.PlyMktMarketOI)
		assert.Equal(t, "0x123", payload[0].Market)
		assert.Equal(t, 100500.5, payload[0].Value)
	})

	t.Run("processTopMarketsOI_MergeOnly", func(t *testing.T) {
		oiData := []oiResponse{{Market: "0xabc", Value: 42.5}}
		data, _ := json.Marshal(oiData)
		resp := &fetcher.Response{
			URL:  "https://data-api.polymarket.com/oi",
			Data: data,
			Request: &fetcher.Request{
				Metadata: map[string]string{
					"Entity":  "top_markets_oi",
					"MergeOI": "true",
				},
			},
		}
		output, err := p.workerTask(ctx, resp)
		require.NoError(t, err)
		assert.Equal(t, 1, output.ItemCount)
		assert.Empty(t, output.SaverPayloads)
		merged := p.TakeMergedOI()
		assert.Equal(t, 42.5, merged["0xabc"])
	})

	t.Run("processTopMarketsTrades", func(t *testing.T) {
		tradesData := []services.PlyMktTrade{
			{TransactionHash: "hash1", ProxyWallet: "wallet1", Side: "buy", Size: 100, Price: 0.5},
			{TransactionHash: "hash2", ProxyWallet: "wallet2", Side: "sell", Size: 200, Price: 0.25},
		}
		data, _ := json.Marshal(tradesData)

		resp := &fetcher.Response{
			URL:  "https://data-api.polymarket.com/trades",
			Data: data,
			Request: &fetcher.Request{
				Metadata: map[string]string{
					"Entity": "top_markets_trades",
				},
			},
		}

		output, err := p.workerTask(ctx, resp)
		require.NoError(t, err)
		assert.Equal(t, 2, output.ItemCount)
		require.Len(t, output.SaverPayloads, 1)
		assert.Equal(t, "trades", output.SaverPayloads[0].TableName)

		payload := output.SaverPayloads[0].Data.([]services.PlyMktTrade)
		assert.Equal(t, "hash1", payload[0].TransactionHash)
		assert.Equal(t, 100.0, payload[0].Size)
	})

	t.Run("processTopMarketsPrices", func(t *testing.T) {
		pricesData := map[string]any{
			"history": []map[string]any{
				{"t": 1700000000, "p": 0.45},
				{"t": 1700000300, "p": 0.47},
			},
		}
		data, _ := json.Marshal(pricesData)

		resp := &fetcher.Response{
			URL:  "https://clob.polymarket.com/prices-history",
			Data: data,
			Request: &fetcher.Request{
				Metadata: map[string]string{
					"Entity":   "top_markets_prices",
					"MarketID": "market1",
					"TokenID":  "token1",
				},
			},
		}

		output, err := p.workerTask(ctx, resp)
		require.NoError(t, err)
		assert.Equal(t, 2, output.ItemCount)
		require.Len(t, output.SaverPayloads, 1)
		assert.Equal(t, "prices_history", output.SaverPayloads[0].TableName)

		payload := output.SaverPayloads[0].Data.([]services.PlyMktPriceHistory)
		assert.Equal(t, "token1", payload[0].TokenID)
		assert.Equal(t, "market1", payload[0].MarketID)
		assert.Equal(t, int64(1700000000), payload[0].Timestamp)
		assert.Equal(t, 0.45, payload[0].Price)
		assert.Equal(t, 5, payload[0].Fidelity)
	})

	t.Run("processTopMarketsPrices_BatchMap", func(t *testing.T) {
		pricesData := map[string]any{
			"history": map[string]any{
				"tokenA": []map[string]any{{"t": 100, "p": 0.1}, {"t": 200, "p": 0.2}},
				"tokenB": []map[string]any{{"t": 100, "p": 0.9}},
			},
		}
		data, _ := json.Marshal(pricesData)
		tokenMap, _ := json.Marshal(map[string]string{"tokenA": "mA", "tokenB": "mB"})
		resp := &fetcher.Response{
			URL:  "https://clob.polymarket.com/batch-prices-history",
			Data: data,
			Request: &fetcher.Request{
				Metadata: map[string]string{
					"Entity":         "top_markets_prices",
					"TokenMarketMap": string(tokenMap),
					"Fidelity":       "5",
				},
			},
		}
		output, err := p.workerTask(ctx, resp)
		require.NoError(t, err)
		assert.Equal(t, 3, output.ItemCount)
		payload := output.SaverPayloads[0].Data.([]services.PlyMktPriceHistory)
		byToken := map[string][]services.PlyMktPriceHistory{}
		for _, pt := range payload {
			byToken[pt.TokenID] = append(byToken[pt.TokenID], pt)
		}
		require.Len(t, byToken["tokenA"], 2)
		assert.Equal(t, "mA", byToken["tokenA"][0].MarketID)
		require.Len(t, byToken["tokenB"], 1)
		assert.Equal(t, 0.9, byToken["tokenB"][0].Price)
	})

	t.Run("processTopMarketsOrderbooks", func(t *testing.T) {
		obData := []orderbookResponse{
			{
				AssetID: "token1",
				MarketID: "market1",
				Bids: []orderbookLevel{
					{Price: "0.55", Size: "100"},
					{Price: "0.54", Size: "200"},
				},
				Asks: []orderbookLevel{
					{Price: "0.57", Size: "150"},
				},
				NegRisk: true,
				Timestamp: "12345",
			},
		}
		data, _ := json.Marshal(obData)

		resp := &fetcher.Response{
			URL:  "https://clob.polymarket.com/books",
			Data: data,
			Request: &fetcher.Request{
				Metadata: map[string]string{
					"Entity": "top_markets_orderbooks",
				},
			},
		}

		output, err := p.workerTask(ctx, resp)
		require.NoError(t, err)
		assert.Equal(t, 1, output.ItemCount)
		require.Len(t, output.SaverPayloads, 1)
		assert.Equal(t, "orderbook_snapshots", output.SaverPayloads[0].TableName)

		payload := output.SaverPayloads[0].Data.([]services.PlyMktOrderbookSnapshot)
		assert.Equal(t, "token1", payload[0].TokenID)
		assert.Equal(t, "market1", payload[0].MarketID)
		assert.Equal(t, 0.55, payload[0].BestBid)
		assert.Equal(t, 0.57, payload[0].BestAsk)
		assert.Equal(t, 300.0, payload[0].TotalBidDepth)
		assert.Equal(t, 150.0, payload[0].TotalAskDepth)
		assert.Equal(t, true, payload[0].NegRisk)
		assert.Equal(t, "12345", payload[0].Timestamp)

		// Check depth JSON unmarshalling
		var depth map[string][]orderbookLevel
		err = json.Unmarshal(payload[0].DepthJSON, &depth)
		require.NoError(t, err)
		assert.Equal(t, "0.55", depth["bids"][0].Price)
		assert.Equal(t, "0.57", depth["asks"][0].Price)
	})
}

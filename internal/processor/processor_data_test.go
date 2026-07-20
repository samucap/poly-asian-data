package processor

import (
	"context"
	"testing"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessLeaderboard(t *testing.T) {
	// Setup
	cfg := &config.Config{
		Services: config.ServicesConfig{
			PlyMkt: config.PlyMktConfig{
				Endpoints: map[string]any{
					"data": "https://data-api.polymarket.com",
				},
			},
		},
	}
	proc, err := New(context.Background(), cfg, 1, 10)
	require.NoError(t, err)

	// Sample JSON Response from Leaderboard API
	jsonBody := `[
		{
			"rank": "1",
			"proxyWallet": "0x123",
			"userName": "User1",
			"vol": 1000.5,
			"pnl": 500.2
		},
		{
			"rank": "2",
			"proxyWallet": "0x456",
			"userName": "User2",
			"vol": 900.0,
			"pnl": 400.0
		}
	]`

	resp := &fetcher.Response{
		URL:  "https://data-api.polymarket.com/v1/leaderboard?limit=10",
		Data: []byte(jsonBody),
		Request: &fetcher.Request{
			URL:    "https://data-api.polymarket.com/v1/leaderboard?limit=10",
			Method: "GET",
			Metadata: map[string]string{
				"IsLeaderboard": "true",
				"Period":        "all", // Metadata passed from fetcher
			},
		},
	}

	output, err := proc.workerTask(context.Background(), resp)
	require.NoError(t, err)
	require.NotNil(t, output)

	// Verify SaverPayloads
	require.Len(t, output.SaverPayloads, 1)
	payload := output.SaverPayloads[0]
	assert.Equal(t, "plymkt_users", payload.TableName)

	items, ok := payload.Data.([]services.PlyMktUser)
	require.True(t, ok)
	require.Len(t, items, 2)
	assert.Equal(t, 1, items[0].Rank)
	assert.Equal(t, "0x123", items[0].ProxyWallet)
}

func TestProcessHolders(t *testing.T) {
	// Setup
	cfg := &config.Config{}
	proc, err := New(context.Background(), cfg, 1, 10)
	require.NoError(t, err)

	// Sample JSON
	jsonBody := `[
		{
			"token": "0xToken1",
			"holders": [
				{
					"proxyWallet": "0xHolder1",
					"amount": 100.5,
					"asset": "0xAsset1"
				}
			]
		}
	]`

	resp := &fetcher.Response{
		URL:  "https://data-api.polymarket.com/holders?market=0x123",
		Data: []byte(jsonBody),
		Request: &fetcher.Request{
			URL: "https://data-api.polymarket.com/holders?market=0x123",
			Metadata: map[string]string{
				"IsHolders": "true",
			},
		},
	}

	output, err := proc.workerTask(context.Background(), resp)
	require.NoError(t, err)
	require.NotNil(t, output)

	// Holders are saved as a single bundle payload (users + holder records)
	require.Len(t, output.SaverPayloads, 1)
	assert.Equal(t, "plymkt_holders_bundle", output.SaverPayloads[0].TableName)

	bundle, ok := output.SaverPayloads[0].Data.(services.PlyMktHoldersBundle)
	require.True(t, ok)
	require.NotEmpty(t, bundle.Users)
	assert.Equal(t, "0xHolder1", bundle.Users[0].ProxyWallet)
	require.NotEmpty(t, bundle.Holders)
	r := bundle.Holders[0]
	assert.Equal(t, "0xToken1", r.TokenID)
	assert.Equal(t, "0xHolder1", r.ProxyWallet)
	assert.Equal(t, 100.5, r.Amount)
}

package services

import (
	"context"
	"testing"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetLeaderboardReqs(t *testing.T) {
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
	svc := &PlyMktService{Cfg: cfg}

	params := PlyMktLeaderboardParams{
		TimePeriod: "month",
		Limit:  100,
	}

	reqs, err := svc.GetLeaderboardReqs(context.Background(), params)
	require.NoError(t, err)
	require.Len(t, reqs, 1)

	req := reqs[0]
	assert.Equal(t, "https://data-api.polymarket.com/v1/leaderboard?limit=100&timePeriod=month", req.URL)
	assert.Equal(t, "GET", req.Method)
	assert.Equal(t, "true", req.Metadata["IsLeaderboard"])
}

func TestGetHoldersReqs(t *testing.T) {
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
	svc := &PlyMktService{Cfg: cfg}

	// Test Case 1: Single Market
	marketIDs := []string{"0x123", "0x456"}
	reqs, err := svc.GetHoldersReqs(context.Background(), marketIDs)
	require.NoError(t, err)
	require.Len(t, reqs, 1) // Should be one request for comma separated list

	req := reqs[0]
	assert.Contains(t, req.URL, "/holders")
	assert.Contains(t, req.URL, "market=0x123,0x456")
	assert.Equal(t, "GET", req.Method)
	assert.Equal(t, "true", req.Metadata["IsHolders"])
}

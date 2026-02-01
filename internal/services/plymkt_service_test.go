package services

import (
	"context"
	"testing"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetSubgraphReqs(t *testing.T) {
	// Setup a mock server to act as the subgraph endpoint if needed, 
	// but GetSubgraphReqs mainly constructs requests, so we might just check the Output.
	// However, it uses config.DefaultEndpoints which might be hardcoded.

	// Let's first inspect how to mock the config or endpoint.
	// The code uses config.DefaultEndpoints["subgraph"]
	
	// We can temporarily modify config.DefaultEndpoints for the test, 
	// assuming it's a public variable.
	
	originalEndpoint := config.DefaultEndpoints["subgraph"]
	defer func() { config.DefaultEndpoints["subgraph"] = originalEndpoint }()
	
	testEndpoint := "http://test-subgraph.com/api"
	config.DefaultEndpoints["subgraph"] = testEndpoint

	cfg := &config.Config{
		SubgraphAPIKey: "test-api-key",
		Services: config.ServicesConfig{
			PlyMkt: config.PlyMktConfig{
				Endpoints: map[string]any{
					"subgraph": testEndpoint,
				},
			},
		},
	}

	svc := &PlyMktService{
		Cfg: cfg,
	}

	ctx := context.Background()
	// GetSubgraphReqs now requires targets and startIds
	targets := []string{"conditions", "orderFilledEvents", "enrichedOrderFilleds", "accounts", "fpmms"}
	reqs, err := svc.GetSubgraphReqs(ctx, targets, nil)
	require.NoError(t, err)
	
	// Verify we get requests for all expected entities
	expectedEntities := []string{
		"conditions", 
		"orderFilledEvents", 
		"enrichedOrderFilleds", 
		"accounts", 
		"fpmms",
	}
	
	assert.Len(t, reqs, len(expectedEntities))

	for _, req := range reqs {
		assert.Equal(t, "POST", req.Method)
		assert.Contains(t, req.URL, testEndpoint)
		assert.Equal(t, "Bearer test-api-key", req.Headers["Authorization"])
		assert.Equal(t, "subgraph", req.Metadata["Type"])
		
		// Check that Entity metadata matches one of expectation
		assert.Contains(t, expectedEntities, req.Metadata["Entity"])
	}
}

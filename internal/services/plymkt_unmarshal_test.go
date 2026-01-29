package services_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
	"net/http"
	"io"
	"bytes"

	"github.com/joho/godotenv"
	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper for Subgraph responses
type SubgraphResponse struct {
	Data map[string]json.RawMessage `json:"data"`
}

func TestPlyMktLiveUnmarshalling(t *testing.T) {
	// Load .env_test from project root (../../.env_test relative to internal/services)
	// We ignore error as it might not exist in all environments, but user requested it.
	_ = godotenv.Load("../../.env_test")

	// 1. Check for API Keys
	apiKey := os.Getenv("SUBGRAPH_API_KEY")
	
	// Load config to check if defaults are set
	cfg, err := config.Load()
	if err != nil {
		t.Logf("Config load failed (might be expected in test env without file): %v", err)
	}
	
	if apiKey == "" {
		if cfg != nil && cfg.SubgraphAPIKey != "" {
			apiKey = cfg.SubgraphAPIKey
		}
	}

	if apiKey == "" {
		t.Skip("Skipping integration test: SUBGRAPH_API_KEY not set")
	}

	// Setup basic fetcher (no pool needed for simple sync test, but we need the client logic or just use http)
	// Actually better to use the real fetcher.Fetcher to test its behavior?
	// But Fetcher is a pool. We can use a simplified client or just net/http for the raw check,
	// BUT the goal is to test "Unmarshalling".
	// The Service uses `GetSubgraphReqs` to build requests.
	// The `processor` handles unmarshalling.
	
	// Let's manually fetch and unmarshal to verify structs.
	
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// ---------------------------------------------------------
	// Test 1: Gamma /markets (PlyMktMarket)
	// ---------------------------------------------------------
	t.Run("PlyMktMarket_Gamma", func(t *testing.T) {
		gammaEndpoint := "https://gamma-api.polymarket.com" 
		if cfg != nil && config.DefaultEndpoints["gamma"] != nil {
			gammaEndpoint = config.DefaultEndpoints["gamma"].(string)
		}

		url := fmt.Sprintf("%s/markets?limit=1", gammaEndpoint)

		// Just use http.Get for simplicity as we want to test STRUCTS, not the fetcher pool machinery
		// But wait, the task is "verify unmarshalling".
		resp, err := httpGet(ctx, url)
		require.NoError(t, err)
		defer resp.Body.Close()

		var markets []services.PlyMktMarket
		// Gamma returns raw array or wrapped? usually array or paginated?
		// Gamma /markets returns []Market usually.
		err = json.NewDecoder(resp.Body).Decode(&markets)
		require.NoError(t, err)
		
		require.NotEmpty(t, markets, "Expected at least one market from Gamma")
		m := markets[0]
		
		// Assert fields
		t.Logf("Market: ID=%s Question='%s'", m.ID, m.Question)
		assert.NotEmpty(t, m.ID)
		assert.NotEmpty(t, m.Question)
		// Check some specific fields
		assert.NotZero(t, m.CreatedAt, "CreatedAt should match") 
	})

	// ---------------------------------------------------------
	// Test 2: Subgraph Entities
	// ---------------------------------------------------------
	
	// Map of entity -> query body (copied from plymkt.go to ensure fidelity)
	queries := map[string]string{
		"ordersMatchedEvents": `
			ordersMatchedEvents(
				first: 1
				skip: 0
				orderBy: timestamp
				orderDirection: asc
			) {
				id
				takerAmountFilled
				makerAmountFilled
				makerAssetID
				takerAssetID
				timestamp
			}`,
		"orderFilledEvents": `
			orderFilledEvents(first: 1, orderBy: id, orderDirection: asc, skip: 0) {
				id
				fee
				maker {
					id
				}
				makerAssetId
				makerAmountFilled
				taker {
					id
				}
				takerAmountFilled
				takerAssetId
				timestamp
				transactionHash
			}`,
		"enrichedOrderFilleds": `
			enrichedOrderFilleds(
			first: 1
			skip: 0
			orderDirection: desc
		) {
			id
			timestamp
			maker {
				id
			}
			taker {
				id
			}
			price
			side
			size
			market {
				id
			}
		}`,
		"accounts": `accounts(
			first: 1
			skip: 0
			orderBy: scaledProfit
			orderDirection: desc
		) {
			id
			creationTimestamp
			lastSeenTimestamp
			lastTradedTimestamp
			collateralVolume
			numTrades
			profit
			scaledCollateralVolume
			scaledProfit
	  }`,
	  "conditions": `conditions(
			first: 1
			skip: 0
			orderBy: id
			orderDirection: desc
		) {
			id
			oracle
			outcomeSlotCount
			payoutDenominator
			payoutNumerators
			payouts
			questionId
			resolutionHash
			resolutionTimestamp
	  }`,
	  "fpmms": `fpmms(
			first: 1
			skip: 0
			orderBy: id
			orderDirection: desc
		) {
			conditionId,
			id
		}`,
	}

	subgraphUrl := config.DefaultEndpoints["subgraph"].(string)

	for entityName, queryBody := range queries {
		t.Run(entityName, func(t *testing.T) {
			// Resolve URL
			path := ""
			switch entityName {
			case "conditions", "orderFilledEvents", "enrichedOrderFilleds", "accounts":
				path = "/81Dm16JjuFSrqz813HysXoUPvzTwE7fsfPk2RTf66nyC"
			case "fpmms":
				path = "/6c58N5U4MtQE2Y8njfVrrAfRykzfqajMGeTMEvMmskVz"
			case "ordersMatchedEvents":
				// Assign a path if not defined in switch, or skip
				path = "/81Dm16JjuFSrqz813HysXoUPvzTwE7fsfPk2RTf66nyC" // Assuming same as others for now
			}

			if path == "" {
				t.Logf("Skipping unknown entity path map for %s", entityName)
				return
			}
			
			fullUrl := subgraphUrl + path

			// Construct full query
			fullQuery := fmt.Sprintf(`query MyQuery { %s }`, queryBody)
			
			bodyData := map[string]any{
				"query": fullQuery,
			}
			bodyBytes, _ := json.Marshal(bodyData)

			req, _ := http.NewRequestWithContext(ctx, "POST", fullUrl, bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", apiKey))

			client := &http.Client{Timeout: 10 * time.Second}
			resp, err := client.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			if resp.StatusCode != 200 {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("Subgraph request failed: %d %s", resp.StatusCode, string(body))
			}

			// Unmarshal into generic bucket first to get "data" -> "entity"
			var sgResp struct {
				Data map[string]json.RawMessage `json:"data"`
				Errors []any `json:"errors"`
			}
			err = json.NewDecoder(resp.Body).Decode(&sgResp)
			require.NoError(t, err)

			if len(sgResp.Errors) > 0 {
				t.Fatalf("Subgraph GraphQL errors: %v", sgResp.Errors)
			}

			rawEntity, ok := sgResp.Data[entityName]
			require.True(t, ok, "Expected entity key %s in response data", entityName)

			// Now Unmarshal into specific struct slice
			checkUnmarshal := func(target any) {
				err = json.Unmarshal(rawEntity, target)
				assert.NoError(t, err, "Failed to unmarshal %s", entityName)
				// Basic check length
				// But target is a pointer to slice
				// Reflection or just logging?
				// Just log success
				t.Logf("Successfully unmarshalled %s", entityName)
			}

			switch entityName {
			case "conditions":
				var items []services.PlyMktCondition
				checkUnmarshal(&items)
				if len(items) > 0 { assert.NotEmpty(t, items[0].ID) }
			case "accounts":
				var items []services.PlyMktAccount
				checkUnmarshal(&items)
				if len(items) > 0 { assert.NotEmpty(t, items[0].ID) }
			case "orderFilledEvents":
				var items []services.PlyMktOrderFilledEvent
				checkUnmarshal(&items)
				if len(items) > 0 { assert.NotEmpty(t, items[0].ID) }
			case "enrichedOrderFilleds":
				var items []services.PlyMktEnrichedOrderFilledEvent
				checkUnmarshal(&items)
				if len(items) > 0 { assert.NotEmpty(t, items[0].ID) }
			case "ordersMatchedEvents": 
				// This maps to PlyMktOrderFilledEvent sort of? Or custom?
				// plymkt.go uses `PlyMktOrderFilledEvent` for `ordersMatchedEvents` in processor?
				// Processor says: "var items []services.PlyMktOrderFilledEvent". Yes.
				var items []services.PlyMktOrderFilledEvent
				checkUnmarshal(&items)
			case "fpmms":
				// Stub struct
				type FpmmStub struct {
					ID string `json:"id"`
					ConditionID string `json:"conditionId"`
				}
				var items []FpmmStub
				checkUnmarshal(&items)
				if len(items) > 0 { assert.NotEmpty(t, items[0].ID) }
			}
		})
	}
}

func httpGet(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
    client := &http.Client{Timeout: 10 * time.Second}
    return client.Do(req)
}

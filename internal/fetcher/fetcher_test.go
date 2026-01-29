package fetcher

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/url"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildNextPageRequest_Subgraph(t *testing.T) {
	// Setup generic logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	f := &Fetcher{logger: logger}

	// Setup initial request
	query := "query { items { id } }"
	initialBody := map[string]any{
		"query": query,
		"variables": map[string]int{
			"first": 1000,
			"skip": 0,
		},
	}
	// We don't actually need the body reader for BuildNextPageRequest logic check
	// unless we want to verify it doesn't crash on nil body?
	// But BuildNextPageRequest doesn't read Input Body, it generates New Body.

	req := &Request{
		URL:    "http://test.com/graphql",
		Method: "POST",
		Params: url.Values{
			"limit":  []string{"1000"},
			"offset": []string{"0"},
		},
		Metadata: map[string]string{
			"GraphqlQuery": query,
		},
	}

	// Case 1: Next Page (Full Page)
	itemCount := 1000
	nextReq := f.BuildNextPageRequest(req, itemCount)
	require.NotNil(t, nextReq)

	// Verify URL - should NOT have query params
	assert.Equal(t, "http://test.com/graphql", nextReq.URL)
	
	// Verify Params - should have updated offset
	assert.Equal(t, "1000", nextReq.Params.Get("offset"))

	// Verify Body - should be regenerated with new variables
	require.NotNil(t, nextReq.Body)
	bodyBytes, err := io.ReadAll(nextReq.Body)
	require.NoError(t, err)

	var bodyMap map[string]any
	err = json.Unmarshal(bodyBytes, &bodyMap)
	require.NoError(t, err)

	assert.Equal(t, query, bodyMap["query"])
	
	vars, ok := bodyMap["variables"].(map[string]any)
	require.True(t, ok)
	
	// JSON unmarshal numbers to float64 usually
	assert.Equal(t, float64(1000), vars["first"])
	assert.Equal(t, float64(1000), vars["skip"]) // 0 + 1000 = 1000

	// Case 2: Next Page (Partial Page -> Last Page?)
	// If itemCount < limit, it returns nil.
	lastPageReq := f.BuildNextPageRequest(req, 999)
	assert.Nil(t, lastPageReq)
}

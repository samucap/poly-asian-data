package catalog

import (
	"testing"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestEndpoint(t *testing.T) {
	assert.Equal(t, "https://fallback", Endpoint(nil, "gamma", "https://fallback"))
	cfg := &config.Config{}
	cfg.Services.PlyMkt.Endpoints = map[string]any{"gamma": "https://gamma.example"}
	assert.Equal(t, "https://gamma.example", Endpoint(cfg, "gamma", "https://fallback"))
}

func TestConstants(t *testing.T) {
	assert.Equal(t, "102982", CategoriesRootTagID)
	assert.Equal(t, 500, GammaKeysetLimit)
	assert.Equal(t, "1.0", SchemaVersion)
}

package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCatalogNeedsRefresh(t *testing.T) {
	now := time.Date(2026, 7, 16, 12, 0, 0, 0, time.UTC)
	ttl := 24 * time.Hour

	assert.True(t, CatalogNeedsRefresh(time.Time{}, false, now, ttl), "missing watermark")
	assert.True(t, CatalogNeedsRefresh(time.Time{}, true, now, ttl), "zero watermark")
	assert.True(t, CatalogNeedsRefresh(now.Add(-25*time.Hour), true, now, ttl), "stale")
	assert.False(t, CatalogNeedsRefresh(now.Add(-1*time.Hour), true, now, ttl), "fresh")
	assert.False(t, CatalogNeedsRefresh(now.Add(-23*time.Hour), true, now, ttl), "within ttl")
	assert.True(t, CatalogNeedsRefresh(now.Add(-24*time.Hour), true, now, ttl), "exactly ttl")
}

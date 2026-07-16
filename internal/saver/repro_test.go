package saver

import (
	"testing"

	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/stretchr/testify/assert"
)

// TestEnrichedOrderFilledEventShape documents the payload shape used by
// batchInsertEnrichedOrderFilledEvents so service type regressions fail here.
func TestEnrichedOrderFilledEventShape(t *testing.T) {
	items := []services.PlyMktEnrichedOrderFilledEvent{
		{
			ID:              "test-event-1",
			Timestamp:       "1700000000",
			Price:           "0.5",
			Size:            "100.0",
			Side:            "BUY",
			TransactionHash: "0xabc",
			OrderHash:       "0xdef",
		},
	}
	items[0].Maker.ID = "maker-1"
	items[0].Taker.ID = "taker-1"
	items[0].Market.ID = "market-1"

	assert.Equal(t, "maker-1", items[0].Maker.ID)
	assert.Equal(t, "0.5", items[0].Price)
	assert.Equal(t, "100.0", items[0].Size)
}

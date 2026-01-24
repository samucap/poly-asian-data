package saver

import (
	"testing"

	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/stretchr/testify/assert"
)

func init() {
	// Initialize logging for tests
	logging.Init("dev")
}

// =============================================================================
// Stats Tests
// =============================================================================

func TestStats_Snapshot(t *testing.T) {
	t.Run("returns stats snapshot", func(t *testing.T) {
		stats := Stats{}
		stats.RecordsSubmitted.Add(10)
		stats.RecordsSaved.Add(8)
		stats.RecordsFailed.Add(2)

		snapshot := stats.Snapshot()
		assert.Equal(t, int64(10), snapshot.RecordsSubmitted)
		assert.Equal(t, int64(8), snapshot.RecordsSaved)
		assert.Equal(t, int64(2), snapshot.RecordsFailed)
	})
}

package processor

import (
	"context"
	"testing"
	"time"

	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/workerpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	// Initialize logging for tests
	logging.Init("dev")
}

// =============================================================================
// Test Helper
// =============================================================================

// testProcessorResult holds a processor and its output channel for testing.
type testProcessorResult struct {
	processor *Processor
	output    <-chan workerpool.Result[*Output]
	cleanup   func()
}

// newTestProcessor creates a processor for testing.
func newTestProcessor(ctx context.Context, numWorkers, qSize int) (*testProcessorResult, error) {
	p, err := New(ctx, numWorkers, qSize)
	if err != nil {
		return nil, err
	}

	return &testProcessorResult{
		processor: p,
		output:    p.Outputs(),
		cleanup: func() {
			p.Stop()
		},
	}, nil
}

// =============================================================================
// Pool Tests
// =============================================================================

func TestNewProcessor(t *testing.T) {
	t.Run("creates processor with valid config", func(t *testing.T) {
		ctx := context.Background()
		result, err := newTestProcessor(ctx, 2, 10)
		require.NoError(t, err)
		require.NotNil(t, result.processor)
		result.cleanup()
	})
}

func TestProcessor_Submit(t *testing.T) {
	t.Run("submit to stopped pool returns error", func(t *testing.T) {
		ctx := context.Background()
		result, _ := newTestProcessor(ctx, 2, 10)
		result.cleanup() // Stop the pool

		err := result.processor.Submit(&Input{ID: "test"})
		assert.Error(t, err)
	})
}

// =============================================================================
// WorkerTask Tests
// =============================================================================

func TestProcessor_WorkerTask(t *testing.T) {
	t.Run("successful processing via WorkerTask", func(t *testing.T) {
		ctx := context.Background()
		p, err := New(ctx, 2, 10)
		require.NoError(t, err)
		defer p.Stop()

		input := &Input{
			ID:        "test-1",
			SourceURL: "http://example.com",
			Data:      []byte("hello"),
			FetchedAt: time.Now(),
		}

		output, err := p.WorkerTask(ctx, input)
		require.NoError(t, err)
		assert.Equal(t, "test-1", output.InputID)
		assert.Equal(t, "http://example.com", output.SourceURL)
		assert.Equal(t, "processorSuccess", output.Data)
		assert.False(t, output.ProcessedAt.IsZero())
	})
}

// =============================================================================
// Stats Tests
// =============================================================================

func TestProcessor_Stats(t *testing.T) {
	ctx := context.Background()
	result, _ := newTestProcessor(ctx, 2, 10)
	defer result.cleanup()

	stats := result.processor.ProcessorStats().Snapshot()
	// Just verify we can get stats without panic
	assert.GreaterOrEqual(t, stats.ItemsSubmitted, int64(0))
}

// =============================================================================
// Shutdown Tests
// =============================================================================

func TestProcessor_Stop(t *testing.T) {
	t.Run("stop is idempotent", func(t *testing.T) {
		ctx := context.Background()
		result, _ := newTestProcessor(ctx, 1, 10)

		// Multiple stops should not panic
		result.cleanup()
		result.processor.Stop()
		result.processor.Stop()
	})
}

// =============================================================================
// Placeholder Processors Tests
// =============================================================================

func TestPassthroughProcessor(t *testing.T) {
	ctx := context.Background()
	input := &Input{Data: []byte("test data")}

	result, err := PassthroughProcessor(ctx, input)

	assert.NoError(t, err)
	assert.Equal(t, []byte("test data"), result)
}

func TestJSONProcessor(t *testing.T) {
	ctx := context.Background()
	input := &Input{Data: []byte(`{"key":"value"}`)}

	result, err := JSONProcessor(ctx, input)

	// Currently just passes through
	assert.NoError(t, err)
	assert.Equal(t, []byte(`{"key":"value"}`), result)
}

package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	// Initialize logging for tests
	logging.Init("dev")
}

// =============================================================================
// Pipeline Tests
// =============================================================================

func createTestConfig() *config.Config {
	return &config.Config{
		ENV:         "dev",
		PostgresURL: "postgres://localhost:5432/test",
		FetcherCfg: config.PoolCfg{
			NumWorkers: 2,
			Qsize:      10,
		},
		ProcessorCfg: config.PoolCfg{
			NumWorkers: 2,
			Qsize:      10,
		},
	}
}

func TestNew(t *testing.T) {
	t.Run("creates pipeline with valid config", func(t *testing.T) {
		ctx := context.Background()
		cfg := createTestConfig()

		p, err := New(ctx, cfg)
		require.NoError(t, err)
		require.NotNil(t, p)
		assert.False(t, p.IsStopped())
		p.Stop()
	})
}

// =============================================================================
// Shutdown Tests
// =============================================================================

func TestPipeline_Stop(t *testing.T) {
	t.Run("graceful stop", func(t *testing.T) {
		ctx := context.Background()
		cfg := createTestConfig()
		p, err := New(ctx, cfg)
		require.NoError(t, err)

		p.Stop()

		assert.True(t, p.IsStopped())
	})

	t.Run("stop is idempotent", func(t *testing.T) {
		ctx := context.Background()
		cfg := createTestConfig()
		p, err := New(ctx, cfg)
		require.NoError(t, err)

		p.Stop()
		p.Stop()
		p.Stop()

		assert.True(t, p.IsStopped())
	})
}

// =============================================================================
// Stats Tests
// =============================================================================

func TestPipeline_Stats(t *testing.T) {
	ctx := context.Background()
	cfg := createTestConfig()
	p, err := New(ctx, cfg)
	require.NoError(t, err)
	defer p.Stop()

	stats := p.Stats()

	assert.False(t, stats.StartedAt.IsZero())
	assert.GreaterOrEqual(t, stats.UptimeDuration, time.Duration(0))
}

func TestPipeline_WaitUntilIdle(t *testing.T) {
	ctx := context.Background()
	cfg := createTestConfig()
	p, err := New(ctx, cfg)
	// If DB connection fails,skip test (common in envs without DB)
	if err != nil {
		t.Skip("Skipping WaitUntilIdle test due to missing DB connection")
	}
	require.NoError(t, err)
	defer p.Stop()

	// Should return almost immediately for an idle pipeline
	done := make(chan struct{})
	go func() {
		p.WaitUntilIdle(ctx, 100*time.Millisecond)
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(2 * time.Second):
		t.Fatal("WaitUntilIdle timed out on idle pipeline")
	}
}

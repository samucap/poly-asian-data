package edgescan

import (
	"testing"

	"github.com/samucap/poly-asian-data/internal/attention"
	"github.com/stretchr/testify/require"
)

func TestDefaultAttention_DelegatesToStage1(t *testing.T) {
	p := DefaultAttention()
	require.Equal(t, attention.ActivityThenEdge, p.Name())
	// Same entry as SelectStage1 — empty pool is a valid no-op enrich set.
	got := p.EnrichSet(nil, BuildOptions{})
	want := SelectStage1(nil, BuildOptions{})
	require.Equal(t, want.Stage1Count, got.Stage1Count)
	require.Equal(t, want.PoolCount, got.PoolCount)
}

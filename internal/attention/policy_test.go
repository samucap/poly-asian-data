package attention

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultName(t *testing.T) {
	require.Equal(t, ActivityThenEdge, DefaultName())
}

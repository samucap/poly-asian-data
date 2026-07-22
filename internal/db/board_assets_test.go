package db

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCapTokenIDs(t *testing.T) {
	in := []string{"a", "b", "c", "d"}
	require.Equal(t, []string{"a", "b"}, CapTokenIDs(in, 2))
	require.Equal(t, in, CapTokenIDs(in, 10))
	require.Equal(t, in, CapTokenIDs(in, 0))
}

func TestParseClobTokenIDs(t *testing.T) {
	require.Equal(t, []string{"111", "222"}, parseClobTokenIDs(`["111","222"]`))
	require.Equal(t, []string{"abc"}, parseClobTokenIDs("abc"))
	require.Empty(t, parseClobTokenIDs(""))
}

func TestCleanTokens(t *testing.T) {
	require.Equal(t, []string{"a", "b"}, cleanTokens([]string{" a ", "", "b"}))
}

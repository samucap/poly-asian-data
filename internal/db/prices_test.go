package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNormalizePriceTS(t *testing.T) {
	sec := NormalizePriceTS(1_700_000_000)
	require.Equal(t, int64(1_700_000_000), sec.Unix())

	ms := NormalizePriceTS(1_700_000_000_000)
	require.Equal(t, int64(1_700_000_000), ms.Unix())

	require.True(t, NormalizePriceTS(0).IsZero())
}

func TestPrimaryTokenFromJSON(t *testing.T) {
	require.Equal(t, "abc", primaryTokenFromJSON(`["abc","def"]`))
	require.Equal(t, "plain", primaryTokenFromJSON("plain"))
	require.Equal(t, "", primaryTokenFromJSON(""))
	require.Equal(t, "x", primaryTokenFromJSON(`  ["x"]  `))
}

func TestPriceTSUnix(t *testing.T) {
	tt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	require.Equal(t, tt.Unix(), PriceTSUnix(tt))
	require.Equal(t, int64(0), PriceTSUnix(time.Time{}))
}

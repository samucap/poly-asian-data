package enrich

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseBooks(t *testing.T) {
	raw := []byte(`[
	  {
	    "market": "m1",
	    "asset_id": "tok1",
	    "bids": [{"price":"0.40","size":"100"},{"price":"0.39","size":"50"}],
	    "asks": [{"price":"0.45","size":"80"},{"price":"0.46","size":"20"}],
	    "neg_risk": false
	  }
	]`)
	snaps, err := parseBooks(raw, time.Now().UTC())
	require.NoError(t, err)
	require.Len(t, snaps, 1)
	require.Equal(t, "tok1", snaps[0].TokenID)
	require.InDelta(t, 0.40, snaps[0].BestBid, 1e-9)
	require.InDelta(t, 0.45, snaps[0].BestAsk, 1e-9)
	require.InDelta(t, 150, snaps[0].TotalBidDepth, 1e-9)
	require.InDelta(t, 100, snaps[0].TotalAskDepth, 1e-9)
	require.InDelta(t, 0.6, snaps[0].Imbalance, 1e-9)
}

func TestParseOI(t *testing.T) {
	now := time.Now().UTC()
	pts, err := parseOI([]byte(`[{"market":"c1","value":1234.5}]`), now)
	require.NoError(t, err)
	require.Len(t, pts, 1)
	require.Equal(t, "c1", pts[0].ConditionID)
	require.InDelta(t, 1234.5, pts[0].Value, 1e-9)
}

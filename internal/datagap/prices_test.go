package datagap

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFillPricesLinearInterp(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := t0.Add(2 * time.Hour)
	t1 := t0.Add(time.Hour)
	real := []PricePoint{
		{Time: t0, Price: 0.40, Source: SourceVenue},
		{Time: t2, Price: 0.60, Source: SourceVenue},
	}
	grid := []time.Time{t1}
	out, mix := FillPrices(real, grid, DefaultOpts())
	require.Equal(t, 1, mix.SynthInterp)
	require.Equal(t, 2, mix.Venue)
	var mid float64
	for _, p := range out {
		if p.Time.Equal(t1) {
			mid = p.Price
			require.Equal(t, SourceSynthInterp, p.Source)
		}
	}
	require.InDelta(t, 0.50, mid, 1e-9)
}

func TestFillPricesMaxGapNoFill(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tFar := t0.Add(100 * time.Hour)
	tMid := t0.Add(50 * time.Hour)
	opts := DefaultOpts()
	opts.MaxGap = 48 * time.Hour
	opts.HoldMax = time.Hour // too short for 50h
	real := []PricePoint{
		{Time: t0, Price: 0.4},
		{Time: tFar, Price: 0.6},
	}
	_, mix := FillPrices(real, []time.Time{tMid}, opts)
	require.Equal(t, 0, mix.SynthInterp)
	require.Equal(t, 1, mix.Missing)
}

func TestFillPricesHold(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(2 * time.Hour)
	opts := DefaultOpts()
	opts.HoldMax = 6 * time.Hour
	real := []PricePoint{{Time: t0, Price: 0.55}}
	out, mix := FillPrices(real, []time.Time{t1}, opts)
	require.Equal(t, 1, mix.SynthHold)
	for _, p := range out {
		if p.Source == SourceSynthHold {
			require.InDelta(t, 0.55, p.Price, 1e-9)
		}
	}
}

func TestReportSignificantBlocksPromote(t *testing.T) {
	r := Report{
		PriceMix: Mix{Venue: 10, SynthInterp: 90},
	}
	r.Finalize(PromoteMaxSynthShare, SignificantSynthShare)
	require.True(t, r.Significant)
	require.True(t, r.BlockPromote)
	require.Contains(t, r.Warning, "SYNTHETIC")
	require.NotEmpty(t, r.Banner())
}

func TestFillBooksFromMid(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	midAt := func(t time.Time) (float64, bool) { return 0.50, true }
	out, mix := FillBooks(nil, []time.Time{t0}, midAt, DefaultOpts())
	require.Equal(t, 1, mix.SynthBook)
	require.Len(t, out, 1)
	require.Less(t, out[0].BestBid, out[0].BestAsk)
	require.InDelta(t, 0.50, (out[0].BestBid+out[0].BestAsk)/2, 0.02)
}

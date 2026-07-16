package marketranking

import (
	"testing"
	"time"

	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sampleMarket(id string, vol24, liq, spread, vola float64, active bool) *services.PlyMktMarket {
	return &services.PlyMktMarket{
		ID:                id,
		ConditionID:       "cond-" + id,
		Active:            active,
		Closed:            false,
		Volume24hr:        vol24,
		LiquidityClob:     liq,
		Spread:            spread,
		OneDayPriceChange: vola,
		ClobTokenIds:      `["token-a","token-b"]`,
		Volume:            "1000000",
		VolumeNum:         1_000_000,
		EndDate:           time.Now().Add(10 * 24 * time.Hour),
		EnableOrderBook:   true,
		AcceptingOrders:   true,
	}
}

func TestRankMarkets_FiltersAndOrdersByScore(t *testing.T) {
	filter := MarketFilter{
		MinVolume24hr: 30_000,
		MinLiquidity:  15_000,
		MaxSpread:     0.05,
		MinVolatility: 0.01,
		MaxN:          10,
	}

	high := sampleMarket("high", 500_000, 200_000, 0.01, 0.05, true)
	mid := sampleMarket("mid", 100_000, 50_000, 0.03, 0.02, true)
	lowVol := sampleMarket("lowvol", 1_000, 50_000, 0.02, 0.05, true)
	wide := sampleMarket("wide", 100_000, 50_000, 0.20, 0.05, true)
	inactive := sampleMarket("inact", 50_000, 20_000, 0.02, 0.02, false)

	ranked := RankMarkets([]*services.PlyMktMarket{mid, lowVol, high, wide, inactive}, filter)
	require.NotEmpty(t, ranked)

	ids := make([]string, len(ranked))
	for i, m := range ranked {
		ids[i] = m.ID
	}
	assert.NotContains(t, ids, "lowvol")
	assert.NotContains(t, ids, "wide")
	assert.Equal(t, "high", ranked[0].ID)
	assert.Greater(t, ranked[0].ComputedScore, 0.0)
	if len(ranked) > 1 {
		assert.GreaterOrEqual(t, ranked[0].ComputedScore, ranked[1].ComputedScore)
	}
}

func TestRankMarkets_RespectsMaxN(t *testing.T) {
	filter := MarketFilter{
		MinVolume24hr: 0,
		MinLiquidity:  0,
		MaxSpread:     1,
		MinVolatility: 0,
		MaxN:          2,
	}
	var mkts []*services.PlyMktMarket
	for i := 0; i < 5; i++ {
		mkts = append(mkts, sampleMarket(string(rune('a'+i)), float64(100_000+i*10_000), 50_000, 0.02, 0.02, true))
	}
	ranked := RankMarkets(mkts, filter)
	require.Len(t, ranked, 2)
}

func TestComputeScore_ZeroWhenInactiveOrNoTokens(t *testing.T) {
	maxVals := ScoreMaxima{MaxVol24hr: 100, MaxLiquidity: 100, MaxVol: 100, MaxVolatility: 0.1}
	w := DefaultScoreWeights()
	w.UseEnvWeights = false

	m := sampleMarket("x", 50, 50, 0.02, 0.05, false)
	assert.Equal(t, 0.0, ComputeScore(*m, maxVals, w))

	m2 := sampleMarket("y", 50, 50, 0.02, 0.05, true)
	m2.ClobTokenIds = ""
	assert.Equal(t, 0.0, ComputeScore(*m2, maxVals, w))
}

func TestComputeScore_HigherVolumeScoresHigher(t *testing.T) {
	maxVals := ScoreMaxima{MaxVol24hr: 100_000, MaxLiquidity: 50_000, MaxVol: 1e6, MaxVolatility: 0.1}
	w := DefaultScoreWeights()
	w.UseEnvWeights = false

	low := sampleMarket("l", 20_000, 50_000, 0.02, 0.05, true)
	high := sampleMarket("h", 100_000, 50_000, 0.02, 0.05, true)
	assert.Greater(t, ComputeScore(*high, maxVals, w), ComputeScore(*low, maxVals, w))
}

func TestComputeScore_TighterSpreadScoresHigher(t *testing.T) {
	maxVals := ScoreMaxima{MaxVol24hr: 100_000, MaxLiquidity: 50_000, MaxVol: 1e6, MaxVolatility: 0.1}
	w := DefaultScoreWeights()
	w.UseEnvWeights = false

	tight := sampleMarket("t", 50_000, 50_000, 0.01, 0.05, true)
	loose := sampleMarket("l", 50_000, 50_000, 0.08, 0.05, true)
	assert.Greater(t, ComputeScore(*tight, maxVals, w), ComputeScore(*loose, maxVals, w))
}

// Package marketranking provides pure market filter, score, and rank helpers
// used by top-markets (and tests) without network or DB dependencies.
package marketranking

import (
	"math"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/samucap/poly-asian-data/internal/services"
)

// MarketFilter thresholds for selecting top markets (adjustable per category).
type MarketFilter struct {
	MinVolume24hr float64
	MinLiquidity  float64
	MaxSpread     float64
	MinVolatility float64
	MaxN          int
}

// ScoreMaxima holds per-batch max values used to normalize ComputeScore.
type ScoreMaxima struct {
	MaxVol24hr    float64
	MaxLiquidity  float64
	MaxVol        float64
	MaxVolatility float64
}

// ScoreWeights are relative field weights for ComputeScore.
// Overridable via SCORE_W_* env vars when UseEnvWeights is true.
type ScoreWeights struct {
	Vol24hr       float64
	Liquidity     float64
	Spread        float64
	Volatility    float64
	TimeLeft      float64
	UseEnvWeights bool
}

// DefaultScoreWeights is the sports-tuned baseline (ground.go / top-markets).
func DefaultScoreWeights() ScoreWeights {
	return ScoreWeights{
		Vol24hr:       0.45,
		Liquidity:     0.25,
		Spread:        0.15,
		Volatility:    0.08,
		TimeLeft:      0.07,
		UseEnvWeights: true,
	}
}

func (w ScoreWeights) resolved() ScoreWeights {
	if !w.UseEnvWeights {
		return w
	}
	if v := os.Getenv("SCORE_W_VOL24HR"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			w.Vol24hr = f
		}
	}
	if v := os.Getenv("SCORE_W_LIQUIDITY"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			w.Liquidity = f
		}
	}
	if v := os.Getenv("SCORE_W_SPREAD"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			w.Spread = f
		}
	}
	if v := os.Getenv("SCORE_W_VOLATILITY"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			w.Volatility = f
		}
	}
	if v := os.Getenv("SCORE_W_TIMELEFT"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			w.TimeLeft = f
		}
	}
	return w
}

func marketLiquidity(m *services.PlyMktMarket) float64 {
	if m.LiquidityClob != 0 {
		return m.LiquidityClob
	}
	if m.LiquidityNum != 0 {
		return m.LiquidityNum
	}
	if val, err := strconv.ParseFloat(m.Liquidity, 64); err == nil {
		return val
	}
	return 0
}

// RankMarkets filters, scores, sorts by score desc, and returns top N.
// Mutates candidates' ComputedScore and LastFetched fields.
func RankMarkets(markets []*services.PlyMktMarket, filter MarketFilter) []*services.PlyMktMarket {
	return RankMarketsWithWeights(markets, filter, DefaultScoreWeights())
}

// RankMarketsWithWeights is RankMarkets with explicit scoring weights.
func RankMarketsWithWeights(markets []*services.PlyMktMarket, filter MarketFilter, weights ScoreWeights) []*services.PlyMktMarket {
	var candidates []*services.PlyMktMarket
	maxVals := ScoreMaxima{}

	for _, m := range markets {
		if m == nil {
			continue
		}
		liq := marketLiquidity(m)

		if m.Volume24hr < filter.MinVolume24hr ||
			liq < filter.MinLiquidity ||
			m.Spread > filter.MaxSpread ||
			math.Abs(m.OneDayPriceChange) < filter.MinVolatility {
			continue
		}

		candidates = append(candidates, m)

		if m.Volume24hr > maxVals.MaxVol24hr {
			maxVals.MaxVol24hr = m.Volume24hr
		}
		if liq > maxVals.MaxLiquidity {
			maxVals.MaxLiquidity = liq
		}
		vol, _ := strconv.ParseFloat(m.Volume, 64)
		if vol == 0 {
			vol = m.VolumeNum
		}
		if vol > maxVals.MaxVol {
			maxVals.MaxVol = vol
		}
		vola := math.Abs(m.OneDayPriceChange)
		if vola > maxVals.MaxVolatility {
			maxVals.MaxVolatility = vola
		}
	}

	if len(candidates) == 0 {
		return nil
	}

	weights = weights.resolved()
	now := time.Now()
	for _, m := range candidates {
		m.ComputedScore = ComputeScore(*m, maxVals, weights)
		m.LastFetched = now
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].ComputedScore > candidates[j].ComputedScore
	})

	if filter.MaxN > 0 && len(candidates) > filter.MaxN {
		return candidates[:filter.MaxN]
	}
	return candidates
}

// ComputeScore ranks a market for opportunity (higher = more attractive).
func ComputeScore(m services.PlyMktMarket, maxVals ScoreMaxima, weights ScoreWeights) float64 {
	if !m.Active || m.Closed || len(m.ClobTokenIds) == 0 {
		return 0.0
	}

	weights = weights.resolved()
	var score float64

	if maxVals.MaxVol24hr > 0 {
		score += (m.Volume24hr / maxVals.MaxVol24hr) * weights.Vol24hr
	}

	liq := marketLiquidity(&m)
	if maxVals.MaxLiquidity > 0 {
		score += (liq / maxVals.MaxLiquidity) * weights.Liquidity
	}

	if m.Spread > 0 {
		score += (1.0 - math.Min(m.Spread/0.10, 1.0)) * weights.Spread
	} else {
		score += weights.Spread
	}

	vola := math.Abs(m.OneDayPriceChange)
	if maxVals.MaxVolatility > 0 {
		score += (vola / maxVals.MaxVolatility) * weights.Volatility
	}

	if !m.EndDate.IsZero() {
		daysLeft := time.Until(m.EndDate).Hours() / 24.0
		if daysLeft > 0 {
			switch {
			case daysLeft >= 4 && daysLeft <= 21:
				score += weights.TimeLeft
			case daysLeft > 21:
				score += (1.0 - math.Min((daysLeft-21)/180.0, 1.0)) * weights.TimeLeft * 0.7
			default:
				score += (daysLeft / 4.0) * weights.TimeLeft * 0.5
			}
		}
	}

	return score
}

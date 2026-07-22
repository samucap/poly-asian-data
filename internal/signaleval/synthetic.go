package signaleval

import (
	"time"

	"github.com/samucap/poly-asian-data/internal/risk"
)

// SyntheticFixture returns deterministic signals + prices for CI / --synthetic.
// Includes a high-edge and low-edge signal at the same time to exercise ranking.
func SyntheticFixture() (signals []SignalIn, prices PriceIndex) {
	t0 := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	signals = []SignalIn{
		{
			Time: t0, SignalID: "11111111-1111-4111-8111-111111111101",
			Strategy: "default", ConditionID: "cond_high", TokenID: "tok_high",
			Side: "BUY", EdgeBps: 400, Conviction: 0.9, SizeUSD: 100, CapacityUSD: 50_000,
			CostBps: 20, Mid: 0.40, HorizonSec: 3600,
		},
		{
			Time: t0, SignalID: "11111111-1111-4111-8111-111111111102",
			Strategy: "default", ConditionID: "cond_low", TokenID: "tok_low",
			Side: "BUY", EdgeBps: 20, Conviction: 0.4, SizeUSD: 100, CapacityUSD: 50_000,
			CostBps: 20, Mid: 0.50, HorizonSec: 3600,
		},
		{
			Time: t0.Add(2 * time.Hour), SignalID: "11111111-1111-4111-8111-111111111103",
			Strategy: "default", ConditionID: "cond_mid", TokenID: "tok_mid",
			Side: "BUY", EdgeBps: 150, Conviction: 0.7, SizeUSD: 80, CapacityUSD: 50_000,
			CostBps: 15, Mid: 0.55, HorizonSec: 3600,
		},
		// Opposing residual later
		{
			Time: t0.Add(5 * time.Hour), SignalID: "11111111-1111-4111-8111-111111111104",
			Strategy: "default", ConditionID: "cond_sell", TokenID: "tok_sell",
			Side: "SELL", EdgeBps: -120, Conviction: 0.6, SizeUSD: 60, CapacityUSD: 50_000,
			CostBps: 15, Mid: 0.60, HorizonSec: 3600,
		},
	}

	// Prices: high token rises (BUY wins); low flat; mid up a bit; sell token drops (SELL wins)
	prices = PriceIndex{
		"tok_high": {
			{Time: t0, Mid: 0.40},
			{Time: t0.Add(time.Hour), Mid: 0.48},
			{Time: t0.Add(2 * time.Hour), Mid: 0.50},
		},
		"tok_low": {
			{Time: t0, Mid: 0.50},
			{Time: t0.Add(time.Hour), Mid: 0.50},
		},
		"tok_mid": {
			{Time: t0.Add(2 * time.Hour), Mid: 0.55},
			{Time: t0.Add(3 * time.Hour), Mid: 0.58},
		},
		"tok_sell": {
			{Time: t0.Add(5 * time.Hour), Mid: 0.60},
			{Time: t0.Add(6 * time.Hour), Mid: 0.52},
		},
	}
	return signals, prices
}

// TightBudgetRisk returns a risk config that forces budget competition on synthetic.
func TightBudgetRisk() risk.Config {
	cfg := risk.DefaultConfig()
	cfg.StartingEquityUSD = 10_000
	cfg.MaxGrossUSD = 100 // only one ~full size trade fits
	cfg.MaxPositionUSD = 100
	cfg.MinSizeUSD = 50 // remaining gross after first trade is unusable
	cfg.SizingMode = risk.SizingSignalSize
	cfg.BatchWindowMs = 1000 // rank simultaneous signals by opportunity
	cfg.MaxDailyDrawdownBps = 5000
	cfg.MaxDrawdownBps = 10000
	return cfg
}

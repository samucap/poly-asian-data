package signaleval

import (
	"math"
	"time"
)

// EntryFill computes shares and cost USD from size and mid.
func EntryFill(side string, sizeUSD, mid, costBps float64) (shares, costUSD, entryMid float64) {
	if mid <= 0 || sizeUSD <= 0 {
		return 0, 0, mid
	}
	entryMid = mid
	shares = sizeUSD / mid
	costUSD = sizeUSD * math.Abs(costBps) / 10_000
	return shares, costUSD, entryMid
}

// RealizePnL computes PnL for a YES-token paper trade.
// BUY: shares*(exit-entry) - costs; SELL: shares*(entry-exit) - costs.
func RealizePnL(side string, shares, entryMid, exitMid, costUSD float64) float64 {
	if shares <= 0 {
		return -costUSD
	}
	var gross float64
	switch side {
	case "SELL":
		gross = shares * (entryMid - exitMid)
	default: // BUY
		gross = shares * (exitMid - entryMid)
	}
	return gross - costUSD
}

// MidAsOf returns last mid at or before t, or 0 if none.
func MidAsOf(points []PricePoint, t time.Time) float64 {
	if len(points) == 0 {
		return 0
	}
	// linear scan is fine for synthetic/small; points assumed sorted ascending
	var best float64
	found := false
	for _, p := range points {
		if p.Time.After(t) {
			break
		}
		best = p.Mid
		found = true
	}
	if !found {
		return 0
	}
	return best
}

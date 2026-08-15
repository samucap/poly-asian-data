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

// SpotCheck is M2 hygiene: signal mid vs price series as-of T.
type SpotCheck struct {
	SignalID     string  `json:"signal_id"`
	ConditionID  string  `json:"condition_id"`
	SignalMid    float64 `json:"signal_mid"`
	SeriesMid    float64 `json:"series_mid"`
	CostBps      float64 `json:"cost_bps"`
	OK           bool    `json:"ok"`
	Detail       string  `json:"detail,omitempty"`
}

// SpotCheckFills compares the first n signals' mids to the price index (join sanity).
func SpotCheckFills(sigs []SignalIn, prices PriceIndex, n int) []SpotCheck {
	if n <= 0 {
		n = 2
	}
	if n > len(sigs) {
		n = len(sigs)
	}
	out := make([]SpotCheck, 0, n)
	for i := 0; i < n; i++ {
		s := sigs[i]
		sc := SpotCheck{SignalID: s.SignalID, ConditionID: s.ConditionID, SignalMid: s.Mid, CostBps: s.CostBps}
		sc.SeriesMid = MidAsOf(prices[s.TokenID], s.Time)
		switch {
		case s.Mid <= 0 && sc.SeriesMid <= 0:
			sc.OK = false
			sc.Detail = "no mid on signal or series"
		case s.Mid <= 0 && sc.SeriesMid > 0:
			sc.OK = true
			sc.Detail = "signal mid empty; series has mid (eval uses series)"
		case sc.SeriesMid <= 0:
			sc.OK = true
			sc.Detail = "no series point at T; eval uses signal mid"
		default:
			rel := math.Abs(s.Mid-sc.SeriesMid) / sc.SeriesMid
			sc.OK = rel < 0.05
			if !sc.OK {
				sc.Detail = "signal mid vs series mid disagree >5%"
			}
		}
		out = append(out, sc)
	}
	return out
}

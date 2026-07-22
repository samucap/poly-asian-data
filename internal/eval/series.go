package eval

import (
	"math"
	"sort"
	"time"
)

// PriceSeries is token mid history for labels + PIT features.
// Times must be sorted ascending.
type PriceSeries struct {
	TokenID string
	Times   []time.Time
	Prices  []float64
}

// MidAsOf returns last price at or before t (ok=false if none).
func (s PriceSeries) MidAsOf(t time.Time) (float64, bool) {
	i := s.indexAtOrBefore(t)
	if i < 0 {
		return 0, false
	}
	return s.Prices[i], true
}

// indexAtOrBefore is last index with Times[i] <= t, or -1.
func (s PriceSeries) indexAtOrBefore(t time.Time) int {
	if len(s.Times) == 0 {
		return -1
	}
	return sort.Search(len(s.Times), func(i int) bool {
		return s.Times[i].After(t)
	}) - 1
}

// AbsRet returns |mid(t)/mid(t-window) - 1| when both mids exist, else 0.
func (s PriceSeries) AbsRet(t time.Time, window time.Duration) float64 {
	midT, ok := s.MidAsOf(t)
	if !ok || midT <= 0 {
		return 0
	}
	mid0, ok := s.MidAsOf(t.Add(-window))
	if !ok || mid0 <= 0 {
		return 0
	}
	return math.Abs(midT/mid0 - 1)
}

// PointCount returns number of samples in (t-window, t].
func (s PriceSeries) PointCount(t time.Time, window time.Duration) int {
	if len(s.Times) == 0 || window <= 0 {
		return 0
	}
	lo := t.Add(-window)
	// first index with Times[i] > lo
	i0 := sort.Search(len(s.Times), func(i int) bool {
		return s.Times[i].After(lo)
	})
	i1 := s.indexAtOrBefore(t)
	if i1 < i0 {
		return 0
	}
	return i1 - i0 + 1
}

// PathActivity is sum of |Δmid| over samples in (t-window, t] — PIT “tape activity”.
func (s PriceSeries) PathActivity(t time.Time, window time.Duration) float64 {
	if len(s.Times) < 2 || window <= 0 {
		return 0
	}
	lo := t.Add(-window)
	i0 := sort.Search(len(s.Times), func(i int) bool {
		return s.Times[i].After(lo)
	})
	i1 := s.indexAtOrBefore(t)
	if i1 <= i0 {
		return 0
	}
	var sum float64
	for i := i0 + 1; i <= i1; i++ {
		sum += math.Abs(s.Prices[i] - s.Prices[i-1])
	}
	return sum
}

// BookPoint is one stored top-of-book sample.
type BookPoint struct {
	Time          time.Time
	BestBid       float64
	BestAsk       float64
	TotalBidDepth float64
	TotalAskDepth float64
}

// BookSeries is ascending book history for one token.
type BookSeries struct {
	TokenID string
	Points  []BookPoint
}

// AsOf returns last book at or before t within maxAge (0 = any age).
func (s BookSeries) AsOf(t time.Time, maxAge time.Duration) (BookPoint, bool) {
	if len(s.Points) == 0 {
		return BookPoint{}, false
	}
	i := sort.Search(len(s.Points), func(i int) bool {
		return s.Points[i].Time.After(t)
	}) - 1
	if i < 0 {
		return BookPoint{}, false
	}
	p := s.Points[i]
	if maxAge > 0 && t.Sub(p.Time) > maxAge {
		return BookPoint{}, false
	}
	return p, true
}

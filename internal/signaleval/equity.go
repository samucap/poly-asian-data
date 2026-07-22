package signaleval

import (
	"math"
	"sort"
	"time"
)

// MinHoursForSharpe is the minimum hourly period returns required to report annualized Sharpe.
// Short paper windows should trust total_pnl / hit_rate / max_dd instead.
const MinHoursForSharpe = 72 // ~3 days of hourly marks


// EquityStats from a path of equity levels (chronological).
type EquityStats struct {
	Sharpe              float64
	SharpeNote          string  // empty | insufficient_periods | zero_variance
	MeanPeriodReturn    float64 // raw hourly mean return (not annualized)
	PeriodReturnStdev   float64 // raw hourly stdev
	NPeriods            int     // hourly returns used
	MaxDrawdownBps      float64
	MaxDailyDrawdownBps float64
	TotalReturnBps      float64
	PeriodsPerYear      float64 // annualization factor used for Sharpe (24*365 when hourly)
}

// ComputeEquityStats derives DD on the raw path and Sharpe on **hourly resampled** equity.
// This avoids event-sparse paths producing absurd annualized Sharpe (√8760 on a handful of trades).
func ComputeEquityStats(points []EquityPoint, startingEquity, periodsPerYear float64) EquityStats {
	out := EquityStats{PeriodsPerYear: periodsPerYear}
	if periodsPerYear <= 0 {
		out.PeriodsPerYear = 24 * 365
		periodsPerYear = out.PeriodsPerYear
	}
	if startingEquity <= 0 || len(points) == 0 {
		out.SharpeNote = "insufficient_periods"
		return out
	}
	end := points[len(points)-1].Equity
	out.TotalReturnBps = (end - startingEquity) / startingEquity * 10_000

	// Max DD on full event path (true peak-to-trough)
	peak := points[0].Equity
	var maxDD float64
	for _, p := range points {
		if p.Equity > peak {
			peak = p.Equity
		}
		if peak > 0 {
			dd := (peak - p.Equity) / peak * 10_000
			if dd > maxDD {
				maxDD = dd
			}
		}
	}
	out.MaxDrawdownBps = maxDD
	out.MaxDailyDrawdownBps = maxDailyDrawdownBps(points)

	// Hourly grid for period returns / Sharpe
	hourly := ResampleHourly(points)
	if len(hourly) < 2 {
		out.SharpeNote = "insufficient_periods"
		return out
	}
	var rets []float64
	for i := 1; i < len(hourly); i++ {
		prev := hourly[i-1].Equity
		if prev <= 0 {
			continue
		}
		rets = append(rets, (hourly[i].Equity-prev)/prev)
	}
	out.NPeriods = len(rets)
	if len(rets) < MinHoursForSharpe {
		out.SharpeNote = "insufficient_periods"
		// still report mean/stdev if any
		if len(rets) >= 1 {
			out.MeanPeriodReturn, out.PeriodReturnStdev = meanStdev(rets)
		}
		return out
	}
	mean, stdev := meanStdev(rets)
	out.MeanPeriodReturn = mean
	out.PeriodReturnStdev = stdev
	if len(rets) < MinHoursForSharpe {
		out.SharpeNote = "insufficient_periods"
		out.Sharpe = 0
		return out
	}
	// Near-constant hourly path → mean/stdev explodes under √(24*365) annualization.
	minStd := 1e-8
	if abs := math.Abs(mean); abs > 0 {
		if floor := abs * 0.05; floor > minStd {
			minStd = floor
		}
	}
	if stdev <= minStd {
		out.SharpeNote = "near_constant_path"
		out.Sharpe = 0
		return out
	}
	out.Sharpe = (mean / stdev) * math.Sqrt(periodsPerYear)
	return out
}

// ResampleHourly builds UTC hour buckets: last equity observed in each hour, forward-filled.
func ResampleHourly(points []EquityPoint) []EquityPoint {
	if len(points) == 0 {
		return nil
	}
	// Sort by time
	pts := append([]EquityPoint(nil), points...)
	sort.SliceStable(pts, func(i, j int) bool {
		return pts[i].Time.Before(pts[j].Time)
	})
	// Drop zero times for grid (keep equity)
	var usable []EquityPoint
	for _, p := range pts {
		if p.Time.IsZero() {
			continue
		}
		usable = append(usable, p)
	}
	if len(usable) == 0 {
		return pts // cannot grid
	}
	start := usable[0].Time.UTC().Truncate(time.Hour)
	end := usable[len(usable)-1].Time.UTC().Truncate(time.Hour)
	if end.Before(start) {
		return usable
	}

	// Map hour → last equity in that hour
	byHour := map[int64]float64{}
	for _, p := range usable {
		h := p.Time.UTC().Truncate(time.Hour).Unix()
		byHour[h] = p.Equity
	}

	var out []EquityPoint
	lastEq := usable[0].Equity
	for t := start; !t.After(end); t = t.Add(time.Hour) {
		if eq, ok := byHour[t.Unix()]; ok {
			lastEq = eq
		}
		out = append(out, EquityPoint{Time: t, Equity: lastEq})
	}
	return out
}

func maxDailyDrawdownBps(points []EquityPoint) float64 {
	type dayAgg struct {
		open, min float64
	}
	days := map[int64]*dayAgg{}
	var keys []int64
	for _, p := range points {
		if p.Time.IsZero() {
			continue
		}
		day := p.Time.UTC().Truncate(24 * time.Hour).Unix()
		a, ok := days[day]
		if !ok {
			a = &dayAgg{open: p.Equity, min: p.Equity}
			days[day] = a
			keys = append(keys, day)
		} else if p.Equity < a.min {
			a.min = p.Equity
		}
	}
	var maxDaily float64
	for _, k := range keys {
		a := days[k]
		if a.open > 0 {
			dd := (a.open - a.min) / a.open * 10_000
			if dd > maxDaily {
				maxDaily = dd
			}
		}
	}
	return maxDaily
}

func meanStdev(rets []float64) (mean, stdev float64) {
	if len(rets) == 0 {
		return 0, 0
	}
	for _, r := range rets {
		mean += r
	}
	mean /= float64(len(rets))
	if len(rets) < 2 {
		return mean, 0
	}
	var sumsq float64
	for _, r := range rets {
		d := r - mean
		sumsq += d * d
	}
	stdev = math.Sqrt(sumsq / float64(len(rets)))
	return mean, stdev
}

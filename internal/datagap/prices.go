package datagap

import (
	"math"
	"sort"
	"time"
)

// PricePoint is one mid sample.
type PricePoint struct {
	Time   time.Time
	Price  float64
	Source string
}

// FillPrices densifies real samples onto grid using interp/hold rules.
// Real points are always kept; synthetic grid points are appended where needed.
// grid should be sorted ascending (e.g. decision times or hourly range).
func FillPrices(real []PricePoint, grid []time.Time, opts Opts) (out []PricePoint, mix Mix) {
	opts.Normalize()
	// normalize real
	var anchors []PricePoint
	for _, p := range real {
		if p.Time.IsZero() || p.Price <= 0 || math.IsNaN(p.Price) {
			continue
		}
		p.Time = p.Time.UTC()
		p.Price = clampPrice(p.Price, opts)
		if p.Source == "" {
			p.Source = SourceVenue
		}
		anchors = append(anchors, p)
	}
	sort.Slice(anchors, func(i, j int) bool { return anchors[i].Time.Before(anchors[j].Time) })
	// dedupe same second keep last
	if len(anchors) > 1 {
		dedup := anchors[:1]
		for i := 1; i < len(anchors); i++ {
			if anchors[i].Time.Equal(dedup[len(dedup)-1].Time) {
				dedup[len(dedup)-1] = anchors[i]
			} else {
				dedup = append(dedup, anchors[i])
			}
		}
		anchors = dedup
	}

	for _, a := range anchors {
		out = append(out, a)
		mix.Venue++
	}
	if len(grid) == 0 || len(anchors) == 0 {
		return out, mix
	}

	// For each grid time, if no venue point "covers" MidAsOf well, try synth at exact T.
	// Coverage: any real point with t in (T-GridStep, T] or equal T.
	for _, t := range grid {
		t = t.UTC()
		if hasVenueNear(anchors, t, opts.GridStep) {
			continue
		}
		p, src, ok := synthAt(anchors, t, opts)
		if !ok {
			mix.Missing++
			continue
		}
		out = append(out, PricePoint{Time: t, Price: p, Source: src})
		switch src {
		case SourceSynthInterp:
			mix.SynthInterp++
		case SourceSynthHold:
			mix.SynthHold++
		default:
			mix.Venue++
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Time.Equal(out[j].Time) {
			// prefer venue over synth at same stamp
			return out[i].Source == SourceVenue && out[j].Source != SourceVenue
		}
		return out[i].Time.Before(out[j].Time)
	})
	// drop synth if venue exists at exact same time
	if len(out) > 1 {
		clean := out[:0]
		for i := 0; i < len(out); i++ {
			if i+1 < len(out) && out[i].Time.Equal(out[i+1].Time) {
				// keep venue if either is venue
				if out[i].Source == SourceVenue {
					clean = append(clean, out[i])
					// skip following same-time non-venue
					for i+1 < len(out) && out[i+1].Time.Equal(out[i].Time) {
						i++
					}
				} else if out[i+1].Source == SourceVenue {
					continue
				} else {
					clean = append(clean, out[i])
				}
				continue
			}
			clean = append(clean, out[i])
		}
		out = clean
	}
	return out, mix
}

func hasVenueNear(anchors []PricePoint, t time.Time, step time.Duration) bool {
	// Exact-ish venue sample only (not "any point within grid step") so interp can densify.
	tol := time.Minute
	if step > 0 && step/4 < tol {
		tol = step / 4
	}
	if tol <= 0 {
		tol = time.Minute
	}
	i := sort.Search(len(anchors), func(j int) bool {
		return anchors[j].Time.After(t)
	}) - 1
	if i >= 0 {
		dt := t.Sub(anchors[i].Time)
		if dt >= 0 && dt <= tol {
			return true
		}
	}
	if i+1 < len(anchors) {
		dt := anchors[i+1].Time.Sub(t)
		if dt >= 0 && dt <= tol {
			return true
		}
	}
	return false
}

func synthAt(anchors []PricePoint, t time.Time, opts Opts) (float64, string, bool) {
	// rightmost <= t
	i := sort.Search(len(anchors), func(j int) bool {
		return anchors[j].Time.After(t)
	}) - 1
	// leftmost >= t
	j := i + 1

	var left, right *PricePoint
	if i >= 0 {
		left = &anchors[i]
	}
	if j < len(anchors) {
		right = &anchors[j]
	}

	if left != nil && right != nil {
		gap := right.Time.Sub(left.Time)
		if gap <= opts.MaxGap && gap > 0 {
			// linear interp
			frac := float64(t.Sub(left.Time)) / float64(gap)
			if frac < 0 {
				frac = 0
			}
			if frac > 1 {
				frac = 1
			}
			p := left.Price + frac*(right.Price-left.Price)
			return clampPrice(p, opts), SourceSynthInterp, true
		}
	}
	if left != nil {
		dt := t.Sub(left.Time)
		if dt >= 0 && dt <= opts.HoldMax {
			return clampPrice(left.Price, opts), SourceSynthHold, true
		}
	}
	if right != nil {
		dt := right.Time.Sub(t)
		if dt >= 0 && dt <= opts.HoldMax {
			return clampPrice(right.Price, opts), SourceSynthHold, true
		}
	}
	return 0, SourceMissing, false
}

func clampPrice(p float64, opts Opts) float64 {
	if p < opts.PriceFloor {
		return opts.PriceFloor
	}
	if p > opts.PriceCeil {
		return opts.PriceCeil
	}
	return p
}

// BuildHourlyGrid returns UTC hourly times in [from, to] inclusive of truncations.
func BuildHourlyGrid(from, to time.Time, step time.Duration) []time.Time {
	if step <= 0 {
		step = time.Hour
	}
	from = from.UTC().Truncate(step)
	to = to.UTC()
	if !to.After(from) {
		return nil
	}
	var out []time.Time
	for t := from; !t.After(to); t = t.Add(step) {
		out = append(out, t)
	}
	return out
}

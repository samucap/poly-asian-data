package datagap

import (
	"sort"
	"time"
)

// BookPoint is top-of-book sample.
type BookPoint struct {
	Time          time.Time
	BestBid       float64
	BestAsk       float64
	TotalBidDepth float64
	TotalAskDepth float64
	Source        string
}

// FillBooks adds synthetic BBA at grid times where no book exists at-or-before within reuse window,
// using mid from filled price series (as-of T).
type MidAsOfFunc func(t time.Time) (mid float64, ok bool)

// FillBooks returns real books plus synth books; mix counts both.
func FillBooks(real []BookPoint, grid []time.Time, midAt MidAsOfFunc, opts Opts) (out []BookPoint, mix Mix) {
	opts.Normalize()
	for _, b := range real {
		if b.Time.IsZero() || b.BestBid <= 0 || b.BestAsk <= 0 {
			continue
		}
		b.Time = b.Time.UTC()
		if b.Source == "" {
			b.Source = SourceVenue
		}
		out = append(out, b)
		mix.Venue++
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Time.Before(out[j].Time) })

	if midAt == nil || len(grid) == 0 {
		return out, mix
	}

	half := opts.DefaultHalfSpreadBps / 10_000 / 2 // half of full spread in price units
	if half <= 0 {
		half = 0.0025
	}
	depth := opts.DefaultDepth

	for _, t := range grid {
		t = t.UTC()
		if bookCovers(out, t, opts.GridStep) {
			continue
		}
		mid, ok := midAt(t)
		if !ok || mid <= 0 {
			mix.Missing++
			continue
		}
		// try reuse last book spread/depth
		hs, dBid, dAsk := half, depth, depth
		if lb, ok := lastBook(out, t); ok && t.Sub(lb.Time) <= opts.BookReuseMax {
			if lb.BestAsk > lb.BestBid && lb.BestBid > 0 {
				m := (lb.BestBid + lb.BestAsk) / 2
				if m > 0 {
					hs = (lb.BestAsk - lb.BestBid) / 2
				}
			}
			if lb.TotalBidDepth > 0 {
				dBid = lb.TotalBidDepth
			}
			if lb.TotalAskDepth > 0 {
				dAsk = lb.TotalAskDepth
			}
		}
		bb := mid - hs
		ba := mid + hs
		if bb < opts.PriceFloor {
			bb = opts.PriceFloor
		}
		if ba > opts.PriceCeil {
			ba = opts.PriceCeil
		}
		if ba <= bb {
			ba = bb + 2*opts.PriceFloor
		}
		out = append(out, BookPoint{
			Time: t, BestBid: bb, BestAsk: ba,
			TotalBidDepth: dBid, TotalAskDepth: dAsk,
			Source: SourceSynthBook,
		})
		mix.SynthBook++
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Time.Before(out[j].Time) })
	return out, mix
}

func bookCovers(books []BookPoint, t time.Time, step time.Duration) bool {
	// Covered if last book at or before t is within one grid step (as-of usable).
	if step <= 0 {
		step = time.Hour
	}
	i := sort.Search(len(books), func(j int) bool {
		return books[j].Time.After(t)
	}) - 1
	if i < 0 {
		return false
	}
	return t.Sub(books[i].Time) <= step
}

func lastBook(books []BookPoint, t time.Time) (BookPoint, bool) {
	i := sort.Search(len(books), func(j int) bool {
		return books[j].Time.After(t)
	}) - 1
	if i < 0 {
		return BookPoint{}, false
	}
	return books[i], true
}

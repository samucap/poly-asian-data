// Package datagap fills sparse price/book history for development evals.
// Synthetic points are deterministic and must be labeled; never treated as venue data.
package datagap

import "time"

// Source labels for lineage / data_quality.
const (
	SourceVenue      = "venue"
	SourceSynthInterp = "synth_interp"
	SourceSynthHold  = "synth_hold"
	SourceSynthBook  = "synth_book_from_mid"
	SourceMissing    = "missing"
)

// Opts controls gap fill (eval-time, no DB write).
type Opts struct {
	// MaxGap is the maximum anchor distance for linear interpolation.
	MaxGap time.Duration
	// HoldMax is max flat forward/backward from a single anchor.
	HoldMax time.Duration
	// GridStep densifies series (default 1h).
	GridStep time.Duration
	// PriceFloor / PriceCeil clamp binary mids.
	PriceFloor float64
	PriceCeil  float64
	// DefaultHalfSpread for book synth when no recent book (price fraction, e.g. 0.0025 = 25 bps half of 50 bps full? use bps).
	DefaultHalfSpreadBps float64
	// BookReuseMax: reuse last book half-spread/depth if not older than this.
	BookReuseMax time.Duration
	// DefaultDepth when synthesizing books.
	DefaultDepth float64
}

// DefaultOpts is conservative for board-policy backtests.
func DefaultOpts() Opts {
	return Opts{
		MaxGap:               48 * time.Hour,
		HoldMax:              6 * time.Hour,
		GridStep:             time.Hour,
		PriceFloor:           0.001,
		PriceCeil:            0.999,
		DefaultHalfSpreadBps: 50, // full spread ~100 bps when using half of this as each side? half-spread = 50 bps total/2
		BookReuseMax:         24 * time.Hour,
		DefaultDepth:         100,
	}
}

// Normalize fills zero fields with defaults.
func (o *Opts) Normalize() {
	d := DefaultOpts()
	if o.MaxGap <= 0 {
		o.MaxGap = d.MaxGap
	}
	if o.HoldMax <= 0 {
		o.HoldMax = d.HoldMax
	}
	if o.GridStep <= 0 {
		o.GridStep = d.GridStep
	}
	if o.PriceFloor <= 0 {
		o.PriceFloor = d.PriceFloor
	}
	if o.PriceCeil <= 0 {
		o.PriceCeil = d.PriceCeil
	}
	if o.DefaultHalfSpreadBps <= 0 {
		o.DefaultHalfSpreadBps = d.DefaultHalfSpreadBps
	}
	if o.BookReuseMax <= 0 {
		o.BookReuseMax = d.BookReuseMax
	}
	if o.DefaultDepth <= 0 {
		o.DefaultDepth = d.DefaultDepth
	}
}

package datagap

import "fmt"

// Mix counts points by source.
type Mix struct {
	Venue       int `json:"venue"`
	SynthInterp int `json:"synth_interp"`
	SynthHold   int `json:"synth_hold"`
	SynthBook   int `json:"synth_book_from_mid,omitempty"`
	Missing     int `json:"missing"`
}

// Total non-missing observations used for share.
func (m Mix) Observed() int {
	return m.Venue + m.SynthInterp + m.SynthHold + m.SynthBook
}

// SynthShare is (synth*) / (venue+synth*) ; 0 if none observed.
func (m Mix) SynthShare() float64 {
	obs := m.Observed()
	if obs == 0 {
		return 0
	}
	synth := m.SynthInterp + m.SynthHold + m.SynthBook
	return float64(synth) / float64(obs)
}

// VenueShare is venue / observed.
func (m Mix) VenueShare() float64 {
	obs := m.Observed()
	if obs == 0 {
		return 0
	}
	return float64(m.Venue) / float64(obs)
}

// Add merges another mix.
func (m *Mix) Add(o Mix) {
	m.Venue += o.Venue
	m.SynthInterp += o.SynthInterp
	m.SynthHold += o.SynthHold
	m.SynthBook += o.SynthBook
	m.Missing += o.Missing
}

// Report is aggregate data quality for an eval run.
type Report struct {
	PriceMix       Mix     `json:"price_source_mix"`
	BookMix        Mix     `json:"book_source_mix"`
	SynthPriceShare float64 `json:"synth_price_share"`
	SynthBookShare  float64 `json:"synth_book_share"`
	FillMode       string  `json:"fill_mode"`
	MaxGap         string  `json:"synth_max_gap,omitempty"`
	HoldMax        string  `json:"synth_hold_max,omitempty"`
	Warning        string  `json:"warning,omitempty"`
	// Significant is true when synth share warrants blocking real actions.
	Significant bool `json:"significant_synth"`
	// BlockPromote is true when promote must stay false.
	BlockPromote bool `json:"block_promote"`
}

// SignificantSynthShare default: ≥20% synthetic is "large" for notifications.
const SignificantSynthShare = 0.20

// PromoteMaxSynthShare default: >5% synthetic cannot promote.
const PromoteMaxSynthShare = 0.05

// Finalize computes shares and warning flags.
func (r *Report) Finalize(maxPromoteShare, significantShare float64) {
	if maxPromoteShare < 0 {
		maxPromoteShare = PromoteMaxSynthShare
	}
	if significantShare <= 0 {
		significantShare = SignificantSynthShare
	}
	r.SynthPriceShare = r.PriceMix.SynthShare()
	r.SynthBookShare = r.BookMix.SynthShare()
	if r.FillMode == "" {
		if r.PriceMix.SynthInterp+r.PriceMix.SynthHold+r.BookMix.SynthBook > 0 {
			r.FillMode = "real_plus_synth"
		} else {
			r.FillMode = "venue_only"
		}
	}
	maxSynth := r.SynthPriceShare
	if r.SynthBookShare > maxSynth {
		maxSynth = r.SynthBookShare
	}
	r.Significant = maxSynth >= significantShare
	r.BlockPromote = r.SynthPriceShare > maxPromoteShare || r.SynthBookShare > maxPromoteShare
	if r.Significant {
		r.Warning = fmt.Sprintf(
			"SIGNIFICANT SYNTHETIC DATA: price_synth=%.1f%% book_synth=%.1f%% — not venue-complete; do not take real actions or promote from this run",
			100*r.SynthPriceShare, 100*r.SynthBookShare,
		)
	} else if r.BlockPromote || r.SynthPriceShare > 0 || r.SynthBookShare > 0 {
		r.Warning = fmt.Sprintf(
			"Development synthetic fill in use (price_synth=%.1f%% book_synth=%.1f%%); aggregation still incomplete — not for promote/live capital",
			100*r.SynthPriceShare, 100*r.SynthBookShare,
		)
	}
}

// Banner is a multi-line stderr/stdout alert when synthetic data is present.
func (r Report) Banner() string {
	if r.SynthPriceShare <= 0 && r.SynthBookShare <= 0 {
		return ""
	}
	level := "NOTICE"
	if r.Significant {
		level = "ALERT"
	}
	return fmt.Sprintf(`
!!!!!!!! %s: SYNTHETIC DATA IN THIS RUN !!!!!!!!
  price: venue=%.1f%% synth=%.1f%% (interp=%d hold=%d venue=%d)
  book:  venue=%.1f%% synth=%.1f%%
  promote blocked: %v
  %s
  Real aggregation has not filled the history yet — treat metrics as developmental only.
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
`,
		level,
		100*r.PriceMix.VenueShare(), 100*r.SynthPriceShare,
		r.PriceMix.SynthInterp, r.PriceMix.SynthHold, r.PriceMix.Venue,
		100*r.BookMix.VenueShare(), 100*r.SynthBookShare,
		r.BlockPromote,
		r.Warning,
	)
}

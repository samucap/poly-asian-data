package risk

import (
	"math"
	"time"
)

// Risk state machine labels.
const (
	StateOK        = "OK"
	StateHaltedDay = "HALTED_DAY"
	StateHaltedDD  = "HALTED_DD"
)

// Reject reasons (funnel for AO).
const (
	ReasonOK           = "ok"
	ReasonHaltedDay    = "halted_day"
	ReasonHaltedDD     = "halted_dd"
	ReasonMaxPositions = "max_positions"
	ReasonMaxGross     = "max_gross"
	ReasonSizeZero     = "size_zero"
	ReasonInvalidSig   = "invalid_signal"
)

// SignalInput is the subset of a paper signal the risk manager needs.
type SignalInput struct {
	Time         time.Time
	ConditionID  string
	Side         string // BUY | SELL
	SizeUSD      float64
	EdgeBps      float64
	Conviction   float64 // 0..1
	CapacityUSD  float64
	Urgency      float64
	KellyFrac    *float64 // optional advisory from signal
}

// Decision is accept/reject + size for one signal.
type Decision struct {
	Accept           bool
	SizeUSD          float64
	Reason           string
	KellyFrac        float64
	Scale            float64
	OpportunityScore float64
}

// Position is one open paper position.
type Position struct {
	ConditionID string
	Side        string
	SizeUSD     float64
	Shares      float64
	EntryMid    float64
	OpenedAt    time.Time
	SignalID    string
}

// Manager is a pure portfolio risk / sizing engine for paper trading.
type Manager struct {
	Cfg Config

	Equity     float64
	PeakEquity float64
	DayOpenEq  float64
	DayUTC     time.Time

	State string

	Positions     map[string]Position
	PeriodReturns []float64
	Events        []Event
}

// Event is a risk state change for audit.
type Event struct {
	Time   time.Time `json:"time"`
	Type   string    `json:"type"`
	Detail string    `json:"detail,omitempty"`
}

// NewManager constructs a manager at starting equity.
func NewManager(cfg Config) *Manager {
	cfg.Normalize()
	eq := cfg.StartingEquityUSD
	return &Manager{
		Cfg:        cfg,
		Equity:     eq,
		PeakEquity: eq,
		DayOpenEq:  eq,
		State:      StateOK,
		Positions:  make(map[string]Position),
	}
}

// GrossExposure sums open |size_usd|.
func (m *Manager) GrossExposure() float64 {
	var g float64
	for _, p := range m.Positions {
		g += math.Abs(p.SizeUSD)
	}
	return g
}

// Accept decides whether to open a new paper trade from a signal.
func (m *Manager) Accept(sig SignalInput) Decision {
	if m == nil {
		return Decision{Reason: ReasonInvalidSig}
	}
	d := Decision{
		Scale:            1,
		Reason:           ReasonOK,
		OpportunityScore: OpportunityScore(m.Cfg, sig.EdgeBps, sig.Conviction, sig.Urgency),
	}

	if sig.ConditionID == "" || (sig.Side != "BUY" && sig.Side != "SELL") {
		d.Reason = ReasonInvalidSig
		return d
	}

	m.maybeRollDay(sig.Time)

	if m.State == StateHaltedDD {
		d.Reason = ReasonHaltedDD
		return d
	}
	if m.State == StateHaltedDay {
		d.Reason = ReasonHaltedDay
		return d
	}

	if _, open := m.Positions[sig.ConditionID]; open {
		d.Reason = ReasonMaxPositions
		return d
	}
	if len(m.Positions) >= m.Cfg.MaxPositions {
		d.Reason = ReasonMaxPositions
		return d
	}

	size, kelly, scale := m.computeSize(sig)
	d.KellyFrac = kelly
	d.Scale = scale
	if size < m.Cfg.MinSizeUSD {
		d.Reason = ReasonSizeZero
		return d
	}

	room := m.Cfg.MaxGrossUSD - m.GrossExposure()
	if room < m.Cfg.MinSizeUSD {
		d.Reason = ReasonMaxGross
		return d
	}
	if size > room {
		size = room
	}
	if size < m.Cfg.MinSizeUSD {
		d.Reason = ReasonMaxGross
		return d
	}

	d.Accept = true
	d.SizeUSD = size
	return d
}

func (m *Manager) computeSize(sig SignalInput) (size, kelly, scale float64) {
	scale = 1
	conv := sig.Conviction
	if conv < 0 {
		conv = 0
	}
	if conv > 1 {
		conv = 1
	}
	if conv == 0 {
		conv = 0.5
	}

	switch m.Cfg.SizingMode {
	case SizingSignalSize:
		size = sig.SizeUSD
		if size <= 0 {
			size = m.Cfg.MinSizeUSD
		}
		size *= 0.25 + 0.75*conv
	case SizingVolTarget:
		size = sig.SizeUSD
		if size <= 0 {
			size = m.Cfg.MaxPositionUSD * 0.1
		}
		scale = m.volScale()
		size *= scale
		size *= 0.25 + 0.75*conv
	default:
		p := m.Cfg.KellyP
		b := m.Cfg.KellyB
		q := 1 - p
		fStar := (p*b - q) / b
		if fStar < 0 {
			fStar = 0
		}
		kelly = m.Cfg.KellyFraction * fStar * conv
		if sig.KellyFrac != nil && *sig.KellyFrac > 0 {
			kelly = math.Min(kelly, m.Cfg.KellyFraction*(*sig.KellyFrac))
		}
		size = m.Equity * kelly
	}

	if size > m.Cfg.MaxPositionUSD {
		size = m.Cfg.MaxPositionUSD
	}
	if sig.CapacityUSD > 0 && m.Cfg.CapacityFrac > 0 {
		cap := sig.CapacityUSD * m.Cfg.CapacityFrac
		if size > cap {
			size = cap
		}
	}
	if sig.SizeUSD > 0 && m.Cfg.SizingMode != SizingFracKelly {
		if size > sig.SizeUSD {
			size = sig.SizeUSD
		}
	}
	return size, kelly, scale
}

func (m *Manager) volScale() float64 {
	n := m.Cfg.VolLookbackPeriods
	if n <= 1 || len(m.PeriodReturns) < 2 {
		return 1
	}
	rets := m.PeriodReturns
	if len(rets) > n {
		rets = rets[len(rets)-n:]
	}
	var mean, sumsq float64
	for _, r := range rets {
		mean += r
	}
	mean /= float64(len(rets))
	for _, r := range rets {
		d := r - mean
		sumsq += d * d
	}
	stdev := math.Sqrt(sumsq / float64(len(rets)))
	if stdev < 1e-12 {
		return 1
	}
	periodTarget := m.Cfg.VolTargetAnn / math.Sqrt(252)
	s := periodTarget / stdev
	if s > 3 {
		s = 3
	}
	if s < 0.1 {
		s = 0.1
	}
	return s
}

// OnOpen registers a new paper position after accept.
func (m *Manager) OnOpen(p Position) {
	if m.Positions == nil {
		m.Positions = make(map[string]Position)
	}
	m.Positions[p.ConditionID] = p
}

// OnClose removes a position and applies realized pnl to equity.
func (m *Manager) OnClose(conditionID string, pnlUSD float64, at time.Time) {
	delete(m.Positions, conditionID)
	m.ApplyPnL(pnlUSD, at)
}

// ApplyPnL updates equity and halt checks.
func (m *Manager) ApplyPnL(pnlUSD float64, at time.Time) {
	m.maybeRollDay(at)
	m.Equity += pnlUSD
	if m.Equity > m.PeakEquity {
		m.PeakEquity = m.Equity
	}
	m.checkHalts(at)
}

// RecordPeriodReturn appends a fractional period return for vol targeting.
func (m *Manager) RecordPeriodReturn(r float64) {
	m.PeriodReturns = append(m.PeriodReturns, r)
	if len(m.PeriodReturns) > 500 {
		m.PeriodReturns = m.PeriodReturns[len(m.PeriodReturns)-500:]
	}
}

// DrawdownBps is peak-to-trough on equity in bps.
func (m *Manager) DrawdownBps() float64 {
	if m.PeakEquity <= 0 {
		return 0
	}
	dd := (m.PeakEquity - m.Equity) / m.PeakEquity * 10_000
	if dd < 0 {
		return 0
	}
	return dd
}

// DailyDrawdownBps is loss from day open in bps.
func (m *Manager) DailyDrawdownBps() float64 {
	if m.DayOpenEq <= 0 {
		return 0
	}
	dd := (m.DayOpenEq - m.Equity) / m.DayOpenEq * 10_000
	if dd < 0 {
		return 0
	}
	return dd
}

func (m *Manager) maybeRollDay(t time.Time) {
	if t.IsZero() {
		return
	}
	day := t.UTC().Truncate(24 * time.Hour)
	if m.DayUTC.IsZero() {
		m.DayUTC = day
		m.DayOpenEq = m.Equity
		return
	}
	if day.After(m.DayUTC) {
		m.DayUTC = day
		m.DayOpenEq = m.Equity
		if m.State == StateHaltedDay {
			m.State = StateOK
			m.Events = append(m.Events, Event{Time: t, Type: "day_resume", Detail: "new UTC day"})
		}
	}
}

func (m *Manager) checkHalts(at time.Time) {
	if m.Cfg.MaxDrawdownBps > 0 && m.DrawdownBps() >= m.Cfg.MaxDrawdownBps {
		if m.State != StateHaltedDD {
			m.State = StateHaltedDD
			m.Events = append(m.Events, Event{
				Time: at, Type: "halted_dd",
				Detail: "max_drawdown_bps breached",
			})
		}
		return
	}
	if m.Cfg.MaxDailyDrawdownBps > 0 && m.DailyDrawdownBps() >= m.Cfg.MaxDailyDrawdownBps {
		if m.State != StateHaltedDay && m.State != StateHaltedDD {
			m.State = StateHaltedDay
			m.Events = append(m.Events, Event{
				Time: at, Type: "halted_day",
				Detail: "max_daily_drawdown_bps breached",
			})
		}
	}
}

// ShouldFlattenAll is true when portfolio max DD halt is active.
func (m *Manager) ShouldFlattenAll() bool {
	return m != nil && m.State == StateHaltedDD
}

package eval

import (
	"math"
	"sort"
	"time"
)

// Label is one PIT decision outcome (pure; no DB).
type Label struct {
	DecisionTime       time.Time
	Horizon            string
	ConditionID        string
	SelectionSet       string
	Hit                bool
	AfterCostReturnBps float64
	Category           string
	NegRisk            bool
	FVSource           string
	TTRBucket          string
	MidAtT             float64
	EdgeBpsAtT         float64 // diagnostic only — forbidden as model input
	// Policy tags for baseline grouping
	Policy string // "candidate" | "volume_top_n" | "activity_stage1" | "random_board"
}

// AggregateLabels builds HorizonMetrics for a set of labels (same horizon expected).
// after_cost_return_bps = mean per-trade (promote gate). Does not set max_drawdown
// from label soup — use BuildPortfolioMetrics for trustworthy DD/Sharpe.
func AggregateLabels(labels []Label) HorizonMetrics {
	out := HorizonMetrics{N: len(labels)}
	if len(labels) == 0 {
		return out
	}
	out.Horizon = labels[0].Horizon
	var sum float64
	hits := 0
	rets := make([]float64, 0, len(labels))
	for _, l := range labels {
		if l.Hit {
			hits++
		}
		if !math.IsNaN(l.AfterCostReturnBps) {
			sum += l.AfterCostReturnBps
			rets = append(rets, l.AfterCostReturnBps)
		}
	}
	n := float64(len(labels))
	out.HitRate = float64(hits) / n
	out.WinRate = out.HitRate
	if len(rets) > 0 {
		out.AfterCostReturnBps = sum / float64(len(rets))
		out.AfterCostReturnMedBps = median(rets)
	}
	return out
}

// PortfolioMetrics is equal-weight board basket risk stats over decision times.
// Period return at T = mean after-cost bps of trades labeled at T (same horizon).
type PortfolioMetrics struct {
	Horizon         string  `json:"horizon,omitempty"`
	NPeriods        int     `json:"n_periods"`
	Sharpe          float64 `json:"sharpe"`
	MaxDrawdownBps  float64 `json:"max_drawdown_bps"`
	TotalReturnBps  float64 `json:"total_return_bps"`
	MeanPeriodBps   float64 `json:"mean_period_bps,omitempty"`
	// PeriodsPerYear used for Sharpe annualization (e.g. 24*365 for hourly).
	PeriodsPerYear float64 `json:"periods_per_year,omitempty"`
}

// BuildPortfolioMetrics builds equity-curve style metrics from candidate labels.
// policy: empty or "candidate". horizon must match labels.
// periodsPerYear: if <=0, defaults to 24*365 (hourly decision grid).
func BuildPortfolioMetrics(labels []Label, horizon, policy string, periodsPerYear float64) PortfolioMetrics {
	out := PortfolioMetrics{Horizon: horizon, PeriodsPerYear: periodsPerYear}
	if periodsPerYear <= 0 {
		out.PeriodsPerYear = 24 * 365
		periodsPerYear = out.PeriodsPerYear
	}
	// Group by decision time
	type bucket struct {
		sum float64
		n   int
	}
	byT := map[int64]*bucket{}
	var times []int64
	for _, l := range labels {
		if l.Horizon != horizon {
			continue
		}
		if policy == "" || policy == "candidate" {
			if l.Policy != "" && l.Policy != "candidate" {
				continue
			}
		} else if l.Policy != policy {
			continue
		}
		if math.IsNaN(l.AfterCostReturnBps) {
			continue
		}
		key := l.DecisionTime.UTC().Unix()
		b, ok := byT[key]
		if !ok {
			b = &bucket{}
			byT[key] = b
			times = append(times, key)
		}
		b.sum += l.AfterCostReturnBps
		b.n++
	}
	if len(times) == 0 {
		return out
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	periodRets := make([]float64, 0, len(times))
	var equity, peak, maxDD, total float64
	for _, t := range times {
		b := byT[t]
		if b.n == 0 {
			continue
		}
		r := b.sum / float64(b.n)
		periodRets = append(periodRets, r)
		total += r
		equity += r
		if equity > peak {
			peak = equity
		}
		dd := peak - equity
		if dd > maxDD {
			maxDD = dd
		}
	}
	out.NPeriods = len(periodRets)
	out.TotalReturnBps = total
	out.MaxDrawdownBps = maxDD
	if len(periodRets) == 0 {
		return out
	}
	mean := total / float64(len(periodRets))
	out.MeanPeriodBps = mean
	// sample stdev
	var ss float64
	for _, r := range periodRets {
		d := r - mean
		ss += d * d
	}
	stdev := 0.0
	if len(periodRets) > 1 {
		stdev = math.Sqrt(ss / float64(len(periodRets)-1))
	}
	if stdev > 1e-12 {
		out.Sharpe = (mean / stdev) * math.Sqrt(periodsPerYear)
	}
	return out
}

func median(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	cp := append([]float64{}, xs...)
	sort.Float64s(cp)
	m := len(cp) / 2
	if len(cp)%2 == 0 {
		return (cp[m-1] + cp[m]) / 2
	}
	return cp[m]
}

// TTRBucket maps hours-to-resolution into near / mid / far.
func TTRBucket(ttrHours float64) string {
	switch {
	case ttrHours < 24:
		return "near"
	case ttrHours < 24*7:
		return "mid"
	default:
		return "far"
	}
}

// BuildMetrics assembles EvalMetrics from labeled decisions for the primary horizon
// (candidate policy) plus baseline policies and strata.
//
// labels: all policies mixed; Policy field selects candidate vs baselines.
// primaryHorizon: e.g. "1h"
//
// Also fills ByHorizon for all horizons present and Portfolio (primary, candidate).
func BuildMetrics(labels []Label, primaryHorizon string) EvalMetrics {
	if primaryHorizon == "" {
		primaryHorizon = "1h"
	}
	m := EvalMetrics{
		PrimaryHorizon:   primaryHorizon,
		ByStratum:        map[string]HorizonMetrics{},
		ByHorizon:        map[string]HorizonMetrics{},
		Baselines:        map[string]HorizonMetrics{},
		DeltaVsBaselines: map[string]float64{},
	}

	// Multi-horizon aggregates (candidate only)
	horizonsSeen := map[string]bool{}
	for _, l := range labels {
		if l.Horizon != "" {
			horizonsSeen[l.Horizon] = true
		}
	}
	for h := range horizonsSeen {
		var hs []Label
		for _, l := range labels {
			if l.Horizon == h {
				hs = append(hs, l)
			}
		}
		candH := filterPolicy(hs, "candidate", true)
		m.ByHorizon[h] = AggregateLabels(candH)
	}

	// Filter primary horizon
	var primary []Label
	for _, l := range labels {
		if l.Horizon == primaryHorizon {
			primary = append(primary, l)
		}
	}

	// Candidate = empty policy or "candidate"
	cand := filterPolicy(primary, "candidate", true)
	m.Overall = AggregateLabels(cand)
	m.N = m.Overall.N

	// Portfolio risk (trusted max DD + Sharpe); overwrite overall max_drawdown for agents
	port := BuildPortfolioMetrics(labels, primaryHorizon, "candidate", 0)
	m.Portfolio = &port
	m.Overall.MaxDrawdownBps = port.MaxDrawdownBps

	// Baselines
	for _, name := range RequiredBaselines {
		base := filterPolicy(primary, name, false)
		m.Baselines[name] = AggregateLabels(base)
		m.DeltaVsBaselines[name] = m.Overall.AfterCostReturnBps - m.Baselines[name].AfterCostReturnBps
	}

	// Strata on candidate only
	byCat := map[string][]Label{}
	byNeg := map[string][]Label{}
	byFV := map[string][]Label{}
	byTTR := map[string][]Label{}
	for _, l := range cand {
		cat := l.Category
		if cat == "" {
			cat = "unknown"
		}
		byCat["category="+cat] = append(byCat["category="+cat], l)
		nr := "neg_risk=false"
		if l.NegRisk {
			nr = "neg_risk=true"
		}
		byNeg[nr] = append(byNeg[nr], l)
		fv := l.FVSource
		if fv == "" {
			fv = "none"
		}
		byFV["fv_source="+fv] = append(byFV["fv_source="+fv], l)
		tb := l.TTRBucket
		if tb == "" {
			tb = "unknown"
		}
		byTTR["ttr_bucket="+tb] = append(byTTR["ttr_bucket="+tb], l)
	}
	for k, ls := range byCat {
		m.ByStratum[k] = AggregateLabels(ls)
	}
	for k, ls := range byNeg {
		m.ByStratum[k] = AggregateLabels(ls)
	}
	for k, ls := range byFV {
		m.ByStratum[k] = AggregateLabels(ls)
	}
	for k, ls := range byTTR {
		m.ByStratum[k] = AggregateLabels(ls)
	}
	return m
}

// filterPolicy keeps labels matching policy. allowEmpty treats "" as candidate.
func filterPolicy(labels []Label, policy string, allowEmpty bool) []Label {
	var out []Label
	for _, l := range labels {
		if l.Policy == policy || (allowEmpty && l.Policy == "") {
			out = append(out, l)
		}
	}
	return out
}

// NewSurface scaffolds an EvalSurface ready for EvaluateGates.
func NewSurface(cfg Config, metrics EvalMetrics, featureNames []string, runID string) *EvalSurface {
	if runID == "" {
		runID = "eval-" + time.Now().UTC().Format("20060102T150405Z")
	}
	sel := cfg.DefaultSelectionSet
	if sel == "" {
		sel = SelectionBoard
	}
	am := cfg.ActionModel
	if am == "" {
		am = ActionSignFromEdge
	}
	s := &EvalSurface{
		SchemaVersion: SchemaVersion,
		RunID:         runID,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339),
		Status:        "failed", // until gates run
		Errors:        nil,
		SelectionSet:  sel,
		Horizons:      append([]string{}, cfg.Horizons...),
		LabelProtocol: cfg.ToLabelProtocol(),
		FillModel:     cfg.ToFillModel(),
		Metrics:       metrics,
		ActionModel:   am,
		FeatureNames:  featureNames,
	}
	return s
}

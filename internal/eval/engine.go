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
		out.MaxDrawdownBps = maxDrawdown(rets)
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

// maxDrawdown treats sequential label returns as a simple equity path (mean units).
func maxDrawdown(rets []float64) float64 {
	if len(rets) == 0 {
		return 0
	}
	var equity, peak, maxDD float64
	for _, r := range rets {
		equity += r
		if equity > peak {
			peak = equity
		}
		dd := peak - equity
		if dd > maxDD {
			maxDD = dd
		}
	}
	return maxDD
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
func BuildMetrics(labels []Label, primaryHorizon string) EvalMetrics {
	if primaryHorizon == "" {
		primaryHorizon = "1h"
	}
	m := EvalMetrics{
		PrimaryHorizon:   primaryHorizon,
		ByStratum:        map[string]HorizonMetrics{},
		Baselines:        map[string]HorizonMetrics{},
		DeltaVsBaselines: map[string]float64{},
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
	// Prefer at least one key from each dimension when present
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
		FeatureNames:  featureNames,
	}
	return s
}

package edge

import "math"

const (
	fvSourceNegRisk = "neg_risk_complement"
	fvClampLo       = 0.01
	fvClampHi       = 0.99
)

// NegRiskComplement prices a leg as 1 − Σ mids of other observed group legs.
// Requires at least MinOtherLegs other mids; never quotes from self alone.
//
// Important: for a fully observed group, raw model edge (FV−mid)×1e4 equals the
// group residual (1−Σ mids)×1e4 for every leg. Per-leg rank is then driven by cost,
// not relative cheap/rich vs siblings. Use residual as a group arb signal.
type NegRiskComplement struct {
	// MinOtherLegs default 1.
	MinOtherLegs int
	// FeeScale multiplies neg_risk_fee_bips/1e4 haircut on FV (default 0 = ignore).
	FeeScale float64
}

func (NegRiskComplement) Name() string { return fvSourceNegRisk }

// Quote implements FairValueProvider.
func (p NegRiskComplement) Quote(in FairValueInput) *FairValueOutput {
	if in.ConditionID == "" || len(in.GroupMids) == 0 {
		return nil
	}
	minOthers := p.MinOtherLegs
	if minOthers <= 0 {
		minOthers = 1
	}

	var sumOthers float64
	nOthers := 0
	for id, mid := range in.GroupMids {
		if id == in.ConditionID {
			continue
		}
		if mid <= 0 || mid >= 1 {
			continue
		}
		sumOthers += mid
		nOthers++
	}
	if nOthers < minOthers {
		return nil
	}

	fv := 1.0 - sumOthers
	if p.FeeScale > 0 && in.NegRiskFeeBips > 0 {
		fv -= (in.NegRiskFeeBips / 10_000) * p.FeeScale
	}
	fv = clamp01(fv, fvClampLo, fvClampHi)

	known := in.KnownGroupSize
	if known <= 0 {
		known = len(in.GroupMids)
	}
	if known < nOthers+1 {
		known = nOthers + 1
	}
	coverage := float64(nOthers+1) / float64(known)
	conf := coverage
	if self, ok := in.GroupMids[in.ConditionID]; ok && self > 0 {
		sumAll := sumOthers + self
		dev := math.Abs(sumAll - 1)
		if dev < 0.05 {
			conf = math.Min(1, conf+0.15)
		} else if dev > 0.25 {
			conf = math.Max(0, conf-0.15)
		}
	}
	if conf > 1 {
		conf = 1
	}

	return &FairValueOutput{
		FairValue:  fv,
		Confidence: conf,
		Source:     fvSourceNegRisk,
		Incomplete: known > nOthers+1,
	}
}

func clamp01(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

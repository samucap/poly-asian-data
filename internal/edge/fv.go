package edge

// FairValueInput is everything a pure FV provider needs (no I/O).
type FairValueInput struct {
	ConditionID string
	Features    FeatureVector
	// GroupMids maps condition_id → mid for legs in the same neg-risk group (including self when known).
	GroupMids map[string]float64
	// KnownGroupSize is |related_legs|+1 when known; 0 means use len(GroupMids).
	KnownGroupSize int
	NegRiskFeeBips float64
	// Underlying optional for ExternalUnderlying provider (nil = no quote).
	Underlying *float64
	// WindowOpen optional strike/open for binary up/down (nil = unused).
	WindowOpen *float64
}

// FairValueOutput is a priced quote.
type FairValueOutput struct {
	FairValue  float64 // [0,1]
	Confidence float64 // [0,1]
	Source     string
	Incomplete bool
}

// FairValueProvider prices an outcome without network I/O.
type FairValueProvider interface {
	Name() string
	// Quote returns nil if this provider cannot price the market.
	Quote(in FairValueInput) *FairValueOutput
}

// FVChain is an ordered list of providers; first quote with Confidence >= MinConfidence wins.
type FVChain struct {
	Providers     []FairValueProvider
	MinConfidence float64
	Enabled       bool
}

// DefaultFVChain returns neg-risk complement then external stub.
func DefaultFVChain(minConf float64) FVChain {
	return DefaultFVChainFromWeights(Weights{
		FVEnabled:       true,
		FVMinConfidence: minConf,
		FVMinOtherLegs:  1,
	})
}

// DefaultFVChainFromWeights builds the production chain from strategy weights (no type-assert mutation).
//
// Note: NegRiskComplement prices group residual — for a fully observed group every leg
// gets the same raw model edge (1−Σ mids); rank among legs is cost-driven.
func DefaultFVChainFromWeights(w Weights) FVChain {
	minConf := w.FVMinConfidence
	if minConf <= 0 {
		minConf = 0.4
	}
	minOthers := w.FVMinOtherLegs
	if minOthers <= 0 {
		minOthers = 1
	}
	return FVChain{
		Providers: []FairValueProvider{
			NegRiskComplement{MinOtherLegs: minOthers},
			ExternalUnderlying{},
		},
		MinConfidence: minConf,
		Enabled:       w.FVEnabled,
	}
}

// IsExtremeMid reports whether mid is outside [lo, hi] (board eligibility policy).
func IsExtremeMid(mid, lo, hi float64) bool {
	if mid <= 0 {
		return false
	}
	if lo <= 0 {
		lo = 0.05
	}
	if hi <= 0 {
		hi = 0.95
	}
	return mid < lo || mid > hi
}

// Resolve runs the chain; returns nil when disabled or no provider quotes.
func (c FVChain) Resolve(in FairValueInput) *FairValueOutput {
	if !c.Enabled {
		return nil
	}
	minC := c.MinConfidence
	if minC <= 0 {
		minC = 0.4
	}
	for _, p := range c.Providers {
		if p == nil {
			continue
		}
		q := p.Quote(in)
		if q == nil {
			continue
		}
		if q.Confidence < minC {
			continue
		}
		return q
	}
	return nil
}

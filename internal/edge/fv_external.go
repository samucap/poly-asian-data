package edge

const fvSourceExternal = "external_underlying"

// ExternalUnderlying is a stub: quotes only when Underlying (+ optional WindowOpen) is injected.
// Full crypto/window feed wiring is a later milestone.
type ExternalUnderlying struct{}

func (ExternalUnderlying) Name() string { return fvSourceExternal }

// Quote implements FairValueProvider.
func (ExternalUnderlying) Quote(in FairValueInput) *FairValueOutput {
	if in.Underlying == nil {
		return nil
	}
	// Naive binary: if window open provided, P(up) ≈ clip(0.5 + k*(spot-open)/open)
	// Without open, do not invent a probability from a raw level alone.
	if in.WindowOpen == nil || *in.WindowOpen <= 0 {
		return nil
	}
	spot := *in.Underlying
	open := *in.WindowOpen
	// Simple linear tilt; not a production vol model.
	move := (spot - open) / open
	fv := 0.5 + move*2 // 25% move → ~1.0
	fv = clamp01(fv, fvClampLo, fvClampHi)
	return &FairValueOutput{
		FairValue:  fv,
		Confidence: 0.35, // below default min_conf 0.4 → chain skips unless lowered
		Source:     fvSourceExternal,
	}
}

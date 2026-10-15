package edge

// GroupLeg is one outcome leg for neg-risk residual math.
type GroupLeg struct {
	ConditionID string
	Mid         float64
}

// GroupResidual computes |Σ mid − 1| in bps for legs with valid mids.
// incomplete is true when fewer than 2 valid mids.
func GroupResidual(legs []GroupLeg) (residualBps float64, incomplete bool) {
	var mids []float64
	for _, leg := range legs {
		if leg.Mid > 0 {
			mids = append(mids, leg.Mid)
		}
	}
	if len(mids) < 2 {
		return 0, true
	}
	return NegRiskResidualBpsFromMids(mids), false
}

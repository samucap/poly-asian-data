package edgescan

import "github.com/samucap/poly-asian-data/internal/attention"

// AttentionPolicy chooses the enrich set (Stage-1 budget) for a scan cycle.
// Fetchers still receive only token/condition IDs.
type AttentionPolicy interface {
	attention.Policy
	// EnrichSet is today’s Stage-1: activity filter + cap (+ sticky).
	EnrichSet(pool []Candidate, opts BuildOptions) Stage1Result
}

// ActivityThenEdgePolicy is the default pack: activity enrich, then edge.SelectBoard listen.
type ActivityThenEdgePolicy struct{}

func (ActivityThenEdgePolicy) Name() string { return attention.ActivityThenEdge }

func (ActivityThenEdgePolicy) EnrichSet(pool []Candidate, opts BuildOptions) Stage1Result {
	return SelectStage1(pool, opts)
}

// DefaultAttention returns the shipped pack. Swap here (or via CycleDeps) for other strategies.
func DefaultAttention() AttentionPolicy {
	return ActivityThenEdgePolicy{}
}

// Package attention separates “who to enrich / listen to” from fetch adapters.
//
// Catalog, book/OI/price HTTP, and WS subscribe take ID lists only.
// Which IDs are requested is a Policy. The default pack is activity-then-edge
// (today’s Stage-1 + SelectBoard). Other packs should implement Policy rather
// than forking enrich or ws-listener.
package attention

// Policy names (stable in artifacts / logs).
const (
	// ActivityThenEdge is the shipped beta pack: activity budget → cost-aware edge board.
	ActivityThenEdge = "activity_then_edge"
)

// IDSet is a strategy-agnostic request for market data or subscriptions.
type IDSet struct {
	Policy       string   `json:"policy"`
	ConditionIDs []string `json:"condition_ids,omitempty"`
	TokenIDs     []string `json:"token_ids,omitempty"`
}

// Policy decides enrich (expensive books/OI) vs listen (WS / published board).
// Concrete EnrichSet / ListenSet live in edgescan (need Candidate types).
type Policy interface {
	Name() string
}

// DefaultName is the pack edge-scan and edge-eval use until another is wired.
func DefaultName() string {
	return ActivityThenEdge
}

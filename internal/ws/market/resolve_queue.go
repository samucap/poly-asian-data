package market

import (
	"sync"
	"time"
)

// MarketResolution is a deferred DB write for market_resolved (backtest hygiene).
// Hot path only enqueues; batch flush every few minutes keeps WS lean.
type MarketResolution struct {
	GammaID        string
	ConditionID    string // market / condition 0x…
	WinningAssetID string
	WinningOutcome string
	ResolvedAt     time.Time
	AssetIDs       []string
}

// ResolveQueue aggregates resolutions in memory until TakeAll for batch DB update.
type ResolveQueue struct {
	mu   sync.Mutex
	byKey map[string]MarketResolution // condition_id or gamma id
}

// NewResolveQueue creates an empty queue.
func NewResolveQueue() *ResolveQueue {
	return &ResolveQueue{byKey: make(map[string]MarketResolution)}
}

// Enqueue stores/overwrites one resolution (dedupe by condition or gamma id).
func (q *ResolveQueue) Enqueue(r MarketResolution) {
	if q == nil {
		return
	}
	key := r.ConditionID
	if key == "" {
		key = r.GammaID
	}
	if key == "" {
		return
	}
	if r.ResolvedAt.IsZero() {
		r.ResolvedAt = time.Now().UTC()
	}
	q.mu.Lock()
	q.byKey[key] = r
	q.mu.Unlock()
}

// Len returns pending unique resolutions.
func (q *ResolveQueue) Len() int {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.byKey)
}

// TakeAll drains the queue for a batch DB write.
func (q *ResolveQueue) TakeAll() []MarketResolution {
	if q == nil {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.byKey) == 0 {
		return nil
	}
	out := make([]MarketResolution, 0, len(q.byKey))
	for _, r := range q.byKey {
		out = append(out, r)
	}
	q.byKey = make(map[string]MarketResolution)
	return out
}

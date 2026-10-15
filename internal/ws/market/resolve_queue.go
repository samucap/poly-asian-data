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

// Enqueue stores one resolution (dedupe by condition or gamma id).
// Prefer newer ResolvedAt; on equal time, keep non-empty winner/asset fields.
// Safe for failed-batch requeue concurrent with a fresher WS event.
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
	defer q.mu.Unlock()
	if prev, ok := q.byKey[key]; ok {
		if r.ResolvedAt.Before(prev.ResolvedAt) {
			// Stale requeue must not overwrite a newer resolution.
			return
		}
		if r.ResolvedAt.Equal(prev.ResolvedAt) {
			r = mergeResolution(prev, r)
		}
	}
	q.byKey[key] = r
}

// mergeResolution prefers non-empty fields from newer (b) over older (a).
func mergeResolution(a, b MarketResolution) MarketResolution {
	out := b
	if out.WinningOutcome == "" {
		out.WinningOutcome = a.WinningOutcome
	}
	if out.WinningAssetID == "" {
		out.WinningAssetID = a.WinningAssetID
	}
	if out.GammaID == "" {
		out.GammaID = a.GammaID
	}
	if out.ConditionID == "" {
		out.ConditionID = a.ConditionID
	}
	if len(out.AssetIDs) == 0 {
		out.AssetIDs = append([]string(nil), a.AssetIDs...)
	}
	return out
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

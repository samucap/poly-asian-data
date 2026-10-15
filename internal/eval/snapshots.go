package eval

import (
	"sort"
	"time"

	"github.com/samucap/poly-asian-data/internal/db"
)

// SnapIndex is an in-memory bulk index of board snapshots for PIT eval (L2/L3/L4/L7).
type SnapIndex struct {
	// times sorted ascending unique selected_at
	times []time.Time
	// byTime maps unix → rows at that snapshot
	byTime map[int64][]db.EdgeBoardSnapshotRow
	// byCondTime for quick cond lookup: condition_id → sorted times with that cond
	nRows int
}

// BuildSnapIndex indexes bulk-loaded snapshot rows (one query, no per-T SQL).
func BuildSnapIndex(rows []db.EdgeBoardSnapshotRow) *SnapIndex {
	idx := &SnapIndex{
		byTime: map[int64][]db.EdgeBoardSnapshotRow{},
	}
	if len(rows) == 0 {
		return idx
	}
	seenT := map[int64]bool{}
	for _, r := range rows {
		t := r.SelectedAt.UTC()
		key := t.UnixNano()
		idx.byTime[key] = append(idx.byTime[key], r)
		if !seenT[key] {
			seenT[key] = true
			idx.times = append(idx.times, t)
		}
		idx.nRows++
	}
	sort.Slice(idx.times, func(i, j int) bool { return idx.times[i].Before(idx.times[j]) })
	return idx
}

// NearestAtOrBefore returns snapshot rows with selected_at ≤ t, nearest prior.
// ok false if no snapshot yet.
func (idx *SnapIndex) NearestAtOrBefore(t time.Time) (at time.Time, rows []db.EdgeBoardSnapshotRow, ok bool) {
	if idx == nil || len(idx.times) == 0 {
		return time.Time{}, nil, false
	}
	t = t.UTC()
	// binary search last time ≤ t
	i := sort.Search(len(idx.times), func(i int) bool {
		return idx.times[i].After(t)
	}) - 1
	if i < 0 {
		return time.Time{}, nil, false
	}
	at = idx.times[i]
	rows = idx.byTime[at.UnixNano()]
	return at, rows, true
}

// MetaAt overlays snapshot meta for a condition at decision time T.
type SnapMeta struct {
	Category       string
	NegRisk        bool
	NegRiskGroupID string
	RelatedLegs    []string
	Volume24hr     float64
	TokenID        string
	HasVolume      bool
	Found          bool
}

// LookupCond returns snapshot meta for condition at nearest snapshot ≤ t.
func (idx *SnapIndex) LookupCond(t time.Time, conditionID string) SnapMeta {
	_, rows, ok := idx.NearestAtOrBefore(t)
	if !ok {
		return SnapMeta{}
	}
	for _, r := range rows {
		if r.ConditionID == conditionID {
			return SnapMeta{
				Category:       r.Category,
				NegRisk:        r.NegRisk,
				NegRiskGroupID: r.NegRiskGroupID,
				RelatedLegs:    r.RelatedLegs,
				Volume24hr:     r.Volume24hr,
				TokenID:        r.TokenID,
				HasVolume:      true,
				Found:          true,
			}
		}
	}
	return SnapMeta{}
}

// Stats for universe_note.
func (idx *SnapIndex) Stats() (nTimes, nRows int) {
	if idx == nil {
		return 0, 0
	}
	return len(idx.times), idx.nRows
}

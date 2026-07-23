package market

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// BookStore holds per-token books with dirty tracking for write-minimized flushes.
type BookStore struct {
	mu     sync.RWMutex
	books  map[string]*BookState
	// Epsilon for dirty mid/BBA change (absolute price units).
	Epsilon float64
}

// NewBookStore creates an empty store.
func NewBookStore() *BookStore {
	return &BookStore{
		books:   make(map[string]*BookState),
		Epsilon: 0.00005, // ~0.5 bps at mid 0.5
	}
}

// Apply applies a parsed event; returns true if book became dirty.
func (s *BookStore) Apply(ev ParsedEvent) bool {
	if s == nil {
		return false
	}
	switch ev.Type {
	case EventBook:
		return s.applyBook(ev)
	case EventPriceChange:
		// Prefer single-leg events from ParseMessage expansion.
		if ev.AssetID != "" && len(ev.PriceChanges) == 0 {
			return s.applyPriceChange(ev)
		}
		// Aggregate event with legs: skip (already expanded).
		if len(ev.PriceChanges) > 0 {
			return false
		}
		return s.applyPriceChange(ev)
	case EventBestBidAsk:
		return s.applyBBA(ev)
	case EventLastTradePrice:
		return s.applyTrade(ev)
	default:
		return false
	}
}

func (s *BookStore) applyBook(ev ParsedEvent) bool {
	if ev.AssetID == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.ensureLocked(ev.AssetID)
	b.MarketID = ev.MarketID
	b.Bids = make(map[string]float64, len(ev.Bids))
	b.Asks = make(map[string]float64, len(ev.Asks))
	for _, l := range ev.Bids {
		if l.Price > 0 && l.Size > 0 {
			b.Bids[priceKey(l.Price)] = l.Size
		}
	}
	for _, l := range ev.Asks {
		if l.Price > 0 && l.Size > 0 {
			b.Asks[priceKey(l.Price)] = l.Size
		}
	}
	recomputeTop(b)
	ts := ev.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	b.UpdatedAt = ts
	b.Dirty = true
	return true
}

func (s *BookStore) applyPriceChange(ev ParsedEvent) bool {
	if ev.AssetID == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.ensureLocked(ev.AssetID)
	if b.MarketID == "" {
		b.MarketID = ev.MarketID
	}
	if b.Bids == nil {
		b.Bids = make(map[string]float64)
	}
	if b.Asks == nil {
		b.Asks = make(map[string]float64)
	}
	key := priceKey(ev.Price)
	side := ev.Side
	if side == "BUY" {
		if ev.Size <= 0 {
			delete(b.Bids, key)
		} else if ev.Price > 0 {
			b.Bids[key] = ev.Size
		}
	} else if side == "SELL" {
		if ev.Size <= 0 {
			delete(b.Asks, key)
		} else if ev.Price > 0 {
			b.Asks[key] = ev.Size
		}
	}
	// Prefer message BBA when present.
	if ev.BestBid > 0 || ev.BestAsk > 0 {
		if ev.BestBid > 0 {
			b.BestBid = ev.BestBid
		}
		if ev.BestAsk > 0 {
			b.BestAsk = ev.BestAsk
		}
		recomputeDepth(b)
	} else {
		recomputeTop(b)
	}
	ts := ev.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	b.UpdatedAt = ts
	if s.changedEnough(b) {
		b.Dirty = true
		return true
	}
	return b.Dirty
}

func (s *BookStore) applyBBA(ev ParsedEvent) bool {
	if ev.AssetID == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.ensureLocked(ev.AssetID)
	if b.MarketID == "" {
		b.MarketID = ev.MarketID
	}
	prevMid := b.Mid()
	if ev.BestBid > 0 {
		b.BestBid = ev.BestBid
	}
	if ev.BestAsk > 0 {
		b.BestAsk = ev.BestAsk
	}
	ts := ev.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	b.UpdatedAt = ts
	if math.Abs(b.Mid()-prevMid) >= s.Epsilon || math.Abs(b.BestBid-b.LastFlushBB) >= s.Epsilon {
		b.Dirty = true
		return true
	}
	return false
}

func (s *BookStore) applyTrade(ev ParsedEvent) bool {
	if ev.AssetID == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.ensureLocked(ev.AssetID)
	if b.MarketID == "" {
		b.MarketID = ev.MarketID
	}
	if ev.LastTradePrice > 0 {
		b.LastTradePrice = ev.LastTradePrice
	}
	ts := ev.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	b.UpdatedAt = ts
	b.Dirty = true
	return true
}

func (s *BookStore) ensureLocked(tokenID string) *BookState {
	b, ok := s.books[tokenID]
	if !ok {
		b = &BookState{
			TokenID: tokenID,
			Bids:    make(map[string]float64),
			Asks:    make(map[string]float64),
		}
		s.books[tokenID] = b
	}
	return b
}

func (s *BookStore) changedEnough(b *BookState) bool {
	if b.LastFlushMid == 0 && b.LastFlushBB == 0 {
		return true
	}
	mid := b.Mid()
	return math.Abs(mid-b.LastFlushMid) >= s.Epsilon ||
		math.Abs(b.BestBid-b.LastFlushBB) >= s.Epsilon ||
		math.Abs(b.BestAsk-b.LastFlushBA) >= s.Epsilon
}

// Snapshot returns a copy of book state for token.
func (s *BookStore) Snapshot(tokenID string) (BookState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.books[tokenID]
	if !ok || b == nil {
		return BookState{}, false
	}
	cp := *b
	return cp, true
}

// TakeDirty returns dirty books and clears Dirty flags; records last-flush BBA for skip logic.
// maxN caps batch size (0 = unlimited).
func (s *BookStore) TakeDirty(maxN int) []BookState {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []BookState
	for _, b := range s.books {
		if b == nil || !b.Dirty {
			continue
		}
		if maxN > 0 && len(out) >= maxN {
			break
		}
		cp := *b
		out = append(out, cp)
		b.Dirty = false
		b.LastFlushMid = b.Mid()
		b.LastFlushBB = b.BestBid
		b.LastFlushBA = b.BestAsk
	}
	return out
}

// DirtyCount returns number of dirty books.
func (s *BookStore) DirtyCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n := 0
	for _, b := range s.books {
		if b != nil && b.Dirty {
			n++
		}
	}
	return n
}

// TakeChangedForSnapshot returns books whose BBA/mid moved enough since last snapshot write.
// Independent of Dirty (features flush). maxN caps batch size.
func (s *BookStore) TakeChangedForSnapshot(maxN int) []BookState {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []BookState
	for _, b := range s.books {
		if b == nil || !b.ValidBook() {
			continue
		}
		mid := b.Mid()
		if b.LastSnapMid != 0 &&
			math.Abs(mid-b.LastSnapMid) < s.Epsilon &&
			math.Abs(b.BestBid-b.LastSnapBB) < s.Epsilon &&
			math.Abs(b.BestAsk-b.LastSnapBA) < s.Epsilon {
			continue
		}
		if maxN > 0 && len(out) >= maxN {
			break
		}
		cp := *b
		out = append(out, cp)
		b.LastSnapMid = mid
		b.LastSnapBB = b.BestBid
		b.LastSnapBA = b.BestAsk
	}
	return out
}

// Len returns tracked books.
func (s *BookStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.books)
}

// RemoveTokens drops in-memory books for the given token IDs (e.g. after market_resolved).
func (s *BookStore) RemoveTokens(tokenIDs []string) {
	if s == nil || len(tokenIDs) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range tokenIDs {
		delete(s.books, id)
	}
}

func recomputeTop(b *BookState) {
	b.BestBid = 0
	b.BestAsk = 0
	for pk, sz := range b.Bids {
		if sz <= 0 {
			continue
		}
		p := parsePriceKey(pk)
		if p > b.BestBid {
			b.BestBid = p
		}
	}
	firstAsk := true
	for pk, sz := range b.Asks {
		if sz <= 0 {
			continue
		}
		p := parsePriceKey(pk)
		if firstAsk || p < b.BestAsk {
			b.BestAsk = p
			firstAsk = false
		}
	}
	recomputeDepth(b)
}

func recomputeDepth(b *BookState) {
	var bidD, askD float64
	for _, sz := range b.Bids {
		bidD += sz
	}
	for _, sz := range b.Asks {
		askD += sz
	}
	b.BidDepth = bidD
	b.AskDepth = askD
	tot := bidD + askD
	if tot > 0 {
		b.Imbalance = (bidD - askD) / tot
	} else {
		b.Imbalance = 0
	}
}

func priceKey(p float64) string {
	return fmt.Sprintf("%.6f", p)
}

func parsePriceKey(k string) float64 {
	var p float64
	_, _ = fmt.Sscanf(k, "%f", &p)
	return p
}

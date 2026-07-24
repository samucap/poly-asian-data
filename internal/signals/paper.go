package signals

import (
	"crypto/rand"
	"encoding/hex"
	"math"
	"sync"
	"time"

	"github.com/samucap/poly-asian-data/internal/edge"
)

// Emitter builds debounced paper signals from board + live books.
type Emitter struct {
	cfg GateConfig
	mu  sync.Mutex
	// last emit per condition
	last map[string]lastEmit
}

type lastEmit struct {
	At      time.Time
	Side    string
	EdgeBps float64
	ID      string
}

// NewEmitter creates a paper signal emitter.
func NewEmitter(cfg GateConfig) *Emitter {
	if cfg.ProbeSizeUSD <= 0 {
		cfg = DefaultGateConfig()
	}
	return &Emitter{cfg: cfg, last: make(map[string]lastEmit)}
}

// Evaluate attempts to build a paper signal. Returns nil if gates fail.
func (e *Emitter) Evaluate(now time.Time, strategy string, board BoardSnap, book BookSnap) *PaperSignal {
	if e == nil {
		return nil
	}
	if strategy == "" {
		strategy = "default"
	}
	if board.ConditionID == "" || board.TokenID == "" {
		return nil
	}
	if !bookOK(book) {
		return nil
	}
	cfg := e.cfg

	// Age gate
	if cfg.MaxBookAge > 0 && !book.UpdatedAt.IsZero() {
		age := now.Sub(book.UpdatedAt)
		if age > cfg.MaxBookAge {
			return nil
		}
	}

	mid := book.Mid
	if mid <= 0 {
		mid = (book.BestBid + book.BestAsk) / 2
	}
	spreadBps := 0.0
	if mid > 0 {
		spreadBps = 10_000 * (book.BestAsk - book.BestBid) / mid
	}
	if cfg.MaxSpreadBps > 0 && spreadBps > cfg.MaxSpreadBps {
		return nil
	}

	// Opportunity + cost
	// edge.Score stores EdgeBps/ModelEdgeBps as net (after cost). Fair-value path
	// yields a raw residual vs live mid and must still subtract live cost once.
	opportunity, modelEdge, path, fvSrc, fv, alreadyNet := opportunityBps(board, mid)
	probe := cfg.ProbeSizeUSD
	cost := edge.ComputeCost(edge.CostInput{
		BestBid:     book.BestBid,
		BestAsk:     book.BestAsk,
		BidDepth:    book.BidDepth,
		AskDepth:    book.AskDepth,
		SizeUSD:     probe,
		TakerFeeBps: cfg.DefaultFeeBps,
		ImpactCoeff: 50,
		ImpactCapBps: 25,
	})
	if !cost.HasBook {
		return nil
	}
	// Signed net: BUY when residual after cost > 0 (cheap vs FV/edge); SELL when rich.
	// Board net paths: do not re-subtract live cost (would double-count scan-time costs).
	// Live cost is still recorded on the signal for microstructure observability.
	var signedNet float64
	if alreadyNet {
		signedNet = opportunity - cfg.BufferBps
	} else {
		signedNet = opportunity - cost.TotalCostBps - cfg.BufferBps
	}
	var side string
	if signedNet > cfg.MinEdgeBps {
		side = SideBuy
	} else if signedNet < -cfg.MinEdgeBps {
		side = SideSell
	} else {
		return nil
	}
	absNet := math.Abs(signedNet)

	// Debounce: read-only check. Commit after successful DB insert (see Commit).
	e.mu.Lock()
	prev, hasPrev := e.last[board.ConditionID]
	if hasPrev && cfg.Cooldown > 0 {
		sameSide := prev.Side == side
		elapsed := now.Sub(prev.At)
		if sameSide && elapsed < cfg.Cooldown {
			if math.Abs(absNet-math.Abs(prev.EdgeBps)) < cfg.ReissueDeltaBps {
				e.mu.Unlock()
				return nil
			}
		}
	}
	e.mu.Unlock()

	conviction := absNet / cfg.ConvictionScale
	if conviction > 1 {
		conviction = 1
	}
	if conviction < 0 {
		conviction = 0
	}

	capUSD := 0.0
	if board.CapacityUSD != nil {
		capUSD = *board.CapacityUSD
	}
	if cost.CapacityUSD > 0 && (capUSD <= 0 || cost.CapacityUSD < capUSD) {
		capUSD = cost.CapacityUSD
	}
	sizeUSD := probe * (0.25 + 0.75*conviction) // scale with conviction
	if cfg.CapacityFrac > 0 && capUSD > 0 {
		lim := capUSD * cfg.CapacityFrac
		if sizeUSD > lim {
			sizeUSD = lim
		}
	}
	if cfg.MaxNotionalUSD > 0 && sizeUSD > cfg.MaxNotionalUSD {
		sizeUSD = cfg.MaxNotionalUSD
	}
	sizeShares := 0.0
	if mid > 0 {
		sizeShares = sizeUSD / mid
	}

	// Advisory quarter-Kelly proxy from conviction only (not full Kelly).
	kf := 0.25 * conviction
	kelly := &kf

	id := newSignalID()
	bookAgeMs := 0
	if !book.UpdatedAt.IsZero() {
		bookAgeMs = int(now.Sub(book.UpdatedAt).Milliseconds())
	}

	factors := BuildFactors(board, book, opportunity, cost.TotalCostBps, signedNet)
	features := map[string]any{}
	if board.KeyFeatures != nil {
		for k, v := range board.KeyFeatures {
			features[k] = v
		}
	}
	features["live_mid"] = mid
	features["live_spread_bps"] = spreadBps

	costBD := map[string]float64{
		"half_spread_bps": cost.HalfSpreadBps,
		"fee_bps":         cost.FeeBps,
		"impact_bps":      cost.ImpactBps,
		"total_cost_bps":  cost.TotalCostBps,
		"buffer_bps":      cfg.BufferBps,
	}

	urgency := 0.0
	if board.Urgency != nil {
		urgency = *board.Urgency
	}

	var modelPtr *float64
	if modelEdge != nil {
		modelPtr = modelEdge
	}
	var fvPtr *float64
	if fv != nil {
		fvPtr = fv
	}

	notes := "board net edge at scan time; live cost observational; paper only"
	if !alreadyNet {
		notes = "live residual after cost; paper only"
	}
	reason := map[string]any{
		"gates":         []string{"on_board", "net_edge_gt_min", "book_fresh", "spread_ok"},
		"min_edge_bps":  cfg.MinEdgeBps,
		"net_edge_bps":  signedNet,
		"reissue":       hasPrev,
		"debounce_sec":  int(cfg.Cooldown.Seconds()),
		"edge_already_net": alreadyNet,
		"notes":         notes,
	}

	sig := &PaperSignal{
		Time:              now.UTC(),
		SignalID:          id,
		Event:             EventOpen,
		Strategy:          strategy,
		StrategyVersionID: board.StrategyVersionID,
		BoardRunID:        board.RunID,
		BoardRank:         board.Rank,
		Mode:              ModePaper,
		ConditionID:       board.ConditionID,
		MarketID:          board.MarketID,
		TokenID:           board.TokenID,
		Outcome:           "YES",
		NegRisk:           board.NegRisk,
		NegRiskGroupID:    board.NegRiskGroupID,
		Side:              side,
		Action:            ActionEnter,
		TimeInForce:       "IOC_PAPER",
		EdgeBps:           signedNet,
		OpportunityBps:    opportunity,
		ModelEdgeBps:      modelPtr,
		CostBps:           cost.TotalCostBps,
		CostBreakdown:     costBD,
		FairValue:         fvPtr,
		FVSource:          fvSrc,
		ScorePath:         path,
		Conviction:        conviction,
		HorizonSec:        cfg.HorizonSec,
		HalfLifeSec:       cfg.HalfLifeSec,
		Urgency:           urgency,
		SizeUSD:           sizeUSD,
		SizeShares:        sizeShares,
		CapacityUSD:       capUSD,
		KellyFrac:         kelly,
		RiskFlags:         append([]string(nil), board.RiskFlags...),
		Mid:               mid,
		BestBid:           book.BestBid,
		BestAsk:           book.BestAsk,
		SpreadBps:         spreadBps,
		Imbalance:         book.Imbalance,
		BidDepth:          book.BidDepth,
		AskDepth:          book.AskDepth,
		LastTradePrice:    book.LastTradePrice,
		BookAgeMs:         bookAgeMs,
		FeatureAgeMs:      bookAgeMs,
		Features:          features,
		Factors:           factors,
		Tags:              append([]string(nil), board.StrategyTags...),
		Reason:            reason,
	}
	// Debounce state is committed only after successful persistence (Commit).
	return sig
}

// Commit records a successfully persisted signal for debounce/cooldown.
// Call only after InsertSignals (or equivalent) succeeds.
func (e *Emitter) Commit(sig *PaperSignal) {
	if e == nil || sig == nil || sig.ConditionID == "" {
		return
	}
	e.mu.Lock()
	e.last[sig.ConditionID] = lastEmit{
		At:      sig.Time,
		Side:    sig.Side,
		EdgeBps: sig.EdgeBps,
		ID:      sig.SignalID,
	}
	e.mu.Unlock()
}

func bookOK(b BookSnap) bool {
	return b.BestBid > 0 && b.BestAsk > 0 && b.BestAsk >= b.BestBid
}

// opportunityBps returns the edge input for netting.
// alreadyNet=true when the value is edge.Score net (ModelEdgeBps/EdgeBps); false for live FV residual.
func opportunityBps(board BoardSnap, mid float64) (opp float64, model *float64, path, fvSrc string, fv *float64, alreadyNet bool) {
	if board.FairValue != nil && mid > 0 {
		v := *board.FairValue
		fv = &v
		fvSrc = board.FVSource
		if fvSrc == "" {
			fvSrc = "board"
		}
		// residual: FV - mid in bps (long YES when FV > mid) — gross; caller subtracts live cost
		opp = (v - mid) * 10_000
		path = "fair_value"
		if board.ModelEdgeBps != nil {
			m := *board.ModelEdgeBps
			model = &m
		} else {
			m := opp
			model = &m
		}
		return opp, model, path, fvSrc, fv, false
	}
	if board.ModelEdgeBps != nil {
		// ModelEdgeBps from edge.Score is already net of cost (+ model buffer on FV path).
		opp = *board.ModelEdgeBps
		m := opp
		model = &m
		path = "model_edge"
		return opp, model, path, "", nil, true
	}
	if board.EdgeBps != nil {
		// EdgeBps from edge.Score is already net of cost.
		opp = *board.EdgeBps
		path = "board_edge"
		return opp, nil, path, "", nil, true
	}
	path = "none"
	return 0, nil, path, "", nil, false
}

func newSignalID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	// UUID-ish: set version 4 nibble
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	// format 8-4-4-4-12
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

package signaleval

import (
	"math"
	"sort"
	"time"

	"github.com/samucap/poly-asian-data/internal/risk"
)

// SimConfig controls the paper simulation loop.
type SimConfig struct {
	Risk            risk.Config
	DefaultHorizon  time.Duration // if signal.HorizonSec==0
	PeriodsPerYear  float64
}

// Simulate runs risk decisions + paper fills on signals in time order.
// When BatchWindowMs > 0, signals in a window are ranked by opportunity before Accept.
func Simulate(cfg SimConfig, signals []SignalIn, prices PriceIndex) Result {
	rcfg := cfg.Risk
	rcfg.Normalize()
	mgr := risk.NewManager(rcfg)
	horizonDef := cfg.DefaultHorizon
	if horizonDef <= 0 {
		horizonDef = time.Hour
	}
	ppy := cfg.PeriodsPerYear
	if ppy <= 0 {
		ppy = 24 * 365
	}

	// Sort by time
	sigs := append([]SignalIn(nil), signals...)
	sort.SliceStable(sigs, func(i, j int) bool {
		return sigs[i].Time.Before(sigs[j].Time)
	})

	rejectReasons := map[string]int{}
	var acceptedEdges, rejectedBudgetEdges []float64
	var acceptedConv, rejectedBudgetConv []float64
	var trades []Trade
	openByCond := map[string]int{} // condition → index in trades
	var equityCurve []EquityPoint
	startEq := mgr.Equity
	equityCurve = append(equityCurve, EquityPoint{Time: time.Time{}, Equity: startEq})
	if len(sigs) > 0 {
		equityCurve[0].Time = sigs[0].Time
	}

	processOne := func(sig SignalIn) {
		// Close due positions before new risk
		closeDue(mgr, &trades, openByCond, prices, sig.Time, &equityCurve)

		if mgr.ShouldFlattenAll() {
			flattenAll(mgr, &trades, openByCond, prices, sig.Time, &equityCurve)
		}

		in := risk.SignalInput{
			Time: sig.Time, ConditionID: sig.ConditionID, Side: sig.Side,
			SizeUSD: sig.SizeUSD, EdgeBps: sig.EdgeBps, Conviction: sig.Conviction,
			CapacityUSD: sig.CapacityUSD, Urgency: sig.Urgency, KellyFrac: sig.KellyFrac,
		}
		d := mgr.Accept(in)
		if !d.Accept {
			rejectReasons[d.Reason]++
			if d.Reason == risk.ReasonMaxGross || d.Reason == risk.ReasonMaxPositions {
				rejectedBudgetEdges = append(rejectedBudgetEdges, math.Abs(sig.EdgeBps))
				rejectedBudgetConv = append(rejectedBudgetConv, sig.Conviction)
			}
			return
		}

		mid := sig.Mid
		if mid <= 0 {
			mid = MidAsOf(prices[sig.TokenID], sig.Time)
		}
		if mid <= 0 {
			rejectReasons["no_mid"]++
			return
		}
		shares, costUSD, entryMid := EntryFill(sig.Side, d.SizeUSD, mid, sig.CostBps)
		if shares <= 0 {
			rejectReasons[risk.ReasonSizeZero]++
			return
		}

		acceptedEdges = append(acceptedEdges, math.Abs(sig.EdgeBps))
		acceptedConv = append(acceptedConv, sig.Conviction)

		tr := Trade{
			SignalID: sig.SignalID, ConditionID: sig.ConditionID, TokenID: sig.TokenID,
			Side: sig.Side, SizeUSD: d.SizeUSD, Shares: shares,
			EntryTime: sig.Time, EntryMid: entryMid, CostUSD: costUSD,
			EdgeBps: sig.EdgeBps, Conviction: sig.Conviction,
		}
		// schedule exit meta on trade via HorizonSec later
		hz := time.Duration(sig.HorizonSec) * time.Second
		if hz <= 0 {
			hz = horizonDef
		}
		// store exit deadline in ExitTime temporarily as planned exit if not closed
		tr.ExitTime = sig.Time.Add(hz)

		idx := len(trades)
		trades = append(trades, tr)
		openByCond[sig.ConditionID] = idx
		mgr.OnOpen(risk.Position{
			ConditionID: sig.ConditionID, Side: sig.Side, SizeUSD: d.SizeUSD,
			Shares: shares, EntryMid: entryMid, OpenedAt: sig.Time, SignalID: sig.SignalID,
		})
		equityCurve = append(equityCurve, EquityPoint{Time: sig.Time, Equity: mgr.Equity})
	}

	// Batch window processing
	win := time.Duration(rcfg.BatchWindowMs) * time.Millisecond
	i := 0
	for i < len(sigs) {
		if win <= 0 {
			processOne(sigs[i])
			i++
			continue
		}
		// collect batch
		start := sigs[i].Time
		var batch []SignalIn
		for i < len(sigs) && !sigs[i].Time.After(start.Add(win)) {
			batch = append(batch, sigs[i])
			i++
		}
		edges := make([]float64, len(batch))
		convs := make([]float64, len(batch))
		urgs := make([]float64, len(batch))
		for j, s := range batch {
			edges[j] = s.EdgeBps
			convs[j] = s.Conviction
			urgs[j] = s.Urgency
		}
		order := risk.BuildRankOrder(rcfg, edges, convs, urgs)
		for _, j := range order {
			processOne(batch[j])
		}
	}

	// Final time: close remaining at last known price
	endT := time.Now().UTC()
	if len(sigs) > 0 {
		endT = sigs[len(sigs)-1].Time.Add(horizonDef)
	}
	closeDue(mgr, &trades, openByCond, prices, endT, &equityCurve)
	// Force close anything still open at end
	for cond, idx := range openByCond {
		if idx < 0 || idx >= len(trades) || trades[idx].Closed {
			continue
		}
		closeTrade(mgr, &trades[idx], prices, endT, &equityCurve)
		delete(openByCond, cond)
	}
	flattenAll(mgr, &trades, openByCond, prices, endT, &equityCurve)

	equityCurve = append(equityCurve, EquityPoint{Time: endT, Equity: mgr.Equity})
	stats := ComputeEquityStats(equityCurve, startEq, ppy)

	// Metrics
	var nClosed, hits int
	var sumPnL, notional float64
	var nOpen int
	for _, tr := range trades {
		notional += tr.SizeUSD
		if tr.Closed {
			nClosed++
			sumPnL += tr.PnLUSD
			if tr.PnLUSD > 0 {
				hits++
			}
		} else {
			nOpen++
		}
	}
	nAccepted := len(trades)
	nRejected := 0
	for _, c := range rejectReasons {
		nRejected += c
	}

	m := Metrics{
		NSignals:            len(sigs),
		NAccepted:           nAccepted,
		NRejectedRisk:       nRejected,
		NClosed:             nClosed,
		NOpen:               nOpen,
		TotalPnLUSD:         mgr.Equity - startEq,
		TotalReturnBps:      stats.TotalReturnBps,
		Sharpe:              stats.Sharpe,
		SharpeNote:          stats.SharpeNote,
		MeanPeriodReturn:    stats.MeanPeriodReturn,
		PeriodReturnStdev:   stats.PeriodReturnStdev,
		NPeriods:            stats.NPeriods,
		MaxDrawdownBps:      stats.MaxDrawdownBps,
		MaxDailyDrawdownBps: stats.MaxDailyDrawdownBps,
		RejectReasons:       rejectReasons,
		StartingEquityUSD:   startEq,
		EndingEquityUSD:     mgr.Equity,
		PeriodsPerYear:      stats.PeriodsPerYear,
		Turnover:            notional / startEq,
	}
	if nClosed > 0 {
		m.HitRate = float64(hits) / float64(nClosed)
		m.AvgTradePnLUSD = sumPnL / float64(nClosed)
	}
	m.SelectionQuality = buildSelectionQuality(acceptedEdges, rejectedBudgetEdges, acceptedConv, rejectedBudgetConv)

	return Result{
		Metrics:     m,
		Trades:      trades,
		RiskEvents:  mgr.Events,
		EquityCurve: equityCurve,
		RiskCfg:     rcfg,
		ConfigHash:  risk.ConfigHash(rcfg),
	}
}

func closeDue(mgr *risk.Manager, trades *[]Trade, openByCond map[string]int, prices PriceIndex, now time.Time, curve *[]EquityPoint) {
	for cond, idx := range openByCond {
		if idx < 0 || idx >= len(*trades) {
			continue
		}
		tr := &(*trades)[idx]
		if tr.Closed {
			delete(openByCond, cond)
			continue
		}
		// ExitTime holds planned exit while open
		if !tr.ExitTime.IsZero() && !now.Before(tr.ExitTime) {
			closeTrade(mgr, tr, prices, tr.ExitTime, curve)
			delete(openByCond, cond)
		}
	}
}

func flattenAll(mgr *risk.Manager, trades *[]Trade, openByCond map[string]int, prices PriceIndex, now time.Time, curve *[]EquityPoint) {
	if !mgr.ShouldFlattenAll() {
		return
	}
	for cond, idx := range openByCond {
		if idx < 0 || idx >= len(*trades) {
			continue
		}
		tr := &(*trades)[idx]
		if tr.Closed {
			delete(openByCond, cond)
			continue
		}
		closeTrade(mgr, tr, prices, now, curve)
		delete(openByCond, cond)
	}
}

func closeTrade(mgr *risk.Manager, tr *Trade, prices PriceIndex, at time.Time, curve *[]EquityPoint) {
	if tr.Closed {
		return
	}
	exitMid := MidAsOf(prices[tr.TokenID], at)
	if exitMid <= 0 {
		exitMid = tr.EntryMid // flat if no price
	}
	pnl := RealizePnL(tr.Side, tr.Shares, tr.EntryMid, exitMid, tr.CostUSD)
	tr.ExitMid = exitMid
	tr.ExitTime = at
	tr.PnLUSD = pnl
	tr.Closed = true
	eqBefore := mgr.Equity
	mgr.OnClose(tr.ConditionID, pnl, at)
	// Feed vol_target lookback (fractional period return on close).
	if eqBefore > 0 {
		mgr.RecordPeriodReturn(pnl / eqBefore)
	}
	if curve != nil {
		*curve = append(*curve, EquityPoint{Time: at, Equity: mgr.Equity})
	}
}

func buildSelectionQuality(accE, rejE, accC, rejC []float64) *SelectionQuality {
	if len(accE) == 0 && len(rejE) == 0 {
		return nil
	}
	sq := &SelectionQuality{}
	sq.MeanAbsEdgeAccepted = mean(accE)
	sq.MeanAbsEdgeRejected = mean(rejE)
	sq.MeanConvictionAccepted = mean(accC)
	sq.MeanConvictionRejected = mean(rejC)
	if len(rejE) == 0 {
		sq.PreferHigherEdge = true
	} else {
		sq.PreferHigherEdge = sq.MeanAbsEdgeAccepted >= sq.MeanAbsEdgeRejected-1e-9
	}
	return sq
}

func mean(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	var s float64
	for _, x := range xs {
		s += x
	}
	return s / float64(len(xs))
}

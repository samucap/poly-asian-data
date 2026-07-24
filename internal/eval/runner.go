package eval

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/samucap/poly-asian-data/internal/artifacts"
	"github.com/samucap/poly-asian-data/internal/datagap"
	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/edge"
)

// PolicyID for offline candidate = edge.SelectBoard (production board policy).
const PolicyIDSelectBoardV1 = "select_board_v1"

// RunnerOpts controls a DB-only edge-eval run.
type RunnerOpts struct {
	DB               db.DBInterface
	Logger           *slog.Logger
	Cfg              Config
	Backtest         BacktestConfig
	ArtifactsRoot    string
	Strategy         string
	PersistLabels    bool
	BookMaxStaleness time.Duration
	UseBooks         *bool
	WeightsPath      string // for lineage hash
	WeightsYAML      []byte // optional raw bytes for hash if path empty
	// StrategyVersionID stamps eval_surface.strategy_version_id (M5).
	StrategyVersionID *int64
	// WeightsHash if non-empty is used as lineage hash (e.g. from strategy_versions).
	// Else hash WeightsPath or WeightsYAML.
	WeightsHash string
	// SynthFill enables development gap-fill (interp/hold prices + synth books). Labeled on surface.
	SynthFill bool
	// Synth is options for gap fill; zero value uses datagap.DefaultOpts when SynthFill.
	Synth datagap.Opts
	// SynthPromoteMaxShare: synth share above this blocks promote (default 0.05).
	SynthPromoteMaxShare float64
	// SynthSignificantShare: share at/above this is a loud alert and forces ok=false (default 0.20).
	SynthSignificantShare float64
}

// RunResult is the outcome of one edge-eval pass.
type RunResult struct {
	Surface  *EvalSurface
	Labels   []Label
	Write    artifacts.WriteResult
	NSnaps   int
	NTokens  int
	Duration time.Duration
}

// RunDBOnly loads prices (+ books) once, SelectBoard ranks, labels with score costs, gates.
func RunDBOnly(ctx context.Context, opts RunnerOpts) (*RunResult, error) {
	start := time.Now()
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	if opts.DB == nil {
		return nil, fmt.Errorf("eval: nil db")
	}
	cfg := opts.Cfg
	if cfg.PrimaryHorizon == "" {
		cfg = DefaultConfig()
	}
	bc := opts.Backtest
	bc.Cfg = cfg
	if bc.BoardN <= 0 {
		bc.BoardN = 50
	}
	if bc.Lookback <= 0 {
		bc.Lookback = 30 * 24 * time.Hour
	}
	if bc.End.IsZero() {
		bc.End = time.Now().UTC()
	}
	if bc.Weights.Name == "" {
		bc.Weights = edge.DefaultWeights()
	}
	bookAge := opts.BookMaxStaleness
	if bookAge <= 0 {
		bookAge = 2 * time.Hour
	}
	useBooks := true
	if opts.UseBooks != nil {
		useBooks = *opts.UseBooks
	}

	markets, tokenIDs, err := loadUniverse(ctx, opts.DB, opts.Strategy, bc.UniverseCap, log)
	if err != nil {
		return nil, err
	}
	if len(markets) == 0 {
		return emptySurface(cfg, opts.ArtifactsRoot, "no markets with tokens in DB", start, bc, opts)
	}

	maxH := MaxHorizonDuration(cfg)
	from := bc.End.Add(-bc.Lookback - maxH - 24*time.Hour)
	to := bc.End

	seriesMap, err := db.LoadPriceSeries(ctx, opts.DB, tokenIDs, from, to)
	if err != nil {
		return nil, fmt.Errorf("eval: load price series: %w", err)
	}
	prices := map[string]PriceSeries{}
	for tok, pts := range seriesMap {
		ps := PriceSeries{TokenID: tok}
		for _, p := range pts {
			ps.Times = append(ps.Times, p.Timestamp)
			ps.Prices = append(ps.Prices, p.Price)
		}
		if len(ps.Times) > 0 {
			prices[tok] = ps
		}
	}
	if len(prices) == 0 {
		return emptySurface(cfg, opts.ArtifactsRoot, "no prices_history rows for token set", start, bc, opts)
	}

	booksByTok := map[string]BookSeries{}
	if useBooks {
		raw, err := db.LoadBookSeries(ctx, opts.DB, tokenIDs, from, to)
		if err != nil {
			log.Warn("load book series failed; prices-only costs", "error", err)
		} else {
			for tok, pts := range raw {
				bs := BookSeries{TokenID: tok}
				for _, p := range pts {
					bs.Points = append(bs.Points, BookPoint{
						Time: p.Time, BestBid: p.BestBid, BestAsk: p.BestAsk,
						TotalBidDepth: p.TotalBidDepth, TotalAskDepth: p.TotalAskDepth,
					})
				}
				if len(bs.Points) > 0 {
					booksByTok[tok] = bs
				}
			}
			log.Info("book series loaded", "tokens", len(booksByTok))
		}
	}

	ts := DecisionTimes(bc, maxH)
	if len(ts) == 0 {
		return emptySurface(cfg, opts.ArtifactsRoot, "empty decision grid", start, bc, opts)
	}

	var dq *DataQuality
	if opts.SynthFill {
		synthOpts := opts.Synth
		synthOpts.Normalize()
		report := applySynthFill(prices, booksByTok, ts, from, to, synthOpts, useBooks)
		promoteMax := opts.SynthPromoteMaxShare
		if promoteMax <= 0 {
			promoteMax = datagap.PromoteMaxSynthShare
		}
		sigShare := opts.SynthSignificantShare
		if sigShare <= 0 {
			sigShare = datagap.SignificantSynthShare
		}
		report.MaxGap = synthOpts.MaxGap.String()
		report.HoldMax = synthOpts.HoldMax.String()
		report.Finalize(promoteMax, sigShare)
		dq = dataQualityFromReport(report)
		if ban := report.Banner(); ban != "" {
			fmt.Fprint(os.Stderr, ban)
			if report.Significant {
				log.Error("significant_synthetic_data",
					"synth_price_share", report.SynthPriceShare,
					"synth_book_share", report.SynthBookShare,
					"block_promote", report.BlockPromote,
					"warning", report.Warning,
				)
			} else {
				log.Warn("synthetic_fill_active",
					"synth_price_share", report.SynthPriceShare,
					"synth_book_share", report.SynthBookShare,
					"block_promote", report.BlockPromote,
					"warning", report.Warning,
				)
			}
		}
		log.Info("synth fill applied",
			"price_venue", report.PriceMix.Venue,
			"price_synth_interp", report.PriceMix.SynthInterp,
			"price_synth_hold", report.PriceMix.SynthHold,
			"book_synth", report.BookMix.SynthBook,
			"synth_price_share", report.SynthPriceShare,
			"synth_book_share", report.SynthBookShare,
		)
	}

	minDepth := bc.Weights.MinDepthShares
	if minDepth <= 0 {
		minDepth = 10
	}

	// Meta index for group FV
	metaByCond := map[string]MetaAsOf{}
	for _, m := range markets {
		metaByCond[m.ConditionID] = MetaAsOf{
			ConditionID: m.ConditionID, TokenID: m.TokenID, Category: m.Category,
			NegRisk: m.NegRisk, NegRiskGroupID: m.NegRiskGroupID, RelatedLegs: m.RelatedLegs,
			EndDate: m.EndDate,
		}
	}

	// Bulk-load board snapshots once (L2/L3/L4/L7) — no per-T SQL.
	strategyName := opts.Strategy
	if strategyName == "" {
		strategyName = "default"
	}
	snapRows, snapErr := db.LoadEdgeBoardSnapshots(ctx, opts.DB, strategyName, from, to)
	if snapErr != nil {
		log.Warn("load edge_board_snapshots failed; series-only PIT meta", "error", snapErr)
		snapRows = nil
	}
	snapIdx := BuildSnapIndex(snapRows)
	snapTimes, snapN := snapIdx.Stats()
	snapHit := 0
	snapMiss := 0

	snaps := make([]SnapshotAtT, 0, len(ts))
	for _, t := range ts {
		var pts []MarketPoint
		for _, m := range markets {
			ps, ok := prices[m.TokenID]
			if !ok {
				continue
			}
			if !m.EndDate.IsZero() && !m.EndDate.After(t) {
				continue
			}
			meta := metaByCond[m.ConditionID]
			// Overlay snapshot meta when available (category, neg_risk, legs, volume).
			sm := snapIdx.LookupCond(t, m.ConditionID)
			if sm.Found {
				snapHit++
				if sm.Category != "" {
					meta.Category = sm.Category
				}
				meta.NegRisk = sm.NegRisk
				if sm.NegRiskGroupID != "" {
					meta.NegRiskGroupID = sm.NegRiskGroupID
				}
				if len(sm.RelatedLegs) > 0 {
					meta.RelatedLegs = sm.RelatedLegs
				}
			} else {
				snapMiss++
			}
			var bookPtr *BookPoint
			if bs, ok := booksByTok[m.TokenID]; ok {
				if bp, ok := bs.AsOf(t, bookAge); ok {
					bookPtr = &bp
				}
			}
			feat, ok := FeaturesAsOf(t, meta, ps, bookPtr, minDepth)
			if !ok {
				continue
			}
			mid, _ := ps.MidAsOf(t)
			hs := 0.0
			costApprox := true
			if bookPtr != nil {
				hs = HalfSpreadBpsFromBook(bookPtr.BestBid, bookPtr.BestAsk)
				costApprox = hs <= 0
			}
			gMids := GroupMidsAtT(t, meta, metaByCond, prices)
			volProxy := VolumeProxy24h(ps, t)
			if sm.Found && sm.HasVolume && sm.Volume24hr > 0 {
				volProxy = sm.Volume24hr
			}
			mp := MarketPoint{
				ConditionID:    m.ConditionID,
				TokenID:        m.TokenID,
				Category:       meta.Category,
				NegRisk:        meta.NegRisk,
				NegRiskGroupID: meta.NegRiskGroupID,
				RelatedLegs:    meta.RelatedLegs,
				Mid:            mid,
				HalfSpreadBps:  hs,
				CostApprox:     costApprox,
				VolumeProxy:    volProxy,
				ActivityProxy:  ActivityProxy24h(ps, t),
				EndTime:        m.EndDate,
				Features:       feat,
				GroupMids:      gMids,
			}
			if !m.EndDate.IsZero() {
				mp.TTRHours = m.EndDate.Sub(t).Hours()
			}
			pts = append(pts, mp)
		}
		if len(pts) > 0 {
			snaps = append(snaps, SnapshotAtT{T: t, Markets: pts})
		}
	}

	if bc.ActionModel == "" {
		bc.ActionModel = cfg.ActionModel
	}
	labels, selStats := RunBacktest(snaps, prices, bc)
	metrics := BuildMetrics(labels, cfg.PrimaryHorizon)

	env := artifacts.NewEnvelope(SchemaVersion, "")
	s := NewSurface(cfg, metrics, CandidateFeatureNames, env.RunID)
	s.PipelineVersion = env.PipelineVersion
	s.CodeCommit = env.CodeCommit
	s.GeneratedAt = env.GeneratedAt
	s.DataQuality = dq
	s.StrategyName = bc.Weights.Name
	if s.StrategyName == "" {
		s.StrategyName = "default"
	}
	s.PolicyID = PolicyIDSelectBoardV1
	s.PolicyParity = PolicyParityScanBoard
	s.ActionModel = bc.ActionModel
	if s.ActionModel == "" {
		s.ActionModel = ActionSignFromEdge
	}
	hitRate := 0.0
	if snapHit+snapMiss > 0 {
		hitRate = float64(snapHit) / float64(snapHit+snapMiss)
	}
	s.UniverseNote = fmt.Sprintf(
		"membership=series_mid@T; snapshots=%d times/%d rows; snap_meta_hit_rate=%.3f; sticky=live_only; features/labels=PIT",
		snapTimes, snapN, hitRate,
	)
	s.BaselineNotes = "volume_top_n=snapshot_volume|series_proxy; activity_stage1=series sum|Δmid| 24h; label costs=edge.ComputeCost/Score TotalCostBps; candidate action_model=" + s.ActionModel + "; baselines=long_yes"
	s.LabelProtocol.AsOfField = "features_asof"
	s.LabelProtocol.NoFutureInFeatures = true
	s.LabelProtocol.PointInTime = true
	s.WeightsPath = opts.WeightsPath
	s.StrategyVersionID = opts.StrategyVersionID
	if opts.WeightsHash != "" {
		s.WeightsHash = opts.WeightsHash
	} else if opts.WeightsPath != "" {
		if h, err := WeightsHashFile(opts.WeightsPath); err == nil {
			s.WeightsHash = h
		}
	} else if len(opts.WeightsYAML) > 0 {
		s.WeightsHash = WeightsHashBytes(opts.WeightsYAML)
	}

	// Coverages from selection stats
	if selStats.BoardN > 0 {
		s.FVCoverage = float64(selStats.FVHits) / float64(selStats.BoardN)
		s.BookCoverage = float64(selStats.WithBook) / float64(max(selStats.UniverseN, 1))
	}

	if metrics.N == 0 {
		s.Errors = append(s.Errors, ErrorItem{
			Code: "min_sample", Message: "no labeled decisions (need prices at T and T+h)", Component: "eval",
		})
	}
	if err := FinalizeSurface(s, cfg); err != nil {
		return nil, err
	}

	if opts.PersistLabels {
		if err := db.EnsureLabelRowsTable(ctx, opts.DB); err != nil {
			log.Warn("ensure label_rows failed", "error", err)
		} else {
			rows := labelsToDB(labels, s.RunID)
			if err := db.InsertLabelRows(ctx, opts.DB, rows); err != nil {
				log.Warn("insert label_rows failed", "error", err)
			} else {
				log.Info("label_rows written", "n", len(rows))
			}
		}
	}

	var wr artifacts.WriteResult
	if opts.ArtifactsRoot != "" {
		wr, err = WriteSurface(s, opts.ArtifactsRoot)
		if err != nil {
			return nil, fmt.Errorf("eval: write surface: %w", err)
		}
		log.Info("eval_surface written",
			"path", wr.RunPath,
			"ok", s.OK,
			"promote_eligible", s.PromoteEligible,
			"policy_parity", s.PolicyParity,
			"fv_coverage", s.FVCoverage,
			"book_coverage", s.BookCoverage,
			"n", s.Metrics.N,
			"weights_hash", s.WeightsHash,
		)
	}

	return &RunResult{
		Surface: s, Labels: labels, Write: wr,
		NSnaps: len(snaps), NTokens: len(prices), Duration: time.Since(start),
	}, nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// applySynthFill mutates prices and booksByTok in place with development gap-fill.
func applySynthFill(
	prices map[string]PriceSeries,
	booksByTok map[string]BookSeries,
	decisionTimes []time.Time,
	from, to time.Time,
	opts datagap.Opts,
	useBooks bool,
) datagap.Report {
	report := datagap.Report{FillMode: "real_plus_synth"}
	grid := decisionTimes
	if len(grid) == 0 {
		grid = datagap.BuildHourlyGrid(from, to, opts.GridStep)
	}
	for tok, ps := range prices {
		real := make([]datagap.PricePoint, 0, len(ps.Times))
		for i := range ps.Times {
			real = append(real, datagap.PricePoint{
				Time: ps.Times[i], Price: ps.Prices[i], Source: datagap.SourceVenue,
			})
		}
		filled, mix := datagap.FillPrices(real, grid, opts)
		report.PriceMix.Add(mix)
		ns := PriceSeries{TokenID: tok}
		for _, p := range filled {
			ns.Times = append(ns.Times, p.Time)
			ns.Prices = append(ns.Prices, p.Price)
		}
		prices[tok] = ns
	}
	if !useBooks {
		return report
	}
	for tok, ps := range prices {
		bs := booksByTok[tok]
		realB := make([]datagap.BookPoint, 0, len(bs.Points))
		for _, p := range bs.Points {
			realB = append(realB, datagap.BookPoint{
				Time: p.Time, BestBid: p.BestBid, BestAsk: p.BestAsk,
				TotalBidDepth: p.TotalBidDepth, TotalAskDepth: p.TotalAskDepth,
				Source: datagap.SourceVenue,
			})
		}
		midAt := func(t time.Time) (float64, bool) {
			return ps.MidAsOf(t)
		}
		filledB, mix := datagap.FillBooks(realB, grid, midAt, opts)
		report.BookMix.Add(mix)
		nbs := BookSeries{TokenID: tok}
		for _, p := range filledB {
			nbs.Points = append(nbs.Points, BookPoint{
				Time: p.Time, BestBid: p.BestBid, BestAsk: p.BestAsk,
				TotalBidDepth: p.TotalBidDepth, TotalAskDepth: p.TotalAskDepth,
			})
		}
		if len(nbs.Points) > 0 {
			booksByTok[tok] = nbs
		}
	}
	// Tokens with prices but no book series entry
	for tok, ps := range prices {
		if _, ok := booksByTok[tok]; ok {
			continue
		}
		midAt := func(t time.Time) (float64, bool) { return ps.MidAsOf(t) }
		filledB, mix := datagap.FillBooks(nil, grid, midAt, opts)
		report.BookMix.Add(mix)
		nbs := BookSeries{TokenID: tok}
		for _, p := range filledB {
			nbs.Points = append(nbs.Points, BookPoint{
				Time: p.Time, BestBid: p.BestBid, BestAsk: p.BestAsk,
				TotalBidDepth: p.TotalBidDepth, TotalAskDepth: p.TotalAskDepth,
			})
		}
		if len(nbs.Points) > 0 {
			booksByTok[tok] = nbs
		}
	}
	return report
}

func dataQualityFromReport(r datagap.Report) *DataQuality {
	return &DataQuality{
		PriceSourceMix: map[string]int{
			"venue":        r.PriceMix.Venue,
			"synth_interp": r.PriceMix.SynthInterp,
			"synth_hold":   r.PriceMix.SynthHold,
			"missing":      r.PriceMix.Missing,
		},
		BookSourceMix: map[string]int{
			"venue":              r.BookMix.Venue,
			"synth_book_from_mid": r.BookMix.SynthBook,
			"missing":            r.BookMix.Missing,
		},
		SynthPriceShare:  r.SynthPriceShare,
		SynthBookShare:   r.SynthBookShare,
		FillMode:         r.FillMode,
		SynthMaxGap:      r.MaxGap,
		SynthHoldMax:     r.HoldMax,
		Warning:          r.Warning,
		SignificantSynth: r.Significant,
		BlockPromote:     r.BlockPromote,
	}
}

func loadUniverse(ctx context.Context, conn db.DBInterface, strategy string, cap int, log *slog.Logger) ([]db.EvalMarketMeta, []string, error) {
	if cap <= 0 {
		cap = 500
	}
	top, err := db.LoadTopVolumeMarkets(ctx, conn, cap)
	if err != nil {
		return nil, nil, fmt.Errorf("eval: load top volume: %w", err)
	}
	board, err := db.LoadEdgeBoardTokens(ctx, conn, strategy)
	if err != nil {
		log.Warn("load edge board tokens failed (volume only)", "error", err)
		board = nil
	}
	byCond := map[string]db.EvalMarketMeta{}
	for _, m := range top {
		byCond[m.ConditionID] = m
	}
	for _, m := range board {
		if existing, ok := byCond[m.ConditionID]; ok {
			if m.Category != "" {
				existing.Category = m.Category
			}
			if m.TokenID != "" {
				existing.TokenID = m.TokenID
			}
			if m.NegRiskGroupID != "" {
				existing.NegRiskGroupID = m.NegRiskGroupID
				existing.NegRisk = m.NegRisk
			}
			if len(m.RelatedLegs) > 0 {
				existing.RelatedLegs = m.RelatedLegs
			}
			byCond[m.ConditionID] = existing
		} else {
			byCond[m.ConditionID] = m
		}
	}
	markets := make([]db.EvalMarketMeta, 0, len(byCond))
	tokenIDs := make([]string, 0, len(byCond))
	for _, m := range byCond {
		if m.TokenID == "" {
			continue
		}
		markets = append(markets, m)
		tokenIDs = append(tokenIDs, m.TokenID)
	}
	return markets, tokenIDs, nil
}

func emptySurface(cfg Config, root, msg string, start time.Time, bc BacktestConfig, opts RunnerOpts) (*RunResult, error) {
	env := artifacts.NewEnvelope(SchemaVersion, "")
	s := NewSurface(cfg, EvalMetrics{PrimaryHorizon: cfg.PrimaryHorizon, Overall: HorizonMetrics{}}, CandidateFeatureNames, env.RunID)
	s.PipelineVersion = env.PipelineVersion
	s.CodeCommit = env.CodeCommit
	s.GeneratedAt = env.GeneratedAt
	s.StrategyName = bc.Weights.Name
	s.PolicyID = PolicyIDSelectBoardV1
	s.PolicyParity = PolicyParityScanBoard
	s.WeightsPath = opts.WeightsPath
	s.Errors = []ErrorItem{{Code: "no_data", Message: msg, Component: "eval"}}
	_ = FinalizeSurface(s, cfg)
	var wr artifacts.WriteResult
	if root != "" {
		wr, _ = WriteSurface(s, root)
	}
	return &RunResult{Surface: s, Write: wr, Duration: time.Since(start)}, nil
}

func labelsToDB(labels []Label, runID string) []db.LabelRow {
	var out []db.LabelRow
	for _, l := range labels {
		sel := l.SelectionSet
		if l.Policy != "" && l.Policy != "candidate" {
			sel = l.Policy
		} else if sel == "" {
			sel = SelectionBoard
		}
		hit := l.Hit
		ac := l.AfterCostReturnBps
		mid := l.MidAtT
		nr := l.NegRisk
		edgeB := l.EdgeBpsAtT
		out = append(out, db.LabelRow{
			DecisionTime: l.DecisionTime, Horizon: l.Horizon, ConditionID: l.ConditionID,
			SelectionSet: sel, RunID: runID, Hit: &hit, AfterCostReturnBps: &ac,
			Category: l.Category, NegRisk: &nr, FVSource: l.FVSource, TTRBucket: l.TTRBucket,
			MidAtT: &mid, EdgeBpsAtT: &edgeB,
		})
	}
	return out
}

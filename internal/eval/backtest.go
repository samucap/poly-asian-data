package eval

import (
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/samucap/poly-asian-data/internal/edge"
)

// MarketPoint is one market's PIT state at a decision time.
type MarketPoint struct {
	ConditionID    string
	TokenID        string
	Category       string
	NegRisk        bool
	NegRiskGroupID string
	RelatedLegs    []string
	Mid            float64
	HalfSpreadBps  float64
	CostApprox     bool
	VolumeProxy    float64
	ActivityProxy  float64
	TTRHours       float64
	EndTime        time.Time
	FVSource       string
	EdgeBpsAtT     float64
	// EntryCostBps from edge.Score Cost.TotalCostBps (label cost identity).
	EntryCostBps float64
	Features     edge.FeatureVector
	GroupMids    map[string]float64
}

// BacktestConfig controls the offline loop.
type BacktestConfig struct {
	Cfg         Config
	BoardN      int
	UniverseCap int
	Lookback    time.Duration
	Bucket      time.Duration
	Stride      int
	Seed        int64
	End         time.Time
	Weights     edge.Weights
	// ActionModel overrides Cfg.ActionModel when non-empty.
	ActionModel string
}

// DefaultBacktestConfig returns sensible M4 defaults.
func DefaultBacktestConfig() BacktestConfig {
	return BacktestConfig{
		Cfg:         DefaultConfig(),
		BoardN:      50,
		UniverseCap: 500,
		Lookback:    30 * 24 * time.Hour,
		Bucket:      time.Hour,
		Stride:      2,
		Seed:        42,
		Weights:     edge.DefaultWeights(),
	}
}

// DecisionTimes builds a strided hour grid over [end-lookback, end - maxHorizon].
func DecisionTimes(bc BacktestConfig, maxHorizon time.Duration) []time.Time {
	end := bc.End
	if end.IsZero() {
		end = time.Now().UTC()
	}
	end = end.Add(-maxHorizon)
	start := end.Add(-bc.Lookback)
	bucket := bc.Bucket
	if bucket <= 0 {
		bucket = time.Hour
	}
	stride := bc.Stride
	if stride <= 0 {
		stride = 1
	}
	t := start.Truncate(bucket)
	if t.Before(start) {
		t = t.Add(bucket)
	}
	var out []time.Time
	i := 0
	for !t.After(end) {
		if i%stride == 0 {
			out = append(out, t)
		}
		t = t.Add(bucket)
		i++
	}
	return out
}

// MaxHorizonDuration returns the longest horizon in cfg.
func MaxHorizonDuration(cfg Config) time.Duration {
	var max time.Duration
	for _, h := range cfg.Horizons {
		d, err := ParseHorizon(h)
		if err != nil {
			continue
		}
		if d > max {
			max = d
		}
	}
	if max == 0 {
		max = 24 * time.Hour
	}
	return max
}

// SnapshotAtT is the universe at decision time T.
type SnapshotAtT struct {
	T       time.Time
	Markets []MarketPoint
}

// BoardSelectStats diagnostics for policy_parity.
type BoardSelectStats struct {
	FVHits         int
	BoardN         int
	WithBook       int
	UniverseN      int
	ExtremeDropped int
}

// RunBacktest ranks with edge.SelectBoard (production policy) + PIT baselines.
func RunBacktest(snaps []SnapshotAtT, prices map[string]PriceSeries, bc BacktestConfig) ([]Label, BoardSelectStats) {
	var stats BoardSelectStats
	if bc.BoardN <= 0 {
		bc.BoardN = 50
	}
	if bc.UniverseCap <= 0 {
		bc.UniverseCap = 500
	}
	w := bc.Weights
	if w.Name == "" {
		w = edge.DefaultWeights()
	}
	// Offline: keep missing-book markets (prices-only path); still apply extreme + FV.
	w.DropMissingBook = false

	rng := rand.New(rand.NewSource(bc.Seed))
	var labels []Label
	horizons := bc.Cfg.Horizons
	if len(horizons) == 0 {
		horizons = DefaultHorizons
	}
	actionModel := bc.ActionModel
	if actionModel == "" {
		actionModel = bc.Cfg.ActionModel
	}
	if actionModel == "" {
		actionModel = ActionSignFromEdge
	}

	for _, snap := range snaps {
		universe := snap.Markets
		if len(universe) > bc.UniverseCap {
			sort.SliceStable(universe, func(i, j int) bool {
				return universe[i].VolumeProxy > universe[j].VolumeProxy
			})
			universe = universe[:bc.UniverseCap]
		}
		if len(universe) == 0 {
			continue
		}
		stats.UniverseN += len(universe)

		cand, st := selectCandidateBoard(universe, bc.BoardN, w)
		stats.FVHits += st.FVHits
		stats.BoardN += len(cand)
		stats.WithBook += st.WithBook

		vol := topN(universe, bc.BoardN, func(m MarketPoint) float64 { return m.VolumeProxy })
		act := topN(universe, bc.BoardN, func(m MarketPoint) float64 { return m.ActivityProxy })
		rnd := randomN(universe, bc.BoardN, rng)

		// Cost as-of T for baselines (same ComputeCost stack as Score).
		costAt := func(m MarketPoint) FillParams {
			c := edge.ComputeCost(edge.CostInput{
				BestBid:      m.Features.BestBid,
				BestAsk:      m.Features.BestAsk,
				BidDepth:     m.Features.BidDepth,
				AskDepth:     m.Features.AskDepth,
				SizeUSD:      w.ProbeSizeUSD,
				TakerFeeBps:  w.DefaultTakerFeeBps,
				ImpactCapBps: w.ImpactCapBps,
				ImpactCoeff:  w.ImpactCoeff,
			})
			return FillParamsFromCost(c)
		}

		policies := []struct {
			name string
			set  []MarketPoint
		}{
			{"candidate", cand},
			{"volume_top_n", vol},
			{"activity_stage1", act},
			{"random_board", rnd},
		}

		for _, pol := range policies {
			for _, m := range pol.set {
				if !m.EndTime.IsZero() && !m.EndTime.After(snap.T) {
					continue
				}
				series, ok := prices[m.TokenID]
				if !ok || m.TokenID == "" {
					continue
				}
				midT, ok := series.MidAsOf(snap.T)
				if !ok || midT <= 0 {
					continue
				}
				var fp FillParams
				if pol.name == "candidate" && m.EntryCostBps != 0 {
					tot := m.EntryCostBps
					fp = FillParams{TotalOverride: &tot}
				} else {
					fp = costAt(m)
				}
				for _, hName := range horizons {
					hd, err := ParseHorizon(hName)
					if err != nil {
						continue
					}
					tH := snap.T.Add(hd)
					if !m.EndTime.IsZero() && m.EndTime.Before(tH) {
						continue
					}
					midH, ok := series.MidAsOf(tH)
					if !ok || midH <= 0 {
						continue
					}
					// Candidate uses action_model; baselines stay long_yes for fair dumb-screen deltas.
					var ac float64
					if pol.name == "candidate" {
						ac = AfterCostReturnBpsAction(midT, midH, fp, actionModel, m.EdgeBpsAtT)
					} else {
						ac = AfterCostReturnBps(midT, midH, fp)
					}
					if math.IsNaN(ac) {
						continue
					}
					fv := m.FVSource
					if fv == "" {
						fv = "none"
					}
					labels = append(labels, Label{
						DecisionTime:       snap.T,
						Horizon:            hName,
						ConditionID:        m.ConditionID,
						SelectionSet:       SelectionBoard,
						Hit:                Hit(ac),
						AfterCostReturnBps: ac,
						Category:           m.Category,
						NegRisk:            m.NegRisk,
						FVSource:           fv,
						TTRBucket:          TTRBucket(m.TTRHours),
						MidAtT:             midT,
						EdgeBpsAtT:         m.EdgeBpsAtT,
						Policy:             pol.name,
					})
				}
			}
		}
	}
	return labels, stats
}

func selectCandidateBoard(universe []MarketPoint, n int, w edge.Weights) ([]MarketPoint, BoardSelectStats) {
	var st BoardSelectStats
	cands := make([]edge.BoardCandidate, 0, len(universe))
	byTok := map[string]MarketPoint{}
	for _, m := range universe {
		byTok[m.TokenID] = m
		if !m.Features.MissingBook {
			st.WithBook++
		}
		cands = append(cands, edge.BoardCandidate{
			Features:       m.Features,
			GroupMids:      m.GroupMids,
			KnownGroupSize: len(m.GroupMids),
			TieBreak:       m.VolumeProxy,
		})
	}
	// Count extremes that would drop (for diagnostics)
	if w.DropExtremePrice {
		for _, m := range universe {
			if !m.Features.MissingBook && edge.IsExtremeMid(m.Features.Mid, w.ExtremeLo, w.ExtremeHi) {
				st.ExtremeDropped++
			}
		}
	}

	ranked := edge.SelectBoard(cands, w, n)
	out := make([]MarketPoint, 0, len(ranked))
	for _, r := range ranked {
		tok := r.Candidate.Features.TokenID
		m := byTok[tok]
		m.EdgeBpsAtT = r.Result.EdgeBps
		m.EntryCostBps = r.Result.Cost.TotalCostBps
		if r.Result.FVSource != "" {
			m.FVSource = r.Result.FVSource
			st.FVHits++
		} else if r.Result.Path != "" {
			m.FVSource = r.Result.Path
		}
		out = append(out, m)
	}
	return out, st
}

func topN(universe []MarketPoint, n int, score func(MarketPoint) float64) []MarketPoint {
	type sc struct {
		m MarketPoint
		s float64
	}
	arr := make([]sc, 0, len(universe))
	for _, m := range universe {
		arr = append(arr, sc{m: m, s: score(m)})
	}
	sort.SliceStable(arr, func(i, j int) bool {
		return arr[i].s > arr[j].s
	})
	if n > len(arr) {
		n = len(arr)
	}
	out := make([]MarketPoint, n)
	for i := 0; i < n; i++ {
		out[i] = arr[i].m
	}
	return out
}

func randomN(universe []MarketPoint, n int, rng *rand.Rand) []MarketPoint {
	cp := append([]MarketPoint{}, universe...)
	rng.Shuffle(len(cp), func(i, j int) { cp[i], cp[j] = cp[j], cp[i] })
	if n > len(cp) {
		n = len(cp)
	}
	return cp[:n]
}

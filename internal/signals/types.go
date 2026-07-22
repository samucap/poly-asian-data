// Package signals builds multi-dimensional paper trade intents (M6).
// No exchange orders. Portfolio max drawdown lives in M8 risk eval, not here.
package signals

import (
	"time"
)

// ModePaper is the only mode in M6.
const ModePaper = "paper"

// Side / action constants.
const (
	SideBuy  = "BUY"
	SideSell = "SELL"

	ActionEnter   = "ENTER"
	ActionExit    = "EXIT"
	ActionScaleIn = "SCALE_IN"
	ActionReduce  = "REDUCE"

	EventOpen   = "open"
	EventUpdate = "update"
	EventCancel = "cancel"
)

// PaperSignal is a multi-dimensional paper intent (DB + artifact shape).
type PaperSignal struct {
	Time              time.Time
	SignalID          string
	Event             string
	SupersedesID      string
	Strategy          string
	StrategyVersionID *int64
	BoardRunID        string
	BoardRank         int
	Mode              string

	ConditionID    string
	MarketID       string
	TokenID        string
	Outcome        string
	NegRisk        bool
	NegRiskGroupID string

	Side        string
	Action      string
	TimeInForce string

	EdgeBps         float64
	OpportunityBps  float64
	ModelEdgeBps    *float64
	CostBps         float64
	CostBreakdown   map[string]float64
	FairValue       *float64
	FVSource        string
	ScorePath       string

	Conviction  float64
	HorizonSec  int
	HalfLifeSec int
	Urgency     float64

	SizeUSD     float64
	SizeShares  float64
	CapacityUSD float64
	KellyFrac   *float64
	RiskFlags   []string

	Mid            float64
	BestBid        float64
	BestAsk        float64
	SpreadBps      float64
	Imbalance      float64
	BidDepth       float64
	AskDepth       float64
	LastTradePrice float64
	BookAgeMs      int
	FeatureAgeMs   int

	Features map[string]any
	Factors  map[string]float64
	Tags     []string
	Reason   map[string]any
}

// BookSnap is live top-of-book for signal build (from memory, not DB).
type BookSnap struct {
	BestBid        float64
	BestAsk        float64
	Mid            float64
	LastTradePrice float64
	Imbalance      float64
	BidDepth       float64
	AskDepth       float64
	UpdatedAt      time.Time
}

// BoardSnap is board metadata for one primary market.
type BoardSnap struct {
	ConditionID       string
	MarketID          string
	TokenID           string
	Rank              int
	Score             float64
	EdgeBps           *float64
	CostBps           *float64
	CapacityUSD       *float64
	Urgency           *float64
	FairValue         *float64
	ModelEdgeBps      *float64
	FVSource          string
	NegRisk           bool
	NegRiskGroupID    string
	StrategyTags      []string
	RiskFlags         []string
	KeyFeatures       map[string]any
	StrategyVersionID *int64
	RunID             string
}

// GateConfig controls emit rules.
type GateConfig struct {
	MinEdgeBps      float64
	MaxSpreadBps    float64
	MaxBookAge      time.Duration
	Cooldown        time.Duration
	ReissueDeltaBps float64
	ProbeSizeUSD    float64
	CapacityFrac    float64
	MaxNotionalUSD  float64
	ConvictionScale float64 // |net_edge| / scale → conviction 0..1
	HorizonSec      int
	HalfLifeSec     int
	BufferBps       float64
	DefaultFeeBps   float64
}

// DefaultGateConfig returns sensible M6 defaults.
func DefaultGateConfig() GateConfig {
	return GateConfig{
		MinEdgeBps:      0,
		MaxSpreadBps:    500,
		MaxBookAge:      30 * time.Second,
		Cooldown:        60 * time.Second,
		ReissueDeltaBps: 50,
		ProbeSizeUSD:    25,
		CapacityFrac:    0.1,
		MaxNotionalUSD:  500,
		ConvictionScale: 200,
		HorizonSec:      3600,
		HalfLifeSec:     1800,
		BufferBps:       0,
		DefaultFeeBps:   0,
	}
}

// Package signaleval simulates paper fills under the risk manager and builds
// signal_eval_surface for external AO (M8). No live orders.
package signaleval

import (
	"time"

	"github.com/samucap/poly-asian-data/internal/risk"
)

// SignalIn is one paper intent for evaluation.
type SignalIn struct {
	Time              time.Time
	SignalID          string
	Strategy          string
	StrategyVersionID *int64
	ConditionID       string
	TokenID           string
	MarketID          string
	Side              string
	Action            string
	EdgeBps           float64
	Conviction        float64
	Urgency           float64
	SizeUSD           float64
	CapacityUSD       float64
	CostBps           float64
	Mid               float64
	HorizonSec        int
	KellyFrac         *float64
}

// PriceIndex maps token_id → sorted (time, mid) points for exit/MTM.
type PriceIndex map[string][]PricePoint

// PricePoint is one mid observation.
type PricePoint struct {
	Time time.Time
	Mid  float64
}

// Trade is a closed or open paper trade.
type Trade struct {
	SignalID    string
	ConditionID string
	TokenID     string
	Side        string
	SizeUSD     float64
	Shares      float64
	EntryTime   time.Time
	EntryMid    float64
	ExitTime    time.Time
	ExitMid     float64
	CostUSD     float64
	PnLUSD      float64
	Closed      bool
	EdgeBps     float64
	Conviction  float64
}

// Metrics is the AO scorecard.
type Metrics struct {
	NSignals              int                `json:"n_signals"`
	NAccepted             int                `json:"n_accepted"`
	NRejectedRisk         int                `json:"n_rejected_risk"`
	NClosed               int                `json:"n_closed"`
	NOpen                 int                `json:"n_open"`
	HitRate             float64 `json:"hit_rate"`
	TotalPnLUSD         float64 `json:"total_pnl_usd"`
	TotalReturnBps      float64 `json:"total_return_bps"`
	AvgTradePnLUSD      float64 `json:"avg_trade_pnl_usd"`
	// Sharpe is annualized from **hourly** equity returns when n_periods is sufficient.
	// Prefer total_pnl_usd / hit_rate / max_drawdown_bps on small samples.
	Sharpe              float64           `json:"sharpe"`
	SharpeNote          string            `json:"sharpe_note,omitempty"` // insufficient_periods | zero_variance
	MeanPeriodReturn    float64           `json:"mean_period_return,omitempty"`  // hourly, not annualized
	PeriodReturnStdev   float64           `json:"period_return_stdev,omitempty"` // hourly, not annualized
	NPeriods            int               `json:"n_periods,omitempty"`          // hourly steps used for Sharpe
	MaxDrawdownBps      float64           `json:"max_drawdown_bps"`
	MaxDailyDrawdownBps float64           `json:"max_daily_drawdown_bps"`
	Turnover            float64           `json:"turnover"`
	RejectReasons       map[string]int    `json:"reject_reasons"`
	StartingEquityUSD   float64           `json:"starting_equity_usd"`
	EndingEquityUSD     float64           `json:"ending_equity_usd"`
	SelectionQuality    *SelectionQuality `json:"selection_quality,omitempty"`
	PeriodsPerYear      float64           `json:"periods_per_year,omitempty"`
}

// SelectionQuality shows whether opportunity ranking preferred better edges.
type SelectionQuality struct {
	MeanAbsEdgeAccepted  float64 `json:"mean_abs_edge_accepted"`
	MeanAbsEdgeRejected  float64 `json:"mean_abs_edge_rejected_budget"`
	MeanConvictionAccepted float64 `json:"mean_conviction_accepted"`
	MeanConvictionRejected float64 `json:"mean_conviction_rejected_budget"`
	PreferHigherEdge     bool    `json:"prefer_higher_edge"` // accepted mean |edge| >= rejected budget
}

// Result is full simulation output.
type Result struct {
	Metrics     Metrics
	Trades      []Trade
	RiskEvents  []risk.Event
	EquityCurve []EquityPoint
	RiskCfg     risk.Config
	ConfigHash  string
}

// EquityPoint is one mark on the equity path.
type EquityPoint struct {
	Time   time.Time
	Equity float64
}

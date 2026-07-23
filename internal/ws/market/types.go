// Package market implements Polymarket CLOB market-channel WS client and book state (M6).
package market

import "time"

const (
	DefaultWSURL     = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
	DefaultPingEvery = 10 * time.Second
)

// EventType values from the market channel.
const (
	EventBook           = "book"
	EventPriceChange    = "price_change"
	EventBestBidAsk     = "best_bid_ask"
	EventLastTradePrice = "last_trade_price"
	EventTickSizeChange = "tick_size_change"
	EventNewMarket      = "new_market"
	EventMarketResolved = "market_resolved"
)

// Level is one price/size book level.
type Level struct {
	Price float64
	Size  float64
}

// BookState is in-memory L2 + BBA for one asset.
type BookState struct {
	TokenID        string
	MarketID       string
	Bids           map[string]float64 // price string → size (stable keys)
	Asks           map[string]float64
	BestBid        float64
	BestAsk        float64
	LastTradePrice float64
	Imbalance      float64
	BidDepth       float64
	AskDepth       float64
	UpdatedAt      time.Time
	Dirty          bool
	// LastFlushed* for change detection across flushes.
	LastFlushMid float64
	LastFlushBB  float64
	LastFlushBA  float64
	// LastSnapMid for slower snapshot hypertable (independent of Dirty).
	LastSnapMid float64
	LastSnapBB  float64
	LastSnapBA  float64
}

// Mid returns mid if book valid.
func (b *BookState) Mid() float64 {
	if b == nil || b.BestBid <= 0 || b.BestAsk <= 0 || b.BestAsk < b.BestBid {
		return 0
	}
	return (b.BestBid + b.BestAsk) / 2
}

// Spread returns ask-bid.
func (b *BookState) Spread() float64 {
	if b == nil || b.BestBid <= 0 || b.BestAsk <= 0 {
		return 0
	}
	return b.BestAsk - b.BestBid
}

// ValidBook reports usable top-of-book.
func (b *BookState) ValidBook() bool {
	return b != nil && b.BestBid > 0 && b.BestAsk > 0 && b.BestAsk >= b.BestBid
}

// ParsedEvent is a normalized WS event.
type ParsedEvent struct {
	Type      string
	AssetID   string
	MarketID  string // condition id (0x…) for market channel
	Timestamp time.Time
	// book
	Bids []Level
	Asks []Level
	// price_change level
	Price float64
	Size  float64
	Side  string // BUY / SELL
	// bba / trade
	BestBid        float64
	BestAsk        float64
	LastTradePrice float64
	// price_change may carry multiple legs
	PriceChanges []PriceChangeLeg
	// market_resolved
	MarketGammaID   string // gamma market id when present
	WinningAssetID  string
	WinningOutcome  string
	ResolvedAssetIDs []string // tokens to unsubscribe
	Raw             []byte
}

// PriceChangeLeg is one asset update inside price_change.
type PriceChangeLeg struct {
	AssetID string
	Price   float64
	Size    float64
	Side    string
	BestBid float64
	BestAsk float64
}

// SubDiff is subscribe/unsubscribe delta.
type SubDiff struct {
	Subscribe   []string
	Unsubscribe []string
}

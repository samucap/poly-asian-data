package processor

import (
	"math"
	"sort"
	"time"
)

// CalculateCollateralValue computes value as size * price.
// Idempotent: Pure math, no side effects.
func CalculateCollateralValue(size float64, price float64) float64 {
	return size * price
}

// CalculatePositionDelta computes delta_quantity and direction.
// Assumes previous and current are from userPositions response structs.
// Idempotent.
func CalculatePositionDelta(prevNetQuantity, currentNetQuantity float64) (delta float64, direction string) {
	delta = currentNetQuantity - prevNetQuantity
	if delta > 0 {
		direction = "buy"
	} else if delta < 0 {
		direction = "sell"
	} else {
		direction = "no_change"
	}
	return delta, direction
}

// CalculateSpread computes spread from best ask and bid.
// From orderbook response: assumes bids/asks are sorted (highest bid, lowest ask).
// Idempotent.
func CalculateSpread(bestBid, bestAsk float64) float64 {
	if bestBid == 0 || bestAsk == 0 {
		return 0 // Handle invalid cases
	}
	return bestAsk - bestBid
}

// AggregateOHLC simplifies OHLC over trades (e.g., 1min bucket).
// Input: Slice of trades with timestamp and price.
// Output: Open, High, Low, Close for the bucket.
// Idempotent: Sorts and aggregates in-memory.
type Trade struct {
	Timestamp time.Time
	Price     float64
}

// ConvertPlyMktTrades is a helper to convert service structs to local Trade struct if needed
// Or we can use services.PlyMktPriceHistory directly if we are just passing through.
// But AggregateOHLC is usually for raw trades -> candles.
func AggregateOHLC(trades []Trade, bucketStart time.Time) (open, high, low, close float64) {
	if len(trades) == 0 {
		return 0, 0, 0, 0
	}
	// Sort by time (assuming unsorted input)
	sort.Slice(trades, func(i, j int) bool {
		return trades[i].Timestamp.Before(trades[j].Timestamp)
	})
	open = trades[0].Price
	close = trades[len(trades)-1].Price
	high = trades[0].Price
	low = trades[0].Price
	for _, t := range trades {
		high = math.Max(high, t.Price)
		low = math.Min(low, t.Price)
	}
	return open, high, low, close
}

// FlagWhaleSignal: Basic idempotent flagging based on delta and book depth threshold.
// Returns true if signal-worthy.
func FlagWhaleSignal(deltaQuantity float64, bookDepthThreshold float64) bool {
	return math.Abs(deltaQuantity) > 1000 && bookDepthThreshold > 5000 // Example thresholds
}

package market

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

// ParseMessage decodes a market-channel JSON payload into zero or more events.
// Non-JSON (e.g. "PONG") returns nil, nil.
func ParseMessage(data []byte) ([]ParsedEvent, error) {
	s := strings.TrimSpace(string(data))
	if s == "" || s == "PONG" || s == "pong" {
		return nil, nil
	}
	if !json.Valid(data) {
		return nil, nil
	}

	// Some messages are arrays.
	if len(s) > 0 && s[0] == '[' {
		var arr []json.RawMessage
		if err := json.Unmarshal(data, &arr); err != nil {
			return nil, err
		}
		var out []ParsedEvent
		for _, raw := range arr {
			evs, err := parseOne(raw)
			if err != nil {
				continue
			}
			out = append(out, evs...)
		}
		return out, nil
	}
	return parseOne(data)
}

func parseOne(data []byte) ([]ParsedEvent, error) {
	var envelope struct {
		EventType string `json:"event_type"`
		AssetID   string `json:"asset_id"`
		Market    string `json:"market"`
		Timestamp string `json:"timestamp"`
		// book
		Bids []struct {
			Price string `json:"price"`
			Size  string `json:"size"`
		} `json:"bids"`
		Asks []struct {
			Price string `json:"price"`
			Size  string `json:"size"`
		} `json:"asks"`
		// price_change
		PriceChanges []struct {
			AssetID string `json:"asset_id"`
			Price   string `json:"price"`
			Size    string `json:"size"`
			Side    string `json:"side"`
			BestBid string `json:"best_bid"`
			BestAsk string `json:"best_ask"`
		} `json:"price_changes"`
		// best_bid_ask / trade
		BestBid string `json:"best_bid"`
		BestAsk string `json:"best_ask"`
		Price   string `json:"price"`
		Size    string `json:"size"`
		Side    string `json:"side"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, err
	}
	if envelope.EventType == "" {
		return nil, nil
	}
	ts := parseTS(envelope.Timestamp)
	base := ParsedEvent{
		Type:      envelope.EventType,
		AssetID:   envelope.AssetID,
		MarketID:  envelope.Market,
		Timestamp: ts,
		Raw:       append([]byte(nil), data...),
	}

	switch envelope.EventType {
	case EventBook:
		for _, l := range envelope.Bids {
			base.Bids = append(base.Bids, Level{Price: parseF(l.Price), Size: parseF(l.Size)})
		}
		for _, l := range envelope.Asks {
			base.Asks = append(base.Asks, Level{Price: parseF(l.Price), Size: parseF(l.Size)})
		}
		return []ParsedEvent{base}, nil

	case EventPriceChange:
		var legs []PriceChangeLeg
		for _, pc := range envelope.PriceChanges {
			legs = append(legs, PriceChangeLeg{
				AssetID: pc.AssetID,
				Price:   parseF(pc.Price),
				Size:    parseF(pc.Size),
				Side:    strings.ToUpper(pc.Side),
				BestBid: parseF(pc.BestBid),
				BestAsk: parseF(pc.BestAsk),
			})
		}
		if len(legs) == 0 {
			return nil, nil
		}
		base.PriceChanges = legs
		// Also expand to one event per leg for simple apply paths.
		out := make([]ParsedEvent, 0, len(legs)+1)
		out = append(out, base)
		for _, leg := range legs {
			ev := ParsedEvent{
				Type:      EventPriceChange,
				AssetID:   leg.AssetID,
				MarketID:  envelope.Market,
				Timestamp: ts,
				Price:     leg.Price,
				Size:      leg.Size,
				Side:      leg.Side,
				BestBid:   leg.BestBid,
				BestAsk:   leg.BestAsk,
			}
			out = append(out, ev)
		}
		return out, nil

	case EventBestBidAsk:
		base.BestBid = parseF(envelope.BestBid)
		base.BestAsk = parseF(envelope.BestAsk)
		return []ParsedEvent{base}, nil

	case EventLastTradePrice:
		base.LastTradePrice = parseF(envelope.Price)
		base.Price = base.LastTradePrice
		base.Size = parseF(envelope.Size)
		base.Side = strings.ToUpper(envelope.Side)
		return []ParsedEvent{base}, nil

	default:
		// tick_size_change, new_market, market_resolved — ignore for book path
		return []ParsedEvent{base}, nil
	}
}

func parseF(s string) float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return f
}

func parseTS(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Now().UTC()
	}
	// ms or sec unix
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		if n > 1e12 {
			return time.UnixMilli(n).UTC()
		}
		return time.Unix(n, 0).UTC()
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t.UTC()
	}
	return time.Now().UTC()
}

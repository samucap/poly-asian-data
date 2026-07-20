package processor

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"time"

	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/services"
)

type oiResponse struct {
	Market string  `json:"market"`
	Value  float64 `json:"value"`
}

type orderbookLevel struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

type orderbookResponse struct {
	AssetID   string           `json:"asset_id"`
	Bids      []orderbookLevel `json:"bids"`
	Asks      []orderbookLevel `json:"asks"`
	MarketID  string           `json:"market"`
	NegRisk   bool             `json:"neg_risk"`
	Timestamp string           `json:"timestamp"`
}

// TakeMergedOI returns and clears conditionID → open-interest values collected
// when enrichment requests set Metadata["MergeOI"]="true".
func (p *Processor) TakeMergedOI() map[string]float64 {
	p.oiMergeMu.Lock()
	defer p.oiMergeMu.Unlock()
	out := p.oiMerge
	p.oiMerge = make(map[string]float64)
	return out
}

func (p *Processor) recordMergedOI(market string, value float64) {
	if market == "" {
		return
	}
	p.oiMergeMu.Lock()
	p.oiMerge[market] = value
	p.oiMergeMu.Unlock()
}

func (p *Processor) processTopMarketsOI(resp *fetcher.Response) (*Output, error) {
	var items []oiResponse
	if err := json.Unmarshal(resp.Data, &items); err != nil {
		return nil, fmt.Errorf("failed to unmarshal oi response: %w", err)
	}

	mergeOnly := resp.Request != nil && resp.Request.Metadata != nil &&
		resp.Request.Metadata["MergeOI"] == "true"

	var data []services.PlyMktMarketOI
	for _, item := range items {
		if mergeOnly {
			p.recordMergedOI(item.Market, item.Value)
		}
		data = append(data, services.PlyMktMarketOI{
			Market: item.Market,
			Value:  item.Value,
		})
	}

	out := &Output{
		ItemCount:       len(data),
		OriginalRequest: resp.Request,
	}
	// Merge-only path: OI is applied to in-memory markets before a single upsert.
	if !mergeOnly && len(data) > 0 {
		out.SaverPayloads = []SaverPayload{
			{TableName: "plymkt_markets_oi", Data: data},
		}
	}
	return out, nil
}

func (p *Processor) processTopMarketsTrades(resp *fetcher.Response) (*Output, error) {
	var items []services.PlyMktTrade
	if err := json.Unmarshal(resp.Data, &items); err != nil {
		var ptrItems []*services.PlyMktTrade
		if err2 := json.Unmarshal(resp.Data, &ptrItems); err2 == nil {
			for _, item := range ptrItems {
				if item != nil {
					items = append(items, *item)
				}
			}
		} else {
			return nil, fmt.Errorf("failed to unmarshal trades response: %w", err)
		}
	}

	return &Output{
		SaverPayloads: []SaverPayload{
			{TableName: "trades", Data: items},
		},
		ItemCount:       len(items),
		OriginalRequest: resp.Request,
	}, nil
}

func (p *Processor) processTopMarketsPrices(resp *fetcher.Response) (*Output, error) {
	// Batch POST /batch-prices-history:
	//   { "history": { "<clobTokenId>": [ {"t": int, "p": number}, ... ] } }
	// Single GET /prices-history:
	//   { "history": [ {"t", "p"} ] } or raw array (legacy)

	type histPoint struct {
		T int64   `json:"t"`
		P float64 `json:"p"`
		// Legacy field names
		Timestamp int64   `json:"timestamp"`
		Price     float64 `json:"price"`
	}

	now := time.Now().Unix()
	fidelity := 5
	if resp.Request != nil && resp.Request.Metadata != nil {
		if f, err := strconv.Atoi(resp.Request.Metadata["Fidelity"]); err == nil && f > 0 {
			fidelity = f
		}
	}

	tokenToMarket := map[string]string{}
	if resp.Request != nil && resp.Request.Metadata != nil {
		if raw := resp.Request.Metadata["TokenMarketMap"]; raw != "" {
			_ = json.Unmarshal([]byte(raw), &tokenToMarket)
		}
		if tid := resp.Request.Metadata["TokenID"]; tid != "" {
			mid := resp.Request.Metadata["MarketID"]
			tokenToMarket[tid] = mid
		}
	}

	var items []services.PlyMktPriceHistory

	// 1) Batch map form
	var batch struct {
		History map[string][]histPoint `json:"history"`
	}
	if err := json.Unmarshal(resp.Data, &batch); err == nil && batch.History != nil {
		for tokenID, points := range batch.History {
			marketID := tokenToMarket[tokenID]
			for _, pt := range points {
				ts, price := pt.T, pt.P
				if ts == 0 {
					ts = pt.Timestamp
				}
				if price == 0 {
					price = pt.Price
				}
				items = append(items, services.PlyMktPriceHistory{
					TokenID:   tokenID,
					Timestamp: ts,
					Price:     price,
					MarketID:  marketID,
					Fidelity:  fidelity,
					UpdatedAt: now,
				})
			}
		}
	} else {
		// 2) Object with history array
		var obj struct {
			History []histPoint `json:"history"`
		}
		var points []histPoint
		if err := json.Unmarshal(resp.Data, &obj); err == nil && obj.History != nil {
			points = obj.History
		} else if err := json.Unmarshal(resp.Data, &points); err != nil {
			// Error payload from CLOB
			var errResp struct {
				Error string `json:"error"`
			}
			if json.Unmarshal(resp.Data, &errResp) == nil && errResp.Error != "" {
				p.logger.Warn("CLOB price history API error",
					slog.String("url", resp.URL),
					slog.String("error", errResp.Error),
				)
				return &Output{ItemCount: 0, OriginalRequest: resp.Request}, nil
			}
			if len(resp.Data) > 0 {
				str := string(resp.Data)
				if str != "[]" && str != "{}" && str != `{"history":[]}` {
					return nil, fmt.Errorf("failed to unmarshal prices history: %w", err)
				}
			}
		}

		tokenID := ""
		marketID := ""
		if resp.Request != nil && resp.Request.Metadata != nil {
			tokenID = resp.Request.Metadata["TokenID"]
			marketID = resp.Request.Metadata["MarketID"]
		}
		if tokenID == "" {
			tokenID = marketID
		}
		for _, pt := range points {
			ts, price := pt.T, pt.P
			if ts == 0 {
				ts = pt.Timestamp
			}
			if price == 0 {
				price = pt.Price
			}
			items = append(items, services.PlyMktPriceHistory{
				TokenID:   tokenID,
				Timestamp: ts,
				Price:     price,
				MarketID:  marketID,
				Fidelity:  fidelity,
				UpdatedAt: now,
			})
		}
	}

	if len(items) == 0 {
		return &Output{ItemCount: 0, OriginalRequest: resp.Request}, nil
	}
	return &Output{
		SaverPayloads: []SaverPayload{
			{TableName: "prices_history", Data: items},
		},
		ItemCount:       len(items),
		OriginalRequest: resp.Request,
	}, nil
}

func (p *Processor) processTopMarketsOrderbooks(resp *fetcher.Response) (*Output, error) {
	var books []orderbookResponse

	if len(resp.Data) > 0 && resp.Data[0] == '[' {
		if err := json.Unmarshal(resp.Data, &books); err != nil {
			return nil, fmt.Errorf("failed to unmarshal batch orderbooks: %w", err)
		}
	} else {
		var book orderbookResponse
		if err := json.Unmarshal(resp.Data, &book); err != nil {
			return nil, fmt.Errorf("failed to unmarshal single orderbook: %w", err)
		}
		books = append(books, book)
	}

	now := time.Now()
	var snapshots []services.PlyMktOrderbookSnapshot
	for _, book := range books {
		tokenID := book.AssetID
		if tokenID == "" && resp.Request.Metadata != nil {
			tokenID = resp.Request.Metadata["TokenID"]
		}

		snap := services.PlyMktOrderbookSnapshot{
			Time:      now,
			TokenID:   tokenID,
			MarketID:  book.MarketID,
			NegRisk:   book.NegRisk,
			Timestamp: book.Timestamp,
		}

		for _, bid := range book.Bids {
			price, _ := strconv.ParseFloat(bid.Price, 64)
			size, _ := strconv.ParseFloat(bid.Size, 64)
			snap.TotalBidDepth += size
			if price > snap.BestBid {
				snap.BestBid = price
			}
		}

		snap.BestAsk = math.MaxFloat64
		for _, ask := range book.Asks {
			price, _ := strconv.ParseFloat(ask.Price, 64)
			size, _ := strconv.ParseFloat(ask.Size, 64)
			snap.TotalAskDepth += size
			if price < snap.BestAsk {
				snap.BestAsk = price
			}
		}
		if len(book.Asks) == 0 {
			snap.BestAsk = 0
		}

		totalDepth := snap.TotalBidDepth + snap.TotalAskDepth
		if totalDepth > 0 {
			snap.Imbalance = snap.TotalBidDepth / totalDepth
		}

		depthData := map[string]any{
			"bids": book.Bids,
			"asks": book.Asks,
		}
		snap.DepthJSON, _ = json.Marshal(depthData)
		snap.RawJSON, _ = json.Marshal(book)

		snapshots = append(snapshots, snap)
	}

	return &Output{
		SaverPayloads: []SaverPayload{
			{TableName: "orderbook_snapshots", Data: snapshots},
		},
		ItemCount:       len(snapshots),
		OriginalRequest: resp.Request,
	}, nil
}

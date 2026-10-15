// Package enrich fetches Stage-1 market microstructure (books, OI) for M3 edge-scan.
package enrich

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/services"
)

// BookSnapshot is a normalized top-of-book (+ depth totals) for one token.
type BookSnapshot struct {
	Time          time.Time
	MarketID      string
	TokenID       string
	BestBid       float64
	BestAsk       float64
	Imbalance     float64
	TotalBidDepth float64
	TotalAskDepth float64
	DepthJSON     []byte
	RawJSON       []byte
	NegRisk       bool
}

// TokenRef links a CLOB token to its market/condition for enrichment.
type TokenRef struct {
	TokenID     string
	MarketID    string
	ConditionID string
}

type tokenReq struct {
	TokenID string `json:"token_id"`
}

type bookLevel struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

type bookResponse struct {
	MarketID  string      `json:"market"`
	AssetID   string      `json:"asset_id"`
	Bids      []bookLevel `json:"bids"`
	Asks      []bookLevel `json:"asks"`
	NegRisk   bool        `json:"neg_risk"`
	Timestamp string      `json:"timestamp"`
}

// FetchBooks POSTs to CLOB /books in batches and returns snapshots.
func FetchBooks(ctx context.Context, client *http.Client, tokenIDs []string, batchSize int) ([]BookSnapshot, error) {
	if client == nil {
		client = http.DefaultClient
	}
	if batchSize <= 0 {
		batchSize = 50
	}
	clob, _ := config.DefaultEndpoints["clob"].(string)
	if clob == "" {
		clob = "https://clob.polymarket.com"
	}
	now := time.Now().UTC()
	var all []BookSnapshot
	for i := 0; i < len(tokenIDs); i += batchSize {
		end := i + batchSize
		if end > len(tokenIDs) {
			end = len(tokenIDs)
		}
		chunk := tokenIDs[i:end]
		var payload []tokenReq
		for _, tid := range chunk {
			if tid == "" {
				continue
			}
			payload = append(payload, tokenReq{TokenID: tid})
		}
		if len(payload) == 0 {
			continue
		}
		body, err := json.Marshal(payload)
		if err != nil {
			return all, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(clob, "/")+"/books", bytes.NewReader(body))
		if err != nil {
			return all, err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return all, fmt.Errorf("enrich books: %w", err)
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return all, err
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return all, fmt.Errorf("enrich books: status %d: %s", resp.StatusCode, truncate(string(data), 200))
		}
		snaps, err := parseBooks(data, now)
		if err != nil {
			return all, err
		}
		all = append(all, snaps...)
	}
	return all, nil
}

func parseBooks(data []byte, now time.Time) ([]BookSnapshot, error) {
	var books []bookResponse
	if len(data) > 0 && data[0] == '[' {
		if err := json.Unmarshal(data, &books); err != nil {
			return nil, fmt.Errorf("enrich books decode batch: %w", err)
		}
	} else {
		var one bookResponse
		if err := json.Unmarshal(data, &one); err != nil {
			return nil, fmt.Errorf("enrich books decode one: %w", err)
		}
		books = append(books, one)
	}
	out := make([]BookSnapshot, 0, len(books))
	for _, book := range books {
		snap := BookSnapshot{
			Time:     now,
			TokenID:  book.AssetID,
			MarketID: book.MarketID,
			NegRisk:  book.NegRisk,
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
		if len(book.Asks) == 0 || snap.BestAsk == math.MaxFloat64 {
			snap.BestAsk = 0
		}
		total := snap.TotalBidDepth + snap.TotalAskDepth
		if total > 0 {
			snap.Imbalance = snap.TotalBidDepth / total
		}
		depthData := map[string]any{"bids": book.Bids, "asks": book.Asks}
		snap.DepthJSON, _ = json.Marshal(depthData)
		snap.RawJSON, _ = json.Marshal(book)
		out = append(out, snap)
	}
	return out, nil
}

// ToServiceSnapshots converts for saver/db paths that use services.PlyMktOrderbookSnapshot.
func ToServiceSnapshots(snaps []BookSnapshot) []services.PlyMktOrderbookSnapshot {
	out := make([]services.PlyMktOrderbookSnapshot, 0, len(snaps))
	for _, s := range snaps {
		out = append(out, services.PlyMktOrderbookSnapshot{
			Time:          s.Time,
			MarketID:      s.MarketID,
			TokenID:       s.TokenID,
			BestBid:       s.BestBid,
			BestAsk:       s.BestAsk,
			Imbalance:     s.Imbalance,
			TotalBidDepth: s.TotalBidDepth,
			TotalAskDepth: s.TotalAskDepth,
			DepthJSON:     s.DepthJSON,
			RawJSON:       s.RawJSON,
			NegRisk:       s.NegRisk,
		})
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

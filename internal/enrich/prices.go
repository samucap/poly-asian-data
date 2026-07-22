package enrich

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/db"
)

// Default price backfill parameters (M4).
const (
	DefaultPriceBatchSize   = 20 // CLOB /batch-prices-history limit
	DefaultPriceTokenCap    = 300
	DefaultPriceFidelityMin = 60
	DefaultPriceLookback    = 30 * 24 * time.Hour
	DefaultPriceWarmMaxAge  = 30 * time.Minute // skip incremental if last point fresher
	DefaultPriceOverlap     = time.Hour         // re-fetch overlap before HWM

	// MaxStartEndSpan is the longest start_ts↔end_ts window CLOB accepts when both
	// bounds are set (~15d; 16d returns 400 "interval is too long"). Prefer start_ts
	// only + interval when you need a longer cold lookback.
	MaxStartEndSpan = 14 * 24 * time.Hour
)

// PriceHistPoint is one API history sample.
type PriceHistPoint struct {
	T int64   `json:"t"`
	P float64 `json:"p"`
}

// PriceToken is a token selected for price backfill.
type PriceToken struct {
	TokenID     string
	MarketID    string
	ConditionID string
}

// SelectPriceTokens unions board tokens with top-volume fill-up to cap.
func SelectPriceTokens(board, topVolume []db.EvalMarketMeta, cap int) []PriceToken {
	if cap <= 0 {
		cap = DefaultPriceTokenCap
	}
	seen := map[string]bool{}
	var out []PriceToken
	add := func(m db.EvalMarketMeta) {
		if m.TokenID == "" || seen[m.TokenID] {
			return
		}
		if len(out) >= cap {
			return
		}
		seen[m.TokenID] = true
		out = append(out, PriceToken{
			TokenID:     m.TokenID,
			MarketID:    m.MarketID,
			ConditionID: m.ConditionID,
		})
	}
	for _, m := range board {
		add(m)
	}
	for _, m := range topVolume {
		add(m)
	}
	return out
}

// BatchPricesParams is a single /batch-prices-history request shape.
//
// CLOB rules (probed 2026-07):
//   - Provide either interval OR (start_ts [+ end_ts]), not camelCase alone.
//   - If both start_ts and end_ts are set, span must be ≤ ~15d.
//   - For multi-week cold backfill: start_ts + interval="max" (omit end_ts).
//   - OpenAPI field names are snake_case: start_ts, end_ts.
type BatchPricesParams struct {
	StartTS  int64  // unix seconds; 0 = omit
	EndTS    int64  // unix seconds; 0 = omit (preferred for long lookbacks)
	Interval string // max|all|1m|1w|1d|6h|1h; empty = omit
	Fidelity int    // minutes
}

// FetchBatchPricesHistory POSTs to CLOB /batch-prices-history (max 20 markets/req).
func FetchBatchPricesHistory(
	ctx context.Context,
	client *http.Client,
	tokenIDs []string,
	params BatchPricesParams,
	batchSize int,
) (map[string][]PriceHistPoint, error) {
	if client == nil {
		client = http.DefaultClient
	}
	if batchSize <= 0 || batchSize > DefaultPriceBatchSize {
		batchSize = DefaultPriceBatchSize
	}
	if params.Fidelity <= 0 {
		params.Fidelity = DefaultPriceFidelityMin
	}
	// Guard: both bounds set and span too long → clamp end or drop end_ts.
	params = normalizeBatchParams(params)

	clob, _ := config.DefaultEndpoints["clob"].(string)
	if clob == "" {
		clob = "https://clob.polymarket.com"
	}
	url := strings.TrimRight(clob, "/") + "/batch-prices-history"
	out := map[string][]PriceHistPoint{}

	for i := 0; i < len(tokenIDs); i += batchSize {
		end := i + batchSize
		if end > len(tokenIDs) {
			end = len(tokenIDs)
		}
		chunk := tokenIDs[i:end]
		var markets []string
		for _, t := range chunk {
			if t != "" {
				markets = append(markets, t)
			}
		}
		if len(markets) == 0 {
			continue
		}
		bodyMap := map[string]any{
			"markets":  markets,
			"fidelity": params.Fidelity,
		}
		if params.StartTS > 0 {
			bodyMap["start_ts"] = params.StartTS
		}
		if params.EndTS > 0 {
			bodyMap["end_ts"] = params.EndTS
		}
		if params.Interval != "" {
			bodyMap["interval"] = params.Interval
		}
		// CLOB requires a time component: interval and/or start_ts.
		if params.StartTS <= 0 && params.Interval == "" {
			bodyMap["interval"] = "max"
		}
		payload, err := json.Marshal(bodyMap)
		if err != nil {
			return out, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
		if err != nil {
			return out, err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return out, fmt.Errorf("enrich prices batch: %w", err)
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return out, err
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return out, fmt.Errorf("enrich prices batch: status %d: %s", resp.StatusCode, truncate(string(data), 200))
		}
		part, err := parseBatchPrices(data)
		if err != nil {
			return out, err
		}
		for tok, pts := range part {
			out[tok] = append(out[tok], pts...)
		}
		// small pause between chunks to be nice to the API
		if end < len(tokenIDs) {
			select {
			case <-ctx.Done():
				return out, ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	return out, nil
}

// normalizeBatchParams fixes known CLOB 400 shapes.
// When both start and end are set beyond MaxStartEndSpan, drop end_ts and ensure interval.
func normalizeBatchParams(p BatchPricesParams) BatchPricesParams {
	if p.StartTS > 0 && p.EndTS > 0 && p.EndTS > p.StartTS {
		span := time.Duration(p.EndTS-p.StartTS) * time.Second
		if span > MaxStartEndSpan {
			// Long window: start_ts + interval (no end_ts) — same as top-markets / playground.
			p.EndTS = 0
			if p.Interval == "" {
				p.Interval = "max"
			}
		}
	}
	return p
}

func parseBatchPrices(data []byte) (map[string][]PriceHistPoint, error) {
	out := map[string][]PriceHistPoint{}
	// Batch form: { "history": { "<token>": [ {t,p}, ... ] } }
	var batch struct {
		History map[string][]PriceHistPoint `json:"history"`
	}
	if err := json.Unmarshal(data, &batch); err == nil && batch.History != nil {
		for k, v := range batch.History {
			out[k] = v
		}
		return out, nil
	}
	// Alternate: { "history": [ ... ] } without token map — cannot attribute
	return out, fmt.Errorf("enrich prices: unexpected batch payload")
}

// EnsurePricesConfig controls cold vs warm price fetches.
type EnsurePricesConfig struct {
	Lookback    time.Duration
	FidelityMin int
	BatchSize   int
	WarmMaxAge  time.Duration
	// Interval for cold lookback when using start_ts without end_ts (default "max").
	// CLOB enum: max|all|1m|1w|1d|6h|1h — "1m" means ~1 month, not 1 minute.
	Interval   string
	ForceCold  bool
	Logger     *slog.Logger
	TokenMarket map[string]string
}

// EnsurePricesResult summarizes a warm-stage run.
type EnsurePricesResult struct {
	TokensRequested int
	TokensWritten   int
	PointsWritten   int
	ColdTokens      int
	WarmTokens      int
	SkippedFresh    int
	Errors          []string
}

// EnsurePrices fetches cold lookback or incremental prices for the token set and upserts DB.
// Never panics; partial failures accumulate in result.Errors.
//
// Cold path: start_ts = now−lookback, interval=max, **no end_ts** (avoids 15d start/end cap).
//
// Warm path (incremental, payload-optimal):
//  1. Skip tokens fresher than WarmMaxAge.
//  2. Sort remaining by per-token HWM (MAX(prices_history.timestamp)) ascending.
//  3. POST ≤20 markets per request; each request uses
//     start_ts = min(HWM in that batch) − overlap (default 1h), floor at cold lookback.
//     So a straggler only pulls back its own batch, not all warm tokens.
func EnsurePrices(
	ctx context.Context,
	conn db.DBInterface,
	client *http.Client,
	tokens []PriceToken,
	cfg EnsurePricesConfig,
) EnsurePricesResult {
	res := EnsurePricesResult{TokensRequested: len(tokens)}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	if len(tokens) == 0 || conn == nil {
		return res
	}
	lookback := cfg.Lookback
	if lookback <= 0 {
		lookback = DefaultPriceLookback
	}
	fid := cfg.FidelityMin
	if fid <= 0 {
		fid = DefaultPriceFidelityMin
	}
	batch := cfg.BatchSize
	if batch <= 0 {
		batch = DefaultPriceBatchSize
	}
	warmAge := cfg.WarmMaxAge
	if warmAge <= 0 {
		warmAge = DefaultPriceWarmMaxAge
	}
	overlapSec := int64(DefaultPriceOverlap.Seconds())
	now := time.Now().UTC()

	tokenIDs := make([]string, 0, len(tokens))
	tokMeta := map[string]PriceToken{}
	for _, t := range tokens {
		if t.TokenID == "" {
			continue
		}
		tokenIDs = append(tokenIDs, t.TokenID)
		tokMeta[t.TokenID] = t
	}
	if len(tokenIDs) == 0 {
		return res
	}

	maxTS, err := db.LoadMaxPriceTimestamp(ctx, conn, tokenIDs)
	if err != nil {
		res.Errors = append(res.Errors, "load max price ts: "+err.Error())
		maxTS = map[string]int64{}
	}

	// Partition cold vs warm
	var cold, warm []string
	for _, tid := range tokenIDs {
		last, ok := maxTS[tid]
		if !ok || last == 0 || cfg.ForceCold {
			cold = append(cold, tid)
			continue
		}
		lastT := db.NormalizePriceTS(last)
		if now.Sub(lastT) < warmAge {
			res.SkippedFresh++
			continue
		}
		warm = append(warm, tid)
	}
	res.ColdTokens = len(cold)
	res.WarmTokens = len(warm)

	fetchAndStore := func(ids []string, params BatchPricesParams) {
		if len(ids) == 0 {
			return
		}
		params.Fidelity = fid
		// Caller already chunks to ≤batch; pass batch so API never exceeds 20.
		hist, err := FetchBatchPricesHistory(ctx, client, ids, params, batch)
		if err != nil {
			res.Errors = append(res.Errors, err.Error())
			log.Warn("ensure prices fetch failed", "error", err, "tokens", len(ids),
				"start_ts", params.StartTS, "end_ts", params.EndTS, "interval", params.Interval)
			return
		}
		var rows []db.PriceRow
		updated := map[string]bool{}
		nowU := now.Unix()
		for tok, pts := range hist {
			meta := tokMeta[tok]
			mid := meta.MarketID
			if mid == "" && cfg.TokenMarket != nil {
				mid = cfg.TokenMarket[tok]
			}
			for _, pt := range pts {
				ts := pt.T
				if ts <= 0 {
					continue
				}
				if ts > 1_000_000_000_000 {
					ts = ts / 1000
				}
				rows = append(rows, db.PriceRow{
					TokenID:       tok,
					TimestampUnix: ts,
					Price:         pt.P,
					MarketID:      mid,
					Fidelity:      fid,
					UpdatedAt:     nowU,
				})
				updated[tok] = true
			}
		}
		if len(rows) == 0 {
			return
		}
		if err := db.InsertPricesHistory(ctx, conn, rows); err != nil {
			res.Errors = append(res.Errors, "insert prices: "+err.Error())
			log.Warn("ensure prices insert failed", "error", err, "rows", len(rows))
			return
		}
		res.PointsWritten += len(rows)
		res.TokensWritten += len(updated)
	}

	// Cold: start_ts + interval, no end_ts (matches top-markets; avoids 15d start/end cap).
	interval := cfg.Interval
	if interval == "" {
		interval = "max"
	}
	coldStart := now.Add(-lookback).Unix()
	for _, chunk := range chunkTokenIDs(cold, batch) {
		fetchAndStore(chunk, BatchPricesParams{
			StartTS:  coldStart,
			Interval: interval,
		})
	}

	// Warm: per-batch earliest HWM − overlap (not global min across all warm tokens).
	if len(warm) > 0 {
		warmSorted := sortTokenIDsByHWMAsc(warm, maxTS)
		for _, chunk := range chunkTokenIDs(warmSorted, batch) {
			start := earliestStartTS(chunk, maxTS, overlapSec, coldStart)
			log.Debug("warm price batch",
				"n", len(chunk),
				"start_ts", start,
				"earliest_hwm", earliestHWM(chunk, maxTS),
			)
			fetchAndStore(chunk, BatchPricesParams{
				StartTS: start,
				// no end_ts; start_ts alone is enough for incremental
			})
		}
	}

	log.Info("ensure prices complete",
		"tokens", len(tokenIDs),
		"cold", res.ColdTokens,
		"warm", res.WarmTokens,
		"skipped_fresh", res.SkippedFresh,
		"points", res.PointsWritten,
		"errors", len(res.Errors),
	)
	return res
}

// priceUnixSeconds normalizes prices_history BIGINT to unix seconds.
func priceUnixSeconds(raw int64) int64 {
	if raw <= 0 {
		return 0
	}
	if raw >= 1_000_000_000_000 {
		return raw / 1000
	}
	return raw
}

// earliestHWM returns the minimum per-token high-water mark (unix seconds) in ids.
// Returns 0 if none have a HWM.
func earliestHWM(ids []string, maxTS map[string]int64) int64 {
	var minLast int64
	for _, tid := range ids {
		last := priceUnixSeconds(maxTS[tid])
		if last <= 0 {
			continue
		}
		if minLast == 0 || last < minLast {
			minLast = last
		}
	}
	return minLast
}

// earliestStartTS is min(HWM in batch) − overlapSec, floored at floor (e.g. cold lookback start).
// Used as CLOB start_ts for one ≤20-token batch.
func earliestStartTS(ids []string, maxTS map[string]int64, overlapSec, floor int64) int64 {
	minLast := earliestHWM(ids, maxTS)
	if minLast <= 0 {
		if floor > 0 {
			return floor
		}
		return 0
	}
	if overlapSec < 0 {
		overlapSec = 0
	}
	start := minLast - overlapSec
	if floor > 0 && start < floor {
		start = floor
	}
	return start
}

// sortTokenIDsByHWMAsc orders tokens oldest-HWM first so batches share similar start_ts.
func sortTokenIDsByHWMAsc(ids []string, maxTS map[string]int64) []string {
	out := append([]string(nil), ids...)
	sort.SliceStable(out, func(i, j int) bool {
		ai := priceUnixSeconds(maxTS[out[i]])
		aj := priceUnixSeconds(maxTS[out[j]])
		if ai == aj {
			return out[i] < out[j]
		}
		// Treat missing HWM as oldest so they cluster with cold-like starts.
		if ai == 0 {
			return true
		}
		if aj == 0 {
			return false
		}
		return ai < aj
	})
	return out
}

// chunkTokenIDs splits ids into slices of at most size (default 20).
func chunkTokenIDs(ids []string, size int) [][]string {
	if len(ids) == 0 {
		return nil
	}
	if size <= 0 {
		size = DefaultPriceBatchSize
	}
	var chunks [][]string
	for i := 0; i < len(ids); i += size {
		end := i + size
		if end > len(ids) {
			end = len(ids)
		}
		chunks = append(chunks, ids[i:end])
	}
	return chunks
}

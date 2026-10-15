package db

import (
	"context"
	"fmt"
	"time"
)

// PricePoint is one prices_history sample.
type PricePoint struct {
	TokenID   string
	Timestamp time.Time // normalized UTC
	Price     float64
	MarketID  string
	Fidelity  int
	// RawTS is the BIGINT stored value (seconds or ms as written).
	RawTS int64
}

// PriceRow is a write row for prices_history.
type PriceRow struct {
	TokenID string
	// TimestampUnix is stored as BIGINT (prefer Unix seconds; see NormalizePriceTS).
	TimestampUnix int64
	Price         float64
	MarketID      string
	Fidelity      int
	UpdatedAt     int64
}

// NormalizePriceTS converts a prices_history BIGINT into time.Time.
// Values ≥ 1e12 are treated as milliseconds; otherwise seconds.
func NormalizePriceTS(ts int64) time.Time {
	if ts <= 0 {
		return time.Time{}
	}
	if ts >= 1_000_000_000_000 {
		return time.UnixMilli(ts).UTC()
	}
	return time.Unix(ts, 0).UTC()
}

// PriceTSUnix returns Unix seconds for storage/query helpers.
func PriceTSUnix(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UTC().Unix()
}

// InsertPricesHistory upserts price points (batched).
func InsertPricesHistory(ctx context.Context, conn DBInterface, rows []PriceRow) error {
	if conn == nil {
		return ErrNilDB
	}
	if len(rows) == 0 {
		return nil
	}
	now := time.Now().Unix()
	args := make([][]any, 0, len(rows))
	for _, r := range rows {
		if r.TokenID == "" || r.TimestampUnix <= 0 {
			continue
		}
		upd := r.UpdatedAt
		if upd <= 0 {
			upd = now
		}
		args = append(args, []any{r.TokenID, r.TimestampUnix, r.Price, r.MarketID, r.Fidelity, upd})
	}
	if len(args) == 0 {
		return nil
	}
	const sql = `INSERT INTO prices_history (token_id, timestamp, price, market_id, fidelity_min, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (token_id, timestamp) DO UPDATE SET
			price = EXCLUDED.price,
			market_id = COALESCE(EXCLUDED.market_id, prices_history.market_id),
			fidelity_min = COALESCE(EXCLUDED.fidelity_min, prices_history.fidelity_min),
			updated_at = EXCLUDED.updated_at`
	if err := BatchExec(ctx, conn, sql, args); err != nil {
		return fmt.Errorf("db: insert prices_history: %w", err)
	}
	return nil
}

// LoadPriceAsOf returns the last price at or before asOf for each token.
// Handles both second-scale and millisecond-scale BIGINT timestamps.
func LoadPriceAsOf(ctx context.Context, conn DBInterface, tokenIDs []string, asOf time.Time) (map[string]PricePoint, error) {
	out := map[string]PricePoint{}
	if conn == nil {
		return out, ErrNilDB
	}
	if len(tokenIDs) == 0 || asOf.IsZero() {
		return out, nil
	}
	asOfSec := asOf.UTC().Unix()
	asOfMs := asOf.UTC().UnixMilli()

	// Second-scale rows: timestamp < 1e12; ms-scale: timestamp >= 1e12.
	rows, err := conn.Query(ctx, `
		SELECT DISTINCT ON (token_id)
			token_id, timestamp, price, COALESCE(market_id,''), COALESCE(fidelity_min,0)
		FROM prices_history
		WHERE token_id = ANY($1)
		  AND (
		    (timestamp < 1000000000000 AND timestamp <= $2) OR
		    (timestamp >= 1000000000000 AND timestamp <= $3)
		  )
		ORDER BY token_id, timestamp DESC
	`, tokenIDs, asOfSec, asOfMs)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var tok string
		var rawTS int64
		var price float64
		var marketID string
		var fid int
		if err := rows.Scan(&tok, &rawTS, &price, &marketID, &fid); err != nil {
			return out, err
		}
		ts := NormalizePriceTS(rawTS)
		if ts.After(asOf.Add(time.Minute)) {
			continue
		}
		out[tok] = PricePoint{
			TokenID: tok, Timestamp: ts, Price: price, MarketID: marketID, Fidelity: fid, RawTS: rawTS,
		}
	}
	return out, rows.Err()
}

// LoadMaxPriceTimestamp returns max timestamp (raw BIGINT) per token.
func LoadMaxPriceTimestamp(ctx context.Context, conn DBInterface, tokenIDs []string) (map[string]int64, error) {
	out := map[string]int64{}
	if conn == nil {
		return out, ErrNilDB
	}
	if len(tokenIDs) == 0 {
		return out, nil
	}
	rows, err := conn.Query(ctx, `
		SELECT token_id, MAX(timestamp) FROM prices_history
		WHERE token_id = ANY($1)
		GROUP BY token_id
	`, tokenIDs)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var tok string
		var ts int64
		if err := rows.Scan(&tok, &ts); err != nil {
			return out, err
		}
		out[tok] = ts
	}
	return out, rows.Err()
}

// LoadPriceSeries loads all points for tokens in [from, to] (inclusive), ordered by time.
func LoadPriceSeries(ctx context.Context, conn DBInterface, tokenIDs []string, from, to time.Time) (map[string][]PricePoint, error) {
	out := map[string][]PricePoint{}
	if conn == nil {
		return out, ErrNilDB
	}
	if len(tokenIDs) == 0 {
		return out, nil
	}
	fromSec, toSec := from.UTC().Unix(), to.UTC().Unix()
	fromMs, toMs := from.UTC().UnixMilli(), to.UTC().UnixMilli()
	rows, err := conn.Query(ctx, `
		SELECT token_id, timestamp, price, COALESCE(market_id,''), COALESCE(fidelity_min,0)
		FROM prices_history
		WHERE token_id = ANY($1)
		  AND (
		    (timestamp < 1000000000000 AND timestamp >= $2 AND timestamp <= $3) OR
		    (timestamp >= 1000000000000 AND timestamp >= $4 AND timestamp <= $5)
		  )
		ORDER BY token_id, timestamp ASC
	`, tokenIDs, fromSec, toSec, fromMs, toMs)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var tok string
		var rawTS int64
		var price float64
		var marketID string
		var fid int
		if err := rows.Scan(&tok, &rawTS, &price, &marketID, &fid); err != nil {
			return out, err
		}
		ts := NormalizePriceTS(rawTS)
		if ts.Before(from.Add(-time.Minute)) || ts.After(to.Add(time.Minute)) {
			continue
		}
		out[tok] = append(out[tok], PricePoint{
			TokenID: tok, Timestamp: ts, Price: price, MarketID: marketID, Fidelity: fid, RawTS: rawTS,
		})
	}
	return out, rows.Err()
}

// BookAsOf is orderbook top-of-book as-of a decision time.
type BookAsOf struct {
	TokenID       string
	Time          time.Time
	BestBid       float64
	BestAsk       float64
	TotalBidDepth float64
	TotalAskDepth float64
}

// LoadBookAsOf returns latest book snapshot at or before asOf per token (within maxAge).
// maxAge 0 → no age filter beyond asOf.
// Prefer LoadBookSeries for multi-T eval (one query).
func LoadBookAsOf(ctx context.Context, conn DBInterface, tokenIDs []string, asOf time.Time, maxAge time.Duration) (map[string]BookAsOf, error) {
	out := map[string]BookAsOf{}
	if conn == nil {
		return out, ErrNilDB
	}
	if len(tokenIDs) == 0 || asOf.IsZero() {
		return out, nil
	}
	rows, err := conn.Query(ctx, `
		SELECT DISTINCT ON (token_id)
			token_id, time,
			COALESCE(best_bid,0), COALESCE(best_ask,0),
			COALESCE(total_bid_depth,0), COALESCE(total_ask_depth,0)
		FROM orderbook_snapshots
		WHERE token_id = ANY($1) AND time <= $2
		ORDER BY token_id, time DESC
	`, tokenIDs, asOf.UTC())
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var b BookAsOf
		if err := rows.Scan(&b.TokenID, &b.Time, &b.BestBid, &b.BestAsk, &b.TotalBidDepth, &b.TotalAskDepth); err != nil {
			return out, err
		}
		if maxAge > 0 && asOf.Sub(b.Time) > maxAge {
			continue
		}
		out[b.TokenID] = b
	}
	return out, rows.Err()
}

// LoadBookSeries loads all book snapshots for tokens in [from, to], ordered by time.
// Eval builds as-of in memory (avoids one SQL per decision time).
func LoadBookSeries(ctx context.Context, conn DBInterface, tokenIDs []string, from, to time.Time) (map[string][]BookAsOf, error) {
	out := map[string][]BookAsOf{}
	if conn == nil {
		return out, ErrNilDB
	}
	if len(tokenIDs) == 0 {
		return out, nil
	}
	rows, err := conn.Query(ctx, `
		SELECT token_id, time,
			COALESCE(best_bid,0), COALESCE(best_ask,0),
			COALESCE(total_bid_depth,0), COALESCE(total_ask_depth,0)
		FROM orderbook_snapshots
		WHERE token_id = ANY($1)
		  AND time >= $2 AND time <= $3
		ORDER BY token_id, time ASC
	`, tokenIDs, from.UTC(), to.UTC())
	if err != nil {
		// Table may be missing on fresh DBs — return empty, not fatal for prices-only eval.
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var b BookAsOf
		if err := rows.Scan(&b.TokenID, &b.Time, &b.BestBid, &b.BestAsk, &b.TotalBidDepth, &b.TotalAskDepth); err != nil {
			return out, err
		}
		out[b.TokenID] = append(out[b.TokenID], b)
	}
	return out, rows.Err()
}

// EvalMarketMeta is lightweight market meta for offline eval.
type EvalMarketMeta struct {
	ConditionID    string
	MarketID       string
	TokenID        string // primary CLOB token
	Category       string
	NegRisk        bool
	NegRiskGroupID string
	RelatedLegs    []string
	Volume24hr     float64
	Spread         float64
	OneDayChange   float64
	EndDate        time.Time
	Active         bool
}

// LoadTopVolumeMarkets returns active markets ordered by volume_24hr with primary token.
func LoadTopVolumeMarkets(ctx context.Context, conn DBInterface, limit int) ([]EvalMarketMeta, error) {
	if conn == nil {
		return nil, ErrNilDB
	}
	if limit <= 0 {
		limit = 300
	}
	rows, err := conn.Query(ctx, `
		SELECT
			COALESCE(condition_id, id::text),
			COALESCE(id::text, ''),
			COALESCE(clob_token_ids, ''),
			COALESCE(category, ''),
			COALESCE(neg_risk_other, false),
			COALESCE(volume_24hr, 0),
			COALESCE(spread, 0),
			COALESCE(one_day_price_change, 0),
			end_date
		FROM plymkt_markets
		WHERE active = true
		  AND clob_token_ids IS NOT NULL AND clob_token_ids <> ''
		ORDER BY volume_24hr DESC NULLS LAST
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EvalMarketMeta
	for rows.Next() {
		var m EvalMarketMeta
		var tokens string
		var end *time.Time
		if err := rows.Scan(
			&m.ConditionID, &m.MarketID, &tokens, &m.Category, &m.NegRisk,
			&m.Volume24hr, &m.Spread, &m.OneDayChange, &end,
		); err != nil {
			return nil, err
		}
		m.TokenID = primaryTokenFromJSON(tokens)
		if end != nil {
			m.EndDate = end.UTC()
		}
		m.Active = true
		if m.TokenID != "" {
			out = append(out, m)
		}
	}
	return out, rows.Err()
}

// LoadEdgeBoardTokens returns primary tokens from the current edge board.
func LoadEdgeBoardTokens(ctx context.Context, conn DBInterface, strategy string) ([]EvalMarketMeta, error) {
	if conn == nil {
		return nil, ErrNilDB
	}
	if strategy == "" {
		strategy = "default"
	}
	rows, err := conn.Query(ctx, `
		SELECT condition_id, COALESCE(market_id,''), clob_token_ids,
			COALESCE(category,''), neg_risk, COALESCE(neg_risk_group_id,''),
			COALESCE(related_legs, '{}'),
			COALESCE(volume_24hr,0), COALESCE(spread,0)
		FROM edge_board
		WHERE strategy = $1
		ORDER BY rank ASC
	`, strategy)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EvalMarketMeta
	for rows.Next() {
		var m EvalMarketMeta
		var tokens []string
		var legs []string
		if err := rows.Scan(
			&m.ConditionID, &m.MarketID, &tokens, &m.Category, &m.NegRisk,
			&m.NegRiskGroupID, &legs, &m.Volume24hr, &m.Spread,
		); err != nil {
			return nil, err
		}
		if len(tokens) > 0 {
			m.TokenID = tokens[0]
		}
		m.RelatedLegs = legs
		if m.TokenID != "" {
			out = append(out, m)
		}
	}
	return out, rows.Err()
}

// primaryTokenFromJSON parses clob_token_ids JSON array or CSV-ish text.
func primaryTokenFromJSON(raw string) string {
	raw = trimSpace(raw)
	if raw == "" {
		return ""
	}
	// JSON array
	if len(raw) > 0 && raw[0] == '[' {
		// cheap parse: find first quoted string
		inQ := false
		var b []byte
		for i := 0; i < len(raw); i++ {
			c := raw[i]
			if c == '"' {
				if inQ {
					return string(b)
				}
				inQ = true
				b = b[:0]
				continue
			}
			if inQ {
				b = append(b, c)
			}
		}
		return ""
	}
	// plain single token
	return raw
}

func trimSpace(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t' || s[0] == '\n') {
		s = s[1:]
	}
	for len(s) > 0 {
		c := s[len(s)-1]
		if c == ' ' || c == '\t' || c == '\n' {
			s = s[:len(s)-1]
			continue
		}
		break
	}
	return s
}

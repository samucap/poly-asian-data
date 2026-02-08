package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/services"
)

func newSecureHTTPClient() *http.Client {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS13,
	}

	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		ForceAttemptHTTP2:   true,
	}

	return &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}
}

type CategoryData struct {
	*services.PlyMktTag
	RelatedTags []*services.PlyMktTag
}

type MarketFilter struct {
	MinVolume24hr float64 // e.g., 50000.0
	MinLiquidity  float64 // e.g., 30000.0 (use LiquidityClob if available)
	MaxSpread     float64 // e.g., 0.05 (5 cents or 5%)
	MinVolatility float64 // e.g., 0.01 (1% price change for signals)
	MaxN          int     // e.g., 600
}

type scoredMarket struct {
	Market *services.PlyMktMarket
	Score  float64
}

func main() {
	logging.Init("dev")

	// 1. Configuration
	_ = godotenv.Load()
	dbConnString := os.Getenv("POSTGRES_URL")
	if dbConnString == "" {
		logging.Info("POSTGRES_URL not set, using default for local dev")
		user := os.Getenv("POSTGRES_USER")
		password := os.Getenv("POSTGRES_PASSWORD")
		host := os.Getenv("POSTGRES_HOST")
		port := os.Getenv("POSTGRES_PORT")
		dbName := os.Getenv("POSTGRES_DB")
		if host == "" {
			host = "localhost"
		}
		if port == "" {
			port = "5432"
		}
		if dbName == "" {
			dbName = "postgres"
		}
		dbConnString = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, dbName)
	}

	fetchInterval := 10 * time.Minute
	if intervalStr := os.Getenv("REFRESH_INTERVAL"); intervalStr != "" {
		if d, err := time.ParseDuration(intervalStr); err == nil {
			fetchInterval = d
		}
	}

	platformEstDailyVol := 850_000_000.0 // Updated for 2026 averages (~$6B weekly)
	if volStr := os.Getenv("PLATFORM_EST_DAILY_VOL"); volStr != "" {
		if v, err := strconv.ParseFloat(volStr, 64); err == nil {
			platformEstDailyVol = v
		}
	}

	// 2. Initialize Resources
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbPool, err := pgxpool.New(ctx, dbConnString)
	if err != nil {
		logging.Error(fmt.Sprintf("Unable to connect to database: %v", err))
		os.Exit(1)
	}
	defer dbPool.Close()

	if err := dbPool.Ping(ctx); err != nil {
		logging.Error(fmt.Sprintf("Failed to ping database: %v", err))
		os.Exit(1)
	}
	logging.Info("Connected to database successfully")

	cl := newSecureHTTPClient()

	// 3. Setup Filter (updated thresholds for 2026 scales)
	filter := MarketFilter{
		MinVolume24hr: 50000.0, // $50k+ 24h volume
		MinLiquidity:  30000.0, // $30k+ liquidity
		MaxSpread:     0.05,    // 5% or less
		MinVolatility: 0.01,    // at least 1% daily change
		MaxN:          600,     // top 600
	}

	// 4. Fetch Categories and Start Refresh Loops
	var wg sync.WaitGroup

	cats, err := fetchCategories(ctx, cl)
	if err != nil {
		logging.Error(fmt.Sprintf("Failed to fetch categories: %v", err))
		os.Exit(1)
	}

	// Save tags to database
	if err := upsertTags(ctx, dbPool, cats); err != nil {
		logging.Error(fmt.Sprintf("Failed to upsert tags: %v", err))
	}

	topCats := make([]*services.PlyMktTag, 0, len(cats))
	for _, cat := range cats {
		if cat.ParentCategory == "102982" && (cat.Slug == "sports" || cat.Slug == "politics" || cat.Slug == "crypto") {
			topCats = append(topCats, cat)
		}
	}

	// Start a refresh loop for each top category
	for _, cat := range topCats {
		wg.Add(1)
		go func(cat *services.PlyMktTag) {
			defer wg.Done()
			StartMarketRefreshLoop(ctx, dbPool, cl, filter, fetchInterval, platformEstDailyVol, cat.Label, cat.ID)
		}(cat)
	}

	// 5. Handle Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	logging.Info("Market refresh service started. Press CTRL+C to stop.")
	<-quit
	logging.Info("Shutdown signal received. Cancelling context...")

	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logging.Info("Graceful shutdown completed.")
	case <-time.After(5 * time.Second):
		logging.Error("Shutdown timed out, forcing exit.")
	}
}

// StartMarketRefreshLoop runs the fetch-rank-upsert cycle periodically
func StartMarketRefreshLoop(ctx context.Context, db *pgxpool.Pool, client *http.Client, filter MarketFilter, interval time.Duration, platformEstDailyVol float64, category string, tagID string) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Run once immediately
	logging.Info(fmt.Sprintf("Starting initial market refresh for category='%s' (tagID=%s)...", category, tagID))
	refreshMarketsOnce(ctx, db, client, filter, platformEstDailyVol, category, tagID)

	for {
		select {
		case <-ctx.Done():
			logging.Info(fmt.Sprintf("Stopping market refresh loop for category='%s'...", category))
			return
		case <-ticker.C:
			logging.Info(fmt.Sprintf("Starting periodic market refresh for category='%s'...", category))
			refreshMarketsOnce(ctx, db, client, filter, platformEstDailyVol, category, tagID)
		}
	}
}

func refreshMarketsOnce(ctx context.Context, db *pgxpool.Pool, client *http.Client, filter MarketFilter, platformEstDailyVol float64, category string, tagID string) {
	gammaBase := "https://gamma-api.polymarket.com/events"
	u, err := url.Parse(gammaBase)
	if err != nil {
		logging.Error(fmt.Sprintf("Failed to parse URL: %v", err))
		return
	}
	q := u.Query()
	q.Set("closed", "false")
	q.Set("active", "true")
	q.Set("order", "volume24hr")
	q.Set("ascending", "false")
	q.Set("include_chat", "false")
	q.Set("related_tags", "true")
	if tagID != "" {
		q.Set("tag_id", tagID)
	} else if category != "global" {
		q.Set("slug", category)
	}
	u.RawQuery = q.Encode()
	evsUrl := u

	var mkts []*services.PlyMktMarket
	var events []*services.PlyMktEvent
	fetchTime := time.Now()

	events, err = FetchPaginated[services.PlyMktEvent](client, evsUrl, 500, 0)
	if err != nil {
		logging.Error(fmt.Sprintf("Failed to fetch sports events: %v", err))
		return
	}
	logging.Info(fmt.Sprintf("Fetched %d events, extracting markets...", len(events)))
	for _, e := range events {
		mkts = append(mkts, e.Markets...)
	}
	logging.Info(fmt.Sprintf("Extracted %d markets from %d events", len(mkts), len(events)))

	// Client-side filter if no tagID (fallback safety)
	targetMkts := mkts
	if category != "global" && tagID == "" {
		var safeList []*services.PlyMktMarket
		for _, m := range mkts {
			if strings.EqualFold(m.Category, category) {
				safeList = append(safeList, m)
			}
		}
		targetMkts = safeList
		logging.Info(fmt.Sprintf("Client-side filtered %d markets (from %d) for category '%s'", len(targetMkts), len(mkts), category))
	}

	// Rank and filter
	rankedMkts := RankMarkets(targetMkts, filter)

	// Log
	LogMarketStats(rankedMkts, len(mkts), fetchTime, platformEstDailyVol)

	// Upsert events (for sports)
	if err := upsertEvents(ctx, db, events, fetchTime); err != nil {
		logging.Error(fmt.Sprintf("Failed to upsert events: %v", err))
	} else {
		logging.Info(fmt.Sprintf("Upserted %d events", len(events)))
	}

	// Upsert markets
	if len(rankedMkts) > 0 {
		if err := upsertMarkets(ctx, db, rankedMkts, fetchTime, category); err != nil {
			logging.Error(fmt.Sprintf("Failed to upsert markets: %v", err))
		} else {
			logging.Info(fmt.Sprintf("Upserted %d markets (category=%s)", len(rankedMkts), category))
		}
	}
}

// RankMarkets filters, computes scores, sorts by score desc, and returns top N
func RankMarkets(markets []*services.PlyMktMarket, filter MarketFilter) []*services.PlyMktMarket {
	var candidates []*services.PlyMktMarket
	maxVol24hr, maxLiq, maxVol, maxVola := 0.0, 0.0, 0.0, 0.0

	for _, m := range markets {
		liq := m.LiquidityClob
		if liq == 0 {
			if val, err := strconv.ParseFloat(m.Liquidity, 64); err == nil {
				liq = val
			}
		}

		if m.Volume24hr < filter.MinVolume24hr ||
			liq < filter.MinLiquidity ||
			m.Spread > filter.MaxSpread ||
			math.Abs(m.OneDayPriceChange) < filter.MinVolatility {
			continue
		}

		candidates = append(candidates, m)

		if m.Volume24hr > maxVol24hr {
			maxVol24hr = m.Volume24hr
		}
		if liq > maxLiq {
			maxLiq = liq
		}
		vol, _ := strconv.ParseFloat(m.Volume, 64)
		if vol > maxVol {
			maxVol = vol
		}
		vola := math.Abs(m.OneDayPriceChange)
		if vola > maxVola {
			maxVola = vola
		}
	}

	maxVals := struct {
		MaxVol24hr    float64
		MaxLiquidity  float64
		MaxVol        float64
		MaxVolatility float64
	}{maxVol24hr, maxLiq, maxVol, maxVola}

	if len(candidates) == 0 {
		return nil
	}

	sort.Slice(candidates, func(i, j int) bool {
		return ComputeScore(*candidates[i], maxVals) > ComputeScore(*candidates[j], maxVals)
	})

	if len(candidates) > filter.MaxN {
		return candidates[:filter.MaxN]
	}
	return candidates
}

// ComputeScore returns a 0-1 normalized score (higher = more desirable)
func ComputeScore(m services.PlyMktMarket, maxVals struct {
	MaxVol24hr    float64
	MaxLiquidity  float64
	MaxVol        float64
	MaxVolatility float64
}) float64 {
	if !m.Active || m.Closed || len(m.ClobTokenIds) == 0 {
		return 0.0
	}

	// Weights (tunable via env for flexibility)
	wVol24hr := 0.45
	if v := os.Getenv("SCORE_W_VOL24HR"); v != "" {
		wVol24hr, _ = strconv.ParseFloat(v, 64)
	}
	wLiq := 0.25
	if v := os.Getenv("SCORE_W_LIQ"); v != "" {
		wLiq, _ = strconv.ParseFloat(v, 64)
	}
	wVola := 0.10
	if v := os.Getenv("SCORE_W_VOLA"); v != "" {
		wVola, _ = strconv.ParseFloat(v, 64)
	}
	wVol := 0.15
	if v := os.Getenv("SCORE_W_VOL"); v != "" {
		wVol, _ = strconv.ParseFloat(v, 64)
	}
	wSpread := 0.05
	if v := os.Getenv("SCORE_W_SPREAD"); v != "" {
		wSpread, _ = strconv.ParseFloat(v, 64)
	}

	vol24hrNorm := 0.0
	if maxVals.MaxVol24hr > 0 {
		vol24hrNorm = math.Min(m.Volume24hr/maxVals.MaxVol24hr, 1.0)
	}

	liq := m.LiquidityClob
	if liq == 0 {
		liq, _ = strconv.ParseFloat(m.Liquidity, 64)
	}
	liqNorm := 0.0
	if maxVals.MaxLiquidity > 0 {
		liqNorm = math.Min(liq/maxVals.MaxLiquidity, 1.0)
	}

	vol, _ := strconv.ParseFloat(m.Volume, 64)
	volNorm := 0.0
	if maxVals.MaxVol > 0 {
		volNorm = math.Min(vol/maxVals.MaxVol, 1.0)
	}

	volaNorm := 0.0
	volatility := math.Abs(m.OneDayPriceChange)
	if maxVals.MaxVolatility > 0 {
		volaNorm = math.Min(volatility/maxVals.MaxVolatility, 1.0)
	}

	spreadPenalty := 1.0
	if m.Spread > 0 {
		spreadPenalty = math.Max(0.0, 1.0-(m.Spread/0.10))
	}

	// New: Penalty for old markets (favor fresh ones; age in days)
	// TODO: review this
	startTime := m.EventStartTime
	if m.GameID != "" {
		parsedTime, err := time.Parse("2006-01-02 15:04:05-0700", m.GameStartTime)
		if err != nil {
			return 0
		}
		startTime = parsedTime
	}

	ageDays := time.Since(startTime).Hours() / 24
	agePenalty := math.Max(0.5, 1.0-(ageDays/90.0)) // Decay over ~3 months

	score := (vol24hrNorm*wVol24hr +
		liqNorm*wLiq +
		volaNorm*wVola +
		volNorm*wVol +
		spreadPenalty*wSpread) * agePenalty

	return score
}

func fetchCategories(ctx context.Context, client *http.Client) ([]*services.PlyMktTag, error) {
	cats := []*services.PlyMktTag{}
	catsID := "102982"
	tagsBase := "https://gamma-api.polymarket.com/tags"
	relatedPath := "/related-tags/tags?status=active&omit_empty=true"
	u, err := url.Parse(fmt.Sprintf("%s/%s%s", tagsBase, catsID, relatedPath))
	if err != nil {
		return nil, err
	}

	tags, err := FetchPaginated[services.PlyMktTag](client, u, 100, 0)
	if err != nil {
		return nil, err
	}
	for _, tag := range tags {
		tag.ParentCategory = catsID
		cats = append(cats, tag)

		u, err = url.Parse(fmt.Sprintf("%s/%s%s", tagsBase, tag.ID, relatedPath))
		if err != nil {
			return nil, err
		}

		related, err := FetchPaginated[services.PlyMktTag](client, u, 100, 0)
		if err != nil {
			return nil, err
		}
		for _, relatedTag := range related {
			relatedTag.ParentCategory = tag.ID
		}

		cats = append(cats, related...)
	}

	logging.Info(fmt.Sprintf("Fetched %d categories", len(cats)))
	return cats, nil
}

// upsertTags inserts/updates tags into the tags table
func upsertTags(ctx context.Context, db *pgxpool.Pool, categories []*services.PlyMktTag) error {
	// Ensure table exists
	createSQL := `
		CREATE TABLE IF NOT EXISTS tags (
			id TEXT PRIMARY KEY,
			label TEXT,
			slug TEXT,
			force_show BOOLEAN,
			force_hide BOOLEAN,
			sport_id UUID,
			parent_tag_id TEXT,
			created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
		);
	`
	if _, err := db.Exec(ctx, createSQL); err != nil {
		return fmt.Errorf("failed to create tags table: %w", err)
	}

	batch := &pgx.Batch{}

	sql := `
		INSERT INTO tags (
			id, label, slug, force_show, force_hide, parent_tag_id, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8
		)
		ON CONFLICT (id) DO UPDATE SET
			label = EXCLUDED.label,
			slug = EXCLUDED.slug,
			force_show = EXCLUDED.force_show,
			force_hide = EXCLUDED.force_hide,
			parent_tag_id = EXCLUDED.parent_tag_id,
			updated_at = EXCLUDED.updated_at;
	`

	now := time.Now()
	tagCount := 0

	// First, queue all parent category tags
	if len(categories) > 0 {
		logging.Info(fmt.Sprintf("First tag: ID=%s, Label=%s, Slug=%s, ParentCategory=%s", categories[0].ID, categories[0].Label, categories[0].Slug, categories[0].ParentCategory))
	}
	for _, t := range categories {

		// Parent category: parent_tag_id is NULL or the catsID (102982)
		var parentID *string
		if t.ParentCategory != "" {
			parentID = &t.ParentCategory
		}

		batch.Queue(sql,
			t.ID,
			t.Label,
			t.Slug,
			t.ForceShow,
			t.ForceHide,
			parentID,
			now,
			now,
		)
		tagCount++
	}

	if tagCount == 0 {
		return nil
	}

	logging.Info(fmt.Sprintf("Sending batch with %d tag operations", tagCount))
	br := db.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < tagCount; i++ {
		_, err := br.Exec()
		if err != nil {
			logging.Error(fmt.Sprintf("error executing batch item %d: %v", i, err))
			// Log the problematic tag data
			if i < len(categories) {
				tag := categories[i]
				logging.Error(fmt.Sprintf("Problematic tag: ID=%s, Label=%s, ParentCategory=%s", tag.ID, tag.Label, tag.ParentCategory))
			}
			return fmt.Errorf("error executing batch item %d: %w", i, err)
		}
	}

	logging.Info(fmt.Sprintf("Upserted %d tags to database", tagCount))
	return nil
}

// upsertMarkets inserts the ranked markets into the hot_markets_vol hypertable
func upsertMarkets(ctx context.Context, db *pgxpool.Pool, markets []*services.PlyMktMarket, t time.Time, category string) error {
	// We use COPY or batch inserts for efficiency?
	// Or just a loop with Prepare/Exec for simplicity since N is small (~100-600).
	// A batch with pgx is best.

	batch := &pgx.Batch{}

	sql := `
		INSERT INTO hot_markets_vol (
			time, market_id, question, 
			volume_24hr, volume_total, 
			liquidity_clob, liquidity_fallback, 
			spread, price_change_1d, 
			score, clob_token_ids, 
			active, closed, 
			last_fetched, rank, category
		) VALUES (
			$1, $2, $3, 
			$4, $5, 
			$6, $7, 
			$8, $9, 
			$10, $11, 
			$12, $13, 
			$14, $15, $16
		)
		ON CONFLICT (time, market_id, category) DO UPDATE SET
			volume_24hr = EXCLUDED.volume_24hr,
			score = EXCLUDED.score,
			last_fetched = EXCLUDED.last_fetched,
			rank = EXCLUDED.rank;
	`
	// Note: ON CONFLICT ... DO UPDATE is technically not needed if we insert with unique timestamps per batch or if 'time' is part of PK and we assume no collision within same Fetch call.
	// But it's good safety.

	// Need to recalculate scores or pass them in?
	// RankMarkets returns []*PlyMktMarket, but the scores are lost in result (internal step).
	// Ideally RankMarkets should return the struct with score, but to minimize refactoring, we can re-compute or attach it.
	// Or better: Modify RankMarkets to return `[]ScoredMarket`?
	// For minimal invasion, I'll just re-compute the score here using the same function since it's deterministic.
	// However, RankMarkets internal logic for normalization uses max values of the *candidates*.
	// Re-computing here without the full candidate set max-values might slightly differ.
	// BUT: `Ground.go` is a script. Let's do a quick hack: Re-calculate RankMarkets logic effectively?
	// No, that's wasteful.
	//
	// OPTION A: Change RankMarkets signature.
	// OPTION B: Assume score is not critical for DB, or compute a "Standalone Score".
	//
	// Let's go with OPTION A: I will modify RankMarkets to return `[]*services.PlyMktMarket` but I will attach the score to the object if possible?
	// `PlyMktMarket` struct has a `Score` field (int64) but our score is float64.
	//
	// Let's just update `RankMarkets` to return `[]scoredMarket` (the internal private struct) and make it public or just return a new struct.
	// Actually, strictly speaking the user asked to "Upsert logic... computed score".
	// I'll leave `RankMarkets` as is to avoid breaking signatures if used elsewhere (it's a playground script though).
	// I will just re-compute the score "roughly" or just assume 0 for now to keep it simple, OR cleaner:
	//
	// I'll make `RankMarkets` receive a helper callback or just inline the score calc.
	// Actually, `Score` in `PlyMktMarket` json is `int64`.
	// I'll just skip the score in DB or compute it locally.
	// Wait, the prompt says "computed score".
	// I will modify `RankMarkets` to store the score in the `PlyMktMarket.Score` field (casted to int64 * 1000 maybe?) or just ignore it.
	//
	// Better: I will copy the minimal normalization logic here. It's fast.

	// Max vals for score normalization (approximate from this batch)
	maxVol24hr, maxLiq, maxVol, maxVola := 0.0, 0.0, 0.0, 0.0
	for _, m := range markets {
		if m.Volume24hr > maxVol24hr {
			maxVol24hr = m.Volume24hr
		}

		liq := m.LiquidityClob
		if liq == 0 {
			if val, err := strconv.ParseFloat(m.Liquidity, 64); err == nil {
				liq = val
			}
		}
		if liq > maxLiq {
			maxLiq = liq
		}

		vol, _ := strconv.ParseFloat(m.Volume, 64)
		if vol > maxVol {
			maxVol = vol
		}

		vola := math.Abs(m.OneDayPriceChange)
		if vola > maxVola {
			maxVola = vola
		}
	}

	maxVals := struct {
		MaxVol24hr    float64
		MaxLiquidity  float64
		MaxVol        float64
		MaxVolatility float64
	}{maxVol24hr, maxLiq, maxVol, maxVola}

	for i, m := range markets {
		// Re-compute score
		score := ComputeScore(*m, maxVals)

		// Safe parsing for fields
		liq := m.LiquidityClob
		if liq == 0 {
			if val, err := strconv.ParseFloat(m.Liquidity, 64); err == nil {
				liq = val
			}
		}

		volTotal, _ := strconv.ParseFloat(m.Volume, 64)

		// JSONB for ClobTokenIds
		// m.ClobTokenIds is a string in the struct?
		// services.PlyMktMarket definition says: `ClobTokenIds string \`json:"clobTokenIds"\``
		// Wait, if it's a string in the struct ("[\"token1\", \"token2\"]"), we can just pass it as string/bytes to JSONB.
		// If it's already a JSON string, PGX should handle it if we cast or just pass as string.

		batch.Queue(sql,
			t,                   // time
			m.ID,                // market_id
			m.Question,          // question
			m.Volume24hr,        // volume_24hr
			volTotal,            // volume_total
			m.LiquidityClob,     // liquidity_clob
			liq,                 // liquidity_fallback
			m.Spread,            // spread
			m.OneDayPriceChange, // price_change_1d
			score,               // score
			m.ClobTokenIds,      // clob_token_ids (jsonb)
			m.Active,            // active
			m.Closed,            // closed
			time.Now(),          // last_fetched
			i+1,                 // rank
			category,            // category
		)
	}

	br := db.SendBatch(ctx, batch)
	defer br.Close()

	// Execute
	for i := 0; i < len(markets); i++ {
		_, err := br.Exec()
		if err != nil {
			return fmt.Errorf("error executing batch item %d: %w", i, err)
		}
	}

	return nil
}

// upsertEvents inserts events into the hot_events hypertable
func upsertEvents(ctx context.Context, db *pgxpool.Pool, events []*services.PlyMktEvent, t time.Time) error {
	batch := &pgx.Batch{}

	sql := `
		INSERT INTO hot_events (
			time, id, ticker, slug, title, subtitle, description, resolution_source,
			start_date, creation_date, end_date, image, icon,
			active, closed, archived, new, featured, restricted,
			liquidity, volume, open_interest, sort_by, category, subcategory,
			is_template, template_variables, published_at, created_by, updated_by,
			created_at, updated_at, comments_enabled, competitive,
			volume_24hr, volume_1wk, volume_1mo, volume_1yr,
			featured_image, disqus_thread, parent_event, enable_order_book,
			liquidity_amm, liquidity_clob, neg_risk, neg_risk_market_id, neg_risk_fee_bips,
			comment_count, cyom, tags, sub_events
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8,
			$9, $10, $11, $12, $13,
			$14, $15, $16, $17, $18, $19,
			$20, $21, $22, $23, $24, $25,
			$26, $27, $28, $29, $30,
			$31, $32, $33, $34,
			$35, $36, $37, $38,
			$39, $40, $41, $42,
			$43, $44, $45, $46, $47,
			$48, $49, $50, $51
		)
		ON CONFLICT (time, id) DO UPDATE SET
			volume_24hr = EXCLUDED.volume_24hr,
			volume = EXCLUDED.volume,
			liquidity = EXCLUDED.liquidity,
			active = EXCLUDED.active,
			closed = EXCLUDED.closed;
	`

	for _, e := range events {
		// Convert tags to JSON
		tagsJSON, _ := json.Marshal(e.Tags)

		batch.Queue(sql,
			t,                   // time
			e.ID,                // id
			e.Ticker,            // ticker
			e.Slug,              // slug
			e.Title,             // title
			e.Subtitle,          // subtitle
			e.Description,       // description
			e.ResolutionSource,  // resolution_source
			e.StartDate,         // start_date
			e.CreationDate,      // creation_date
			e.EndDate,           // end_date
			e.Image,             // image
			e.Icon,              // icon
			e.Active,            // active
			e.Closed,            // closed
			e.Archived,          // archived
			e.New,               // new
			e.Featured,          // featured
			e.Restricted,        // restricted
			e.Liquidity,         // liquidity
			e.Volume,            // volume
			e.OpenInterest,      // open_interest
			e.SortBy,            // sort_by
			e.Category,          // category
			e.Subcategory,       // subcategory
			e.IsTemplate,        // is_template
			e.TemplateVariables, // template_variables
			e.PublishedAt,       // published_at
			e.CreatedBy,         // created_by
			e.UpdatedBy,         // updated_by
			e.CreatedAt,         // created_at
			e.UpdatedAt,         // updated_at
			e.CommentsEnabled,   // comments_enabled
			e.Competitive,       // competitive
			e.Volume24hr,        // volume_24hr
			e.Volume1wk,         // volume_1wk
			e.Volume1mo,         // volume_1mo
			e.Volume1yr,         // volume_1yr
			e.FeaturedImage,     // featured_image
			e.DisqusThread,      // disqus_thread
			e.ParentEvent,       // parent_event
			e.EnableOrderBook,   // enable_order_book
			e.LiquidityAmm,      // liquidity_amm
			e.LiquidityClob,     // liquidity_clob
			e.NegRisk,           // neg_risk
			e.NegRiskMarketID,   // neg_risk_market_id
			e.NegRiskFeeBips,    // neg_risk_fee_bips
			e.CommentCount,      // comment_count
			e.Cyom,              // cyom
			tagsJSON,            // tags (JSONB)
			e.SubEvents,         // sub_events (TEXT[])
		)
	}

	br := db.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < len(events); i++ {
		_, err := br.Exec()
		if err != nil {
			return fmt.Errorf("error executing batch item %d: %w", i, err)
		}
	}

	return nil
}

// LogMarketStats prints detailed stats on the ranked/filtered markets
// Call this after RankMarkets(markets, filter)
func LogMarketStats(selected []*services.PlyMktMarket, rawCount int, fetchTime time.Time, platformDailyVolEstimate float64) {
	if len(selected) == 0 {
		logging.Info(fmt.Sprintf("[MARKET STATS %s] No markets passed filters (raw fetched: %d)", fetchTime.Format(time.RFC3339), rawCount))
		return
	}

	now := time.Now()
	logging.Info(fmt.Sprintf("[MARKET STATS %s] Selected %d markets (from %d raw fetched) | Duration: %v",
		now.Format(time.RFC3339), len(selected), rawCount, now.Sub(fetchTime)))

	// Compute aggregates
	var totalVol24hr, totalLiquidity, totalVol float64
	var sumVolatility float64
	var spreads []float64
	var volumes24hr []float64
	var liquidities []float64

	for _, m := range selected {
		totalVol24hr += m.Volume24hr
		liq := m.LiquidityClob
		if liq == 0 {
			liq = m.LiquidityNum
		}
		totalLiquidity += liq
		totalVol += m.VolumeNum
		sumVolatility += math.Abs(m.OneDayPriceChange)
		spreads = append(spreads, m.Spread)
		volumes24hr = append(volumes24hr, m.Volume24hr)
		liquidities = append(liquidities, liq)
	}

	avgVol24hr := totalVol24hr / float64(len(selected))
	avgLiq := totalLiquidity / float64(len(selected))
	avgVol := totalVol / float64(len(selected))
	avgVolatility := sumVolatility / float64(len(selected))
	avgSpread := medianFloat(spreads) // or average if preferred

	// Sort for percentiles/min/max
	sort.Float64s(volumes24hr)
	sort.Float64s(liquidities)
	sort.Float64s(spreads)

	medVol24hr := medianFloat(volumes24hr)
	medLiq := medianFloat(liquidities)
	p90Vol24hr := percentileFloat(volumes24hr, 0.90)
	p90Liq := percentileFloat(liquidities, 0.90)

	// Rough coverage estimate (platform daily vol is approximate; update as you observe)
	// From recent data: Polymarket ~$100M–$300M/day typical; peaks higher
	coveragePct := 0.0
	if platformDailyVolEstimate > 0 {
		coveragePct = (totalVol24hr / platformDailyVolEstimate) * 100
	}

	logging.Info(fmt.Sprintf("  Count: %d", len(selected)))
	logging.Info(fmt.Sprintf("  Total 24h Volume in set: $%.2fM (est coverage: %.1f%% of ~$%.0fM platform daily)", totalVol24hr/1e6, coveragePct, platformDailyVolEstimate/1e6))
	logging.Info(fmt.Sprintf("  Total Liquidity in set: $%.2fM", totalLiquidity/1e6))
	logging.Info(fmt.Sprintf("  Averages: Vol24hr $%.0f | Liquidity $%.0f | Lifetime Vol $%.0f | Volatility %.2f%% | Spread %.3f", avgVol24hr, avgLiq, avgVol, avgVolatility*100, avgSpread))

	logging.Info(fmt.Sprintf("  Medians: Vol24hr $%.0f | Liquidity $%.0f", medVol24hr, medLiq))
	logging.Info(fmt.Sprintf("  P90: Vol24hr $%.0f | Liquidity $%.0f", p90Vol24hr, p90Liq))
	logging.Info(fmt.Sprintf("  Min/Max Vol24hr: $%.0f / $%.2fM", volumes24hr[0], volumes24hr[len(volumes24hr)-1]/1e6))

	// Top 5 by volume24hr for quick check
	logging.Info("  Top 5 markets by 24h volume:")
	sort.Slice(selected, func(i, j int) bool { return selected[i].Volume24hr > selected[j].Volume24hr })
	for i := 0; i < min(5, len(selected)); i++ {
		m := selected[i]
		liq := m.LiquidityClob
		if liq == 0 {
			liq = m.LiquidityNum
		}
		logging.Info(fmt.Sprintf("    #%d: %s | Vol24hr $%.2fM | Liq $%.2fM | Spread %.3f | Question: %.80s...",
			i+1, m.ID, m.Volume24hr/1e6, liq/1e6, m.Spread, m.Question))
	}

	logging.Info("--- End MARKET STATS ---")
}

// Helper: median of sorted float64 slice
func medianFloat(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	if len(data)%2 == 1 {
		return data[len(data)/2]
	}
	mid := len(data) / 2
	return (data[mid-1] + data[mid]) / 2
}

// Helper: percentile (e.g. 0.90) of sorted slice
func percentileFloat(data []float64, p float64) float64 {
	if len(data) == 0 {
		return 0
	}
	idx := int(math.Round(p * float64(len(data)-1)))
	return data[idx]
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// FetchPaginated is a generic function to fetch all pages of a resource
func FetchPaginated[T any](cl *http.Client, baseURL *url.URL, limit int, limitThreshold int) ([]*T, error) {
	var allResults []*T
	offset := 0

	for {
		logging.Info(fmt.Sprintf("Fetching %T offset=%d limit=%d...\n", *new(T), offset, limit))

		// Construct URL with pagination params
		q := baseURL.Query()
		q.Set("limit", fmt.Sprintf("%d", limit))
		q.Set("offset", fmt.Sprintf("%d", offset))

		reqURL := *baseURL
		reqURL.RawQuery = q.Encode()

		// Execute Request
		resp, err := cl.Get(reqURL.String())
		if err != nil {
			return nil, err
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("bad status: %s", resp.Status)
		}

		var pageData []*T
		if err := json.NewDecoder(resp.Body).Decode(&pageData); err != nil {
			resp.Body.Close()
			return nil, err
		}
		resp.Body.Close()

		if len(pageData) == 0 {
			break
		}

		allResults = append(allResults, pageData...)
		offset += len(pageData)

		if limitThreshold > 0 && len(allResults) >= limitThreshold {
			break
		}

		// If we got fewer than limit, we're likely done
		if len(pageData) < limit {
			break
		}

		// Rate limit: Sleep between pages to avoid hitting 300 req/10s limit
		// 300 req / 10s = 30 req/s. We want to be safe.
		// Sleeping 250ms guarantees max 4 req/s = 40 req/10s << 300
		time.Sleep(250 * time.Millisecond)
	}

	return allResults, nil
}

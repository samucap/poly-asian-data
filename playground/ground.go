package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
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
	TotalVol     float64
	TotalVol24hr float64
	TotalLiq     float64
	TotalMarkets int
	RelatedTags  map[string]*CategoryData
}

type MarketFilter struct {
	MinVolume24hr float64 // e.g., 50000.0
	MinLiquidity  float64 // e.g., 30000.0 (use LiquidityClob if available)
	MaxSpread     float64 // e.g., 0.05 (5 cents or 5%)
	MinVolatility float64 // e.g., 0.01 (1% price change for signals)
	MaxN          int     // e.g., 600
}

// lastPriceFetchTs tracks the timestamp of the last successful price history fetch.
// 0 on first run → triggers full 30-day backfill. Updated after each cycle.
var lastPriceFetchTs int64

type EventFilter struct {
	MinVolume24hr float64 // e.g., 50000.0
	MinLiquidity  float64 // e.g., 30000.0 (use LiquidityClob if available)
	MaxN          int     // e.g., 100 (top events to keep)
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
	// TODO: do we need minSpread ~3%? I think all taker orders have fees (~2%), so spread needs to account for that
	filter := MarketFilter{
		MinVolume24hr: 30000.0, // $30k+ 24h volume
		MinLiquidity:  15000.0, // $15k+ liquidity
		MaxSpread:     0.05,    // 5% or less
		MinVolatility: 0.01,    // at least 1% daily change
		MaxN:          500,     // top 500
	}

	// 4. Fetch Categories and Start Refresh Loops
	var wg sync.WaitGroup
	var topCats []*services.PlyMktTag

	dbCats, maxUpdated := fetchTagsFromDB(ctx, dbPool)
	if len(dbCats) > 0 && time.Since(maxUpdated) < 24*time.Hour {
		logging.Info(fmt.Sprintf("Loaded %d TOP tags from database (last updated %s ago)", len(dbCats), time.Since(maxUpdated).Round(time.Minute)))
		topCats = dbCats
	} else {
		if len(dbCats) == 0 {
			logging.Info("No top tags found in DB (or table missing). Fetching from API...")
		} else {
			logging.Info(fmt.Sprintf("Tags in DB are older than 24h (%s old). Fetching from API...", time.Since(maxUpdated).Round(time.Hour)))
		}

		cats, err := fetchCategories(ctx, cl)
		if err != nil {
			logging.Error(fmt.Sprintf("Failed to fetch categories: %v", err))
			os.Exit(1)
		}

		// Save tags to database
		if err := upsertTags(ctx, dbPool, cats); err != nil {
			logging.Error(fmt.Sprintf("Failed to upsert tags: %v", err))
		}

		for _, cat := range cats {
			if cat.ParentTagID == "102982" {
				topCats = append(topCats, cat)
			}
		}
	}

	// Start a refresh loop for each top category
	wg.Add(1)
	go func() {
		defer wg.Done()
		StartMarketRefreshLoop(ctx, dbPool, cl, filter, topCats, fetchInterval)
	}()

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
func StartMarketRefreshLoop(ctx context.Context, db *pgxpool.Pool, client *http.Client, filter MarketFilter, cats []*services.PlyMktTag, interval time.Duration) {
	// Run once immediately
	logging.Info("Starting initial market refresh...")
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	start := time.Now()
	refreshMarketsOnce(ctx, db, client, filter, cats)
	elapsed := time.Since(start)
	nextRun := time.Now().Add(interval)
	logging.Info(fmt.Sprintf("[>>] Refresh cycle completed in %s. Next run at %s (%s from now)",
		elapsed.Round(time.Second), nextRun.Format("15:04:05"), interval))

	for {
		select {
		case <-ctx.Done():
			logging.Info("Stopping market refresh loop...")
			return
		case <-ticker.C:
			logging.Info("Starting periodic market refresh...")
			start := time.Now()
			refreshMarketsOnce(ctx, db, client, filter, cats)
			elapsed := time.Since(start)
			nextRun := time.Now().Add(interval)
			logging.Info(fmt.Sprintf("[>>] Refresh cycle completed in %s. Next run at %s (%s from now)",
				elapsed.Round(time.Second), nextRun.Format("15:04:05"), interval))
		}
	}
}

func refreshMarketsOnce(ctx context.Context, db *pgxpool.Pool, client *http.Client, filter MarketFilter, cats []*services.PlyMktTag) {
	gammaBase := "https://gamma-api.polymarket.com/events"
	u, err := url.Parse(gammaBase)
	if err != nil {
		logging.Error(fmt.Sprintf("Failed to parse URL: %v", err))
		return
	}
	q := u.Query()
	q.Set("closed", "false")
	q.Set("active", "true")
	q.Set("archived", "false")
	q.Set("order", "volume24hr")
	q.Set("ascending", "false")
	q.Set("include_chat", "false")
	u.RawQuery = q.Encode()
	evsUrl := u

	var events []*services.PlyMktEvent
	fetchTime := time.Now()

	events, err = FetchPaginated[services.PlyMktEvent](client, evsUrl, 500, 0)
	if err != nil {
		logging.Error(fmt.Sprintf("Failed to fetch events: %v", err))
		return
	}
	logging.Info(fmt.Sprintf("Fetched %d events, extracting markets...", len(events)))

	// Build lookup map for top-level tags
	topTagByID := map[string]*services.PlyMktTag{}
	for _, c := range cats {
		topTagByID[c.ID] = c
	}

	byCat := map[string][]*services.PlyMktMarket{}
	catStats := map[string]*CategoryData{}

	for _, e := range events {
		// Classify tags into top tag and sub-tags in one pass
		var topTag *services.PlyMktTag
		var subTags []*services.PlyMktTag
		for _, tag := range e.Tags {
			if t, ok := topTagByID[tag.ID]; ok {
				topTag = t
			} else {
				subTags = append(subTags, tag)
			}
		}

		catSlug := ""
		if topTag != nil {
			catSlug = topTag.Slug
		}

		// Initialize category stats once
		if _, ok := catStats[catSlug]; !ok {
			catStats[catSlug] = &CategoryData{
				PlyMktTag:   topTag,
				RelatedTags: make(map[string]*CategoryData),
			}
		}
		cat := catStats[catSlug]

		// Accumulate per-market directly (eliminates intermediate variables)
		for i := range e.Markets {
			m := e.Markets[i]
			if !m.EnableOrderBook || !m.AcceptingOrders || !m.Active || m.Closed || m.ClosedTime != "" {
				continue
			}
			m.Category = catSlug
			byCat[catSlug] = append(byCat[catSlug], m)

			// Accumulate into top category
			cat.TotalVol += m.VolumeClob
			cat.TotalVol24hr += m.Volume24hrClob
			cat.TotalLiq += m.LiquidityClob
			cat.TotalMarkets++

			// Accumulate into each subcategory
			for _, sub := range subTags {
				if _, ok := cat.RelatedTags[sub.ID]; !ok {
					cat.RelatedTags[sub.ID] = &CategoryData{PlyMktTag: sub, RelatedTags: make(map[string]*CategoryData)}
				}
				rel := cat.RelatedTags[sub.ID]
				rel.TotalVol += m.VolumeClob
				rel.TotalVol24hr += m.Volume24hrClob
				rel.TotalLiq += m.LiquidityClob
				rel.TotalMarkets++
			}
		}
	}

	logCategoryStats(catStats)

	// Update tags with aggregated data
	// TODO: look into combining the initial tags update with this
	if err := updateTagsWithAggregates(ctx, db, catStats); err != nil {
		logging.Error(fmt.Sprintf("Failed to update tags with aggregates: %v", err))
	}

	// TODO: review hardcoding minVals for categories..
	catFilters := map[string]MarketFilter{
		"sports":   {MinVolume24hr: 30000.0, MinLiquidity: 15000.0, MaxSpread: 0.05, MinVolatility: 0.01, MaxN: 300},
		"crypto":   {MinVolume24hr: 50000.0, MinLiquidity: 25000.0, MaxSpread: 0.03, MinVolatility: 0.015, MaxN: 300},
		"politics": {MinVolume24hr: 60000.0, MinLiquidity: 30000.0, MaxSpread: 0.04, MinVolatility: 0.01, MaxN: 300},
		"":         {MinVolume24hr: 20000.0, MinLiquidity: 10000.0, MaxSpread: 0.08, MinVolatility: 0.005, MaxN: 300},
	}

	rankedByCat := map[string][]*services.PlyMktMarket{}
	for cat, mkts := range byCat {
		f, ok := catFilters[cat]
		if !ok {
			f = catFilters[""]
		}

		ranked := RankMarkets(mkts, f)
		if len(ranked) == 0 {
			continue
		}
		rankedByCat[cat] = ranked
		LogMarketStats(ranked, len(mkts), cat, fetchTime)
	}

	for cat, ranked := range rankedByCat {
		// Fetch OI, Trades, Prices, Orderbooks
		fetchTopMktsData(ctx, db, client, ranked, lastPriceFetchTs)
		lastPriceFetchTs = time.Now().Unix()

		// Upsert markets to DB
		if err := upsertMarkets(ctx, db, byCat[cat], fetchTime, cat); err != nil {
			logging.Error(fmt.Sprintf("Failed to upsert markets (category=%s): %v", cat, err))
		} else {
			logging.Info(fmt.Sprintf("Upserted %d markets (category=%s)", len(ranked), cat))
		}
	}
}

func fetchTopMktsData(ctx context.Context, db *pgxpool.Pool, client *http.Client, markets []*services.PlyMktMarket, priceFetchFrom int64) {
	var conditionIDs []string
	var clobTokens []clobToken
	marketByCondition := map[string]*services.PlyMktMarket{}

	for _, m := range markets {
		if m.ConditionID != "" {
			conditionIDs = append(conditionIDs, m.ConditionID)
			marketByCondition[m.ConditionID] = m
		}

		var tokenIDs []string
		if err := json.Unmarshal([]byte(m.ClobTokenIds), &tokenIDs); err == nil {
			for _, tid := range tokenIDs {
				clobTokens = append(clobTokens, clobToken{TokenID: tid, MarketID: m.ConditionID})
			}
		}
	}

	// --- Fetch Open Interest and map to markets ---
	if len(conditionIDs) > 0 {
		oiData, err := fetchOpenInterest(client, conditionIDs)
		if err != nil {
			logging.Error(fmt.Sprintf("Failed to fetch open interest: %v", err))
		} else {
			for _, oi := range oiData {
				if hm, ok := marketByCondition[oi.Market]; ok {
					hm.OpenInterest = oi.Value
				}
			}
			logging.Info(fmt.Sprintf("Mapped open interest for %d markets", len(oiData)))
		}

		trades, err := fetchTrades(client, conditionIDs)
		if err != nil {
			logging.Error(fmt.Sprintf("Failed to fetch trades: %v", err))
		} else {
			logging.Info(fmt.Sprintf("Fetched %d trades", len(trades)))
			if err := upsertTrades(ctx, db, trades); err != nil {
				logging.Error(fmt.Sprintf("Failed to upsert trades: %v", err))
			}
		}
	}

	// --- Fetch Prices History (concurrent, one per token) and upsert ---
	// --- Fetch Orderbooks and upsert snapshots ---
	if len(clobTokens) > 0 {
		allPrices := fetchPricesHistoryConcurrent(client, clobTokens, 5, priceFetchFrom) // fidelity=5
		logging.Info(fmt.Sprintf("Fetched %d price history points across %d tokens", len(allPrices), len(clobTokens)))
		if len(allPrices) > 0 {
			if err := upsertPricesHistory(ctx, db, allPrices); err != nil {
				logging.Error(fmt.Sprintf("Failed to upsert prices history: %v", err))
			}
		}

		tokenIDs := make([]string, 0, len(clobTokens))
		for _, ct := range clobTokens {
			tokenIDs = append(tokenIDs, ct.TokenID)
		}

		snapshots, err := fetchOrderbooks(client, tokenIDs)
		if err != nil {
			logging.Error(fmt.Sprintf("Failed to fetch orderbooks: %v", err))
		} else {
			logging.Info(fmt.Sprintf("Fetched %d orderbook snapshots", len(snapshots)))
			if err := upsertOrderbookSnapshots(ctx, db, snapshots); err != nil {
				logging.Error(fmt.Sprintf("Failed to upsert orderbook snapshots: %v", err))
			}
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

	now := time.Now()
	for _, m := range candidates {
		m.ComputedScore = ComputeScore(*m, maxVals)
		m.LastFetched = now
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].ComputedScore > candidates[j].ComputedScore
	})

	if len(candidates) > filter.MaxN {
		return candidates[:filter.MaxN]
	}

	return candidates
}

// ComputeScore – sports-tuned version (medium-short holding bias)
// Ranks Polymarket markets for opportunity (higher = more attractive)
// Focus: recent activity, tight execution, time-left sweet spot ~4–21 days
func ComputeScore(m services.PlyMktMarket, maxVals struct {
	MaxVol24hr    float64
	MaxLiquidity  float64
	MaxVol        float64
	MaxVolatility float64
},
) float64 {
	if !m.Active || m.Closed || len(m.ClobTokenIds) == 0 {
		return 0.0
	}

	// ────────────────────────────────────────────────
	// Weights – sports bias (tunable via env vars)
	// ────────────────────────────────────────────────
	wVol24hr := 0.45    // recent momentum/news reaction – core for sports
	wLiquidity := 0.25  // enter/exit without huge slippage
	wSpread := 0.15     // tight markets = better fills & lower cost
	wVolatility := 0.08 // repricing speed from news/line moves
	wTimeLeft := 0.07   // structural: avoid too short or too far

	// Env overrides (for testing / regime changes)
	if v := os.Getenv("SCORE_W_VOL24HR"); v != "" {
		wVol24hr, _ = strconv.ParseFloat(v, 64)
	}
	if v := os.Getenv("SCORE_W_LIQ"); v != "" {
		wLiquidity, _ = strconv.ParseFloat(v, 64)
	}
	if v := os.Getenv("SCORE_W_SPREAD"); v != "" {
		wSpread, _ = strconv.ParseFloat(v, 64)
	}
	if v := os.Getenv("SCORE_W_VOLA"); v != "" {
		wVolatility, _ = strconv.ParseFloat(v, 64)
	}
	if v := os.Getenv("SCORE_W_TIMELEFT"); v != "" {
		wTimeLeft, _ = strconv.ParseFloat(v, 64)
	}

	// ────────────────────────────────────────────────
	// Normalized features [0,1]
	// ────────────────────────────────────────────────
	vol24hrNorm := 0.0
	if maxVals.MaxVol24hr > 0 {
		vol24hrNorm = math.Min(m.Volume24hr/maxVals.MaxVol24hr, 1.0)
	}

	liq := m.LiquidityClob
	if liq <= 0 {
		liq, _ = strconv.ParseFloat(m.Liquidity, 64) // fallback
	}
	liqNorm := 0.0
	if maxVals.MaxLiquidity > 0 {
		liqNorm = math.Min(liq/maxVals.MaxLiquidity, 1.0)
	}

	volaNorm := 0.0
	volatility := math.Abs(m.OneDayPriceChange)
	if maxVals.MaxVolatility > 0 {
		volaNorm = math.Min(volatility/maxVals.MaxVolatility, 1.0)
	}

	// Spread penalty: harsh on wide spreads (sports can gap hard)
	spreadPenalty := 1.0
	if m.Spread > 0.003 { // penalize above ~0.3%
		spreadPenalty = math.Max(0.10, 1.0-math.Log1p(m.Spread*200)/math.Log1p(8))
		// ~0.5% spread → ~0.55 factor; 2% → ~0.18
	}

	// ────────────────────────────────────────────────
	// Time-left factor (key for sports – peaked window)
	// ────────────────────────────────────────────────
	timeLeftDays := 999.0 // default = no strong penalty
	if !m.EndDate.IsZero() {
		timeLeft := time.Until(m.EndDate).Hours() / 24
		if timeLeft > 0 {
			timeLeftDays = timeLeft
		} else {
			return 0.0 // already past resolution
		}
	}

	timeLeftFactor := 1.0
	if timeLeftDays < 3 {
		timeLeftFactor = math.Max(0.10, timeLeftDays/4.0) // sharp drop very close
	} else if timeLeftDays <= 21 {
		timeLeftFactor = 1.0 + 0.15*(21-timeLeftDays)/17 // slight peak near 10–15d
	} else if timeLeftDays > 45 {
		timeLeftFactor = math.Max(0.45, 1.0-(timeLeftDays-45)/90.0) // decay for long-dated
	}

	// ────────────────────────────────────────────────
	// Age / freshness (milder for sports – new info appears late)
	// ────────────────────────────────────────────────
	ageDays := 999.0
	startTime := m.EventStartTime // or GameStartTime for sports
	if m.GameStartTime != "" {
		parsed, err := time.Parse("2006-01-02 15:04:05-0700", m.GameStartTime)
		if err == nil {
			startTime = parsed
		}
	}
	if !startTime.IsZero() {
		ageDays = time.Since(startTime).Hours() / 24
	}
	ageFactor := 1.0
	if ageDays > 30 { // discount very old markets
		ageFactor = math.Max(0.60, 1.0-(ageDays-30)/90.0)
	}

	// ────────────────────────────────────────────────
	// Composite
	// ────────────────────────────────────────────────
	baseScore := (vol24hrNorm*wVol24hr +
		liqNorm*wLiquidity +
		volaNorm*wVolatility +
		spreadPenalty*wSpread +
		timeLeftFactor*wTimeLeft)

	score := baseScore * ageFactor

	// Clamp
	score = math.Max(0.0, math.Min(1.2, score)) // slight headroom

	return score
}

func fetchCategories(ctx context.Context, client *http.Client) ([]*services.PlyMktTag, error) {
	cats := []*services.PlyMktTag{}
	catsID := "102982"

	// Add the root tag so it exists in the database
	cats = append(cats, &services.PlyMktTag{
		ID:    catsID,
		Label: "Categories",
		Slug:  "categories",
	})

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
		tag.ParentTagID = catsID
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
			relatedTag.ParentTagID = tag.ID
		}

		cats = append(cats, related...)
	}

	logging.Info(fmt.Sprintf("Fetched %d categories", len(cats)))
	return cats, nil
}

// upsertTags inserts/updates tags into the tags table
func upsertTags(ctx context.Context, db *pgxpool.Pool, categories []*services.PlyMktTag) error {
	batch := &pgx.Batch{}

	sql := `
		INSERT INTO tags (
			id, label, slug, force_show, force_hide, parent_tag_id, total_vol, total_vol_24hr, total_liq, total_markets
		) VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''), $7, $8, $9, $10)
		ON CONFLICT (id) DO UPDATE SET
			label = EXCLUDED.label,
			slug = EXCLUDED.slug,
			force_show = EXCLUDED.force_show,
			force_hide = EXCLUDED.force_hide,
			parent_tag_id = EXCLUDED.parent_tag_id,
			total_vol = EXCLUDED.total_vol,
			total_vol_24hr = EXCLUDED.total_vol_24hr,
			total_liq = EXCLUDED.total_liq,
			total_markets = EXCLUDED.total_markets
	`

	tagCount := 0

	// First, queue all parent category tags
	if len(categories) > 0 {
		logging.Info(fmt.Sprintf("First tag: ID=%s, Label=%s, Slug=%s, ParentCategory=%s", categories[0].ID, categories[0].Label, categories[0].Slug, categories[0].ParentTagID))
	}
	for _, t := range categories {
		batch.Queue(sql,
			t.ID,
			t.Label,
			t.Slug,
			t.ForceShow,
			t.ForceHide,
			t.ParentTagID,
			t.TotalVol,
			t.TotalVol24hr,
			t.TotalLiq,
			t.TotalMarkets,
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
				logging.Error(fmt.Sprintf("Problematic tag: ID=%s, Label=%s, ParentCategory=%s", tag.ID, tag.Label, tag.ParentTagID))
			}
			return fmt.Errorf("error executing batch item %d: %w", i, err)
		}
	}

	if err := br.Close(); err != nil {
		return fmt.Errorf("error closing batch (possible constraint violation): %w", err)
	}

	logging.Info(fmt.Sprintf("Upserted %d tags to database", tagCount))
	return nil
}

// updateTagsWithAggregates updates tags with aggregated volume/liquidity/market data
func updateTagsWithAggregates(ctx context.Context, db *pgxpool.Pool, catStats map[string]*CategoryData) error {
	if len(catStats) == 0 {
		return nil
	}

	batch := &pgx.Batch{}

	sql := `
		UPDATE tags SET
			total_vol = $2,
			total_vol_24hr = $3,
			total_liq = $4,
			total_markets = $5,
			updated_at = NOW()
		WHERE id = $1
	`

	// Update top-level categories
	for _, catData := range catStats {
		if catData.PlyMktTag != nil {
			batch.Queue(sql,
				catData.PlyMktTag.ID,
				catData.TotalVol,
				catData.TotalVol24hr,
				catData.TotalLiq,
				catData.TotalMarkets,
			)
		}

		// Update subcategories
		for _, subData := range catData.RelatedTags {
			batch.Queue(sql,
				subData.PlyMktTag.ID,
				subData.TotalVol,
				subData.TotalVol24hr,
				subData.TotalLiq,
				subData.TotalMarkets,
			)
		}
	}

	br := db.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("failed to update tag aggregate %d: %w", i, err)
		}
	}

	logging.Info(fmt.Sprintf("Updated %d tags with aggregated data", batch.Len()))
	return nil
}

// fetchTagsFromDB reads top tags from the database and returns them along with the exact time of the most recently updated record.
func fetchTagsFromDB(ctx context.Context, db *pgxpool.Pool) ([]*services.PlyMktTag, time.Time) {
	var tags []*services.PlyMktTag
	var maxUpdatedAt time.Time

	// Graceful try: table might not exist
	query := `SELECT id, label, slug, force_show, force_hide, parent_tag_id, updated_at FROM tags WHERE parent_tag_id = '102982'`
	rows, err := db.Query(ctx, query)
	if err != nil {
		return nil, time.Time{}
	}
	defer rows.Close()

	for rows.Next() {
		var t services.PlyMktTag
		var parentID *string
		var updatedAt time.Time
		err := rows.Scan(&t.ID, &t.Label, &t.Slug, &t.ForceShow, &t.ForceHide, &parentID, &updatedAt)
		if err != nil {
			return nil, time.Time{}
		}
		if parentID != nil {
			t.ParentTagID = *parentID
		}
		if updatedAt.After(maxUpdatedAt) {
			maxUpdatedAt = updatedAt
		}
		tags = append(tags, &t)
	}

	return tags, maxUpdatedAt
}

// upsertPlyMktEvents inserts/updates events into the plymkt_events table
func upsertPlyMktEvents(ctx context.Context, db *pgxpool.Pool, events []*services.PlyMktEvent) error {
	if len(events) == 0 {
		return nil
	}

	batch := &pgx.Batch{}

	sql := `
		INSERT INTO plymkt_events (
			id, ticker, slug, title, description, start_date, end_date, category,
			image, icon, active, closed, archived, new, featured, restricted,
			liquidity, volume, volume_24hr, volume_1wk, volume_1mo, volume_1yr,
			liquidity_clob, competitive, neg_risk, neg_risk_market_id, comment_count,
			enable_order_book, series_slug, live, ended, creator_id,
			created_at, updated_at
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8,
			$9, $10, $11, $12, $13, $14, $15, $16,
			$17, $18, $19, $20, $21, $22,
			$23, $24, $25, $26, $27,
			$28, $29, $30, $31, $32,
			$33, $34
		)
		ON CONFLICT (id) DO UPDATE SET
			ticker = EXCLUDED.ticker,
			slug = EXCLUDED.slug,
			title = EXCLUDED.title,
			description = EXCLUDED.description,
			start_date = EXCLUDED.start_date,
			end_date = EXCLUDED.end_date,
			category = EXCLUDED.category,
			image = EXCLUDED.image,
			icon = EXCLUDED.icon,
			active = EXCLUDED.active,
			closed = EXCLUDED.closed,
			archived = EXCLUDED.archived,
			new = EXCLUDED.new,
			featured = EXCLUDED.featured,
			restricted = EXCLUDED.restricted,
			liquidity = EXCLUDED.liquidity,
			volume = EXCLUDED.volume,
			volume_24hr = EXCLUDED.volume_24hr,
			volume_1wk = EXCLUDED.volume_1wk,
			volume_1mo = EXCLUDED.volume_1mo,
			volume_1yr = EXCLUDED.volume_1yr,
			liquidity_clob = EXCLUDED.liquidity_clob,
			competitive = EXCLUDED.competitive,
			neg_risk = EXCLUDED.neg_risk,
			neg_risk_market_id = EXCLUDED.neg_risk_market_id,
			comment_count = EXCLUDED.comment_count,
			enable_order_book = EXCLUDED.enable_order_book,
			series_slug = EXCLUDED.series_slug,
			live = EXCLUDED.live,
			ended = EXCLUDED.ended,
			creator_id = EXCLUDED.creator_id,
			updated_at = EXCLUDED.updated_at;
	`

	for _, e := range events {
		batch.Queue(sql,
			e.ID,              // id
			e.Ticker,          // ticker
			e.Slug,            // slug
			e.Title,           // title
			e.Description,     // description
			e.StartDate,       // start_date
			e.EndDate,         // end_date
			e.Category,        // category
			e.Image,           // image
			e.Icon,            // icon
			e.Active,          // active
			e.Closed,          // closed
			e.Archived,        // archived
			e.New,             // new
			e.Featured,        // featured
			e.Restricted,      // restricted
			e.Liquidity,       // liquidity
			e.Volume,          // volume
			e.Volume24hr,      // volume_24hr
			e.Volume1wk,       // volume_1wk
			e.Volume1mo,       // volume_1mo
			e.Volume1yr,       // volume_1yr
			e.LiquidityClob,   // liquidity_clob
			e.Competitive,     // competitive
			e.NegRisk,         // neg_risk
			e.NegRiskMarketID, // neg_risk_market_id
			e.CommentCount,    // comment_count
			e.EnableOrderBook, // enable_order_book
			e.SeriesSlug,      // series_slug
			false,             // live (default)
			false,             // ended (default)
			e.CreatedBy,       // creator_id
			e.CreatedAt,       // created_at
			time.Now(),        // updated_at
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

// upsertMarkets upserts ranked markets into the plymkt_markets table
func upsertMarkets(ctx context.Context, db *pgxpool.Pool, markets []*services.PlyMktMarket, t time.Time, category string) error {
	if len(markets) == 0 {
		return nil
	}

	batch := &pgx.Batch{}

	sql := `
		INSERT INTO plymkt_markets (
			id, question, condition_id, slug, resolution_source, end_date,
			category, liquidity, sponsor_name, start_date, fee, image, icon,
			description, volume, active, market_type, closed, created_by, updated_by,
			created_at, updated_at, wide_format, new, featured, archived, restricted,
			question_id, enable_order_book, order_price_min_tick_size, order_min_size,
			volume_num, liquidity_num, volume_24hr, volume_1wk, volume_1mo, volume_1yr,
			clob_token_ids, fpmm_live, volume_24hr_amm, volume_1wk_amm, volume_1mo_amm,
			volume_1yr_amm, volume_24hr_clob, volume_1wk_clob, volume_1mo_clob,
			volume_1yr_clob, volume_amm, volume_clob, liquidity_amm, liquidity_clob,
			maker_base_fee, taker_base_fee, accepting_orders, notifications_enabled,
			score, creator, ready, funded, ready_timestamp, funded_timestamp,
			accepting_orders_timestamp, competitive, rewards_min_size, rewards_max_spread,
			spread, automatically_resolved, one_day_price_change, one_hour_price_change,
			one_week_price_change, one_month_price_change, one_year_price_change,
			last_trade_price, best_bid, best_ask, automatically_active, clear_book_on_start,
			manual_activation, neg_risk_other, game_id, sports_market_type,
			pending_deployment, deploying, rfq_enabled, event_start_time
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULLIF($11, '')::DOUBLE PRECISION, $12, $13, $14, $15, $16, $17,
			$18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32,
			$33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47,
			$48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62,
			$63, $64, $65, $66, $67, $68, $69, $70, $71, $72, $73, $74, $75, $76, $77,
			$78, $79, $80, $81, $82, $83, $84, $85
		)
		ON CONFLICT (id) DO UPDATE SET
			question = EXCLUDED.question,
			condition_id = EXCLUDED.condition_id,
			slug = EXCLUDED.slug,
			resolution_source = EXCLUDED.resolution_source,
			end_date = EXCLUDED.end_date,
			category = EXCLUDED.category,
			liquidity = EXCLUDED.liquidity,
			sponsor_name = EXCLUDED.sponsor_name,
			start_date = EXCLUDED.start_date,
			fee = EXCLUDED.fee,
			image = EXCLUDED.image,
			icon = EXCLUDED.icon,
			description = EXCLUDED.description,
			volume = EXCLUDED.volume,
			active = EXCLUDED.active,
			market_type = EXCLUDED.market_type,
			closed = EXCLUDED.closed,
			updated_by = EXCLUDED.updated_by,
			updated_at = EXCLUDED.updated_at,
			wide_format = EXCLUDED.wide_format,
			new = EXCLUDED.new,
			featured = EXCLUDED.featured,
			archived = EXCLUDED.archived,
			restricted = EXCLUDED.restricted,
			question_id = EXCLUDED.question_id,
			enable_order_book = EXCLUDED.enable_order_book,
			order_price_min_tick_size = EXCLUDED.order_price_min_tick_size,
			order_min_size = EXCLUDED.order_min_size,
			volume_num = EXCLUDED.volume_num,
			liquidity_num = EXCLUDED.liquidity_num,
			volume_24hr = EXCLUDED.volume_24hr,
			volume_1wk = EXCLUDED.volume_1wk,
			volume_1mo = EXCLUDED.volume_1mo,
			volume_1yr = EXCLUDED.volume_1yr,
			clob_token_ids = EXCLUDED.clob_token_ids,
			fpmm_live = EXCLUDED.fpmm_live,
			volume_24hr_amm = EXCLUDED.volume_24hr_amm,
			volume_1wk_amm = EXCLUDED.volume_1wk_amm,
			volume_1mo_amm = EXCLUDED.volume_1mo_amm,
			volume_1yr_amm = EXCLUDED.volume_1yr_amm,
			volume_24hr_clob = EXCLUDED.volume_24hr_clob,
			volume_1wk_clob = EXCLUDED.volume_1wk_clob,
			volume_1mo_clob = EXCLUDED.volume_1mo_clob,
			volume_1yr_clob = EXCLUDED.volume_1yr_clob,
			volume_amm = EXCLUDED.volume_amm,
			volume_clob = EXCLUDED.volume_clob,
			liquidity_amm = EXCLUDED.liquidity_amm,
			liquidity_clob = EXCLUDED.liquidity_clob,
			maker_base_fee = EXCLUDED.maker_base_fee,
			taker_base_fee = EXCLUDED.taker_base_fee,
			accepting_orders = EXCLUDED.accepting_orders,
			notifications_enabled = EXCLUDED.notifications_enabled,
			score = EXCLUDED.score,
			creator = EXCLUDED.creator,
			ready = EXCLUDED.ready,
			funded = EXCLUDED.funded,
			ready_timestamp = EXCLUDED.ready_timestamp,
			funded_timestamp = EXCLUDED.funded_timestamp,
			accepting_orders_timestamp = EXCLUDED.accepting_orders_timestamp,
			competitive = EXCLUDED.competitive,
			rewards_min_size = EXCLUDED.rewards_min_size,
			rewards_max_spread = EXCLUDED.rewards_max_spread,
			spread = EXCLUDED.spread,
			automatically_resolved = EXCLUDED.automatically_resolved,
			one_day_price_change = EXCLUDED.one_day_price_change,
			one_hour_price_change = EXCLUDED.one_hour_price_change,
			one_week_price_change = EXCLUDED.one_week_price_change,
			one_month_price_change = EXCLUDED.one_month_price_change,
			one_year_price_change = EXCLUDED.one_year_price_change,
			last_trade_price = EXCLUDED.last_trade_price,
			best_bid = EXCLUDED.best_bid,
			best_ask = EXCLUDED.best_ask,
			automatically_active = EXCLUDED.automatically_active,
			clear_book_on_start = EXCLUDED.clear_book_on_start,
			manual_activation = EXCLUDED.manual_activation,
			neg_risk_other = EXCLUDED.neg_risk_other,
			game_id = EXCLUDED.game_id,
			sports_market_type = EXCLUDED.sports_market_type,
			pending_deployment = EXCLUDED.pending_deployment,
			deploying = EXCLUDED.deploying,
			rfq_enabled = EXCLUDED.rfq_enabled,
			event_start_time = EXCLUDED.event_start_time
	`

	for _, m := range markets {
		m.Category = category
		batch.Queue(sql,
			m.ID, m.Question, m.ConditionID, m.Slug, m.ResolutionSource, m.EndDate,
			m.Category, m.Liquidity, m.SponsorName, m.StartDate, m.Fee, m.Image, m.Icon,
			m.Description, m.Volume, m.Active, m.MarketType, m.Closed, m.CreatedBy, m.UpdatedBy,
			m.CreatedAt, t, m.WideFormat, m.New, m.Featured, m.Archived, m.Restricted,
			m.QuestionID, m.EnableOrderBook, m.OrderPriceMinTickSize, m.OrderMinSize,
			m.VolumeNum, m.LiquidityNum, m.Volume24hr, m.Volume1wk, m.Volume1mo, m.Volume1yr,
			m.ClobTokenIds, m.FpmmLive, m.Volume24hrAmm, m.Volume1wkAmm, m.Volume1moAmm,
			m.Volume1yrAmm, m.Volume24hrClob, m.Volume1wkClob, m.Volume1moClob,
			m.Volume1yrClob, m.VolumeAmm, m.VolumeClob, m.LiquidityAmm, m.LiquidityClob,
			m.MakerBaseFee, m.TakerBaseFee, m.AcceptingOrders, m.NotificationsEnabled,
			m.Score, m.Creator, m.Ready, m.Funded, m.ReadyTimestamp, m.FundedTimestamp,
			m.AcceptingOrdersTimestamp, m.Competitive, m.RewardsMinSize, m.RewardsMaxSpread,
			m.Spread, m.AutomaticallyResolved, m.OneDayPriceChange, m.OneHourPriceChange,
			m.OneWeekPriceChange, m.OneMonthPriceChange, m.OneYearPriceChange,
			m.LastTradePrice, m.BestBid, m.BestAsk, m.AutomaticallyActive, m.ClearBookOnStart,
			m.ManualActivation, m.NegRiskOther, m.GameID, m.SportsMarketType,
			m.PendingDeployment, m.Deploying, m.RfqEnabled, m.EventStartTime,
		)
	}

	br := db.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < len(markets); i++ {
		_, err := br.Exec()
		if err != nil {
			return fmt.Errorf("error executing batch item %d: %w", i, err)
		}
	}

	logging.Info(fmt.Sprintf("Upserted %d markets to plymkt_markets (category=%s)", len(markets), category))
	return nil
}

// upsertHotEvents inserts events into the hot_events hypertable
func upsertHotEvents(ctx context.Context, db *pgxpool.Pool, events []*services.PlyMktEvent, t time.Time) error {
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
func LogMarketStats(ranked []*services.PlyMktMarket, totalMkts int, cat string, fetchTime time.Time) {
	if len(ranked) == 0 {
		logging.Info(fmt.Sprintf("[%s] MARKET STATS %s] No markets passed filters (raw events fetched: %d)", cat, fetchTime.Format(time.RFC3339), totalMkts))
		return
	}

	now := time.Now()
	logging.Info(fmt.Sprintf("[%s] MARKET STATS %s] Selected %d total markets (from %d raw events) | Duration: %v",
		cat, now.Format(time.RFC3339), len(ranked), totalMkts, now.Sub(fetchTime)))

	printAggregateStats := func(selected []*services.PlyMktMarket, prefix string) {
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
		avgSpread := medianFloat(spreads)

		sort.Float64s(volumes24hr)
		sort.Float64s(liquidities)
		sort.Float64s(spreads)

		medVol24hr := medianFloat(volumes24hr)
		medLiq := medianFloat(liquidities)
		p90Vol24hr := percentileFloat(volumes24hr, 0.90)
		p90Liq := percentileFloat(liquidities, 0.90)

		logging.Info(fmt.Sprintf("%s Count: %d", prefix, len(selected)))
		logging.Info(fmt.Sprintf("%s Total 24h Volume: $%.2fM", prefix, totalVol24hr/1e6))
		logging.Info(fmt.Sprintf("%s Total Liquidity: $%.2fM", prefix, totalLiquidity/1e6))
		logging.Info(fmt.Sprintf("%s Averages: Vol24hr $%.0f | Liquidity $%.0f | Lifetime Vol $%.0f | Volatility %.2f%% | Spread %.3f", prefix, avgVol24hr, avgLiq, avgVol, avgVolatility*100, avgSpread))
		logging.Info(fmt.Sprintf("%s Medians: Vol24hr $%.0f | Liquidity $%.0f", prefix, medVol24hr, medLiq))
		logging.Info(fmt.Sprintf("%s P90: Vol24hr $%.0f | Liquidity $%.0f", prefix, p90Vol24hr, p90Liq))
		logging.Info(fmt.Sprintf("%s Min/Max Vol24hr: $%.0f / $%.2fM", prefix, volumes24hr[0], volumes24hr[len(volumes24hr)-1]/1e6))
	}

	// Overall stats for this category
	logging.Info("  --- OVERALL STATS ---")
	printAggregateStats(ranked, fmt.Sprintf("    [%s]", cat))

	// 3. Overall Top 5 by Volume24hr
	logging.Info("  --- TOP 5 MARKETS OVERALL (by 24h volume) ---")
	sort.Slice(ranked, func(i, j int) bool { return ranked[i].Volume24hr > ranked[j].Volume24hr })
	for i := 0; i < min(5, len(ranked)); i++ {
		m := ranked[i]
		liq := m.LiquidityClob
		if liq == 0 {
			liq = m.LiquidityNum
		}
		logging.Info(fmt.Sprintf("    #%d [%s]: %s | Vol24hr $%.2fM | Liq $%.2fM | Spread %.3f | Question: %.80s...",
			i+1, cat, m.ID, m.Volume24hr/1e6, liq/1e6, m.Spread, m.Question))
	}

	logging.Info("--- End MARKET STATS ---")
}

func logCategoryStats(catStats map[string]*CategoryData) {
	logging.Info("=== CATEGORY VOLUME SUMMARY ===")

	// Sort categories by total volume descending
	type catEntry struct {
		slug string
		data *CategoryData
	}
	var cats []catEntry
	for slug, data := range catStats {
		cats = append(cats, catEntry{slug, data})
	}
	sort.Slice(cats, func(i, j int) bool {
		return cats[i].data.TotalVol > cats[j].data.TotalVol
	})

	for _, catEntry := range cats {
		cat := catEntry.data
		logging.Info(fmt.Sprintf("  [%s] Vol: $%.1fM | Vol24hr: $%.1fM | Liq: $%.1fM | Markets: %d",
			catEntry.slug,
			cat.TotalVol/1e6,
			cat.TotalVol24hr/1e6,
			cat.TotalLiq/1e6,
			cat.TotalMarkets))

		// Sort subcategories by volume descending
		type subEntry struct {
			slug string
			data *CategoryData
		}
		var subs []subEntry
		for _, data := range cat.RelatedTags {
			subSlug := data.PlyMktTag.Slug
			if data.PlyMktTag.Label != "" {
				subSlug = data.PlyMktTag.Label
			}
			subs = append(subs, subEntry{subSlug, data})
		}
		sort.Slice(subs, func(i, j int) bool {
			return subs[i].data.TotalVol > subs[j].data.TotalVol
		})

		for _, sub := range subs {
			logging.Info(fmt.Sprintf("    -> [%s] Vol: $%.1fM | Vol24hr: $%.1fM | Markets: %d",
				sub.slug,
				sub.data.TotalVol/1e6,
				sub.data.TotalVol24hr/1e6,
				sub.data.TotalMarkets))
		}
	}
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

// =============================================================================
// Market Data Fetchers & Upsert Functions
// =============================================================================

// --- Open Interest ---

type oiResponse struct {
	Market string  `json:"market"`
	Value  float64 `json:"value"`
}

// fetchOpenInterest fetches open interest for the given conditionIDs in batches,
// using FetchPaginated for each batch chunk.
func fetchOpenInterest(client *http.Client, conditionIDs []string) ([]*oiResponse, error) {
	const batchSize = 50
	var allOI []*oiResponse

	for i := 0; i < len(conditionIDs); i += batchSize {
		end := i + batchSize
		if end > len(conditionIDs) {
			end = len(conditionIDs)
		}
		chunk := conditionIDs[i:end]

		u, _ := url.Parse("https://data-api.polymarket.com/oi")
		q := u.Query()
		for _, id := range chunk {
			q.Add("market", id)
		}
		u.RawQuery = q.Encode()

		// OI endpoint returns all results at once, no real pagination needed
		batch, err := FetchPaginated[oiResponse](client, u, 500, 0)
		if err != nil {
			return allOI, fmt.Errorf("OI fetch error: %w", err)
		}
		allOI = append(allOI, batch...)
	}
	return allOI, nil
}

// --- Trades ---

// fetchTrades fetches trades for the given conditionIDs in batches,
// using FetchPaginated for each batch chunk.
func fetchTrades(client *http.Client, conditionIDs []string) ([]*services.PlyMktTrade, error) {
	const batchSize = 20
	var allTrades []*services.PlyMktTrade

	for i := 0; i < len(conditionIDs); i += batchSize {
		end := i + batchSize
		if end > len(conditionIDs) {
			end = len(conditionIDs)
		}
		chunk := conditionIDs[i:end]

		u, _ := url.Parse("https://data-api.polymarket.com/trades")
		q := u.Query()
		q.Set("takerOnly", "true")
		for _, id := range chunk {
			q.Add("market", id)
		}
		u.RawQuery = q.Encode()

		batch, err := FetchPaginated[services.PlyMktTrade](client, u, 100, 3000)
		if err != nil {
			logging.Error(fmt.Sprintf("trades fetch error for chunk %d: %v", i/batchSize, err))
			continue // skip this chunk, try the rest
		}
		allTrades = append(allTrades, batch...)
		time.Sleep(250 * time.Millisecond)
	}
	return allTrades, nil
}

func upsertTrades(ctx context.Context, db *pgxpool.Pool, trades []*services.PlyMktTrade) error {
	if len(trades) == 0 {
		return nil
	}

	// Create table if not exists
	createSQL := `CREATE TABLE IF NOT EXISTS trades (
		transaction_hash TEXT PRIMARY KEY,
		proxy_wallet     TEXT,
		side             TEXT,
		asset            TEXT,
		condition_id     TEXT,
		size             INTEGER,
		price            INTEGER,
		timestamp        INTEGER,
		title            TEXT,
		slug             TEXT,
		icon             TEXT,
		event_slug       TEXT,
		outcome          TEXT,
		outcome_index    INTEGER,
		name             TEXT,
		pseudonym        TEXT,
		created_at       TIMESTAMPTZ DEFAULT NOW()
	)`
	if _, err := db.Exec(ctx, createSQL); err != nil {
		return fmt.Errorf("failed to create trades table: %w", err)
	}

	batch := &pgx.Batch{}
	sql := `INSERT INTO trades (transaction_hash, proxy_wallet, side, asset, condition_id, size, price, timestamp, title, slug, icon, event_slug, outcome, outcome_index, name, pseudonym)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
		ON CONFLICT (transaction_hash) DO NOTHING`

	for _, t := range trades {
		batch.Queue(sql,
			t.TransactionHash, t.ProxyWallet, t.Side, t.Asset, t.ConditionId,
			t.Size, t.Price, t.Timestamp, t.Title, t.Slug, t.Icon, t.EventSlug,
			t.Outcome, t.OutcomeIndex, t.Name, t.Pseudonym,
		)
	}

	br := db.SendBatch(ctx, batch)
	defer br.Close()
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("trades batch item %d error: %w", i, err)
		}
	}
	logging.Info(fmt.Sprintf("Upserted %d trades", len(trades)))
	return nil
}

// --- Prices History ---

type clobToken struct {
	TokenID  string
	MarketID string
}

// fetchPricesHistoryConcurrent fetches prices-history for all tokens concurrently
// with a semaphore to limit concurrency.
// startTs controls the lookback: 0 = full 30-day backfill, >0 = incremental from that timestamp.
func fetchPricesHistoryConcurrent(client *http.Client, tokens []clobToken, fidelity int, startTs int64) []services.PlyMktPriceHistory {
	var mu sync.Mutex
	var allPrices []services.PlyMktPriceHistory
	sem := make(chan struct{}, 10) // max 10 concurrent requests
	var wg sync.WaitGroup
	now := time.Now().Unix()

	// First run (startTs=0): full 30-day backfill
	// Subsequent runs: from startTs minus 1-hour buffer for overlap safety
	fetchFrom := now - 30*24*60*60
	if startTs > 0 {
		fetchFrom = startTs - 3600 // 1-hour overlap buffer
	}

	for _, tok := range tokens {
		wg.Add(1)
		go func(t clobToken) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			u, _ := url.Parse("https://clob.polymarket.com/prices-history")
			q := u.Query()
			q.Set("market", t.TokenID)
			q.Set("interval", "max")
			q.Set("fidelity", strconv.Itoa(fidelity))
			q.Set("startTs", strconv.FormatInt(fetchFrom, 10))
			u.RawQuery = q.Encode()

			resp, err := client.Get(u.String())
			if err != nil {
				logging.Error(fmt.Sprintf("prices-history fetch error for token %s: %v", t.TokenID[:16], err))
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				logging.Error(fmt.Sprintf("prices-history bad status %d for token %s", resp.StatusCode, t.TokenID[:16]))
				return
			}

			var wrapper struct {
				History []services.PlyMktPriceHistory `json:"history"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&wrapper); err != nil {
				logging.Error(fmt.Sprintf("prices-history decode error for token %s: %v", t.TokenID[:16], err))
				return
			}

			// Enrich with metadata
			for i := range wrapper.History {
				wrapper.History[i].TokenID = t.TokenID
				wrapper.History[i].MarketID = t.MarketID
				wrapper.History[i].Fidelity = fidelity
				wrapper.History[i].UpdatedAt = now
			}

			mu.Lock()
			allPrices = append(allPrices, wrapper.History...)
			mu.Unlock()
		}(tok)
	}
	wg.Wait()
	return allPrices
}

func upsertPricesHistory(ctx context.Context, db *pgxpool.Pool, prices []services.PlyMktPriceHistory) error {
	if len(prices) == 0 {
		return nil
	}

	// Ensure table has the right columns (idempotent migration)
	createSQL := `CREATE TABLE IF NOT EXISTS prices_history (
		token_id     TEXT         NOT NULL,
		timestamp    BIGINT       NOT NULL,
		price        DOUBLE PRECISION NOT NULL,
		market_id    TEXT,
		fidelity_min INTEGER,
		updated_at   BIGINT,
		PRIMARY KEY (token_id, timestamp)
	)`
	if _, err := db.Exec(ctx, createSQL); err != nil {
		return fmt.Errorf("failed to ensure prices_history table: %w", err)
	}

	sql := `INSERT INTO prices_history (token_id, timestamp, price, market_id, fidelity_min, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6)
		ON CONFLICT (token_id, timestamp) DO UPDATE SET price = EXCLUDED.price, updated_at = EXCLUDED.updated_at`

	// Send in sub-batches if large
	const maxBatch = 500
	for i := 0; i < len(prices); i += maxBatch {
		end := i + maxBatch
		if end > len(prices) {
			end = len(prices)
		}
		subBatch := &pgx.Batch{}
		for j := i; j < end; j++ {
			p := prices[j]
			subBatch.Queue(sql, p.TokenID, p.Timestamp, p.Price, p.MarketID, p.Fidelity, p.UpdatedAt)
		}
		br := db.SendBatch(ctx, subBatch)
		for k := 0; k < subBatch.Len(); k++ {
			if _, err := br.Exec(); err != nil {
				br.Close()
				return fmt.Errorf("prices_history batch item %d error: %w", i+k, err)
			}
		}
		br.Close()
	}

	logging.Info(fmt.Sprintf("Upserted %d prices_history rows", len(prices)))
	return nil
}

// --- Orderbooks ---

type orderbookSnapshot struct {
	Time          time.Time
	MarketID      string
	TokenID       string
	BestBid       float64
	BestAsk       float64
	Imbalance     float64
	TotalBidDepth float64
	TotalAskDepth float64
	DepthJSON     []byte // marshalled bids+asks
	RawJSON       []byte // full response
	NegRisk       bool
	Timestamp     string
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

// Build POST body: [{"token_id": "..."}, ...]
type tokenReq struct {
	TokenID string `json:"token_id"`
}

func fetchOrderbooks(client *http.Client, tokenIDs []string) ([]orderbookSnapshot, error) {
	const batchSize = 50
	var allSnapshots []orderbookSnapshot
	now := time.Now()

	for i := 0; i < len(tokenIDs); i += batchSize {
		end := i + batchSize
		if end > len(tokenIDs) {
			end = len(tokenIDs)
		}
		chunk := tokenIDs[i:end]

		var payload []tokenReq
		for _, tid := range chunk {
			payload = append(payload, tokenReq{TokenID: tid})
		}
		bodyBytes, _ := json.Marshal(payload)

		req, _ := http.NewRequest("POST", "https://clob.polymarket.com/books", bytes.NewReader(bodyBytes))
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return allSnapshots, fmt.Errorf("orderbook fetch error: %w", err)
		}

		var books []orderbookResponse
		if err := json.NewDecoder(resp.Body).Decode(&books); err != nil {
			resp.Body.Close()
			return allSnapshots, fmt.Errorf("orderbook decode error: %w", err)
		}
		resp.Body.Close()

		for _, book := range books {
			snap := orderbookSnapshot{
				Time:      now,
				TokenID:   book.AssetID,
				MarketID:  book.MarketID,
				NegRisk:   book.NegRisk,
				Timestamp: book.Timestamp,
			}

			// Compute best_bid, total_bid_depth
			for _, bid := range book.Bids {
				price, _ := strconv.ParseFloat(bid.Price, 64)
				size, _ := strconv.ParseFloat(bid.Size, 64)
				snap.TotalBidDepth += size
				if price > snap.BestBid {
					snap.BestBid = price
				}
			}

			// Compute best_ask, total_ask_depth
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

			// Compute imbalance
			totalDepth := snap.TotalBidDepth + snap.TotalAskDepth
			if totalDepth > 0 {
				snap.Imbalance = snap.TotalBidDepth / totalDepth
			}

			// Serialize depth + raw
			depthData := map[string]interface{}{
				"bids": book.Bids,
				"asks": book.Asks,
			}
			snap.DepthJSON, _ = json.Marshal(depthData)
			snap.RawJSON, _ = json.Marshal(book)

			allSnapshots = append(allSnapshots, snap)
		}
		time.Sleep(200 * time.Millisecond)
	}
	return allSnapshots, nil
}

func upsertOrderbookSnapshots(ctx context.Context, db *pgxpool.Pool, snapshots []orderbookSnapshot) error {
	if len(snapshots) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	sql := `INSERT INTO orderbook_snapshots (time, market_id, token_id, best_bid, best_ask, imbalance, total_bid_depth, total_ask_depth, depth_json, raw_response_json)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (time, market_id, token_id) DO UPDATE SET
			best_bid = EXCLUDED.best_bid,
			best_ask = EXCLUDED.best_ask,
			imbalance = EXCLUDED.imbalance,
			total_bid_depth = EXCLUDED.total_bid_depth,
			total_ask_depth = EXCLUDED.total_ask_depth,
			depth_json = EXCLUDED.depth_json,
			raw_response_json = EXCLUDED.raw_response_json`

	for _, s := range snapshots {
		batch.Queue(sql,
			s.Time, s.MarketID, s.TokenID,
			s.BestBid, s.BestAsk, s.Imbalance,
			s.TotalBidDepth, s.TotalAskDepth,
			s.DepthJSON, s.RawJSON,
		)
	}

	br := db.SendBatch(ctx, batch)
	defer br.Close()
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("orderbook_snapshots batch item %d error: %w", i, err)
		}
	}
	logging.Info(fmt.Sprintf("Upserted %d orderbook snapshots", len(snapshots)))
	return nil
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
			bodyBytes, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return nil, fmt.Errorf("bad status: %s, body: %s, url: %s", resp.Status, string(bodyBytes), reqURL.String())
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

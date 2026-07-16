package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/marketranking"
	"github.com/samucap/poly-asian-data/internal/pipeline"
	"github.com/samucap/poly-asian-data/internal/saver"
	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/samucap/poly-asian-data/internal/tagagg"
)

// Categories root on Polymarket; top tags have parent_tag_id = this.
const topCategoriesParentTagID = "102982"

// Gamma keyset max page size.
const gammaKeysetLimit = 500

type clobToken struct {
	TokenID  string
	MarketID string
}

var lastPriceFetchTs int64

func main() {
	logging.Init(os.Getenv("ENV"))

	cfg, err := config.Load()
	if err != nil {
		logging.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	logging.Init(cfg.ENV)
	logger := logging.Logger

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	factory, err := pipeline.NewFactory(ctx, cfg, logger)
	if err != nil {
		logger.Error("failed to create pipeline factory", slog.Any("error", err))
		os.Exit(1)
	}
	defer factory.Close()

	pipe, err := factory.Create(ctx, pipeline.Options{Name: "top-markets"})
	if err != nil {
		logger.Error("failed to create pipeline", slog.Any("error", err))
		os.Exit(1)
	}
	defer pipe.Stop()

	dbPool := factory.DB()
	// TODO: abstract secure HTTP client dial into a modular reusable constructor.
	httpClient := fetcher.NewSecureHTTPClient()

	defaultFilter := marketranking.MarketFilter{
		MinVolume24hr: cfg.TopMarkets.MinVolume24hr,
		MinLiquidity:  cfg.TopMarkets.MinLiquidity,
		MaxSpread:     cfg.TopMarkets.MaxSpread,
		MinVolatility: cfg.TopMarkets.MinVolatility,
		MaxN:          cfg.TopMarkets.MaxN,
	}
	fetchInterval := cfg.TopMarkets.RefreshInterval

	// Catalog: top tags under 102982. Prefer DB; API only when empty.
	// Metric aggregates are written later after the market pass — not here.
	// TODO(later): explore related-tags for tags outside root 102982 if taxonomy gaps appear.
	// TODO(later): event/market tags as FK arrays in Postgres.
	topCats := loadTopTags(ctx, logger, pipe, dbPool, httpClient, cfg)
	topTagByID := tagagg.TopByID(topCats, topCategoriesParentTagID)
	logger.Info("top tags ready", "count", len(topTagByID))

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	runCycle := func() {
		logger.Info("market refresh cycle starting")
		start := time.Now()
		statsBefore := pipe.Stats()

		// 1. Keyset-fetch all open events (no offset; cursor only).
		gammaBase, _ := cfg.Services.PlyMkt.Endpoints["gamma"].(string)
		if gammaBase == "" {
			gammaBase = "https://gamma-api.polymarket.com"
		}
		u, err := url.Parse(gammaBase + "/events/keyset")
		if err != nil {
			logger.Error("failed to parse gamma keyset URL", "error", err)
			return
		}
		// Documented keyset params only — omit offset (callers must not set it).
		q := u.Query()
		q.Set("closed", "false")
		q.Set("active", "true")
		q.Set("order", "volume24hr")
		q.Set("ascending", "false")
		q.Set("include_chat", "false")
		u.RawQuery = q.Encode()

		events, err := fetcher.FetchPaginatedKeyset[services.PlyMktEvent](ctx, httpClient, u, gammaKeysetLimit, 0)
		if err != nil {
			logger.Error("failed to fetch events (keyset)", "error", err)
			return
		}
		logger.Info("events fetched", "count", logging.FormatCount(int64(len(events))))

		// 2. Classify + aggregate once per tradable market onto top category only.
		agg := tagagg.Aggregate(events, topTagByID)

		// 3. Global rank over tradable pool.
		ranked := marketranking.RankMarkets(agg.Tradable, defaultFilter)

		var conditionIDs []string
		var clobTokens []clobToken
		seenCond := map[string]bool{}
		for _, m := range ranked {
			if m == nil {
				continue
			}
			if m.ConditionID != "" && !seenCond[m.ConditionID] {
				seenCond[m.ConditionID] = true
				conditionIDs = append(conditionIDs, m.ConditionID)
			}
			var tokenIDs []string
			if err := json.Unmarshal([]byte(m.ClobTokenIds), &tokenIDs); err == nil {
				for _, tid := range tokenIDs {
					clobTokens = append(clobTokens, clobToken{TokenID: tid, MarketID: m.ConditionID})
				}
			}
		}

		// 4. Enrichment then merge OI into save-side markets (and ranked).
		submitEnrichment(ctx, pipe, cfg, conditionIDs, clobTokens)
		pipe.WaitUntilIdle(ctx, 500*time.Millisecond)

		mergedOI := pipe.TakeMergedOI()
		for cond, oi := range mergedOI {
			if m, ok := agg.CondMarket[cond]; ok && m != nil {
				m.OpenInterest = oi
			}
		}
		for _, m := range ranked {
			if m == nil || m.ConditionID == "" {
				continue
			}
			if sm, ok := agg.CondMarket[m.ConditionID]; ok && sm != nil {
				m.OpenInterest = sm.OpenInterest
			}
		}

		// 5. Persist category aggregates after scan (zeros for idle top tags), then events/markets.
		if err := db.UpdateTagAggregates(ctx, dbPool, tagagg.ToTagAggregates(agg.Categories, topTagByID)); err != nil {
			logger.Error("failed to update tag aggregates", "error", err)
		}

		eventsSlice := make([]services.PlyMktEvent, 0, len(events))
		for _, e := range events {
			if e != nil {
				eventsSlice = append(eventsSlice, *e)
			}
		}
		if len(eventsSlice) > 0 {
			if err := pipe.SubmitSave(ctx, &saver.Record{
				TableName: "plymkt_events",
				Data:      eventsSlice,
				ItemCount: len(eventsSlice),
			}); err != nil {
				logger.Error("failed to submit events save", "error", err)
			}
		}
		if len(agg.Markets) > 0 {
			if err := pipe.SubmitSave(ctx, &saver.Record{
				TableName: "plymkt_markets",
				Data:      agg.Markets,
				ItemCount: len(agg.Markets),
			}); err != nil {
				logger.Error("failed to submit markets save", "error", err)
			}
		}

		pipe.WaitUntilIdle(ctx, 500*time.Millisecond)

		duration := time.Since(start)
		nextAt := time.Now().Add(fetchInterval)
		logger.Info("cycle complete",
			"duration", logging.FormatDuration(duration),
			"next_in", logging.FormatDuration(fetchInterval),
			"next_at", nextAt.Format("15:04:05"),
		)
		// Direct keyset HTTP is outside the fetcher worker pool — stage report is enrichment+saves only.
		pipe.LogStageReport("cycle", pipeline.DiffStats(statsBefore, pipe.Stats()))
		logCycleOverview(logger, events, agg, ranked)
	}

	runCycle()

	ticker := time.NewTicker(fetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Shutting down periodic refresh loop...")
			return
		case <-ticker.C:
			runCycle()
		case <-quit:
			logger.Info("Received stop signal. Stopping...")
			cancel()
			return
		}
	}
}

// loadTopTags loads top tags under 102982. Freshness uses pipeline watermark
// (sync_state key top_markets.tag_catalog), NOT tags.updated_at (polluted by aggregates).
// When watermark is missing/stale (>24h), re-fetch related-tags and refresh definitions/hierarchy.
func loadTopTags(
	ctx context.Context,
	logger *slog.Logger,
	pipe *pipeline.Pipeline,
	dbPool db.DBInterface,
	httpClient *http.Client,
	cfg *config.Config,
) []*services.PlyMktTag {
	dbTags, _, err := db.FetchTopCategoryTags(ctx, dbPool, topCategoriesParentTagID)
	if err != nil {
		logger.Warn("failed to load top tags from DB", "error", err)
		dbTags = nil
	}

	wm, hasWM, err := db.GetWatermark(ctx, dbPool, db.WatermarkTopMarketsTagCatalog)
	if err != nil {
		logger.Warn("failed to read tag catalog watermark; will refresh from API", "error", err)
		hasWM = false
	}
	needsRefresh := db.CatalogNeedsRefresh(wm, hasWM, time.Now(), db.DefaultTagCatalogTTL)

	if len(dbTags) > 0 && !needsRefresh {
		logger.Info("loaded top tags from database",
			"count", len(dbTags),
			"catalog_age", logging.FormatDuration(time.Since(wm).Round(time.Minute)),
		)
		return dbTags
	}

	if needsRefresh && len(dbTags) > 0 {
		logger.Info("tag catalog watermark stale or missing; refreshing related-tags from API",
			"db_count", len(dbTags),
			"has_watermark", hasWM,
		)
	} else {
		logger.Info("no top tags in DB; fetching from API")
	}

	cats, err := fetchTopCategories(ctx, httpClient, cfg)
	if err != nil {
		if len(dbTags) > 0 {
			logger.Error("failed to refresh top categories; using stale DB catalog", "error", err)
			return dbTags
		}
		logger.Error("failed to fetch top categories", "error", err)
		os.Exit(1)
	}

	var tagsSlice []services.PlyMktTag
	for _, t := range cats {
		if t != nil {
			tagsSlice = append(tagsSlice, *t)
		}
	}
	if len(tagsSlice) > 0 {
		// Definitions/hierarchy only — metric aggregates written after market scan.
		if err := pipe.SubmitSave(ctx, &saver.Record{
			TableName: "tags_definitions",
			Data:      tagsSlice,
			ItemCount: len(tagsSlice),
		}); err != nil {
			logger.Error("failed to submit tags save", "error", err)
		} else {
			pipe.WaitUntilIdle(ctx, 200*time.Millisecond)
		}
		if err := pipe.SubmitSave(ctx, &saver.Record{
			TableName: "tags_hierarchy",
			Data:      tagsSlice,
			ItemCount: len(tagsSlice),
		}); err != nil {
			logger.Error("failed to submit tags hierarchy save", "error", err)
		} else {
			pipe.WaitUntilIdle(ctx, 200*time.Millisecond)
		}
	}

	if err := db.SetWatermark(ctx, dbPool, db.WatermarkTopMarketsTagCatalog, time.Now().UTC()); err != nil {
		logger.Warn("failed to set tag catalog watermark", "error", err)
	} else {
		logger.Info("tag catalog watermark updated", "key", db.WatermarkTopMarketsTagCatalog)
	}
	return cats
}

func endpoint(cfg *config.Config, key, fallback string) string {
	if cfg != nil {
		if v, ok := cfg.Services.PlyMkt.Endpoints[key].(string); ok && v != "" {
			return v
		}
	}
	return fallback
}

func submitEnrichment(ctx context.Context, pipe *pipeline.Pipeline, cfg *config.Config, conditionIDs []string, clobTokens []clobToken) {
	dataAPI := endpoint(cfg, "data_api", "https://data-api.polymarket.com")
	clobAPI := endpoint(cfg, "clob", "https://clob.polymarket.com")

	for _, id := range conditionIDs {
		if id == "" {
			continue
		}
		u, _ := url.Parse(dataAPI + "/oi")
		q := u.Query()
		q.Set("market", id)
		u.RawQuery = q.Encode()
		_ = pipe.SubmitFetch(ctx, &fetcher.Request{
			URL:    u.String(),
			Method: "GET",
			Metadata: map[string]string{
				"Entity":  "top_markets_oi",
				"MergeOI": "true",
			},
		})
	}

	const tradesBatchSize = 20
	for i := 0; i < len(conditionIDs); i += tradesBatchSize {
		end := i + tradesBatchSize
		if end > len(conditionIDs) {
			end = len(conditionIDs)
		}
		chunk := conditionIDs[i:end]
		u, _ := url.Parse(dataAPI + "/trades")
		q := u.Query()
		q.Set("takerOnly", "true")
		for _, id := range chunk {
			q.Add("market", id)
		}
		u.RawQuery = q.Encode()
		_ = pipe.SubmitFetch(ctx, &fetcher.Request{
			URL:    u.String(),
			Method: "GET",
			Metadata: map[string]string{
				"Entity": "top_markets_trades",
			},
		})
	}

	priceFetchFrom := lastPriceFetchTs
	if priceFetchFrom == 0 {
		priceFetchFrom = time.Now().Unix() - 30*24*60*60
	} else {
		priceFetchFrom -= 3600
	}
	lastPriceFetchTs = time.Now().Unix()

	const priceBatchSize = 20
	for i := 0; i < len(clobTokens); i += priceBatchSize {
		end := i + priceBatchSize
		if end > len(clobTokens) {
			end = len(clobTokens)
		}
		chunk := clobTokens[i:end]
		markets := make([]string, 0, len(chunk))
		tokenMap := make(map[string]string, len(chunk))
		for _, ct := range chunk {
			markets = append(markets, ct.TokenID)
			tokenMap[ct.TokenID] = ct.MarketID
		}
		body := map[string]any{
			"markets":  markets,
			"start_ts": float64(priceFetchFrom),
			"interval": "max",
			"fidelity": 5,
		}
		bodyBytes, _ := json.Marshal(body)
		mapBytes, _ := json.Marshal(tokenMap)
		_ = pipe.SubmitFetch(ctx, &fetcher.Request{
			URL:     clobAPI + "/batch-prices-history",
			Method:  "POST",
			Headers: map[string]string{"Content-Type": "application/json"},
			Body:    bytes.NewReader(bodyBytes),
			Metadata: map[string]string{
				"Entity":         "top_markets_prices",
				"TokenMarketMap": string(mapBytes),
				"Fidelity":       "5",
			},
		})
	}

	const obBatchSize = 50
	for i := 0; i < len(clobTokens); i += obBatchSize {
		end := i + obBatchSize
		if end > len(clobTokens) {
			end = len(clobTokens)
		}
		chunk := clobTokens[i:end]
		type tokenReq struct {
			TokenID string `json:"token_id"`
		}
		var bodyData []tokenReq
		for _, ct := range chunk {
			bodyData = append(bodyData, tokenReq{TokenID: ct.TokenID})
		}
		bodyBytes, _ := json.Marshal(bodyData)
		_ = pipe.SubmitFetch(ctx, &fetcher.Request{
			URL:     clobAPI + "/books",
			Method:  "POST",
			Headers: map[string]string{"Content-Type": "application/json"},
			Body:    bytes.NewReader(bodyBytes),
			Metadata: map[string]string{
				"Entity": "top_markets_orderbooks",
			},
		})
	}
}

// fetchTopCategories loads only tags under the categories root (no subtag walk).
// Root is included so hierarchy parent FK (102982) can be written.
func fetchTopCategories(ctx context.Context, client *http.Client, cfg *config.Config) ([]*services.PlyMktTag, error) {
	catsID := topCategoriesParentTagID
	gammaBase := endpoint(cfg, "gamma", "https://gamma-api.polymarket.com")
	u, err := url.Parse(fmt.Sprintf("%s/tags/%s/related-tags/tags?status=active&omit_empty=true", gammaBase, catsID))
	if err != nil {
		return nil, err
	}
	tags, err := fetcher.FetchPaginated[services.PlyMktTag](ctx, client, u, 100, 0)
	if err != nil {
		return nil, err
	}
	out := make([]*services.PlyMktTag, 0, len(tags)+1)
	out = append(out, &services.PlyMktTag{
		ID:    catsID,
		Label: "Categories",
		Slug:  "categories",
	})
	for _, tag := range tags {
		if tag == nil {
			continue
		}
		tag.ParentTagID = catsID
		out = append(out, tag)
	}
	return out, nil
}

func logCycleOverview(
	logger *slog.Logger,
	events []*services.PlyMktEvent,
	agg tagagg.Result,
	ranked []*services.PlyMktMarket,
) {
	logger.Info("markets selected",
		"total", logging.FormatCount(int64(len(ranked))),
		"tradable_pool", logging.FormatCount(int64(len(agg.Tradable))),
		"events", logging.FormatCount(int64(len(events))),
		"markets_seen", logging.FormatCount(int64(len(agg.Markets))),
	)

	type selAgg struct {
		n          int
		vol24, liq float64
	}
	selectedByKey := map[string]*selAgg{}
	for _, m := range ranked {
		if m == nil {
			continue
		}
		rk := agg.CondCategory[m.ConditionID]
		if rk == "" {
			rk = "default"
		}
		a := selectedByKey[rk]
		if a == nil {
			a = &selAgg{}
			selectedByKey[rk] = a
		}
		a.n++
		a.vol24 += m.Volume24hr
		l := m.LiquidityClob
		if l == 0 {
			l = m.LiquidityNum
		}
		a.liq += l
	}

	type keyRow struct {
		key string
		a   *selAgg
	}
	var rows []keyRow
	for k, a := range selectedByKey {
		rows = append(rows, keyRow{k, a})
	}
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].a.vol24 > rows[j].a.vol24
	})
	for _, r := range rows {
		pool := agg.PoolBySlug[r.key]
		if r.key == "default" {
			pool = agg.PoolBySlug[""]
		}
		logger.Info("category",
			"slug", r.key,
			"selected", fmt.Sprintf("%s of %s",
				logging.FormatCount(int64(r.a.n)),
				logging.FormatCount(int64(pool))),
			"vol24hr", logging.FormatFloat(r.a.vol24),
			"liq", logging.FormatFloat(r.a.liq),
		)
	}

	sorted := tagagg.SortedByVol24(agg.Categories)
	const maxTagLines = 15
	for i, c := range sorted {
		if i >= maxTagLines {
			break
		}
		slug := c.Slug
		if slug == "" {
			slug = c.ID
		}
		logger.Info("top tag by volume",
			"slug", slug,
			"vol24hr", logging.FormatFloat(c.TotalVol24hr),
			"vol", logging.FormatFloat(c.TotalVol),
			"liq", logging.FormatFloat(c.TotalLiq),
			"markets", logging.FormatCount(int64(c.TotalMarkets)),
		)
	}
}

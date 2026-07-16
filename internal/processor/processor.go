// Package processor provides a worker pool for processing data payloads.
// Uses the generic workerpool.Pool for worker management.
package processor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/samucap/poly-asian-data/internal/utils"
	"github.com/samucap/poly-asian-data/internal/workerpool"
)

// =============================================================================
// Error Definitions
// =============================================================================

var (
	ErrPoolStopped   = errors.New("processor pool has been stopped")
	ErrInvalidConfig = errors.New("invalid processor configuration")
	ErrUnknownType   = errors.New("unknown response type")
)

// =============================================================================
// Type Definitions
// =============================================================================

type Processor struct {
	*workerpool.Pool[*fetcher.Response, *Output]
	stats  Stats
	logger *slog.Logger
	cfg    *config.Config

	// Dedup
	processedConditions map[string]bool
	conditionsMu        sync.Mutex

	// OI merge buffer for top-markets: conditionID → oi value (MergeOI metadata).
	oiMergeMu sync.Mutex
	oiMerge   map[string]float64
}

// SaverPayload represents a chunk of data destined for a specific table.
type SaverPayload struct {
	TableName string
	Data      any
}

// Output represents processed data.
type Output struct {
	ID              string
	WorkerID        int
	SaverPayloads   []SaverPayload
	DerivedRequests []*fetcher.Request
	NextPageRequest *fetcher.Request
	ItemCount       int
	Duration        time.Duration
	ProcessedAt     time.Time
	OriginalRequest *fetcher.Request
	// For incremental sync cursor tracking
	SyncType   string // Entity type (e.g., "accounts", "fpmms")
	LastCursor string // Last ID processed (for cursor pagination)
}

// Stats contains atomic counters for processor statistics.
type Stats struct {
	ItemsSubmitted atomic.Int64
	ItemsProcessed atomic.Int64
	ItemsFailed    atomic.Int64
	BytesProcessed atomic.Int64
	TotalDuration  atomic.Int64
}

// Snapshot returns a point-in-time copy.
func (s *Stats) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		ItemsSubmitted: s.ItemsSubmitted.Load(),
		ItemsProcessed: s.ItemsProcessed.Load(),
		ItemsFailed:    s.ItemsFailed.Load(),
		BytesProcessed: s.BytesProcessed.Load(),
		TotalDuration:  time.Duration(s.TotalDuration.Load()),
	}
}

// StatsSnapshot is an immutable snapshot.
type StatsSnapshot struct {
	ItemsSubmitted int64
	ItemsProcessed int64
	ItemsFailed    int64
	BytesProcessed int64
	TotalDuration  time.Duration
}

// =============================================================================
// Configuration & Constants
// =============================================================================

// Sport Categories from tester-concurrent.go
var sportSlugs = []string{
	"football", "basketball", "hockey", "tennis", "esports", "baseball",
	"soccer", "cricket", "rugby", "golf", "ufc", "formula1", "chess",
	"boxing", "pickleball",
}

const gamesTagID = "100639"

// New creates and initializes a processor pool.
func New(ctx context.Context, cfg *config.Config, numWorkers, qSize int) (*Processor, error) {
	logger := logging.Logger.With(
		slog.String("component", "processor"),
	)

	p := &Processor{
		logger:              logger,
		cfg:                 cfg,
		processedConditions: make(map[string]bool),
		oiMerge:             make(map[string]float64),
	}

	pool, err := workerpool.NewPool(ctx, "processor", numWorkers, qSize, logger, p.workerTask)
	if err != nil {
		return nil, err
	}

	p.Pool = pool

	logger.Debug("processor initialized",
		slog.Int("workers", numWorkers),
		slog.Int("queue_size", qSize),
	)

	return p, nil
}

// =============================================================================
// Worker Task - Type Dispatch
// =============================================================================

func (p *Processor) workerTask(ctx context.Context, resp *fetcher.Response) (*Output, error) {
	start := time.Now()

	if resp == nil || resp.Request == nil {
		return nil, errors.New("nil response or request")
	}

	p.logger.Debug("processing response",
		slog.String("url", resp.URL),
		slog.Int("bytes", len(resp.Data)),
	)

	var output *Output
	var err error

	// Dispatch based on URL path or logic
	// Note: We need to differentiate "/tags", "/events", "/sports" (leagues), "/teams"
	urlPath := resp.URL // In real usage, check parsed URL path better

	// Intercept top_markets entity requests
	entity := ""
	if resp.Request.Metadata != nil {
		entity = resp.Request.Metadata["Entity"]
	}

	switch {
	case strings.HasPrefix(entity, "top_markets_"):
		switch entity {
		case "top_markets_oi":
			output, err = p.processTopMarketsOI(resp)
		case "top_markets_trades":
			output, err = p.processTopMarketsTrades(resp)
		case "top_markets_prices":
			output, err = p.processTopMarketsPrices(resp)
		case "top_markets_orderbooks":
			output, err = p.processTopMarketsOrderbooks(resp)
		default:
			err = fmt.Errorf("unknown top_markets entity type: %s", entity)
		}
	case strings.Contains(urlPath, "/tags"):
		output, err = p.processTags(resp)
	case strings.Contains(urlPath, "/events"):
		output, err = p.processEvents(resp)
	case strings.Contains(urlPath, "/sports"):
		output, err = p.processLeagues(resp)
	case strings.Contains(urlPath, "/subgraphs/"):
		output, err = p.processSubgraph(resp)
	case strings.Contains(urlPath, "/markets"):
		output, err = p.processMarkets(resp)
	case strings.Contains(urlPath, "/teams"):
		output, err = p.processTeams(resp)
	case strings.Contains(urlPath, "/book"):
		output, err = p.processOrderbook(resp)
	case strings.Contains(urlPath, "/prices-history"):
		output, err = p.processPricesHistory(resp)
	case strings.Contains(urlPath, "/leaderboard"):
		output, err = p.processLeaderboard(resp)
	case strings.Contains(urlPath, "/holders"):
		output, err = p.processHolders(resp)
	default:
		// Fallback for unexpected URLs
		output = &Output{
			ProcessedAt:     time.Now(),
			OriginalRequest: resp.Request,
			ItemCount:       1,
		}
		p.logger.Warn("unknown url pattern encountered", slog.String("url", resp.URL))
	}

	if err != nil {
		p.stats.ItemsFailed.Add(1)
		p.logger.Error("processing failed",
			slog.String("url", resp.URL),
			slog.String("error", err.Error()),
		)
		return nil, err
	}

	output.Duration = time.Since(start)
	output.WorkerID = 0 // handled by pool actually? ID not exposed by generic pool easily
	output.ProcessedAt = time.Now()

	p.stats.ItemsProcessed.Add(int64(output.ItemCount))
	p.stats.BytesProcessed.Add(int64(len(resp.Data)))
	p.stats.TotalDuration.Add(int64(output.Duration))

	p.logger.Debug("processed response",
		slog.String("url", resp.URL),
		slog.Int("itemCount", output.ItemCount),
		slog.Int("derivedRequests", len(output.DerivedRequests)),
		slog.Int("saverPayloads", len(output.SaverPayloads)),
		slog.Duration("duration", output.Duration),
	)

	return output, nil
}

// =============================================================================
// Logic Implementation
// =============================================================================

// processTags handles /tags response.
// 1. Identifies Sport Categories.
// 2. Generates Derived Requests for events for each Sport Category.
// 3. Emits all tags for saving.
func (p *Processor) processTags(resp *fetcher.Response) (*Output, error) {
	var tags []services.PlyMktTag
	if err := json.Unmarshal(resp.Data, &tags); err != nil {
		return nil, err
	}

	payloads := []SaverPayload{}
	if len(tags) > 0 {
		// Start of the pipeline: definitions
		payloads = append(payloads, SaverPayload{TableName: "tags_definitions", Data: tags})
	}

	return &Output{
		SaverPayloads:   payloads,
		ItemCount:       len(tags),
		OriginalRequest: resp.Request,
	}, nil
}

// processEvents handles /events response.
// 1. Uses Metadata to identify which sport we are enriching.
// 2. Updates tags in the event to link to that sport.
// 3. Emits updated tags for saving.
func (p *Processor) processEvents(resp *fetcher.Response) (*Output, error) {
	var events []services.PlyMktEvent
	if err := json.Unmarshal(resp.Data, &events); err != nil {
		return nil, err
	}

	// Determine Context (SportSlug)
	var sportSlug string
	if resp.Request.Metadata != nil {
		sportSlug = resp.Request.Metadata["SportSlug"]
	}

	// We can also find sport from event tags if metadata is missing (Games logic)
	// But let's rely on Metdata first.
	// Actually, gamesTag logic requires analyzing event tags.

	var derivedReqs []*fetcher.Request
	var marketsToSave []services.PlyMktMarket
	var tagsToUpdate []services.PlyMktTag
	var allTokenIds []string            // Collect all tokens for batch orderbook request
	seenTokens := make(map[string]bool) // Global deduplication for price history

	for _, ev := range events {
		// Determine effective sport slug for this event
		effectiveSlug := sportSlug
		if effectiveSlug == "" {
			// Try to find from tags (Games logic scenario)
			effectiveSlug = getSportFromEvTags(ev.Tags, sportSlugs)
		}

		// Save Markets and Generate CLOB Requests
		for _, m := range ev.Markets {
			if m == nil {
				continue
			}
			// Enrich market with event ID if needed (usually relation handled by DB)
			m.EventID = ev.ID // Ensure link

			// NORMALIZE MARKET DATA
			m.Liquidity = utils.CleanNumericString(m.Liquidity)
			m.Volume = utils.CleanNumericString(m.Volume)
			m.Fee = utils.CleanNumericString(m.Fee)
			m.OutcomePrices = utils.NormalizeString(m.OutcomePrices) // Just trim, it's a JSON string usually

			// PlyMktMarket struct definition check:
			// Liquidity string
			// Volume string
			// Fee string
			// Volume24hr float64 (already typed) -> no need to clean string unless we change struct.
			// The plan focused on "string fields" normalization.

			marketsToSave = append(marketsToSave, *m)

			// Parse ClobTokenIds for this market
			var tokenIds []string
			if m.ClobTokenIds != "" {
				if err := json.Unmarshal([]byte(m.ClobTokenIds), &tokenIds); err != nil {
					tokenIds = strings.Split(m.ClobTokenIds, ",")
				}
			}

			// Collect unique tokens for orderbook and price history
			for _, tid := range tokenIds {
				tid = strings.TrimSpace(tid)
				if tid != "" && !seenTokens[tid] {
					seenTokens[tid] = true
					allTokenIds = append(allTokenIds, tid)
				}
			}
		}

		if effectiveSlug == "" {
			continue
		}

		for _, t := range ev.Tags {
			if t.ID == "1" || t.ID == gamesTagID {
				continue
			}
			tCopy := *t
			tCopy.SportSlug = effectiveSlug
			tagsToUpdate = append(tagsToUpdate, tCopy)
		}
	}

	// NOTE: Price history requests are now handled by runLiveDataSync to avoid
	// duplicate requests across paginated event responses (was generating 30k+ requests).
	// The batch orderbook request below still uses allTokenIds.

	// Create batch orderbook requests (POST /books with token IDs, chunked to avoid API limits)
	if len(allTokenIds) > 0 {
		clobAPI, _ := p.cfg.Services.PlyMkt.Endpoints["clob"].(string)
		if clobAPI != "" {
			// Chunk tokens into batches of 100 to avoid API size limits
			const batchSize = 100
			type tokenReq struct {
				TokenID string `json:"token_id"`
			}

			for i := 0; i < len(allTokenIds); i += batchSize {
				end := i + batchSize
				if end > len(allTokenIds) {
					end = len(allTokenIds)
				}
				chunk := allTokenIds[i:end]

				var bodyItems []tokenReq
				for _, tid := range chunk {
					bodyItems = append(bodyItems, tokenReq{TokenID: tid})
				}
				bodyBytes, _ := json.Marshal(bodyItems)

				derivedReqs = append(derivedReqs, &fetcher.Request{
					URL:     fmt.Sprintf("%s/books", clobAPI),
					Method:  "POST",
					Headers: map[string]string{"Content-Type": "application/json"},
					Body:    bytes.NewReader(bodyBytes),
					Metadata: map[string]string{
						"Type":       "orderbooks_batch",
						"TokenCount": fmt.Sprintf("%d", len(chunk)),
					},
				})
			}
		}
	}

	payloads := []SaverPayload{}
	if len(events) > 0 {
		payloads = append(payloads, SaverPayload{TableName: "plymkt_events", Data: events})
	}
	if len(tagsToUpdate) > 0 {
		payloads = append(payloads, SaverPayload{TableName: "tags_sport_link", Data: tagsToUpdate})
	}
	if len(marketsToSave) > 0 {
		payloads = append(payloads, SaverPayload{TableName: "plymkt_markets", Data: marketsToSave})
	}

	// Pagination Logic
	var nextReq *fetcher.Request
	if len(events) > 0 {
		// Parse current URL to get offset/limit
		u, err := url.Parse(resp.Request.URL)
		if err == nil {
			q := u.Query()
			limitStr := q.Get("limit")
			offsetStr := q.Get("offset")

			if limitStr != "" && offsetStr != "" {
				limit, _ := strconv.Atoi(limitStr)
				offset, _ := strconv.Atoi(offsetStr)
				newOffset := offset + limit
				q.Set("offset", fmt.Sprintf("%d", newOffset))
				u.RawQuery = q.Encode()

				nextReq = &fetcher.Request{
					URL:      u.String(),
					Method:   resp.Request.Method,
					Headers:  resp.Request.Headers,
					Metadata: resp.Request.Metadata,
				}
				p.logger.Debug("built next page request",
					slog.String("url", nextReq.URL),
					slog.Int("newOffset", newOffset),
				)
			}
		}
	}

	return &Output{
		SaverPayloads:   payloads,
		DerivedRequests: derivedReqs,
		ItemCount:       len(events),
		OriginalRequest: resp.Request,
		NextPageRequest: nextReq,
	}, nil
}

// processLeagues handles /sports response (which are Leagues).
func (p *Processor) processLeagues(resp *fetcher.Response) (*Output, error) {
	var leagues []services.PlyMktSport
	if err := json.Unmarshal(resp.Data, &leagues); err != nil {
		return nil, err
	}

	payloads := []SaverPayload{}
	if len(leagues) > 0 {
		payloads = append(payloads, SaverPayload{TableName: "leagues", Data: leagues})
	}

	return &Output{
		SaverPayloads:   payloads,
		ItemCount:       len(leagues),
		OriginalRequest: resp.Request,
	}, nil
}

// processTeams handles /teams response.
func (p *Processor) processTeams(resp *fetcher.Response) (*Output, error) {
	var teams []services.PlyMktTeam
	if err := json.Unmarshal(resp.Data, &teams); err != nil {
		return nil, err
	}

	payloads := []SaverPayload{}
	if len(teams) > 0 {
		payloads = append(payloads, SaverPayload{TableName: "teams", Data: teams})
	}

	return &Output{
		SaverPayloads:   payloads,
		ItemCount:       len(teams),
		OriginalRequest: resp.Request,
	}, nil
}

func (p *Processor) processSubgraph(resp *fetcher.Response) (*Output, error) {
	entity := ""
	if resp.Request.Metadata != nil {
		entity = resp.Request.Metadata["Entity"]
	}

	var payloads []SaverPayload
	var derivedReqs []*fetcher.Request
	var nextReq *fetcher.Request
	var itemCount int
	var lastID string

	// Wrapper for subgraph response: { "data": { "entityName": [...] } }
	// We need to unmarshal generic container first
	type SubgraphResponse struct {
		Data   map[string]json.RawMessage `json:"data"`
		Errors []map[string]any           `json:"errors"`
	}
	var sgResp SubgraphResponse
	if err := json.Unmarshal(resp.Data, &sgResp); err != nil {
		return nil, err
	}

	if len(sgResp.Errors) > 0 {
		return nil, fmt.Errorf("subgraph error: %v", sgResp.Errors)
	}

	// Helper to extract array
	extract := func(key string, target any) error {
		if raw, ok := sgResp.Data[key]; ok {
			return json.Unmarshal(raw, target)
		}
		return nil // Field not present, might be empty result
	}

	switch entity {
	case "conditions":
		var items []services.PlyMktCondition
		if err := extract("conditions", &items); err != nil {
			return nil, err
		}
		itemCount = len(items)
		if itemCount > 0 {
			lastID = items[itemCount-1].ID
		}
		payloads = append(payloads, SaverPayload{TableName: "conditions", Data: items})

	case "accounts":
		var items []services.PlyMktAccount
		if err := extract("accounts", &items); err != nil {
			return nil, err
		}
		itemCount = len(items)
		if itemCount > 0 {
			lastID = items[itemCount-1].ID
		}
		// Normalize Accounts
		for i := range items {
			items[i].CollateralVolume = utils.CleanNumericString(items[i].CollateralVolume)
			items[i].ScaledCollateralVolume = utils.CleanNumericString(items[i].ScaledCollateralVolume)
			items[i].Profit = utils.CleanNumericString(items[i].Profit)
			items[i].ScaledProfit = utils.CleanNumericString(items[i].ScaledProfit)
			items[i].NumTrades = utils.CleanNumericString(items[i].NumTrades)
		}
		payloads = append(payloads, SaverPayload{TableName: "accounts", Data: items})

	case "orderFilledEvents":
		var items []services.PlyMktOrderFilledEvent
		if err := extract("orderFilledEvents", &items); err != nil {
			return nil, err
		}
		itemCount = len(items)
		if itemCount > 0 {
			lastID = items[itemCount-1].ID
		}
		// Normalize OrderFilledEvents
		for i := range items {
			items[i].Fee = utils.CleanNumericString(items[i].Fee)
			items[i].MakerAmountFilled = utils.CleanNumericString(items[i].MakerAmountFilled)
			items[i].TakerAmountFilled = utils.CleanNumericString(items[i].TakerAmountFilled)
			items[i].Timestamp = utils.CleanTimestampString(items[i].Timestamp)
		}
		payloads = append(payloads, SaverPayload{TableName: "order_filled_events", Data: items})

	case "enrichedOrderFilleds":
		var items []services.PlyMktEnrichedOrderFilledEvent
		if err := extract("enrichedOrderFilleds", &items); err != nil {
			return nil, err
		}
		itemCount = len(items)
		if itemCount > 0 {
			lastID = items[itemCount-1].ID
		}

		// Normalize EnrichedOrderFilledEvents (Modify IN PLACE before saving/aggregating)
		for i := range items {
			items[i].Price = utils.CleanNumericString(items[i].Price)
			items[i].Size = utils.CleanNumericString(items[i].Size)
			items[i].Timestamp = utils.CleanTimestampString(items[i].Timestamp)
		}

		// 1. Save Fills
		payloads = append(payloads, SaverPayload{TableName: "enriched_order_filled_events", Data: items})

		// 2. Aggregate Account Stats (In-Memory)
		type AccAgg struct {
			ID         string
			Vol        float64
			Trades     int64
			LastTraded int64
		}
		accStats := make(map[string]*AccAgg)

		updateAcc := func(id string, vol float64, ts int64) {
			if _, exists := accStats[id]; !exists {
				accStats[id] = &AccAgg{
					ID:     id,
					Vol:    0,
					Trades: 0,
				}
			}
			accStats[id].Vol += vol
			accStats[id].Trades++
			if ts > accStats[id].LastTraded {
				accStats[id].LastTraded = ts
			}
		}

		for _, item := range items {
			price, _ := strconv.ParseFloat(item.Price, 64)
			size, _ := strconv.ParseFloat(item.Size, 64)
			vol := price * size

			// Parse timestamp (usually string seconds)
			tsLine, _ := strconv.ParseInt(item.Timestamp, 10, 64)

			if item.Maker.ID != "" {
				updateAcc(item.Maker.ID, vol, tsLine)
			}
			if item.Taker.ID != "" {
				updateAcc(item.Taker.ID, vol, tsLine)
			}
		}

		// Convert to PlyMktAccount (stringified) for Saver
		// We use "accounts_increment" virtual table to tell Saver to ADD values
		var accs []services.PlyMktAccount
		for _, agg := range accStats {
			accs = append(accs, services.PlyMktAccount{
				ID:                  agg.ID,
				CollateralVolume:    fmt.Sprintf("%f", agg.Vol),
				NumTrades:           fmt.Sprintf("%d", agg.Trades),
				LastTradedTimestamp: fmt.Sprintf("%d", agg.LastTraded),
				// Other fields empty, Saver should handle incomplete structs for increment
			})
		}

		if len(accs) > 0 {
			payloads = append(payloads, SaverPayload{TableName: "accounts_increment", Data: accs})
		}

	case "ordersMatchedEvents":
		var items []services.PlyMktOrderFilledEvent // Reusing struct as fields match closely
		if err := extract("ordersMatchedEvents", &items); err != nil {
			return nil, err
		}
		itemCount = len(items)
		if itemCount > 0 {
			lastID = items[itemCount-1].ID
		}
		payloads = append(payloads, SaverPayload{TableName: "orders_matched_events", Data: items})

	case "fpmms":
		// User removed PlyMktFpmm definition, but we need to extract IDs.
		// We can define a local struct or use map.
		type FpmmStub struct {
			ID          string `json:"id"`
			ConditionID string `json:"conditionId"`
		}
		var items []FpmmStub
		if err := extract("fpmms", &items); err != nil {
			return nil, err
		}
		itemCount = len(items)
		if itemCount > 0 {
			lastID = items[itemCount-1].ID
		}

		// If we want to save fpmms themselves? The plan says "For fpmms, just save the records for now (stubbed logic)" but also "create Derived Requests".
		// Actually the plan update said: "Handle fpmms specially... Split list of conditionIds... Save PlyMktMarket"
		// It didn't explicitly say "don't save fpmms". But "fpmms will do derived requests...".
		// Let's assume we don't save fpmms themselves, or maybe we do.
		// Let's safe-guard and save them if we have a table (we added one in plan).
		// Wait, did we? Plan "Define fpmms (containing conditionId, id)" was REPLACED by "plymkt_markets".
		// So we do NOT save fpmms table.

		// Logic to split conditionIds
		var conditionIds []string

		p.conditionsMu.Lock()
		for _, item := range items {
			if item.ConditionID != "" {
				if !p.processedConditions[item.ConditionID] {
					conditionIds = append(conditionIds, item.ConditionID)
					p.processedConditions[item.ConditionID] = true
				}
			}
		}
		p.conditionsMu.Unlock()

		// Chunk by 500 (Gamma limit is 500, but let's go safer e.g. 50? User said "at most 2 requests" implying small count per page or simple logic.
		// User said "/markets limit is at 500". So let's use 100 to be safe or 500.
		chunkSize := 100 // Safe
		for i := 0; i < len(conditionIds); i += chunkSize {
			end := i + chunkSize
			if end > len(conditionIds) {
				end = len(conditionIds)
			}
			u, _ := url.Parse(fmt.Sprintf("%s/markets", p.cfg.Services.PlyMkt.Endpoints["gamma"]))
			// Manually construct query to avoid URL encoding commas
			q := u.Query()
			for j := i; j < end; j++ {
				q.Add("condition_ids", conditionIds[j])
			}
			u.RawQuery = q.Encode()

			req := &fetcher.Request{
				URL:     u.String(),
				Method:  "GET",
				Headers: map[string]string{"Content-Type": "application/json"},
				Metadata: map[string]string{
					"Entity": "plymkt_markets", // Dispatcher needs to know to call processMarkets
				},
			}
			derivedReqs = append(derivedReqs, req)
		}

	case "plymkt_markets":
		return p.processMarkets(resp)

	case "userPositions":
		var items []services.PlyMktUserPosition
		if err := extract("userPositions", &items); err != nil {
			return nil, err
		}
		itemCount = len(items)
		if itemCount > 0 {
			lastID = items[itemCount-1].ID
		}
		// Normalize UserPositions
		for i := range items {
			items[i].Amount = utils.CleanNumericString(items[i].Amount)
			items[i].AvgPrice = utils.CleanNumericString(items[i].AvgPrice)
			items[i].RealizedPnl = utils.CleanNumericString(items[i].RealizedPnl)
			items[i].TotalBought = utils.CleanNumericString(items[i].TotalBought)
		}
		// Use custom handler for computation logic
		return p.processUserPositions(resp, items)
	}

	// Debug log if result is empty but no error

	// Build Next Request if Cursor Pagination is enabled and we have a lastID
	if itemCount >= 1000 && lastID != "" && resp.Request.Metadata["CursorPagination"] == "true" {
		// Check MaxPages limit
		if maxPagesStr, ok := resp.Request.Metadata["MaxPages"]; ok && maxPagesStr != "" {
			maxPages, _ := strconv.Atoi(maxPagesStr)
			currentPage := 1
			if cpStr, ok := resp.Request.Metadata["CurrentPage"]; ok && cpStr != "" {
				currentPage, _ = strconv.Atoi(cpStr)
			}
			if currentPage >= maxPages {
				p.logger.Debug("reached max pages limit",
					slog.Int("maxPages", maxPages),
					slog.Int("currentPage", currentPage),
				)
				// Return early - no next page
				return &Output{
					SaverPayloads:   payloads,
					DerivedRequests: derivedReqs,
					NextPageRequest: nil, // Stop pagination
					ItemCount:       itemCount,
					OriginalRequest: resp.Request,
				}, nil
			}
		}

		// Construct full query using request metadata template
		fullQuery := resp.Request.Metadata["GraphqlQuery"]

		// Restore variables from Metadata or Default
		vars := make(map[string]any)
		if varsJSON, ok := resp.Request.Metadata["GraphqlVariables"]; ok && varsJSON != "" {
			if err := json.Unmarshal([]byte(varsJSON), &vars); err != nil {
				p.logger.Error("failed to unmarshal graphql variables from metadata", slog.String("error", err.Error()))
				// Fallback to default?
				vars["first"] = 1000
				vars["lastId"] = lastID
			}
		} else {
			// Default fallback
			vars["first"] = 1000
			vars["lastId"] = lastID
		}

		// Update Cursor
		vars["lastId"] = lastID

		// Build body
		bodyData := map[string]any{
			"query":     fullQuery,
			"variables": vars,
		}
		bodyBytes, _ := json.Marshal(bodyData) // Error ignored, should be safe

		// Increment CurrentPage in metadata for next request
		newMetadata := make(map[string]string)
		for k, v := range resp.Request.Metadata {
			newMetadata[k] = v
		}
		if cpStr, ok := resp.Request.Metadata["CurrentPage"]; ok && cpStr != "" {
			cp, _ := strconv.Atoi(cpStr)
			newMetadata["CurrentPage"] = strconv.Itoa(cp + 1)
		}

		nextReq = &fetcher.Request{
			URL:      resp.Request.URL,
			Method:   resp.Request.Method,
			Headers:  resp.Request.Headers,
			Body:     bytes.NewReader(bodyBytes),
			Metadata: newMetadata,
			// No Params
		}
	}

	return &Output{
		SaverPayloads:   payloads,
		DerivedRequests: derivedReqs,
		NextPageRequest: nextReq,
		ItemCount:       itemCount,
		OriginalRequest: resp.Request,
		SyncType:        entity, // For cursor tracking
		LastCursor:      lastID, // Last ID processed
	}, nil
}

func (p *Processor) processMarkets(resp *fetcher.Response) (*Output, error) {
	var markets []services.PlyMktMarket
	if err := json.Unmarshal(resp.Data, &markets); err != nil {
		return nil, err
	}

	payloads := []SaverPayload{
		{TableName: "plymkt_markets", Data: markets},
	}

	return &Output{
		SaverPayloads:   payloads,
		ItemCount:       len(markets),
		OriginalRequest: resp.Request,
	}, nil
}

// ProcessorStats returns statistics.
func (p *Processor) ProcessorStats() *Stats {
	return &p.stats
}

// =============================================================================
// Helpers
// =============================================================================

// Helper to extract base URL (scheme + host)
func getBaseURL(fullURL string) string {
	// Simple slice
	// http://host.com/path -> http://host.com
	// Just use Split
	parts := strings.Split(fullURL, "/")
	if len(parts) >= 3 {
		return strings.Join(parts[:3], "/")
	}
	return ""
}

func getSportFromEvTags(evTags []*services.PlyMktTag, slugs []string) string {
	for _, tag := range evTags {
		if slices.Contains(slugs, tag.Slug) {
			return tag.Slug
		}
	}
	return ""
}

// Defaults for League Mapping
var leagueDefaults = map[string]string{
	"acn": "soccer", "bl2": "soccer", "scop": "soccer", "fr2": "soccer", "itsb": "soccer",
	"nba": "basketball", "wnba": "basketball", "ncaab": "basketball", "cbb": "basketball",
	"nhl": "hockey", "cfb": "football", "nfl": "football", "mlb": "baseball",
	"csgo": "esports", "starcraft2": "esports", "es2": "esports", "bnd": "esports",
	"bpl": "cricket", "cpl": "cricket", "wtc": "cricket", "odc": "cricket",
	"ecc": "cricket", "weth": "cricket", "eth": "cricket",
}

func findSportSlugForLeague(l *services.PlyMktSport) string {
	// Check defaults
	// We might need to split series or check raw tags?
	// The original code check l.Tags (string)
	// Original logic was complex: check defaults, check if in existing leagues options...

	// Simplified logic for migration:
	// 1. Check if Series matches a known sport slug (unlikely)
	// 2. Check tags string for sport slug

	tagsList := strings.Split(l.Tags, ",")
	for _, t := range tagsList {
		t = strings.TrimSpace(t)
		// Check if t in sportSlugs? No we have IDs mainly in l.Tags?
		// But let's check defaults map against known keys
		// Wait, l.Tags are IDs?
		// Actually leagueDefaults keys look like "nba", "nfl". Where do these come from?
		// They match `l.Series` or `l.Sport`.
	}

	// Check l.Sport (which is the PRIMARY KEY, e.g. "NBA")
	key := strings.ToLower(l.Sport)
	if val, ok := leagueDefaults[key]; ok {
		return val
	}

	// If l.Series or l.Sport matches a slug directly?
	if slices.Contains(sportSlugs, key) {
		return key
	}

	// Original tester check: strings.Contains(league.Resolution, cat.Tag.Slug)
	// We can't easily check that without iterating all sports.
	// Saver will handle it.

	return ""
}

func findSportSlugForTeam(t *services.PlyMktTeam) string {
	key := strings.ToLower(t.League)
	if val, ok := leagueDefaults[key]; ok {
		return val
	}
	return "" // Simplified
}

// processOrderbook handles /book and /books (batch) responses.
func (p *Processor) processOrderbook(resp *fetcher.Response) (*Output, error) {
	var books []services.PlyMktOrderbook

	// Check if batch response (array) or single (object)
	if len(resp.Data) > 0 && resp.Data[0] == '[' {
		// Batch response: array of orderbooks
		if err := json.Unmarshal(resp.Data, &books); err != nil {
			return nil, err
		}
	} else {
		// Single orderbook
		var book services.PlyMktOrderbook
		if err := json.Unmarshal(resp.Data, &book); err != nil {
			return nil, err
		}
		// Enrich from metadata
		if book.TokenID == "" && resp.Request.Metadata != nil {
			book.TokenID = resp.Request.Metadata["MarketID"]
		}
		books = append(books, book)
	}

	// Compute Spread for all books
	for i := range books {
		var bestBid, bestAsk float64
		if len(books[i].Bids) > 0 {
			bestBid, _ = strconv.ParseFloat(books[i].Bids[0].Price, 64)
		}
		if len(books[i].Asks) > 0 {
			bestAsk, _ = strconv.ParseFloat(books[i].Asks[0].Price, 64)
		}
		books[i].Spread = CalculateSpread(bestBid, bestAsk)

		// Use asset_id as TokenID if not set (batch API returns asset_id)
		if books[i].TokenID == "" && books[i].AssetID != "" {
			books[i].TokenID = books[i].AssetID
		}
	}

	payloads := []SaverPayload{
		{TableName: "orderbooks", Data: books},
	}

	return &Output{
		SaverPayloads:   payloads,
		ItemCount:       len(books),
		OriginalRequest: resp.Request,
	}, nil
}

// processPricesHistory handles /prices-history response.
func (p *Processor) processPricesHistory(resp *fetcher.Response) (*Output, error) {
	// CLOB API response for history is usually: { "history": [...] } or direct array.
	type HistoryResponse struct {
		History []services.PlyMktPriceHistory `json:"history"`
	}

	var points []services.PlyMktPriceHistory

	// properties from metadata
	marketID := ""
	tokenID := ""
	if resp.Request.Metadata != nil {
		marketID = resp.Request.Metadata["MarketID"]
		tokenID = resp.Request.Metadata["TokenID"]
	}
	if tokenID == "" {
		tokenID = marketID // Fallback
	}

	// Try Object format first (includes empty history cases like {"history":[]})
	var objResp HistoryResponse
	if err := json.Unmarshal(resp.Data, &objResp); err == nil {
		points = objResp.History // May be empty, that's OK
	} else {
		// Check if it's an error response
		type ErrorResp struct {
			Error string `json:"error"`
		}
		var errResp ErrorResp
		if json.Unmarshal(resp.Data, &errResp) == nil && errResp.Error != "" {
			p.logger.Warn("price history API error", slog.String("url", resp.URL), slog.String("error", errResp.Error))
			return &Output{ItemCount: 0, OriginalRequest: resp.Request}, nil
		}

		// Try Array format
		if err := json.Unmarshal(resp.Data, &points); err != nil {
			// If both fail, and data isn't empty, it's an error.
			if len(resp.Data) > 0 {
				str := string(resp.Data)
				if str != "[]" && str != "{}" && str != "{\"history\":[]}" {
					p.logger.Warn("failed to unmarshal price history", slog.String("url", resp.URL), slog.String("err", err.Error()))
					return nil, err
				}
			}
		}
	}

	// Convert points to standardized struct for Saver
	// Using PlyMktPriceHistory as the common carrier or SaverPayloads
	// The Saver expects `prices_history` table payload.
	// Let's create a struct that matches schema: timestamp, token_id, price.
	// Reuse PlyMktPriceHistory but ensure Price is set.

	var items []services.PlyMktPriceHistory
	for _, pt := range points {
		items = append(items, services.PlyMktPriceHistory{
			Timestamp: pt.Timestamp,
			Price:     pt.Price,
			TokenID:   tokenID,
			MarketID:  marketID,
		})
	}

	payloads := []SaverPayload{}
	if len(items) > 0 {
		payloads = append(payloads, SaverPayload{TableName: "prices_history", Data: items})
	}

	return &Output{
		SaverPayloads:   payloads,
		ItemCount:       len(items),
		OriginalRequest: resp.Request,
	}, nil
}

// processUserPositions handles userPositions subgraph response.
// Acts as simple transformer - delta computation happens in Saver.
func (p *Processor) processUserPositions(resp *fetcher.Response, items []services.PlyMktUserPosition) (*Output, error) {
	payloads := []SaverPayload{
		{TableName: "position_snapshots", Data: items},
	}

	return &Output{
		SaverPayloads:   payloads,
		ItemCount:       len(items),
		OriginalRequest: resp.Request,
	}, nil
}

// processLeaderboard handles /leaderboard response
func (p *Processor) processLeaderboard(resp *fetcher.Response) (*Output, error) {
	var items []services.PlyMktLeaderboardEntry
	if err := json.Unmarshal(resp.Data, &items); err != nil {
		return nil, fmt.Errorf("failed to unmarshal leaderboard: %w", err)
	}

	// Map to PlyMktUser
	var users []services.PlyMktUser
	for _, item := range items {
		rankVal, _ := strconv.Atoi(item.Rank)
		users = append(users, services.PlyMktUser{
			ProxyWallet:   item.ProxyWallet,
			Username:      item.UserName,
			ProfileImage:  item.ProfileImage,
			XUsername:     item.XUsername,
			VerifiedBadge: item.VerifiedBadge,
			Vol:           item.Vol,
			Pnl:           item.Pnl,
			Rank:          rankVal,
		})
	}

	return &Output{
		SaverPayloads: []SaverPayload{
			{
				TableName: "plymkt_users",
				Data:      users,
			},
		},
		ItemCount: len(items),
	}, nil
}

// processHolders handles /holders response
func (p *Processor) processHolders(resp *fetcher.Response) (*Output, error) {
	var tokens []services.PlyMktHolderToken
	// API returns array of tokens with holders
	if err := json.Unmarshal(resp.Data, &tokens); err != nil {
		// Try single token object? API docs "List of holder profiles".
		// Actually https://data-api.polymarket.com/holders usually returns array of objects with token and holders list.
		return nil, fmt.Errorf("failed to unmarshal holders: %w", err)
	}

	var holders []services.PlyMktHolderRecord
	var users []services.PlyMktUser
	now := time.Now()

	for _, t := range tokens {
		for _, h := range t.Holders {
			// Extract User
			users = append(users, services.PlyMktUser{
				ProxyWallet:  h.ProxyWallet,
				Name:         h.Name,
				Username:     h.Pseudonym, // Pseudonym maps to Username? Or Name? User said "pseudonym": "<string>".
				Bio:          h.Bio,
				ProfileImage: h.ProfileImage,
				// Pnl/Vol not in holders response
			})

			// Extract Holder Link
			holders = append(holders, services.PlyMktHolderRecord{
				TokenID:     t.Token,
				ProxyWallet: h.ProxyWallet,
				Amount:      h.Amount,
				UpdatedAt:   now,
			})
		}
	}

	return &Output{
		SaverPayloads: []SaverPayload{
			{
				TableName: "plymkt_holders_bundle",
				Data: services.PlyMktHoldersBundle{
					Users:   users,
					Holders: holders,
				},
			},
		},
		ItemCount: len(holders), // One record per holding link
	}, nil
}

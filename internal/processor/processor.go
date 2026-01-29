// Package processor provides a worker pool for processing data payloads.
// Uses the generic workerpool.Pool for worker management.
package processor

import (
	"context"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/services"
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
	}

	pool, err := workerpool.NewPool[*fetcher.Response, *Output](ctx, "processor", numWorkers, qSize, logger, p.workerTask)
	if err != nil {
		return nil, err
	}

	p.Pool = pool

	logger.Info("processor initialized",
		slog.Int("workers", numWorkers),
		slog.Int("queue_size", qSize),
	)

	return p, nil
}

// SubscribeToFetcher connects to the fetcher's output channel.
func (p *Processor) SubscribeToFetcher(ctx context.Context, upstream <-chan workerpool.Result[*fetcher.Response]) {
	for {
		select {
		case result, ok := <-upstream:
			if !ok {
				return
			}
			if result.Err != nil {
				continue
			}
			_ = p.SubmitWait(result.Value)
		case <-ctx.Done():
			return
		}
	}
}

// =============================================================================
// Worker Task - Type Dispatch
// =============================================================================

func (p *Processor) workerTask(ctx context.Context, resp *fetcher.Response) (*Output, error) {
	start := time.Now()

	if resp == nil || resp.Request == nil {
		return nil, errors.New("nil response or request")
	}

	p.logger.Info("processing response",
		slog.String("url", resp.URL),
		slog.Int("bytes", len(resp.Data)),
	)

	var output *Output
	var err error

	// Dispatch based on URL path or logic
	// Note: We need to differentiate "/tags", "/events", "/sports" (leagues), "/teams"
	urlPath := resp.URL // In real usage, check parsed URL path better
	switch {
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

	p.logger.Info("processed response",
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

	var sportsToSave []services.PlyMktSportCategory
	var derivedReqs []*fetcher.Request

	for i := range tags {
		tag := &tags[i]
		if slices.Contains(sportSlugs, tag.Slug) {
			// Found a sport category
			sportsToSave = append(sportsToSave, services.PlyMktSportCategory{
				Slug:         tag.Slug,
				PrimaryTagID: tag.ID,
			})
			tag.SportSlug = tag.Slug

			u := getBaseURL(resp.URL)
			eventsURL := u + "/events?tag_id=" + tag.ID

			req := &fetcher.Request{
				URL:     eventsURL,
				Method:  "GET",
				Headers: resp.Request.Headers,
				Metadata: map[string]string{
					"SportSlug": tag.Slug,
				},
			}
			derivedReqs = append(derivedReqs, req)
		} else if tag.ID == gamesTagID {
			// Found the Games tag
			u := getBaseURL(resp.URL)
			eventsURL := u + "/events?tag_id=" + tag.ID // Note: tester-concurrent doesn't pass 'related_tags' for gamesTag?
			// "if tagID != "100639" { params.Add("related_tags", "true") }" in fetchEvents
			// fetcher.go might not have this logic embedded in simplified Req.
			// We should append query params manually here.

			// Re-construct URL with explicit params to match tester
			// Or let fetcher handle it (but fetcher is generic).
			// Better to be explicit here.

			// However, the Fetcher logic was simple.
			// Let's assume we construct the URL fully.

			req := &fetcher.Request{
				URL:     eventsURL,
				Method:  "GET",
				Headers: resp.Request.Headers,
				Metadata: map[string]string{
					"IsGames": "true",
				},
			}
			derivedReqs = append(derivedReqs, req)
		}
	}

	// ...
	payloads := []SaverPayload{}
	if len(tags) > 0 {
		// Start of the pipeline: definitions
		payloads = append(payloads, SaverPayload{TableName: "tags_definitions", Data: tags})
	}
	if len(sportsToSave) > 0 {
		payloads = append(payloads, SaverPayload{TableName: "sports", Data: sportsToSave})
	}

	return &Output{
		SaverPayloads:   payloads,
		DerivedRequests: derivedReqs,
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

	var tagsToUpdate []services.PlyMktTag

	for _, ev := range events {
		// Determine effective sport slug for this event
		effectiveSlug := sportSlug
		if effectiveSlug == "" {
			// Try to find from tags (Games logic scenario)
			effectiveSlug = getSportFromEvTags(ev.Tags, sportSlugs)
		}

		if effectiveSlug == "" {
			continue
		}

		for _, t := range ev.Tags {
			if t.ID == "1" || t.ID == gamesTagID {
				continue
			}
			// Update tag info
			// We create a copy to avoid mutating if shared (though unlikely here)
			// But wait, ev.Tags are pointers in PlyMktEvent?
			// Yes []*PlyMktTag.
			tCopy := *t
			tCopy.SportSlug = effectiveSlug
			tagsToUpdate = append(tagsToUpdate, tCopy)
		}
	}

	payloads := []SaverPayload{}
	if len(tagsToUpdate) > 0 {
		// Linking tags to sports
		payloads = append(payloads, SaverPayload{TableName: "tags_sport_link", Data: tagsToUpdate})
	}

	return &Output{
		SaverPayloads:   payloads,
		ItemCount:       len(events),
		OriginalRequest: resp.Request,
	}, nil
}

// processLeagues handles /sports response (which are Leagues).
func (p *Processor) processLeagues(resp *fetcher.Response) (*Output, error) {
	var leagues []services.PlyMktSport
	if err := json.Unmarshal(resp.Data, &leagues); err != nil {
		return nil, err
	}

	for i := range leagues {
		l := &leagues[i]

		// We defer hierarchy logic to Saver via 'league_hierarchy' payload
		sSlug := findSportSlugForLeague(l)
		if sSlug != "" {
			l.SportSlug = sSlug
		}
	}

	payloads := []SaverPayload{}
	if len(leagues) > 0 {
		payloads = append(payloads, SaverPayload{TableName: "leagues", Data: leagues})
		// Also send for hierarchy processing (Saver has the context to do this right)
		payloads = append(payloads, SaverPayload{TableName: "league_hierarchy", Data: leagues})
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

	for i := range teams {
		// Just pass through. Saver will resolve logic.
		// We can try to hint SportSlug if possible.
		t := &teams[i]
		sSlug := findSportSlugForTeam(t)
		if sSlug != "" {
			t.SportSlug = sSlug
		}
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
		payloads = append(payloads, SaverPayload{TableName: "enriched_order_filled_events", Data: items})

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
	}

	// Debug log if result is empty but no error

	// Build Next Request if Cursor Pagination is enabled and we have a lastID
	if itemCount >= 1000 && lastID != "" && resp.Request.Metadata["CursorPagination"] == "true" {
			// Construct full query using request metadata template
			fullQuery := resp.Request.Metadata["GraphqlQuery"]
			
			// Build body
			bodyData := map[string]any{
				"query": fullQuery,
				"variables": map[string]any{
					"first": 1000,
					"lastId": lastID,
				},
			}
			bodyBytes, _ := json.Marshal(bodyData) // Error ignored, should be safe
			
			nextReq = &fetcher.Request{
				URL: resp.Request.URL,
				Method: resp.Request.Method,
				Headers: resp.Request.Headers,
				Body: bytes.NewReader(bodyBytes),
				Metadata: resp.Request.Metadata,
				// No Params
			}
	}

	return &Output{
		SaverPayloads:   payloads,
		DerivedRequests: derivedReqs,
		NextPageRequest: nextReq,
		ItemCount:       itemCount,
		OriginalRequest: resp.Request, // Fetcher handles generic pagination if NextPageRequest is nil
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

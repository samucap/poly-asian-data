// Package saver provides a database client wrapper for saving processed data.
// Uses the generic workerpool.Pool for worker management.
package saver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/samucap/poly-asian-data/internal/workerpool"
)

// =============================================================================
// Error Definitions
// =============================================================================

var (
	ErrPoolStopped   = errors.New("saver pool has been stopped")
	ErrInvalidConfig = errors.New("invalid saver configuration")
	ErrSaveFailed    = errors.New("save failed")
)

// =============================================================================
// Type Definitions
// =============================================================================

type Saver struct {
	*workerpool.Pool[*Record, *Result]
	db     *pgxpool.Pool
	logger *slog.Logger
	stats  Stats

	// Cache for resolving Sport Slug -> UUID
	// Key: Sport Slug
	sportCache map[string]*CachedSport
	cacheMu    sync.RWMutex
}

var (
	// Sport Categories from tester-concurrent.go
	// We need these for defaults mapping
	sportSlugs = []string{
		"football", "basketball", "hockey", "tennis", "esports", "baseball",
		"soccer", "cricket", "rugby", "golf", "ufc", "formula1", "chess",
		"boxing", "pickleball",
	}

	leagueDefaults = map[string]string{
		"acn": "soccer", "bl2": "soccer", "scop": "soccer", "fr2": "soccer", "itsb": "soccer",
		"nba": "basketball", "wnba": "basketball", "ncaab": "basketball", "cbb": "basketball",
		"nhl": "hockey", "cfb": "football", "nfl": "football", "mlb": "baseball",
		"csgo": "esports", "starcraft2": "esports", "es2": "esports", "bnd": "esports",
		"bpl": "cricket", "cpl": "cricket", "wtc": "cricket", "odc": "cricket",
		"ecc": "cricket", "weth": "cricket", "eth": "cricket",
	}
)

// Record represents data to be saved.
type Record struct {
	ID          string
	TableName   string
	Data        any
	ItemCount   int
	ProcessedAt time.Time
}

// Result represents the outcome of a save operation.
type Result struct {
	ID           string
	RowsAffected int64
	Duration     time.Duration
}

// Stats contains atomic counters for saver statistics.
type Stats struct {
	RecordsSubmitted atomic.Int64
	RecordsSaved     atomic.Int64
	RecordsFailed    atomic.Int64
	RowsAffected     atomic.Int64
	TotalDuration    atomic.Int64
}

// Snapshot returns a point-in-time copy.
func (s *Stats) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		RecordsSubmitted: s.RecordsSubmitted.Load(),
		RecordsSaved:     s.RecordsSaved.Load(),
		RecordsFailed:    s.RecordsFailed.Load(),
		RowsAffected:     s.RowsAffected.Load(),
		TotalDuration:    time.Duration(s.TotalDuration.Load()),
	}
}

// StatsSnapshot is an immutable snapshot.
type StatsSnapshot struct {
	RecordsSubmitted int64
	RecordsSaved     int64
	RecordsFailed    int64
	RowsAffected     int64
	TotalDuration    time.Duration
}

// =============================================================================
// Constructor
// =============================================================================

// New creates and initializes a saver pool.
// db must be a live connection pool owned by the caller (typically pipeline.Factory).
// The saver does not close db; call Factory.Close / caller's responsibility.
func New(ctx context.Context, logger *slog.Logger, cfg *config.Config, db *pgxpool.Pool, numWorkers, qSize int) (*Saver, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: db pool is required", ErrInvalidConfig)
	}
	if logger == nil {
		return nil, fmt.Errorf("%w: logger is required", ErrInvalidConfig)
	}

	s := &Saver{
		db:         db,
		logger:     logger,
		sportCache: make(map[string]*CachedSport),
	}

	// Pre-load cache
	if err := s.loadSportIDs(ctx); err != nil {
		logger.Warn("failed to pre-load sport IDs", slog.String("error", err.Error()))
	}

	// Terminal stage: process still updates pool + saver stats; no OutputQ consumer/Drain.
	pool, err := workerpool.NewPool(ctx, "saver", numWorkers, qSize, logger, s.workerTask, workerpool.WithDiscardOutput())
	if err != nil {
		return nil, err
	}

	s.Pool = pool

	logger.Debug("saver initialized",
		slog.Int("workers", numWorkers),
		slog.Int("queue_size", qSize),
	)

	return s, nil
}

// DB returns the underlying database pool (shared; do not close from here).
func (s *Saver) DB() *pgxpool.Pool {
	return s.db
}

// SaverStats returns statistics.
func (s *Saver) SaverStats() *Stats {
	return &s.stats
}

// loadSportIDs populates the cache from the DB.
func (s *Saver) loadSportIDs(ctx context.Context) error {
	rows, err := s.db.Query(ctx, "SELECT slug, id, primary_tag_id FROM sports")
	if err != nil {
		return err
	}
	defer rows.Close()

	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()

	// Reset cache
	s.sportCache = make(map[string]*CachedSport)
	for rows.Next() {
		var slug, id string
		var primaryTagID *string
		if err := rows.Scan(&slug, &id, &primaryTagID); err != nil {
			// If scan fails (maybe schema mismatch if primary_tag_id added later?), relax or fail.
			// Assuming schema matches.
			return err
		}

		if _, ok := s.sportCache[slug]; ok {
			s.logger.Warn("duplicate sport slug", slog.String("slug", slug))
		}
		s.sportCache[slug] = &CachedSport{
			UUID:         id,
			PrimaryTagID: primaryTagID,
		}
	}
	return nil
}

type CachedSport struct {
	UUID         string
	PrimaryTagID *string
}

// getSportID returns the UUID for a sport slug, if it exists.
func (s *Saver) getSportID(slug string) (string, bool) {
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()
	if c, ok := s.sportCache[slug]; ok {
		return c.UUID, true
	}
	return "", false
}

// setSportID updates the cache.
func (s *Saver) setSportID(slug, id string, primaryTagID *string) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	s.sportCache[slug] = &CachedSport{
		UUID:         id,
		PrimaryTagID: primaryTagID,
	}
}

// findSportForLeague attempts to resolve the Sport Slug for a league
func (s *Saver) findSportForLeague(l *services.PlyMktSport) string {
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()

	// 1. Check direct defaults
	key := strings.ToLower(l.Sport) // l.Sport is effectively the league 'slug' or 'key' often
	if val, ok := leagueDefaults[key]; ok {
		return val
	}

	// 2. Check Tags (matches Sport Primary Tag ID)
	tagIDs := strings.Split(l.Tags, ",")
	for _, tagID := range tagIDs {
		tagID = strings.TrimSpace(tagID)
		for slug, sport := range s.sportCache {
			if sport.PrimaryTagID != nil && *sport.PrimaryTagID == tagID {
				return slug
			}
		}
	}

	// 3. Check Resolution string for Slug
	for slug := range s.sportCache {
		// Simple case insensitive check? Original logic was `strings.Contains(l.Resolution, cat.Tag.Slug)`
		// cat.Tag.Slug == slug (mostly)
		if strings.Contains(l.Resolution, slug) {
			return slug
		}
	}

	// 4. Default Slug match
	if val, ok := leagueDefaults[l.Sport]; ok { // l.Sport again
		return val
	}

	return ""
}

// findSportForTeam attempts to resolve Sport Slug for a team
func (s *Saver) findSportForTeam(t *services.PlyMktTeam) string {
	s.cacheMu.RLock()
	defer s.cacheMu.RUnlock()

	// 1. Check League in defaults
	if val, ok := leagueDefaults[t.League]; ok {
		return val
	}

	// 2. Check if League is in any Sport's list of Leagues (Saver doesn't track this easily without huge state)
	// But we can check if t.League matches any known Sport-linked logic?
	// Simplified: relying on defaults mainly.

	return ""
}

// =============================================================================
// Worker Task
// =============================================================================

func (s *Saver) workerTask(ctx context.Context, record *Record) (*Result, error) {
	start := time.Now()
	// Job-level counters (one SubmitSave → one submitted; success/fail for rate).
	s.stats.RecordsSubmitted.Add(1)

	s.logger.Debug("saving record",
		slog.String("table", record.TableName),
		slog.Int("itemCount", record.ItemCount),
	)

	var rowsAffected int64
	var err error

	switch record.TableName {
	case "sports":
		rowsAffected, err = s.batchInsertSports(ctx, record.Data)
	case "tags_definitions":
		rowsAffected, err = s.batchInsertTagsDefinitions(ctx, record.Data)
	case "tags_sport_link":
		rowsAffected, err = s.batchInsertTagsSportLink(ctx, record.Data)
	case "tags_hierarchy":
		rowsAffected, err = s.batchInsertTagsHierarchy(ctx, record.Data)
	case "leagues":
		rowsAffected, err = s.batchInsertLeagues(ctx, record.Data)
	case "teams":
		rowsAffected, err = s.batchInsertTeams(ctx, record.Data)
	case "league_hierarchy":
		rowsAffected, err = s.batchInsertLeagueHierarchy(ctx, record.Data)
	case "plymkt_markets":
		rowsAffected, err = s.batchInsertMarkets(ctx, record.Data)
	case "conditions":
		rowsAffected, err = s.batchInsertConditions(ctx, record.Data)
	case "accounts":
		rowsAffected, err = s.batchInsertAccounts(ctx, record.Data)
	case "prices_history":
		rowsAffected, err = s.batchInsertPricesHistory(ctx, record.Data)
	case "trades":
		rowsAffected, err = s.batchInsertTrades(ctx, record.Data)
	case "orderbook_snapshots":
		rowsAffected, err = s.batchInsertOrderbookSnapshots(ctx, record.Data)
	case "plymkt_markets_oi":
		rowsAffected, err = s.batchUpdateMarketsOI(ctx, record.Data)
	case "accounts_increment":
		rowsAffected, err = s.batchIncrementAccounts(ctx, record.Data)
	case "position_snapshots":
		rowsAffected, err = s.batchInsertPositionSnapshots(ctx, record.Data)
	case "orderbooks":
		rowsAffected, err = s.batchInsertOrderbooks(ctx, record.Data)
	case "plymkt_events":
		rowsAffected, err = s.batchInsertEvents(ctx, record.Data)
	case "enriched_order_filled_events":
		rowsAffected, err = s.batchInsertEnrichedOrderFilledEvents(ctx, record.Data)
	case "plymkt_users":
		rowsAffected, err = s.batchInsertUsers(ctx, record.Data)
	case "plymkt_holders":
		rowsAffected, err = s.batchInsertHolders(ctx, record.Data)
	case "plymkt_holders_bundle":
		rowsAffected, err = s.batchInsertHoldersBundle(ctx, record.Data)
	default:
		return nil, fmt.Errorf("unknown table: %s", record.TableName)
	}

	if err != nil {
		s.stats.RecordsFailed.Add(1)
		s.logger.Error("save failed",
			slog.String("table", record.TableName),
			slog.String("error", err.Error()),
		)
		return nil, err
	}

	duration := time.Since(start)
	// Count successful jobs (not ItemCount) so success rates match submitted/failed.
	// Row volume lives in RowsAffected.
	s.stats.RecordsSaved.Add(1)
	s.stats.RowsAffected.Add(rowsAffected)
	s.stats.TotalDuration.Add(int64(duration))

	s.logger.Debug("saved record",
		slog.String("table", record.TableName),
		slog.Int64("rowsAffected", rowsAffected),
		slog.Duration("duration", duration),
	)

	return &Result{
		ID:           record.ID,
		RowsAffected: rowsAffected,
		Duration:     duration,
	}, nil
}

// ... existing batch insert methods ...

func (s *Saver) batchInsertPricesHistory(ctx context.Context, data any) (int64, error) {
	items, ok := data.([]services.PlyMktPriceHistory)
	if !ok {
		return 0, fmt.Errorf("invalid data type for prices_history: got %T", data)
	}

	sql := `INSERT INTO prices_history (token_id, timestamp, price, market_id, fidelity_min, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (token_id, timestamp) DO UPDATE SET 
			price = EXCLUDED.price, 
			updated_at = EXCLUDED.updated_at`

	rows := make([][]any, 0, len(items))
	for _, item := range items {
		rows = append(rows, []any{
			item.TokenID,
			item.Timestamp,
			item.Price,
			item.MarketID,
			item.Fidelity,
			item.UpdatedAt,
		})
	}

	if err := db.BatchExec(ctx, s.db, sql, rows); err != nil {
		return 0, err
	}
	return int64(len(items)), nil
}

func (s *Saver) batchInsertTrades(ctx context.Context, data any) (int64, error) {
	items, ok := data.([]services.PlyMktTrade)
	if !ok {
		return 0, fmt.Errorf("invalid data type for trades: got %T", data)
	}

	sql := `INSERT INTO trades (
			transaction_hash, proxy_wallet, side, asset, condition_id, 
			size, price, timestamp, title, slug, icon, event_slug, 
			outcome, outcome_index, name, pseudonym
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		ON CONFLICT (transaction_hash) DO NOTHING`

	rows := make([][]any, 0, len(items))
	for _, t := range items {
		rows = append(rows, []any{
			t.TransactionHash, t.ProxyWallet, t.Side, t.Asset, t.ConditionId,
			t.Size, t.Price, t.Timestamp, t.Title, t.Slug, t.Icon, t.EventSlug,
			t.Outcome, t.OutcomeIndex, t.Name, t.Pseudonym,
		})
	}

	if err := db.BatchExec(ctx, s.db, sql, rows); err != nil {
		return 0, err
	}
	return int64(len(items)), nil
}

func (s *Saver) batchInsertOrderbookSnapshots(ctx context.Context, data any) (int64, error) {
	items, ok := data.([]services.PlyMktOrderbookSnapshot)
	if !ok {
		return 0, fmt.Errorf("invalid data type for orderbook_snapshots: got %T", data)
	}

	sql := `INSERT INTO orderbook_snapshots (
			time, market_id, token_id, best_bid, best_ask, imbalance, 
			total_bid_depth, total_ask_depth, depth_json, raw_response_json
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (time, market_id, token_id) DO UPDATE SET
			best_bid = EXCLUDED.best_bid,
			best_ask = EXCLUDED.best_ask,
			imbalance = EXCLUDED.imbalance,
			total_bid_depth = EXCLUDED.total_bid_depth,
			total_ask_depth = EXCLUDED.total_ask_depth,
			depth_json = EXCLUDED.depth_json,
			raw_response_json = EXCLUDED.raw_response_json`

	rows := make([][]any, 0, len(items))
	for _, item := range items {
		rows = append(rows, []any{
			item.Time, item.MarketID, item.TokenID,
			item.BestBid, item.BestAsk, item.Imbalance,
			item.TotalBidDepth, item.TotalAskDepth,
			item.DepthJSON, item.RawJSON,
		})
	}

	if err := db.BatchExec(ctx, s.db, sql, rows); err != nil {
		return 0, err
	}
	return int64(len(items)), nil
}

func (s *Saver) batchUpdateMarketsOI(ctx context.Context, data any) (int64, error) {
	items, ok := data.([]services.PlyMktMarketOI)
	if !ok {
		return 0, fmt.Errorf("invalid data type for plymkt_markets_oi: got %T", data)
	}

	sql := `UPDATE plymkt_markets SET oi = $2 WHERE condition_id = $1`

	rows := make([][]any, 0, len(items))
	for _, item := range items {
		rows = append(rows, []any{item.Market, item.Value})
	}

	if err := db.BatchExec(ctx, s.db, sql, rows); err != nil {
		return 0, err
	}
	return int64(len(items)), nil
}

// GetActiveTokenIDs fetches token IDs for active markets/whales to prioritize for price history.
func (s *Saver) GetActiveTokenIDs(ctx context.Context) ([]string, error) {
	// Simple strategy: Fetch standard active markets from plymkt_markets
	rows, err := s.db.Query(ctx, `
		SELECT clob_token_ids FROM plymkt_markets 
		WHERE active = true AND clob_token_ids IS NOT NULL AND clob_token_ids != ''
		ORDER BY volume_24hr DESC
		LIMIT 200
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tokenIDs []string
	for rows.Next() {
		var idsVal string
		if err := rows.Scan(&idsVal); err != nil {
			s.logger.Warn("scan error in GetActiveTokenIDs", slog.String("error", err.Error()))
			continue
		}
		// clob_token_ids is usually a JSON array string `["..."]` or plain string?
		// Schema says 'clob_token_ids TEXT'. API returns JSON encoded string.
		// Let's assume it's JSON string.
		var ids []string
		if err := json.Unmarshal([]byte(idsVal), &ids); err == nil {
			tokenIDs = append(tokenIDs, ids...)
		} else {
			// Fallback if not JSON
			tokenIDs = append(tokenIDs, idsVal)
		}
	}
	return tokenIDs, nil
}

// GetActiveMarketIDs fetches distinct market IDs (Condition IDs) for active markets with high volume.
func (s *Saver) GetActiveMarketIDs(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}

	// Select condition_id (Hex) instead of id (Numeric Asset ID)
	rows, err := s.db.Query(ctx, `
		SELECT condition_id FROM plymkt_markets 
		WHERE active = true AND condition_id != ''
		ORDER BY volume_24hr DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var marketIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			s.logger.Warn("scan error in GetActiveMarketIDs", slog.String("error", err.Error()))
			continue
		}
		marketIDs = append(marketIDs, id)
	}
	return marketIDs, nil
}

// GetWhaleIDs fetches account IDs for top whales by collateral volume.
func (s *Saver) GetWhaleIDs(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 100
	}

	rows, err := s.db.Query(ctx, `
		SELECT id FROM accounts 
		WHERE collateral_volume IS NOT NULL AND collateral_volume != '' AND collateral_volume != '0'
		ORDER BY COALESCE(collateral_volume::numeric, 0) DESC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var whaleIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			s.logger.Warn("scan error in GetWhaleIDs", slog.String("error", err.Error()))
			continue
		}
		whaleIDs = append(whaleIDs, id)
	}
	return whaleIDs, nil
}

// GetLastAccountID fetches the lexically last account ID currently in the DB.
// Used for resumable syncing.
func (s *Saver) GetLastAccountID(ctx context.Context) (string, error) {
	var id string
	// accounts are sorted by ID in the subgraph query.
	// So we just need the max ID we have.
	err := s.db.QueryRow(ctx, "SELECT id FROM accounts ORDER BY id DESC LIMIT 1").Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return id, nil
}

// =============================================================================
// Sync State Management (for incremental syncing)
// =============================================================================

// GetSyncCursor returns the last cursor for a given sync type.
// Returns empty string if no cursor exists (first sync).
func (s *Saver) GetSyncCursor(ctx context.Context, syncType string) (string, error) {
	var cursor *string
	err := s.db.QueryRow(ctx,
		"SELECT last_cursor FROM sync_state WHERE sync_type = $1",
		syncType,
	).Scan(&cursor)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	if cursor == nil {
		return "", nil
	}
	return *cursor, nil
}

// SetSyncCursor updates the cursor for a sync type.
// Also marks the sync as running and updates last_sync_at.
func (s *Saver) SetSyncCursor(ctx context.Context, syncType, cursor string, itemCount int) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO sync_state (sync_type, last_cursor, last_sync_at, total_items, status)
		VALUES ($1, $2, NOW(), $3, 'running')
		ON CONFLICT (sync_type) DO UPDATE SET
			last_cursor = $2,
			last_sync_at = NOW(),
			total_items = sync_state.total_items + $3,
			status = 'running'
	`, syncType, cursor, itemCount)
	return err
}

// SetSyncStatus updates the status of a sync type without changing the cursor.
// Used to indicate "running" state at the start of a cycle.
func (s *Saver) SetSyncStatus(ctx context.Context, syncType, status string) error {
	_, err := s.db.Exec(ctx, `
		INSERT INTO sync_state (sync_type, last_sync_at, status, total_items, last_cursor)
		VALUES ($1, NOW(), $2, 0, '')
		ON CONFLICT (sync_type) DO UPDATE SET
			status = $2,
			last_sync_at = NOW()
	`, syncType, status)
	return err
}

// GetAllSyncCursors returns a map of sync_type -> last_cursor for all entities.
func (s *Saver) GetAllSyncCursors(ctx context.Context) (map[string]string, error) {
	rows, err := s.db.Query(ctx, "SELECT sync_type, last_cursor FROM sync_state WHERE last_cursor IS NOT NULL")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cursors := make(map[string]string)
	for rows.Next() {
		var syncType string
		var cursor *string
		if err := rows.Scan(&syncType, &cursor); err != nil {
			s.logger.Warn("scan error in GetAllSyncCursors", slog.String("error", err.Error()))
			continue
		}
		if cursor != nil {
			cursors[syncType] = *cursor
		}
	}
	return cursors, nil
}

// MarkSyncComplete marks a sync as completed.
func (s *Saver) MarkSyncComplete(ctx context.Context, syncType string) error {
	_, err := s.db.Exec(ctx, `
		UPDATE sync_state SET status = 'completed', last_sync_at = NOW()
		WHERE sync_type = $1
	`, syncType)
	return err
}

// ResetSyncCursor clears the cursor for a full resync.
func (s *Saver) ResetSyncCursor(ctx context.Context, syncType string) error {
	_, err := s.db.Exec(ctx, `
		DELETE FROM sync_state WHERE sync_type = $1
	`, syncType)
	return err
}

// =============================================================================
// Batch Insert Methods
// =============================================================================

func (s *Saver) batchInsertEvents(ctx context.Context, data any) (int64, error) {
	events, ok := data.([]services.PlyMktEvent)
	if !ok {
		return 0, fmt.Errorf("invalid data type for events: got %T", data)
	}

	batch := &pgx.Batch{}
	for _, e := range events {
		batch.Queue(`
			INSERT INTO plymkt_events (
				id, ticker, slug, title, description, start_date, end_date,
				category, image, icon, active, closed, archived, new, featured,
				restricted, liquidity, volume, volume_24hr, volume_1wk, volume_1mo,
				volume_1yr, liquidity_clob, competitive, neg_risk, neg_risk_market_id,
				comment_count, enable_order_book, series_slug, live, ended,
				creator_id
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
				$16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28,
				$29, $30, $31, $32
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
				updated_at = CURRENT_TIMESTAMP
		`,
			e.ID, e.Ticker, e.Slug, e.Title, e.Description, e.StartDate, e.EndDate,
			e.Category, e.Image, e.Icon, e.Active, e.Closed, e.Archived, e.New, e.Featured,
			e.Restricted, e.Liquidity, e.Volume, e.Volume24hr, e.Volume1wk, e.Volume1mo,
			e.Volume1yr, e.LiquidityClob, e.Competitive, e.NegRisk, e.NegRiskMarketID,
			e.CommentCount, e.EnableOrderBook, e.SeriesSlug, e.Live, e.Ended,
			e.CreatedBy,
		)
	}

	rowsAffected, failIdx, err := s.execBatch(ctx, batch, len(events))
	if err != nil {
		if failIdx != -1 && failIdx < len(events) {
			e := events[failIdx]
			s.logger.Error("failed to save event",
				slog.String("id", e.ID),
				slog.String("slug", e.Slug),
				slog.String("error", err.Error()),
			)
		}
		return rowsAffected, err
	}
	return rowsAffected, nil
}

func (s *Saver) batchInsertMarkets(ctx context.Context, data any) (int64, error) {
	markets, ok := data.([]services.PlyMktMarket)
	if !ok {
		return 0, fmt.Errorf("invalid data type for markets: got %T", data)
	}

	batch := &pgx.Batch{}
	for _, m := range markets {
		// fee is DOUBLE PRECISION in schema; API often sends "" — use NULL instead of empty string.
		var feeVal any
		if m.Fee != "" {
			if f, err := strconv.ParseFloat(m.Fee, 64); err == nil {
				feeVal = f
			}
		}
		// Only upsert fields present in schema.sql.
		// Note: schema has NUMERIC for float64 fields. PGX handles float64 -> numeric mapping well usually.
		batch.Queue(`
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
				pending_deployment, deploying, rfq_enabled, event_start_time, oi
			) VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, 
				$18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, 
				$33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, 
				$48, $49, $50, $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, 
				$63, $64, $65, $66, $67, $68, $69, $70, $71, $72, $73, $74, $75, $76, $77, 
				$78, $79, $80, $81, $82, $83, $84, $85, $86
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
				event_start_time = EXCLUDED.event_start_time,
				oi = COALESCE(EXCLUDED.oi, plymkt_markets.oi)
		`,
			m.ID, m.Question, m.ConditionID, m.Slug, m.ResolutionSource, m.EndDate,
			m.Category, m.Liquidity, m.SponsorName, m.StartDate, feeVal, m.Image, m.Icon,
			m.Description, m.Volume, m.Active, m.MarketType, m.Closed, m.CreatedBy, m.UpdatedBy,
			m.CreatedAt, m.UpdatedAt, m.WideFormat, m.New, m.Featured, m.Archived, m.Restricted,
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
			m.OpenInterest,
		)
	}

	// ... (inside batchInsertMarkets)
	rowsAffected, failIdx, err := s.execBatch(ctx, batch, len(markets))
	if err != nil {
		if failIdx != -1 && failIdx < len(markets) {
			m := markets[failIdx]
			s.logger.Error("failed to save market",
				slog.String("id", m.ID),
				slog.String("slug", m.Slug),
				slog.String("error", err.Error()),
			)
		}
		return rowsAffected, err
	}
	return rowsAffected, nil
}

func (s *Saver) batchInsertSports(ctx context.Context, data any) (int64, error) {
	// struct matching internal/services/plymkt.go
	// But wait, the services package might not have the simplified struct for 'sports' table
	// The processor produces data. We assume it matches services definitions or a local struct.
	// Let's assume we reuse PlyMktTag or a custom struct for sports?
	// Reviewer note: `tester-concurrent.go` uses `Sport` struct: ID, Tag, Slug.
	// The Processor needs to send this. Let's assume it sends `[]*services.PlyMktSportCategory` (we need to define this if not exists)
	// Or we reuse `PlyMktSport` but that's for 'leagues'.
	// Let's use `services.PlyMktSportCategory`.

	sports, ok := data.([]services.PlyMktSportCategory)
	if !ok {
		return 0, fmt.Errorf("invalid data type for sports: got %T", data)
	}

	batch := &pgx.Batch{}
	for _, sport := range sports {
		// Upsert and Return ID to update cache?
		// Batch operations don't easily return values for mapping back to inputs in a simple way
		// unless we process them one by one or trust the slug.
		// For the CACHE, we need the IDs.
		// Strategy: Upsert all. Then Query all slugs we just inserted to get their IDs.
		// Or: Use `RETURNING slug, id`.
		// Strategy: Ensure the tag exists (stub) before inserting the sport to satisfy FK.
		if sport.PrimaryTagID != "" {
			// Pre-insert tag stub to ensure FK constraint met immediately
			// The actual tag definition will update this later or has already done so.
			batch.Queue(`
				INSERT INTO tags (id) VALUES ($1) ON CONFLICT (id) DO NOTHING
			`, sport.PrimaryTagID)
		}

		batch.Queue(`
			INSERT INTO sports (slug, primary_tag_id)
			VALUES ($1, $2)
			ON CONFLICT (slug) DO UPDATE SET
				primary_tag_id = EXCLUDED.primary_tag_id
			RETURNING slug, id
		`, sport.Slug, sport.PrimaryTagID)
	}

	// EXECUTE and Capture IDs
	results := s.db.SendBatch(ctx, batch)

	// We must defer cache updates until AFTER the batch is fully processed (and implicitly committed).
	// Otherwise, concurrent workers might try to use the ID before it's visible in DB.
	type update struct {
		slug         string
		id           string
		primaryTagID *string
	}
	var updates []update

	var totalAffected int64
	for i := 0; i < len(sports); i++ {
		// If we queued a stub insert, we must consume its result (Exec)
		if sports[i].PrimaryTagID != "" {
			if _, err := results.Exec(); err != nil {
				results.Close()
				return totalAffected, fmt.Errorf("tag stub upsert at index %d: %w", i, err)
			}
		}

		var slug, id string
		err := results.QueryRow().Scan(&slug, &id)
		if err != nil {
			results.Close() // Close before return ensuring tx rollback/cleanup
			return totalAffected, fmt.Errorf("sport upsert at index %d: %w", i, err)
		}

		var primaryTagID *string
		if sports[i].PrimaryTagID != "" {
			pid := sports[i].PrimaryTagID
			primaryTagID = &pid
		}
		updates = append(updates, update{slug, id, primaryTagID})
		totalAffected++
	}
	results.Close() // Commit implicit transaction

	// Update Cache now that data is committed
	for _, u := range updates {
		s.setSportID(u.slug, u.id, u.primaryTagID)
	}

	return totalAffected, nil
}

func (s *Saver) batchInsertTagsDefinitions(ctx context.Context, data any) (int64, error) {
	tags, ok := data.([]services.PlyMktTag)
	if !ok {
		return 0, fmt.Errorf("invalid data type for tags definitions: got %T", data)
	}

	batch := &pgx.Batch{}
	for _, t := range tags {
		// Only insert/update definition fields: label, slug, force_show, force_hide
		// Ignore relationships.
		batch.Queue(`
			INSERT INTO tags (id, label, slug, force_show, force_hide)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (id) DO UPDATE SET
				label = EXCLUDED.label,
				slug = EXCLUDED.slug,
				force_show = EXCLUDED.force_show,
				force_hide = EXCLUDED.force_hide
		`, t.ID, t.Label, t.Slug, t.ForceShow, t.ForceHide)
	}

	rowsAffected, failIdx, err := s.execBatch(ctx, batch, len(tags))
	if err != nil {
		if failIdx != -1 && failIdx < len(tags) {
			t := tags[failIdx]
			s.logger.Error("failed to save tag definition",
				slog.String("id", t.ID),
				slog.String("slug", t.Slug),
				slog.String("error", err.Error()),
			)
		}
		return rowsAffected, err
	}
	return rowsAffected, nil
}

func (s *Saver) batchInsertTagsSportLink(ctx context.Context, data any) (int64, error) {
	tags, ok := data.([]services.PlyMktTag)
	if !ok {
		return 0, fmt.Errorf("invalid data type for tags sport link: got %T", data)
	}

	batch := &pgx.Batch{}
	for _, t := range tags {
		var sportID *string
		if t.SportSlug != "" {
			if id, found := s.getSportID(t.SportSlug); found {
				sportID = &id
			}
		} else if t.SportID != "" {
			sid := t.SportID
			sportID = &sid
		}

		if sportID == nil {
			// Skip if we can't resolve sport. Or should we insert with null?
			// Ideally we want to link it. If missing, maybe retry or skip.
			// For now, allow null to ensure ID exists, but update won't do much.
			// Actually, if we can't resolve sport, it's useless for "SportLink".
			s.logger.Debug("skipping tag sport link - sport unresolved",
				slog.String("tag_id", t.ID),
				slog.String("sport_slug", t.SportSlug))
			continue
		}

		batch.Queue(`
			INSERT INTO tags (id, sport_id)
			VALUES ($1, $2)
			ON CONFLICT (id) DO UPDATE SET
				sport_id = EXCLUDED.sport_id
		`, t.ID, sportID)
	}

	rowsAffected, failIdx, err := s.execBatch(ctx, batch, len(tags))
	if err != nil {
		if failIdx != -1 && failIdx < len(tags) {
			t := tags[failIdx]
			s.logger.Error("failed to save tag sport link",
				slog.String("id", t.ID),
				slog.String("error", err.Error()),
			)
		}
		return rowsAffected, err
	}
	return rowsAffected, nil
}

func (s *Saver) batchInsertTagsHierarchy(ctx context.Context, data any) (int64, error) {
	tags, ok := data.([]services.PlyMktTag)
	if !ok {
		return 0, fmt.Errorf("invalid data type for tags hierarchy: got %T", data)
	}

	batch := &pgx.Batch{}
	for _, t := range tags {
		var sportID *string
		if t.SportSlug != "" {
			if id, found := s.getSportID(t.SportSlug); found {
				sportID = &id
			}
		}

		var parentID *string
		if t.ParentTagID != "" {
			pid := t.ParentTagID
			parentID = &pid
		}

		// Hierarchy updates parent_tag_id and potentially sport_id (if derived from league)
		batch.Queue(`
			INSERT INTO tags (id, parent_tag_id, sport_id)
			VALUES ($1, $2, $3)
			ON CONFLICT (id) DO UPDATE SET
				parent_tag_id = EXCLUDED.parent_tag_id,
				sport_id = COALESCE(EXCLUDED.sport_id, tags.sport_id) 
		`, t.ID, parentID, sportID)
	}

	rowsAffected, failIdx, err := s.execBatch(ctx, batch, len(tags))
	if err != nil {
		if failIdx != -1 && failIdx < len(tags) {
			t := tags[failIdx]
			s.logger.Error("failed to save tag hierarchy",
				slog.String("id", t.ID),
				slog.String("error", err.Error()),
			)
		}
		return rowsAffected, err
	}
	return rowsAffected, nil
}

func (s *Saver) batchInsertLeagues(ctx context.Context, data any) (int64, error) {
	leagues, ok := data.([]services.PlyMktSport)
	if !ok {
		return 0, fmt.Errorf("invalid data type for leagues: got %T", data)
	}

	batch := &pgx.Batch{}
	for _, l := range leagues {
		// Try resolving sport slug if missing
		if l.SportSlug == "" {
			l.SportSlug = s.findSportForLeague(&l)
		}

		// Resolve SportID
		var sportID *string
		if l.SportSlug != "" {
			if id, found := s.getSportID(l.SportSlug); found {
				sportID = &id
			}
		}

		batch.Queue(`
			INSERT INTO leagues (sport, image, resolution, ordering, raw_tags, series, sport_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (sport) DO UPDATE SET
				image = EXCLUDED.image,
				resolution = EXCLUDED.resolution,
				ordering = EXCLUDED.ordering,
				raw_tags = EXCLUDED.raw_tags,
				series = EXCLUDED.series,
				sport_id = EXCLUDED.sport_id
		`, l.Sport, l.Image, l.Resolution, l.Ordering, l.Tags, l.Series, sportID)
	}

	// ... (inside batchInsertLeagues)
	rowsAffected, failIdx, err := s.execBatch(ctx, batch, len(leagues))
	if err != nil {
		if failIdx != -1 && failIdx < len(leagues) {
			l := leagues[failIdx]
			s.logger.Error("failed to save league",
				slog.String("sport", l.Sport),
				slog.String("error", err.Error()),
			)
		}
		return rowsAffected, err
	}
	return rowsAffected, nil
}

func (s *Saver) batchInsertTeams(ctx context.Context, data any) (int64, error) {
	teams, ok := data.([]services.PlyMktTeam)
	if !ok {
		return 0, fmt.Errorf("invalid data type for teams: got %T", data)
	}

	batch := &pgx.Batch{}
	for _, t := range teams {
		// Try resolving sport slug if missing
		if t.SportSlug == "" {
			t.SportSlug = s.findSportForTeam(&t)
		}

		// Resolve SportID
		var sportID *string
		if t.SportSlug != "" {
			if id, found := s.getSportID(t.SportSlug); found {
				sportID = &id
			}
		}

		batch.Queue(`
			INSERT INTO teams (id, name, league, record, logo, abbreviation, alias, provider_id, color, sport_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (id) DO UPDATE SET
				name = EXCLUDED.name,
				league = EXCLUDED.league,
				record = EXCLUDED.record,
				logo = EXCLUDED.logo,
				abbreviation = EXCLUDED.abbreviation,
				alias = EXCLUDED.alias,
				provider_id = EXCLUDED.provider_id,
				color = EXCLUDED.color,
				sport_id = EXCLUDED.sport_id
		`, t.ID, t.Name, t.League, t.Record, t.Logo, t.Abbreviation, t.Alias, t.ProviderID, t.Color, sportID)
	}

	// ... (inside batchInsertTeams)
	rowsAffected, failIdx, err := s.execBatch(ctx, batch, len(teams))
	if err != nil {
		if failIdx != -1 && failIdx < len(teams) {
			t := teams[failIdx]
			s.logger.Error("failed to save team",
				slog.String("id", fmt.Sprintf("%d", t.ID)),
				slog.String("name", t.Name),
				slog.String("error", err.Error()),
			)
		}
		return rowsAffected, err
	}
	return rowsAffected, nil
}

func (s *Saver) batchInsertLeagueHierarchy(ctx context.Context, data any) (int64, error) {
	leagues, ok := data.([]services.PlyMktSport)
	if !ok {
		return 0, fmt.Errorf("invalid data type for league hierarchy: got %T", data)
	}

	batch := &pgx.Batch{}
	totalOps := 0

	for _, l := range leagues {
		// 1. Resolve Sport Slug
		sSlug := l.SportSlug
		if sSlug == "" {
			sSlug = s.findSportForLeague(&l)
		}
		if sSlug == "" {
			continue
		}

		// 2. Get Sport Info from Cache
		s.cacheMu.RLock()
		sport, cached := s.sportCache[sSlug]
		s.cacheMu.RUnlock()

		if !cached {
			// If not in cache, we can't reliably determine root structure.
			// Ideally we retry or warn.
			s.logger.Warn("sport not found in cache for hierarchy", slog.String("slug", sSlug))
			continue
		}

		primaryTagID := ""
		if sport.PrimaryTagID != nil {
			primaryTagID = *sport.PrimaryTagID
		}
		sportID := sport.UUID

		// 3. Reconstruct Chain (Logic: SportTag -> Tag1 -> Tag2 per definition)
		currTags := strings.Split(l.Tags, ",")
		cleaned := make([]string, 0, len(currTags)+1)

		// Root is correctly the Sport Primary Tag
		if primaryTagID != "" {
			cleaned = append(cleaned, primaryTagID)
		}

		gamesTagID := "100639"
		for _, id := range currTags {
			id = strings.TrimSpace(id)
			if id != "" && id != primaryTagID && id != "1" && id != gamesTagID {
				cleaned = append(cleaned, id)
			}
		}

		// 4. Queue Updates
		for i := 0; i < len(cleaned); i++ {
			tagID := cleaned[i]
			var parentID *string

			// Chain parent to the previous tag in the list
			if i > 0 {
				pid := cleaned[i-1]
				parentID = &pid
			}

			// We insert stub if missing, or update if exists.
			// This ensures the tag exists even if definition fetch is lagging.
			batch.Queue(`
				INSERT INTO tags (id, parent_tag_id, sport_id)
				VALUES ($1, $2, $3)
				ON CONFLICT (id) DO UPDATE SET
					parent_tag_id = EXCLUDED.parent_tag_id,
					sport_id = EXCLUDED.sport_id
			`, tagID, parentID, sportID)
			totalOps++
		}
	}

	rowsAffected, _, err := s.execBatch(ctx, batch, totalOps)
	return rowsAffected, err
}

// execBatch executes a batch and returns total rows affected, the index of failure (if applicable), and error.
func (s *Saver) execBatch(ctx context.Context, batch *pgx.Batch, count int) (int64, int, error) {
	results := s.db.SendBatch(ctx, batch)
	defer results.Close()

	var totalAffected int64
	for i := 0; i < count; i++ {
		ct, err := results.Exec()
		if err != nil {
			return totalAffected, i, fmt.Errorf("batch exec at index %d: %w", i, err)
		}
		totalAffected += ct.RowsAffected()
	}

	return totalAffected, -1, nil
}

// batchInsertConditions inserts or updates conditions.
func (s *Saver) batchInsertConditions(ctx context.Context, data any) (int64, error) {
	items, ok := data.([]services.PlyMktCondition)
	if !ok {
		return 0, fmt.Errorf("invalid data type for conditions: got %T", data)
	}

	batch := &pgx.Batch{}
	for _, item := range items {
		batch.Queue(`
			INSERT INTO conditions (id, oracle, outcome_slot_count, payout_denominator, payout_numerators, payouts, question_id, resolution_hash, resolution_timestamp, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
			ON CONFLICT (id) DO UPDATE SET
				oracle = EXCLUDED.oracle,
				outcome_slot_count = EXCLUDED.outcome_slot_count,
				payout_denominator = EXCLUDED.payout_denominator,
				payout_numerators = EXCLUDED.payout_numerators,
				payouts = EXCLUDED.payouts,
				question_id = EXCLUDED.question_id,
				resolution_hash = EXCLUDED.resolution_hash,
				resolution_timestamp = EXCLUDED.resolution_timestamp,
				updated_at = NOW()
		`,
			item.ID,
			item.Oracle,
			fmt.Sprintf("%d", item.OutcomeSlotCount), // Map int to TEXT as per schema comment or type
			item.PayoutDenominator,
			item.PayoutNumerators,
			item.Payouts,
			item.QuestionId,
			item.ResolutionHash,
			item.ResolutionTimestamp,
		)
	}

	totalAffected, failIdx, err := s.execBatch(ctx, batch, len(items))
	if err != nil && failIdx != -1 && failIdx < len(items) {
		s.logger.Error("failed to save condition", "id", items[failIdx].ID, "error", err)
	}
	return totalAffected, err
}

// batchInsertAccounts inserts or updates accounts.
// batchInsertAccounts inserts or updates accounts.
func (s *Saver) batchInsertAccounts(ctx context.Context, data any) (int64, error) {
	items, ok := data.([]services.PlyMktAccount)
	if !ok {
		return 0, fmt.Errorf("invalid data type for accounts: got %T", data)
	}

	batch := &pgx.Batch{}
	for _, item := range items {
		// Convert Timestamps
		creationTS := s.parseUnixTimestamp(item.CreationTimestamp)
		lastSeenTS := s.parseUnixTimestamp(item.LastSeenTimestamp)
		lastTradedTS := s.parseUnixTimestamp(item.LastTradedTimestamp)

		batch.Queue(`
			INSERT INTO accounts (id, creation_timestamp, last_seen_timestamp, last_traded_timestamp, collateral_volume, num_trades, profit, scaled_collateral_volume, scaled_profit, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
			ON CONFLICT (id) DO UPDATE SET
				creation_timestamp = EXCLUDED.creation_timestamp,
				last_seen_timestamp = EXCLUDED.last_seen_timestamp,
				last_traded_timestamp = EXCLUDED.last_traded_timestamp,
				collateral_volume = EXCLUDED.collateral_volume,
				num_trades = EXCLUDED.num_trades,
				profit = EXCLUDED.profit,
				scaled_collateral_volume = EXCLUDED.scaled_collateral_volume,
				scaled_profit = EXCLUDED.scaled_profit,
				updated_at = NOW()
		`,
			item.ID,
			creationTS,
			lastSeenTS,
			lastTradedTS,
			item.CollateralVolume,
			item.NumTrades,
			item.Profit,
			item.ScaledCollateralVolume,
			item.ScaledProfit,
		)
	}

	totalAffected, failIdx, err := s.execBatch(ctx, batch, len(items))
	if err != nil && failIdx != -1 && failIdx < len(items) {
		s.logger.Error("failed to save account", "id", items[failIdx].ID, "error", err)
	}
	return totalAffected, err
}

// parseUnixTimestamp converts a unix timestamp string (seconds) to *time.Time.
// Returns nil if string is empty or invalid.
func (s *Saver) parseUnixTimestamp(tsStr string) *time.Time {
	if tsStr == "" || tsStr == "0" {
		return nil
	}
	// Subgraph often returns numeric strings "1600..."
	// Sometimes it might return "0" for null
	val, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return nil
	}
	if val == 0 {
		return nil
	}
	t := time.Unix(val, 0)
	return &t
}

// batchIncrementAccounts increments account statistics (collateral_volume, num_trades).
func (s *Saver) batchIncrementAccounts(ctx context.Context, data any) (int64, error) {
	items, ok := data.([]services.PlyMktAccount)
	if !ok {
		return 0, fmt.Errorf("invalid data type for accounts_increment: got %T", data)
	}

	// Sort by ID to prevent deadlocks from inconsistent lock ordering
	sort.Slice(items, func(i, j int) bool {
		return items[i].ID < items[j].ID
	})

	batch := &pgx.Batch{}
	for _, item := range items {
		// Parse numeric values from string fields
		vol, _ := strconv.ParseFloat(item.CollateralVolume, 64)
		trades, _ := strconv.ParseInt(item.NumTrades, 10, 64)

		// Convert to timestamptz using helper
		ts := s.parseUnixTimestamp(item.LastTradedTimestamp)

		batch.Queue(`
			INSERT INTO accounts (id, collateral_volume, num_trades, last_traded_timestamp, updated_at)
			VALUES ($1, $2::text, $3::text, $4, NOW())
			ON CONFLICT (id) DO UPDATE SET
				collateral_volume = (COALESCE(accounts.collateral_volume::numeric, 0) + EXCLUDED.collateral_volume::numeric)::text,
				num_trades = (COALESCE(accounts.num_trades::bigint, 0) + EXCLUDED.num_trades::bigint)::text,
				last_traded_timestamp = GREATEST(accounts.last_traded_timestamp, EXCLUDED.last_traded_timestamp),
				updated_at = NOW()
		`, item.ID, fmt.Sprintf("%.6f", vol), fmt.Sprintf("%d", trades), ts)
	}

	totalAffected, failIdx, err := s.execBatch(ctx, batch, len(items))
	if err != nil && failIdx != -1 && failIdx < len(items) {
		s.logger.Error("failed to increment account", "id", items[failIdx].ID, "error", err)
	}
	return totalAffected, err
}

func (s *Saver) batchInsertOrderFilledEvents(ctx context.Context, data any) (int64, error) {
	items, ok := data.([]services.PlyMktOrderFilledEvent)
	if !ok {
		return 0, fmt.Errorf("invalid data type for order_filled_events: got %T", data)
	}

	batch := &pgx.Batch{}
	for _, item := range items {
		ts := s.parseUnixTimestamp(item.Timestamp)
		batch.Queue(`
			INSERT INTO order_filled_events (id, maker_asset_id, taker_asset_id, maker_amount_filled, taker_amount_filled, maker_id, taker_id, fee, timestamp, transaction_hash, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
			ON CONFLICT (id) DO NOTHING
		`,
			item.ID,
			item.MakerAssetID,
			item.TakerAssetID,
			item.MakerAmountFilled,
			item.TakerAmountFilled,
			item.Maker.ID,
			item.Taker.ID,
			item.Fee,
			ts,
			item.ID, // Transaction hash is ID? Wait, struct has TransactionHash too. Let's use ID as primary key.
		)
	}

	totalAffected, failIdx, err := s.execBatch(ctx, batch, len(items))
	if err != nil && failIdx != -1 && failIdx < len(items) {
		s.logger.Error("failed to save order_filled_event", "id", items[failIdx].ID, "error", err)
	}
	return totalAffected, err
}

// batchInsertEnrichedOrderFilledEvents inserts or updates enriched_order_filled_events.
func (s *Saver) batchInsertEnrichedOrderFilledEvents(ctx context.Context, data any) (int64, error) {
	items, ok := data.([]services.PlyMktEnrichedOrderFilledEvent)
	if !ok {
		return 0, fmt.Errorf("invalid data type for enriched_order_filled_events: got %T", data)
	}

	batch := &pgx.Batch{}
	for _, item := range items {
		ts := s.parseUnixTimestamp(item.Timestamp)
		batch.Queue(`
			INSERT INTO enriched_order_filled_events (id, price, side, size, maker_id, taker_id, market_id, timestamp, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
			ON CONFLICT (id, timestamp) DO NOTHING
		`,
			item.ID,
			item.Price,
			item.Side,
			item.Size,
			item.Maker.ID,
			item.Taker.ID,
			item.Market.ID,
			ts,
		)
	}

	s.logger.Debug("saving enriched order filled events", slog.Int("count", len(items)))
	totalAffected, failIdx, err := s.execBatch(ctx, batch, len(items))
	if err != nil {
		if failIdx != -1 && failIdx < len(items) {
			s.logger.Error("failed to save enriched_order_filled_event",
				slog.String("id", items[failIdx].ID),
				slog.Any("error", err),
			)
		} else {
			s.logger.Error("failed to save enriched_order_filled_events", slog.Any("error", err))
		}
	}
	return totalAffected, err
}

// batchInsertPositionSnapshots inserts position snapshots with computed deltas.
// Uses Read-Compute-Write pattern: fetches latest position, computes delta, writes new snapshot.
func (s *Saver) batchInsertPositionSnapshots(ctx context.Context, data any) (int64, error) {
	items, ok := data.([]services.PlyMktUserPosition)
	if !ok {
		return 0, fmt.Errorf("invalid data type for position_snapshots: got %T", data)
	}

	if len(items) == 0 {
		return 0, nil
	}

	// Step 1: Build lookup keys for latest positions
	keys := make([]posKey, 0, len(items))
	for _, item := range items {
		keys = append(keys, posKey{AccountID: item.User, MarketID: item.TokenID})
	}

	// Step 2: Fetch latest positions for comparison (batch query)
	latestPositions := make(map[posKey]struct {
		Quantity float64
		Value    float64
	})

	// Query for latest snapshot per account+market
	rows, err := s.db.Query(ctx, `
		SELECT DISTINCT ON (account_id, market_id)
			account_id, market_id, net_quantity, net_value
		FROM position_snapshots
		WHERE (account_id, market_id) IN (SELECT unnest($1::text[]), unnest($2::text[]))
		ORDER BY account_id, market_id, snapshot_time DESC
	`, s.extractAccountIDs(keys), s.extractMarketIDs(keys))
	if err != nil {
		return 0, fmt.Errorf("failed to query latest positions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var accID, mktID string
		var qty, val float64
		if err := rows.Scan(&accID, &mktID, &qty, &val); err == nil {
			latestPositions[posKey{accID, mktID}] = struct {
				Quantity float64
				Value    float64
			}{qty, val}
		}
	}

	// Step 3: Insert new snapshots with computed deltas
	batch := &pgx.Batch{}
	now := time.Now()

	for _, item := range items {
		key := posKey{AccountID: item.User, MarketID: item.TokenID}

		// Parse current values
		qty, _ := strconv.ParseFloat(item.Amount, 64)
		// Value might be 0 if not returned by subgraph, use RealizedPnl?
		// Or if simple quantity tracking, value is less important or need price.
		// For now, let's use 0 if NetValue is missing or try to derive.
		// Queries return realizedPnl/avgPrice/totalBought.
		// Current Value ~= amount * current_price (not available here)
		// OR avgPrice * amount? That's cost basis.
		// Let's rely on Amount (qty) mostly.
		val := 0.0 // Placeholder as netValue is not in query

		// OutcomeIndex is not in query anymore, default to 0 or parse from TokenID if possible?
		// Using 0 for now as it's often not critical for whale flow unless split.
		outcomeIdx := 0

		// Compute deltas
		deltaQty := qty
		deltaVal := val
		if prev, exists := latestPositions[key]; exists {
			deltaQty = qty - prev.Quantity
			deltaVal = val - prev.Value
		}

		// Only insert if there's a meaningful change (or first snapshot)
		if deltaQty == 0 && deltaVal == 0 {
			continue
		}

		batch.Queue(`
			INSERT INTO position_snapshots 
				(snapshot_time, account_id, market_id, outcome_index, net_quantity, net_value, delta_quantity, delta_value)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		`, now, item.User, item.TokenID, outcomeIdx, qty, val, deltaQty, deltaVal)
	}

	if batch.Len() == 0 {
		return 0, nil
	}

	totalAffected, failIdx, err := s.execBatch(ctx, batch, batch.Len())
	if err != nil && failIdx != -1 {
		s.logger.Error("failed to save position snapshot", "error", err)
	}
	return totalAffected, err
}

// posKey is a lookup key for account+market position pairs
type posKey struct {
	AccountID string
	MarketID  string
}

// Helper to extract account IDs for batch query
func (s *Saver) extractAccountIDs(keys []posKey) []string {
	result := make([]string, len(keys))
	for i, k := range keys {
		result[i] = k.AccountID
	}
	return result
}

// Helper to extract market IDs for batch query
func (s *Saver) extractMarketIDs(keys []posKey) []string {
	result := make([]string, len(keys))
	for i, k := range keys {
		result[i] = k.MarketID
	}
	return result
}

// batchInsertOrderbooks inserts orderbook snapshots.
func (s *Saver) batchInsertOrderbooks(ctx context.Context, data any) (int64, error) {
	var items []services.PlyMktOrderbook

	switch v := data.(type) {
	case []services.PlyMktOrderbook:
		items = v
	case services.PlyMktOrderbook:
		items = []services.PlyMktOrderbook{v}
	default:
		return 0, fmt.Errorf("invalid data type for orderbooks: got %T", data)
	}

	if len(items) == 0 {
		return 0, nil
	}

	batch := &pgx.Batch{}
	now := time.Now()

	for _, item := range items {
		// Marshal bids and asks to JSONB
		bidsJSON, _ := json.Marshal(item.Bids)
		asksJSON, _ := json.Marshal(item.Asks)

		batch.Queue(`
			INSERT INTO orderbooks (timestamp, token_id, bids, asks, spread)
			VALUES ($1, $2, $3, $4, $5)
		`, now, item.TokenID, bidsJSON, asksJSON, item.Spread)
	}

	totalAffected, failIdx, err := s.execBatch(ctx, batch, len(items))
	if err != nil && failIdx != -1 {
		s.logger.Error("failed to save orderbook", "error", err)
	}
	return totalAffected, err
}

// batchInsertUsers inserts user/profile data.
func (s *Saver) batchInsertUsers(ctx context.Context, data any) (int64, error) {
	items, ok := data.([]services.PlyMktUser)
	if !ok {
		return 0, fmt.Errorf("invalid data type for users: got %T", data)
	}

	// Sort items by ProxyWallet to prevent deadlocks
	sort.Slice(items, func(i, j int) bool {
		return items[i].ProxyWallet < items[j].ProxyWallet
	})

	batch := &pgx.Batch{}
	for _, item := range items {
		batch.Queue(`
			INSERT INTO plymkt_users (
				proxy_wallet, username, name, bio, profile_image, x_username, verified_badge,
				vol, pnl, rank, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CURRENT_TIMESTAMP)
			ON CONFLICT (proxy_wallet) DO UPDATE SET
				username = COALESCE(NULLIF(EXCLUDED.username, ''), plymkt_users.username),
				name = COALESCE(NULLIF(EXCLUDED.name, ''), plymkt_users.name),
				bio = COALESCE(NULLIF(EXCLUDED.bio, ''), plymkt_users.bio),
				profile_image = COALESCE(NULLIF(EXCLUDED.profile_image, ''), plymkt_users.profile_image),
				x_username = COALESCE(NULLIF(EXCLUDED.x_username, ''), plymkt_users.x_username),
				verified_badge = COALESCE(EXCLUDED.verified_badge, plymkt_users.verified_badge),
				vol = COALESCE(EXCLUDED.vol, plymkt_users.vol),
				pnl = COALESCE(EXCLUDED.pnl, plymkt_users.pnl),
				rank = COALESCE(EXCLUDED.rank, plymkt_users.rank),
				updated_at = CURRENT_TIMESTAMP
		`,
			item.ProxyWallet, item.Username, item.Name, item.Bio, item.ProfileImage,
			item.XUsername, item.VerifiedBadge, item.Vol, item.Pnl, item.Rank,
		)
	}

	rows, _, err := s.execBatch(ctx, batch, len(items))
	return rows, err
}

// batchInsertHolders inserts holder records.
func (s *Saver) batchInsertHolders(ctx context.Context, data any) (int64, error) {
	items, ok := data.([]services.PlyMktHolderRecord)
	if !ok {
		return 0, fmt.Errorf("invalid data type for holders: got %T", data)
	}

	// Sort items by TokenID + ProxyWallet to prevent deadlocks
	sort.Slice(items, func(i, j int) bool {
		if items[i].TokenID != items[j].TokenID {
			return items[i].TokenID < items[j].TokenID
		}
		return items[i].ProxyWallet < items[j].ProxyWallet
	})

	batch := &pgx.Batch{}
	for _, item := range items {
		batch.Queue(`
			INSERT INTO plymkt_holders (
				token_id, proxy_wallet, amount, updated_at
			) VALUES ($1, $2, $3, $4)
			ON CONFLICT (token_id, proxy_wallet) DO UPDATE SET
				amount = EXCLUDED.amount,
				updated_at = EXCLUDED.updated_at
		`,
			item.TokenID, item.ProxyWallet, item.Amount, item.UpdatedAt,
		)
	}

	rows, _, err := s.execBatch(ctx, batch, len(items))
	return rows, err
}

// batchInsertHoldersBundle handles atomic save of users and holders.
func (s *Saver) batchInsertHoldersBundle(ctx context.Context, data any) (int64, error) {
	bundle, ok := data.(services.PlyMktHoldersBundle)
	if !ok {
		return 0, fmt.Errorf("invalid data type for holders bundle: got %T", data)
	}

	var totalAffected int64

	// 1. Insert Users first
	if len(bundle.Users) > 0 {
		n, err := s.batchInsertUsers(ctx, bundle.Users)
		if err != nil {
			return 0, fmt.Errorf("failed to save bundle users: %w", err)
		}
		totalAffected += n
	}

	// 2. Insert Holders
	if len(bundle.Holders) > 0 {
		n, err := s.batchInsertHolders(ctx, bundle.Holders)
		if err != nil {
			return totalAffected, fmt.Errorf("failed to save bundle holders: %w", err)
		}
		totalAffected += n
	}

	return totalAffected, nil
}

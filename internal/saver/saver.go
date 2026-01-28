// Package saver provides a database client wrapper for saving processed data.
// Uses the generic workerpool.Pool for worker management.
package saver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/logging"
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

// New creates and initializes a saver pool with a PostgreSQL connection.
func New(ctx context.Context, cfg *config.Config, numWorkers, qSize int) (*Saver, error) {
	logger := logging.Logger.With(
		slog.String("component", "saver"),
	)

	// Initialize pgx connection pool
	poolConfig, err := pgxpool.ParseConfig(cfg.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("parsing postgres config: %w", err)
	}

	db, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("creating postgres pool: %w", err)
	}

	s := &Saver{
		db:           db,
		logger:       logger,
		sportCache: make(map[string]*CachedSport),
	}

	// Pre-load cache
	if err := s.loadSportIDs(ctx); err != nil {
		// Log warning but continue, cache will populate as we go (or fail if strict)
		logger.Warn("failed to pre-load sport IDs", slog.String("error", err.Error()))
	}

	pool, err := workerpool.NewPool[*Record, *Result](ctx, "saver", numWorkers, qSize, logger, s.workerTask)
	if err != nil {
		db.Close()
		return nil, err
	}

	s.Pool = pool

	logger.Info("saver initialized",
		slog.Int("workers", numWorkers),
		slog.Int("queue_size", qSize),
	)

	return s, nil
}

// Close closes the database connection pool.
func (s *Saver) Close() {
	s.db.Close()
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
			UUID: id,
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

	s.logger.Info("saving record",
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
	s.stats.RecordsSaved.Add(int64(record.ItemCount))
	s.stats.RowsAffected.Add(rowsAffected)
	s.stats.TotalDuration.Add(int64(duration))

	s.logger.Info("saved record",
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

// =============================================================================
// Batch Insert Methods
// =============================================================================

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

	return s.execBatch(ctx, batch, len(tags))
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
			continue
		}

		batch.Queue(`
			INSERT INTO tags (id, sport_id)
			VALUES ($1, $2)
			ON CONFLICT (id) DO UPDATE SET
				sport_id = EXCLUDED.sport_id
		`, t.ID, sportID)
	}

	return s.execBatch(ctx, batch, len(tags))
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

	return s.execBatch(ctx, batch, len(tags))
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

	return s.execBatch(ctx, batch, len(leagues))
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

	return s.execBatch(ctx, batch, len(teams))
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

	return s.execBatch(ctx, batch, totalOps)
}

// execBatch executes a batch and returns total rows affected.
func (s *Saver) execBatch(ctx context.Context, batch *pgx.Batch, count int) (int64, error) {
	results := s.db.SendBatch(ctx, batch)
	defer results.Close()

	var totalAffected int64
	for i := 0; i < count; i++ {
		ct, err := results.Exec()
		if err != nil {
			return totalAffected, fmt.Errorf("batch exec at index %d: %w", i, err)
		}
		totalAffected += ct.RowsAffected()
	}

	return totalAffected, nil
}
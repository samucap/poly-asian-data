package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/services"
)

// Internal Sport struct for relationship mapping
type Sport struct {
	ID          string // UUID
	Tag         *services.PlyMktTag
	Slug        string
	RelatedTags []*services.PlyMktTag
	Leagues     []string
}

// potential tag_id for live events? but might need to be in selenim w cookie or something
// {
//"id": "102982",
//"label": "Top Navbar",
//"slug": "top-navbar",
//"forceShow": false,
//"createdAt": "2025-12-21T02:17:55.713778Z",
//"updatedAt": "2026-01-24T21:31:59.83094Z",
//"isCarousel": false,
//"requiresTranslation": false
//}
func main() {
	// 1. Setup Database
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	poolConfig, err := pgxpool.ParseConfig(cfg.PostgresURL)
	if err != nil {
		panic(err)
	}

	db, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	dropTables := true
	if err := createTables(ctx, db, dropTables); err != nil {
		panic(err)
	}

	// 2. Fetch and Process Data
	start := time.Now()

	// Define Sport Categories
	slugs := []string{
		"football", "basketball", "hockey", "tennis", "esports", "baseball",
		"soccer", "cricket", "rugby", "golf", "ufc", "formula1", "chess",
		"boxing", "pickleball",
	}
	gamesTag := "100639"

	// Step 1: Fetch all tags (paginated, sequential)
	fmt.Println("Fetching Tags...")
	tags, err := fetchTags()
	if err != nil {
		panic(err)
	}
	tagsMap := make(map[string]*services.PlyMktTag)
	for _, t := range tags {
		tagsMap[t.ID] = t
	}

	// Step 2: Initialize Sports Map from tags
	var mu sync.Mutex
	sportsCats := make(map[string]*Sport)
	for _, tag := range tags {
		if slices.Contains(slugs, tag.Slug) {
			sportsCats[tag.Slug] = &Sport{
				Tag:  tag,
				Slug: tag.Slug,
			}
			tag.SportID = sportsCats[tag.Slug].ID // Initially empty, updated later
		}
	}

	// Step 3: Concurrently enrich Sports with related tags from events
	fmt.Println("Enriching Sports with Events...")
	var wg sync.WaitGroup
	errChan := make(chan error, len(sportsCats)+1)

	// Enrich each sport's events
	for _, sport := range sportsCats {
		wg.Add(1)
		go func(s *Sport) {
			defer wg.Done()
			evs, err := fetchEvents(s.Tag.ID)
			if err != nil {
				errChan <- fmt.Errorf("failed to fetch events for %s: %w", s.Slug, err)
				return
			}
			for _, ev := range evs {
				for _, t := range ev.Tags {
					if t.ID == "1" || t.ID == gamesTag {
						continue
					}
					linkTagToSport(t.ID, s.Slug, tagsMap, sportsCats, &mu)
				}
			}
		}(sport)
	}

	// Enrich from games events
	wg.Add(1)
	go func() {
		defer wg.Done()
		evs, err := fetchEvents(gamesTag)
		if err != nil {
			errChan <- fmt.Errorf("failed to fetch events for games: %w", err)
			return
		}
		for _, ev := range evs {
			sportSlug := getSportFromEvTags(ev.Tags, slugs)
			if sportSlug == "" {
				continue
			}
			for _, t := range ev.Tags {
				if t.ID == "1" || t.ID == gamesTag {
					continue
				}
				linkTagToSport(t.ID, sportSlug, tagsMap, sportsCats, &mu)
			}
		}
	}()

	wg.Wait()
	close(errChan)
	for e := range errChan {
		if e != nil {
			panic(e)
		}
	}

	// Step 4: Concurrently fetch leagues and teams
	fmt.Println("Fetching Leagues and Teams...")
	var leagues []*services.PlyMktSport
	var teams []*services.PlyMktTeam
	wg = sync.WaitGroup{}
	errChan = make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		leagues, err = fetchLeagues()
		if err != nil {
			errChan <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		teams, err = fetchTeams()
		if err != nil {
			errChan <- err
		}
	}()

	wg.Wait()
	close(errChan)
	for e := range errChan {
		if e != nil {
			panic(e)
		}
	}

	// Step 5: Process leagues (chain parents, link to sports)
	for _, league := range leagues {
		sportSlug := findSportTags(league, sportsCats)
		if sportSlug == "" {
			continue
		}
		sport := sportsCats[sportSlug]

		// Chain parent tags (Top-down: Sport -> Tag1 -> Tag2 ...)
		currTags := strings.Split(league.Tags, ",")
		cleaned := make([]string, 0, len(currTags)+1)
		
		// 1. Sport is always the root/parent
		cleaned = append(cleaned, sport.Tag.ID)

		// 2. Append other tags, filtering out duplicates/ignored
		for _, id := range currTags {
			id = strings.TrimSpace(id)
			if id != "" && id != sport.Tag.ID && id != "1" && id != gamesTag {
				cleaned = append(cleaned, id)
			}
		}

		for i := 0; i < len(cleaned)-1; i++ {
			parentID := cleaned[i]
			childID := cleaned[i+1]
			
			mu.Lock()
			// Ensure both child and parent exist in our map (which means they will be inserted)
			if t, ok := tagsMap[childID]; ok {
				t.ParentTagID = parentID
				if _, ok := sportsCats[sportSlug]; ok && !slices.ContainsFunc(sportsCats[sportSlug].RelatedTags, func(existing *services.PlyMktTag) bool {
					return existing.ID == t.ID
				}) {
					sportsCats[sportSlug].RelatedTags = append(sportsCats[sportSlug].RelatedTags, t)
				}
			}
			mu.Unlock()
		}

		// Link all tags in chain to sport (sets SportID, appends to RelatedTags)
		for _, id := range cleaned {
			if id == "1" || id == gamesTag {
				continue
			}
			linkTagToSport(id, sportSlug, tagsMap, sportsCats, &mu)
		}
	}

	// Step 6: Process teams (link to sports, append leagues, link league tags)
	for _, team := range teams {
		sportSlug := findTeamSport(team, sportsCats)
		if sportSlug == "" {
			continue
		}
		sport := sportsCats[sportSlug]

		// Append league if unique
		if !slices.Contains(sport.Leagues, team.League) {
			sport.Leagues = append(sport.Leagues, team.League)
		}

		// Link league tag if found
		if t := findTag(team.League, tags); t != nil {
			linkTagToSport(t.ID, sportSlug, tagsMap, sportsCats, &mu)
		}
	}

	fmt.Printf("Data gathering and processing complete in %v\n", time.Since(start))

	// 3. Batch Upsert to DB
	tx, err := db.Begin(ctx)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback(ctx)

	// Phase 1: Upsert Sports
	batchSports := &pgx.Batch{}
	for _, s := range sportsCats {
		var primaryTagID *string
		if s.Tag != nil {
			id := s.Tag.ID
			primaryTagID = &id
		}
		batchSports.Queue(`
			INSERT INTO sports (slug, primary_tag_id)
			VALUES ($1, $2)
			ON CONFLICT (slug) DO UPDATE SET
				primary_tag_id = EXCLUDED.primary_tag_id
		`, s.Slug, primaryTagID)
	}

	fmt.Printf("Phase 1: Upserting %d sports...\n", batchSports.Len())
	brSports := tx.SendBatch(ctx, batchSports)
	for i := 0; i < batchSports.Len(); i++ {
		if _, err := brSports.Exec(); err != nil {
			brSports.Close()
			panic(fmt.Errorf("sports upsert failed at index %d: %w", i, err))
		}
	}
	brSports.Close()

	// Phase 2: Refresh Sports IDs from DB
	existingSportsMap := make(map[string]string) // slug -> id
	rows, err := tx.Query(ctx, "SELECT slug, id FROM sports")
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var slug, id string
		if err := rows.Scan(&slug, &id); err != nil {
			panic(err)
		}
		existingSportsMap[slug] = id
	}
	rows.Close()

	// Update in-memory sports with DB IDs and propagate
	for slug, sport := range sportsCats {
		if id, ok := existingSportsMap[slug]; ok {
			sport.ID = id
			if sport.Tag != nil {
				sport.Tag.SportID = id
			}
			for _, t := range sport.RelatedTags {
				t.SportID = id
			}
		} else {
			fmt.Printf("Warning: Sport %s not found in DB after upsert\n", slug)
		}
	}

	// Phase 3: Upsert Tags, Leagues, Teams
	batch := &pgx.Batch{}

	// Tags
	for _, t := range tagsMap {
		var sportID *string
		if t.SportID != "" {
			sid := t.SportID
			sportID = &sid
		}
		var parentID *string
		if t.ParentTagID != "" {
			pid := t.ParentTagID
			parentID = &pid
		}
		batch.Queue(`
			INSERT INTO tags (id, label, slug, force_show, force_hide, sport_id, parent_tag_id)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (id) DO UPDATE SET
				label = EXCLUDED.label,
				slug = EXCLUDED.slug,
				force_show = EXCLUDED.force_show,
				force_hide = EXCLUDED.force_hide,
				sport_id = EXCLUDED.sport_id,
				parent_tag_id = EXCLUDED.parent_tag_id
		`, t.ID, t.Label, t.Slug, t.ForceShow, t.ForceHide, sportID, parentID)
	}

	// Leagues
	for _, l := range leagues {
		// Refresh SportID using current in-memory (now with DB IDs)
		sportSlug := findSportTags(l, sportsCats)
		if sportSlug != "" {
			if s, ok := sportsCats[sportSlug]; ok {
				l.SportID = s.ID
			}
		}
		var sportID *string
		if l.SportID != "" {
			sid := l.SportID
			sportID = &sid
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

	// Teams
	for _, t := range teams {
		// Refresh SportID
		sportSlug := findTeamSport(t, sportsCats)
		if sportSlug != "" {
			if s, ok := sportsCats[sportSlug]; ok {
				t.SportID = s.ID
			}
		}
		var sportID *string
		if t.SportID != "" {
			sid := t.SportID
			sportID = &sid
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

	fmt.Printf("Phase 3: Upserting %d dependents (Tags, Leagues, Teams)...\n", batch.Len())
	br := tx.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			br.Close()
			panic(fmt.Errorf("dependents upsert failed at index %d: %w", i, err))
		}
	}
	br.Close()

	if err := tx.Commit(ctx); err != nil {
		panic(err)
	}

	fmt.Println("Done! Success.")
}

// =============================================================================
// Helper Functions
// =============================================================================

func createTables(ctx context.Context, db *pgxpool.Pool, drop bool) error {
	var queries []string

	if drop {
		queries = append(queries,
			`DROP TABLE IF EXISTS teams CASCADE;`,
			`DROP TABLE IF EXISTS leagues CASCADE;`,
			`DROP TABLE IF EXISTS tags CASCADE;`,
			`DROP TABLE IF EXISTS sports CASCADE;`,
		)
	}

	queries = append(queries,
		// 1. Function for auto-updating updated_at
		`CREATE OR REPLACE FUNCTION update_updated_at_column()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.updated_at = NOW();
			RETURN NEW;
		END;
		$$ language 'plpgsql';`,
	)

	tables := []string{
		// 2. Tables
		`CREATE TABLE IF NOT EXISTS sports (
			id UUID DEFAULT gen_random_uuid() UNIQUE,
			slug TEXT PRIMARY KEY NOT NULL,
			primary_tag_id TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS tags (
			id TEXT PRIMARY KEY,
			label TEXT,
			slug TEXT,
			force_show BOOLEAN,
			force_hide BOOLEAN,
			sport_id UUID REFERENCES sports(id),
			parent_tag_id TEXT REFERENCES tags(id) DEFERRABLE INITIALLY DEFERRED,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);`,
		// Deferrable Constraint to allow circular references during transaction
		`ALTER TABLE sports DROP CONSTRAINT IF EXISTS fk_sports_primary_tag;`,
		`ALTER TABLE sports ADD CONSTRAINT fk_sports_primary_tag 
			FOREIGN KEY (primary_tag_id) REFERENCES tags(id) 
			DEFERRABLE INITIALLY DEFERRED;`,

		`CREATE TABLE IF NOT EXISTS leagues (
			sport TEXT PRIMARY KEY,
			image TEXT,
			resolution TEXT,
			ordering TEXT,
			raw_tags TEXT,
			series TEXT,
			sport_id UUID REFERENCES sports(id),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);`,
		`CREATE TABLE IF NOT EXISTS teams (
			id INT PRIMARY KEY,
			name TEXT,
			league TEXT,
			record TEXT,
			logo TEXT,
			abbreviation TEXT,
			alias TEXT,
			provider_id INT,
			color TEXT,
			sport_id UUID REFERENCES sports(id),
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);`,

		// 3. Triggers
		`DROP TRIGGER IF EXISTS update_sports_updated_at ON sports;`,
		`CREATE TRIGGER update_sports_updated_at BEFORE UPDATE ON sports FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();`,
		
		`DROP TRIGGER IF EXISTS update_tags_updated_at ON tags;`,
		`CREATE TRIGGER update_tags_updated_at BEFORE UPDATE ON tags FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();`,

		`DROP TRIGGER IF EXISTS update_leagues_updated_at ON leagues;`,
		`CREATE TRIGGER update_leagues_updated_at BEFORE UPDATE ON leagues FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();`,

		`DROP TRIGGER IF EXISTS update_teams_updated_at ON teams;`,
		`CREATE TRIGGER update_teams_updated_at BEFORE UPDATE ON teams FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();`,
	}
	queries = append(queries, tables...)

	for _, q := range queries {
		if _, err := db.Exec(ctx, q); err != nil {
			return fmt.Errorf("query failed: %s: %w", q, err)
		}
	}
	return nil
}

func linkTagToSport(tagID, sportSlug string, tagsMap map[string]*services.PlyMktTag, sportsCats map[string]*Sport, mu *sync.Mutex) {
	mu.Lock()
	defer mu.Unlock()
	if t, ok := tagsMap[tagID]; ok {
		if sport, ok := sportsCats[sportSlug]; ok {
			t.SportID = sport.ID // Initially "", updated later
			// Do not set ParentTagID here; handled by chaining where applicable
			if !slices.ContainsFunc(sport.RelatedTags, func(existing *services.PlyMktTag) bool {
				return existing.ID == t.ID
			}) {
				sport.RelatedTags = append(sport.RelatedTags, t)
			}
		}
	}
}

func getSportFromEvTags(evTags []*services.PlyMktTag, slugs []string) string {
	for _, tag := range evTags {
		if slices.Contains(slugs, tag.Slug) {
			return tag.Slug
		}
	}
	return ""
}

func findTag(idOrSlug string, tags []*services.PlyMktTag) *services.PlyMktTag {
	for _, tag := range tags {
		if tag.ID == idOrSlug || tag.Slug == idOrSlug {
			return tag
		}
	}
	return nil
}

func findTeamSport(team *services.PlyMktTeam, sportsCats map[string]*Sport) string {
	defaults := map[string]string{
		"acn": "soccer", "bl2": "soccer", "scop": "soccer", "fr2": "soccer", "itsb": "soccer",
		"nba": "basketball", "wnba": "basketball", "ncaab": "basketball", "cbb": "basketball",
		"nhl": "hockey", "cfb": "football", "nfl": "football", "mlb": "baseball",
		"csgo": "esports", "starcraft2": "esports", "es2": "esports", "bnd": "esports",
		"bpl": "cricket", "cpl": "cricket", "wtc": "cricket", "odc": "cricket",
		"ecc": "cricket", "weth": "cricket", "eth": "cricket",
	}
	for slug, cat := range sportsCats {
		if slices.Contains(cat.Leagues, team.League) {
			return slug
		}
		if defaultSlug, ok := defaults[team.League]; ok && defaultSlug == slug {
			return slug
		}
		if slices.ContainsFunc(cat.RelatedTags, func(tag *services.PlyMktTag) bool {
			return tag.Slug == team.League
		}) {
			return slug
		}
	}
	if team.Logo != "" {
		parts := strings.Split(team.Logo, "/")
		for i, part := range parts {
			if part == "team_logos" && i+1 < len(parts) {
				if _, ok := sportsCats[parts[i+1]]; ok {
					return parts[i+1]
				}
			}
		}
	}
	return ""
}

func findSportTags(league *services.PlyMktSport, sportsCats map[string]*Sport) string {
	defaults := map[string]string{
		"acn": "soccer", "bl2": "soccer", "scop": "soccer", "fr2": "soccer", "itsb": "soccer",
		"nba": "basketball", "wnba": "basketball", "ncaab": "basketball", "cbb": "basketball",
		"nhl": "hockey", "cfb": "football", "nfl": "football", "mlb": "baseball",
		"csgo": "esports", "starcraft2": "esports", "es2": "esports", "bnd": "esports",
		"bpl": "cricket", "cpl": "cricket", "wtc": "cricket", "odc": "cricket",
		"ecc": "cricket", "weth": "cricket", "eth": "cricket",
	}
	tagIDs := strings.Split(league.Tags, ",")
	for slug, cat := range sportsCats {
		for _, tagID := range tagIDs {
			if tagID == cat.Tag.ID {
				return slug
			}
		}
		if strings.Contains(league.Resolution, cat.Tag.Slug) {
			return slug
		}
		if defaultSlug, ok := defaults[league.Sport]; ok && defaultSlug == slug {
			return slug
		}
	}
	return ""
}

func fetchTags() ([]*services.PlyMktTag, error) {
	baseURL := "https://gamma-api.polymarket.com/tags"
	limit := 300
	offset := 0
	var fullTags []*services.PlyMktTag
	for {
		params := url.Values{}
		params.Add("limit", strconv.Itoa(limit))
		params.Add("offset", strconv.Itoa(offset))
		resp, err := http.Get(baseURL + "?" + params.Encode())
		if err != nil {
			return nil, err
		}

		defer resp.Body.Close()
		var tags []*services.PlyMktTag
		if err := json.NewDecoder(resp.Body).Decode(&tags); err != nil {
			return nil, err
		}
		if len(tags) == 0 {
			break
		}
		fullTags = append(fullTags, tags...)
		offset += limit
	}
	return fullTags, nil
}

func fetchLeagues() ([]*services.PlyMktSport, error) {
	resp, err := http.Get("https://gamma-api.polymarket.com/sports")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var leagues []*services.PlyMktSport
	if err := json.NewDecoder(resp.Body).Decode(&leagues); err != nil {
		return nil, err
	}
	return leagues, nil
}

func fetchTeams() ([]*services.PlyMktTeam, error) {
	baseURL := "https://gamma-api.polymarket.com/teams"
	limit := 500
	offset := 0
	var fullTeams []*services.PlyMktTeam
	for {
		params := url.Values{}
		params.Add("limit", strconv.Itoa(limit))
		params.Add("offset", strconv.Itoa(offset))
		resp, err := http.Get(baseURL + "?" + params.Encode())
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		var teams []*services.PlyMktTeam
		if err := json.NewDecoder(resp.Body).Decode(&teams); err != nil {
			return nil, err
		}
		if len(teams) == 0 {
			break
		}
		fullTeams = append(fullTeams, teams...)
		offset += limit
	}
	return fullTeams, nil
}

func fetchEvents(tagID string) ([]*services.PlyMktEvent, error) {
	baseURL := "https://gamma-api.polymarket.com/events"
	limit := 500
	offset := 0
	var fullEvents []*services.PlyMktEvent
	for {
		params := url.Values{}
		params.Add("limit", strconv.Itoa(limit))
		params.Add("offset", strconv.Itoa(offset))
		params.Add("tag_id", tagID)
		params.Add("active", "true")
		params.Add("closed", "false")
		params.Add("include_chat", "false")
		params.Add("order", "id")
		params.Add("ascending", "false")
		if tagID != "100639" {
			params.Add("related_tags", "true")
		}
		resp, err := http.Get(baseURL + "?" + params.Encode())
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		var events []*services.PlyMktEvent
		if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
			return nil, err
		}
		if len(events) == 0 {
			break
		}
		fullEvents = append(fullEvents, events...)
		offset += limit
	}
	return fullEvents, nil
}
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

	// 2. Fetch All Data first
	start := time.Now()

	// Define Sport Categories
	slugs := []string{
		"football", "basketball", "hockey", "tennis", "esports", "baseball",
		"soccer", "cricket", "rugby", "golf", "ufc", "formula1", "chess",
		"boxing", "pickleball",
	}
	gamesTag := "100639"

	// Fetch Tags
	fmt.Println("Fetching Tags...")
	tags, err := fetchTags()
	if err != nil {
		panic(err)
	}
	tagsMap := make(map[string]*services.PlyMktTag)
	for _, t := range tags {
		tagsMap[t.ID] = t
	}

	// Initialize Sports Map
	sportsCats := map[string]*Sport{}
	for _, tag := range tags {
		if slices.Contains(slugs, tag.Slug) {
			// UUID generation should be handled by database
			sportsCats[tag.Slug] = &Sport{
				Tag:  tag,
				Slug: tag.Slug,
			}
			tag.SportID = sportsCats[tag.Slug].ID
		}
	}

	// Fetch Events and Link Related Tags
	// We do this for each sport category + the general "games" tag
	fmt.Println("Fetching Events & Linking Tags...")

	// Helper to link a tag to a sport
	linkTagToSport := func(tagID, sportSlug string) {
		if t, ok := tagsMap[tagID]; ok {
			// Prevent overwriting if already assigned (prioritize first assignment or specific logic?)
			// For now, simple assignment.
			if sport, exists := sportsCats[sportSlug]; exists {
				t.SportID = sport.ID
				t.ParentTagID = sport.Tag.ID
				// Avoid duplicates in RelatedTags
				if !slices.ContainsFunc(sport.RelatedTags, func(existing *services.PlyMktTag) bool {
					return existing.ID == t.ID
				}) {
					sport.RelatedTags = append(sport.RelatedTags, t)
				}
			}
		}
	}

	processEvents := func(tagID string) error {
		evs, err := fetchEvents(tagID)
		if err != nil {
			return err
		}
		for _, ev := range evs {
			// Find which sport this event belongs to based on its tags
			sportSlug := getSportFromEvTags(ev.Tags, slugs)
			if sportSlug == "" {
				continue
			}

			// Identify "related" tags (tags that are NOT the sport itself, NOT "games", etc.)
			for _, t := range ev.Tags {
				if t.ID == "1" || t.ID == "100639" || t.Slug == sportSlug {
					continue
				}
				// This tag is related to the sport
				linkTagToSport(t.ID, sportSlug)
			}
		}
		return nil
	}

	// Process each sport's events
	for _, cat := range sportsCats {
		if err := processEvents(cat.Tag.ID); err != nil {
			fmt.Printf("Error fetching events for %s: %v\n", cat.Slug, err)
			return
		}
	}
	// Process "games" events
	if err := processEvents(gamesTag); err != nil {
		fmt.Printf("Error fetching events for games: %v\n", err)
		return
	}

	// Fetch Leagues and Link
	fmt.Println("Fetching Leagues...")
	leagues, err := fetchLeagues()
	if err != nil {
		panic(err)
	}
	for _, league := range leagues {
		sportSlug := findSportTags(league, sportsCats)
		if sportSlug != "" {
			s := sportsCats[sportSlug]
			league.SportID = s.ID // Set UUID

			if !slices.Contains(s.Leagues, league.Sport) {
				s.Leagues = append(s.Leagues, league.Sport)
			}

			// Leagues also have a "raw_tags" csv, link those too
			currTags := strings.Split(league.Tags, ",")
			for _, tagID := range currTags {
				// Clean up tagID
				tagID = strings.TrimSpace(tagID)
				if tagID == "" || tagID == s.Tag.ID || tagID == "1" || tagID == "100639" {
					continue
				}
				linkTagToSport(tagID, sportSlug)
			}
		}
	}

	// Fetch Teams and Link
	fmt.Println("Fetching Teams...")
	teams, err := fetchTeams()
	if err != nil {
		panic(err)
	}
	for _, team := range teams {
		sportSlug := findTeamSport(team, sportsCats)
		if sportSlug != "" {
			s := sportsCats[sportSlug]
			team.SportID = s.ID

			if !slices.Contains(s.Leagues, team.League) {
				s.Leagues = append(s.Leagues, team.League)
				// If the league itself wasn't found in tags earlier, try to find it now?
				// Logic from original tester.go: if found := findTag(team.League)...
				// We can skip that specific intricate logic for now or implement if critical.
				// Preserving original logic simple attempt:
				if t := findTag(team.League, tags); t != nil {
					linkTagToSport(t.ID, sportSlug)
				}
			}
		} else {
			// fmt.Printf("Unknown team sport: %s (%s)\n", team.Name, team.League)
		}
	}

	fmt.Printf("Data gathering complete in %v\n", time.Since(start))

	// 3. Batch Upsert Everything
	// We split this into phases to handle UUID generation by the DB.
	// Phase 1: Upsert Sports to ensure they exist and trigger ID generation if needed.
	// Phase 2: Fetch all Sports IDs to update our in-memory references.
	// Phase 3: Upsert Tags, Leagues, and Teams with valid foreign keys.

	tx, err := db.Begin(ctx)
	if err != nil {
		panic(err)
	}
	defer tx.Rollback(ctx)

	// --- Phase 1: Sports ---
	batchSports := &pgx.Batch{}
	for _, s := range sportsCats {
		var primaryTagID *string
		if s.Tag != nil {
			id := s.Tag.ID
			primaryTagID = &id
		}
		// Note: we don't insert 'id' here, we let the DB generate it for new rows.
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
			panic(fmt.Errorf("sports Upsert failed at index %d: %w", i, err))
		}
	}
	brSports.Close()

	// --- Phase 2: Refresh IDs ---
	existingSportsMap := make(map[string]string) // Slug -> ID
	rows, err := tx.Query(ctx, "SELECT slug, id FROM sports")
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var s, i string
		if err := rows.Scan(&s, &i); err != nil {
			panic(err)
		}
		existingSportsMap[s] = i
	}
	rows.Close()

	// Update in-memory objects with real UUIDs
	for slug, sport := range sportsCats {
		if id, ok := existingSportsMap[slug]; ok {
			sport.ID = id
			// Propagate to self-tag
			if sport.Tag != nil {
				sport.Tag.SportID = id
			}
			// Propagate to related tags
			for _, t := range sport.RelatedTags {
				t.SportID = id
			}
			// Note: related tags in 'tagsMap' share pointers with 'sport.RelatedTags',
			// so they are effectively updated too.
		} else {
			fmt.Printf("Warning: Sport %s not found in DB after upsert?\n", slug)
		}
	}

	// --- Phase 3: Dependents ---
	batch := &pgx.Batch{}

	// Queue Tags
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

	// Queue Leagues
	for _, l := range leagues {
		// Ensure Sport ID is fresh
		if slug := findSportTags(l, sportsCats); slug != "" {
			if s, ok := sportsCats[slug]; ok {
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

	// Queue Teams
	for _, t := range teams {
		// Ensure Sport ID is fresh
		if slug := findTeamSport(t, sportsCats); slug != "" {
			if s, ok := sportsCats[slug]; ok {
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

	fmt.Printf("Phase 3: Queueing %d dependent operations (Tags, Leagues, Teams). Executing batch...\n", batch.Len())
	br := tx.SendBatch(ctx, batch)

	for i := 0; i < batch.Len(); i++ {
		_, err := br.Exec()
		if err != nil {
			br.Close()
			panic(fmt.Errorf("batch execution failed at index %d: %w", i, err))
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

// --- Fetch Functions (Refined from original) ---

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
		if tag.ID == idOrSlug {
			return tag
		}
	}
	for _, tag := range tags {
		if tag.Slug == idOrSlug {
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
	for _, cat := range sportsCats {
		if slices.Contains(cat.Leagues, team.League) {
			return cat.Slug
		}
		if defaultSlug, ok := defaults[team.League]; ok && defaultSlug == cat.Slug {
			return cat.Slug
		}
		if slices.ContainsFunc(cat.RelatedTags, func(tag *services.PlyMktTag) bool {
			return tag.Slug == team.League
		}) {
			return cat.Slug
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

func findSportTags(sport *services.PlyMktSport, cats map[string]*Sport) string {
	defaults := map[string]string{
		"acn": "soccer", "bl2": "soccer", "scop": "soccer", "fr2": "soccer", "itsb": "soccer",
		"nba": "basketball", "wnba": "basketball", "ncaab": "basketball", "cbb": "basketball",
		"nhl": "hockey", "cfb": "football", "nfl": "football", "mlb": "baseball",
		"csgo": "esports", "starcraft2": "esports", "es2": "esports", "bnd": "esports",
		"bpl": "cricket", "cpl": "cricket", "wtc": "cricket", "odc": "cricket",
		"ecc": "cricket", "weth": "cricket", "eth": "cricket",
	}
	sportTagIDs := strings.Split(sport.Tags, ",")
	for _, cat := range cats {
		if slices.Contains(sportTagIDs, cat.Tag.ID) || strings.Contains(sport.Resolution, cat.Tag.Slug) {
			return cat.Tag.Slug
		}
	}
	if slug, ok := defaults[sport.Sport]; ok {
		return slug
	}
	return ""
}

func fetchLeagues() ([]*services.PlyMktSport, error) {
	resp, err := http.Get("https://gamma-api.polymarket.com/sports")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var sports []*services.PlyMktSport
	if err := json.NewDecoder(resp.Body).Decode(&sports); err != nil {
		return nil, err
	}
	return sports, nil
}

func fetchTags() ([]*services.PlyMktTag, error) {
	baseURL := "https://gamma-api.polymarket.com/tags"
	limit := 300
	offset := 0
	var fullTags []*services.PlyMktTag
	for {
		params := url.Values{}
		params.Add("limit", strconv.Itoa(limit))
		if offset > 0 {
			params.Add("offset", strconv.Itoa(offset))
		}
		resp, err := http.Get(baseURL + "?" + params.Encode())
		if err != nil {
			return nil, err
		}
		var tags []*services.PlyMktTag
		if err := json.NewDecoder(resp.Body).Decode(&tags); err != nil {
			resp.Body.Close()
			return nil, err
		}
		resp.Body.Close()
		if len(tags) == 0 {
			break
		}
		fullTags = append(fullTags, tags...)
		offset += limit
	}
	return fullTags, nil
}

func fetchTeams() ([]*services.PlyMktTeam, error) {
	baseURL := "https://gamma-api.polymarket.com/teams"
	limit := 500
	offset := 0
	var fullTeams []*services.PlyMktTeam
	for {
		params := url.Values{}
		params.Add("limit", strconv.Itoa(limit))
		if offset > 0 {
			params.Add("offset", strconv.Itoa(offset))
		}
		resp, err := http.Get(baseURL + "?" + params.Encode())
		if err != nil {
			return nil, err
		}
		var teams []*services.PlyMktTeam
		if err := json.NewDecoder(resp.Body).Decode(&teams); err != nil {
			resp.Body.Close()
			return nil, err
		}
		resp.Body.Close()
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
	params := url.Values{}
	params.Add("limit", strconv.Itoa(limit))
	params.Add("offset", "0")
	params.Add("tag_id", tagID)
	params.Add("active", "true")
	params.Add("closed", "false")
	params.Add("include_chat", "false")
	params.Add("order", "id")
	params.Add("ascending", "false")
	if tagID != "100639" {
		params.Add("related_tags", "true")
	}

	// For simplicity in tester, just fetch first page or a few?
	// Original logic had infinite loop, but typically we want all specific events.
	// Let's implement full fetch for safety.
	var fullEvents []*services.PlyMktEvent
	for {
		resp, err := http.Get(baseURL + "?" + params.Encode())
		if err != nil {
			return nil, err
		}
		var events []*services.PlyMktEvent
		if err := json.NewDecoder(resp.Body).Decode(&events); err != nil {
			resp.Body.Close()
			return nil, err
		}
		resp.Body.Close()
		if len(events) == 0 {
			break
		}
		fullEvents = append(fullEvents, events...)
		// Check for pagination next?
		// API usually supports offset.
		off, _ := strconv.Atoi(params.Get("offset"))
		params.Set("offset", strconv.Itoa(off+limit))

		// Safety break for testing if too many?
		// remove break for production-like behavior
	}
	return fullEvents, nil
}

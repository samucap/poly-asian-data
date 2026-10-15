package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/saver"
	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/samucap/poly-asian-data/internal/sportstags"
)

// Sports sync HTTP tuning (env overrides optional later).
const (
	sportsHTTPWorkers = 10
	sportsHTTPRPS     = 18 // shared token budget ~req/s
	sportsHTTPTimeout = 20 * time.Second
	sportsTeamPage    = 100
)

// SyncSportsTags runs the sports hierarchy seed (self-contained; OK on empty tags):
//
//  1. GET /sports once → save leagues
//  2. Derive parent edges (auto-family + inject)
//  3. Parallel GET /tags/{id} for sports CSV ids → upsert defs
//  4. Batch apply league/family parent_tag_id
//  5. Parallel per-league GET /teams → save teams
//  6. Match teams → tags; apply team→league parents
//
// Does not stop the pipeline (safe from catalog API-refresh path).
func (p *Pipeline) SyncSportsTags() error {
	p.logger.Info("Starting sports sync (/sports + parallel tag defs + /teams → parent_tag_id)...")
	start := time.Now()

	gammaBase := "https://gamma-api.polymarket.com"
	if p.cfg != nil {
		if v, ok := p.cfg.Services.PlyMkt.Endpoints["gamma"].(string); ok && v != "" {
			gammaBase = trimRightSlash(v)
		}
	}
	client := &http.Client{Timeout: sportsHTTPTimeout}
	lim := newTokenLimiter(sportsHTTPRPS)

	// 1) /sports once
	leagues, err := fetchSportsMetadata(p.ctx, client, lim, gammaBase)
	if err != nil {
		return fmt.Errorf("sports-sync: /sports: %w", err)
	}
	if len(leagues) == 0 {
		return fmt.Errorf("sports-sync: /sports returned empty")
	}
	p.logger.Info("fetched /sports", slog.Int("leagues", len(leagues)))

	if err := p.saverPool.SubmitWait(p.ctx, &saver.Record{
		TableName: "leagues",
		Data:      leagues,
		ItemCount: len(leagues),
	}); err != nil {
		return fmt.Errorf("sports-sync: save leagues: %w", err)
	}

	// 2) Edges + tag ids
	rows := make([]sportstags.SportsRow, 0, len(leagues))
	for _, l := range leagues {
		rows = append(rows, sportstags.SportsRow{Sport: l.Sport, Tags: l.Tags})
	}
	leagueEdges, meta := sportstags.BuildFromSportsMetadata(rows, sportstags.DefaultFamilyMinFreq)
	tagIDs := sportstags.AllTagIDsFromSports(rows)
	p.logger.Info("sports hierarchy derived",
		slog.Int("edges", meta.EdgeCount),
		slog.Int("auto_families", meta.AutoFamilies),
		slog.Int("inject_hits", meta.InjectHits),
		slog.Int("no_family_leagues", meta.NoFamilyLeagues),
		slog.Int("tag_ids", len(tagIDs)),
	)

	// 3) Parallel tag defs
	ok, missing, fail := fetchAndSaveTagDefs(p, client, lim, gammaBase, tagIDs)
	p.logger.Info("sport tag definitions complete",
		slog.Int("ok", ok),
		slog.Int("missing_404", missing),
		slog.Int("fail", fail),
		slog.Int("total", len(tagIDs)),
	)

	// 4) League/family parents
	dbConn := p.saverPool.DB()
	if n, err := db.ApplySportsParentTags(p.ctx, dbConn, leagueEdges); err != nil {
		return fmt.Errorf("sports-sync: apply league parents: %w", err)
	} else {
		p.logger.Info("league/family parent_tag_id applied", slog.Int64("edges", n))
	}

	// 5) Teams per league (parallel)
	teams, err := fetchAllTeamsByLeague(p.ctx, client, lim, gammaBase, leagues)
	if err != nil {
		return fmt.Errorf("sports-sync: teams: %w", err)
	}
	p.logger.Info("fetched /teams", slog.Int("teams", len(teams)))
	if len(teams) > 0 {
		if err := p.saverPool.SubmitWait(p.ctx, &saver.Record{
			TableName: "teams",
			Data:      teams,
			ItemCount: len(teams),
		}); err != nil {
			return fmt.Errorf("sports-sync: save teams: %w", err)
		}
	}

	// 6) Team → league title edges
	keyTags := make([]sportstags.LeagueKeyTags, 0, len(leagues))
	for _, l := range leagues {
		keyTags = append(keyTags, sportstags.LeagueKeyTags{Sport: l.Sport, RawTags: l.Tags})
	}
	titles := sportstags.LeagueTitleByKey(keyTags)
	tagRefs, err := db.FetchSportsScopedTags(p.ctx, dbConn)
	if err != nil {
		p.logger.Warn("fetch tags for team match failed", slog.Any("error", err))
	} else {
		teamRows := make([]sportstags.TeamRow, 0, len(teams))
		for _, t := range teams {
			teamRows = append(teamRows, sportstags.TeamRow{
				Name: t.Name, League: t.League, Abbreviation: t.Abbreviation, Alias: t.Alias,
			})
		}
		matches := sportstags.MatchTeamsToTags(teamRows, tagRefs, titles)
		teamEdges := sportstags.TeamParentEdges(matches)
		// Only apply edges where both ends already exist (FK tags_parent_tag_id_fkey).
		teamEdges = filterEdgesExistingTags(p.ctx, dbConn, teamEdges)
		p.logger.Info("team tag matches",
			slog.Int("matched", len(matches)),
			slog.Int("edges", len(teamEdges)),
		)
		if len(teamEdges) > 0 {
			if n, err := db.ApplySportsParentTags(p.ctx, dbConn, teamEdges); err != nil {
				p.logger.Warn("apply team parents failed", slog.Any("error", err))
			} else {
				p.logger.Info("team parent_tag_id applied", slog.Int64("edges", n))
			}
		}
	}

	// Ensure sports category rows for FK convenience
	if err := p.saverPool.SubmitWait(p.ctx, &saver.Record{
		TableName: "league_hierarchy",
		Data:      leagues,
		ItemCount: len(leagues),
	}); err != nil {
		// Non-fatal: parents already applied above; this also ensures sport categories.
		p.logger.Warn("league_hierarchy saver path failed", slog.Any("error", err))
	}

	p.WaitUntilIdle(p.ctx, 300*time.Millisecond)
	p.logger.Info("sports sync complete",
		slog.Duration("duration", time.Since(start)),
		slog.Int("leagues", len(leagues)),
		slog.Int("teams", len(teams)),
	)
	return nil
}

// RunSportsTagsSync runs sports/tags sync then stops the pipeline (cmd/sports-sync entrypoint).
func (p *Pipeline) RunSportsTagsSync() {
	if err := p.SyncSportsTags(); err != nil {
		p.logger.Error("sports/tags sync failed", slog.Any("error", err))
	}
	p.PrintFinalReport()
	p.Stop()
}

// --- HTTP helpers ---

type tokenLimiter struct {
	interval time.Duration
	mu       sync.Mutex
	next     time.Time
}

func newTokenLimiter(rps int) *tokenLimiter {
	if rps <= 0 {
		rps = sportsHTTPRPS
	}
	return &tokenLimiter{interval: time.Second / time.Duration(rps)}
}

func (l *tokenLimiter) wait(ctx context.Context) error {
	l.mu.Lock()
	now := time.Now()
	if l.next.Before(now) {
		l.next = now
	}
	waitUntil := l.next
	l.next = l.next.Add(l.interval)
	l.mu.Unlock()
	d := time.Until(waitUntil)
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func fetchSportsMetadata(ctx context.Context, client *http.Client, lim *tokenLimiter, gammaBase string) ([]services.PlyMktSport, error) {
	if err := lim.wait(ctx); err != nil {
		return nil, err
	}
	u := gammaBase + "/sports"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "PolyAsianData/1.0")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, truncate(body, 200))
	}
	var leagues []services.PlyMktSport
	if err := json.Unmarshal(body, &leagues); err != nil {
		return nil, err
	}
	// Dedupe by sport key (API may not paginate but be defensive).
	seen := map[string]bool{}
	out := make([]services.PlyMktSport, 0, len(leagues))
	for _, l := range leagues {
		key := l.Sport
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, l)
	}
	return out, nil
}

func fetchAndSaveTagDefs(p *Pipeline, client *http.Client, lim *tokenLimiter, gammaBase string, ids []string) (ok, missing, fail int) {
	if len(ids) == 0 {
		return 0, 0, 0
	}
	var (
		okN, missN, failN atomic.Int64
		wg                sync.WaitGroup
		sem               = make(chan struct{}, sportsHTTPWorkers)
	)
	for _, id := range ids {
		id := id
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			if p.ctx.Err() != nil {
				failN.Add(1)
				return
			}
			tag, status, err := fetchOneTag(p.ctx, client, lim, gammaBase, id)
			if err != nil {
				failN.Add(1)
				return
			}
			if status == 404 {
				missN.Add(1)
				return
			}
			if status != 200 || tag == nil {
				failN.Add(1)
				return
			}
			// Save definition
			_ = p.saverPool.SubmitWait(p.ctx, &saver.Record{
				TableName: "tags_definitions",
				Data:      []services.PlyMktTag{*tag},
				ItemCount: 1,
			})
			okN.Add(1)
		}()
	}
	wg.Wait()
	return int(okN.Load()), int(missN.Load()), int(failN.Load())
}

func fetchOneTag(ctx context.Context, client *http.Client, lim *tokenLimiter, gammaBase, id string) (*services.PlyMktTag, int, error) {
	var lastStatus int
	for attempt := 0; attempt < 3; attempt++ {
		if err := lim.wait(ctx); err != nil {
			return nil, 0, err
		}
		u := fmt.Sprintf("%s/tags/%s", gammaBase, id)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, 0, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "PolyAsianData/1.0")
		resp, err := client.Do(req)
		if err != nil {
			time.Sleep(time.Duration(attempt+1) * time.Second)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		lastStatus = resp.StatusCode
		if resp.StatusCode == 429 || contains1015(body) {
			time.Sleep(time.Duration(2+attempt*3) * time.Second)
			continue
		}
		if resp.StatusCode == 404 {
			return nil, 404, nil
		}
		if resp.StatusCode != 200 {
			return nil, resp.StatusCode, fmt.Errorf("status %d", resp.StatusCode)
		}
		var tag services.PlyMktTag
		if err := json.Unmarshal(body, &tag); err != nil {
			return nil, 200, err
		}
		if tag.ID == "" {
			tag.ID = id
		}
		return &tag, 200, nil
	}
	return nil, lastStatus, fmt.Errorf("retries exhausted for tag %s", id)
}

func fetchAllTeamsByLeague(ctx context.Context, client *http.Client, lim *tokenLimiter, gammaBase string, leagues []services.PlyMktSport) ([]services.PlyMktTeam, error) {
	var (
		mu    sync.Mutex
		all   []services.PlyMktTeam
		wg    sync.WaitGroup
		sem   = make(chan struct{}, sportsHTTPWorkers)
		errCh = make(chan error, 1)
	)
	for _, l := range leagues {
		key := l.Sport
		if key == "" {
			continue
		}
		wg.Add(1)
		go func(leagueKey string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			offset := 0
			for {
				if ctx.Err() != nil {
					return
				}
				page, err := fetchTeamsPage(ctx, client, lim, gammaBase, leagueKey, offset, sportsTeamPage)
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
					return
				}
				if len(page) == 0 {
					return
				}
				mu.Lock()
				all = append(all, page...)
				mu.Unlock()
				if len(page) < sportsTeamPage {
					return
				}
				offset += sportsTeamPage
				if offset > 5000 {
					return
				}
			}
		}(key)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		// Partial teams still useful
		if len(all) == 0 {
			return nil, err
		}
	default:
	}
	// Dedupe by team id
	seen := map[int]bool{}
	out := make([]services.PlyMktTeam, 0, len(all))
	for _, t := range all {
		if seen[t.ID] {
			continue
		}
		seen[t.ID] = true
		out = append(out, t)
	}
	return out, nil
}

func fetchTeamsPage(ctx context.Context, client *http.Client, lim *tokenLimiter, gammaBase, league string, offset, limit int) ([]services.PlyMktTeam, error) {
	for attempt := 0; attempt < 3; attempt++ {
		if err := lim.wait(ctx); err != nil {
			return nil, err
		}
		q := url.Values{}
		q.Set("league", league)
		q.Set("limit", strconv.Itoa(limit))
		q.Set("offset", strconv.Itoa(offset))
		u := gammaBase + "/teams?" + q.Encode()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Accept", "application/json")
		req.Header.Set("User-Agent", "PolyAsianData/1.0")
		resp, err := client.Do(req)
		if err != nil {
			time.Sleep(time.Second)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == 429 || contains1015(body) {
			time.Sleep(time.Duration(2+attempt*3) * time.Second)
			continue
		}
		if resp.StatusCode != 200 {
			return nil, fmt.Errorf("teams %s: status %d", league, resp.StatusCode)
		}
		var teams []services.PlyMktTeam
		if err := json.Unmarshal(body, &teams); err != nil {
			return nil, err
		}
		return teams, nil
	}
	return nil, fmt.Errorf("teams %s offset %d: retries exhausted", league, offset)
}

func contains1015(body []byte) bool {
	return bytes.Contains(body, []byte("1015")) ||
		bytes.Contains(body, []byte("rate limit")) ||
		bytes.Contains(body, []byte("Rate limited"))
}

func trimRightSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}

// filterEdgesExistingTags drops edges whose child or parent is missing from tags.
func filterEdgesExistingTags(ctx context.Context, conn db.DBInterface, edges map[string]string) map[string]string {
	if len(edges) == 0 || conn == nil {
		return edges
	}
	ids := make([]string, 0, len(edges)*2)
	for c, p := range edges {
		ids = append(ids, c, p)
	}
	existing, err := db.FetchTagsByIDs(ctx, conn, ids)
	if err != nil {
		return edges
	}
	have := map[string]bool{}
	for _, t := range existing {
		if t != nil && t.ID != "" {
			have[t.ID] = true
		}
	}
	out := map[string]string{}
	for c, p := range edges {
		if have[c] && have[p] {
			out[c] = p
		}
	}
	return out
}

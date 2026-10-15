package catalog

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/logging"
	"github.com/samucap/poly-asian-data/internal/services"
	"github.com/samucap/poly-asian-data/internal/sportstags"
	"github.com/samucap/poly-asian-data/internal/tagagg"
)

// TagSource describes where the tag catalog was loaded from.
type TagSource string

const (
	TagSourceDB    TagSource = "db"
	TagSourceAPI   TagSource = "api"
	TagSourceEmpty TagSource = "empty"
)

// LoadTagCatalogResult is the tag map plus provenance.
type LoadTagCatalogResult struct {
	Catalog map[string]*services.PlyMktTag
	Source  TagSource
}

// LoadTagCatalogOptions controls API-refresh hooks.
type LoadTagCatalogOptions struct {
	// BeforeAPIRefresh runs before related-tags BFS (e.g. sports-sync).
	// Called only when the tag catalog is refreshed from the API.
	// Must be self-contained (OK on empty tags table).
	BeforeAPIRefresh func(ctx context.Context) error
}

// LoadTagCatalog loads the category tag map under CategoriesRootTagID.
// Freshness uses sync_state watermark top_markets.tag_catalog.
// On API refresh: optional BeforeAPIRefresh (sports-sync) then related-tags BFS.
func LoadTagCatalog(
	ctx context.Context,
	logger *slog.Logger,
	dbPool db.DBInterface,
	httpClient *http.Client,
	cfg *config.Config,
	now time.Time,
	opts LoadTagCatalogOptions,
) LoadTagCatalogResult {
	if logger == nil {
		logger = slog.Default()
	}
	wm, hasWM, err := db.GetWatermark(ctx, dbPool, db.WatermarkTopMarketsTagCatalog)
	if err != nil {
		logger.Warn("failed to read tag catalog watermark; will refresh from API", "error", err)
		hasWM = false
	}

	needsRefresh := db.CatalogNeedsRefresh(wm, hasWM, now, db.DefaultTagCatalogTTL)

	if !needsRefresh {
		catalog, err := LoadCategoriesFromDB(ctx, dbPool)
		if err != nil {
			logger.Warn("failed to load tag catalog from DB; falling back to API", "error", err)
		} else if len(catalog) > 0 {
			logger.Info("tag catalog loaded from DB", "count", logging.FormatCount(int64(len(catalog))))
			return LoadTagCatalogResult{Catalog: catalog, Source: TagSourceDB}
		} else {
			logger.Warn("tag catalog empty in DB despite fresh watermark; refreshing from API")
		}
	} else {
		logger.Info("tag catalog watermark stale or missing; refreshing from API (sports-sync + related-tags)")
	}

	// API refresh path: sports-sync first (seeds sports tags + parents on empty DB).
	if opts.BeforeAPIRefresh != nil {
		logger.Info("running BeforeAPIRefresh (sports-sync) before related-tags BFS")
		if err := opts.BeforeAPIRefresh(ctx); err != nil {
			logger.Error("BeforeAPIRefresh failed; continuing with BFS", "error", err)
		} else {
			logger.Info("BeforeAPIRefresh complete")
		}
	}

	// Load sports edges so BFS cannot clobber sports parents.
	sportsEdges, _ := db.SportsParentEdges(ctx, dbPool)

	catalog, err := FetchCategories(ctx, logger, httpClient, cfg, sportsEdges)
	if err != nil {
		logger.Error("failed to fetch categories from API", "error", err)
		if dbCatalog, dbErr := LoadCategoriesFromDB(ctx, dbPool); dbErr == nil && len(dbCatalog) > 0 {
			logger.Warn("using DB tag catalog after API failure", "count", logging.FormatCount(int64(len(dbCatalog))))
			return LoadTagCatalogResult{Catalog: dbCatalog, Source: TagSourceDB}
		}
		return LoadTagCatalogResult{Catalog: map[string]*services.PlyMktTag{}, Source: TagSourceEmpty}
	}

	// Merge sports-owned tags missing from BFS (depth limits) and force sports parents.
	mergeSportsTagsIntoCatalog(ctx, logger, dbPool, catalog, sportsEdges)

	if err := db.SetWatermark(ctx, dbPool, db.WatermarkTopMarketsTagCatalog, now.UTC()); err != nil {
		logger.Warn("failed to set tag catalog watermark", "error", err)
	} else {
		logger.Info("tag catalog watermark updated", "key", db.WatermarkTopMarketsTagCatalog)
	}
	logger.Info("tag catalog fetched from API", "count", logging.FormatCount(int64(len(catalog))))
	return LoadTagCatalogResult{Catalog: catalog, Source: TagSourceAPI}
}

// LoadCategoriesFromDB rebuilds the seed catalog map from tags under the categories root.
func LoadCategoriesFromDB(ctx context.Context, dbPool db.DBInterface) (map[string]*services.PlyMktTag, error) {
	tags, _, err := db.FetchTagSubtree(ctx, dbPool, CategoriesRootTagID)
	if err != nil {
		return nil, err
	}
	return tagagg.CatalogToMap(tags, CategoriesRootTagID), nil
}

// OverlaySportsHierarchy applies sports parent_tag_id edges onto catMap (from leagues/teams DB).
// Call after LoadTagCatalog so Aggregate sees correct sports parents. Pure read of edges already
// produced by sports-sync — no hierarchy invention after metrics.
func OverlaySportsHierarchy(
	ctx context.Context,
	logger *slog.Logger,
	dbPool db.DBInterface,
	catMap map[string]*services.PlyMktTag,
) (map[string]string, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if catMap == nil {
		catMap = map[string]*services.PlyMktTag{}
	}
	edges, err := db.SportsParentEdges(ctx, dbPool)
	if err != nil {
		return nil, err
	}
	mergeSportsTagsIntoCatalog(ctx, logger, dbPool, catMap, edges)
	return edges, nil
}

func mergeSportsTagsIntoCatalog(
	ctx context.Context,
	logger *slog.Logger,
	dbPool db.DBInterface,
	catMap map[string]*services.PlyMktTag,
	edges map[string]string,
) {
	if catMap == nil {
		return
	}
	if len(edges) > 0 {
		ids := make([]string, 0, len(edges)*2+1)
		for child, parent := range edges {
			ids = append(ids, child, parent)
		}
		ids = append(ids, sportstags.TagIDSports)
		if dbTags, err := db.FetchTagsByIDs(ctx, dbPool, ids); err != nil {
			logger.Warn("fetch sports edge tags from DB failed", "error", err)
		} else {
			for _, t := range dbTags {
				if t == nil || t.ID == "" {
					continue
				}
				if existing, ok := catMap[t.ID]; ok && existing != nil {
					if existing.Label == "" && t.Label != "" {
						existing.Label = t.Label
					}
					if existing.Slug == "" && t.Slug != "" {
						existing.Slug = t.Slug
					}
					// Sports parent always wins when edge known.
					if p, ok := sportstags.ParentOf(t.ID, edges); ok {
						existing.ParentTagID = p
					} else if existing.ParentTagID == "" && t.ParentTagID != "" {
						existing.ParentTagID = t.ParentTagID
					}
				} else {
					if p, ok := sportstags.ParentOf(t.ID, edges); ok {
						t.ParentTagID = p
					}
					catMap[t.ID] = t
				}
			}
		}
	}
	// Sports top under categories root.
	if t, ok := catMap[sportstags.TagIDSports]; ok && t != nil {
		t.ParentTagID = CategoriesRootTagID
	} else {
		label, slug := sportstags.KnownLabelSlug(sportstags.TagIDSports)
		catMap[sportstags.TagIDSports] = &services.PlyMktTag{
			ID:          sportstags.TagIDSports,
			Label:       label,
			Slug:        slug,
			ParentTagID: CategoriesRootTagID,
		}
	}
	n := sportstags.ApplyParents(catMap, edges)
	if n > 0 || len(edges) > 0 {
		logger.Info("sports hierarchy applied on tag catalog",
			"edges", logging.FormatCount(int64(len(edges))),
			"parents_set", logging.FormatCount(int64(n)),
		)
	}
}

// FetchCategories BFS-walks Gamma related-tags under the categories root.
// sportsEdges: when set, sports-owned tags keep sports parents (BFS parent is ignored).
func FetchCategories(
	ctx context.Context,
	logger *slog.Logger,
	client *http.Client,
	cfg *config.Config,
	sportsEdges map[string]string,
) (map[string]*services.PlyMktTag, error) {
	if logger == nil {
		logger = slog.Default()
	}
	gammaBase := Endpoint(cfg, "gamma", "https://gamma-api.polymarket.com")
	queue := []string{CategoriesRootTagID}
	seen := map[string]bool{CategoriesRootTagID: true}
	catalog := map[string]*services.PlyMktTag{}

	for len(queue) > 0 {
		currID := queue[0]
		queue = queue[1:]

		u, err := url.Parse(fmt.Sprintf("%s/tags/%s/related-tags/tags?status=active&omit_empty=true", gammaBase, currID))
		if err != nil {
			return nil, err
		}
		tags, err := fetcher.FetchPaginated[services.PlyMktTag](ctx, client, u, 100, 0)
		if err != nil {
			logger.Error("related-tags fetch failed", "tag_id", currID, "error", err)
			return nil, err
		}

		for _, t := range tags {
			if t == nil || t.ID == "" {
				continue
			}
			if tagagg.IsCatchAllTag(t) {
				t.ParentTagID = CategoriesRootTagID
				if _, exists := catalog[t.ID]; !exists {
					catalog[t.ID] = t
				}
				seen[t.ID] = true
				logger.Info("skipping catch-all tag expansion",
					"tag_id", t.ID,
					"slug", t.Slug,
					"active_events_count", t.ActiveEventsCount,
				)
				continue
			}

			// Sports-owned: never use BFS parent.
			if p, ok := sportstags.ParentOf(t.ID, sportsEdges); ok {
				t.ParentTagID = p
			} else if sportstags.IsOwned(t.ID, sportsEdges) && t.ID == sportstags.TagIDSports {
				t.ParentTagID = CategoriesRootTagID
			} else {
				t.ParentTagID = currID
			}

			if _, exists := catalog[t.ID]; !exists {
				catalog[t.ID] = t
			} else if sportstags.IsOwned(t.ID, sportsEdges) {
				// Keep sports parent if we revisit via BFS.
				if p, ok := sportstags.ParentOf(t.ID, sportsEdges); ok {
					catalog[t.ID].ParentTagID = p
				}
			}
			if !seen[t.ID] {
				seen[t.ID] = true
				// Still expand related-tags for discovery of non-sports siblings.
				queue = append(queue, t.ID)
			}
		}
	}

	return catalog, nil
}

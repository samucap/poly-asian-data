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

// LoadTagCatalog loads the category tag map under CategoriesRootTagID.
// Freshness uses sync_state watermark top_markets.tag_catalog (catalog plane ownership;
// key name retained for compatibility with existing deployments).
// Always returns a non-nil map (empty if both API and DB fail).
func LoadTagCatalog(
	ctx context.Context,
	logger *slog.Logger,
	dbPool db.DBInterface,
	httpClient *http.Client,
	cfg *config.Config,
	now time.Time,
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
		logger.Info("tag catalog watermark stale or missing; refreshing related-tags from API")
	}

	catalog, err := FetchCategories(ctx, logger, httpClient, cfg)
	if err != nil {
		logger.Error("failed to fetch categories from API", "error", err)
		if dbCatalog, dbErr := LoadCategoriesFromDB(ctx, dbPool); dbErr == nil && len(dbCatalog) > 0 {
			logger.Warn("using DB tag catalog after API failure", "count", logging.FormatCount(int64(len(dbCatalog))))
			return LoadTagCatalogResult{Catalog: dbCatalog, Source: TagSourceDB}
		}
		return LoadTagCatalogResult{Catalog: map[string]*services.PlyMktTag{}, Source: TagSourceEmpty}
	}

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

// FetchCategories BFS-walks Gamma related-tags under the categories root.
// Catch-all tags (e.g. id 100215 "All") are stored for reference/ActiveEventsCount
// but are not expanded and not used as hierarchy parents — related-tags edges are not
// a true tree, and expanding "All" previously parented most of the taxonomy under it.
func FetchCategories(ctx context.Context, logger *slog.Logger, client *http.Client, cfg *config.Config) (map[string]*services.PlyMktTag, error) {
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
			// First visit wins parent edge (BFS from root).
			// Catch-all (100215 "All"): keep node + activeEventsCount, do not expand.
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
			t.ParentTagID = currID
			if _, exists := catalog[t.ID]; !exists {
				catalog[t.ID] = t
			}
			if !seen[t.ID] {
				seen[t.ID] = true
				queue = append(queue, t.ID)
			}
		}
	}

	return catalog, nil
}

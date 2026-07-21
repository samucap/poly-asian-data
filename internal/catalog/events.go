package catalog

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/services"
)

// KeysetFilters are optional Gamma /events/keyset query params.
// Zero values omit the corresponding param (except Closed/Active always set by callers).
type KeysetFilters struct {
	// VolumeMin is Gamma event volume filter (not necessarily 24h).
	VolumeMin float64
	// LiquidityMin is Gamma event liquidity filter.
	LiquidityMin float64
	// EndDateMin / EndDateMax as absolute times; zero means omit.
	EndDateMin time.Time
	EndDateMax time.Time
	// LimitThreshold stops pagination after this many events (0 = no cap).
	LimitThreshold int
}

// FetchOpenEventsKeyset fetches the full open active event universe via Gamma keyset.
// No volume/liquidity/end-date server filters (catalog completeness).
func FetchOpenEventsKeyset(ctx context.Context, client *http.Client, cfg *config.Config) ([]*services.PlyMktEvent, error) {
	return FetchEventsKeyset(ctx, client, cfg, KeysetFilters{})
}

// FetchEventsKeyset fetches open active events via keyset with optional server-side filters.
// Used by edge-scan to bound the candidate universe.
func FetchEventsKeyset(ctx context.Context, client *http.Client, cfg *config.Config, f KeysetFilters) ([]*services.PlyMktEvent, error) {
	gammaBase := Endpoint(cfg, "gamma", "https://gamma-api.polymarket.com")
	u, err := url.Parse(gammaBase + "/events/keyset")
	if err != nil {
		return nil, fmt.Errorf("catalog: parse keyset url: %w", err)
	}
	q := u.Query()
	q.Set("closed", "false")
	q.Set("active", "true")
	q.Set("order", "volume24hr")
	q.Set("ascending", "false")
	q.Set("include_chat", "false")
	if f.VolumeMin > 0 {
		q.Set("volume_min", strconv.FormatFloat(f.VolumeMin, 'f', -1, 64))
	}
	if f.LiquidityMin > 0 {
		q.Set("liquidity_min", strconv.FormatFloat(f.LiquidityMin, 'f', -1, 64))
	}
	if !f.EndDateMin.IsZero() {
		q.Set("end_date_min", f.EndDateMin.UTC().Format(time.RFC3339))
	}
	if !f.EndDateMax.IsZero() {
		q.Set("end_date_max", f.EndDateMax.UTC().Format(time.RFC3339))
	}
	u.RawQuery = q.Encode()

	events, err := fetcher.FetchPaginatedKeyset[services.PlyMktEvent](ctx, client, u, GammaKeysetLimit, f.LimitThreshold)
	if err != nil {
		return nil, fmt.Errorf("catalog: keyset fetch: %w", err)
	}
	return events, nil
}

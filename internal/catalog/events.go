package catalog

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	"github.com/samucap/poly-asian-data/internal/config"
	"github.com/samucap/poly-asian-data/internal/fetcher"
	"github.com/samucap/poly-asian-data/internal/services"
)

// FetchOpenEventsKeyset fetches the full open active event universe via Gamma keyset.
// No volume/liquidity/end-date server filters (catalog completeness).
func FetchOpenEventsKeyset(ctx context.Context, client *http.Client, cfg *config.Config) ([]*services.PlyMktEvent, error) {
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
	u.RawQuery = q.Encode()

	events, err := fetcher.FetchPaginatedKeyset[services.PlyMktEvent](ctx, client, u, GammaKeysetLimit, 0)
	if err != nil {
		return nil, fmt.Errorf("catalog: keyset fetch: %w", err)
	}
	return events, nil
}

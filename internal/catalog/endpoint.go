package catalog

import "github.com/samucap/poly-asian-data/internal/config"

// Endpoint returns a named PlyMkt endpoint string or fallback.
func Endpoint(cfg *config.Config, key, fallback string) string {
	if cfg != nil {
		if v, ok := cfg.Services.PlyMkt.Endpoints[key].(string); ok && v != "" {
			return v
		}
	}
	return fallback
}

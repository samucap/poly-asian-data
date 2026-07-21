package enrich

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/samucap/poly-asian-data/internal/config"
)

// OIPoint is open interest for one condition/market at a point in time.
type OIPoint struct {
	Time        time.Time
	ConditionID string
	Value       float64
}

type oiAPIItem struct {
	Market string  `json:"market"`
	Value  float64 `json:"value"`
}

// FetchOI loads open interest for condition IDs from the Data API.
// Endpoint shape: GET {data_api}/oi?market=<id> (batch via repeated calls or comma list when supported).
func FetchOI(ctx context.Context, client *http.Client, conditionIDs []string) ([]OIPoint, error) {
	if client == nil {
		client = http.DefaultClient
	}
	base, _ := config.DefaultEndpoints["data_api"].(string)
	if base == "" {
		base = "https://data-api.polymarket.com"
	}
	now := time.Now().UTC()
	var out []OIPoint

	// API accepts market query; batch in chunks of 20 comma-separated when possible.
	const chunk = 20
	for i := 0; i < len(conditionIDs); i += chunk {
		end := i + chunk
		if end > len(conditionIDs) {
			end = len(conditionIDs)
		}
		ids := conditionIDs[i:end]
		// Prefer individual GETs for reliability (Data API varies).
		for _, id := range ids {
			if id == "" {
				continue
			}
			u, err := url.Parse(strings.TrimRight(base, "/") + "/oi")
			if err != nil {
				return out, err
			}
			q := u.Query()
			q.Set("market", id)
			u.RawQuery = q.Encode()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
			if err != nil {
				return out, err
			}
			resp, err := client.Do(req)
			if err != nil {
				return out, fmt.Errorf("enrich oi %s: %w", id, err)
			}
			data, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				return out, err
			}
			if resp.StatusCode < 200 || resp.StatusCode >= 300 {
				// Skip hard-fail single ID (partial OK).
				continue
			}
			pts, err := parseOI(data, now)
			if err != nil {
				continue
			}
			// Ensure condition id set when API omits market field.
			for j := range pts {
				if pts[j].ConditionID == "" {
					pts[j].ConditionID = id
				}
			}
			out = append(out, pts...)
		}
	}
	return out, nil
}

func parseOI(data []byte, now time.Time) ([]OIPoint, error) {
	// Array form
	var items []oiAPIItem
	if err := json.Unmarshal(data, &items); err == nil && len(items) > 0 {
		out := make([]OIPoint, 0, len(items))
		for _, it := range items {
			out = append(out, OIPoint{Time: now, ConditionID: it.Market, Value: it.Value})
		}
		return out, nil
	}
	// Single object
	var one oiAPIItem
	if err := json.Unmarshal(data, &one); err == nil && (one.Market != "" || one.Value != 0) {
		return []OIPoint{{Time: now, ConditionID: one.Market, Value: one.Value}}, nil
	}
	// Bare number
	var v float64
	if err := json.Unmarshal(data, &v); err == nil {
		return []OIPoint{{Time: now, Value: v}}, nil
	}
	return nil, fmt.Errorf("enrich oi: unrecognized payload")
}

package enrich

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
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

// FetchOI loads open interest for condition IDs from the Data API with bounded concurrency.
// concurrency <= 0 defaults to 16.
func FetchOI(ctx context.Context, client *http.Client, conditionIDs []string, concurrency int) ([]OIPoint, error) {
	if client == nil {
		client = http.DefaultClient
	}
	if concurrency <= 0 {
		concurrency = 16
	}
	base, _ := config.DefaultEndpoints["data_api"].(string)
	if base == "" {
		base = "https://data-api.polymarket.com"
	}
	now := time.Now().UTC()

	ids := make([]string, 0, len(conditionIDs))
	for _, id := range conditionIDs {
		if id != "" {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	if concurrency > len(ids) {
		concurrency = len(ids)
	}

	type result struct {
		pts []OIPoint
	}
	jobs := make(chan string, len(ids))
	outCh := make(chan result, len(ids))

	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range jobs {
				if ctx.Err() != nil {
					return
				}
				pts, err := fetchOneOI(ctx, client, base, id, now)
				if err != nil || len(pts) == 0 {
					continue
				}
				outCh <- result{pts: pts}
			}
		}()
	}
	for _, id := range ids {
		jobs <- id
	}
	close(jobs)
	go func() {
		wg.Wait()
		close(outCh)
	}()

	var out []OIPoint
	for r := range outCh {
		out = append(out, r.pts...)
	}
	// Partial success is OK; only hard-fail if context canceled with zero results.
	if ctx.Err() != nil && len(out) == 0 {
		return out, ctx.Err()
	}
	return out, nil
}

func fetchOneOI(ctx context.Context, client *http.Client, base, id string, now time.Time) ([]OIPoint, error) {
	u, err := url.Parse(strings.TrimRight(base, "/") + "/oi")
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("market", id)
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("enrich oi %s: %w", id, err)
	}
	data, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, nil
	}
	pts, err := parseOI(data, now)
	if err != nil {
		return nil, err
	}
	for j := range pts {
		if pts[j].ConditionID == "" {
			pts[j].ConditionID = id
		}
	}
	return pts, nil
}

func parseOI(data []byte, now time.Time) ([]OIPoint, error) {
	var items []oiAPIItem
	if err := json.Unmarshal(data, &items); err == nil && len(items) > 0 {
		out := make([]OIPoint, 0, len(items))
		for _, it := range items {
			out = append(out, OIPoint{Time: now, ConditionID: it.Market, Value: it.Value})
		}
		return out, nil
	}
	var one oiAPIItem
	if err := json.Unmarshal(data, &one); err == nil && (one.Market != "" || one.Value != 0) {
		return []OIPoint{{Time: now, ConditionID: one.Market, Value: one.Value}}, nil
	}
	var v float64
	if err := json.Unmarshal(data, &v); err == nil {
		return []OIPoint{{Time: now, Value: v}}, nil
	}
	return nil, fmt.Errorf("enrich oi: unrecognized payload")
}

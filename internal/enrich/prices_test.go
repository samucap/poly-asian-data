package enrich

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/samucap/poly-asian-data/internal/db"
	"github.com/stretchr/testify/require"
)

func TestSelectPriceTokens_BoardThenVolumeCap(t *testing.T) {
	board := []db.EvalMarketMeta{
		{TokenID: "a", ConditionID: "A"},
		{TokenID: "b", ConditionID: "B"},
	}
	vol := []db.EvalMarketMeta{
		{TokenID: "a", ConditionID: "A"}, // dup
		{TokenID: "c", ConditionID: "C"},
		{TokenID: "d", ConditionID: "D"},
	}
	got := SelectPriceTokens(board, vol, 3)
	require.Len(t, got, 3)
	require.Equal(t, "a", got[0].TokenID)
	require.Equal(t, "b", got[1].TokenID)
	require.Equal(t, "c", got[2].TokenID)
}

func TestParseBatchPrices(t *testing.T) {
	raw := []byte(`{"history":{"tok1":[{"t":100,"p":0.4},{"t":200,"p":0.5}]}}`)
	m, err := parseBatchPrices(raw)
	require.NoError(t, err)
	require.Len(t, m["tok1"], 2)
	require.InDelta(t, 0.5, m["tok1"][1].P, 1e-9)
}

func TestNormalizeBatchParams_DropsEndWhenSpanTooLong(t *testing.T) {
	now := time.Now().Unix()
	start := now - int64((30 * 24 * time.Hour).Seconds())
	p := normalizeBatchParams(BatchPricesParams{
		StartTS: start,
		EndTS:   now,
		// no interval — should be filled when end dropped
	})
	require.Equal(t, int64(0), p.EndTS, "end_ts must be omitted for >14d span")
	require.Equal(t, "max", p.Interval)
	require.Equal(t, start, p.StartTS)
}

func TestNormalizeBatchParams_KeepsShortWindow(t *testing.T) {
	now := time.Now().Unix()
	start := now - int64((7 * 24 * time.Hour).Seconds())
	p := normalizeBatchParams(BatchPricesParams{StartTS: start, EndTS: now})
	require.Equal(t, now, p.EndTS)
	require.Equal(t, "", p.Interval)
}

func TestFetchBatchPricesHistory_SendsSnakeCaseNoEndForCold(t *testing.T) {
	var got map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"history": map[string]any{
				"t1": []map[string]any{{"t": 1, "p": 0.3}},
			},
		})
	}))
	defer srv.Close()

	// Point DefaultEndpoints at the test server by temporarily calling with a
	// custom round-trip... FetchBatch hardcodes config clob URL. Instead rewrite
	// via client transport that redirects clob host to srv.
	client := &http.Client{Transport: rewriteHostTransport{base: srv.URL}}
	now := time.Now().Unix()
	start := now - int64((30 * 24 * time.Hour).Seconds())
	_, err := FetchBatchPricesHistory(context.Background(), client, []string{"t1"}, BatchPricesParams{
		StartTS:  start,
		EndTS:    now, // should be stripped by normalize
		Interval: "max",
		Fidelity: 60,
	}, 20)
	require.NoError(t, err)
	require.Contains(t, got, "start_ts")
	require.NotContains(t, got, "end_ts", "cold 30d must not send end_ts")
	require.NotContains(t, got, "startTs")
	require.Equal(t, "max", got["interval"])
	require.EqualValues(t, 60, got["fidelity"])
}

// rewriteHostTransport sends all requests to base, ignoring the request URL host.
type rewriteHostTransport struct{ base string }

func (t rewriteHostTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	u, err := http.NewRequestWithContext(req.Context(), req.Method, t.base, req.Body)
	if err != nil {
		return nil, err
	}
	u.Header = req.Header
	return http.DefaultTransport.RoundTrip(u)
}

func TestNormalizePriceTS_via_db(t *testing.T) {
	sec := db.NormalizePriceTS(1_700_000_000)
	require.False(t, sec.IsZero())
	ms := db.NormalizePriceTS(1_700_000_000_000)
	require.False(t, ms.IsZero())
}

func TestPriceUnixSeconds(t *testing.T) {
	require.Equal(t, int64(0), priceUnixSeconds(0))
	require.Equal(t, int64(1_700_000_000), priceUnixSeconds(1_700_000_000))
	require.Equal(t, int64(1_700_000_000), priceUnixSeconds(1_700_000_000_000))
}

func TestEarliestStartTS_PerBatch(t *testing.T) {
	// batch A: HWM 1000 and 2000 → start 1000-3600 floored? floor 0 → 1000-3600
	maxTS := map[string]int64{
		"a": 1000,
		"b": 2000,
		"c": 10_000,
		"d": 10_500,
	}
	startA := earliestStartTS([]string{"a", "b"}, maxTS, 3600, 0)
	require.Equal(t, int64(1000-3600), startA)

	startB := earliestStartTS([]string{"c", "d"}, maxTS, 3600, 0)
	require.Equal(t, int64(10_000-3600), startB)
	require.Greater(t, startB, startA, "fresher batch must not inherit older batch start")

	// floor clamps
	require.Equal(t, int64(500), earliestStartTS([]string{"a"}, maxTS, 3600, 500))
}

func TestSortTokenIDsByHWMAsc(t *testing.T) {
	maxTS := map[string]int64{
		"fresh": 9000,
		"old":   1000,
		"mid":   5000,
	}
	got := sortTokenIDsByHWMAsc([]string{"fresh", "old", "mid"}, maxTS)
	require.Equal(t, []string{"old", "mid", "fresh"}, got)
}

func TestChunkTokenIDs(t *testing.T) {
	ids := make([]string, 45)
	for i := range ids {
		ids[i] = string(rune('a' + i%26))
	}
	// use unique ids
	for i := range ids {
		ids[i] = "t" + string(rune('0'+i/10)) + string(rune('0'+i%10))
	}
	chunks := chunkTokenIDs(ids, 20)
	require.Len(t, chunks, 3)
	require.Len(t, chunks[0], 20)
	require.Len(t, chunks[1], 20)
	require.Len(t, chunks[2], 5)
}

func TestWarmBatches_UsePerBatchEarliestStart(t *testing.T) {
	// Simulate: 25 warm tokens, first 20 old HWM, last 5 fresher.
	// After sort + chunk, second request start_ts must be higher than first.
	type reqBody struct {
		Markets []string `json:"markets"`
		StartTS int64    `json:"start_ts"`
	}
	var bodies []reqBody
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b reqBody
		require.NoError(t, json.NewDecoder(r.Body).Decode(&b))
		bodies = append(bodies, b)
		// return empty history map keyed by markets
		hist := map[string]any{}
		for _, m := range b.Markets {
			hist[m] = []map[string]any{{"t": b.StartTS + 1, "p": 0.5}}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"history": hist})
	}))
	defer srv.Close()

	now := time.Now().Unix()
	oldHWM := now - int64((48 * time.Hour).Seconds())
	freshHWM := now - int64((45 * time.Minute).Seconds())
	maxTS := map[string]int64{}
	var warm []string
	for i := 0; i < 20; i++ {
		id := "old" + string(rune('A'+i))
		warm = append(warm, id)
		maxTS[id] = oldHWM
	}
	for i := 0; i < 5; i++ {
		id := "new" + string(rune('A'+i))
		warm = append(warm, id)
		maxTS[id] = freshHWM
	}

	// Exercise pure batching path used by EnsurePrices warm branch
	sorted := sortTokenIDsByHWMAsc(warm, maxTS)
	chunks := chunkTokenIDs(sorted, 20)
	require.Len(t, chunks, 2)

	client := &http.Client{Transport: rewriteHostTransport{base: srv.URL}}
	floor := now - int64((30 * 24 * time.Hour).Seconds())
	for _, chunk := range chunks {
		start := earliestStartTS(chunk, maxTS, 3600, floor)
		_, err := FetchBatchPricesHistory(context.Background(), client, chunk, BatchPricesParams{
			StartTS:  start,
			Fidelity: 60,
		}, 20)
		require.NoError(t, err)
	}

	require.Len(t, bodies, 2)
	require.Len(t, bodies[0].Markets, 20)
	require.Len(t, bodies[1].Markets, 5)
	// first batch all old → start ≈ oldHWM-3600
	require.Equal(t, oldHWM-3600, bodies[0].StartTS)
	// second batch all fresher → start ≈ freshHWM-3600
	require.Equal(t, freshHWM-3600, bodies[1].StartTS)
	require.Greater(t, bodies[1].StartTS, bodies[0].StartTS)
}


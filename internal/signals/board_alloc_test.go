package signals

import (
	"testing"
	"time"

	"github.com/samucap/poly-asian-data/internal/alloc"
	"github.com/samucap/poly-asian-data/internal/risk"
	"github.com/stretchr/testify/require"
)

func TestSelectTaken_CapsAtMaxPositions(t *testing.T) {
	now := time.Now().UTC()
	cands := make([]*PaperSignal, 0, 50)
	names := make([]alloc.Name, 0, 50)
	for i := 0; i < 50; i++ {
		id := string(rune('a'+i%26)) + string(rune('0'+i/26))
		sig := &PaperSignal{
			Time: now, ConditionID: id, TokenID: "t" + id, Side: SideBuy,
			EdgeBps: float64(50-i) * 10, Conviction: 0.5, SizeUSD: 25, Reason: map[string]any{},
		}
		cands = append(cands, sig)
		names = append(names, NameFromPaper(sig))
	}
	cfg := risk.DefaultConfig()
	cfg.MaxPositions = 20
	cfg.MaxGrossUSD = 1e9
	res := alloc.Run(alloc.Input{Names: names, Risk: cfg})
	taken := SelectTaken(cands, res)
	require.Len(t, taken, 20)
	require.Equal(t, cands[0].ConditionID, taken[0].ConditionID)
	require.Equal(t, "board_alloc", taken[0].Reason["source"])
}

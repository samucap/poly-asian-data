package signaleval

import "math"

// WorthinessFilter is a replay-time D2 bar (does not rewrite the DB stream).
type WorthinessFilter struct {
	MinEdgeBps   float64
	MaxSpreadBps float64
	RequireFV    bool
}

// ApplyWorthiness drops intents that would not clear the auction eligibility bar.
func ApplyWorthiness(sigs []SignalIn, f WorthinessFilter) []SignalIn {
	if f.MinEdgeBps <= 0 && f.MaxSpreadBps <= 0 && !f.RequireFV {
		return sigs
	}
	out := make([]SignalIn, 0, len(sigs))
	for _, s := range sigs {
		if f.MinEdgeBps > 0 && math.Abs(s.EdgeBps) < f.MinEdgeBps {
			continue
		}
		if f.MaxSpreadBps > 0 && s.SpreadBps > f.MaxSpreadBps {
			continue
		}
		if f.RequireFV {
			if s.FVSource == "" || s.FVSource == "none" || s.ScorePath == "proxy" {
				continue
			}
		}
		out = append(out, s)
	}
	return out
}

func (f WorthinessFilter) Note() string {
	if f.MinEdgeBps <= 0 && f.MaxSpreadBps <= 0 && !f.RequireFV {
		return ""
	}
	return "worthiness_filter applied (replay); live emit still uses ws-listener gates"
}

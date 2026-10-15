package tagagg

import (
	"fmt"
	"math"
	"sort"

	"github.com/samucap/poly-asian-data/internal/services"
)

// TagAttributionCheck compares cycle attribution to Gamma activeEventsCount.
type TagAttributionCheck struct {
	TagID              string
	Slug               string
	Label              string
	ActiveEventsCount  int // from Gamma (catalog seed)
	AttributedEvents   int // unique events we credited this cycle
	AttributedMarkets  int // TotalMarkets sum this cycle
	RatioEvents        float64
	Suspicious         bool
	Reason             string
}

// ValidateAgainstActiveEvents compares EventCountByTag / TotalMarkets to
// ActiveEventsCount preserved on catalog working copies.
//
// A tag is flagged when Gamma reports a small activeEventsCount but we attributed
// many more events (classic catch-all / hierarchy bug), or when ratio is extreme.
func ValidateAgainstActiveEvents(res Result, minActiveForCheck int) []TagAttributionCheck {
	if minActiveForCheck <= 0 {
		minActiveForCheck = 0
	}
	var out []TagAttributionCheck
	for id, t := range res.Tags {
		if t == nil || id == "" {
			continue
		}
		attrEv := 0
		if res.EventCountByTag != nil {
			attrEv = res.EventCountByTag[id]
		}
		// Skip idle tags with no attribution and no gamma signal.
		if attrEv == 0 && t.TotalMarkets == 0 && t.ActiveEventsCount == 0 {
			continue
		}
		// Only validate when Gamma provided a count (API field present / non-zero or catch-all).
		// activeEventsCount=0 may mean "unknown" on old DB rows — skip those.
		gammaN := t.ActiveEventsCount
		if gammaN == 0 && !IsCatchAllTag(t) && attrEv == 0 {
			continue
		}

		ratio := 0.0
		if gammaN > 0 {
			ratio = float64(attrEv) / float64(gammaN)
		} else if attrEv > 0 {
			ratio = math.Inf(1)
		}

		c := TagAttributionCheck{
			TagID:             id,
			Slug:              t.Slug,
			Label:             t.Label,
			ActiveEventsCount: gammaN,
			AttributedEvents:  attrEv,
			AttributedMarkets: t.TotalMarkets,
			RatioEvents:       ratio,
		}

		// Suspicious: catch-all with any inflation, or attributed events >> gamma count.
		switch {
		case IsCatchAllTag(t) && (attrEv > gammaN+2 || t.TotalMarkets > 10):
			c.Suspicious = true
			c.Reason = "catch-all tag should not absorb hierarchy volume"
		case gammaN > 0 && attrEv > gammaN*3 && attrEv-gammaN >= 5:
			c.Suspicious = true
			c.Reason = fmt.Sprintf("attributed_events=%d >> activeEventsCount=%d", attrEv, gammaN)
		case gammaN == 0 && attrEv >= 50 && IsCatchAllTag(t):
			c.Suspicious = true
			c.Reason = "catch-all with large attribution and missing activeEventsCount"
		}

		// Always include catch-all and suspicious; include tops with gamma count for logging sample.
		if c.Suspicious || IsCatchAllTag(t) || (gammaN >= minActiveForCheck && attrEv > 0) {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Suspicious != out[j].Suspicious {
			return out[i].Suspicious
		}
		return out[i].AttributedEvents > out[j].AttributedEvents
	})
	return out
}

// SuspiciousOnly filters validation results.
func SuspiciousOnly(checks []TagAttributionCheck) []TagAttributionCheck {
	var out []TagAttributionCheck
	for _, c := range checks {
		if c.Suspicious {
			out = append(out, c)
		}
	}
	return out
}

// Ensure ActiveEventsCount is copied on zeroCopy — already is via struct copy.
var _ = services.PlyMktTag{}

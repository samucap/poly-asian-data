// Package eval defines the M4 evaluation contract: bias-aware metrics, gates,
// and forbidden inputs so edge-eval cannot ship vanity win-rates.
//
// Implementation of label writers and full edge-eval CLI is M4 proper; this
// package is the enforceable contract those tools must satisfy.
package eval

import "strings"

// SchemaVersion is eval_surface artifact schema_version.
const SchemaVersion = "1.0"

// ArtifactPipeline is artifacts/ subdirectory for eval surfaces.
const ArtifactPipeline = "eval_surface"

// Default horizons for PIT labels (M4).
var DefaultHorizons = []string{"5m", "1h", "1d"}

// Required strata dimensions for any promote-eligible eval_surface.
var RequiredStrata = []string{
	"overall",
	"by_category",
	"by_neg_risk",   // neg_risk vs standalone
	"by_fv_source",  // fair_value path vs proxy / none
	"by_ttr_bucket", // near / mid / far
}

// RequiredBaselines must appear in metrics.baselines for non-vanity eval.
// Candidate strategies are compared against these, not absolute win-rate alone.
var RequiredBaselines = []string{
	"volume_top_n",    // rank by volume24hr only
	"activity_stage1", // Stage-1 activity score cut to board N
	"random_board",    // random N from Stage-1 or pool (seeded)
}

// ForbiddenFeatureNames must never appear as model/strategy *inputs* in eval configs.
// Ranking scores that selected the sample induce circular selection bias.
var ForbiddenFeatureNames = []string{
	"computed_score",
	"ComputedScore",
	"stage1_score",
	"score", // Stage-1 activity diagnostic on board rows
	"edge_bps",
	"model_edge_bps",
	"opportunity_bps",
	"rank",
	"board_rank",
}

// ForbiddenFeaturePrefixes catch variants.
var ForbiddenFeaturePrefixes = []string{
	"score_",
	"rank_",
	"edge_bps_",
}

// SelectionSet labels which population metrics were computed on.
const (
	SelectionBoard  = "board_at_t"  // markets on edge_board at decision time t
	SelectionStage1 = "stage1_at_t" // Stage-1 budget set (broader; optional report)
	SelectionPool   = "pool_at_t"   // filtered keyset pool (optional)
)

// GateIDs are machine-stable gate names on eval_surface.
const (
	GatePITLabels             = "pit_labels"
	GateNoLookahead           = "no_lookahead"
	GateNoForbiddenFeatures   = "no_forbidden_features"
	GateSelectionDocumented   = "selection_documented"
	GateBaselinesPresent      = "baselines_present"
	GateBeatsVolumeBaseline   = "beats_volume_baseline"
	GateBeatsActivityBaseline = "beats_activity_baseline"
	GateAfterCostReported     = "after_cost_reported"
	GateStratified            = "stratified_metrics"
	GateMinSample             = "min_sample"
	GateFillModelDocumented   = "fill_model_documented"
	// GatePromoteEligible: ok + after_cost > 0 + policy_parity=scan_board_v1
	GatePromoteEligible = "promote_eligible"
	// GatePolicyParity: candidate uses edge.SelectBoard (scan_board_v1)
	GatePolicyParity = "policy_parity"
)

// DefaultMinSample is the minimum labeled decisions for promote-eligible overall metrics.
const DefaultMinSample = 50

// IsForbiddenFeature reports whether name is disallowed as an eval/model input feature.
func IsForbiddenFeature(name string) bool {
	n := strings.TrimSpace(name)
	if n == "" {
		return false
	}
	lower := strings.ToLower(n)
	for _, f := range ForbiddenFeatureNames {
		if lower == strings.ToLower(f) {
			return true
		}
	}
	for _, p := range ForbiddenFeaturePrefixes {
		if strings.HasPrefix(lower, strings.ToLower(p)) {
			return true
		}
	}
	return false
}

// CheckForbiddenFeatures returns names that violate the protocol.
func CheckForbiddenFeatures(names []string) (bad []string) {
	seen := map[string]bool{}
	for _, n := range names {
		if IsForbiddenFeature(n) && !seen[n] {
			seen[n] = true
			bad = append(bad, n)
		}
	}
	return bad
}

#!/usr/bin/env bash
# freeze_scorecards.sh — snapshot latest board + paper backtest surfaces.
#
# You do NOT need a clever NOTE/LABEL every time. Manifest auto-fills:
#   git, strategy, weights_hash, risk config_hash, headline metrics,
#   auto_title, auto_summary, and delta vs previous freeze (if any).
# Optional LABEL/NOTE only if you want a human tag on top.
#
# Directory name (local wall clock by default — not UTC):
#   artifacts/scorecards/2026-08-06T202748[_optional-label]/
#     manifest.json
#     board_eval.json    # board-policy backtest (edge-eval)
#     paper_eval.json    # paper portfolio backtest (signal-eval)
#   artifacts/scorecards/latest -> most recent freeze
#
# Usage:
#   make scorecard-freeze
#   make scorecard-freeze LABEL=after-d2
#   ./scripts/freeze_scorecards.sh --board path --paper path
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

SCORECARDS_ROOT="${SCORECARDS_ROOT:-artifacts/scorecards}"
BOARD_SRC="${BOARD:-artifacts/eval_surface/latest.json}"
PAPER_SRC="${PAPER:-artifacts/signal_eval_surface/latest.json}"
LABEL="${LABEL:-}"
NOTE="${NOTE:-}"
# local | utc
TIME_MODE="${SCORECARD_TIME:-local}"

while [[ $# -gt 0 ]]; do
	case "$1" in
	--label) LABEL="${2:-}"; shift 2 ;;
	--note) NOTE="${2:-}"; shift 2 ;;
	--board) BOARD_SRC="${2:-}"; shift 2 ;;
	--paper) PAPER_SRC="${2:-}"; shift 2 ;;
	--root) SCORECARDS_ROOT="${2:-}"; shift 2 ;;
	--utc) TIME_MODE=utc; shift ;;
	--local) TIME_MODE=local; shift ;;
	-h | --help)
		sed -n '2,22p' "$0"
		exit 0
		;;
	*)
		echo "unknown arg: $1" >&2
		exit 2
		;;
	esac
done

need() {
	if [[ ! -f "$1" ]]; then
		echo "error: missing source surface: $1" >&2
		echo "run edge-eval and signal-eval first, or pass --board / --paper" >&2
		exit 1
	fi
}
need "$BOARD_SRC"
need "$PAPER_SRC"

if ! command -v jq >/dev/null 2>&1; then
	echo "error: jq is required for scorecard freezes" >&2
	exit 1
fi

LABEL_SAFE=""
if [[ -n "$LABEL" ]]; then
	LABEL_SAFE="$(echo "$LABEL" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9._-]+/-/g; s/^-+//; s/-+$//')"
fi

if [[ "$TIME_MODE" == "utc" ]]; then
	STAMP="$(date -u +%Y-%m-%dT%H%M%SZ)"
else
	# Local wall clock (what your machine shows) — easier to match "I froze this at 8pm"
	STAMP="$(date +%Y-%m-%dT%H%M%S)"
fi

if [[ -n "$LABEL_SAFE" ]]; then
	DIR_NAME="${STAMP}_${LABEL_SAFE}"
else
	DIR_NAME="$STAMP"
fi
OUT_DIR="${SCORECARDS_ROOT}/${DIR_NAME}"

if [[ -e "$OUT_DIR" ]]; then
	echo "error: freeze dir already exists: $OUT_DIR" >&2
	exit 1
fi

mkdir -p "$SCORECARDS_ROOT" "$OUT_DIR"
cp "$BOARD_SRC" "$OUT_DIR/board_eval.json"
cp "$PAPER_SRC" "$OUT_DIR/paper_eval.json"

GIT_SHA="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
GIT_SHORT="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
GIT_BRANCH="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
GIT_DIRTY=false
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
	if [[ -n "$(git status --porcelain 2>/dev/null)" ]]; then
		GIT_DIRTY=true
	fi
fi

hash_file() {
	local f="$1"
	if [[ ! -f "$f" ]]; then
		echo ""
		return
	fi
	if command -v shasum >/dev/null 2>&1; then
		shasum -a 256 "$f" | awk '{print $1}'
	elif command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$f" | awk '{print $1}'
	else
		echo "unavailable"
	fi
}

BOARD_SHA="$(hash_file "$OUT_DIR/board_eval.json")"
PAPER_SHA="$(hash_file "$OUT_DIR/paper_eval.json")"

WEIGHTS_PATH="$(jq -r '.weights_path // empty' "$OUT_DIR/board_eval.json")"
if [[ -z "$WEIGHTS_PATH" ]]; then
	WEIGHTS_PATH="configs/strategies/default.yaml"
fi
WEIGHTS_FILE_SHA="$(hash_file "$WEIGHTS_PATH")"

# Risk YAML path is not always on the surface; default + allow override
RISK_PATH="${RISK:-configs/risk/default.yaml}"
RISK_FILE_SHA="$(hash_file "$RISK_PATH")"

# Previous freeze for deltas (before we retarget latest)
PREV_JSON_FILE="$(mktemp)"
cleanup() { rm -f "$PREV_JSON_FILE"; }
trap cleanup EXIT
if [[ -e "${SCORECARDS_ROOT}/latest/manifest.json" ]]; then
	cp "${SCORECARDS_ROOT}/latest/manifest.json" "$PREV_JSON_FILE"
else
	echo '[]' >"$PREV_JSON_FILE" # empty slurp → no previous (we use object file below)
	# slurpfile expects JSON value; use null object list via a single null
	echo 'null' >"$PREV_JSON_FILE"
fi

FROZEN_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
FROZEN_LOCAL="$(date +%Y-%m-%dT%H:%M:%S%z)"
TZ_NAME="$(date +%Z)"

# shellcheck disable=SC2016
jq -n \
	--arg freeze_id "$DIR_NAME" \
	--arg frozen_at_utc "$FROZEN_UTC" \
	--arg frozen_at_local "$FROZEN_LOCAL" \
	--arg tz "$TZ_NAME" \
	--arg time_mode "$TIME_MODE" \
	--arg label "$LABEL" \
	--arg note "$NOTE" \
	--arg git_sha "$GIT_SHA" \
	--arg git_short "$GIT_SHORT" \
	--arg git_branch "$GIT_BRANCH" \
	--argjson git_dirty "$GIT_DIRTY" \
	--arg board_src "$BOARD_SRC" \
	--arg paper_src "$PAPER_SRC" \
	--arg board_sha256 "$BOARD_SHA" \
	--arg paper_sha256 "$PAPER_SHA" \
	--arg weights_path "$WEIGHTS_PATH" \
	--arg weights_file_sha256 "$WEIGHTS_FILE_SHA" \
	--arg risk_path "$RISK_PATH" \
	--arg risk_file_sha256 "$RISK_FILE_SHA" \
	--slurpfile board "$OUT_DIR/board_eval.json" \
	--slurpfile paper "$OUT_DIR/paper_eval.json" \
	--slurpfile prev "$PREV_JSON_FILE" \
	'
	($board[0]) as $b | ($paper[0]) as $p |
	# prev slurpfile: [manifest] or [null]
	(if ($prev|length) > 0 and $prev[0] != null and ($prev[0]|type) == "object" then $prev[0] else null end) as $prev |
	($b.weights_hash // "") as $wh |
	($p.config_hash // "") as $rh |
	($b.strategy_name // "default") as $sname |
	# Auto title from machine identity — no manual annotation required
	(
		$sname
		+ "_w" + (if ($wh|length) >= 8 then $wh[0:8] else "unknown" end)
		+ "_r" + (if ($rh|length) >= 8 then $rh[0:8] else "unknown" end)
	) as $auto_title |
	# One-line economic readout (never use bool // x — false is "missing" under //)
	(
		"board after_cost_bps=" + (( $b.metrics.overall.after_cost_return_bps )|tostring)
		+ " hit=" + (( $b.metrics.overall.hit_rate )|tostring)
		+ " promote=" + (( $b.promote_eligible )|tostring)
		+ " | paper pnl_usd=" + (( $p.metrics.total_pnl_usd )|tostring)
		+ " acc=" + (( $p.metrics.n_accepted )|tostring)
		+ "/" + (( $p.metrics.n_signals )|tostring)
		+ " max_dd_bps=" + (( $p.metrics.max_drawdown_bps )|tostring)
	) as $auto_summary |
	# Verdicts are machine booleans/strings, not UI colors
	{
		board_ok: ($b.ok == true),
		board_promote_eligible: ($b.promote_eligible == true),
		board_after_cost_positive: (( $b.metrics.overall.after_cost_return_bps // -1 ) > 0),
		paper_ok: ($p.ok == true),
		paper_economic_pass: ($p.economic_pass == true),
		paper_pnl_positive: (( $p.metrics.total_pnl_usd // -1 ) > 0)
	} as $verdicts |
	(
		if $prev == null then null else {
			prev_freeze_id: $prev.freeze_id,
			prev_auto_title: $prev.auto_title,
			delta_after_cost_bps: (
				if ($b.metrics.overall.after_cost_return_bps != null)
					and ($prev.board_eval.after_cost_return_bps != null)
				then $b.metrics.overall.after_cost_return_bps - $prev.board_eval.after_cost_return_bps
				else null end
			),
			delta_paper_pnl_usd: (
				if ($p.metrics.total_pnl_usd != null)
					and ($prev.paper_eval.total_pnl_usd != null)
				then $p.metrics.total_pnl_usd - $prev.paper_eval.total_pnl_usd
				else null end
			),
			delta_paper_max_dd_bps: (
				if ($p.metrics.max_drawdown_bps != null)
					and ($prev.paper_eval.max_drawdown_bps != null)
				then $p.metrics.max_drawdown_bps - $prev.paper_eval.max_drawdown_bps
				else null end
			),
			same_weights_hash: (($b.weights_hash // null) == ($prev.board_eval.weights_hash // false)),
			same_risk_config_hash: (($p.config_hash // null) == ($prev.paper_eval.config_hash // false)),
			same_git: (($git_short) == ($prev.code.git_short // ""))
		} end
	) as $vs_prev |
	{
		schema: "scorecard_freeze_v1",
		kind: "backtest_scorecard_pair",
		freeze_id: $freeze_id,
		frozen_at_utc: $frozen_at_utc,
		frozen_at_local: $frozen_at_local,
		timezone: $tz,
		dir_time_mode: $time_mode,
		# Optional human tag — not required for interpretability
		label: (if $label == "" then null else $label end),
		note: (if $note == "" then null else $note end),
		auto_title: $auto_title,
		auto_summary: $auto_summary,
		verdicts: $verdicts,
		vs_previous: $vs_prev,
		code: {
			git_sha: $git_sha,
			git_short: $git_short,
			git_branch: $git_branch,
			git_dirty: $git_dirty
		},
		configs: {
			strategy_name: $sname,
			weights_path: $weights_path,
			weights_hash: $wh,
			weights_file_sha256: (if $weights_file_sha256 == "" then null else $weights_file_sha256 end),
			risk_path: $risk_path,
			risk_config_hash: $rh,
			risk_file_sha256: (if $risk_file_sha256 == "" then null else $risk_file_sha256 end),
			strategy_version_id: $b.strategy_version_id,
			action_model: $b.action_model,
			policy_parity: $b.policy_parity
		},
		sources: {
			board_eval_path: $board_src,
			paper_eval_path: $paper_src
		},
		files: {
			board_eval: "board_eval.json",
			paper_eval: "paper_eval.json",
			board_sha256: $board_sha256,
			paper_sha256: $paper_sha256
		},
		board_eval: {
			role: "board_policy_backtest",
			pipeline: "edge-eval",
			run_id: $b.run_id,
			generated_at: $b.generated_at,
			pipeline_version: $b.pipeline_version,
			code_commit: $b.code_commit,
			strategy_name: $b.strategy_name,
			strategy_version_id: $b.strategy_version_id,
			weights_hash: $b.weights_hash,
			weights_path: $b.weights_path,
			policy_parity: $b.policy_parity,
			action_model: $b.action_model,
			ok: $b.ok,
			promote_eligible: $b.promote_eligible,
			status: $b.status,
			n: $b.metrics.n,
			after_cost_return_bps: $b.metrics.overall.after_cost_return_bps,
			hit_rate: $b.metrics.overall.hit_rate,
			portfolio_sharpe: $b.metrics.portfolio.sharpe,
			portfolio_max_drawdown_bps: $b.metrics.portfolio.max_drawdown_bps,
			book_coverage: $b.book_coverage,
			fv_coverage: $b.fv_coverage,
			delta_vs_baselines: $b.metrics.delta_vs_baselines
		},
		paper_eval: {
			role: "paper_portfolio_backtest",
			pipeline: "signal-eval",
			run_id: $p.run_id,
			generated_at: $p.generated_at,
			pipeline_version: $p.pipeline_version,
			strategy_name: $p.strategy_name,
			config_hash: $p.config_hash,
			ok: $p.ok,
			economic_pass: $p.economic_pass,
			status: $p.status,
			window: $p.window,
			n_signals: $p.metrics.n_signals,
			n_accepted: $p.metrics.n_accepted,
			n_rejected_risk: $p.metrics.n_rejected_risk,
			total_pnl_usd: $p.metrics.total_pnl_usd,
			hit_rate: $p.metrics.hit_rate,
			sharpe: $p.metrics.sharpe,
			max_drawdown_bps: $p.metrics.max_drawdown_bps,
			starting_equity_usd: ($p.metrics.starting_equity_usd // $p.risk.starting_equity_usd),
			ending_equity_usd: $p.metrics.ending_equity_usd,
			reject_reasons: $p.metrics.reject_reasons,
			risk: $p.risk
		}
	}
	' >"$OUT_DIR/manifest.json"

# Retarget latest symlink
if [[ -L "${SCORECARDS_ROOT}/latest" ]]; then
	rm -f "${SCORECARDS_ROOT}/latest"
elif [[ -e "${SCORECARDS_ROOT}/latest" ]]; then
	echo "error: artifacts/scorecards/latest exists and is not a symlink; move it aside" >&2
	exit 1
fi
ln -sfn "$DIR_NAME" "${SCORECARDS_ROOT}/latest"

echo "scorecard freeze: $OUT_DIR"
jq -r '
	"  when:    \(.frozen_at_local) (\(.timezone); dir=\(.dir_time_mode))",
	"  title:   \(.auto_title)",
	"  summary: \(.auto_summary)",
	"  git:     \(.code.git_short) \(.code.git_branch) dirty=\(.code.git_dirty)",
	"  configs: strategy=\(.configs.strategy_name) weights=\(.configs.weights_path)",
	"           risk=\(.configs.risk_path)",
	(if .label then "  label:   \(.label)" else empty end),
	(if .note then "  note:    \(.note)" else empty end),
	(if .vs_previous then
		"  vs prev: \(.vs_previous.prev_freeze_id)",
		"           Δafter_cost_bps=\(.vs_previous.delta_after_cost_bps) Δpnl_usd=\(.vs_previous.delta_paper_pnl_usd) same_weights=\(.vs_previous.same_weights_hash) same_risk=\(.vs_previous.same_risk_config_hash)"
	 else "  vs prev: (none — first freeze or no prior latest)" end),
	"  verdicts: board_ok=\(.verdicts.board_ok) promote=\(.verdicts.board_promote_eligible) after_cost>0=\(.verdicts.board_after_cost_positive) paper_pnl>0=\(.verdicts.paper_pnl_positive)"
' "$OUT_DIR/manifest.json"

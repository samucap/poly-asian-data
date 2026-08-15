#!/usr/bin/env bash
# show_scorecard.sh [freeze_id]
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
NAME="${1:-}"
if [[ -n "$NAME" ]]; then
	DIR="artifacts/scorecards/${NAME}"
elif [[ -e artifacts/scorecards/latest/manifest.json ]]; then
	DIR="artifacts/scorecards/latest"
else
	DIR="$(ls -1d artifacts/scorecards/[0-9]*/ 2>/dev/null | tail -1 || true)"
fi
if [[ -z "${DIR:-}" || ! -f "${DIR}/manifest.json" ]]; then
	echo "no freeze found" >&2
	exit 1
fi
echo "$DIR"
jq '{
  freeze_id, auto_title, auto_summary,
  when: .frozen_at_local, tz: .timezone,
  label, note,
  git: .code.git_short,
  configs: {
    strategy: .configs.strategy_name,
    weights_hash: .configs.weights_hash,
    risk_config_hash: .configs.risk_config_hash
  },
  verdicts, vs_previous,
  board: {
    after_cost_bps: .board_eval.after_cost_return_bps,
    promote: .board_eval.promote_eligible,
    n: .board_eval.n,
    book_coverage_board: .board_eval.book_coverage_board,
    book_coverage_universe: .board_eval.book_coverage_universe,
    fv_coverage: .board_eval.fv_coverage
  },
  paper: {
    pnl_usd: .paper_eval.total_pnl_usd,
    accepted: .paper_eval.n_accepted,
    signals: .paper_eval.n_signals,
    n_signals_db: .paper_eval.n_signals_db,
    capped: .paper_eval.capped,
    max_dd_bps: .paper_eval.max_drawdown_bps,
    reject_reasons: .paper_eval.reject_reasons
  }
}' "${DIR}/manifest.json"

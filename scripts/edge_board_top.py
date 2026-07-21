#!/usr/bin/env python3
"""Explain top N markets from artifacts/edge_board/latest.json (M3 checklist).

Usage:
  python3 scripts/edge_board_top.py
  python3 scripts/edge_board_top.py --n 10
  python3 scripts/edge_board_top.py --path artifacts/edge_board/latest.json
  make edge-board-top
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def _num(x: Any, default: float = 0.0) -> float:
    if x is None:
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _fmt(x: Any, digits: int = 1) -> str:
    if x is None:
        return "—"
    try:
        return f"{float(x):.{digits}f}"
    except (TypeError, ValueError):
        return str(x)


def explain_row(r: dict[str, Any]) -> list[str]:
    """Human reasons why this market ranked (or why to distrust it)."""
    kf = r.get("key_features") or {}
    reasons: list[str] = []
    cost = _num(r.get("cost_bps"))
    edge = _num(r.get("edge_bps"))
    mid = _num(kf.get("mid"))
    spread_bps = _num(kf.get("spread_bps"))
    one_day = _num(kf.get("one_day_abs"))
    ttr = _num(kf.get("ttr_hours"))
    imb = _num(kf.get("imbalance"))
    residual = _num(kf.get("neg_risk_residual_bps"))
    vol = _num(kf.get("volume_24hr") or r.get("volume_24hr"))

    if cost < 30:
        reasons.append(f"cheap book (cost≈{_fmt(cost)} bps)")
    elif cost < 100:
        reasons.append(f"moderate cost ({_fmt(cost)} bps)")
    else:
        reasons.append(f"expensive book (cost≈{_fmt(cost)} bps)")

    if one_day >= 0.05:
        reasons.append(f"large 1d move (|Δp|≈{_fmt(one_day, 3)})")
    elif one_day >= 0.02:
        reasons.append(f"recent move (|Δp|≈{_fmt(one_day, 3)})")

    if 96 <= ttr <= 504:
        reasons.append(f"TTR in preferred window (~{_fmt(ttr / 24, 1)}d)")
    elif 0 < ttr < 96:
        reasons.append(f"near-term resolution (~{_fmt(ttr, 0)}h)")
    elif ttr > 1080:
        reasons.append(f"long-dated (~{_fmt(ttr / 24, 0)}d)")

    if vol >= 50_000:
        reasons.append(f"active (vol24≈{_fmt(vol, 0)})")

    if residual >= 50:
        reasons.append(f"neg-risk residual ≈{_fmt(residual)} bps")

    if abs(imb - 0.5) >= 0.25:
        side = "bid-heavy" if imb > 0.5 else "ask-heavy"
        reasons.append(f"skewed book ({side}, imb={_fmt(imb, 2)})")

    if mid >= 0.98 or mid <= 0.02:
        reasons.append(f"extreme mid≈{_fmt(mid, 3)} (near-certain / near-zero)")

    if spread_bps > 0 and spread_bps < 50:
        reasons.append(f"tight spread ({_fmt(spread_bps)} bps)")
    elif spread_bps >= 200:
        reasons.append(f"wide spread ({_fmt(spread_bps)} bps)")

    flags = r.get("risk_flags") or []
    if flags:
        reasons.append("FLAGS: " + ",".join(flags))

    if edge > 0:
        reasons.append(f"net edge positive after costs (+{_fmt(edge)} bps)")
    else:
        reasons.append(f"net edge negative after costs ({_fmt(edge)} bps)")

    return reasons


def main() -> int:
    ap = argparse.ArgumentParser(description="Explain top edge_board markets from artifact")
    ap.add_argument(
        "--path",
        default="artifacts/edge_board/latest.json",
        help="path to edge_board artifact (default: artifacts/edge_board/latest.json)",
    )
    ap.add_argument("-n", "--n", type=int, default=5, help="how many rows to explain (default 5)")
    ap.add_argument("--json", action="store_true", help="raw JSON for the selected rows only")
    args = ap.parse_args()

    path = Path(args.path)
    if not path.is_file():
        print(f"error: artifact not found: {path}", file=sys.stderr)
        print("hint: run  make run-edge-scan-once  then  make edge-board-top", file=sys.stderr)
        return 1

    with path.open() as f:
        doc = json.load(f)

    board = doc.get("board") or []
    stats = doc.get("board_stats") or {}
    n = max(0, args.n)
    top = board[:n]

    if args.json:
        json.dump(
            {
                "status": doc.get("status"),
                "schema_version": doc.get("schema_version"),
                "board_stats": stats,
                "board": top,
            },
            sys.stdout,
            indent=2,
        )
        print()
        return 0

    print("edge_board top")
    print("-" * 72)
    print(
        f"status={doc.get('status')}  schema={doc.get('schema_version')}  "
        f"strategy={doc.get('strategy', 'default')}"
    )
    print(
        f"pool={stats.get('n_candidates')}  stage1={stats.get('n_stage1')}  "
        f"board={stats.get('n_board')}  "
        f"enrich_coverage={stats.get('enrich_coverage')}  "
        f"cycle_ms={stats.get('cycle_ms')}  enrich_ms={stats.get('enrich_ms')}"
    )
    print(
        f"median_edge_bps={_fmt(stats.get('median_edge_bps'))}  "
        f"median_stage1={_fmt(stats.get('median_score'), 3)}  "
        f"run_id={doc.get('run_id', '')[:8]}…"
        if doc.get("run_id")
        else f"median_edge_bps={_fmt(stats.get('median_edge_bps'))}"
    )

    null_edge = sum(1 for r in board if r.get("edge_bps") is None)
    print(f"rows_with_edge={len(board) - null_edge}/{len(board)}")
    print()

    if not top:
        print("(empty board)")
        return 1

    for r in top:
        kf = r.get("key_features") or {}
        q = (r.get("question_short") or r.get("condition_id") or "?")[:72]
        print("=" * 72)
        print(
            f"#{r.get('rank')}  edge_bps={_fmt(r.get('edge_bps'))}  "
            f"cost_bps={_fmt(r.get('cost_bps'))}  stage1={_fmt(r.get('score'), 3)}  "
            f"capacity_usd={_fmt(r.get('capacity_usd'), 0)}"
        )
        print(q)
        print(
            f"  category={r.get('category') or '—'}  "
            f"neg_risk={r.get('neg_risk')}  "
            f"tags={r.get('strategy_tags') or []}"
        )
        print(
            f"  mid={_fmt(kf.get('mid'), 3)}  spread_bps={_fmt(kf.get('spread_bps'))}  "
            f"imb={_fmt(kf.get('imbalance'), 3)}  "
            f"one_day_abs={_fmt(kf.get('one_day_abs'), 4)}  "
            f"ttr_h={_fmt(kf.get('ttr_hours'), 1)}  "
            f"vol24={_fmt(kf.get('volume_24hr') or r.get('volume_24hr'), 0)}"
        )
        if kf.get("neg_risk_residual_bps"):
            print(f"  residual_bps={_fmt(kf.get('neg_risk_residual_bps'))}")
        if r.get("risk_flags"):
            print(f"  risk_flags={r.get('risk_flags')}")
        reasons = explain_row(r)
        print("  → " + "; ".join(reasons))

    print("=" * 72)
    print(
        "note: edge_bps is cost-adjusted opportunity screen, not guaranteed alpha. "
        "stage1 is activity budget only."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

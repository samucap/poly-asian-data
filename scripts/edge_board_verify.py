#!/usr/bin/env python3
"""Verify M3.5 edge_board artifact accuracy + basic optimality gates.

Usage:
  python3 scripts/edge_board_verify.py
  python3 scripts/edge_board_verify.py --path artifacts/edge_board/latest.json
  python3 scripts/edge_board_verify.py --buffer 20 --max-cycle-ms 120000
  make edge-board-verify

Exit 0 = all hard gates pass; exit 1 = failure.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


def _num(x: Any, default: float | None = None) -> float | None:
    if x is None:
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def main() -> int:
    ap = argparse.ArgumentParser(description="Verify edge_board artifact (M3.5 gates)")
    ap.add_argument(
        "--path",
        default="artifacts/edge_board/latest.json",
        help="edge_board artifact path",
    )
    ap.add_argument(
        "--buffer",
        type=float,
        default=20.0,
        help="model_buffer_bps used when scoring (default 20, match configs/strategies/default.yaml)",
    )
    ap.add_argument(
        "--tol",
        type=float,
        default=2.0,
        help="allowed bps error for FV identity (default 2)",
    )
    ap.add_argument(
        "--max-cycle-ms",
        type=float,
        default=120_000.0,
        help="hard fail if cycle_ms above this (default 120s)",
    )
    ap.add_argument(
        "--min-enrich-coverage",
        type=float,
        default=0.5,
        help="hard fail if enrich_coverage below this when books were expected (default 0.5)",
    )
    ap.add_argument(
        "--require-fv",
        action="store_true",
        help="hard fail if no FV rows (default: warn only — groups may be incomplete)",
    )
    ap.add_argument(
        "--strict-extreme",
        action="store_true",
        help="hard fail if top-5 contains mid outside [0.05,0.95] (default: warn)",
    )
    args = ap.parse_args()

    path = Path(args.path)
    if not path.is_file():
        print(f"FAIL  artifact not found: {path}", file=sys.stderr)
        print("hint: make run-edge-scan ARGS='--once'", file=sys.stderr)
        return 1

    with path.open() as f:
        doc = json.load(f)

    board = doc.get("board") or []
    stats = doc.get("board_stats") or {}
    status = doc.get("status")
    schema = doc.get("schema_version")

    hard: list[str] = []
    soft: list[str] = []

    print("edge_board verify")
    print("-" * 72)
    print(f"path={path}")
    print(f"status={status}  schema={schema}  strategy={doc.get('strategy', 'default')}")
    print(
        f"board={stats.get('n_board')}  stage1={stats.get('n_stage1')}  "
        f"enrich_coverage={stats.get('enrich_coverage')}  "
        f"fv_coverage={stats.get('fv_coverage')}  fv_hits={stats.get('fv_hits')}  "
        f"cycle_ms={stats.get('cycle_ms')}  enrich_ms={stats.get('enrich_ms')}"
    )
    print(f"buffer_bps={args.buffer}  tol_bps={args.tol}")
    print()

    # --- hard: status / schema / non-empty ---
    if status not in ("success", "partial"):
        hard.append(f"status must be success|partial, got {status!r}")
    if schema not in ("1.0", "2.0"):
        hard.append(f"unexpected schema_version {schema!r}")
    if schema != "2.0":
        soft.append(f"schema is {schema}, prefer 2.0 for M3.5 fields")
    if not board:
        hard.append("empty board")

    # --- hard: cycle budget ---
    cycle_ms = _num(stats.get("cycle_ms"), 0.0) or 0.0
    if cycle_ms > args.max_cycle_ms:
        hard.append(f"cycle_ms {cycle_ms:.0f} > max {args.max_cycle_ms:.0f}")

    # --- enrich coverage ---
    cov = _num(stats.get("enrich_coverage"))
    if cov is not None and cov < args.min_enrich_coverage:
        hard.append(
            f"enrich_coverage {cov:.3f} < min {args.min_enrich_coverage:.3f}"
        )
    elif cov is not None and cov < 0.9:
        soft.append(f"enrich_coverage {cov:.3f} < 0.9 (partial books)")

    # --- FV identity ---
    fv_rows = 0
    id_mismatch = 0
    edge_mismatch = 0
    missing_fields = 0
    extremes_top5 = 0

    for i, r in enumerate(board):
        kf = r.get("key_features") or {}
        mid = _num(kf.get("mid"))
        if i < 5 and mid is not None and (mid < 0.05 or mid > 0.95):
            extremes_top5 += 1

        fv = r.get("fair_value")
        if fv is None:
            if r.get("fv_source"):
                hard.append(
                    f"rank {r.get('rank')}: fv_source set but fair_value null"
                )
            continue

        fv_rows += 1
        fv_f = _num(fv)
        me = _num(r.get("model_edge_bps"))
        eb = _num(r.get("edge_bps"))
        cost = _num(r.get("cost_bps"), 0.0) or 0.0
        src = r.get("fv_source") or ""

        if not src:
            hard.append(f"rank {r.get('rank')}: fair_value set but fv_source empty")
        if me is None:
            missing_fields += 1
            hard.append(f"rank {r.get('rank')}: fair_value set but model_edge_bps null")
            continue
        if mid is None or mid <= 0:
            soft.append(
                f"rank {r.get('rank')}: missing/rounded mid in key_features; skip identity"
            )
            continue

        # model_edge = (fv - mid)*1e4 - cost - buffer
        expected = (fv_f - mid) * 10_000 - cost - args.buffer
        if abs(expected - me) > args.tol:
            id_mismatch += 1
            if id_mismatch <= 8:
                hard.append(
                    f"rank {r.get('rank')} {r.get('condition_id')}: "
                    f"model_edge identity fail exp={expected:.1f} got={me:.1f} "
                    f"(fv={fv_f} mid={mid} cost={cost} buf={args.buffer})"
                )

        if eb is not None and abs(eb - me) > 1e-3:
            edge_mismatch += 1
            if edge_mismatch <= 5:
                hard.append(
                    f"rank {r.get('rank')}: edge_bps ({eb}) != model_edge_bps ({me}) on FV path"
                )

    print(f"fv_rows={fv_rows}/{len(board)}  identity_mismatches={id_mismatch}  "
          f"edge_ne_model={edge_mismatch}  extremes_in_top5={extremes_top5}")

    if fv_rows == 0:
        msg = "no FV rows (fv_coverage=0) — incomplete groups or Stage-1 composition"
        if args.require_fv:
            hard.append(msg)
        else:
            soft.append(msg + " (warn only; pass --require-fv to fail)")

    if extremes_top5:
        msg = f"{extremes_top5} of top-5 have extreme mid (outside 0.05–0.95)"
        if args.strict_extreme:
            hard.append(msg)
        else:
            soft.append(msg + " (warn; pass --strict-extreme to fail)")

    # --- report ---
    print()
    if soft:
        print("WARNINGS")
        for w in soft:
            print(f"  · {w}")
        print()
    if hard:
        print("FAIL")
        for h in hard:
            print(f"  · {h}")
        print()
        print("result: FAIL")
        return 1

    print("PASS  hard gates ok")
    if soft:
        print(f"       ({len(soft)} soft warning(s))")
    print()
    print("note: this checks pricing consistency, not trading profitability (M4).")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Build data/cash_residual_book_latest.json for dashboard visualization.

Neutral labeling: \"cash_residual_policy\" (never operator-desk / firm names).

Sources (first hit wins):
  1. --from-compare PATH  (h_first vs crash-only compare JSON)
  2. OPERATOR_SIZING_RUN_DIR or sibling ../quant/Diamond-Creek-Quant/data/runs/_latest
     with b4_crash_budget.csv (+ optional b4_h_first.csv, b4_exec_cadence.json)
  3. Existing data/cash_residual_book_latest.json (refresh metadata only) — skipped if stale empty
"""
from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / "data" / "cash_residual_book_latest.json"
DEFAULT_COMPARE = (
    REPO.parent.parent
    / "quant"
    / "Diamond-Creek-Quant"
    / "data"
    / "runs"
    / "_compare"
    / "b4_h_first_compare.json"
)


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def from_compare(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    a = raw.get("crash_only") or {}
    b = raw.get("crash_plus_h_first") or {}
    bumps = {f"{r.get('etf')}|{r.get('und')}": r for r in (b.get("h_bumps") or [])}
    pairs = []
    for row in b.get("pairs") or []:
        key = f"{row.get('etf')}|{row.get('und')}"
        bump = bumps.get(key) or {}
        pairs.append(
            {
                "etf": row.get("etf"),
                "underlying": row.get("und"),
                "weight": row.get("weight_final"),
                "gross_usd": row.get("gross_final"),
                "gross_pre_crash_usd": row.get("gross_solved"),
                "crash_mult": row.get("mult"),
                "L": row.get("L"),
                "runup": row.get("runup"),
                "h0": bump.get("h0"),
                "h1": bump.get("h1"),
                "h_first_reason": bump.get("reason"),
                "cadence_due": None,
            }
        )
    return {
        "schema": "cash_residual_book.v1",
        "policy_id": "cash_residual_policy",
        "policy_label": "Cash-residual policy",
        "generated_at": _now(),
        "asof": str(raw.get("generated") or "")[:10] or None,
        "source_kind": "compare_harness",
        "source_path": path.name,
        "note": str(raw.get("note") or ""),
        "methods": [
            "relative_pair_weights",
            "vcr_hedge",
            "crash_budget_cash_residual",
            "h_first",
            "exec_cadence",
        ],
        "sleeve": {
            "budget_usd": b.get("budget") or a.get("budget"),
            "deployed_usd": b.get("sleeve_out"),
            "cash_residual_usd": b.get("cash_residual"),
            "cash_residual_pct": b.get("cash_residual_pct"),
            "rho": b.get("rho"),
            "rho_basis": b.get("rho_basis") or "pct_of_sleeve",
            "n_pairs": b.get("n_pairs"),
            "n_capped": b.get("n_capped"),
            "h_first_enabled": bool(b.get("h_first_enabled")),
        },
        "baseline_crash_only": {
            "deployed_usd": a.get("sleeve_out"),
            "cash_residual_usd": a.get("cash_residual"),
            "cash_residual_pct": a.get("cash_residual_pct"),
            "n_capped": a.get("n_capped"),
        },
        "pairs": pairs,
        "pair_deltas": raw.get("pair_deltas") or [],
    }


def from_run_dir(run_dir: Path) -> dict[str, Any] | None:
    crash = run_dir / "b4_crash_budget.csv"
    if not crash.is_file():
        return None
    pairs: list[dict[str, Any]] = []
    with crash.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            etf = (row.get("ETF") or row.get("etf") or "").strip().upper()
            und = (row.get("Underlying") or row.get("underlying") or "").strip().upper()
            if not etf:
                continue
            pairs.append(
                {
                    "etf": etf,
                    "underlying": und,
                    "weight": _f(row.get("weight_gated") or row.get("weight")),
                    "gross_usd": _f(row.get("gross_capped_usd") or row.get("gross_final")),
                    "gross_pre_crash_usd": _f(row.get("gross_solved_usd") or row.get("gross_solved")),
                    "crash_mult": _f(row.get("crash_budget_mult") or row.get("mult")),
                    "L": _f(row.get("L")),
                    "runup": _f(row.get("runup")),
                    "h0": None,
                    "h1": None,
                    "h_first_reason": None,
                    "cadence_due": _boolish(row.get("cadence_due")),
                    "apply_reason": row.get("apply_reason"),
                }
            )
    h1_path = run_dir / "b4_h_first.csv"
    if h1_path.is_file():
        by = {(p["etf"], p["underlying"]): p for p in pairs}
        with h1_path.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                etf = (row.get("ETF") or "").strip().upper()
                und = (row.get("Underlying") or "").strip().upper()
                p = by.get((etf, und))
                if not p:
                    continue
                p["h0"] = _f(row.get("h0") or row.get("h_before"))
                p["h1"] = _f(row.get("h1") or row.get("h_after"))
                p["h_first_reason"] = row.get("reason") or row.get("h_first_reason")
    budget = None
    deployed = None
    if pairs:
        weights = [p["weight"] for p in pairs if p.get("weight") is not None]
        grosses = [p["gross_usd"] for p in pairs if p.get("gross_usd") is not None]
        if grosses:
            deployed = float(sum(grosses))
        # Prefer sidecar meta if present
    side = run_dir / "b4_exec_cadence.json"
    if side.is_file():
        try:
            side_raw = json.loads(side.read_text(encoding="utf-8"))
            due_map = {
                k: bool(v.get("due"))
                for k, v in (side_raw.get("pairs") or {}).items()
                if isinstance(v, dict)
            }
            for p in pairs:
                key = f"{p['etf']}|{p['underlying']}"
                if key in due_map:
                    p["cadence_due"] = due_map[key]
        except Exception:
            pass
    return {
        "schema": "cash_residual_book.v1",
        "policy_id": "cash_residual_policy",
        "policy_label": "Cash-residual policy",
        "generated_at": _now(),
        "asof": run_dir.name if run_dir.name not in ("_latest",) else None,
        "source_kind": "gtp_run",
        "source_path": run_dir.name,
        "note": "Built from GTP crash-budget / h_first / exec-cadence telemetry.",
        "methods": [
            "relative_pair_weights",
            "vcr_hedge",
            "crash_budget_cash_residual",
            "h_first",
            "exec_cadence",
        ],
        "sleeve": {
            "budget_usd": budget,
            "deployed_usd": deployed,
            "cash_residual_usd": None if budget is None or deployed is None else budget - deployed,
            "cash_residual_pct": None
            if budget is None or not budget or deployed is None
            else 100.0 * (budget - deployed) / budget,
            "rho": None,
            "rho_basis": "pct_of_sleeve",
            "n_pairs": len(pairs),
            "n_capped": sum(1 for p in pairs if (p.get("crash_mult") or 1) < 0.999),
            "h_first_enabled": any(p.get("h1") is not None for p in pairs),
        },
        "baseline_crash_only": None,
        "pairs": pairs,
        "pair_deltas": [],
    }


def _f(x: Any) -> float | None:
    if x is None or x == "":
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _boolish(x: Any) -> bool | None:
    if x is None or x == "":
        return None
    s = str(x).strip().lower()
    if s in ("1", "true", "yes", "y"):
        return True
    if s in ("0", "false", "no", "n"):
        return False
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-compare", type=Path, default=None)
    ap.add_argument("--from-run", type=Path, default=None)
    ap.add_argument("--out", type=Path, default=OUT)
    args = ap.parse_args()

    payload: dict[str, Any] | None = None
    if args.from_run:
        payload = from_run_dir(Path(args.from_run))
    if payload is None and args.from_compare:
        payload = from_compare(Path(args.from_compare))
    if payload is None:
        env = os.environ.get("OPERATOR_SIZING_RUN_DIR") or os.environ.get("CASH_RESIDUAL_RUN_DIR")
        if env:
            payload = from_run_dir(Path(env))
    if payload is None:
        sib = (
            REPO.parent.parent
            / "quant"
            / "Diamond-Creek-Quant"
            / "data"
            / "runs"
            / "_latest"
        )
        if sib.is_dir():
            payload = from_run_dir(sib)
    if payload is None and DEFAULT_COMPARE.is_file():
        payload = from_compare(DEFAULT_COMPARE)
    if payload is None:
        raise SystemExit("No compare harness or GTP run dir found to build cash_residual_book_latest.json")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {args.out} pairs={len(payload.get('pairs') or [])} source={payload.get('source_kind')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Audit B4 plan ledgers and nested inception_research paths for wipe/cliff anomalies.

Example:
  python scripts/audit_b4_inception_paths.py
  python scripts/audit_b4_inception_paths.py --fail-on-inception-fail
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PAIRS_DIR = REPO / "data" / "bucket4_pairs"
BOOK = REPO / "data" / "bucket4_backtest.json"
STANDALONE_DIRS = {
    "cash_residual_path": REPO / "data" / "bucket4_cash_residual_path",
    "inception_research": REPO / "data" / "bucket4_inception_research",
    "inception_research_stable": REPO / "data" / "bucket4_inception_research_stable",
}
NEST_KEYS = tuple(STANDALONE_DIRS.keys())


def _standalone_exists(etf: str, nest_key: str) -> bool:
    path = STANDALONE_DIRS[nest_key] / f"{etf}.json"
    if not path.is_file():
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    daily = payload.get("daily") if isinstance(payload, dict) else None
    return isinstance(daily, dict) and bool(daily.get("dates"))


def _book_etfs() -> list[str]:
    """Audit universe: book pairs ∪ screener B4 ∪ shards with nested inception."""
    etfs: set[str] = set()
    if BOOK.is_file():
        payload = json.loads(BOOK.read_text(encoding="utf-8"))
        for p in payload.get("pairs") or []:
            if p.get("etf"):
                etfs.add(str(p.get("etf")).upper())
    dash = REPO / "data" / "dashboard_data.json"
    if dash.is_file():
        try:
            payload = json.loads(dash.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        for row in payload.get("records") or payload.get("rows") or []:
            if str(row.get("screener_bucket") or "").lower() == "bucket_4" and row.get("symbol"):
                etfs.add(str(row.get("symbol")).upper())
    if PAIRS_DIR.is_dir():
        for path in PAIRS_DIR.glob("*.json"):
            try:
                shard = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if shard.get("inception_research") or shard.get("in_production_book"):
                etfs.add(path.stem.upper())
    if not etfs:
        return sorted(p.stem.upper() for p in PAIRS_DIR.glob("*.json"))
    return sorted(etfs)


def audit_daily(daily: dict) -> dict:
    if not daily or not daily.get("dates"):
        return {"fail": True, "warn": False, "error": "no_daily", "issues": ["no_daily"]}
    dates = daily["dates"]
    eq = daily.get("equity") or daily.get("equity_dollars") or []
    ret = daily.get("ret") or []
    gross = daily.get("etf_gross") or daily.get("etf_usd") or []
    reasons = daily.get("rebalance_reason") or []
    flags = daily.get("rebalance") or []
    issues: list[str] = []
    min_eq = None
    max_abs_ret = 0.0
    max_abs_ret_d = None
    neg_n = 0
    neg_first = None
    zero_gross_with_reb = 0
    for i, d in enumerate(dates):
        e = eq[i] if i < len(eq) else None
        r = ret[i] if i < len(ret) else None
        g = gross[i] if i < len(gross) else None
        try:
            e = float(e) if e is not None else None
        except (TypeError, ValueError):
            e = None
        try:
            r = float(r) if r is not None else None
        except (TypeError, ValueError):
            r = None
        try:
            g = float(g) if g is not None else None
        except (TypeError, ValueError):
            g = None
        if e is not None and math.isfinite(e):
            min_eq = e if min_eq is None else min(min_eq, e)
            if e <= 0:
                neg_n += 1
                if neg_first is None:
                    neg_first = d
        if r is not None and math.isfinite(r) and abs(r) > max_abs_ret:
            max_abs_ret = abs(r)
            max_abs_ret_d = d
        reb = bool(flags[i]) if i < len(flags) else False
        rsn = str(reasons[i]) if i < len(reasons) and reasons[i] else ""
        if reb and (g is None or abs(g) < 1e-9) and rsn not in ("hard_exit", "equity_wipeout"):
            zero_gross_with_reb += 1
    flat_tail = 0
    for i in range(len(ret) - 1, -1, -1):
        r = ret[i] if i < len(ret) else None
        try:
            r = float(r) if r is not None else None
        except (TypeError, ValueError):
            r = None
        if r == 0.0:
            flat_tail += 1
        else:
            break
    if neg_n:
        issues.append(f"equity<=0 x{neg_n} first={neg_first}")
    if max_abs_ret > 1.0:
        issues.append(f"|ret|>100% {max_abs_ret_d} {max_abs_ret:.2f}")
    elif max_abs_ret > 0.5:
        issues.append(f"|ret|>50% {max_abs_ret_d} {max_abs_ret:.2f}")
    if flat_tail >= 20:
        issues.append(f"flat_tail {flat_tail}d")
    if zero_gross_with_reb >= 3:
        issues.append(f"ghost_reb {zero_gross_with_reb}")
    fail = any(
        ("equity<=0" in x) or ("flat_tail" in x) or ("ghost_reb" in x) or (">100%" in x)
        for x in issues
    )
    warn = (not fail) and bool(issues)
    return {
        "n_days": len(dates),
        "start": dates[0],
        "end": dates[-1],
        "min_eq": None if min_eq is None else round(min_eq, 6),
        "max_abs_ret": round(max_abs_ret, 6),
        "max_abs_ret_d": max_abs_ret_d,
        "issues": issues,
        "fail": fail,
        "warn": warn,
        "ok": not issues,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=REPO / "data" / "_b4_inception_audit.json")
    ap.add_argument(
        "--fail-on-inception-fail",
        action="store_true",
        help="Exit 1 if any nested inception path fails hard gates",
    )
    ap.add_argument(
        "--fail-on-plan-fail",
        action="store_true",
        help="Exit 1 if any production plan ledger fails hard gates",
    )
    ap.add_argument(
        "--fail-on-missing-nests",
        action="store_true",
        help=(
            "Exit 1 when a production-book (or standalone-backed) pair is missing "
            "cash_residual_path / inception_research / inception_research_stable nests"
        ),
    )
    args = ap.parse_args(argv)

    rows = []
    for etf in _book_etfs():
        path = PAIRS_DIR / f"{etf}.json"
        if not path.is_file():
            rows.append({"etf": etf, "error": "missing_shard"})
            continue
        shard = json.loads(path.read_text(encoding="utf-8"))
        plan = audit_daily(shard.get("daily") or {})
        ir = shard.get("inception_research") or {}
        irs = shard.get("inception_research_stable") or {}
        cr = shard.get("cash_residual_path") or {}
        # Require full nest set when the pair is in the production book and standalone
        # research exists; otherwise flag only keys that have a standalone orphan.
        require_nests = bool(shard.get("in_production_book")) and any(
            _standalone_exists(etf, k) for k in NEST_KEYS
        )
        if require_nests:
            missing_nests = [
                key
                for key in NEST_KEYS
                if not (
                    isinstance(shard.get(key), dict)
                    and isinstance((shard.get(key) or {}).get("daily"), dict)
                    and (shard.get(key) or {}).get("daily", {}).get("dates")
                )
            ]
        else:
            missing_nests = [
                key
                for key in NEST_KEYS
                if _standalone_exists(etf, key)
                and not (
                    isinstance(shard.get(key), dict)
                    and isinstance((shard.get(key) or {}).get("daily"), dict)
                    and (shard.get(key) or {}).get("daily", {}).get("dates")
                )
            ]
        if ir:
            inception = audit_daily(ir.get("daily") or {})
            inception["final_equity"] = (ir.get("summary") or {}).get("final_equity")
            inception["cagr"] = (ir.get("summary") or {}).get("cagr")
            inception["stable_nested"] = bool(irs)
            if not irs:
                inception.setdefault("issues", []).append("missing_stable_nest")
                inception["warn"] = True
        else:
            inception = {"fail": True, "warn": False, "error": "no_inception", "issues": ["no_inception"]}
        rows.append(
            {
                "etf": etf,
                "underlying": shard.get("underlying"),
                "plan": plan,
                "inception": inception,
                "stable_nested": bool(irs),
                "cash_residual_nested": bool(
                    isinstance(cr, dict)
                    and isinstance(cr.get("daily"), dict)
                    and cr.get("daily", {}).get("dates")
                ),
                "missing_nests": missing_nests,
                "in_production_book": bool(shard.get("in_production_book")),
            }
        )
        p_st = "FAIL" if plan.get("fail") else ("WARN" if plan.get("warn") else "OK")
        if inception.get("error"):
            i_st = "MISSING"
        elif inception.get("fail"):
            i_st = "FAIL"
        elif inception.get("warn"):
            i_st = "WARN"
        else:
            i_st = "OK"
        nest_note = f" missing_nests={missing_nests}" if missing_nests else ""
        print(
            f"{etf}: plan={p_st} inception={i_st} "
            f"plan_issues={plan.get('issues') or ['ok']} "
            f"inc_issues={inception.get('issues') or inception.get('error') or ['ok']}"
            f"{nest_note}"
        )

    report = {
        "schema": "b4_inception_path_audit.v2",
        "n_pairs": len(rows),
        "pairs": rows,
        "inception_fail": [r["etf"] for r in rows if (r.get("inception") or {}).get("fail")],
        "inception_warn": [r["etf"] for r in rows if (r.get("inception") or {}).get("warn")],
        "plan_fail": [r["etf"] for r in rows if (r.get("plan") or {}).get("fail")],
        "plan_warn": [r["etf"] for r in rows if (r.get("plan") or {}).get("warn")],
        "inception_missing": [
            r["etf"] for r in rows if (r.get("inception") or {}).get("error") == "no_inception"
        ],
        "stable_missing": [
            r["etf"]
            for r in rows
            if (r.get("inception") or {}).get("error") != "no_inception"
            and not r.get("stable_nested")
        ],
        "missing_nests": [
            {"etf": r["etf"], "missing": r.get("missing_nests")}
            for r in rows
            if r.get("missing_nests")
        ],
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({
        "ok": True,
        "report": str(args.out),
        "inception_fail": report["inception_fail"],
        "plan_fail": report["plan_fail"],
        "inception_missing": report["inception_missing"],
        "missing_nests": report["missing_nests"],
    }))
    if args.fail_on_inception_fail and report["inception_fail"]:
        return 1
    if args.fail_on_plan_fail and report["plan_fail"]:
        return 1
    if args.fail_on_missing_nests and report["missing_nests"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Freshness invariant checks for options, VRP, metrics, and LETF flow artifacts."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA = REPO_ROOT / "data"
sys.path.insert(0, str(REPO_ROOT / "scripts"))


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _age_minutes(ts: str | None, now: datetime) -> int | None:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return max(0, int((now - dt).total_seconds() // 60))


def _check_options(cache: dict, *, max_underlying_hours: float) -> tuple[dict, list[str]]:
    violations: list[str] = []
    now = datetime.now(UTC)
    symbols = cache.get("symbols") if isinstance(cache.get("symbols"), dict) else {}
    und_refreshed = cache.get("yieldboost_underlyings_refreshed") or []
    sleeves_only = bool(cache.get("yieldboost_sleeves_only"))
    if sleeves_only and cache.get("yieldboost_targeted"):
        violations.append("options_cache.yieldboost_sleeves_only=true (underlyings not refreshed)")

    und_ages: list[tuple[str, int]] = []
    for sym, payload in symbols.items():
        if not isinstance(payload, dict):
            continue
        ts = payload.get("updated_at")
        age = _age_minutes(str(ts) if ts else None, now)
        if age is not None:
            und_ages.append((str(sym).upper(), age))
    und_ages.sort(key=lambda x: x[1], reverse=True)

    worst_sym, worst_age = und_ages[0] if und_ages else (None, None)
    max_underlying_min = int(max_underlying_hours * 60)
    if worst_age is not None and worst_age > max_underlying_min:
        violations.append(
            f"oldest symbol cache {worst_sym} is {worst_age // 60}h old (limit {max_underlying_hours:.0f}h)"
        )

    return {
        "build_time": cache.get("build_time"),
        "yieldboost_targeted": cache.get("yieldboost_targeted"),
        "yieldboost_sleeves_only": sleeves_only,
        "yieldboost_underlyings_refreshed_count": len(und_refreshed),
        "yieldboost_underlyings_skipped_fresh_count": len(cache.get("yieldboost_underlyings_skipped_fresh") or []),
        "oldest_symbol": worst_sym,
        "oldest_symbol_age_minutes": worst_age,
    }, violations


def _check_vrp(health: dict, *, max_underlying_hours: float) -> tuple[dict, list[str]]:
    violations: list[str] = []
    age = health.get("worst_underlying_options_age_minutes")
    sym = health.get("worst_underlying_symbol")
    max_min = int(max_underlying_hours * 60)
    if isinstance(age, (int, float)) and age > max_min:
        violations.append(
            f"vrp worst underlying {sym or '?'} quote {int(age) // 60}h old (limit {max_underlying_hours:.0f}h)"
        )
    return {
        "worst_underlying_symbol": sym,
        "worst_underlying_options_age_minutes": age,
        "underlying_options_as_of_min": health.get("underlying_options_as_of_min"),
        "iv_coverage_front_pct": health.get("iv_coverage_front_pct"),
    }, violations


def _check_flow(flow: dict, *, max_stale_pct: float) -> tuple[dict, list[str]]:
    violations: list[str] = []
    by_u = flow.get("by_underlying") if isinstance(flow.get("by_underlying"), dict) else {}
    total = len(by_u)
    stale = sum(1 for v in by_u.values() if isinstance(v, dict) and v.get("is_latest_global") is False)
    pct = (100.0 * stale / total) if total else 0.0
    if pct > max_stale_pct:
        violations.append(f"flow {stale}/{total} underlyings stale ({pct:.1f}% > {max_stale_pct:.0f}%)")
    top_stale = sorted(
        (
            (k, v.get("date"), abs(float(v.get("net_moc_dollars") or 0)))
            for k, v in by_u.items()
            if isinstance(v, dict) and v.get("is_latest_global") is False
        ),
        key=lambda x: x[2],
        reverse=True,
    )[:5]
    return {
        "latest_date": flow.get("latest_date"),
        "build_time": flow.get("build_time"),
        "underlyings_total": total,
        "underlyings_stale": stale,
        "underlyings_stale_pct": round(pct, 2),
        "top_stale_by_abs_net_moc": [
            {"underlying": u, "date": d, "abs_net_moc_dollars": n} for u, d, n in top_stale
        ],
    }, violations


def _check_metrics(health: dict) -> tuple[dict, list[str]]:
    violations: list[str] = []
    latest_stale = int(health.get("latest_stale_ok") or 0)
    latest_ok = int(health.get("latest_ok") or 0)
    total = latest_ok + latest_stale + int(health.get("latest_partial") or 0) + int(health.get("latest_missing") or 0)
    return {
        "latest_date": health.get("latest_date"),
        "latest_ok": latest_ok,
        "latest_stale_ok": latest_stale,
        "latest_missing": health.get("latest_missing"),
    }, violations


def build_summary(
    *,
    max_underlying_hours: float = 48.0,
    max_flow_stale_pct: float = 25.0,
) -> dict:
    now = datetime.now(UTC)
    options = _load_json(DATA / "options_cache.json")
    vrp = _load_json(DATA / "vrp_health.json")
    flow = _load_json(DATA / "letf_rebalance_flows_latest.json")
    metrics = _load_json(DATA / "etf_metrics_health.json")

    opt_block, opt_v = _check_options(options, max_underlying_hours=max_underlying_hours)
    vrp_block, vrp_v = _check_vrp(vrp, max_underlying_hours=max_underlying_hours)
    flow_block, flow_v = _check_flow(flow, max_stale_pct=max_flow_stale_pct)
    met_block, met_v = _check_metrics(metrics)
    violations = opt_v + vrp_v + flow_v + met_v

    return {
        "build_time": now.isoformat().replace("+00:00", "Z"),
        "thresholds": {
            "max_underlying_hours": max_underlying_hours,
            "max_flow_stale_pct": max_flow_stale_pct,
        },
        "options": opt_block,
        "vrp": vrp_block,
        "flow": flow_block,
        "metrics": met_block,
        "violations": violations,
        "ok": not violations,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Check freshness invariants across data artifacts.")
    parser.add_argument("--max-underlying-hours", type=float, default=48.0)
    parser.add_argument("--max-flow-stale-pct", type=float, default=25.0)
    parser.add_argument("--write", type=Path, default=DATA / "freshness_summary.json")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    summary = build_summary(
        max_underlying_hours=args.max_underlying_hours,
        max_flow_stale_pct=args.max_flow_stale_pct,
    )
    if args.write:
        args.write.parent.mkdir(parents=True, exist_ok=True)
        args.write.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if not args.quiet:
        print(json.dumps(summary, indent=2))
    if summary["violations"]:
        for v in summary["violations"]:
            print(f"[FAIL] {v}", file=sys.stderr)
        return 1
    if not args.quiet:
        print("[OK] freshness invariants passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

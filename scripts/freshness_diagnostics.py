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


def _load_monitored_options_symbols() -> set[str]:
    try:
        from build_data import load_monitored_options_symbols

        return load_monitored_options_symbols()
    except Exception:
        return set()


def _freshness_enforced_options_symbols(cache: dict) -> set[str]:
    """Symbols whose quote age can fail diagnostics (last refresh batch + YB underlyings)."""
    enforced: set[str] = set()
    for key in ("refresh_symbols",):
        val = cache.get(key)
        if isinstance(val, list):
            enforced.update(str(s).upper() for s in val if s)
    und_refreshed = cache.get("yieldboost_underlyings_refreshed") or []
    enforced.update(str(u).upper() for u in und_refreshed if u)
    if not enforced:
        head = cache.get("yieldboost_refresh_order_head") or []
        enforced.update(str(s).upper() for s in head if s)
    return enforced


def _check_options(cache: dict, *, max_underlying_hours: float) -> tuple[dict, list[str]]:
    violations: list[str] = []
    now = datetime.now(UTC)
    symbols = cache.get("symbols") if isinstance(cache.get("symbols"), dict) else {}
    und_refreshed = cache.get("yieldboost_underlyings_refreshed") or []
    sleeves_only = bool(cache.get("yieldboost_sleeves_only"))
    max_underlying_min = int(max_underlying_hours * 60)
    if sleeves_only and cache.get("yieldboost_targeted"):
        violations.append("options_cache.yieldboost_sleeves_only=true (underlyings not refreshed)")

    prefetched = cache.get("yieldboost_underlyings_prefetched")
    if und_refreshed and prefetched is not None and int(prefetched) < min(3, len(und_refreshed)):
        violations.append(
            f"only {prefetched}/{len(und_refreshed)} YB underlyings prefetched this run"
        )

    monitored = _load_monitored_options_symbols()
    enforced = _freshness_enforced_options_symbols(cache)
    if not enforced and und_refreshed:
        enforced = {str(u).upper() for u in und_refreshed}

    und_ages: list[tuple[str, int]] = []
    enforced_ages: list[tuple[str, int]] = []
    for sym, payload in symbols.items():
        if not isinstance(payload, dict):
            continue
        ts = payload.get("updated_at")
        age = _age_minutes(str(ts) if ts else None, now)
        if age is not None:
            sym_u = str(sym).upper()
            und_ages.append((sym_u, age))
            if not enforced or sym_u in enforced:
                enforced_ages.append((sym_u, age))
    und_ages.sort(key=lambda x: x[1], reverse=True)
    enforced_ages.sort(key=lambda x: x[1], reverse=True)

    und_set = {str(u).upper() for u in und_refreshed}
    if und_set:
        yb_und_ages = [(s, a) for s, a in und_ages if s in und_set]
        if yb_und_ages:
            worst_und, worst_und_age = max(yb_und_ages, key=lambda x: x[1])
            if worst_und_age > max_underlying_min:
                violations.append(
                    f"YB underlying {worst_und} cache {worst_und_age // 60}h old "
                    f"(limit {max_underlying_hours:.0f}h)"
                )

    scope_ages = enforced_ages if enforced_ages else und_ages
    worst_sym, worst_age = scope_ages[0] if scope_ages else (None, None)
    if worst_age is not None and worst_age > max_underlying_min:
        scope_label = "enforced" if enforced else "symbol"
        violations.append(
            f"oldest {scope_label} cache {worst_sym} is {worst_age // 60}h old "
            f"(limit {max_underlying_hours:.0f}h)"
        )

    cached_monitored = {str(s).upper() for s in symbols if str(s).upper() in monitored} if monitored else set()
    missing_monitored = sorted(monitored - cached_monitored) if monitored else []
    stale_enforced = [
        s for s, a in enforced_ages if a > max_underlying_min
    ]

    return {
        "build_time": cache.get("build_time"),
        "yieldboost_targeted": cache.get("yieldboost_targeted"),
        "yieldboost_sleeves_only": sleeves_only,
        "yieldboost_underlyings_refreshed_count": len(und_refreshed),
        "yieldboost_underlyings_prefetched_count": prefetched,
        "yieldboost_underlyings_skipped_fresh_count": len(cache.get("yieldboost_underlyings_skipped_fresh") or []),
        "yieldboost_refresh_order_head": cache.get("yieldboost_refresh_order_head"),
        "monitored_symbols_count": len(monitored),
        "freshness_enforced_count": len(enforced),
        "cached_monitored_count": len(cached_monitored),
        "missing_monitored_symbols": missing_monitored[:20],
        "stale_enforced_symbols": stale_enforced[:20],
        "pruned_unmonitored_count": cache.get("pruned_unmonitored_count"),
        "oldest_symbol": worst_sym,
        "oldest_symbol_age_minutes": worst_age,
        "oldest_enforced_symbol": worst_sym if enforced_ages else None,
        "oldest_enforced_symbol_age_minutes": worst_age if enforced_ages else None,
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

    stale_summary = flow.get("flow_stale_summary") if isinstance(flow.get("flow_stale_summary"), dict) else {}
    by_reason = stale_summary.get("underlyings_stale_by_reason") or {}
    actionable = int(stale_summary.get("underlyings_actionable_stale") or 0)
    actionable_pct = (100.0 * actionable / total) if total else 0.0
    issuer_lag = int(by_reason.get("issuer_publish_lag") or 0)

    if actionable_pct > max_stale_pct:
        violations.append(
            f"flow {actionable}/{total} underlyings actionable-stale "
            f"({actionable_pct:.1f}% > {max_stale_pct:.0f}%)"
        )
    top_stale = sorted(
        (
            (k, v.get("date"), abs(float(v.get("net_moc_dollars") or 0)), v.get("aggregate_stale_reason"))
            for k, v in by_u.items()
            if isinstance(v, dict) and v.get("is_latest_global") is False
        ),
        key=lambda x: x[2],
        reverse=True,
    )[:5]
    quality = flow.get("flow_quality_on_latest_date") if isinstance(flow.get("flow_quality_on_latest_date"), dict) else {}
    return {
        "latest_date": flow.get("latest_date"),
        "build_time": flow.get("build_time"),
        "underlyings_total": total,
        "underlyings_stale": stale,
        "underlyings_stale_pct": round(pct, 2),
        "underlyings_issuer_publish_lag": issuer_lag,
        "underlyings_actionable_stale": actionable,
        "underlyings_actionable_stale_pct": round(actionable_pct, 2),
        "underlyings_stale_by_reason": by_reason,
        "fund_rows_on_latest_date": quality.get("fund_rows_total"),
        "fund_quality_counts": quality.get("quality_counts"),
        "stale_aum_by_prior_kind": quality.get("stale_aum_by_prior_kind"),
        "top_stale_by_abs_net_moc": [
            {"underlying": u, "date": d, "abs_net_moc_dollars": n, "reason": r}
            for u, d, n, r in top_stale
        ],
    }, violations


def _check_metrics(health: dict) -> tuple[dict, list[str]]:
    violations: list[str] = []
    latest_stale = int(health.get("latest_stale_ok") or 0)
    latest_ok = int(health.get("latest_ok") or 0)
    prem = health.get("prem_disc") if isinstance(health.get("prem_disc"), dict) else {}
    lock_pct = float(prem.get("lockstep_nav_close_pct") or 0.0)
    rex_div = int(prem.get("rex_nav_vs_implied_div_bps_gt5") or 0)
    rex_div_material = int(prem.get("rex_nav_vs_implied_div_bps_gt50") or 0)
    by_rex = (prem.get("by_provider") or {}).get("rex_shares") or {}
    rex_lock = int(by_rex.get("lockstep_count") or 0)
    rex_rows = int(by_rex.get("rows") or 0)
    if rex_rows >= 5 and rex_lock >= max(3, int(rex_rows * 0.8)):
        violations.append(
            f"metrics: rex_shares lockstep nav≈close on {rex_lock}/{rex_rows} rows "
            "(check issuer NAV vs Closing Price ingest)",
        )
    if lock_pct >= 35.0 and int(prem.get("rows_with_nav_and_close") or 0) >= 20:
        violations.append(
            f"metrics: {lock_pct:.1f}% of latest rows have nav≈close (>{lock_pct:.0f}% lockstep)",
        )
    if rex_div_material >= 3:
        violations.append(
            f"metrics: {rex_div_material} rex row(s) with published NAV vs AUM/shares divergence >50bp "
            f"({rex_div} >5bp)",
        )
    return {
        "latest_date": health.get("latest_date"),
        "latest_ok": latest_ok,
        "latest_stale_ok": latest_stale,
        "latest_missing": health.get("latest_missing"),
        "latest_stale_by_kind": health.get("latest_stale_by_kind") or {},
        "flow_blockers_prior_stale": health.get("flow_blockers_prior_stale"),
        "prem_disc": prem,
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
    parser.add_argument(
        "--fail-on-violations",
        action="store_true",
        help="Alias for default exit code 1 when violations exist (explicit CI gate).",
    )
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

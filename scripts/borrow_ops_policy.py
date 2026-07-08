#!/usr/bin/env python3
"""Hybrid borrow ops policy: v2 spike tiers + boosting drift (dashboard-only)."""
from __future__ import annotations

import math
from typing import Any

BORROW_OPS_POLICY = "v2_spike_boosting_drift"

TIER_RANK: dict[str, int] = {
    "": 0,
    "none": 0,
    "low": 0,
    "watch": 1,
    "elevated": 2,
    "high": 3,
}

BORROW_OPS_FIELD_NAMES = (
    "borrow_ops_policy",
    "borrow_ops_spike_block",
    "borrow_ops_spike_watch",
    "borrow_ops_model_disagree",
    "borrow_ops_drift_tightening",
    "borrow_ops_spike_tier_v2",
    "borrow_ops_spike_tier_boosting",
    "borrow_ops_effective_net_edge_p50",
)


def _finite(v: object) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _norm_tier(v: object) -> str:
    return str(v or "").strip().lower()


def compute_borrow_ops_fields(rec: dict[str, Any]) -> dict[str, Any]:
    """Dashboard-only ops flags; does not mutate ls-algo net edge."""
    tier_v2 = _norm_tier(rec.get("borrow_spike_alert_tier") or rec.get("borrow_spike_risk_band"))
    tier_boost = _norm_tier(rec.get("borrow_spike_alert_tier_boosting"))
    delta = _finite(rec.get("borrow_forecast_delta_5d_p50"))
    eligible = rec.get("borrow_spike_scoring_eligible") is not False
    screener_spike = rec.get("exclude_borrow_spike") is True

    spike_block = eligible and (tier_v2 in ("elevated", "high") or screener_spike)
    spike_watch = eligible and tier_v2 == "watch" and not spike_block
    disagree = eligible and TIER_RANK.get(tier_boost, 0) > TIER_RANK.get(tier_v2, 0)
    drift_tight = delta is not None and delta > 0.02
    eff = effective_net_edge_for_sizing(rec)

    return {
        "borrow_ops_policy": BORROW_OPS_POLICY,
        "borrow_ops_spike_block": bool(spike_block),
        "borrow_ops_spike_watch": bool(spike_watch),
        "borrow_ops_model_disagree": bool(disagree),
        "borrow_ops_drift_tightening": bool(drift_tight),
        "borrow_ops_spike_tier_v2": tier_v2 or None,
        "borrow_ops_spike_tier_boosting": tier_boost or None,
        "borrow_ops_effective_net_edge_p50": round(eff, 6) if eff is not None else None,
    }


def effective_net_edge_for_sizing(rec: dict[str, Any]) -> float | None:
    """min(p50, stress_p50) when v2 tier is elevated/high; else headline p50."""
    p50 = _finite(rec.get("net_edge_p50_annual"))
    if p50 is None:
        return None
    tier = _norm_tier(rec.get("borrow_spike_alert_tier") or rec.get("borrow_spike_risk_band"))
    stress = _finite(rec.get("net_edge_stress_p50_annual"))
    if tier in ("elevated", "high") and stress is not None:
        return min(p50, stress)
    return p50


def merge_borrow_ops_into_short_edge_map(
    short_edge_map: dict[str, dict[str, Any]],
    dashboard_records: list[dict[str, Any]] | None,
) -> dict[str, dict[str, Any]]:
    """Overlay live borrow ops from dashboard rows onto screener short-edge map."""
    if not short_edge_map or not dashboard_records:
        return short_edge_map
    by_sym: dict[str, dict[str, Any]] = {}
    for row in dashboard_records:
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol") or "").strip().upper()
        if sym:
            by_sym[sym] = row
    for yb, rec in list(short_edge_map.items()):
        if yb == "__asof__":
            continue
        dash = by_sym.get(str(yb).upper())
        if not dash:
            continue
        ops = compute_borrow_ops_fields(dash)
        rec.update(ops)
        for key in (
            "borrow_spike_alert_tier",
            "borrow_spike_alert_tier_boosting",
            "borrow_spike_scoring_eligible",
            "net_edge_stress_p50_annual",
            "borrow_forecast_delta_5d_p50",
            "borrow_forecast_method",
        ):
            if key in dash:
                rec[key] = dash.get(key)
    return short_edge_map

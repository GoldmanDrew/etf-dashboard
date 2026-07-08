"""Attach borrow-spike and MOC flow signals to dashboard rows."""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

STRESS_TIERS = frozenset({"elevated", "high"})


def _norm_sym(s: object) -> str:
    return str(s or "").strip().upper().replace(".", "-")


def _json_finite(v: Any) -> Any:
    """Drop NaN/inf numeric values so dashboard JSON stays browser-parseable."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return v
    return f if math.isfinite(f) else None


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def load_flow_signal_maps(
    data_dir: Path,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Return (intraday_by_fund, eod_by_ticker) keyed by upper symbol."""
    intraday: dict[str, dict[str, Any]] = {}
    eod: dict[str, dict[str, Any]] = {}

    intra_payload = _load_json(data_dir / "letf_rebalance_flows_intraday_latest.json")
    for sym, row in (intra_payload.get("by_fund") or {}).items():
        if isinstance(row, dict):
            intraday[_norm_sym(sym)] = row

    eod_payload = _load_json(data_dir / "letf_rebalance_flows_latest.json")
    rows = eod_payload.get("rows") or []
    if not rows and isinstance(eod_payload.get("daily"), dict):
        rows = eod_payload["daily"].get("rows") or []
    latest_date = str(eod_payload.get("latest_date") or "")
    for row in rows:
        if not isinstance(row, dict):
            continue
        if latest_date and str(row.get("date") or "") != latest_date:
            continue
        sym = _norm_sym(row.get("ticker"))
        if sym:
            eod[sym] = row
    if not eod and rows:
        # Fallback: most recent row per ticker
        by_sym: dict[str, dict[str, Any]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            sym = _norm_sym(row.get("ticker"))
            if not sym:
                continue
            prev = by_sym.get(sym)
            if prev is None or str(row.get("date") or "") >= str(prev.get("date") or ""):
                by_sym[sym] = row
        eod = by_sym

    return intraday, eod


def compute_net_edge_stress_fields(rec: dict[str, Any]) -> dict[str, Any]:
    """Stress-case net edge for elevated/high tiers (display only; ls-algo unchanged)."""
    tier = str(rec.get("borrow_spike_alert_tier") or rec.get("borrow_spike_risk_band") or "").lower()
    if tier not in STRESS_TIERS:
        return {}
    net_p50 = rec.get("net_edge_p50_annual")
    borrow_post = rec.get("borrow_for_net_annual")
    if borrow_post is None:
        borrow_post = rec.get("borrow_fee_annual") or rec.get("borrow_current")
    forecast = rec.get("borrow_forecast_5d_p50")
    if not all(
        isinstance(x, (int, float)) and math.isfinite(float(x))
        for x in (net_p50, borrow_post)
    ):
        return {}
    borrow_post_f = float(borrow_post)
    net_f = float(net_p50)
    stress_borrow = borrow_post_f
    if isinstance(forecast, (int, float)) and math.isfinite(float(forecast)):
        stress_borrow = max(borrow_post_f, float(forecast))
    if stress_borrow <= borrow_post_f + 1e-9:
        return {}
    return {
        "borrow_stress_borrow_annual": round(stress_borrow, 6),
        "net_edge_stress_p50_annual": round(net_f - (stress_borrow - borrow_post_f), 6),
        "net_edge_stress_basis": "max_posterior_forecast_borrow",
    }


def enrich_records_with_operational_signals(
    records: list[dict[str, Any]],
    *,
    borrow_spike_risk: dict[str, Any] | None,
    data_dir: Path,
) -> None:
    """Mutate records in place with borrow spike + MOC flow fields."""
    spike_syms = (borrow_spike_risk or {}).get("symbols") or {}
    intra_by_fund, eod_by_ticker = load_flow_signal_maps(data_dir)
    forecast_payload = _load_json(data_dir / "borrow_forecast_latest.json")
    forecast_by = (forecast_payload.get("by_symbol") or {}) if forecast_payload else {}
    ml_scores = _load_json(data_dir / "borrow_ml_scores_latest.json")
    registry = _load_json(data_dir / "borrow_model_registry.json")

    for rec in records:
        sym = _norm_sym(rec.get("symbol"))
        if not sym:
            continue

        spike = spike_syms.get(sym) if isinstance(spike_syms, dict) else None
        if isinstance(spike, dict):
            rec["borrow_spike_p_5d"] = _json_finite(spike.get("p_spike_5d"))
            rec["borrow_spike_p_5d_l2_calibrated"] = _json_finite(spike.get("p_spike_5d_l2_calibrated"))
            rec["borrow_spike_p_5d_l2_boosting"] = _json_finite(spike.get("p_spike_5d_l2_boosting"))
            rec["borrow_spike_p_5d_l2_boosting_calibrated"] = _json_finite(
                spike.get("p_spike_5d_l2_boosting_calibrated")
            )
            rec["borrow_spike_alert_tier"] = spike.get("alert_tier")
            rec["borrow_spike_alert_tier_boosting"] = spike.get("alert_tier_boosting")
            rec["borrow_spike_risk_band"] = spike.get("risk_band") or spike.get("alert_tier")
            rec["borrow_spike_quality_band"] = spike.get("quality_band")
            rec["borrow_spike_scoring_eligible"] = spike.get("scoring_eligible")
            rec["borrow_spike_supply_data_grade"] = spike.get("supply_data_grade")

        fc = forecast_by.get(sym) if isinstance(forecast_by, dict) else None
        if isinstance(fc, dict):
            rec["borrow_forecast_delta_5d_p50"] = _json_finite(fc.get("delta_borrow_5d_p50"))
            rec["borrow_forecast_5d_p50"] = _json_finite(fc.get("borrow_forecast_5d_p50"))
            rec["borrow_forecast_delta_5d_p25"] = _json_finite(fc.get("delta_borrow_5d_p25"))
            rec["borrow_forecast_delta_5d_p75"] = _json_finite(fc.get("delta_borrow_5d_p75"))
            rec["borrow_forecast_method"] = fc.get("method") or forecast_payload.get("method")

        rec.update(compute_net_edge_stress_fields(rec))

        if registry:
            rec["borrow_model_drift_winner"] = (registry.get("drift") or {}).get("winner")
            rec["borrow_model_spike_winner"] = (registry.get("spike_l2") or {}).get("winner")

        if rec.get("bucket") != "bucket_1_high_beta":
            continue

        intra = intra_by_fund.get(sym) or {}
        eod = eod_by_ticker.get(sym) or {}
        rec["moc_flow_est_rebalance_dollars"] = intra.get("estimated_close_rebalance_dollars")
        rec["moc_flow_est_rebalance_pct_adv"] = intra.get("estimated_close_rebalance_pct_adv_20d")
        rec["moc_flow_remaining_pct_adv"] = intra.get("remaining_close_rebalance_pct_adv_20d")
        rec["moc_flow_remaining_pct_float"] = intra.get("remaining_close_rebalance_pct_tradable_float")
        rec["moc_flow_eod_rebalance_pct_adv"] = eod.get("rebalance_pct_adv_20d")
        rec["moc_flow_eod_rebalance_dollars"] = eod.get("rebalance_signed_dollars")
        rec["moc_flow_as_of"] = intra.get("as_of") or eod.get("date")

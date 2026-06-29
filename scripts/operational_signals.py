"""Attach borrow-spike and MOC flow signals to dashboard rows."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _norm_sym(s: object) -> str:
    return str(s or "").strip().upper().replace(".", "-")


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


def enrich_records_with_operational_signals(
    records: list[dict[str, Any]],
    *,
    borrow_spike_risk: dict[str, Any] | None,
    data_dir: Path,
) -> None:
    """Mutate records in place with borrow spike + MOC flow fields."""
    spike_syms = (borrow_spike_risk or {}).get("symbols") or {}
    intra_by_fund, eod_by_ticker = load_flow_signal_maps(data_dir)

    for rec in records:
        sym = _norm_sym(rec.get("symbol"))
        if not sym:
            continue

        spike = spike_syms.get(sym) if isinstance(spike_syms, dict) else None
        if isinstance(spike, dict):
            rec["borrow_spike_p_5d"] = spike.get("p_spike_5d")
            rec["borrow_spike_risk_band"] = spike.get("risk_band")
            rec["borrow_spike_quality_band"] = spike.get("quality_band")
            rec["borrow_spike_scoring_eligible"] = spike.get("scoring_eligible")

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

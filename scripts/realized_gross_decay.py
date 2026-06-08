"""Split-aware realized gross decay from etf_metrics_daily (ls-algo port target).

``daily_screener.py`` in ls-algo should mirror ``compute_gross_decay_annual`` when
ingesting prices with adj-basis-switch forward splits (e.g. APLX 3-for-1).
"""
from __future__ import annotations

import datetime as dt
import json
import math
from pathlib import Path
from typing import Any

import pandas as pd

from price_basis import build_tr_series_from_metrics, parse_split_events_from_corp

TRADING_DAYS = 252
DEFAULT_MIN_OBS = 40


def compute_gross_decay_annual(
    rows: list[dict[str, Any]],
    beta: float,
    split_events: list[tuple[dt.date, float]] | None = None,
    *,
    min_obs: int = DEFAULT_MIN_OBS,
) -> dict[str, Any] | None:
    """Mean daily log-drag annualized: beta * log(R_u) - log(R_etf) on split-aware TR."""
    if not math.isfinite(beta):
        return None
    tr = build_tr_series_from_metrics(rows, split_events or [])
    if len(tr) < min_obs + 1:
        return None
    drags: list[float] = []
    for i in range(1, len(tr)):
        u0, u1 = tr[i - 1]["tr_und_px"], tr[i]["tr_und_px"]
        e0, e1 = tr[i - 1]["tr_etf_px"], tr[i]["tr_etf_px"]
        if u0 > 0 and u1 > 0 and e0 > 0 and e1 > 0:
            drags.append(beta * math.log(u1 / u0) - math.log(e1 / e0))
    if len(drags) < min_obs:
        return None
    mean_drag = sum(drags) / len(drags)
    return {
        "gross_decay_annual": round(mean_drag * TRADING_DAYS, 6),
        "n_obs": len(drags),
        "start_date": tr[1]["date"],
        "end_date": tr[-1]["date"],
    }


def load_gross_decay_from_metrics(
    metrics_path: Path,
    universe_symbols: set[str],
    *,
    corp_actions_path: Path | None = None,
    beta_by_symbol: dict[str, float] | None = None,
    min_obs: int = DEFAULT_MIN_OBS,
) -> dict[str, dict[str, Any]]:
    """Build per-symbol realized gross decay from joint ETF metrics rows."""
    if not metrics_path.exists():
        return {}
    corp_path = corp_actions_path or Path(__file__).resolve().parent.parent / "data" / "corporate_actions.json"
    corp_payload: dict = {"events": []}
    if corp_path.exists():
        corp_payload = json.loads(corp_path.read_text(encoding="utf-8"))

    df = pd.read_parquet(metrics_path)
    df["date"] = df["date"].astype(str).str[:10]
    df["ticker"] = df["ticker"].astype(str).str.upper()
    out: dict[str, dict[str, Any]] = {}

    for sym in sorted(universe_symbols):
        sym_u = str(sym or "").strip().upper()
        if not sym_u:
            continue
        sub = df[df["ticker"] == sym_u].sort_values("date")
        if sub.empty:
            continue
        rows = sub.to_dict("records")
        joint = [
            r
            for r in rows
            if pd.notna(r.get("close_price") or r.get("nav"))
            and pd.notna(r.get("underlying_adj_close"))
        ]
        if len(joint) < min_obs + 1:
            continue
        beta = (beta_by_symbol or {}).get(sym_u)
        if beta is None:
            try:
                beta = float(sub.iloc[-1].get("delta") or sub.iloc[-1].get("Delta") or float("nan"))
            except (TypeError, ValueError):
                beta = float("nan")
        if not math.isfinite(float(beta)):
            continue
        events = parse_split_events_from_corp(corp_payload, sym_u)
        result = compute_gross_decay_annual(joint, float(beta), events, min_obs=min_obs)
        if result:
            result["source"] = "etf_metrics_daily"
            out[sym_u] = result
    return out

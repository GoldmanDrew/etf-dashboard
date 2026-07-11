"""Point-in-time borrow / membership helpers for Bucket 4 walk-forward."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
DEFAULT_BORROW_HISTORY = REPO / "data" / "borrow_history.json"


def _norm(sym: object) -> str:
    return str(sym).strip().upper().replace(".", "-")


def load_borrow_history(path: str | Path | None = None) -> dict[str, pd.Series]:
    """Return {ETF: Series(date -> borrow_current)} forward-filled for lookups."""
    p = Path(path) if path else DEFAULT_BORROW_HISTORY
    if not p.is_file():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    symbols = raw.get("symbols") if isinstance(raw, dict) else None
    if not isinstance(symbols, dict):
        return {}
    out: dict[str, pd.Series] = {}
    for sym, rows in symbols.items():
        if not isinstance(rows, list):
            continue
        dates: list[pd.Timestamp] = []
        vals: list[float] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            d = row.get("date")
            b = row.get("borrow_current")
            if d is None:
                continue
            try:
                bv = float(b) if b is not None else float("nan")
            except (TypeError, ValueError):
                bv = float("nan")
            dates.append(pd.Timestamp(d))
            vals.append(bv)
        if not dates:
            continue
        s = pd.Series(vals, index=pd.DatetimeIndex(dates)).sort_index()
        # Forward-fill last known borrow; leave leading NaNs.
        s = s.ffill()
        out[_norm(sym)] = s
    return out


def borrow_asof(
    history: Mapping[str, pd.Series],
    symbol: str,
    as_of: str | pd.Timestamp,
    *,
    fallback: float | None = None,
) -> float | None:
    """Last known borrow on or before as_of; fallback if missing."""
    ser = history.get(_norm(symbol))
    if ser is None or ser.empty:
        return fallback
    ts = pd.Timestamp(as_of)
    sub = ser.loc[ser.index <= ts].dropna()
    if sub.empty:
        return fallback
    v = float(sub.iloc[-1])
    if not np.isfinite(v) or v < 0:
        return fallback
    return v


def borrow_series_for_calendar(
    history: Mapping[str, pd.Series],
    symbol: str,
    calendar: pd.DatetimeIndex,
    *,
    fallback: float = 0.0,
) -> pd.Series:
    """Daily borrow path aligned to calendar (ffill from history, else constant)."""
    ser = history.get(_norm(symbol))
    if ser is None or ser.empty:
        return pd.Series(float(fallback), index=calendar)
    aligned = ser.reindex(ser.index.union(calendar)).sort_index().ffill()
    out = aligned.reindex(calendar)
    out = out.fillna(float(fallback))
    return out.clip(lower=0.0)


def apply_pit_borrow_to_universe(
    uni: pd.DataFrame,
    history: Mapping[str, pd.Series],
    as_of: str | pd.Timestamp,
    *,
    max_borrow: float | None = None,
    etf_col: str = "ETF",
) -> pd.DataFrame:
    """Copy universe with borrow_current overridden as-of date; optional borrow gate."""
    if uni.empty:
        return uni.copy()
    out = uni.copy()
    keep = []
    for idx, row in out.iterrows():
        etf = _norm(row.get(etf_col))
        fb = float(pd.to_numeric(row.get("borrow_current"), errors="coerce") or 0.0)
        if not np.isfinite(fb) or fb < 0:
            fb = 0.0
        b = borrow_asof(history, etf, as_of, fallback=fb)
        if b is None:
            b = fb
        out.at[idx, "borrow_current"] = float(b)
        if max_borrow is not None and float(b) > float(max_borrow) + 1e-12:
            keep.append(False)
        else:
            keep.append(True)
    return out.loc[keep].reset_index(drop=True)


def pit_meta(history: Mapping[str, pd.Series], *, enabled: bool) -> dict[str, Any]:
    return {
        "pit_borrow": bool(enabled),
        "borrow_history_symbols": int(len(history)),
    }

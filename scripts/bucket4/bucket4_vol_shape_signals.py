"""Bucket 4 vol-shape signals (TR/VCR) for hedge cadence."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd

_SCRIPTS = Path(__file__).resolve().parents[1]
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from vol_shape_metrics import build_underlying_vol_shape_history  # noqa: E402


def _default_norm(x: str) -> str:
    return str(x).strip().upper().replace(".", "-")


SIGNAL_COLUMNS = ["tr", "tr_est", "cadence_score", "vcr", "vcr_med", "rv_daily", "rv_weekly"]


def load_vol_shape_history(
    path: str | Path,
    *,
    norm_sym: Callable[[str], str] = _default_norm,
) -> dict[str, pd.DataFrame]:
    p = Path(path)
    if not p.is_file():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}
    symbols = raw.get("symbols") if isinstance(raw, dict) else None
    if not isinstance(symbols, dict):
        return {}

    out: dict[str, pd.DataFrame] = {}
    for sym, payload in symbols.items():
        series = (payload or {}).get("series") if isinstance(payload, dict) else None
        if not series:
            continue
        df = pd.DataFrame(series)
        if "date" not in df.columns or df.empty:
            continue
        ix = pd.to_datetime(df["date"], errors="coerce")
        if getattr(ix, "tz", None) is not None:
            ix = ix.tz_convert("UTC").tz_localize(None)
        df.index = pd.DatetimeIndex(ix).normalize()
        df = df.loc[df.index.notna()].sort_index()
        df = df[~df.index.duplicated(keep="last")]
        keep = {
            "tr": pd.to_numeric(df.get("trend_ratio"), errors="coerce"),
            "tr_est": pd.to_numeric(df.get("trend_ratio_fwd"), errors="coerce"),
            "cadence_score": pd.to_numeric(df.get("rebalance_cadence_score"), errors="coerce"),
            "vcr": pd.to_numeric(df.get("vcr"), errors="coerce"),
            "vcr_med": pd.to_numeric(df.get("vcr_median"), errors="coerce"),
            "rv_daily": pd.to_numeric(df.get("rv_daily"), errors="coerce"),
            "rv_weekly": pd.to_numeric(df.get("rv_weekly"), errors="coerce"),
        }
        out[norm_sym(sym)] = pd.DataFrame(keep, index=df.index)
    return out


def _recompute_signal_from_prices(prices: pd.Series, *, window: int = 60) -> pd.DataFrame:
    hist = build_underlying_vol_shape_history(pd.Series(prices).astype(float), window=int(window), max_points=0)
    series = hist.get("series") or []
    if not series:
        return pd.DataFrame(columns=SIGNAL_COLUMNS)
    df = pd.DataFrame(series)
    ix = pd.to_datetime(df["date"], errors="coerce")
    if getattr(ix, "tz", None) is not None:
        ix = ix.tz_convert("UTC").tz_localize(None)
    df.index = pd.DatetimeIndex(ix).normalize()
    df = df.loc[df.index.notna()].sort_index()
    return pd.DataFrame(
        {
            "tr": pd.to_numeric(df.get("trend_ratio"), errors="coerce"),
            "tr_est": pd.to_numeric(df.get("trend_ratio_fwd"), errors="coerce"),
            "cadence_score": pd.to_numeric(df.get("rebalance_cadence_score"), errors="coerce"),
            "vcr": pd.to_numeric(df.get("vcr"), errors="coerce"),
            "vcr_med": pd.to_numeric(df.get("vcr_median"), errors="coerce"),
            "rv_daily": pd.to_numeric(df.get("rv_daily"), errors="coerce"),
            "rv_weekly": pd.to_numeric(df.get("rv_weekly"), errors="coerce"),
        },
        index=df.index,
    )


def get_pair_signal(
    etf: str,
    und: str,
    calendar: pd.DatetimeIndex,
    *,
    history: dict[str, pd.DataFrame],
    underlying_prices: pd.Series | None = None,
    window: int = 60,
    lookahead_shift: int = 1,
    prefer_underlying_recompute: bool = True,
    norm_sym: Callable[[str], str] = _default_norm,
) -> pd.DataFrame:
    cal = pd.DatetimeIndex(calendar)
    if getattr(cal, "tz", None) is not None:
        cal = cal.tz_convert("UTC").tz_localize(None)
    cal = cal.normalize()

    hist_etf = history.get(norm_sym(etf))
    rec = (
        _recompute_signal_from_prices(underlying_prices, window=window)
        if underlying_prices is not None
        else None
    )

    if prefer_underlying_recompute and rec is not None and not rec.empty:
        df = rec
        source = "recompute_underlying"
    elif hist_etf is not None and not hist_etf.empty:
        df = hist_etf
        source = "history_etf"
    elif rec is not None and not rec.empty:
        df = rec
        source = "recompute_underlying"
    else:
        df = None
        source = "missing"

    if df is None or df.empty:
        empty = pd.DataFrame({c: np.nan for c in SIGNAL_COLUMNS}, index=cal)
        empty.attrs["signal_source"] = "missing"
        return empty

    aligned = df.reindex(df.index.union(cal)).sort_index().ffill().reindex(cal)
    if "vcr_med" not in aligned or aligned["vcr_med"].isna().all():
        aligned["vcr_med"] = aligned["vcr"].expanding(min_periods=1).median()
    aligned["vcr_med"] = aligned["vcr_med"].fillna(aligned["vcr"].expanding(min_periods=1).median())

    if lookahead_shift:
        aligned = aligned.shift(int(lookahead_shift))
    aligned.attrs["signal_source"] = source
    return aligned


__all__ = ["SIGNAL_COLUMNS", "get_pair_signal", "load_vol_shape_history"]

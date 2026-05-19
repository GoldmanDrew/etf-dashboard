"""Underlying vol-shape from joint ETF metrics (matches index.html charts)."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

TRADING_DAYS = 252
VOL_SHAPE_WINDOWS: tuple[int, ...] = (20, 60)
VOL_SHAPE_PRIMARY_WINDOW = 60
VOL_SHAPE_HISTORY_MAX_POINTS = 252


def _vol_shape_columns_for_window(window: int) -> tuple[str, ...]:
    return (
        f"und_rv_{window}d_daily_annual",
        f"und_rv_{window}d_weekly_annual",
        f"und_trend_ratio_{window}d",
        f"und_vcr_{window}d",
        f"und_return_{window}d",
        f"und_abs_return_{window}d_pctile",
        f"und_rv_{window}d_pctile",
        f"und_trend_ratio_{window}d_pctile",
        f"und_vcr_{window}d_pctile",
        f"und_vcr_{window}d_median",
        f"und_vol_shape_{window}d",
    )


def _all_vol_shape_columns() -> tuple[str, ...]:
    cols: list[str] = []
    for w in VOL_SHAPE_WINDOWS:
        cols.extend(_vol_shape_columns_for_window(w))
    return tuple(cols)


def _percentile_of_latest(values: list[float]) -> float | None:
    a = np.asarray([v for v in values if np.isfinite(v)], dtype=float)
    if a.size < 2:
        return None
    latest = float(a[-1])
    return float(np.mean(a <= latest))


def vol_shape_label(
    *,
    trend_ratio: float | None,
    vcr: float | None,
    abs_return_pctile: float | None,
    rv_pctile: float | None,
    vcr_pctile: float | None,
) -> str | None:
    if trend_ratio is None or vcr is None:
        return None
    notable = (
        (abs_return_pctile is not None and abs_return_pctile >= 0.80)
        or (rv_pctile is not None and rv_pctile >= 0.80)
    )
    trending = trend_ratio >= 1.05
    mean_reverting = trend_ratio <= 0.95
    jumpy = vcr >= 0.40 or (vcr_pctile is not None and vcr_pctile >= 0.80)

    if notable and trending and jumpy:
        return "jumpy_trend"
    if notable and trending:
        return "boiling_trend"
    if notable and mean_reverting:
        return "choppy_volatile"
    if notable:
        return "volatile_mixed"
    if trending:
        return "quiet_trend"
    if mean_reverting:
        return "quiet_chop"
    return "quiet_mixed"


def underlying_vol_shape_from_prices(prices: pd.Series, window: int) -> dict[str, Any]:
    """Rolling vol-shape on underlying prices (log returns), ls-algo semantics."""
    cols = _vol_shape_columns_for_window(window)
    label_col = f"und_vol_shape_{window}d"
    empty: dict[str, Any] = {col: None for col in cols if col != label_col}
    empty[label_col] = None
    if window <= 0 or window % 5 != 0:
        return empty
    if prices is None:
        return empty

    s = pd.to_numeric(prices, errors="coerce").dropna()
    s = s[~s.index.duplicated(keep="last")].sort_index()
    if len(s) < window + 1:
        return empty

    r = np.log(s / s.shift(1)).replace([np.inf, -np.inf], np.nan).dropna()
    r = r[np.isfinite(r)]
    if len(r) < window:
        return empty

    rv_daily_hist: list[float] = []
    rv_weekly_hist: list[float] = []
    tr_hist: list[float] = []
    vcr_hist: list[float] = []
    ret_hist: list[float] = []

    n_weeks = window // 5
    vals = r.to_numpy(dtype=float)
    for end in range(window, vals.size + 1):
        tail = vals[end - window : end]
        sq = tail**2
        sum_sq = float(np.sum(sq))
        if not np.isfinite(sum_sq) or sum_sq <= 0:
            continue
        rv_daily = float(np.sqrt(np.mean(sq) * TRADING_DAYS))
        weekly = tail.reshape(n_weeks, 5).sum(axis=1)
        rv_weekly = float(np.sqrt(np.mean(weekly**2) * (TRADING_DAYS / 5.0)))
        trend_ratio = rv_weekly / rv_daily if rv_daily > 0 else np.nan
        vcr = float(np.max(sq) / sum_sq)
        rv_daily_hist.append(rv_daily)
        rv_weekly_hist.append(rv_weekly)
        tr_hist.append(float(trend_ratio))
        vcr_hist.append(vcr)
        ret_hist.append(float(np.sum(tail)))

    if not rv_daily_hist:
        return empty

    rv_pctile = _percentile_of_latest(rv_daily_hist)
    abs_ret_pctile = _percentile_of_latest([abs(x) for x in ret_hist])
    trend_pctile = _percentile_of_latest(tr_hist)
    vcr_pctile = _percentile_of_latest(vcr_hist)
    vcr_median_hist = float(np.median(vcr_hist)) if vcr_hist else np.nan
    label = vol_shape_label(
        trend_ratio=tr_hist[-1],
        vcr=vcr_hist[-1],
        abs_return_pctile=abs_ret_pctile,
        rv_pctile=rv_pctile,
        vcr_pctile=vcr_pctile,
    )

    def _f(v: float | None) -> float | None:
        if v is None or not np.isfinite(v):
            return None
        return round(float(v), 6)

    return {
        f"und_rv_{window}d_daily_annual": _f(rv_daily_hist[-1]),
        f"und_rv_{window}d_weekly_annual": _f(rv_weekly_hist[-1]),
        f"und_trend_ratio_{window}d": _f(tr_hist[-1]),
        f"und_vcr_{window}d": _f(vcr_hist[-1]),
        f"und_return_{window}d": _f(ret_hist[-1]),
        f"und_abs_return_{window}d_pctile": _f(abs_ret_pctile),
        f"und_rv_{window}d_pctile": _f(rv_pctile),
        f"und_trend_ratio_{window}d_pctile": _f(trend_pctile),
        f"und_vcr_{window}d_pctile": _f(vcr_pctile),
        f"und_vcr_{window}d_median": _f(vcr_median_hist),
        f"und_vol_shape_{window}d": label,
    }


def underlying_vol_shape_panel_from_prices(prices: pd.Series) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for w in VOL_SHAPE_WINDOWS:
        out.update(underlying_vol_shape_from_prices(prices, w))
    return out


def _round_hist(v: float | None) -> float | None:
    if v is None or not np.isfinite(v):
        return None
    return round(float(v), 6)


def build_underlying_vol_shape_history(
    prices: pd.Series,
    window: int = VOL_SHAPE_PRIMARY_WINDOW,
    max_points: int = VOL_SHAPE_HISTORY_MAX_POINTS,
) -> dict[str, Any]:
    """Rolling TR/VCR/RV series (matches index.html buildUnderlyingVolShapeHistory)."""
    empty: dict[str, Any] = {"series": [], "vcrMedian": None, "window": window}
    if window <= 0 or window % 5 != 0:
        return empty
    if prices is None:
        return empty

    s = pd.to_numeric(prices, errors="coerce").dropna()
    s = s[~s.index.duplicated(keep="last")].sort_index()
    if len(s) < window + 1:
        return empty

    log_r = np.log(s / s.shift(1)).replace([np.inf, -np.inf], np.nan).dropna()
    log_r = log_r[np.isfinite(log_r)]
    if len(log_r) < window:
        return empty

    rets: list[tuple[str, float]] = [
        (str(log_r.index[i]), float(log_r.iloc[i])) for i in range(len(log_r))
    ]
    n_weeks = window // 5
    out: list[dict[str, Any]] = []
    for end in range(window, len(rets) + 1):
        tail = np.asarray([r for _, r in rets[end - window : end]], dtype=float)
        sum_sq = float(np.sum(tail**2))
        if not np.isfinite(sum_sq) or sum_sq <= 0:
            continue
        rv_daily = float(np.sqrt((sum_sq / window) * TRADING_DAYS))
        weekly = tail.reshape(n_weeks, 5).sum(axis=1)
        rv_weekly = float(np.sqrt(np.mean(weekly**2) * (TRADING_DAYS / 5.0)))
        trend_ratio = rv_weekly / rv_daily if rv_daily > 0 else None
        vcr = float(np.max(tail**2) / sum_sq)
        out.append(
            {
                "date": rets[end - 1][0],
                "rv_daily": _round_hist(rv_daily),
                "rv_weekly": _round_hist(rv_weekly),
                "trend_ratio": _round_hist(trend_ratio),
                "vcr": _round_hist(vcr),
            }
        )

    if max_points > 0 and len(out) > max_points:
        out = out[-max_points:]

    vcr_vals = sorted(
        float(x["vcr"]) for x in out if x.get("vcr") is not None and np.isfinite(float(x["vcr"]))
    )
    if vcr_vals:
        mid = len(vcr_vals) // 2
        vcr_median = (
            vcr_vals[mid]
            if len(vcr_vals) % 2
            else 0.5 * (vcr_vals[mid - 1] + vcr_vals[mid])
        )
    else:
        vcr_median = None
    vcr_median_r = _round_hist(vcr_median)
    series = [{**row, "vcr_median": vcr_median_r} for row in out]
    return {"series": series, "vcrMedian": vcr_median_r, "window": window}


def _joint_metrics_price_series(rows: pd.DataFrame) -> pd.Series | None:
    if rows is None or rows.empty:
        return None
    prices: list[tuple[str, float]] = []
    for _, row in rows.iterrows():
        ds = str(row.get("date") or "").strip()
        pl = row.get("close_price")
        if pl is None or (isinstance(pl, float) and not np.isfinite(pl)):
            pl = row.get("nav")
        ps = row.get("underlying_adj_close")
        try:
            pl_f = float(pl)
            ps_f = float(ps)
        except (TypeError, ValueError):
            continue
        if not ds or not (np.isfinite(pl_f) and pl_f > 0 and np.isfinite(ps_f) and ps_f > 0):
            continue
        prices.append((ds, ps_f))
    if not prices:
        return None
    prices.sort(key=lambda x: x[0])
    idx = pd.Index([p[0] for p in prices], name="date")
    return pd.Series([p[1] for p in prices], index=idx, dtype=float)


def load_vol_shape_from_metrics(
    metrics_path: Path,
    universe_symbols: set[str] | None = None,
    *,
    history_max_points: int = VOL_SHAPE_HISTORY_MAX_POINTS,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Headline vol-shape panels and rolling history keyed by ETF symbol."""
    empty_history: dict[str, Any] = {
        "window": VOL_SHAPE_PRIMARY_WINDOW,
        "history_max_points": history_max_points,
        "symbols": {},
        "symbols_count": 0,
    }
    if not metrics_path.exists():
        return {}, empty_history
    try:
        df = pd.read_csv(metrics_path)
    except Exception as e:
        print(f"  Warning: could not read ETF metrics for vol-shape: {e}")
        return {}, empty_history

    if "ticker" not in df.columns:
        return {}, empty_history

    if "date" in df.columns:
        df = df.sort_values(["ticker", "date"], kind="stable")

    out: dict[str, dict[str, Any]] = {}
    history_symbols: dict[str, Any] = {}
    for ticker, grp in df.groupby("ticker", sort=False):
        sym = str(ticker or "").strip().upper()
        if not sym:
            continue
        if universe_symbols is not None and sym not in universe_symbols:
            continue
        px = _joint_metrics_price_series(grp)
        if px is None:
            continue
        panel = underlying_vol_shape_panel_from_prices(px)
        primary_tr = panel.get(f"und_trend_ratio_{VOL_SHAPE_PRIMARY_WINDOW}d")
        if primary_tr is None:
            continue
        panel["und_vol_shape_source"] = "etf_metrics_daily"
        panel["und_vol_shape_metrics_asof"] = str(px.index[-1]) if len(px.index) else None
        panel["und_vol_shape_joint_days"] = int(len(px))
        out[sym] = panel
        hist = build_underlying_vol_shape_history(
            px,
            window=VOL_SHAPE_PRIMARY_WINDOW,
            max_points=history_max_points,
        )
        if hist.get("series"):
            history_symbols[sym] = hist

    empty_history["symbols"] = history_symbols
    empty_history["symbols_count"] = len(history_symbols)
    return out, empty_history


def load_vol_shape_by_symbol(
    metrics_path: Path,
    universe_symbols: set[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Per-ETF headline vol-shape only (backward-compatible helper)."""
    panels, _ = load_vol_shape_from_metrics(metrics_path, universe_symbols)
    return panels


def apply_vol_shape_to_record(rec: dict[str, Any], panel: dict[str, Any] | None) -> None:
    """Overwrite vol-shape fields when metrics panel is available."""
    if not panel:
        rec.setdefault("und_vol_shape_source", "screener")
        return
    for key in _all_vol_shape_columns():
        if key in panel and panel[key] is not None:
            rec[key] = panel[key]
    rec["und_vol_shape_source"] = panel.get("und_vol_shape_source", "etf_metrics_daily")
    if panel.get("und_vol_shape_metrics_asof"):
        rec["und_vol_shape_metrics_asof"] = panel["und_vol_shape_metrics_asof"]
    if panel.get("und_vol_shape_joint_days") is not None:
        rec["und_vol_shape_joint_days"] = panel["und_vol_shape_joint_days"]

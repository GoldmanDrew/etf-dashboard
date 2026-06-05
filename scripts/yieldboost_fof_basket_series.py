"""Basket TR, chart payloads, vol, and static scenarios for YieldBOOST FoF."""
from __future__ import annotations

import math
from typing import Any, Callable

import numpy as np
import pandas as pd

from yieldboost_fof_constants import YIELDBOOST_CHILD_TO_UNDERLYING

_TRADING_DAYS = 252
_CHART_MAX_POINTS = 130
_SCENARIO_HORIZONS = (
    ("1M", 21 / _TRADING_DAYS),
    ("3M", 63 / _TRADING_DAYS),
    ("6M", 126 / _TRADING_DAYS),
)
_SCENARIO_SHOCKS = (-1.0, -0.5, 0.0, 0.5, 1.0)
_MAX_DAILY_RETURN = 0.35
_FOF_CHART_RANGE_DAYS: dict[str, int] = {
    "1M": 21,
    "3M": 63,
    "6M": 126,
    "12M": 252,
    "ALL": 0,
}


def _norm_sym(s: object) -> str:
    return str(s or "").strip().upper().replace(".", "-")


def _tickers_for_underlying(underlying: str) -> list[str]:
    und = _norm_sym(underlying)
    tickers = [t for t, u in YIELDBOOST_CHILD_TO_UNDERLYING.items() if _norm_sym(u) == und]
    if und not in tickers:
        tickers.append(und)
    return [_norm_sym(t) for t in tickers]


def _price_col(sub: pd.DataFrame, col: str) -> pd.Series:
    if col not in sub.columns:
        return pd.Series(index=sub.index, dtype=float)
    return pd.to_numeric(sub[col], errors="coerce")


def _child_price_series(metrics: pd.DataFrame, yb_etf: str) -> pd.Series:
    """Child ETF price path for FoF rollup — issuer NAV first, then adj close."""
    sym = _norm_sym(yb_etf)
    if metrics.empty:
        return pd.Series(dtype=float)
    sub = metrics[metrics["ticker"].astype(str).str.upper() == sym].copy()
    if sub.empty:
        return pd.Series(dtype=float)
    sub = sub.sort_values("date")
    nav_s = _price_col(sub, "nav")
    adj_s = _price_col(sub, "etf_adj_close")
    close_s = _price_col(sub, "close_price")
    px = nav_s.fillna(adj_s).fillna(close_s)
    out = pd.Series(px.values, index=sub["date"].astype(str))
    return out.dropna()


def _underlying_tr_series(metrics: pd.DataFrame, underlying: str) -> pd.Series:
    if metrics.empty or "underlying_adj_close" not in metrics.columns:
        return pd.Series(dtype=float)
    tickers = _tickers_for_underlying(underlying)
    sub = metrics[metrics["ticker"].astype(str).str.upper().isin(tickers)].copy()
    if sub.empty:
        return pd.Series(dtype=float)
    sub = sub.sort_values("date")
    by_date = sub.groupby("date", sort=True)["underlying_adj_close"].first()
    return pd.to_numeric(by_date, errors="coerce").dropna()


def _weights_for_date(history_snaps: list[dict[str, Any]], d: str) -> dict[str, float]:
    applicable = [s for s in history_snaps if str(s.get("as_of") or "") <= d]
    snap = applicable[-1] if applicable else (history_snaps[0] if history_snaps else None)
    if not snap:
        return {}
    return dict(snap.get("underlying_weights") or {})


def _child_weights_for_date(history_snaps: list[dict[str, Any]], d: str) -> dict[str, float]:
    applicable = [s for s in history_snaps if str(s.get("as_of") or "") <= d]
    snap = applicable[-1] if applicable else (history_snaps[0] if history_snaps else None)
    if not snap:
        return {}
    children = snap.get("children") or []
    wsum = sum(float(c.get("weight_pct") or 0) for c in children) or 100.0
    out: dict[str, float] = {}
    for c in children:
        yb = _norm_sym(c.get("yb_etf"))
        w = float(c.get("weight_pct") or 0) / wsum
        if yb and w > 0:
            out[yb] = w
    return out


def _compound_weighted_index(
    dates: list[str],
    weight_fn: Callable[[str], dict[str, float]],
    price_maps: dict[str, pd.Series],
) -> pd.Series:
    """Total-return index (base=100) via compounded daily weighted log returns."""
    if not dates:
        return pd.Series(dtype=float)

    index_vals: list[float] = []
    valid_dates: list[str] = []
    level = 100.0
    prev_prices: dict[str, float] = {}

    for d in dates:
        wmap = weight_fn(d)
        if not wmap:
            continue
        prices: dict[str, float] = {}
        for sym, w in wmap.items():
            if w <= 0:
                continue
            s = price_maps.get(sym)
            if s is None or d not in s.index:
                continue
            try:
                px = float(s.loc[d])
            except (KeyError, TypeError, ValueError):
                continue
            if math.isfinite(px) and px > 0:
                prices[sym] = px
        if not prices:
            continue
        if not prev_prices:
            prev_prices = prices
            valid_dates.append(d)
            index_vals.append(level)
            continue

        log_ret = 0.0
        wsum = 0.0
        for sym, w in wmap.items():
            if sym not in prices or sym not in prev_prices:
                continue
            try:
                r = math.log(prices[sym] / prev_prices[sym])
            except (ValueError, ZeroDivisionError):
                continue
            if not math.isfinite(r) or abs(r) > _MAX_DAILY_RETURN:
                continue
            log_ret += w * r
            wsum += w
        if wsum <= 0:
            continue
        log_ret /= wsum
        level *= math.exp(log_ret)
        prev_prices = prices
        valid_dates.append(d)
        index_vals.append(level)

    if not valid_dates:
        return pd.Series(dtype=float)
    return pd.Series(index_vals, index=valid_dates)


def build_synthetic_fof_nav_series(
    history_snaps: list[dict[str, Any]],
    metrics: pd.DataFrame,
) -> pd.Series:
    """Implied FoF TR index from weighted child ETF NAVs (compounded daily returns)."""
    if not history_snaps:
        return pd.Series(dtype=float)
    child_maps: dict[str, pd.Series] = {}
    all_dates: set[str] = set()
    for snap in history_snaps:
        for c in snap.get("children") or []:
            yb = _norm_sym(c.get("yb_etf"))
            if not yb or yb in child_maps:
                continue
            s = _child_price_series(metrics, yb)
            if not s.empty:
                child_maps[yb] = s
                all_dates.update(s.index.tolist())
    if not child_maps:
        return pd.Series(dtype=float)
    dates = sorted(all_dates)
    return _compound_weighted_index(
        dates,
        lambda d: _child_weights_for_date(history_snaps, d),
        child_maps,
    )


def build_basket_tr_series(
    history_snaps: list[dict[str, Any]],
    metrics: pd.DataFrame,
) -> pd.Series:
    """Weighted underlying total-return index (compounded daily returns, base=100)."""
    if not history_snaps:
        return pd.Series(dtype=float)
    und_maps: dict[str, pd.Series] = {}
    all_dates: set[str] = set()
    last_snap = history_snaps[-1]
    for und in (last_snap.get("underlying_weights") or {}):
        s = _underlying_tr_series(metrics, und)
        if not s.empty:
            und_maps[_norm_sym(und)] = s
            all_dates.update(s.index.tolist())
    if not und_maps:
        return pd.Series(dtype=float)
    dates = sorted(all_dates)
    return _compound_weighted_index(
        dates,
        lambda d: { _norm_sym(k): v for k, v in _weights_for_date(history_snaps, d).items() },
        und_maps,
    )


def series_has_price_anomaly(series: pd.Series, *, max_daily_return: float = _MAX_DAILY_RETURN) -> bool:
    """True when level series has impossible one-day jumps (scale mismatch / bad overlay)."""
    if series is None or len(series) < 2:
        return False
    rets = series.astype(float).pct_change().dropna()
    if rets.empty:
        return False
    return bool((rets.abs() > max_daily_return).any())


def resolve_fof_price_series(
    fof_symbol: str,
    history_snaps: list[dict[str, Any]],
    metrics: pd.DataFrame,
) -> pd.Series:
    """FoF price path for pair/chart — compounded child NAV preferred over poisoned store rows."""
    sym = _norm_sym(fof_symbol)
    compounded = build_synthetic_fof_nav_series(history_snaps, metrics) if history_snaps else pd.Series(dtype=float)

    if metrics.empty:
        return compounded

    sub = metrics[metrics["ticker"].astype(str).str.upper() == sym].copy()
    if sub.empty:
        return compounded
    sub = sub.sort_values("date")
    px = pd.to_numeric(sub.get("nav"), errors="coerce")
    if px.notna().sum() < 3 and "etf_adj_close" in sub.columns:
        px = pd.to_numeric(sub["etf_adj_close"], errors="coerce")
    if px.notna().sum() < 3:
        px = pd.to_numeric(sub.get("close_price"), errors="coerce")
    stored = pd.Series(px.values, index=sub["date"].astype(str)).dropna()

    if stored.empty:
        return compounded
    if compounded.empty:
        return stored
    if series_has_price_anomaly(stored):
        return compounded
    if len(compounded) >= max(5, int(len(stored) * 0.8)):
        return compounded
    return stored


def _pct_change_from_base(series: pd.Series, start_date: str | None = None) -> pd.Series:
    if series.empty:
        return series
    sub = series
    if start_date:
        sub = series[series.index >= start_date]
    if sub.empty:
        return pd.Series(dtype=float)
    base = float(sub.iloc[0])
    if not math.isfinite(base) or base <= 0:
        return pd.Series(dtype=float)
    return (sub / base - 1.0) * 100.0


def _downsample_series(dates: list[str], *series: list[float]) -> tuple[list[str], list[list[float]]]:
    n = len(dates)
    if n <= _CHART_MAX_POINTS:
        return dates, [list(s) for s in series]
    step = max(1, n // _CHART_MAX_POINTS)
    idxs = list(range(0, n, step))
    if idxs[-1] != n - 1:
        idxs.append(n - 1)
    out_dates = [dates[i] for i in idxs]
    out_series = [[s[i] for i in idxs] for s in series]
    return out_dates, out_series


def chart_window_days_for_range(range_key: str | None, *, n_available: int) -> int:
    key = str(range_key or "6M").upper()
    if key == "ALL":
        return 0
    if key == "YTD":
        return 0
    days = _FOF_CHART_RANGE_DAYS.get(key, 126)
    if days <= 0:
        return 0
    return min(days, n_available) if n_available > 0 else days


def build_fof_chart_payload(
    fof_px: pd.Series,
    basket_px: pd.Series,
    *,
    window_days: int = 126,
) -> dict[str, Any]:
    """Compact FoF vs basket % change series for the chart tab."""
    if fof_px.empty and basket_px.empty:
        return {"ok": False, "error": "no price series"}

    common = sorted(set(fof_px.index) & set(basket_px.index)) if not fof_px.empty and not basket_px.empty else sorted(
        set(fof_px.index) | set(basket_px.index)
    )
    if window_days > 0 and len(common) > window_days:
        common = common[-window_days:]

    if not common:
        return {"ok": False, "error": "no aligned dates"}

    start = common[0]
    fof_win = fof_px.reindex(common).ffill().dropna()
    basket_win = basket_px.reindex(common).ffill().dropna()
    aligned_dates = sorted(set(fof_win.index) & set(basket_win.index))
    if not aligned_dates:
        return {"ok": False, "error": "no aligned price days"}

    fof_pct = _pct_change_from_base(fof_win.reindex(aligned_dates), start)
    basket_pct = _pct_change_from_base(basket_win.reindex(aligned_dates), start)
    spread = basket_pct - fof_pct

    fof_list = [round(float(x), 4) if math.isfinite(float(x)) else None for x in fof_pct.tolist()]
    basket_list = [round(float(x), 4) if math.isfinite(float(x)) else None for x in basket_pct.tolist()]
    spread_list = [round(float(x), 4) if math.isfinite(float(x)) else None for x in spread.tolist()]
    dates, series = _downsample_series(aligned_dates, fof_list, basket_list, spread_list)

    fof_end = fof_list[-1] if fof_list else None
    basket_end = basket_list[-1] if basket_list else None
    spread_end = (basket_end - fof_end) if basket_end is not None and fof_end is not None else None

    return {
        "ok": True,
        "dates": dates,
        "fof_pct": series[0],
        "basket_pct": series[1],
        "spread_pct": series[2],
        "fof_end_pct": fof_end,
        "basket_end_pct": basket_end,
        "spread_end_pct": spread_end,
        "n_days": len(aligned_dates),
        "window_days": window_days,
        "start_date": aligned_dates[0] if aligned_dates else None,
        "end_date": aligned_dates[-1] if aligned_dates else None,
        "fof_index": [round(float(x), 6) for x in fof_win.reindex(aligned_dates).tolist()],
        "basket_index": [round(float(x), 6) for x in basket_win.reindex(aligned_dates).tolist()],
        "all_dates": aligned_dates,
    }


def compute_basket_realized_vol(
    basket_px: pd.Series,
    *,
    window: int = 60,
) -> dict[str, float | None]:
    if len(basket_px) < max(5, window // 2):
        return {"vol_annual": None, "n_obs": len(basket_px)}
    rets = np.log(basket_px / basket_px.shift(1)).dropna()
    if len(rets) < 5:
        return {"vol_annual": None, "n_obs": len(rets)}
    tail = rets.tail(window)
    vol = float(tail.std(ddof=1) * math.sqrt(_TRADING_DAYS)) if len(tail) > 1 else None
    return {"vol_annual": round(vol, 6) if vol is not None and math.isfinite(vol) else None, "n_obs": int(len(tail))}


def build_fof_static_scenario_grid(
    *,
    forward_p50: float | None,
    effective_beta: float | None,
    borrow_annual: float | None,
    basket_vol: float | None,
) -> dict[str, Any]:
    """Simple shock × horizon grid for FoF short vs basket (static, no MC)."""
    beta = float(effective_beta) if effective_beta is not None and math.isfinite(float(effective_beta)) else 1.0
    sigma = float(basket_vol) if basket_vol is not None and math.isfinite(float(basket_vol)) and basket_vol > 0 else 0.5
    fwd = float(forward_p50) if forward_p50 is not None and math.isfinite(float(forward_p50)) else None
    borrow = float(borrow_annual) if borrow_annual is not None and math.isfinite(float(borrow_annual)) else 0.0
    rows: list[dict[str, Any]] = []
    for shock in _SCENARIO_SHOCKS:
        cells: list[dict[str, Any]] = []
        for label, years in _SCENARIO_HORIZONS:
            und_ret = math.expm1(shock * sigma * math.sqrt(years))
            gross_log = beta * math.log1p(und_ret) if und_ret > -1 else None
            if gross_log is None:
                cells.append({"horizon": label, "gross_simple": None, "net_simple": None})
                continue
            if fwd is not None:
                gross_log = 0.5 * gross_log + 0.5 * (fwd * years)
            gross_simple = math.expm1(gross_log)
            net_simple = math.expm1(gross_log - borrow * years) if gross_log is not None else None
            cells.append({
                "horizon": label,
                "gross_simple": round(gross_simple, 6),
                "net_simple": round(net_simple, 6) if net_simple is not None else None,
            })
        rows.append({"shock_sigma": shock, "underlying_return_0": round(math.expm1(shock * sigma * math.sqrt(_SCENARIO_HORIZONS[1][1])), 6), "cells": cells})
    return {
        "ok": True,
        "engine": "fof_static_basket_shock",
        "sigma_annual": round(sigma, 6),
        "effective_beta": round(beta, 4),
        "rows": rows,
    }


def enrich_fof_dashboard_extras(
    fof_symbol: str,
    *,
    history_snaps: list[dict[str, Any]],
    metrics: pd.DataFrame,
    fof_px: pd.Series | None = None,
    forward_p50: float | None = None,
    effective_beta: float | None = None,
    borrow_annual: float | None = None,
) -> dict[str, Any]:
    """Build chart, vol, scenario, and data-status payloads for a FoF row."""
    sym = _norm_sym(fof_symbol)
    if fof_px is None or fof_px.empty:
        fof_px = resolve_fof_price_series(sym, history_snaps, metrics)
    n_metrics = int((metrics["ticker"].astype(str).str.upper() == sym).sum()) if not metrics.empty else 0

    basket_px = build_basket_tr_series(history_snaps, metrics)
    chart = build_fof_chart_payload(fof_px, basket_px, window_days=0)
    vol = compute_basket_realized_vol(basket_px)
    scenario = build_fof_static_scenario_grid(
        forward_p50=forward_p50,
        effective_beta=effective_beta,
        borrow_annual=borrow_annual,
        basket_vol=vol.get("vol_annual"),
    )

    sparkline: list[float] = []
    if chart.get("ok") and chart.get("basket_pct"):
        sparkline = [x for x in chart["basket_pct"] if x is not None][-40:]

    status = "ok" if n_metrics >= 20 else ("partial" if n_metrics >= 5 else "insufficient")
    return {
        "fof_metrics_days": n_metrics,
        "fof_data_status": status,
        "fof_chart": chart,
        "fof_basket_vol": vol,
        "fof_scenario_grid": scenario,
        "fof_basket_sparkline": sparkline,
    }

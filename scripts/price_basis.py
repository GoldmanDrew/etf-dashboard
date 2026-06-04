"""Split-aware total-return price series for ETF metrics rows (Python mirror of assets/price_basis.js)."""
from __future__ import annotations

import datetime as dt
import math
from typing import Any

from split_adjustments import (
    filter_splits_needing_close_basis_fix,
    nearest_split_ratio,
)


def parse_split_events_from_corp(payload: dict | None, ticker: str) -> list[tuple[dt.date, float]]:
    sym = str(ticker or "").strip().upper()
    if not sym or not payload:
        return []
    out: list[tuple[dt.date, float]] = []
    for ev in payload.get("events") or []:
        if str(ev.get("ticker") or "").strip().upper() != sym:
            continue
        typ = str(ev.get("type") or "")
        if typ not in {"reverse_split", "forward_split"}:
            continue
        ed = str(ev.get("execution_date") or "")[:10]
        rf, rt = ev.get("ratio_from"), ev.get("ratio_to")
        if not ed or rf is None or rt is None:
            continue
        try:
            d0 = dt.date.fromisoformat(ed)
            mult = float(rf) / float(rt)
        except (ValueError, TypeError, ZeroDivisionError):
            continue
        if mult > 0:
            out.append((d0, mult))
    return sorted(out)


def _close_points(rows: list[dict[str, Any]]) -> list[tuple[dt.date, float]]:
    pts: list[tuple[dt.date, float]] = []
    for row in rows:
        ds = str(row.get("date") or "")[:10]
        if len(ds) != 10:
            continue
        close = row.get("close_price") or row.get("nav")
        try:
            px = float(close)
        except (TypeError, ValueError):
            continue
        if px > 0:
            pts.append((dt.date.fromisoformat(ds), px))
    return sorted(pts)


def detect_split_boundary(points: list[tuple[dt.date, float]], split_mult: float) -> dt.date | None:
    if len(points) < 2:
        return None
    for i in range(1, len(points)):
        prev, cur = points[i - 1][1], points[i][1]
        if prev <= 0 or cur <= 0:
            continue
        expected = nearest_split_ratio(cur / prev)
        if expected is not None and abs(expected - split_mult) <= max(1e-6, 0.15 * abs(split_mult)):
            return points[i][0]
    return None


def resolve_split_context(
    close_points: list[tuple[dt.date, float]],
    split_events: list[tuple[dt.date, float]],
) -> dict[str, Any]:
    if not close_points or not split_events:
        return {"mode": "continuous", "boundary": None, "mult": None, "filtered": []}
    dated = [(d, c) for d, c in close_points]
    filtered = filter_splits_needing_close_basis_fix(
        [(d, c, c) for d, c in dated],
        split_events,
    )
    mult = None
    boundary = None
    if filtered:
        mult = filtered[0][1]
        boundary = detect_split_boundary(close_points, mult)
        if boundary is None:
            boundary = filtered[0][0]
    if boundary is None:
        for _d, m in split_events:
            b = detect_split_boundary(close_points, m)
            if b is not None:
                boundary, mult = b, m
                break
    mode = "discrete_split" if boundary and mult else "continuous"
    return {"mode": mode, "boundary": boundary, "mult": mult, "filtered": filtered}


def _is_pre_split(ds: str, ctx: dict[str, Any]) -> bool:
    boundary = ctx.get("boundary")
    mult = ctx.get("mult")
    if ctx.get("mode") != "discrete_split" or not boundary or not mult:
        return False
    try:
        d0 = dt.date.fromisoformat(ds[:10])
    except ValueError:
        return False
    return d0 < boundary


def etf_tr_price_for_row(row: dict[str, Any], ctx: dict[str, Any]) -> float | None:
    ds = str(row.get("date") or "")[:10]
    if len(ds) != 10:
        return None
    try:
        close = float(row.get("close_price") or row.get("nav") or 0)
    except (TypeError, ValueError):
        return None
    if close <= 0:
        return None
    adj = row.get("etf_adj_close")
    nav_tr = row.get("nav_total_return")
    try:
        adj_f = float(adj) if adj is not None else float("nan")
    except (TypeError, ValueError):
        adj_f = float("nan")
    try:
        nav_f = float(nav_tr) if nav_tr is not None else float("nan")
    except (TypeError, ValueError):
        nav_f = float("nan")

    mult = float(ctx.get("mult") or 0)
    if _is_pre_split(ds, ctx) and mult > 0:
        if math.isfinite(adj_f) and adj_f > 0:
            return adj_f * mult
        if math.isfinite(nav_f) and nav_f > 0 and close > 0 and mult > 1.05:
            if nav_f / close >= mult * 0.85 or nav_f / close > 2.5:
                return close * mult
            return nav_f * mult
        if math.isfinite(nav_f) and nav_f > 0:
            return nav_f * mult
        return close * mult
    if math.isfinite(adj_f) and adj_f > 0:
        return adj_f
    if math.isfinite(nav_f) and nav_f > 0:
        return nav_f
    return close


def build_tr_series_from_metrics(
    rows: list[dict[str, Any]],
    split_events: list[tuple[dt.date, float]] | None = None,
) -> list[dict[str, Any]]:
    split_events = split_events or []
    sorted_rows = sorted(
        [r for r in rows if str(r.get("date") or "")[:10]],
        key=lambda r: str(r.get("date") or ""),
    )
    close_pts = _close_points(sorted_rows)
    ctx = resolve_split_context(close_pts, split_events)
    out: list[dict[str, Any]] = []
    for row in sorted_rows:
        tr_etf = etf_tr_price_for_row(row, ctx)
        try:
            und = float(row.get("underlying_adj_close"))
        except (TypeError, ValueError):
            und = float("nan")
        if tr_etf and tr_etf > 0 and und > 0:
            out.append(
                {
                    "date": str(row.get("date") or "")[:10],
                    "tr_etf_px": tr_etf,
                    "tr_und_px": und,
                    "trade_close": float(row.get("close_price") or row.get("nav") or 0),
                }
            )
    return out


def max_abs_log_return(series: list[dict[str, Any]], key: str) -> tuple[float, str | None]:
    max_jump = 0.0
    at: str | None = None
    for i in range(1, len(series)):
        try:
            a = float(series[i - 1][key])
            b = float(series[i][key])
        except (KeyError, TypeError, ValueError):
            continue
        if a <= 0 or b <= 0:
            continue
        lr = abs(math.log(b / a))
        if lr > max_jump:
            max_jump = lr
            at = str(series[i].get("date"))
    return max_jump, at

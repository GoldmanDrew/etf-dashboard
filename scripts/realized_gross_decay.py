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

from price_basis import (
    build_tr_series_from_metrics,
    parse_split_events_from_corp,
    resolve_split_context,
    sanitize_fabricated_adj_basis,
)

TRADING_DAYS = 252
DEFAULT_MIN_OBS = 40
REALIZED_PAIR_GROSS_60D_HORIZON = 60
MAX_CONTIGUOUS_METRICS_GAP_DAYS = 45
HARD_LIFECYCLE_GAP_DAYS = 365


def _log_to_simple_period(log_ret: float | None) -> float | None:
    if log_ret is None or not math.isfinite(float(log_ret)):
        return None
    return math.expm1(float(log_ret))


def _period_borrow_log(borrow_annual: float | None, obs_days: int) -> float:
    if borrow_annual is None or not math.isfinite(float(borrow_annual)):
        return 0.0
    n = max(0, int(obs_days))
    if n <= 0:
        return 0.0
    return float(borrow_annual) * (n / TRADING_DAYS)


def _parse_iso_date(value: Any) -> dt.date | None:
    ds = str(value or "")[:10]
    if len(ds) != 10:
        return None
    try:
        return dt.date.fromisoformat(ds)
    except ValueError:
        return None


def latest_contiguous_metrics_segment(
    rows: list[dict[str, Any]],
    *,
    max_gap_days: int = MAX_CONTIGUOUS_METRICS_GAP_DAYS,
) -> list[dict[str, Any]]:
    """Return the latest joint-price segment, cutting ticker-reuse/lifecycle gaps.

    ETF tickers can be reused after years of no trading history. The metrics store
    may then contain an old Yahoo bootstrap segment plus a new issuer segment under
    the same ticker. Treating that gap as one daily return corrupts realized decay
    and backtests, so downstream calculations only use the latest contiguous block.
    """
    dated: list[tuple[dt.date, dict[str, Any]]] = []
    for row in rows or []:
        d0 = _parse_iso_date(row.get("date") if isinstance(row, dict) else None)
        if d0 is not None:
            dated.append((d0, row))
    if len(dated) < 2:
        return [r for _d, r in dated]
    dated.sort(key=lambda x: x[0])
    start_idx = 0
    max_gap = max(1, int(max_gap_days))
    def source_key(row: dict[str, Any]) -> str:
        return "|".join(
            str(row.get(k) or "").strip().lower()
            for k in ("source_provider", "source_url", "status")
        )

    for i in range(1, len(dated)):
        gap = (dated[i][0] - dated[i - 1][0]).days
        prev_src = source_key(dated[i - 1][1])
        cur_src = source_key(dated[i][1])
        source_changed = bool(prev_src or cur_src) and prev_src != cur_src
        if gap > HARD_LIFECYCLE_GAP_DAYS or (gap > max_gap and source_changed):
            start_idx = i
    return [r for _d, r in dated[start_idx:]]


def build_daily_log_drag_series(
    tr_rows: list[dict[str, Any]],
    beta: float,
    *,
    max_gap_days: int = MAX_CONTIGUOUS_METRICS_GAP_DAYS,
) -> list[dict[str, Any]]:
    """Daily log-drag: beta * log(U_t/U_{t-1}) - log(L_t/L_{t-1}) on split-aware TR."""
    if not math.isfinite(beta):
        return []
    clean = [
        {
            "date": str(row.get("date") or "")[:10],
            "etf_px": float(row["tr_etf_px"]),
            "und_px": float(row["tr_und_px"]),
        }
        for row in (tr_rows or [])
        if str(row.get("date") or "")[:10]
        and float(row.get("tr_etf_px") or 0) > 0
        and float(row.get("tr_und_px") or 0) > 0
    ]
    clean.sort(key=lambda x: x["date"])
    if len(clean) < 2:
        return []
    out: list[dict[str, Any]] = []
    for i in range(1, len(clean)):
        d0 = _parse_iso_date(clean[i - 1]["date"])
        d1 = _parse_iso_date(clean[i]["date"])
        if d0 is not None and d1 is not None and (d1 - d0).days > HARD_LIFECYCLE_GAP_DAYS:
            continue
        u0, u1 = clean[i - 1]["und_px"], clean[i]["und_px"]
        e0, e1 = clean[i - 1]["etf_px"], clean[i]["etf_px"]
        r_u = math.log(u1 / u0)
        r_l = math.log(e1 / e0)
        if not math.isfinite(r_u) or not math.isfinite(r_l):
            continue
        out.append(
            {
                "date": clean[i]["date"],
                "drag": beta * r_u - r_l,
                "etf_px": e1,
                "und_px": u1,
                "etf_px_prev": e0,
                "und_px_prev": u0,
            }
        )
    return out


def _slice_period_metrics(
    drags: list[float],
    daily_series: list[dict[str, Any]],
    start_idx: int,
    end_idx: int,
    borrow_annual: float | None,
) -> dict[str, Any]:
    drag_slice = drags[start_idx : end_idx + 1]
    obs = len(drag_slice)
    if obs <= 0:
        return {
            "gross_log": None,
            "gross_simple": None,
            "net_log": None,
            "net_simple": None,
            "borrow_log": None,
            "obs": 0,
            "start_date": None,
            "end_date": None,
            "etf_start_px": None,
            "etf_end_px": None,
            "und_start_px": None,
            "und_end_px": None,
        }
    gross_log = float(sum(drag_slice))
    borrow_log = _period_borrow_log(borrow_annual, obs)
    net_log = gross_log - borrow_log
    start_row = daily_series[start_idx] if 0 <= start_idx < len(daily_series) else {}
    end_row = daily_series[end_idx] if 0 <= end_idx < len(daily_series) else {}
    return {
        "gross_log": gross_log,
        "gross_simple": _log_to_simple_period(gross_log),
        "net_log": net_log,
        "net_simple": _log_to_simple_period(net_log),
        "borrow_log": borrow_log,
        "obs": obs,
        "start_date": str(start_row.get("date") or "")[:10] or None,
        "end_date": str(end_row.get("date") or "")[:10] or None,
        "etf_start_px": start_row.get("etf_px_prev"),
        "etf_end_px": end_row.get("etf_px"),
        "und_start_px": start_row.get("und_px_prev"),
        "und_end_px": end_row.get("und_px"),
    }


def compute_horizon_period_returns(
    daily_series: list[dict[str, Any]],
    horizons: list[int] | None = None,
    borrow_annual: float | None = None,
) -> dict[str, Any]:
    """Mirror assets/realized_decay.js::computeHorizonPeriodReturns."""
    hs = horizons or [REALIZED_PAIR_GROSS_60D_HORIZON]
    series = daily_series or []
    drags = [float(x["drag"]) for x in series]
    n = len(series)
    end_date = str(series[n - 1]["date"])[:10] if n else None
    rows: list[dict[str, Any]] = []
    for h_raw in hs:
        h = max(1, int(h_raw))
        start_idx = max(0, n - h)
        end_idx = n - 1
        if end_idx < start_idx or n == 0:
            continue
        m = _slice_period_metrics(drags, series, start_idx, end_idx, borrow_annual)
        rows.append(
            {
                "horizon_days": h,
                **m,
                "sufficient": m["obs"] >= h,
            }
        )
    return {"horizons": rows, "n_days": n, "end_date": end_date, "borrow_annual": borrow_annual}


def realized_pair_gross_60d_fields(
    horizon_row: dict[str, Any] | None,
    *,
    source: str = "etf_metrics_daily",
) -> dict[str, Any]:
    if not horizon_row or horizon_row.get("gross_simple") is None:
        return {}
    gross_simple = horizon_row.get("gross_simple")
    if gross_simple is None or not math.isfinite(float(gross_simple)):
        return {}
    sufficient = bool(horizon_row.get("sufficient"))
    gross_log = (
        round(float(horizon_row.get("gross_log")), 6)
        if horizon_row.get("gross_log") is not None and math.isfinite(float(horizon_row.get("gross_log")))
        else None
    )
    out: dict[str, Any] = {
        "realized_pair_gross_60d_obs": int(horizon_row.get("obs") or 0),
        "realized_pair_gross_60d_sufficient": sufficient,
        "realized_pair_gross_60d_start_date": horizon_row.get("start_date"),
        "realized_pair_gross_60d_end_date": horizon_row.get("end_date"),
        "realized_pair_gross_60d_source": source,
    }
    net_simple = horizon_row.get("net_simple")
    if sufficient:
        out["realized_pair_gross_60d"] = round(float(gross_simple), 6)
        out["realized_pair_gross_60d_log"] = gross_log
        if net_simple is not None and math.isfinite(float(net_simple)):
            out["realized_pair_net_60d"] = round(float(net_simple), 6)
    else:
        out["realized_pair_gross_partial"] = round(float(gross_simple), 6)
        out["realized_pair_gross_partial_log"] = gross_log
        out["realized_pair_gross_partial_horizon_days"] = REALIZED_PAIR_GROSS_60D_HORIZON
        if net_simple is not None and math.isfinite(float(net_simple)):
            out["realized_pair_net_partial"] = round(float(net_simple), 6)
    return out


def _metrics_row_has_usable_prices(row: dict[str, Any]) -> bool:
    """Rows carried forward from stale ETF metrics should not drive realized decay."""
    if not str(row.get("date") or "")[:10]:
        return False
    source_url = str(row.get("source_url") or "")
    source_provider = str(row.get("source_provider") or "")
    stale_kind = str(row.get("stale_kind") or "")
    if (
        source_url.startswith("carry_forward://")
        or source_provider.lower().startswith("carry_forward")
        or stale_kind.lower() == "carry_forward"
    ):
        return False
    try:
        close_like = row.get("close_price") if row.get("close_price") is not None else row.get("nav")
        if float(close_like) <= 0:
            return False
        if float(row.get("underlying_adj_close")) <= 0:
            return False
    except (TypeError, ValueError):
        return False
    return True


def compute_realized_pair_gross_60d(
    rows: list[dict[str, Any]],
    beta: float,
    split_events: list[tuple[dt.date, float]] | None = None,
    *,
    borrow_annual: float | None = None,
    min_obs: int = 2,
) -> dict[str, Any] | None:
    """60d gross pair decay from joint metrics rows (Decay tab parity)."""
    if not math.isfinite(beta):
        return None
    usable_rows = [r for r in rows if _metrics_row_has_usable_prices(r)]
    usable_rows = latest_contiguous_metrics_segment(usable_rows)
    tr = build_tr_series_from_metrics(usable_rows, split_events or [])
    daily = build_daily_log_drag_series(tr, float(beta))
    if len(daily) < min_obs:
        return None
    result = compute_horizon_period_returns(
        daily,
        horizons=[REALIZED_PAIR_GROSS_60D_HORIZON],
        borrow_annual=borrow_annual,
    )
    h60 = next(
        (h for h in result.get("horizons") or [] if int(h.get("horizon_days") or 0) == REALIZED_PAIR_GROSS_60D_HORIZON),
        None,
    )
    if not h60:
        return None
    fields = realized_pair_gross_60d_fields(h60, source="etf_metrics_daily")
    if not fields:
        return None
    fields["n_days"] = result.get("n_days")
    return fields


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
    rows = latest_contiguous_metrics_segment(
        [r for r in rows if _metrics_row_has_usable_prices(r)]
    )
    rows = sanitize_fabricated_adj_basis(rows, split_events or [])
    tr = build_tr_series_from_metrics(rows, split_events or [])
    if len(tr) < min_obs + 1:
        return None

    skip_dates: set[dt.date] = set()
    if split_events:
        close_pts = []
        adj_by_date: dict[dt.date, float] = {}
        for row in rows:
            ds = str(row.get("date") or "")[:10]
            if len(ds) != 10:
                continue
            try:
                d0 = dt.date.fromisoformat(ds)
                close = float(row.get("close_price") or row.get("nav") or 0)
                if close <= 0:
                    continue
                close_pts.append((d0, close))
                if row.get("etf_adj_close") is not None:
                    adj_by_date[d0] = float(row["etf_adj_close"])
            except (ValueError, TypeError):
                continue
        ctx = resolve_split_context(
            close_pts,
            split_events or [],
            metric_rows=rows,
            adj_by_date=adj_by_date or None,
        )
        boundary = ctx.get("boundary")
        if boundary is not None:
            bnd = boundary if isinstance(boundary, dt.date) else dt.date.fromisoformat(str(boundary)[:10])
            for delta in range(-2, 3):
                skip_dates.add(bnd + dt.timedelta(days=delta))

    drags: list[float] = []
    for i in range(1, len(tr)):
        ds = str(tr[i].get("date") or "")[:10]
        try:
            d0 = dt.date.fromisoformat(ds)
        except ValueError:
            d0 = None
        u0, u1 = tr[i - 1]["tr_und_px"], tr[i]["tr_und_px"]
        e0, e1 = tr[i - 1]["tr_etf_px"], tr[i]["tr_etf_px"]
        if u0 > 0 and u1 > 0 and e0 > 0 and e1 > 0:
            lr_e = abs(math.log(e1 / e0))
            lr_u = abs(math.log(u1 / u0))
            if d0 in skip_dates and lr_e > 0.35 and lr_u < 0.15:
                continue
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
        joint = [r for r in rows if _metrics_row_has_usable_prices(r)]
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


def load_realized_pair_gross_60d_from_metrics(
    metrics_path: Path,
    universe_symbols: set[str],
    *,
    corp_actions_path: Path | None = None,
    beta_by_symbol: dict[str, float] | None = None,
    borrow_by_symbol: dict[str, float] | None = None,
    min_obs: int = 2,
) -> dict[str, dict[str, Any]]:
    """Build per-symbol 60d gross pair decay from joint ETF metrics rows."""
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
        joint = [r for r in rows if _metrics_row_has_usable_prices(r)]
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
        borrow = (borrow_by_symbol or {}).get(sym_u)
        events = parse_split_events_from_corp(corp_payload, sym_u)
        result = compute_realized_pair_gross_60d(
            joint,
            float(beta),
            events,
            borrow_annual=borrow,
            min_obs=min_obs,
        )
        if result:
            out[sym_u] = result
    return out

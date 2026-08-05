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
    find_underlying_adj_cliffs,
    parse_split_events_from_corp,
    resolve_split_context,
    sanitize_fabricated_adj_basis,
)

TRADING_DAYS = 252
DEFAULT_MIN_OBS = 40
# Below full min_obs but still enough to publish a best-estimate annualized
# mean daily log-drag (noisier; tagged quality=partial / source=*_partial).
PARTIAL_MIN_OBS = 10
REALIZED_PAIR_GROSS_20D_HORIZON = 20
MAX_CONTIGUOUS_METRICS_GAP_DAYS = 45
HARD_LIFECYCLE_GAP_DAYS = 365
# Skip pair-drag across holes larger than a long weekend. Carry-forward rows are
# already dropped upstream; without this, May29→Jun16-style stitches count as one "day".
MAX_PAIR_DRAG_GAP_DAYS = 5
# Skip daily pair-drag when one leg jumps and the other is flat — bad underlying
# backfill (HON ~2× discontinuity) or pre-split ETF prints (VRTL adj vs close).
ORPHAN_LEG_LOG_THRESHOLD = 0.35
ORPHAN_LEG_COMPANION_MAX = 0.15
# Directional plausibility. The orphan test above only fires when one leg is flat,
# so it misses the case where BOTH legs move but the ETF goes the wrong way
# relative to beta * underlying — an inverse ETF rising on an up day, say. That is
# economically impossible for a leveraged/inverse wrapper on a real session, so a
# hit means an unadjusted corporate action or a bad print, not tracking error.
DIRECTION_MIN_UNDERLYING_LOG = 0.02
DIRECTION_MIN_GAP_LOG = 0.30
# A violating day may only be dropped when the pair demonstrably tracks. On a pair
# whose series never tracked, dropping flagged days does not recover a true value,
# it manufactures a confident wrong one. Untracked pairs get suppressed instead.
PAIR_TRACK_MIN_R2 = 0.90
PAIR_TRACK_MAX_BETA_DEVIATION = 0.30
PAIR_TRACK_MIN_OBS = 30
# Rebuilding more than this much of a window is no longer a correction.
PAIR_MAX_EXCLUDED_IN_HORIZON = 2
# |log-drag| above this with near-perfect simple tracking ⇒ convexity_day flag.
CONVEXITY_DRAG_LOG_THRESHOLD = 0.35
CONVEXITY_SIMPLE_TRACK_EPS = 0.02
# Canonical period engine: log-drag (endpoint identity). Keep in sync with
# assets/realized_decay.js::PAIR_DRAG_BASIS.
PAIR_DRAG_BASIS = "beta_log_minus_etf_log"


def _is_orphan_leg_jump(r_u: float, r_l: float) -> bool:
    if not math.isfinite(r_u) or not math.isfinite(r_l):
        return False
    if abs(r_u) > ORPHAN_LEG_LOG_THRESHOLD and abs(r_l) < ORPHAN_LEG_COMPANION_MAX:
        return True
    if abs(r_l) > ORPHAN_LEG_LOG_THRESHOLD and abs(r_u) < ORPHAN_LEG_COMPANION_MAX:
        return True
    return False


def _is_direction_violation(beta: float, r_u: float, r_l: float) -> bool:
    """True when the ETF moved the wrong way versus ``beta * underlying``.

    Requires a meaningful underlying move, so noise around a flat tape cannot trip
    it, and a large gap to the expected leg return.
    """
    try:
        vals = (float(beta), float(r_u), float(r_l))
    except (TypeError, ValueError):
        return False
    if not all(math.isfinite(v) for v in vals):
        return False
    b, u, l = vals
    if b == 0 or abs(u) < DIRECTION_MIN_UNDERLYING_LOG:
        return False
    expected = b * u
    if expected * l >= 0:  # same side (or one leg exactly flat) -> fine
        return False
    return abs(l - expected) > DIRECTION_MIN_GAP_LOG


def compute_pair_track_quality(
    log_returns: list[tuple[float, float]],
    beta: float,
) -> dict[str, Any]:
    """Empirical beta and R² of ETF log-returns on underlying log-returns.

    ``log_returns`` is a list of ``(r_underlying, r_etf)`` pairs. Answers "does this
    pair actually track?", which is the precondition for trusting any per-day
    outlier judgement. A 2x/3x wrapper on its own index should sit near R² 1.0.
    """
    out: dict[str, Any] = {
        "n_obs": len(log_returns),
        "beta_empirical": None,
        "r2": None,
        "tracks_well": False,
        "reason": None,
    }
    if len(log_returns) < PAIR_TRACK_MIN_OBS:
        out["reason"] = "insufficient_obs"
        return out
    sxx = sum(x * x for x, _ in log_returns)
    if sxx <= 0:
        out["reason"] = "no_underlying_variance"
        return out
    ss_tot = sum(y * y for _, y in log_returns)
    if ss_tot <= 0:
        out["reason"] = "no_etf_variance"
        return out
    b_emp = sum(x * y for x, y in log_returns) / sxx
    ss_res = sum((y - b_emp * x) ** 2 for x, y in log_returns)
    r2 = 1.0 - (ss_res / ss_tot)
    out["beta_empirical"] = round(float(b_emp), 6)
    out["r2"] = round(float(r2), 6)
    if r2 < PAIR_TRACK_MIN_R2:
        out["reason"] = "low_r2"
    elif not math.isfinite(beta) or abs(b_emp - float(beta)) > PAIR_TRACK_MAX_BETA_DEVIATION:
        out["reason"] = "beta_deviation"
    else:
        out["tracks_well"] = True
        out["reason"] = "ok"
    return out


def _is_convexity_day(beta: float, r_u_simple: float, r_l_simple: float, drag_log: float) -> bool:
    if not math.isfinite(beta) or not math.isfinite(drag_log):
        return False
    if abs(drag_log) < CONVEXITY_DRAG_LOG_THRESHOLD:
        return False
    track_err = abs(r_l_simple - beta * r_u_simple)
    return math.isfinite(track_err) and track_err < CONVEXITY_SIMPLE_TRACK_EPS


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
        # Lifecycle / ticker-reuse: cut on huge gaps always, or mid-size gaps when the
        # feed source flips. Shorter post-carry-forward stitches are skipped in
        # build_daily_log_drag_series (MAX_PAIR_DRAG_GAP_DAYS) without discarding history.
        if gap > HARD_LIFECYCLE_GAP_DAYS or (gap > max_gap and source_changed):
            start_idx = i
    return [r for _d, r in dated[start_idx:]]


class _DragSeries(list):
    """list subclass so diagnostics can live on ``._meta`` (builtins disallow attrs)."""

    _meta: dict[str, Any]


def build_daily_log_drag_series(
    tr_rows: list[dict[str, Any]],
    beta: float,
    *,
    max_gap_days: int = MAX_PAIR_DRAG_GAP_DAYS,
) -> list[dict[str, Any]]:
    """Daily log-drag: beta * log(U_t/U_{t-1}) - log(L_t/L_{t-1}) on split-aware TR.

    Calendar gaps larger than ``max_gap_days`` are skipped so carry-forward deserts
    do not count as a single trading day. The returned list has ``._meta`` with
    ``skipped_gaps`` / ``convexity_days``.
    """
    result = build_daily_log_drag_series_with_meta(tr_rows, beta, max_gap_days=max_gap_days)
    series = _DragSeries(result["series"])
    series._meta = result["meta"]
    return series


def build_daily_log_drag_series_with_meta(
    tr_rows: list[dict[str, Any]],
    beta: float,
    *,
    max_gap_days: int = MAX_PAIR_DRAG_GAP_DAYS,
) -> dict[str, Any]:
    meta: dict[str, Any] = {
        "skipped_gaps": [],
        "convexity_days": [],
        # Every directional violation seen, whether or not it was dropped.
        "direction_violations": [],
        # The subset actually excluded from the series (well-tracking pairs only).
        "direction_violations_excluded": [],
        "pair_track": None,
        "pair_drag_basis": PAIR_DRAG_BASIS,
    }
    if not math.isfinite(beta):
        return {"series": [], "meta": meta}
    max_gap = max(1, int(max_gap_days))
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
        return {"series": [], "meta": meta}

    # First pass: does this pair track at all? Only a tracking pair earns the right
    # to have individual days judged as bad prints and dropped. Measured on the
    # NON-violating days, because a single impossible print is itself a massive
    # regression outlier and including it collapses the very statistic used to
    # judge it. Judging on clean days separates "one bad print on a good pair"
    # (correctable) from "this pair never tracked" (suppress).
    track_returns: list[tuple[float, float]] = []
    for i in range(1, len(clean)):
        u0, u1 = clean[i - 1]["und_px"], clean[i]["und_px"]
        e0, e1 = clean[i - 1]["etf_px"], clean[i]["etf_px"]
        if u0 <= 0 or e0 <= 0:
            continue
        r_u_t, r_l_t = math.log(u1 / u0), math.log(e1 / e0)
        if not (math.isfinite(r_u_t) and math.isfinite(r_l_t)):
            continue
        if _is_direction_violation(beta, r_u_t, r_l_t):
            continue
        track_returns.append((r_u_t, r_l_t))
    track = compute_pair_track_quality(track_returns, beta)
    meta["pair_track"] = track
    may_exclude = bool(track.get("tracks_well"))

    out: list[dict[str, Any]] = []
    for i in range(1, len(clean)):
        d0 = _parse_iso_date(clean[i - 1]["date"])
        d1 = _parse_iso_date(clean[i]["date"])
        if d0 is not None and d1 is not None and (d1 - d0).days > max_gap:
            meta["skipped_gaps"].append(
                {
                    "from": clean[i - 1]["date"],
                    "to": clean[i]["date"],
                    "calendar_gap": (d1 - d0).days,
                }
            )
            continue
        u0, u1 = clean[i - 1]["und_px"], clean[i]["und_px"]
        e0, e1 = clean[i - 1]["etf_px"], clean[i]["etf_px"]
        r_u = math.log(u1 / u0)
        r_l = math.log(e1 / e0)
        if not math.isfinite(r_u) or not math.isfinite(r_l):
            continue
        if _is_orphan_leg_jump(r_u, r_l):
            continue
        if _is_direction_violation(beta, r_u, r_l):
            violation = {
                "date": clean[i]["date"],
                "beta": float(beta),
                "r_underlying_log": r_u,
                "r_etf_log": r_l,
                "expected_etf_log": float(beta) * r_u,
                "gap_log": r_l - float(beta) * r_u,
                "drag_log": float(beta) * r_u - r_l,
                "excluded": may_exclude,
                "pair_track_reason": track.get("reason"),
            }
            meta["direction_violations"].append(violation)
            if may_exclude:
                # Tracking pair, impossible day -> a bad print or an unadjusted
                # corporate action. Drop it.
                meta["direction_violations_excluded"].append(violation)
                continue
            # Untracked pair -> the day is not separable from the pair's own
            # unreliability. Keep it and let the caller suppress the metric.
        r_u_simple = u1 / u0 - 1.0
        r_l_simple = e1 / e0 - 1.0
        drag = beta * r_u - r_l
        if not math.isfinite(drag):
            continue
        convexity = _is_convexity_day(beta, r_u_simple, r_l_simple, drag)
        if convexity:
            meta["convexity_days"].append(
                {
                    "date": clean[i]["date"],
                    "drag": drag,
                    "r_u_simple": r_u_simple,
                    "r_l_simple": r_l_simple,
                    "simple_track_err": r_l_simple - beta * r_u_simple,
                }
            )
        out.append(
            {
                "date": clean[i]["date"],
                "drag": drag,
                "simple_pnl": beta * r_u_simple - r_l_simple,
                "convexity_day": convexity,
                "etf_px": e1,
                "und_px": u1,
                "etf_px_prev": e0,
                "und_px_prev": u0,
            }
        )
    return {"series": out, "meta": meta}


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
            "convexity_days": 0,
            "convexity_drag_log": 0.0,
        }
    gross_log = float(sum(drag_slice))
    borrow_log = _period_borrow_log(borrow_annual, obs)
    net_log = gross_log - borrow_log
    start_row = daily_series[start_idx] if 0 <= start_idx < len(daily_series) else {}
    end_row = daily_series[end_idx] if 0 <= end_idx < len(daily_series) else {}
    window_rows = daily_series[start_idx : end_idx + 1]
    convexity_rows = [r for r in window_rows if r.get("convexity_day")]
    convexity_drag = float(sum(float(r.get("drag") or 0) for r in convexity_rows))
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
        "convexity_days": len(convexity_rows),
        "convexity_drag_log": convexity_drag,
    }


def compute_horizon_period_returns(
    daily_series: list[dict[str, Any]],
    horizons: list[int] | None = None,
    borrow_annual: float | None = None,
) -> dict[str, Any]:
    """Mirror assets/realized_decay.js::computeHorizonPeriodReturns."""
    hs = horizons or [REALIZED_PAIR_GROSS_20D_HORIZON]
    series = daily_series or []
    drags = [float(x["drag"]) for x in series]
    n = len(series)
    end_date = str(series[n - 1]["date"])[:10] if n else None
    meta = getattr(series, "_meta", None) or {}
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
    return {
        "horizons": rows,
        "n_days": n,
        "end_date": end_date,
        "borrow_annual": borrow_annual,
        "pair_drag_basis": PAIR_DRAG_BASIS,
        "skipped_gaps": meta.get("skipped_gaps") or [],
        "convexity_days": meta.get("convexity_days") or [],
    }


def collapse_partial_horizons(horizon_result: dict[str, Any] | None) -> dict[str, Any]:
    """Collapse duplicate partial longer horizons into one available-history row."""
    if not horizon_result:
        return {"horizons": [], "n_days": 0}
    rows = list(horizon_result.get("horizons") or [])
    if not rows:
        return horizon_result
    full = [h for h in rows if h.get("sufficient")]
    partial = [h for h in rows if not h.get("sufficient") and int(h.get("obs") or 0) > 0]
    if not partial:
        return horizon_result
    partial.sort(key=lambda h: int(h.get("horizon_days") or 0))
    rep = {**partial[0], "available_history": True}
    out = full + [rep]
    out.sort(key=lambda h: int(h.get("horizon_days") or 0))
    return {**horizon_result, "horizons": out, "collapsed_partials": len(partial)}


def _normalize_horizon_row(horizon_row: dict[str, Any]) -> dict[str, Any]:
    """Accept Decay-tab snake_case and FoF camelCase horizon payloads."""
    if horizon_row.get("gross_simple") is not None:
        return horizon_row
    gross_simple = horizon_row.get("grossSimple")
    if gross_simple is None:
        return horizon_row
    gross_log = horizon_row.get("gross_log")
    if gross_log is None:
        gross_log = horizon_row.get("grossLog")
    net_simple = horizon_row.get("net_simple")
    if net_simple is None:
        net_simple = horizon_row.get("netSimple")
    return {
        **horizon_row,
        "gross_simple": gross_simple,
        "gross_log": gross_log,
        "net_simple": net_simple,
        "obs": horizon_row.get("obs") if horizon_row.get("obs") is not None else horizon_row.get("days"),
        "start_date": horizon_row.get("start_date") or horizon_row.get("startDate"),
        "end_date": horizon_row.get("end_date") or horizon_row.get("endDate"),
    }


def realized_pair_gross_20d_fields(
    horizon_row: dict[str, Any] | None,
    *,
    source: str = "etf_metrics_daily",
) -> dict[str, Any]:
    if not horizon_row:
        return {}
    horizon_row = _normalize_horizon_row(horizon_row)
    if horizon_row.get("gross_simple") is None:
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
        "realized_pair_gross_20d_obs": int(horizon_row.get("obs") or 0),
        "realized_pair_gross_20d_sufficient": sufficient,
        "realized_pair_gross_20d_start_date": horizon_row.get("start_date"),
        "realized_pair_gross_20d_end_date": horizon_row.get("end_date"),
        "realized_pair_gross_20d_source": source,
    }
    net_simple = horizon_row.get("net_simple")
    if sufficient:
        out["realized_pair_gross_20d"] = round(float(gross_simple), 6)
        out["realized_pair_gross_20d_log"] = gross_log
        if net_simple is not None and math.isfinite(float(net_simple)):
            out["realized_pair_net_20d"] = round(float(net_simple), 6)
    else:
        out["realized_pair_gross_partial"] = round(float(gross_simple), 6)
        out["realized_pair_gross_partial_log"] = gross_log
        out["realized_pair_gross_partial_horizon_days"] = REALIZED_PAIR_GROSS_20D_HORIZON
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


def compute_realized_pair_gross_20d(
    rows: list[dict[str, Any]],
    beta: float,
    split_events: list[tuple[dt.date, float]] | None = None,
    *,
    underlying_split_events: list[tuple[dt.date, float]] | None = None,
    borrow_annual: float | None = None,
    min_obs: int = 2,
) -> dict[str, Any] | None:
    """20 trading-day gross pair decay from joint metrics rows (main-grid headline)."""
    if not math.isfinite(beta):
        return None
    usable_rows = [r for r in rows if _metrics_row_has_usable_prices(r)]
    usable_rows = latest_contiguous_metrics_segment(usable_rows)
    tr = build_tr_series_from_metrics(
        usable_rows,
        split_events or [],
        underlying_split_events=underlying_split_events or [],
    )
    daily = build_daily_log_drag_series(tr, float(beta))
    if len(daily) < min_obs:
        return None
    result = compute_horizon_period_returns(
        daily,
        horizons=[REALIZED_PAIR_GROSS_20D_HORIZON],
        borrow_annual=borrow_annual,
    )
    h20 = next(
        (h for h in result.get("horizons") or [] if int(h.get("horizon_days") or 0) == REALIZED_PAIR_GROSS_20D_HORIZON),
        None,
    )
    if not h20:
        return None
    fields = realized_pair_gross_20d_fields(h20, source="etf_metrics_daily")
    if not fields:
        return None
    fields["n_days"] = result.get("n_days")

    # Pair-track provenance travels with the number so the dashboard can show why
    # a value is trustworthy, corrected, or withheld.
    drag_meta = getattr(daily, "_meta", {}) or {}
    track = drag_meta.get("pair_track") or {}
    fields["pair_track_r2"] = track.get("r2")
    fields["pair_beta_empirical"] = track.get("beta_empirical")
    fields["pair_track_reason"] = track.get("reason")
    fields["pair_tracks_well"] = bool(track.get("tracks_well"))

    excluded = drag_meta.get("direction_violations_excluded") or []
    violations = drag_meta.get("direction_violations") or []
    start = str(fields.get("realized_pair_gross_20d_start_date") or "")
    end = str(fields.get("realized_pair_gross_20d_end_date") or "")

    def _in_window(v: dict[str, Any]) -> bool:
        d = str(v.get("date") or "")
        return bool(start and end and start <= d <= end)

    excluded_in_window = [v for v in excluded if _in_window(v)]
    fields["realized_pair_excluded_days"] = len(excluded_in_window)
    if excluded_in_window:
        fields["realized_pair_excluded_dates"] = [v["date"] for v in excluded_in_window]

    # Rebuilding too much of a 20-day window is not a correction any more.
    if len(excluded_in_window) > PAIR_MAX_EXCLUDED_IN_HORIZON:
        fields["pair_untracked"] = True
        fields["pair_suppress_reason"] = "too_many_excluded_days"
        fields["suppressed"] = True
        return fields

    # An untracked pair that also shows impossible days has no defensible realized
    # figure: withhold rather than publish a number we cannot stand behind.
    if not track.get("tracks_well") and any(_in_window(v) for v in violations):
        fields["pair_untracked"] = True
        fields["pair_suppress_reason"] = track.get("reason") or "pair_untracked"
        fields["suppressed"] = True
    return fields


def annualize_log_drag_mean(drags: list[float]) -> float | None:
    """Annualize mean daily log-drag (short-favorable +)."""
    clean = [float(x) for x in (drags or []) if x is not None and math.isfinite(float(x))]
    if not clean:
        return None
    return float(sum(clean) / len(clean) * TRADING_DAYS)


def annualize_period_log_drag(gross_log: float, obs_days: int) -> float | None:
    """Scale a period Σ log-drag to a 252d rate: gross_log × (252 / N)."""
    if not math.isfinite(float(gross_log)):
        return None
    n = int(obs_days)
    if n < PARTIAL_MIN_OBS:
        return None
    return float(gross_log) * (TRADING_DAYS / float(n))


def best_estimate_gross_from_realized_pair_fields(
    fields: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Fallback annualized gross from 20d (or partial) period log-drag fields."""
    if not fields or fields.get("suppressed"):
        return None
    if fields.get("realized_pair_gross_20d_log") is not None:
        obs = int(fields.get("realized_pair_gross_20d_obs") or 0)
        gross_log = float(fields["realized_pair_gross_20d_log"])
        source = "annualized_from_20d_period"
    elif fields.get("realized_pair_gross_partial_log") is not None:
        obs = int(fields.get("realized_pair_gross_20d_obs") or 0)
        gross_log = float(fields["realized_pair_gross_partial_log"])
        source = "annualized_from_partial_period"
    else:
        return None
    annual = annualize_period_log_drag(gross_log, obs)
    if annual is None:
        return None
    return {
        "gross_decay_annual": round(annual, 6),
        "n_obs": obs,
        "quality": "partial",
        "gross_decay_annual_source": source,
        "start_date": fields.get("realized_pair_gross_20d_start_date"),
        "end_date": fields.get("realized_pair_gross_20d_end_date"),
        "pair_drag_basis": PAIR_DRAG_BASIS,
    }


def compute_gross_decay_annual(
    rows: list[dict[str, Any]],
    beta: float,
    split_events: list[tuple[dt.date, float]] | None = None,
    *,
    underlying_split_events: list[tuple[dt.date, float]] | None = None,
    min_obs: int = DEFAULT_MIN_OBS,
    partial_min_obs: int = PARTIAL_MIN_OBS,
) -> dict[str, Any] | None:
    """Mean daily log-drag annualized: beta * log(R_u) - log(R_etf) on split-aware TR.

    When ``partial_min_obs <= n_obs < min_obs``, still returns an annualized
    best estimate with ``quality="partial"`` (noisier short panel).
    """
    if not math.isfinite(beta):
        return None
    full_min = max(1, int(min_obs))
    part_min = max(1, min(int(partial_min_obs), full_min))
    rows = latest_contiguous_metrics_segment(
        [r for r in rows if _metrics_row_has_usable_prices(r)]
    )
    rows = sanitize_fabricated_adj_basis(rows, split_events or [])
    tr = build_tr_series_from_metrics(
        rows,
        split_events or [],
        underlying_split_events=underlying_split_events or [],
    )
    if len(tr) < part_min + 1:
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

    daily = build_daily_log_drag_series(tr, float(beta))
    drags: list[float] = []
    for day in daily:
        d0 = _parse_iso_date(day.get("date"))
        if d0 is not None and d0 in skip_dates:
            # Near known split boundaries, only drop orphan-sized ETF cliffs.
            e0 = float(day.get("etf_px_prev") or 0)
            e1 = float(day.get("etf_px") or 0)
            u0 = float(day.get("und_px_prev") or 0)
            u1 = float(day.get("und_px") or 0)
            if e0 > 0 and e1 > 0 and u0 > 0 and u1 > 0:
                lr_e = abs(math.log(e1 / e0))
                lr_u = abs(math.log(u1 / u0))
                if lr_e > 0.35 and lr_u < 0.15:
                    continue
        drag = day.get("drag")
        if drag is None or not math.isfinite(float(drag)):
            continue
        drags.append(float(drag))
    if len(drags) < part_min:
        return None
    mean_ann = annualize_log_drag_mean(drags)
    if mean_ann is None:
        return None
    quality = "full" if len(drags) >= full_min else "partial"
    return {
        "gross_decay_annual": round(mean_ann, 6),
        "n_obs": len(drags),
        "quality": quality,
        "gross_decay_annual_source": (
            "etf_metrics_daily" if quality == "full" else "etf_metrics_daily_partial"
        ),
        "start_date": daily[0]["date"] if daily else None,
        "end_date": daily[-1]["date"] if daily else None,
        "pair_drag_basis": PAIR_DRAG_BASIS,
    }


def load_gross_decay_from_metrics(
    metrics_path: Path,
    universe_symbols: set[str],
    *,
    corp_actions_path: Path | None = None,
    beta_by_symbol: dict[str, float] | None = None,
    underlying_by_symbol: dict[str, str] | None = None,
    min_obs: int = DEFAULT_MIN_OBS,
    partial_min_obs: int = PARTIAL_MIN_OBS,
) -> dict[str, dict[str, Any]]:
    """Build per-symbol realized gross decay from joint ETF metrics rows."""
    if not metrics_path.exists():
        return {}
    corp_path = corp_actions_path or Path(__file__).resolve().parent.parent / "data" / "corporate_actions.json"
    corp_payload: dict = {"events": []}
    if corp_path.exists():
        corp_payload = json.loads(corp_path.read_text(encoding="utf-8"))

    if metrics_path.suffix.lower() == ".parquet":
        df = pd.read_parquet(metrics_path)
    else:
        df = pd.read_csv(metrics_path)
    df["date"] = df["date"].astype(str).str[:10]
    df["ticker"] = df["ticker"].astype(str).str.upper()
    out: dict[str, dict[str, Any]] = {}
    part_min = max(1, min(int(partial_min_obs), int(min_obs)))

    for sym in sorted(universe_symbols):
        sym_u = str(sym or "").strip().upper()
        if not sym_u:
            continue
        sub = df[df["ticker"] == sym_u].sort_values("date")
        if sub.empty:
            continue
        rows = sub.to_dict("records")
        joint = [r for r in rows if _metrics_row_has_usable_prices(r)]
        if len(joint) < part_min + 1:
            continue
        # Corporate-action/cliff checks must use the same current lifecycle as
        # the realized calculation.  Reused tickers can have an unrelated old
        # Yahoo segment years before the current fund; inspecting both segments
        # falsely suppresses an otherwise clean current 20-day window.
        joint_current = latest_contiguous_metrics_segment(joint)
        beta = (beta_by_symbol or {}).get(sym_u)
        if beta is None:
            try:
                beta = float(sub.iloc[-1].get("delta") or sub.iloc[-1].get("Delta") or float("nan"))
            except (TypeError, ValueError):
                beta = float("nan")
        if not math.isfinite(float(beta)):
            continue
        und_sym = str((underlying_by_symbol or {}).get(sym_u) or "").strip().upper()
        etf_events = parse_split_events_from_corp(corp_payload, sym_u)
        und_events = parse_split_events_from_corp(corp_payload, und_sym) if und_sym else []
        und_cliff_rows = [
            {"date": r.get("date"), "underlying_adj_close": r.get("underlying_adj_close")}
            for r in joint_current
        ]
        und_cliffs = find_underlying_adj_cliffs(und_cliff_rows, und_events) if und_sym else []
        result = compute_gross_decay_annual(
            joint_current,
            float(beta),
            etf_events,
            underlying_split_events=und_events,
            min_obs=min_obs,
            partial_min_obs=part_min,
        )
        if result:
            result["source"] = result.get("gross_decay_annual_source") or "etf_metrics_daily"
            if und_cliffs:
                result["underlying_split_suspect"] = True
                result["underlying_split_cliff_dates"] = [
                    str(c["date"]) for c in und_cliffs[:4]
                ]
            out[sym_u] = result
    return out


def load_realized_pair_gross_20d_from_metrics(
    metrics_path: Path,
    universe_symbols: set[str],
    *,
    corp_actions_path: Path | None = None,
    beta_by_symbol: dict[str, float] | None = None,
    borrow_by_symbol: dict[str, float] | None = None,
    underlying_by_symbol: dict[str, str] | None = None,
    min_obs: int = 2,
) -> dict[str, dict[str, Any]]:
    """Build per-symbol 20d gross pair decay from joint ETF metrics rows."""
    if not metrics_path.exists():
        return {}
    corp_path = corp_actions_path or Path(__file__).resolve().parent.parent / "data" / "corporate_actions.json"
    corp_payload: dict = {"events": []}
    if corp_path.exists():
        corp_payload = json.loads(corp_path.read_text(encoding="utf-8"))

    if metrics_path.suffix.lower() == ".parquet":
        df = pd.read_parquet(metrics_path)
    else:
        df = pd.read_csv(metrics_path)
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
        joint_current = latest_contiguous_metrics_segment(joint)
        beta = (beta_by_symbol or {}).get(sym_u)
        if beta is None:
            try:
                beta = float(sub.iloc[-1].get("delta") or sub.iloc[-1].get("Delta") or float("nan"))
            except (TypeError, ValueError):
                beta = float("nan")
        if not math.isfinite(float(beta)):
            continue
        borrow = (borrow_by_symbol or {}).get(sym_u)
        und_sym = str((underlying_by_symbol or {}).get(sym_u) or "").strip().upper()
        etf_events = parse_split_events_from_corp(corp_payload, sym_u)
        und_events = parse_split_events_from_corp(corp_payload, und_sym) if und_sym else []
        und_cliff_rows = [
            {"date": r.get("date"), "underlying_adj_close": r.get("underlying_adj_close")}
            for r in joint_current
        ]
        und_cliffs = find_underlying_adj_cliffs(und_cliff_rows, und_events) if und_sym else []
        result = compute_realized_pair_gross_20d(
            joint_current,
            float(beta),
            etf_events,
            underlying_split_events=und_events,
            borrow_annual=borrow,
            min_obs=min_obs,
        )
        if result:
            if und_cliffs:
                result["underlying_split_suspect"] = True
                result["suppressed"] = True
            out[sym_u] = result
    return out

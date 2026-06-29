"""Split-aware total-return price series for ETF metrics rows (Python mirror of assets/price_basis.js)."""
from __future__ import annotations

import datetime as dt
import math
from typing import Any

from split_adjustments import (
    adj_basis_switch_tr_price,
    detect_adj_basis_switch_splits,
    detect_adj_boundary,
    detect_forward_normalized_splits,
    detect_staggered_discrete_splits,
    detect_staggered_reverse_adj_first,
    etf_adj_on_back_adjusted_forward_basis,
    etf_adj_on_forward_normalized_basis,
    etf_adj_on_post_split_basis,
    filter_splits_needing_close_basis_fix,
    match_split_to_price_jump,
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


FABRICATED_ADJ_MIN_LOG_DIVERGENCE = math.log(1.8)
FABRICATED_ADJ_EVENT_WINDOW_DAYS = 21
FABRICATED_ADJ_MULT_REL_TOL = 0.25


def find_fabricated_adj_cliffs(
    rows: list[dict[str, Any]],
    split_events: list[tuple[dt.date, float]] | None = None,
) -> list[dict[str, Any]]:
    """Dates where ``etf_adj_close`` jumps relative to close without a matching split.

    A mis-applied split adjustment (e.g. pre-split rows scaled up while post-split
    rows are scaled down) fabricates a return cliff in the adjusted series that the
    raw close series does not have. Legitimate back-adjustment also diverges from
    close — but only at a declared split date and only by that split's multiplier.
    Anything else is treated as corruption.
    """
    pts: list[tuple[dt.date, float, float]] = []
    for row in rows or []:
        ds = str(row.get("date") or "")[:10]
        if len(ds) != 10:
            continue
        try:
            d0 = dt.date.fromisoformat(ds)
            close = float(row.get("close_price") or row.get("nav") or 0)
            adj = float(row.get("etf_adj_close")) if row.get("etf_adj_close") is not None else float("nan")
        except (ValueError, TypeError):
            continue
        if close > 0 and math.isfinite(adj) and adj > 0:
            pts.append((d0, close, adj))
    pts.sort()
    if len(pts) < 2:
        return []
    events = [(d, float(m)) for d, m in (split_events or []) if m and float(m) > 0]
    cliffs: list[dict[str, Any]] = []
    for i in range(1, len(pts)):
        d_prev, c_prev, a_prev = pts[i - 1]
        d_cur, c_cur, a_cur = pts[i]
        adj_ret = math.log(a_cur / a_prev)
        close_ret = math.log(c_cur / c_prev)
        divergence = adj_ret - close_ret
        if abs(divergence) < FABRICATED_ADJ_MIN_LOG_DIVERGENCE:
            continue
        # Provider back-adjustment shows up as a close jump with a smooth adj
        # series — legitimate even when the split is missing from declared
        # events. Only the adjusted series carrying the jump is fabricated.
        if abs(adj_ret) <= abs(close_ret):
            continue
        factor = math.exp(abs(divergence))
        explained = False
        for eff, mult in events:
            if abs((d_cur - eff).days) > FABRICATED_ADJ_EVENT_WINDOW_DAYS:
                continue
            for trial in (mult, 1.0 / mult):
                if trial > 0 and abs(factor / max(trial, 1.0 / trial) - 1.0) <= FABRICATED_ADJ_MULT_REL_TOL:
                    explained = True
                    break
            if explained:
                break
        if not explained:
            cliffs.append({"date": d_cur, "factor": factor, "divergence": divergence})
    return cliffs


def sanitize_fabricated_adj_basis(
    rows: list[dict[str, Any]],
    split_events: list[tuple[dt.date, float]] | None = None,
) -> list[dict[str, Any]]:
    """Drop ``etf_adj_close`` for the whole ticker when it has fabricated cliffs.

    The raw close/nav series is continuous and economically meaningful in these
    cases, so falling back to it is strictly safer than consuming a corrupted
    adjusted basis.
    """
    if not find_fabricated_adj_cliffs(rows, split_events):
        return rows
    return [
        {**row, "etf_adj_close": None} if isinstance(row, dict) else row
        for row in rows
    ]


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
        matched = match_split_to_price_jump(cur / prev, split_mult)
        if matched is not None and abs(matched - split_mult) <= max(1e-6, 0.15 * abs(split_mult)):
            return points[i][0]
    return None


def resolve_split_context(
    close_points: list[tuple[dt.date, float]],
    split_events: list[tuple[dt.date, float]],
    metric_rows: list[dict[str, Any]] | None = None,
    adj_by_date: dict[dt.date, float] | None = None,
) -> dict[str, Any]:
    if not close_points or not split_events:
        return {"mode": "continuous", "boundary": None, "mult": None, "filtered": []}
    dated = [(d, c) for d, c in close_points]
    adj_map = adj_by_date or {}
    points3 = [
        (d, c, float(adj_map.get(d, c)))
        for d, c in dated
    ]

    staggered_adj_first = detect_staggered_reverse_adj_first(
        points3,
        close_points,
        split_events,
    )
    if staggered_adj_first:
        _eff, mult, adj_boundary, close_boundary = staggered_adj_first[0]
        return {
            "mode": "staggered_reverse_adj_first",
            "boundary": close_boundary,
            "adj_boundary": adj_boundary,
            "mult": mult,
            "filtered": staggered_adj_first,
            "variant": "staggered_reverse_adj_first",
        }

    staggered = detect_staggered_discrete_splits(
        points3,
        close_points,
        split_events,
    )
    if staggered:
        _eff, mult, boundary = staggered[0]
        return {
            "mode": "discrete_split",
            "boundary": boundary,
            "mult": mult,
            "filtered": staggered,
            "variant": "staggered_reverse",
        }

    fwd_norm = detect_forward_normalized_splits(points3, split_events)
    if fwd_norm:
        eff, mult, variant = fwd_norm[0]
        boundary = detect_split_boundary(close_points, mult) or eff
        return {
            "mode": "forward_normalized",
            "boundary": boundary,
            "mult": mult,
            "filtered": fwd_norm,
            "variant": variant,
        }

    filtered = filter_splits_needing_close_basis_fix(
        points3,
        split_events,
        metric_rows=metric_rows,
    )
    if filtered:
        mult = filtered[0][1]
        boundary = detect_split_boundary(close_points, mult)
        if boundary is None:
            boundary = filtered[0][0]
        return {
            "mode": "discrete_split",
            "boundary": boundary,
            "mult": mult,
            "filtered": filtered,
        }

    adj_switch = detect_adj_basis_switch_splits(
        points3,
        split_events,
        metric_rows=metric_rows,
    )
    if adj_switch:
        eff, mult, variant = adj_switch[0]
        boundary = detect_adj_boundary(points3, eff, mult) or eff
        mode = (
            "continuous_close_tr"
            if variant in {"reverse_continuous", "forward_continuous_close"}
            else "adj_basis_switch"
        )
        return {
            "mode": mode,
            "boundary": boundary,
            "mult": mult,
            "filtered": adj_switch,
            "variant": variant,
        }

    mult = None
    boundary = None
    if boundary is None:
        for _d, m in split_events:
            b = detect_split_boundary(close_points, m)
            if b is not None and abs((b - _d).days) <= 45:
                boundary, mult = b, m
                break
    mode = "discrete_split" if boundary and mult else "continuous"
    return {"mode": mode, "boundary": boundary, "mult": mult, "filtered": filtered}


def _row_date(ds: str) -> dt.date | None:
    try:
        return dt.date.fromisoformat(ds[:10])
    except ValueError:
        return None


def _is_pre_split(ds: str, ctx: dict[str, Any]) -> bool:
    boundary = ctx.get("boundary")
    mult = ctx.get("mult")
    mode = ctx.get("mode")
    if mode not in {"discrete_split", "adj_basis_switch", "continuous_close_tr", "forward_normalized"} or not boundary or not mult:
        return False
    d0 = _row_date(ds)
    if d0 is None:
        return False
    return d0 < boundary


def _tr_mode_for_row(row: dict[str, Any], ctx: dict[str, Any]) -> str:
    ds = str(row.get("date") or "")[:10]
    try:
        close = float(row.get("close_price") or row.get("nav") or 0)
    except (TypeError, ValueError):
        return "unknown"
    if close <= 0:
        return "unknown"
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
    pre_split = _is_pre_split(ds, ctx)
    mult = float(ctx.get("mult") or 0)
    mode = ctx.get("mode")

    if mode == "adj_basis_switch":
        return "pre_split_back_adj" if pre_split else "post_split_back_adj_mapped"
    if mode == "continuous_close_tr":
        return "continuous_close_tr"
    if mode == "staggered_reverse_adj_first":
        d0 = _row_date(ds)
        adj_boundary = ctx.get("adj_boundary")
        close_boundary = ctx.get("boundary")
        if d0 is None or not adj_boundary or not close_boundary:
            return "unknown"
        if d0 >= close_boundary:
            return "post_split_close"
        if d0 >= adj_boundary:
            return "pre_split_close_scaled"
        if math.isfinite(adj_f) and adj_f > 0:
            return "pre_split_adj_mapped"
        return "pre_split_close_scaled"
    if pre_split and mult > 0:
        if math.isfinite(adj_f) and adj_f > 0:
            if mult < 1.0 and etf_adj_on_back_adjusted_forward_basis(close, adj_f, mult):
                return "pre_split_back_adj"
            if etf_adj_on_post_split_basis(close, adj_f, mult):
                return "pre_split_adj_already_mapped"
            return "pre_split_adj_mapped"
        if math.isfinite(nav_f) and nav_f > 0 and close > 0 and mult > 1.05:
            if nav_f / close >= mult * 0.85 or nav_f / close > 2.5:
                return "pre_split_close_scaled"
            return "pre_split_nav_tr_scaled"
        if math.isfinite(nav_f) and nav_f > 0:
            return "pre_split_nav_tr_scaled"
        return "pre_split_close_scaled"
    if math.isfinite(adj_f) and adj_f > 0:
        return "post_split_adj" if mode == "discrete_split" else "continuous_adj"
    if math.isfinite(nav_f) and nav_f > 0:
        return "nav_tr_fallback"
    return "close_fallback"


def _forward_pre_split_tr_price(
    close: float,
    adj: float,
    mult: float,
    *,
    rel_tol: float = 0.15,
) -> float:
    """Map pre-split ETF row onto post-split TR basis for forward adj-basis-switch."""
    if not (math.isfinite(close) and close > 0 and math.isfinite(adj) and adj > 0 and 0 < mult < 1):
        return adj
    ratio = adj / close
    # Ingest-normalized pre-split (adj ≈ close × mult): use adj on both sides.
    if abs(ratio / mult - 1.0) <= rel_tol:
        return adj
    # Raw Yahoo pre-split (adj ≈ close × 1/mult): continuous raw close is the TR series.
    if abs(ratio * mult - 1.0) <= rel_tol or abs(ratio - 1.0 / mult) <= rel_tol:
        return close
    return adj


def _forward_post_split_tr_price(
    close: float,
    adj: float,
    mult: float,
    *,
    rel_tol: float = 0.15,
) -> float:
    if not (math.isfinite(close) and close > 0 and 0 < mult < 1):
        return close
    if math.isfinite(adj) and adj > 0:
        ratio = adj / close
        if abs(ratio / mult - 1.0) <= rel_tol:
            return adj
        if abs(ratio - 1.0) <= rel_tol:
            return adj_basis_switch_tr_price(close, mult)
    return adj_basis_switch_tr_price(close, mult)


def _adj_basis_switch_uses_flat_close(
    sorted_rows: list[dict[str, Any]],
    ctx: dict[str, Any],
) -> bool:
    """True when raw close is flat across the adj basis boundary (SNDU-style)."""
    boundary = ctx.get("boundary")
    if not boundary:
        return False
    pre_close = post_close = None
    for row in sorted_rows:
        ds = str(row.get("date") or "")[:10]
        if len(ds) != 10:
            continue
        try:
            d0 = dt.date.fromisoformat(ds)
            close = float(row.get("close_price") or row.get("nav") or 0)
        except (ValueError, TypeError):
            continue
        if close <= 0:
            continue
        if d0 < boundary:
            pre_close = close
        elif d0 >= boundary and post_close is None:
            post_close = close
            break
    if pre_close is None or post_close is None or pre_close <= 0:
        return False
    return abs(post_close / pre_close - 1.0) <= 0.02


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
    mode = ctx.get("mode")

    if mode == "forward_normalized":
        if _is_pre_split(ds, ctx) and math.isfinite(adj_f) and adj_f > 0:
            return adj_f
        return close

    if mode == "adj_basis_switch":
        if ctx.get("_flat_close_tr"):
            return close
        if _is_pre_split(ds, ctx):
            if math.isfinite(adj_f) and adj_f > 0:
                return _forward_pre_split_tr_price(close, adj_f, mult)
            return adj_basis_switch_tr_price(close, mult)
        if math.isfinite(adj_f) and adj_f > 0:
            return _forward_post_split_tr_price(close, adj_f, mult)
        return adj_basis_switch_tr_price(close, mult)

    if mode == "continuous_close_tr":
        return close

    if mode == "staggered_reverse_adj_first":
        d0 = _row_date(ds)
        adj_boundary = ctx.get("adj_boundary")
        close_boundary = ctx.get("boundary")
        if d0 is None or not adj_boundary or not close_boundary or mult <= 0:
            return close
        if d0 >= close_boundary:
            if (
                math.isfinite(adj_f)
                and adj_f > 0
                and abs(adj_f / close - 1.0) <= 0.15
            ):
                return adj_f
            return close
        if d0 >= adj_boundary:
            return close * mult
        if math.isfinite(adj_f) and adj_f > 0:
            if etf_adj_on_forward_normalized_basis(close, adj_f, mult):
                return close * mult
            if etf_adj_on_post_split_basis(close, adj_f, mult):
                return adj_f
            return adj_f * mult
        return close * mult

    if _is_pre_split(ds, ctx) and mult > 0:
        if math.isfinite(adj_f) and adj_f > 0:
            if mult < 1.0 and etf_adj_on_back_adjusted_forward_basis(close, adj_f, mult):
                return adj_f
            if etf_adj_on_post_split_basis(close, adj_f, mult):
                return adj_f
            return adj_f * mult
        if math.isfinite(nav_f) and nav_f > 0 and close > 0 and mult > 1.05:
            if nav_f / close >= mult * 0.85 or nav_f / close > 2.5:
                return close * mult
            return nav_f * mult
        if math.isfinite(nav_f) and nav_f > 0:
            return nav_f * mult
        return close * mult
    if (
        mode == "discrete_split"
        and mult > 1.05
        and math.isfinite(adj_f)
        and adj_f > 0
        and etf_adj_on_post_split_basis(close, adj_f, mult)
    ):
        # Transitional bar: close already on post-split basis, adj still back-adjusted.
        return close
    if (
        mode == "discrete_split"
        and mult > 1.05
        and math.isfinite(adj_f)
        and adj_f > 0
        and abs((adj_f / close) * mult - 1.0) <= 0.15
    ):
        # Reverse split post row wrongly forward-normalized (adj ≈ close / mult).
        return close
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
    sorted_rows = sanitize_fabricated_adj_basis(sorted_rows, split_events)
    close_pts = _close_points(sorted_rows)
    adj_by_date: dict[dt.date, float] = {}
    for row in sorted_rows:
        ds = str(row.get("date") or "")[:10]
        if len(ds) != 10:
            continue
        try:
            d0 = dt.date.fromisoformat(ds)
            adj = row.get("etf_adj_close")
            close = row.get("close_price") or row.get("nav")
            if adj is not None:
                adj_by_date[d0] = float(adj)
            elif close is not None:
                adj_by_date[d0] = float(close)
        except (ValueError, TypeError):
            continue
    ctx = resolve_split_context(close_pts, split_events, metric_rows=sorted_rows, adj_by_date=adj_by_date)
    if ctx.get("mode") == "adj_basis_switch":
        ctx["_flat_close_tr"] = _adj_basis_switch_uses_flat_close(sorted_rows, ctx)
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
                    "tr_mode": _tr_mode_for_row(row, ctx),
                }
            )
    repaired = _repair_split_outlier_bars(out, ctx, split_events)
    return _normalize_split_sized_basis_jumps(repaired, split_events)


def _known_split_multipliers(split_events: list[tuple[dt.date, float]]) -> list[float]:
    out: list[float] = []
    for _d, mult in split_events or []:
        try:
            m = float(mult)
        except (TypeError, ValueError):
            continue
        if m <= 0:
            continue
        cand = m if m >= 1 else 1.0 / m
        if cand <= 1.05:
            continue
        if not any(abs(cand / existing - 1.0) <= 1e-6 for existing in out):
            out.append(cand)
    return sorted(out)


def _match_known_split_jump(
    jump_abs: float,
    multipliers: list[float],
    *,
    rel_tol: float = 0.18,
) -> float | None:
    if not (math.isfinite(jump_abs) and jump_abs > 1.0):
        return None
    best: tuple[float, float] | None = None
    for mult in multipliers:
        err = abs(jump_abs / mult - 1.0)
        if err <= rel_tol and (best is None or err < best[0]):
            best = (err, mult)
    return best[1] if best else None


def _normalize_split_sized_basis_jumps(
    tr: list[dict[str, Any]],
    split_events: list[tuple[dt.date, float]],
    *,
    max_underlying_log_move: float = 0.25,
    min_etf_log_jump: float = 0.55,
) -> list[dict[str, Any]]:
    """Map provider-restated split-sized TR segments onto the latest price basis.

    Some metrics histories contain rows where one provider has already restated
    close/adjusted-close for an upcoming reverse split while neighboring rows
    are still on the old basis. Walk backward from the latest row and scale
    earlier segments whenever an adjacent ETF-only jump matches a known split
    multiplier. This repairs whole segments instead of interpolating away a
    single bad bar.
    """
    if len(tr) < 2:
        return tr
    multipliers = _known_split_multipliers(split_events)
    if not multipliers:
        return tr

    out = [dict(row) for row in tr]
    scale = 1.0
    adjusted: list[float | None] = [None] * len(out)
    adjusted[-1] = float(out[-1]["tr_etf_px"])
    mode_suffix: list[str] = [""] * len(out)

    for i in range(len(out) - 2, -1, -1):
        try:
            prev_raw = float(out[i]["tr_etf_px"])
            cur_adj = float(adjusted[i + 1])
            u0 = float(out[i]["tr_und_px"])
            u1 = float(out[i + 1]["tr_und_px"])
        except (TypeError, ValueError, KeyError):
            adjusted[i] = None
            continue
        if min(prev_raw, cur_adj, u0, u1) <= 0:
            adjusted[i] = prev_raw * scale if prev_raw > 0 else None
            continue

        prev_adj = prev_raw * scale
        lr_e = math.log(prev_adj / cur_adj)
        lr_u = abs(math.log(u1 / u0))
        if abs(lr_e) >= min_etf_log_jump and lr_u < max_underlying_log_move:
            matched = _match_known_split_jump(math.exp(abs(lr_e)), multipliers)
            if matched is not None:
                scale = scale / matched if lr_e > 0 else scale * matched
                prev_adj = prev_raw * scale
                mode_suffix[i] = f"|basis_jump_scaled({matched:g})"

        adjusted[i] = prev_adj

    for i, px in enumerate(adjusted):
        if px is None or not math.isfinite(px) or px <= 0:
            continue
        if abs(px / float(out[i]["tr_etf_px"]) - 1.0) > 1e-9:
            out[i]["tr_etf_px"] = px
            out[i]["tr_mode"] = f"{out[i].get('tr_mode') or 'unknown'}{mode_suffix[i] or '|basis_jump_scaled'}"
    return out


def _repair_split_outlier_bars(
    tr: list[dict[str, Any]],
    ctx: dict[str, Any],
    split_events: list[tuple[dt.date, float]],
) -> list[dict[str, Any]]:
    """Replace orphan post-split bad prints when ETF cliff is not mirrored in underlying."""
    boundary = ctx.get("boundary")
    if not boundary or len(tr) < 3:
        return tr
    bnd = boundary if isinstance(boundary, dt.date) else None
    if bnd is None:
        try:
            bnd = dt.date.fromisoformat(str(boundary)[:10])
        except ValueError:
            return tr
    repaired = list(tr)
    mode = ctx.get("mode")
    mult = float(ctx.get("mult") or 0)
    for i in range(1, len(repaired) - 1):
        try:
            d0 = dt.date.fromisoformat(str(repaired[i]["date"])[:10])
        except ValueError:
            continue
        if d0 < bnd or abs((d0 - bnd).days) > 5:
            continue
        e0, e1, u0, u1 = (
            float(repaired[i - 1]["tr_etf_px"]),
            float(repaired[i]["tr_etf_px"]),
            float(repaired[i - 1]["tr_und_px"]),
            float(repaired[i]["tr_und_px"]),
        )
        if min(e0, e1, u0, u1) <= 0:
            continue
        lr_e = abs(math.log(e1 / e0))
        lr_u = abs(math.log(u1 / u0))
        if mult > 0 and abs((d0 - bnd).days) <= 2:
            expected = abs(math.log(mult)) if mult >= 1 else abs(math.log(1.0 / mult))
            if abs(lr_e - expected) <= 0.2 or mode in {"forward_normalized", "continuous_close_tr"}:
                continue
        if lr_u >= 0.20 or lr_e < 0.55 or lr_e < lr_u + 0.18:
            continue
        prev_e = float(repaired[i - 1]["tr_etf_px"])
        nxt_e = float(repaired[i + 1]["tr_etf_px"])
        if prev_e > 0 and nxt_e > 0:
            repaired[i] = dict(repaired[i])
            repaired[i]["tr_etf_px"] = math.sqrt(prev_e * nxt_e)
    return repaired


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

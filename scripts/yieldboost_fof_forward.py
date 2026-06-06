"""FoF-native forward gross and net-edge bootstrap (YBTY/YBST)."""
from __future__ import annotations

import json
import math
from typing import Any

import numpy as np

from income_schedule import scenario_grid_put_spread_pair
from yieldboost_fof_constants import FOF_DEFAULT_EXPENSE_RATIO_ANNUAL

_TRADING_DAYS = 252
_SIGMA_MULTIPLIERS = (0.5, 0.7, 1.0, 1.3, 1.5)
_DRIFTS = (-0.50, -0.25, 0.00, 0.25, 0.50)
_HORIZON_YEARS = 1.0


def _norm_sym(s: object) -> str:
    return str(s or "").strip().upper().replace(".", "-")


def _cash_fraction(basket: dict[str, Any]) -> float:
    raw = basket.get("cash_pct")
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(v):
        return 0.0
    # Stored as percent (0–100), e.g. 33.37 for YBST.
    if v > 1.0:
        return min(1.0, max(0.0, v / 100.0))
    return min(1.0, max(0.0, v))


def weighted_child_pair_pnl_blend(
    basket: dict[str, Any],
    child_records: dict[str, dict[str, Any]],
) -> dict[str, float | None]:
    """Legacy v1: weighted child short-YB pair forward (diagnostic only)."""
    children = basket.get("children") or []
    if not children:
        return {"child_pair_p50_annual": None, "child_pair_p10_annual": None, "child_pair_p90_annual": None}
    wsum = sum(float(c.get("weight_pct") or 0) for c in children) or 100.0
    acc = {"child_pair_p50_annual": 0.0, "child_pair_p10_annual": 0.0, "child_pair_p90_annual": 0.0}
    got = {k: False for k in acc}
    for c in children:
        yb = _norm_sym(c.get("yb_etf"))
        w = float(c.get("weight_pct") or 0) / wsum
        if w <= 0:
            continue
        rec = child_records.get(yb) or {}
        for src, dst in (
            ("expected_pair_pnl_p50_annual", "child_pair_p50_annual"),
            ("expected_pair_pnl_p10_annual", "child_pair_p10_annual"),
            ("expected_pair_pnl_p90_annual", "child_pair_p90_annual"),
        ):
            v = rec.get(src)
            if v is None:
                continue
            try:
                fv = float(v)
                if math.isfinite(fv):
                    acc[dst] += w * fv
                    got[dst] = True
            except (TypeError, ValueError):
                pass
    return {k: (round(acc[k], 6) if got[k] else None) for k in acc}


def weighted_child_nav_decay_forward(
    basket: dict[str, Any],
    child_records: dict[str, dict[str, Any]],
    *,
    expense_ratio_annual: float = FOF_DEFAULT_EXPENSE_RATIO_ANNUAL,
    cash_return_annual: float = 0.0,
) -> dict[str, float | None]:
    """
    FoF forward gross from weighted child NAV structural decay (put-spread p50 band),
    minus FoF expense ratio and cash drag.
    """
    children = basket.get("children") or []
    if not children:
        return {
            "expected_gross_decay_p50_annual": None,
            "expected_gross_decay_p10_annual": None,
            "expected_gross_decay_p90_annual": None,
            "effective_beta": None,
            "cash_drag_annual": None,
            "expense_ratio_annual": expense_ratio_annual,
        }

    wsum = sum(float(c.get("weight_pct") or 0) for c in children) or 100.0
    cash_frac = _cash_fraction(basket)
    invested_frac = max(0.0, 1.0 - cash_frac)

    acc = {
        "expected_gross_decay_p50_annual": 0.0,
        "expected_gross_decay_p10_annual": 0.0,
        "expected_gross_decay_p90_annual": 0.0,
        "effective_beta": 0.0,
    }
    got = {k: False for k in acc}

    for c in children:
        yb = _norm_sym(c.get("yb_etf"))
        w = float(c.get("weight_pct") or 0) / wsum
        if w <= 0:
            continue
        rec = child_records.get(yb) or {}
        for key in (
            "expected_gross_decay_p50_annual",
            "expected_gross_decay_p10_annual",
            "expected_gross_decay_p90_annual",
        ):
            v = rec.get(key)
            if v is None:
                v = rec.get(key.replace("expected_gross_decay", "expected_pair_pnl"))
            if v is None:
                continue
            try:
                fv = float(v)
                if math.isfinite(fv):
                    short_key = key.replace("expected_gross_decay", "expected_gross_decay")
                    acc[key] += w * invested_frac * fv
                    got[key] = True
            except (TypeError, ValueError):
                pass
        d = rec.get("delta")
        if d is not None:
            try:
                fd = float(d)
                if math.isfinite(fd):
                    acc["effective_beta"] += w * invested_frac * fd
                    got["effective_beta"] = True
            except (TypeError, ValueError):
                pass

    # Cash earns cash_return; invested sleeve pays FoF ER.
    cash_drag = cash_frac * max(0.0, cash_return_annual)
    er = max(0.0, float(expense_ratio_annual))

    out: dict[str, float | None] = {
        "expense_ratio_annual": round(er, 6),
        "cash_drag_annual": round(cash_drag, 6),
        "cash_pct": round(cash_frac * 100.0, 4),
        "effective_invested_pct": round(invested_frac * 100.0, 4),
    }
    for key in ("expected_gross_decay_p50_annual", "expected_gross_decay_p10_annual", "expected_gross_decay_p90_annual"):
        if got[key]:
            out[key.replace("expected_gross_decay", "expected_gross_decay")] = round(
                acc[key] - er - cash_drag, 6
            )
        else:
            out[key] = None
    out["effective_beta"] = round(acc["effective_beta"], 6) if got["effective_beta"] else None
    p10 = out.get("expected_gross_decay_p10_annual")
    p90 = out.get("expected_gross_decay_p90_annual")
    p50 = out.get("expected_gross_decay_p50_annual")
    collapsed = (
        p10 is not None and p90 is not None and p50 is not None
        and abs(float(p90) - float(p10)) < 1e-9
    )
    out["forward_band_collapsed"] = collapsed or not (got["expected_gross_decay_p10_annual"] and got["expected_gross_decay_p90_annual"])
    return out


def weighted_child_forecast_vol(
    basket: dict[str, Any],
    child_records: dict[str, dict[str, Any]],
    *,
    basket_vol_fallback: float | None = None,
) -> dict[str, float | str | None]:
    """Weighted child ``forecast_vol_underlying_annual`` for FoF scenario σ."""
    children = basket.get("children") or []
    if not children:
        vol = basket_vol_fallback
        return {
            "forecast_vol_underlying_annual": round(vol, 6) if vol is not None and math.isfinite(vol) else None,
            "forecast_vol_source": "fof_basket_realized_60d" if vol is not None else None,
        }
    wsum = sum(float(c.get("weight_pct") or 0) for c in children) or 100.0
    acc = 0.0
    wgot = 0.0
    for c in children:
        yb = _norm_sym(c.get("yb_etf"))
        w = float(c.get("weight_pct") or 0) / wsum
        if w <= 0:
            continue
        rec = child_records.get(yb) or {}
        raw = rec.get("forecast_vol_underlying_annual")
        if raw is None:
            raw = rec.get("vol_underlying_annual")
        if raw is None:
            continue
        try:
            fv = float(raw)
            if math.isfinite(fv) and fv > 0:
                acc += w * fv
                wgot += w
        except (TypeError, ValueError):
            pass
    if wgot > 0:
        return {
            "forecast_vol_underlying_annual": round(acc / wgot, 6),
            "forecast_vol_source": "fof_basket_weighted_child_forecast",
            "forecast_vol_blend_weight_model": round(wgot, 4),
        }
    vol = basket_vol_fallback
    return {
        "forecast_vol_underlying_annual": round(vol, 6) if vol is not None and math.isfinite(vol) else None,
        "forecast_vol_source": "fof_basket_realized_60d" if vol is not None else None,
    }


def build_fof_income_distribution_calibration(
    basket: dict[str, Any],
    child_records: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    """Weighted rollup of child ``income_distribution_calibration`` blocks."""
    children = basket.get("children") or []
    if not children:
        return None
    wsum = sum(float(c.get("weight_pct") or 0) for c in children) or 100.0
    ratio_acc = 0.0
    median_acc = 0.0
    events_used = 0
    events_total = 0
    wgot = 0.0
    events_recent: list[dict[str, Any]] = []
    cross_ratios: list[float] = []
    for c in children:
        yb = _norm_sym(c.get("yb_etf"))
        w = float(c.get("weight_pct") or 0) / wsum
        if w <= 0:
            continue
        calib = (child_records.get(yb) or {}).get("income_distribution_calibration")
        if not calib:
            continue
        br = calib.get("blended_ratio_used")
        fm = calib.get("fund_ratio_median")
        if br is not None:
            try:
                fbr = float(br)
                if math.isfinite(fbr):
                    ratio_acc += w * fbr
                    wgot += w
            except (TypeError, ValueError):
                pass
        if fm is not None:
            try:
                ffm = float(fm)
                if math.isfinite(ffm):
                    median_acc += w * ffm
            except (TypeError, ValueError):
                pass
        events_used += int(calib.get("events_used") or 0)
        events_total += int(calib.get("events_total") or 0)
        cr = calib.get("cross_fund_ratio")
        if cr is not None:
            try:
                cross_ratios.append(float(cr))
            except (TypeError, ValueError):
                pass
        for ev in calib.get("events_recent") or []:
            if not isinstance(ev, dict):
                continue
            tagged = {**ev, "child_etf": yb, "child_weight_pct": round(w * 100.0, 2)}
            events_recent.append(tagged)
    if wgot <= 0 and not events_recent:
        return None
    events_recent.sort(key=lambda e: str(e.get("ex_date") or ""), reverse=True)
    events_recent = events_recent[:16]
    confidence = "high" if events_used >= 12 else ("medium" if events_used >= 6 else "low")
    blended = ratio_acc / wgot if wgot > 0 else None
    return {
        "events_used": events_used,
        "events_total": events_total,
        "fund_ratio_median": round(median_acc / wgot, 6) if wgot > 0 else None,
        "fund_ratio_p25": None,
        "fund_ratio_p75": None,
        "fund_ratio_confidence": confidence,
        "cross_fund_ratio": round(sum(cross_ratios) / len(cross_ratios), 6) if cross_ratios else None,
        "blended_ratio_used": round(blended, 6) if blended is not None else None,
        "cadence_label": "weekly",
        "periods_per_year": 52,
        "nav_missing_count": 0,
        "run_rate_annual_display": None,
        "template_yields": [],
        "events_recent": events_recent,
        "latest_event": events_recent[0] if events_recent else None,
        "current_sigma": None,
        "trailing_window_days": 365,
        "schema_version": 1,
        "source": "fof_weighted_child_rollup",
    }


def _child_scenario_grid_cell(
    rec: dict[str, Any],
    row_i: int,
    col_j: int,
) -> float | None:
    grid = rec.get("pair_scenario_grid")
    if isinstance(grid, dict) and isinstance(grid.get("p50_log_grid"), list):
        try:
            val = grid["p50_log_grid"][row_i][col_j]
            if val is not None and math.isfinite(float(val)):
                return float(val)
        except (IndexError, TypeError, ValueError):
            pass
    sigma = rec.get("forecast_vol_underlying_annual")
    if sigma is None:
        sigma = rec.get("vol_underlying_annual")
    beta = rec.get("delta")
    if sigma is None or beta is None:
        return None
    calib = rec.get("income_distribution_calibration") or {}
    cap = calib.get("blended_ratio_used")
    try:
        cap_f = float(cap) if cap is not None else 0.65
    except (TypeError, ValueError):
        cap_f = 0.65
    anchor = rec.get("expected_gross_decay_p50_annual")
    if anchor is None:
        anchor = rec.get("expected_pair_pnl_p50_annual")
    built = scenario_grid_put_spread_pair(
        sigma_annual=float(sigma),
        beta=float(beta),
        capture_ratio=cap_f,
        sigma_multipliers=_SIGMA_MULTIPLIERS,
        drifts=_DRIFTS,
        gross_anchor_p50=float(anchor) if anchor is not None else None,
    )
    if not built:
        return None
    try:
        val = built["p50_log_grid"][row_i][col_j]
        return float(val) if val is not None and math.isfinite(float(val)) else None
    except (IndexError, TypeError, ValueError):
        return None


def build_fof_pair_scenario_grid(
    basket: dict[str, Any],
    child_records: dict[str, dict[str, Any]],
    *,
    anchor_p50: float | None,
    expense_ratio_annual: float = FOF_DEFAULT_EXPENSE_RATIO_ANNUAL,
    cash_drag_annual: float = 0.0,
) -> dict[str, Any] | None:
    """5×5 basket-weighted child structural gross grid (schema_v=4 compatible)."""
    children = basket.get("children") or []
    if not children:
        return None
    wsum = sum(float(c.get("weight_pct") or 0) for c in children) or 100.0
    cash_frac = _cash_fraction(basket)
    invested_frac = max(0.0, 1.0 - cash_frac)
    er = max(0.0, float(expense_ratio_annual))
    cash_drag = max(0.0, float(cash_drag_annual))

    n_rows = len(_SIGMA_MULTIPLIERS)
    n_cols = len(_DRIFTS)
    grid: list[list[float | None]] = [[None] * n_cols for _ in range(n_rows)]
    und_grid: list[list[float | None]] = [[None] * n_cols for _ in range(n_rows)]

    for i in range(n_rows):
        for j in range(n_cols):
            acc = 0.0
            wgot = 0.0
            for c in children:
                yb = _norm_sym(c.get("yb_etf"))
                w = float(c.get("weight_pct") or 0) / wsum
                if w <= 0:
                    continue
                rec = child_records.get(yb) or {}
                cell = _child_scenario_grid_cell(rec, i, j)
                if cell is None:
                    continue
                acc += w * cell
                wgot += w
            if wgot > 0:
                gross = acc * invested_frac - er - cash_drag
                grid[i][j] = round(gross, 6)
            und_grid[i][j] = round(float(math.expm1(_DRIFTS[j] * _HORIZON_YEARS)), 6)

    center_i = _SIGMA_MULTIPLIERS.index(1.0)
    center_j = _DRIFTS.index(0.0)
    raw_center = grid[center_i][center_j]
    anchor_delta = 0.0
    if (
        anchor_p50 is not None
        and raw_center is not None
        and math.isfinite(float(anchor_p50))
        and math.isfinite(float(raw_center))
    ):
        anchor_delta = float(anchor_p50) - float(raw_center)
        grid = [
            [round(v + anchor_delta, 6) if v is not None else None for v in row]
            for row in grid
        ]

    if not any(v is not None for row in grid for v in row):
        return None

    return {
        "sigma_multipliers": list(_SIGMA_MULTIPLIERS),
        "drifts": list(_DRIFTS),
        "p50_log_grid": grid,
        "und_p50_simple_grid": und_grid,
        "borrow_annual": 0.0,
        "expense_ratio_annual": er,
        "n_paths_per_cell": 0,
        "axis": "log_continuous_annual",
        "basis": "fof_weighted_child_put_spread_gross",
        "engine": "fof_weighted_child_structural",
        "anchor_p50_annual": float(anchor_p50) if anchor_p50 is not None else None,
        "anchor_delta_annual": round(anchor_delta, 6),
        "cash_drag_annual": round(cash_drag, 6),
        "invested_fraction": round(invested_frac, 6),
        "horizon_years": _HORIZON_YEARS,
    }


def _band_sigma(p10: float | None, p90: float | None) -> float | None:
    if p10 is None or p90 is None:
        return None
    try:
        return abs(float(p90) - float(p10)) / (2.0 * 1.2816)
    except (TypeError, ValueError):
        return None


def bootstrap_fof_net_edge(
    daily_drags: list[float],
    *,
    borrow_annual: float | None,
    forward_p50: float | None,
    forward_p10: float | None = None,
    forward_p90: float | None = None,
    n_bootstrap: int = 2000,
    seed: int = 42,
) -> dict[str, Any]:
    """
    Block-bootstrap daily log-drags → gross fan; subtract FoF borrow once for net.
    Optional inverse-variance level shift toward forward p50 when band exists.
    """
    drags = [float(x) for x in daily_drags if math.isfinite(float(x))]
    if len(drags) < 5:
        return {}

    rng = np.random.default_rng(seed)
    arr = np.array(drags, dtype=float)
    mu_r_daily = float(np.mean(arr))
    mu_r_annual = mu_r_daily * _TRADING_DAYS
    sigma_r_daily = float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0
    sigma_r_annual = sigma_r_daily * math.sqrt(_TRADING_DAYS)

    mu_f_annual = (
        float(forward_p50)
        if forward_p50 is not None and math.isfinite(float(forward_p50))
        else mu_r_annual
    )
    sigma_f = _band_sigma(forward_p10, forward_p90)
    w_f = 0.0
    if sigma_f is not None and sigma_f > 1e-12 and sigma_r_annual > 1e-12:
        w_f = float(sigma_r_annual**2 / (sigma_f**2 + sigma_r_annual**2))
    elif sigma_f is not None and sigma_f <= 1e-12:
        w_f = 1.0
    posterior_mu = w_f * mu_f_annual + (1.0 - w_f) * mu_r_annual

    block = max(1, min(5, len(arr) // 10))
    gross_samples = []
    for _ in range(n_bootstrap):
        idxs = []
        while len(idxs) < len(arr):
            start = int(rng.integers(0, max(1, len(arr) - block + 1)))
            idxs.extend(range(start, min(start + block, len(arr))))
        idxs = idxs[: len(arr)]
        sample = arr[idxs]
        gross_samples.append(float(np.mean(sample)) * _TRADING_DAYS)

    gross_samples = np.array(gross_samples, dtype=float)
    shift = posterior_mu - float(np.mean(gross_samples))
    gross_samples = gross_samples + shift

    b = float(borrow_annual) if borrow_annual is not None and math.isfinite(borrow_annual) else 0.0
    net_samples = gross_samples - b

    def q(x, p):
        return float(np.quantile(x, p))

    gross_p50 = q(gross_samples, 0.5)
    hist_edges = np.linspace(q(gross_samples, 0.01), q(gross_samples, 0.99), 22)
    counts, _ = np.histogram(gross_samples, bins=hist_edges)
    centers = (hist_edges[:-1] + hist_edges[1:]) / 2.0

    if sigma_f is not None and sigma_f > 1e-12 and sigma_r_annual > 1e-12:
        blend_method = "inverse_variance"
    elif sigma_f is not None and sigma_f <= 1e-12:
        blend_method = "anchor_shift_fallback"
    else:
        blend_method = "anchor_shift_fallback"

    return {
        "net_edge_p05_annual": round(q(net_samples, 0.05), 6),
        "net_edge_p25_annual": round(q(net_samples, 0.25), 6),
        "net_edge_p50_annual": round(q(net_samples, 0.50), 6),
        "net_edge_p75_annual": round(q(net_samples, 0.75), 6),
        "net_edge_p95_annual": round(q(net_samples, 0.95), 6),
        "gross_realized_mean_annual": round(mu_r_annual, 6),
        "gross_anchor_target_annual": round(posterior_mu, 6),
        "gross_blend_weight_forward": round(w_f, 4) if math.isfinite(w_f) else None,
        "gross_sigma_realized_annual": round(sigma_r_annual, 6),
        "gross_sigma_forward_annual": round(sigma_f, 6) if sigma_f is not None else None,
        "gross_blend_method": blend_method,
        "gross_anchor_source": "fof_weighted_child_nav_decay",
        "net_edge_hist_json": json.dumps({
            "e": [round(float(x), 6) for x in centers],
            "c": [int(x) for x in counts],
        }),
    }

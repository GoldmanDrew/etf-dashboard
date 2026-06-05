"""FoF-native forward gross and net-edge bootstrap (YBTY/YBST)."""
from __future__ import annotations

import json
import math
from typing import Any

import numpy as np

from yieldboost_fof_constants import FOF_DEFAULT_EXPENSE_RATIO_ANNUAL

_TRADING_DAYS = 252


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
    mu_r = float(np.mean(arr))
    sigma_r = float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0

    mu_f = forward_p50 if forward_p50 is not None and math.isfinite(forward_p50) else mu_r
    sigma_f = _band_sigma(forward_p10, forward_p90)
    w_f = 0.0
    if sigma_f is not None and sigma_f > 1e-12 and sigma_r > 1e-12:
        w_f = float(sigma_r**2 / (sigma_f**2 + sigma_r**2))
    elif sigma_f is not None and sigma_f <= 1e-12:
        w_f = 1.0
    posterior_mu = w_f * mu_f + (1.0 - w_f) * mu_r

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
    shift = posterior_mu * _TRADING_DAYS - float(np.mean(gross_samples))
    gross_samples = gross_samples + shift

    b = float(borrow_annual) if borrow_annual is not None and math.isfinite(borrow_annual) else 0.0
    net_samples = gross_samples - b

    def q(x, p):
        return float(np.quantile(x, p))

    gross_p50 = q(gross_samples, 0.5)
    hist_edges = np.linspace(q(gross_samples, 0.01), q(gross_samples, 0.99), 22)
    counts, _ = np.histogram(gross_samples, bins=hist_edges)
    centers = (hist_edges[:-1] + hist_edges[1:]) / 2.0

    return {
        "net_edge_p05_annual": round(q(net_samples, 0.05), 6),
        "net_edge_p25_annual": round(q(net_samples, 0.25), 6),
        "net_edge_p50_annual": round(q(net_samples, 0.50), 6),
        "net_edge_p75_annual": round(q(net_samples, 0.75), 6),
        "net_edge_p95_annual": round(q(net_samples, 0.95), 6),
        "gross_realized_mean_annual": round(mu_r * _TRADING_DAYS, 6),
        "gross_anchor_target_annual": round(posterior_mu * _TRADING_DAYS, 6),
        "gross_blend_weight_forward": round(w_f, 4) if math.isfinite(w_f) else None,
        "gross_sigma_realized_annual": round(sigma_r * math.sqrt(_TRADING_DAYS), 6),
        "gross_sigma_forward_annual": round(sigma_f, 6) if sigma_f is not None else None,
        "net_edge_hist_json": json.dumps({
            "e": [round(float(x), 6) for x in centers],
            "c": [int(x) for x in counts],
        }),
    }

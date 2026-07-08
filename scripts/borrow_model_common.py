#!/usr/bin/env python3
"""Shared constants and helpers for borrow ML models (boosting, CNN, eval)."""
from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

# Top features from borrow_predictor_study_summary.json (v2 candidate set).
BOOSTING_FEATURE_COLS = [
    "borrow_current",
    "etf_aum_over_float",
    "borrow_vol10",
    "borrow_z60",
    "log_aum",
    "delta",
    "shares_available",
    "turnover_20d",
    "borrow_slope5",
    "borrow_pctile_60",
    "utilization_proxy",
    "peer_shares_avail_sum",
    "prem_disc_bps",
    "shares_drop5",
    "tradable_float_shares",
    "shares_drop3",
    "peer_borrow_z_mean",
    "rebalance_pct_adv",
    "forecast_vol_underlying_annual",
    "avail_to_adv",
]

SEQUENCE_CHANNELS = ["borrow_current", "shares_available", "shares_drop3", "utilization_proxy"]
SEQUENCE_WINDOW = 32

STATIC_FEATURE_COLS = [
    "borrow_z60",
    "borrow_slope5",
    "borrow_vol10",
    "borrow_pctile_60",
    "log_aum",
    "delta",
    "etf_aum_over_float",
    "forecast_vol_underlying_annual",
    "rebalance_pct_adv",
]

HORIZON_OBS = 5
DRIFT_TARGET = "delta_borrow_5"
SPIKE_TARGET = "y_spike_5"
SPIKE_EVENT_COL = "spike_event"

BORROW_OPS_POLICY = "v2_spike_boosting_drift"


def finite_optional(v: Any) -> float | None:
    """Return a finite float or None (NaN/inf/missing are dropped)."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def round_optional(v: Any, ndigits: int = 6) -> float | None:
    f = finite_optional(v)
    return round(f, ndigits) if f is not None else None


def shrink_delta(delta: float, obs_count: float | int | None) -> float:
    """Shrink extreme drift toward 0 when borrow history is thin."""
    if obs_count is None or not np.isfinite(obs_count):
        return delta
    n = float(obs_count)
    if n >= 60:
        return delta
    w = max(0.25, min(1.0, n / 60.0))
    return float(delta * w)


def prepare_feature_matrix(
    df: pd.DataFrame,
    feature_cols: list[str],
    *,
    fill_value: float = 0.0,
) -> tuple[np.ndarray, list[str]]:
    work = df.copy()
    cols = [c for c in feature_cols if c in work.columns]
    if not cols:
        return np.zeros((len(work), 0), dtype=float), []
    x = work[cols].replace([np.inf, -np.inf], np.nan).fillna(fill_value)
    return x.to_numpy(dtype=float), cols


def drift_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float | None]:
    mask = np.isfinite(y_true) & np.isfinite(y_pred)
    if mask.sum() < 10:
        return {"mae": None, "rmse": None, "r2": None, "n": int(mask.sum())}
    yt = y_true[mask]
    yp = y_pred[mask]
    err = yp - yt
    mae = float(np.mean(np.abs(err)))
    rmse = float(np.sqrt(np.mean(err**2)))
    ss_res = float(np.sum((yt - yp) ** 2))
    ss_tot = float(np.sum((yt - np.mean(yt)) ** 2))
    r2 = float(1.0 - ss_res / ss_tot) if ss_tot > 1e-12 else None
    return {"mae": round(mae, 6), "rmse": round(rmse, 6), "r2": round(r2, 6) if r2 is not None else None, "n": int(mask.sum())}


def row_obs_count(row: pd.Series, panel: pd.DataFrame | None, symbol: str) -> int | None:
    obs = row.get("obs_count")
    if obs is not None and np.isfinite(obs):
        return int(obs)
    if panel is not None and "borrow_current" in panel.columns:
        return int(panel[panel["symbol"] == symbol]["borrow_current"].notna().sum())
    return None


def default_registry() -> dict[str, Any]:
    return {
        "version": "1",
        "policy": BORROW_OPS_POLICY,
        "drift": {"method": "pooled_ols_top_features_shrunk", "winner": "ols", "artifact": None},
        "spike_l2": {
            "method": "logistic_v2_l2_isotonic",
            "winner": "logistic_v2",
            "shadow": "boosting",
            "artifact": None,
        },
        "fallback": {
            "drift": "pooled_ols",
            "spike_l2": "logistic_v2",
        },
        "gates": {
            "precision_at_10_lift_floor": 2.0,
            "primary_eval_label": "L2",
            "spike_production": "logistic_v2",
        },
    }

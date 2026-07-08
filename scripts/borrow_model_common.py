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


def _finite_metric(v: float | None) -> float | None:
    if v is None or not math.isfinite(v):
        return None
    return round(float(v), 6)


def sanitize_for_json(obj: Any) -> Any:
    """Replace NaN/inf with null so json.dump emits browser-safe JSON."""
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_for_json(v) for v in obj]
    if isinstance(obj, (float, np.floating)):
        f = float(obj)
        if math.isnan(f) or math.isinf(f):
            return None
        return f
    return obj


def drift_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float | None]:
    mask = np.isfinite(y_true) & np.isfinite(y_pred)
    if mask.sum() < 10:
        return {"mae": None, "rmse": None, "r2": None, "n": int(mask.sum())}
    yt = y_true[mask]
    yp = y_pred[mask]
    # Clip pathological model blow-ups before aggregate metrics (borrow deltas live in ~±1).
    err = np.clip(yp - yt, -5.0, 5.0)
    yp_clip = yt + err
    mae = float(np.mean(np.abs(err)))
    rmse = float(np.sqrt(np.mean(np.square(err))))
    ss_res = float(np.sum(np.square(yt - yp_clip)))
    ss_tot = float(np.sum(np.square(yt - np.mean(yt))))
    r2 = float(1.0 - ss_res / ss_tot) if ss_tot > 1e-12 else None
    return {
        "mae": _finite_metric(mae),
        "rmse": _finite_metric(rmse),
        "r2": _finite_metric(r2),
        "n": int(mask.sum()),
    }


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

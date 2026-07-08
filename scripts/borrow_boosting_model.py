#!/usr/bin/env python3
"""Gradient-boosting borrow drift + L2 spike models with walk-forward replay."""
from __future__ import annotations

import json
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd

from borrow_model_common import (
    BOOSTING_FEATURE_COLS,
    DRIFT_TARGET,
    SPIKE_EVENT_COL,
    drift_metrics,
    finite_optional,
    prepare_feature_matrix,
    round_optional,
    shrink_delta,
)
from borrow_spike_v2 import (
    IsotonicCalibrator,
    alert_tier,
    fit_isotonic_calibrator,
    walk_forward_replay_model,
)

_LGBM = None
_SKLEARN_HGB = None
try:
    import lightgbm as lgb

    _LGBM = lgb
except ImportError:
    try:
        from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor

        _SKLEARN_HGB = (HistGradientBoostingRegressor, HistGradientBoostingClassifier)
    except ImportError:
        pass


@dataclass
class BoostingBundle:
    backend: str
    drift_model: Any
    spike_model: Any
    feature_names: tuple[str, ...]
    calibrator: IsotonicCalibrator | None
    train_rows: int
    drift_train_rows: int
    spike_positives: int


def _backend_name() -> str:
    if _LGBM is not None:
        return "lightgbm"
    if _SKLEARN_HGB is not None:
        return "sklearn_histgb"
    return "unavailable"


def _fit_drift(X: np.ndarray, y: np.ndarray) -> Any | None:
    mask = np.isfinite(y) & np.all(np.isfinite(X), axis=1)
    if mask.sum() < 40:
        return None
    Xf, yf = X[mask], y[mask]
    if _LGBM is not None:
        ds = _LGBM.Dataset(Xf, label=yf)
        params = {
            "objective": "huber",
            "metric": "mae",
            "learning_rate": 0.05,
            "num_leaves": 31,
            "max_depth": 6,
            "min_data_in_leaf": 50,
            "feature_fraction": 0.85,
            "verbosity": -1,
            "seed": 42,
        }
        return _LGBM.train(params, ds, num_boost_round=120)
    if _SKLEARN_HGB is not None:
        Regressor, _ = _SKLEARN_HGB
        m = Regressor(
            max_depth=6,
            learning_rate=0.05,
            max_iter=120,
            min_samples_leaf=50,
            random_state=42,
        )
        m.fit(Xf, yf)
        return m
    return None


def _fit_spike(X: np.ndarray, y: np.ndarray) -> Any | None:
    mask = np.isfinite(y) & np.all(np.isfinite(X), axis=1)
    if mask.sum() < 40:
        return None
    Xf, yf = X[mask], y[mask].astype(int)
    pos = int((yf > 0).sum())
    neg = int((yf <= 0).sum())
    if pos == 0 or neg == 0:
        return None
    spw = min(25.0, max(1.0, neg / max(1, pos)))
    if _LGBM is not None:
        ds = _LGBM.Dataset(Xf, label=yf, weight=np.where(yf > 0, spw, 1.0))
        params = {
            "objective": "binary",
            "metric": "binary_logloss",
            "learning_rate": 0.05,
            "num_leaves": 31,
            "max_depth": 6,
            "min_data_in_leaf": 50,
            "feature_fraction": 0.85,
            "verbosity": -1,
            "seed": 42,
            "is_unbalance": False,
        }
        return _LGBM.train(params, ds, num_boost_round=150)
    if _SKLEARN_HGB is not None:
        _, Classifier = _SKLEARN_HGB
        m = Classifier(
            max_depth=6,
            learning_rate=0.05,
            max_iter=150,
            min_samples_leaf=50,
            class_weight={0: 1.0, 1: spw},
            random_state=42,
        )
        m.fit(Xf, yf)
        return m
    return None


def _predict_drift(model: Any, X: np.ndarray) -> np.ndarray:
    if model is None:
        return np.zeros(len(X), dtype=float)
    if _LGBM is not None and hasattr(model, "predict"):
        return np.asarray(model.predict(X), dtype=float)
    return np.asarray(model.predict(X), dtype=float)


def _predict_spike(model: Any, X: np.ndarray) -> np.ndarray:
    if model is None:
        return np.zeros(len(X), dtype=float)
    if _LGBM is not None and hasattr(model, "predict"):
        return np.clip(np.asarray(model.predict(X), dtype=float), 0.0, 1.0)
    if hasattr(model, "predict_proba"):
        return np.clip(model.predict_proba(X)[:, 1], 0.0, 1.0)
    return np.clip(np.asarray(model.predict(X), dtype=float), 0.0, 1.0)


def fit_boosting_bundle(train_df: pd.DataFrame) -> BoostingBundle | None:
    if _backend_name() == "unavailable":
        return None
    if train_df.empty or len(train_df) < 40:
        return None
    X, feat_names = prepare_feature_matrix(train_df, BOOSTING_FEATURE_COLS)
    if X.shape[1] == 0:
        return None
    y_drift = train_df.get(DRIFT_TARGET)
    if y_drift is None:
        return None
    y_drift = y_drift.to_numpy(dtype=float)
    y_spike = train_df.get(SPIKE_EVENT_COL)
    if y_spike is None:
        y_spike = train_df.get("y_spike_5")
    if y_spike is None:
        return None
    y_spike = y_spike.to_numpy(dtype=float)
    drift_m = _fit_drift(X, y_drift)
    spike_m = _fit_spike(X, y_spike)
    if drift_m is None and spike_m is None:
        return None
    pos = int((y_spike > 0.5).sum())
    return BoostingBundle(
        backend=_backend_name(),
        drift_model=drift_m,
        spike_model=spike_m,
        feature_names=tuple(feat_names),
        calibrator=None,
        train_rows=int(len(train_df)),
        drift_train_rows=int(np.isfinite(y_drift).sum()),
        spike_positives=pos,
    )


def score_drift(bundle: BoostingBundle, df: pd.DataFrame) -> np.ndarray:
    X, _ = prepare_feature_matrix(df, list(bundle.feature_names))
    if X.shape[1] != len(bundle.feature_names):
        X, _ = prepare_feature_matrix(df, BOOSTING_FEATURE_COLS)
    return _predict_drift(bundle.drift_model, X)


def score_spike(bundle: BoostingBundle, df: pd.DataFrame) -> np.ndarray:
    X, _ = prepare_feature_matrix(df, list(bundle.feature_names))
    if X.shape[1] != len(bundle.feature_names):
        X, _ = prepare_feature_matrix(df, BOOSTING_FEATURE_COLS)
    return _predict_spike(bundle.spike_model, X)


def _fit_spike_wrapper(train_df: pd.DataFrame) -> BoostingBundle | None:
    b = fit_boosting_bundle(train_df)
    if b is None or b.spike_model is None:
        return b
    X, _ = prepare_feature_matrix(train_df, list(b.feature_names))
    p_train = _predict_spike(b.spike_model, X)
    y_col = SPIKE_EVENT_COL if SPIKE_EVENT_COL in train_df.columns else "y_spike_5"
    valid = train_df[y_col].notna().to_numpy()
    p_train = p_train[valid]
    y_train = train_df.loc[valid, y_col].to_numpy(dtype=float)
    cal = fit_isotonic_calibrator(p_train, y_train) if len(y_train) >= 30 else None
    return BoostingBundle(
        backend=b.backend,
        drift_model=b.drift_model,
        spike_model=b.spike_model,
        feature_names=b.feature_names,
        calibrator=cal,
        train_rows=b.train_rows,
        drift_train_rows=b.drift_train_rows,
        spike_positives=b.spike_positives,
    )


def _score_spike_wrapper(bundle: BoostingBundle, df: pd.DataFrame) -> np.ndarray:
    if bundle is None or bundle.spike_model is None:
        return np.zeros(len(df), dtype=float)
    return score_spike(bundle, df)


class _BoostingSpikeAdapter:
    """Adapter so walk_forward_replay_model can refit boosting spike heads."""

    def __init__(self) -> None:
        self.bundle: BoostingBundle | None = None
        self.feature_names: tuple[str, ...] = ()
        self.weights = np.array([])
        self.bias = 0.0
        self.mean = np.array([])
        self.std = np.array([1.0])
        self.train_rows = 0
        self.positives = 0
        self.negatives = 0
        self.positive_weight = 1.0

    def fit_from_train(self, train_df: pd.DataFrame) -> _BoostingSpikeAdapter | None:
        b = _fit_spike_wrapper(train_df)
        if b is None or b.spike_model is None:
            return None
        self.bundle = b
        self.feature_names = b.feature_names
        self.train_rows = b.train_rows
        y = train_df[SPIKE_EVENT_COL if SPIKE_EVENT_COL in train_df.columns else "y_spike_5"].to_numpy(dtype=float)
        self.positives = int((y > 0.5).sum())
        self.negatives = int((y <= 0.5).sum())
        return self


def fit_boosting_spike_adapter(train_df: pd.DataFrame) -> _BoostingSpikeAdapter | None:
    ad = _BoostingSpikeAdapter()
    return ad.fit_from_train(train_df)


def score_boosting_spike_adapter(adapter: _BoostingSpikeAdapter, df: pd.DataFrame) -> np.ndarray:
    if adapter is None or adapter.bundle is None:
        return np.zeros(len(df), dtype=float)
    return score_spike(adapter.bundle, df)


def walk_forward_replay_boosting(
    panel: pd.DataFrame,
    *,
    refit_cadence_days: int = 7,
    min_train_rows: int = 200,
    calibrate: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (spike_replay_df, drift_replay_df)."""
    if panel.empty:
        return pd.DataFrame(), pd.DataFrame()
    work = panel.copy()
    if SPIKE_EVENT_COL not in work.columns and "y_spike_5" in work.columns:
        work[SPIKE_EVENT_COL] = work["y_spike_5"]
    if DRIFT_TARGET not in work.columns:
        return pd.DataFrame(), pd.DataFrame()

    spike_replay = walk_forward_replay_model(
        work,
        fit_fn=fit_boosting_spike_adapter,
        score_fn=score_boosting_spike_adapter,
        model_name="boosting_l2",
        calibrate=calibrate,
        refit_cadence_days=refit_cadence_days,
        min_train_rows=min_train_rows,
    )

    # Drift replay (custom loop — regression target)
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work.dropna(subset=["date"]).sort_values(["date", "symbol"]).reset_index(drop=True)
    unique_dates = sorted(work["date"].dropna().unique())
    if len(unique_dates) < 5:
        return spike_replay, pd.DataFrame()

    refit_dates: set[pd.Timestamp] = {unique_dates[0]}
    last_refit = unique_dates[0]
    for d in unique_dates[1:]:
        if (d - last_refit).days >= refit_cadence_days:
            refit_dates.add(d)
            last_refit = d
    refit_dates.add(unique_dates[-1])

    bundle: BoostingBundle | None = None
    drift_chunks: list[pd.DataFrame] = []
    for eval_date in unique_dates:
        if eval_date in refit_dates:
            train = work[work["date"] < eval_date]
            if len(train) >= min_train_rows:
                bundle = fit_boosting_bundle(train)
        if bundle is None or bundle.drift_model is None:
            continue
        eval_rows = work[work["date"] == eval_date]
        if eval_rows.empty:
            continue
        pred = score_drift(bundle, eval_rows)
        chunk = eval_rows.copy()
        chunk["delta_pred"] = np.round(pred, 6)
        chunk["model"] = f"boosting_drift_{bundle.backend}"
        chunk["pred_date"] = chunk["date"].dt.strftime("%Y-%m-%d")
        chunk["y_drift"] = chunk[DRIFT_TARGET]
        drift_chunks.append(chunk)

    drift_replay = pd.concat(drift_chunks, ignore_index=True) if drift_chunks else pd.DataFrame()
    return spike_replay, drift_replay


def build_boosting_panel_from_predictor(panel: pd.DataFrame, *, label_variant: str = "L2") -> pd.DataFrame:
    """Align predictor panel rows with L2 spike_event for boosting replay."""
    if panel.empty:
        return panel
    from borrow_spike_model import apply_spike_labels, _symbol_history_frame

    out = panel.copy()
    if SPIKE_EVENT_COL not in out.columns:
        if "y_spike_5" in out.columns:
            out[SPIKE_EVENT_COL] = out["y_spike_5"]
        else:
            out[SPIKE_EVENT_COL] = np.nan
    if "obs_count" not in out.columns:
        out["obs_count"] = out.groupby("symbol")["borrow_current"].transform(lambda s: s.notna().sum())
    return out


def save_bundle(bundle: BoostingBundle, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        pickle.dump(bundle, f)


def load_bundle(path: Path) -> BoostingBundle | None:
    if not path.exists():
        return None
    with path.open("rb") as f:
        return pickle.load(f)


def build_drift_forecast_from_panel(
    panel: pd.DataFrame,
    bundle: BoostingBundle,
) -> dict[str, dict[str, Any]]:
    if panel.empty or bundle.drift_model is None:
        return {}
    panel = panel.copy()
    panel["date"] = pd.to_datetime(panel["date"], errors="coerce")
    latest = panel.sort_values("date").groupby("symbol", as_index=False).tail(1)
    by_symbol: dict[str, dict] = {}
    for _, row in latest.iterrows():
        sym = str(row["symbol"]).upper()
        pred_df = pd.DataFrame([row])
        delta_raw = float(score_drift(bundle, pred_df)[0])
        obs = row.get("obs_count")
        delta = shrink_delta(delta_raw, obs)
        cur = finite_optional(row.get("borrow_current"))
        by_symbol[sym] = {
            "delta_borrow_5d_p50": round_optional(delta),
            "borrow_forecast_5d_p50": round_optional(cur + delta if cur is not None else None),
            "borrow_current": round_optional(cur),
            "obs_count": int(obs) if obs is not None and np.isfinite(obs) else None,
            "as_of_date": row["date"].strftime("%Y-%m-%d") if pd.notna(row.get("date")) else None,
            "method": f"boosting_{bundle.backend}",
        }
    return by_symbol


def evaluate_drift_replay(drift_replay: pd.DataFrame) -> dict[str, Any]:
    if drift_replay.empty:
        return {"status": "empty"}
    y = drift_replay["y_drift"].to_numpy(dtype=float)
    p = drift_replay["delta_pred"].to_numpy(dtype=float)
    return {"status": "ok", **drift_metrics(y, p)}

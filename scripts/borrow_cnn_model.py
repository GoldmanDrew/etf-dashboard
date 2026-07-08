#!/usr/bin/env python3
"""Numpy temporal CNN for borrow drift + L2 spike (research lane, no torch dependency)."""
from __future__ import annotations

import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from borrow_model_common import (
    DRIFT_TARGET,
    SEQUENCE_CHANNELS,
    SEQUENCE_WINDOW,
    SPIKE_EVENT_COL,
    STATIC_FEATURE_COLS,
    drift_metrics,
    prepare_feature_matrix,
    shrink_delta,
)
from borrow_spike_v2 import IsotonicCalibrator, fit_isotonic_calibrator


def _relu(x: np.ndarray) -> np.ndarray:
    return np.maximum(0.0, x)


def _sigmoid(x: np.ndarray) -> np.ndarray:
    x = np.clip(x, -40, 40)
    return 1.0 / (1.0 + np.exp(-x))


@dataclass
class NumpyTemporalCNN:
    conv_w: np.ndarray  # (n_filters, n_channels, kernel)
    conv_b: np.ndarray
    static_w: np.ndarray
    static_b: float
    head_drift_w: np.ndarray
    head_drift_b: float
    head_spike_w: np.ndarray
    head_spike_b: float
    seq_channels: tuple[str, ...]
    static_features: tuple[str, ...]
    window: int
    train_rows: int


def _extract_seq_tensor(df: pd.DataFrame, channels: list[str], window: int) -> np.ndarray:
    n = len(df)
    c = len(channels)
    out = np.zeros((n, c, window), dtype=float)
    for j, ch in enumerate(channels):
        col = f"seq_{ch}"
        if col not in df.columns:
            continue
        for i, val in enumerate(df[col].tolist()):
            if isinstance(val, (list, tuple, np.ndarray)):
                arr = np.asarray(val, dtype=float)
                if len(arr) >= window:
                    out[i, j, :] = arr[-window:]
                elif len(arr) > 0:
                    out[i, j, -len(arr) :] = arr
    return out


def _conv1d_maxpool(x: np.ndarray, w: np.ndarray, b: np.ndarray, kernel: int = 3) -> np.ndarray:
    """x: (batch, channels, time), w: (filters, channels, kernel) -> (batch, filters)."""
    n, c, t = x.shape
    n_f = w.shape[0]
    out_len = t - kernel + 1
    if out_len < 1:
        return np.zeros((n, n_f), dtype=float)
    feats = np.zeros((n, n_f, out_len), dtype=float)
    for f in range(n_f):
        for i in range(out_len):
            patch = x[:, :, i : i + kernel]
            feats[:, f, i] = np.sum(patch * w[f], axis=(1, 2)) + b[f]
    pooled = np.max(_relu(feats), axis=2)
    return pooled


def _init_cnn(n_channels: int, n_static: int, window: int, seed: int = 42) -> NumpyTemporalCNN:
    rng = np.random.default_rng(seed)
    n_filters = 8
    kernel = 3
    scale = 0.05
    return NumpyTemporalCNN(
        conv_w=rng.normal(0, scale, (n_filters, n_channels, kernel)),
        conv_b=np.zeros(n_filters),
        static_w=rng.normal(0, scale, (n_static, n_filters)),
        static_b=0.0,
        head_drift_w=rng.normal(0, scale, n_filters + n_static),
        head_drift_b=0.0,
        head_spike_w=rng.normal(0, scale, n_filters + n_static),
        head_spike_b=0.0,
        seq_channels=tuple(SEQUENCE_CHANNELS),
        static_features=tuple(STATIC_FEATURE_COLS),
        window=window,
        train_rows=0,
    )


def _forward(model: NumpyTemporalCNN, seq: np.ndarray, static: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    conv = _conv1d_maxpool(seq, model.conv_w, model.conv_b, kernel=3)
    if static.shape[1] > 0 and model.static_w.shape[0] == static.shape[1]:
        conv = conv + static @ model.static_w
    h = np.concatenate([conv, static], axis=1)
    drift = h @ model.head_drift_w + model.head_drift_b
    spike = _sigmoid(h @ model.head_spike_w + model.head_spike_b)
    return drift, spike


def fit_numpy_cnn(
    train_df: pd.DataFrame,
    *,
    window: int = SEQUENCE_WINDOW,
    epochs: int = 25,
    lr: float = 0.02,
    batch_size: int = 256,
    seed: int = 42,
) -> NumpyTemporalCNN | None:
    if train_df.empty or len(train_df) < 100:
        return None
    channels = [c for c in SEQUENCE_CHANNELS if f"seq_{c}" in train_df.columns]
    if not channels:
        return None
    static_cols = [c for c in STATIC_FEATURE_COLS if c in train_df.columns]
    seq = _extract_seq_tensor(train_df, channels, window)
    static, _ = prepare_feature_matrix(train_df, static_cols)
    y_drift = train_df.get(DRIFT_TARGET)
    y_col = SPIKE_EVENT_COL if SPIKE_EVENT_COL in train_df.columns else "y_spike_5"
    y_spike = train_df.get(y_col)
    if y_drift is None or y_spike is None:
        return None
    y_d = y_drift.to_numpy(dtype=float)
    y_s = y_spike.to_numpy(dtype=float)
    mask = np.isfinite(y_d) & np.isfinite(y_s)
    if mask.sum() < 80:
        return None
    seq, static = seq[mask], static[mask]
    y_d, y_s = y_d[mask], y_s[mask]
    pos = max(1, int((y_s > 0.5).sum()))
    neg = max(1, int((y_s <= 0.5).sum()))
    spw = min(20.0, neg / pos)

    model = _init_cnn(len(channels), static.shape[1], window, seed=seed)
    n = len(y_d)
    rng = np.random.default_rng(seed)
    for _ in range(epochs):
        idx = rng.permutation(n)
        for start in range(0, n, batch_size):
            batch_idx = idx[start : start + batch_size]
            bs = seq[batch_idx]
            bx = static[batch_idx]
            by_d = y_d[batch_idx]
            by_s = y_s[batch_idx]
            drift_p, spike_p = _forward(model, bs, bx)
            # drift MSE
            d_err = drift_p - by_d
            # spike BCE
            sp = np.clip(spike_p, 1e-6, 1 - 1e-6)
            w = np.where(by_s > 0.5, spw, 1.0)
            bce = -w * (by_s * np.log(sp) + (1 - by_s) * np.log(1 - sp))
            loss = np.mean(d_err**2) + 0.5 * np.mean(bce)
            if not np.isfinite(loss):
                continue
            # Numeric gradient step on heads only (keep conv stable)
            conv = _conv1d_maxpool(bs, model.conv_w, model.conv_b, kernel=3)
            h = np.concatenate([conv, bx], axis=1)
            h_drift_grad = np.clip(2 * d_err / len(by_d), -1.0, 1.0)
            h_spike_grad = np.clip(w * (spike_p - by_s) / len(by_s), -1.0, 1.0)
            model.head_drift_w -= lr * np.clip((h.T @ h_drift_grad) / max(1, len(by_d)), -0.5, 0.5)
            model.head_drift_b -= lr * float(np.clip(np.mean(h_drift_grad), -0.5, 0.5))
            model.head_spike_w -= lr * np.clip((h.T @ h_spike_grad) / max(1, len(by_s)), -0.5, 0.5)
            model.head_spike_b -= lr * float(np.clip(np.mean(h_spike_grad), -0.5, 0.5))
            model.head_drift_w = np.clip(model.head_drift_w, -5.0, 5.0)
            model.head_spike_w = np.clip(model.head_spike_w, -5.0, 5.0)
    model.train_rows = int(n)
    return model


def score_cnn(model: NumpyTemporalCNN, df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    channels = list(model.seq_channels)
    static_cols = list(model.static_features)
    seq = _extract_seq_tensor(df, channels, model.window)
    static, _ = prepare_feature_matrix(df, static_cols)
    return _forward(model, seq, static)


def walk_forward_replay_cnn(
    panel: pd.DataFrame,
    *,
    refit_cadence_days: int = 14,
    min_train_rows: int = 500,
    epochs: int = 20,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if panel.empty:
        return pd.DataFrame(), pd.DataFrame()
    work = panel.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work.dropna(subset=["date"]).sort_values(["date", "symbol"]).reset_index(drop=True)
    unique_dates = sorted(work["date"].dropna().unique())
    if len(unique_dates) < 6:
        return pd.DataFrame(), pd.DataFrame()

    refit_dates: set[pd.Timestamp] = {unique_dates[0]}
    last_refit = unique_dates[0]
    for d in unique_dates[1:]:
        if (d - last_refit).days >= refit_cadence_days:
            refit_dates.add(d)
            last_refit = d
    refit_dates.add(unique_dates[-1])

    model: NumpyTemporalCNN | None = None
    calibrator: IsotonicCalibrator | None = None
    spike_chunks: list[pd.DataFrame] = []
    drift_chunks: list[pd.DataFrame] = []

    for eval_date in unique_dates:
        if eval_date in refit_dates:
            train = work[work["date"] < eval_date]
            if len(train) >= min_train_rows:
                model = fit_numpy_cnn(train, epochs=epochs)
                if model is not None:
                    _, p_train = score_cnn(model, train)
                    y_train = train[SPIKE_EVENT_COL if SPIKE_EVENT_COL in train.columns else "y_spike_5"].to_numpy(dtype=float)
                    calibrator = fit_isotonic_calibrator(p_train, y_train)
        if model is None:
            continue
        eval_rows = work[work["date"] == eval_date]
        if eval_rows.empty:
            continue
        drift_p, spike_p = score_cnn(model, eval_rows)
        if calibrator is not None:
            spike_cal = calibrator.transform_array(spike_p)
        else:
            spike_cal = spike_p
        sc = eval_rows.copy()
        sc["p_replay"] = np.round(spike_p, 6)
        sc["p_replay_calibrated"] = np.round(spike_cal, 6)
        sc["model"] = "cnn_numpy_l2"
        sc["pred_date"] = sc["date"].dt.strftime("%Y-%m-%d")
        sc["y_spike"] = sc[SPIKE_EVENT_COL if SPIKE_EVENT_COL in sc.columns else "y_spike_5"].astype(int)
        spike_chunks.append(sc)

        dc = eval_rows.copy()
        dc["delta_pred"] = np.round(drift_p, 6)
        dc["model"] = "cnn_numpy_drift"
        dc["pred_date"] = dc["date"].dt.strftime("%Y-%m-%d")
        dc["y_drift"] = dc[DRIFT_TARGET]
        drift_chunks.append(dc)

    spike_replay = pd.concat(spike_chunks, ignore_index=True) if spike_chunks else pd.DataFrame()
    drift_replay = pd.concat(drift_chunks, ignore_index=True) if drift_chunks else pd.DataFrame()
    return spike_replay, drift_replay


def save_cnn(model: NumpyTemporalCNN, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        pickle.dump(model, f)


def load_cnn(path: Path) -> NumpyTemporalCNN | None:
    if not path.exists():
        return None
    with path.open("rb") as f:
        return pickle.load(f)

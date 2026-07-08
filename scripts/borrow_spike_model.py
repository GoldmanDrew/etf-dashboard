#!/usr/bin/env python3
"""Borrow spike model core: labels, features, logistic_v1, walk-forward replay.

Predictor study (borrow_predictor_study_summary.json) recommended_v2_features for
future logistic_v2 (not implemented here): borrow_current, etf_aum_over_float,
borrow_vol10, borrow_z60, log_aum, delta, shares_available, turnover_20d,
borrow_slope5, borrow_pctile_60, utilization_proxy, peer_shares_avail_sum,
prem_disc_bps, shares_drop5, tradable_float_shares, shares_drop3,
peer_borrow_z_mean, rebalance_pct_adv, forecast_vol_underlying_annual,
gross_decay_annual. Supply block (borrow_plus_supply) edges borrow-only; peer
basket underperforms in the study ablation.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

HORIZON_DAYS_DEFAULT = 5
MIN_OBS_FOR_SCORING = 30

FEATURE_COLS = [
    "borrow_current",
    "borrow_z60",
    "borrow_slope5",
    "borrow_vol10",
    "shares_available",
    "shares_drop1",
    "shares_drop3",
    "shares_drop5",
    "near_zero_shares",
]

LABEL_VARIANTS: dict[str, dict[str, Any]] = {
    "L0": {
        "name": "production",
        "description": "future_max > max(1.0, 3*med60, p99_180) and jump > 0.25",
        "mult_med": 3.0,
        "jump_min": 0.25,
        "abs_floor": 1.0,
        "use_p99": True,
    },
    "L1": {
        "name": "moderate",
        "description": "future_max > max(1.0, 2*med60, p99_180) and jump > 0.15",
        "mult_med": 2.0,
        "jump_min": 0.15,
        "abs_floor": 1.0,
        "use_p99": True,
    },
    "L2": {
        "name": "p90_relative",
        "description": "future_max > p90_60 and jump > 0.10",
        "jump_min": 0.10,
        "use_p90": True,
    },
    "L3": {
        "name": "absolute_5pct",
        "description": "future_max > 0.05 and jump > 0.10",
        "jump_min": 0.10,
        "abs_borrow_min": 0.05,
    },
}


@dataclass(frozen=True)
class LogisticV1Model:
    feature_names: tuple[str, ...]
    weights: np.ndarray
    bias: float
    mean: np.ndarray
    std: np.ndarray
    train_rows: int
    positives: int
    negatives: int
    positive_weight: float


def _sigmoid(x: np.ndarray) -> np.ndarray:
    x = np.clip(x, -40, 40)
    return 1.0 / (1.0 + np.exp(-x))


def _fit_logistic_l2(
    X: np.ndarray,
    y: np.ndarray,
    sample_w: np.ndarray,
    *,
    l2: float = 1.0,
    lr: float = 0.05,
    steps: int = 700,
) -> tuple[np.ndarray, float]:
    n, d = X.shape
    w = np.zeros(d, dtype=float)
    b = 0.0
    sw = np.maximum(1e-9, sample_w.astype(float))
    sw /= np.mean(sw)
    for _ in range(steps):
        z = X @ w + b
        p = _sigmoid(z)
        e = (p - y) * sw
        grad_w = (X.T @ e) / max(1, n) + l2 * w
        grad_b = float(np.sum(e) / max(1, n))
        w -= lr * grad_w
        b -= lr * grad_b
    return w, b


def risk_band(p: float | None) -> str:
    if p is None or not math.isfinite(p):
        return "unknown"
    if p >= 0.30:
        return "high"
    if p >= 0.10:
        return "elevated"
    return "low"


def _symbol_history_frame(hist: list[dict]) -> pd.DataFrame:
    s = (
        pd.DataFrame(hist)
        .assign(
            date=lambda d: pd.to_datetime(d["date"], errors="coerce"),
            borrow_current=lambda d: pd.to_numeric(d.get("borrow_current"), errors="coerce"),
            shares_available=lambda d: pd.to_numeric(d.get("shares_available"), errors="coerce"),
        )
        .dropna(subset=["date"])
        .sort_values("date")
        .drop_duplicates(subset=["date"], keep="last")
        .reset_index(drop=True)
    )
    if s.empty:
        return s
    s["borrow_lag1"] = s["borrow_current"].shift(1)
    s["borrow_lag5"] = s["borrow_current"].shift(5)
    s["shares_lag1"] = s["shares_available"].shift(1)
    s["shares_lag3"] = s["shares_available"].shift(3)
    s["shares_lag5"] = s["shares_available"].shift(5)
    s["med60"] = s["borrow_current"].rolling(60, min_periods=10).median()
    s["std60"] = s["borrow_current"].rolling(60, min_periods=10).std()
    s["p99_180"] = s["borrow_current"].rolling(180, min_periods=20).quantile(0.99)
    s["p90_60"] = s["borrow_current"].rolling(60, min_periods=10).quantile(0.90)
    s["borrow_slope5"] = (s["borrow_current"] - s["borrow_lag5"]) / 5.0
    s["borrow_d1"] = s["borrow_current"] - s["borrow_lag1"]
    s["borrow_vol10"] = s["borrow_d1"].rolling(10, min_periods=4).std()
    s["borrow_z60"] = (s["borrow_current"] - s["med60"]) / s["std60"].replace(0, np.nan)
    s["shares_drop1"] = (s["shares_lag1"] - s["shares_available"]) / s["shares_lag1"].clip(lower=1)
    s["shares_drop3"] = (s["shares_lag3"] - s["shares_available"]) / s["shares_lag3"].clip(lower=1)
    s["shares_drop5"] = (s["shares_lag5"] - s["shares_available"]) / s["shares_lag5"].clip(lower=1)
    s["near_zero_shares"] = (s["shares_available"] <= 1000).astype(float)
    return s


def _spike_threshold_row(row: pd.Series, variant: dict[str, Any]) -> float:
    if variant.get("use_p90"):
        base = row.get("p90_60")
        return float(base) if pd.notna(base) else np.nan
    if variant.get("abs_borrow_min") is not None:
        return float(variant["abs_borrow_min"])
    med = row.get("med60")
    parts = [float(variant.get("abs_floor", 1.0))]
    if pd.notna(med):
        parts.append(float(variant.get("mult_med", 3.0)) * float(med))
    if variant.get("use_p99", True):
        p99 = row.get("p99_180")
        if pd.notna(p99):
            parts.append(float(p99))
    return float(np.nanmax(parts)) if parts else np.nan


def apply_spike_labels(
    s: pd.DataFrame,
    *,
    horizon_days: int = HORIZON_DAYS_DEFAULT,
    label_variant: str = "L0",
) -> pd.DataFrame:
    if s.empty:
        return s
    variant = LABEL_VARIANTS.get(label_variant, LABEL_VARIANTS["L0"])
    jump_min = float(variant.get("jump_min", 0.25))
    out = s.copy()
    fut_max = out["borrow_current"].rolling(horizon_days, min_periods=1).max().shift(-horizon_days)
    fut_jump = (fut_max - out["borrow_current"]).astype(float)
    thresh = out.apply(lambda r: _spike_threshold_row(r, variant), axis=1)
    out["spike_event"] = ((fut_max > thresh) & (fut_jump > jump_min)).astype(float)
    out.loc[thresh.isna() | fut_max.isna(), "spike_event"] = np.nan
    return out


def compute_borrow_spike_event_by_date(
    hist: list[dict],
    *,
    horizon_days: int = HORIZON_DAYS_DEFAULT,
    label_variant: str = "L0",
) -> dict[str, float | None]:
    if not hist or len(hist) < 12:
        return {}
    s = _symbol_history_frame(hist)
    if len(s) < 12:
        return {}
    s = apply_spike_labels(s, horizon_days=horizon_days, label_variant=label_variant)
    out: dict[str, float | None] = {}
    for _, row in s.iterrows():
        d = row["date"]
        if pd.isna(d):
            continue
        ds = d.strftime("%Y-%m-%d")
        ev = row["spike_event"]
        if pd.isna(ev) or not np.isfinite(ev):
            out[ds] = None
        else:
            out[ds] = float(ev)
    return out


def borrow_outcome_at_date(
    hist: list[dict],
    pred_date: str,
    *,
    horizon_days: int = HORIZON_DAYS_DEFAULT,
) -> dict[str, Any] | None:
    """Borrow path over the forward observation window from pred_date."""
    if not hist:
        return None
    s = _symbol_history_frame(hist)
    if s.empty:
        return None
    s = s.copy()
    s["date_str"] = s["date"].dt.strftime("%Y-%m-%d")
    idx_list = s.index[s["date_str"] == pred_date].tolist()
    if not idx_list:
        return None
    idx = idx_list[0]
    pos = s.index.get_loc(idx)
    if isinstance(pos, slice):
        pos = pos.start
    borrow_at = s.iloc[pos]["borrow_current"]
    if pd.isna(borrow_at):
        return None
    fut = s.iloc[pos + 1 : pos + 1 + horizon_days]
    if fut.empty:
        return None
    fut_borrows = fut["borrow_current"].dropna()
    if fut_borrows.empty:
        return None
    max_borrow = float(fut_borrows.max())
    jump = max_borrow - float(borrow_at)
    borrow_end = float(fut_borrows.iloc[-1])
    gaps: list[float] = []
    dates = s["date"].dropna()
    if len(dates) >= 2:
        deltas = dates.diff().dt.days.dropna()
        gaps = [float(x) for x in deltas if np.isfinite(x)]
    obs_gap_median = float(np.median(gaps)) if gaps else None
    return {
        "borrow_at_pred": round(float(borrow_at), 6),
        "borrow_at_horizon": round(borrow_end, 6),
        "max_borrow_in_window": round(max_borrow, 6),
        "jump_magnitude": round(jump, 6),
        "obs_gap_median_days": round(obs_gap_median, 3) if obs_gap_median is not None else None,
    }


def _quality_band(obs_count: int, shares_cov: float) -> str:
    if obs_count >= 60 and shares_cov >= 0.70:
        return "strong"
    if obs_count >= MIN_OBS_FOR_SCORING and shares_cov >= 0.40:
        return "moderate"
    return "insufficient"


def build_symbol_panel_rows(
    sym: str,
    hist: list[dict],
    *,
    horizon_days: int = HORIZON_DAYS_DEFAULT,
    label_variant: str = "L0",
) -> list[dict]:
    s = _symbol_history_frame(hist)
    if len(s) < 12:
        return []
    s = apply_spike_labels(s, horizon_days=horizon_days, label_variant=label_variant)
    obs_count = int(s["borrow_current"].notna().sum())
    shares_obs_count = int(s["shares_available"].notna().sum())
    shares_cov = float(shares_obs_count / max(1, len(s)))
    quality = _quality_band(obs_count, shares_cov)
    scoring_eligible = obs_count >= MIN_OBS_FOR_SCORING

    usable = s.iloc[:-horizon_days].copy() if len(s) > horizon_days else s.iloc[0:0].copy()
    usable = usable.dropna(subset=["spike_event"])
    usable = usable.dropna(subset=["borrow_current", "med60"])
    if variant_needs_p99(label_variant):
        usable = usable.dropna(subset=["p99_180"])
    rows: list[dict] = []
    for _, row in usable.iterrows():
        rec: dict[str, Any] = {
            "symbol": sym,
            "date": row["date"].strftime("%Y-%m-%d"),
            "spike_event": float(row["spike_event"]),
            "obs_count": obs_count,
            "shares_obs_count": shares_obs_count,
            "shares_coverage": shares_cov,
            "quality_band": quality,
            "scoring_eligible": scoring_eligible,
        }
        for c in FEATURE_COLS:
            v = row.get(c)
            rec[c] = float(v) if v is not None and pd.notna(v) and np.isfinite(v) else 0.0
        rows.append(rec)
    return rows


def variant_needs_p99(label_variant: str) -> bool:
    v = LABEL_VARIANTS.get(label_variant, LABEL_VARIANTS["L0"])
    return bool(v.get("use_p99", False) and not v.get("use_p90"))


def build_panel_from_history(
    borrow_symbols: dict[str, list[dict]],
    *,
    horizon_days: int = HORIZON_DAYS_DEFAULT,
    label_variant: str = "L0",
    as_of_max: str | None = None,
) -> pd.DataFrame:
    rows: list[dict] = []
    for sym, hist in (borrow_symbols or {}).items():
        if not hist:
            continue
        rows.extend(
            build_symbol_panel_rows(
                str(sym).upper(),
                hist,
                horizon_days=horizon_days,
                label_variant=label_variant,
            )
        )
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values(["date", "symbol"]).reset_index(drop=True)
    if as_of_max:
        cutoff = pd.Timestamp(as_of_max)
        df = df[df["date"] <= cutoff].reset_index(drop=True)
    return df


def _prepare_X(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    x = df[FEATURE_COLS].copy()
    x["log_shares"] = np.log1p(x["shares_available"].clip(lower=0))
    x = x.drop(columns=["shares_available"], errors="ignore")
    x = x.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return x, list(x.columns)


def fit_logistic_v1(train_df: pd.DataFrame) -> LogisticV1Model | None:
    if train_df.empty or len(train_df) < 40:
        return None
    y = train_df["spike_event"].to_numpy(dtype=float)
    x_raw, feat_names = _prepare_X(train_df)
    x_train = x_raw.to_numpy(dtype=float)
    mean_v = x_train.mean(axis=0)
    std_v = x_train.std(axis=0)
    std_v = np.where(std_v < 1e-9, 1.0, std_v)
    x_train_s = (x_train - mean_v) / std_v
    pos_n = int((y > 0.5).sum())
    neg_n = int((y <= 0.5).sum())
    if pos_n == 0 and neg_n == 0:
        return None
    pos_w = float(neg_n / max(1, pos_n))
    pos_w = min(25.0, max(1.0, pos_w))
    sample_w = np.where(y > 0.5, pos_w, 1.0)
    w_v, b_v = _fit_logistic_l2(x_train_s, y, sample_w, l2=0.03, lr=0.08, steps=650)
    return LogisticV1Model(
        feature_names=tuple(feat_names),
        weights=w_v,
        bias=b_v,
        mean=mean_v,
        std=std_v,
        train_rows=int(len(train_df)),
        positives=pos_n,
        negatives=neg_n,
        positive_weight=round(pos_w, 4),
    )


def score_rows(model: LogisticV1Model, df: pd.DataFrame) -> np.ndarray:
    x_raw, feat_names = _prepare_X(df)
    for c in model.feature_names:
        if c not in x_raw.columns:
            x_raw[c] = 0.0
    x = x_raw[list(model.feature_names)].to_numpy(dtype=float)
    x_s = (x - model.mean) / model.std
    p = _sigmoid(x_s @ model.weights + model.bias)
    return np.clip(p, 0.0, 1.0)


def score_feature_dict(model: LogisticV1Model, feats: dict[str, Any]) -> tuple[float, list[dict]]:
    x = []
    for c in model.feature_names:
        if c == "log_shares":
            raw_sh = float(feats.get("shares_available") or 0.0)
            x.append(np.log1p(max(0.0, raw_sh)))
        else:
            v = feats.get(c)
            x.append(float(v) if v is not None and np.isfinite(v) else 0.0)
    xv = (np.array(x, dtype=float) - model.mean) / model.std
    p = float(_sigmoid(np.array([xv @ model.weights + model.bias]))[0])
    p = max(0.0, min(1.0, p))
    contrib = np.array(xv * model.weights, dtype=float)
    top_idx = np.argsort(np.abs(contrib))[-2:][::-1]
    top_drivers = []
    for idx in top_idx:
        fname = model.feature_names[int(idx)]
        if fname == "log_shares":
            fname = "shares_available_log"
        top_drivers.append(
            {
                "feature": fname,
                "direction": "up_risk" if float(contrib[idx]) >= 0 else "down_risk",
                "strength": round(float(abs(contrib[idx])), 6),
            }
        )
    return p, top_drivers


def walk_forward_replay(
    panel: pd.DataFrame,
    *,
    min_train_rows: int = 200,
    refit_cadence_days: int = 7,
    min_eval_dates: int = 5,
) -> pd.DataFrame:
    if panel.empty:
        return panel
    panel = panel.copy()
    panel["date"] = pd.to_datetime(panel["date"], errors="coerce")
    panel = panel.dropna(subset=["date"]).sort_values(["date", "symbol"]).reset_index(drop=True)
    unique_dates = sorted(panel["date"].dropna().unique())
    if len(unique_dates) < min_eval_dates:
        return pd.DataFrame()

    refit_dates: set[pd.Timestamp] = {unique_dates[0]}
    last_refit = unique_dates[0]
    for d in unique_dates[1:]:
        if (d - last_refit).days >= refit_cadence_days:
            refit_dates.add(d)
            last_refit = d
    refit_dates.add(unique_dates[-1])

    model: LogisticV1Model | None = None
    results: list[pd.DataFrame] = []
    for eval_date in unique_dates:
        if eval_date in refit_dates:
            train = panel[panel["date"] < eval_date]
            if len(train) >= min_train_rows:
                model = fit_logistic_v1(train)
        if model is None:
            continue
        eval_rows = panel[panel["date"] == eval_date]
        if eval_rows.empty:
            continue
        p = score_rows(model, eval_rows)
        chunk = eval_rows.copy()
        chunk["p_replay"] = np.round(p, 6)
        chunk["risk_band"] = [risk_band(float(x)) for x in p]
        chunk["pred_date"] = chunk["date"].dt.strftime("%Y-%m-%d")
        chunk["y_spike"] = chunk["spike_event"].astype(int)
        chunk["model_train_rows"] = model.train_rows
        results.append(chunk)

    if not results:
        return pd.DataFrame()
    return pd.concat(results, ignore_index=True)


def build_borrow_spike_risk_payload(
    borrow_history_symbols: dict[str, list[dict]],
    as_of_date: str,
    horizon_days: int = HORIZON_DAYS_DEFAULT,
) -> dict:
    """Production payload: pooled logistic_v1 on full history, latest scores per symbol."""
    rows: list[dict] = []
    latest_feats: dict[str, dict] = {}

    for sym, hist in (borrow_history_symbols or {}).items():
        if not hist:
            continue
        s = _symbol_history_frame(hist)
        if len(s) < 12:
            continue
        s = apply_spike_labels(s, horizon_days=horizon_days, label_variant="L0")
        obs_count = int(s["borrow_current"].notna().sum())
        shares_obs_count = int(s["shares_available"].notna().sum())
        shares_cov = float(shares_obs_count / max(1, len(s)))
        quality_band = _quality_band(obs_count, shares_cov)

        usable = s.iloc[:-horizon_days].copy() if len(s) > horizon_days else s.iloc[0:0].copy()
        usable = usable.dropna(subset=["spike_event", "borrow_current", "med60", "p99_180"])
        if not usable.empty:
            temp = usable[FEATURE_COLS + ["spike_event"]].copy()
            temp["symbol"] = sym
            temp["date"] = usable["date"].dt.strftime("%Y-%m-%d")
            rows.extend(temp.to_dict("records"))

        latest = s.iloc[-1].copy()
        latest_date = latest["date"].strftime("%Y-%m-%d") if pd.notna(latest["date"]) else None
        lf = {c: latest.get(c) for c in FEATURE_COLS}
        recent_borrows = s["borrow_current"].dropna().tail(60)
        borrow_pctile_60 = None
        if not recent_borrows.empty and pd.notna(latest.get("borrow_current")):
            borrow_pctile_60 = float((recent_borrows <= float(latest["borrow_current"])).mean())
        lf["obs_count"] = obs_count
        lf["shares_obs_count"] = shares_obs_count
        lf["shares_coverage"] = shares_cov
        lf["quality_band"] = quality_band
        lf["scoring_eligible"] = bool(obs_count >= MIN_OBS_FOR_SCORING)
        lf["borrow_pctile_60"] = borrow_pctile_60
        lf["latest_date"] = latest_date
        latest_feats[str(sym).upper()] = lf

    if not rows:
        return {
            "as_of": as_of_date,
            "horizon_days": horizon_days,
            "model": {"name": "logistic_v1", "status": "insufficient_training_data"},
            "quality_gate": _quality_gate_payload(),
            "accuracy_tracking": {"status": "no_training_rows"},
            "symbols": {},
        }

    all_df = pd.DataFrame(rows)
    all_df["date"] = pd.to_datetime(all_df["date"], errors="coerce")
    all_df = all_df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    y_all = all_df["spike_event"].to_numpy(dtype=float)

    eval_metrics: dict = {"status": "insufficient_eval_window"}
    unique_dates = sorted({d for d in all_df["date"].dropna().tolist()})
    if len(unique_dates) >= 10:
        split_idx = max(1, int(len(unique_dates) * 0.80))
        eval_start = unique_dates[split_idx - 1]
        train_mask = all_df["date"] < eval_start
        eval_mask = all_df["date"] >= eval_start
        if int(train_mask.sum()) >= 40 and int(eval_mask.sum()) >= 20:
            train_model = fit_logistic_v1(all_df.loc[train_mask])
            if train_model is not None:
                p_eval = score_rows(train_model, all_df.loc[eval_mask])
                y_eval = y_all[eval_mask.to_numpy()]
                p_eval = np.clip(p_eval, 1e-6, 1.0 - 1e-6)
                brier = float(np.mean((p_eval - y_eval) ** 2))
                log_loss = float(-np.mean(y_eval * np.log(p_eval) + (1.0 - y_eval) * np.log(1.0 - p_eval)))
                pred_band_eval = np.array([risk_band(float(p)) for p in p_eval], dtype=object)
                calib = []
                for bname in ("low", "elevated", "high"):
                    mask = pred_band_eval == bname
                    n = int(mask.sum())
                    if n == 0:
                        continue
                    calib.append(
                        {
                            "band": bname,
                            "count": n,
                            "avg_pred": round(float(np.mean(p_eval[mask])), 6),
                            "realized_rate": round(float(np.mean(y_eval[mask])), 6),
                        }
                    )
                eval_metrics = {
                    "status": "ok" if int((y_eval > 0.5).sum()) > 0 else "ok_no_positive_events",
                    "method": "out_of_time_holdout",
                    "eval_start_date": pd.Timestamp(eval_start).strftime("%Y-%m-%d"),
                    "eval_rows": int(len(y_eval)),
                    "eval_positives": int((y_eval > 0.5).sum()),
                    "brier_score": round(brier, 6),
                    "log_loss": round(log_loss, 6),
                    "calibration_by_band": calib,
                }

    model = fit_logistic_v1(all_df)
    if model is None:
        return {
            "as_of": as_of_date,
            "horizon_days": horizon_days,
            "model": {"name": "logistic_v1", "status": "fit_failed"},
            "quality_gate": _quality_gate_payload(),
            "accuracy_tracking": eval_metrics,
            "symbols": {},
        }

    symbols_payload: dict[str, dict] = {}
    for sym, lf in latest_feats.items():
        eligible = bool(lf.get("scoring_eligible", False))
        if eligible and model is not None:
            p, top_drivers = score_feature_dict(model, lf)
        else:
            p, top_drivers = 0.0, []
        symbols_payload[sym] = {
            "p_spike_5d": round(p, 6) if eligible else None,
            "risk_band": risk_band(p) if eligible else "insufficient",
            "obs_count": int(lf.get("obs_count", 0)),
            "shares_obs_count": int(lf.get("shares_obs_count", 0)),
            "shares_coverage": round(float(lf.get("shares_coverage", 0.0)), 6),
            "quality_band": str(lf.get("quality_band", "insufficient")),
            "scoring_eligible": eligible,
            "borrow_pctile_60": round(float(lf.get("borrow_pctile_60")), 6)
            if lf.get("borrow_pctile_60") is not None
            else None,
            "asof_history_date": lf.get("latest_date"),
            "top_drivers": top_drivers,
        }

    return {
        "as_of": as_of_date,
        "horizon_days": horizon_days,
        "label_definition": LABEL_VARIANTS["L0"],
        "model": {
            "name": "logistic_v1",
            "status": "ok",
            "train_rows": model.train_rows,
            "positives": model.positives,
            "negatives": model.negatives,
            "positive_weight": model.positive_weight,
        },
        "quality_gate": _quality_gate_payload(),
        "accuracy_tracking": eval_metrics,
        "symbols": symbols_payload,
    }


def _quality_gate_payload() -> dict:
    return {
        "min_obs_for_scoring": MIN_OBS_FOR_SCORING,
        "bands": {
            "strong": "obs>=60 and shares_coverage>=0.70",
            "moderate": "obs>=30 and shares_coverage>=0.40",
            "insufficient": "otherwise",
        },
    }

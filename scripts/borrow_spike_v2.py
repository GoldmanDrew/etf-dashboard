#!/usr/bin/env python3
"""logistic_v2, isotonic calibration, alert tiers, dual-label scoring."""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Callable

import numpy as np
import pandas as pd

from borrow_metrics_enrichment import join_supply_to_panel, latest_supply_features_for_symbol
from borrow_spike_model import (
    FEATURE_COLS,
    HORIZON_DAYS_DEFAULT,
    LABEL_VARIANTS,
    MIN_OBS_FOR_SCORING,
    LogisticV1Model,
    _fit_logistic_l2,
    _quality_gate_payload,
    _sigmoid,
    _symbol_history_frame,
    apply_spike_labels,
    fit_logistic_v1,
    risk_band,
    score_rows,
    variant_needs_p99,
)

FEATURE_COLS_V2 = [
    "borrow_current",
    "borrow_z60",
    "borrow_slope5",
    "borrow_vol10",
    "borrow_pctile_60",
    "shares_drop1",
    "shares_drop3",
    "shares_drop5",
    "near_zero_shares",
    "utilization_proxy",
    "avail_to_adv",
    "log_aum",
    "turnover_20d",
]

ALERT_TIER_THRESHOLDS = {
    "watch": 0.05,
    "elevated": 0.12,
    "high": 0.25,
}


def _attach_boosting_scores(sym_payload: dict, sym_u: str, lf: dict) -> None:
    """Shadow/production boosting spike scores when bundle + registry allow."""
    try:
        from pathlib import Path
        import json
        from borrow_boosting_model import load_bundle, score_spike

        root = Path(__file__).resolve().parent.parent
        reg_path = root / "data" / "borrow_model_registry.json"
        if not reg_path.exists():
            return
        reg = json.loads(reg_path.read_text(encoding="utf-8"))
        if reg.get("spike_l2", {}).get("winner") != "boosting" and not reg.get("boosting", {}).get("gate_pass"):
            return
        bundle = load_bundle(root / "data" / "borrow_boosting_bundle.pkl")
        if bundle is None or bundle.spike_model is None:
            return
        row = pd.DataFrame([{**lf, "symbol": sym_u}])
        p = float(score_spike(bundle, row)[0])
        p_cal = bundle.calibrator.transform(p) if bundle.calibrator else p
        sym_payload["p_spike_5d_l2_boosting"] = round(p, 6)
        sym_payload["p_spike_5d_l2_boosting_calibrated"] = round(p_cal, 6)
        sym_payload["alert_tier_boosting"] = alert_tier(p_cal)
    except Exception:
        return


@dataclass(frozen=True)
class IsotonicCalibrator:
    bin_edges: tuple[float, ...]
    bin_calibrated: tuple[float, ...]

    def transform(self, p: float) -> float:
        if not math.isfinite(p):
            return 0.0
        edges = self.bin_edges
        vals = self.bin_calibrated
        if len(edges) < 2 or not vals:
            return float(np.clip(p, 0, 1))
        if p <= edges[0]:
            return float(vals[0])
        if p >= edges[-1]:
            return float(vals[-1])
        for i in range(len(edges) - 1):
            if edges[i] <= p < edges[i + 1]:
                return float(vals[min(i, len(vals) - 1)])
        return float(vals[-1])

    def transform_array(self, ps: np.ndarray) -> np.ndarray:
        return np.array([self.transform(float(x)) for x in ps], dtype=float)


def fit_isotonic_calibrator(
    ps: np.ndarray,
    ys: np.ndarray,
    *,
    n_bins: int = 12,
) -> IsotonicCalibrator | None:
    if len(ps) < 30 or len(set(int(y) for y in ys)) < 2:
        return None
    df = pd.DataFrame({"p": ps, "y": ys})
    df["bin"] = pd.cut(df["p"], bins=n_bins, labels=False, include_lowest=True)
    grp = df.groupby("bin", observed=False).agg(avg_p=("p", "mean"), avg_y=("y", "mean"), n=("y", "count"))
    grp = grp[grp["n"] > 0].sort_index()
    if grp.empty:
        return None
    y_vals = grp["avg_y"].to_numpy(dtype=float).copy()
    for i in range(1, len(y_vals)):
        y_vals[i] = max(y_vals[i], y_vals[i - 1])
    x_pts = grp["avg_p"].tolist() + [1.0]
    y_pts = y_vals.tolist() + [float(y_vals[-1])]
    edges = [0.0] + sorted(set(float(x) for x in x_pts))
    cal = [float(np.clip(np.interp(e, x_pts, y_pts), 0, 1)) for e in edges]
    return IsotonicCalibrator(bin_edges=tuple(edges), bin_calibrated=tuple(cal))


def alert_tier(p_calibrated: float | None) -> str:
    if p_calibrated is None or not math.isfinite(p_calibrated):
        return "none"
    if p_calibrated >= ALERT_TIER_THRESHOLDS["high"]:
        return "high"
    if p_calibrated >= ALERT_TIER_THRESHOLDS["elevated"]:
        return "elevated"
    if p_calibrated >= ALERT_TIER_THRESHOLDS["watch"]:
        return "watch"
    return "low"


def _prepare_X_v2(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    x = df.copy()
    for c in FEATURE_COLS_V2:
        if c not in x.columns:
            x[c] = 0.0
    x = x[FEATURE_COLS_V2].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return x, list(x.columns)


def fit_logistic_v2(train_df: pd.DataFrame) -> LogisticV1Model | None:
    if train_df.empty or len(train_df) < 40:
        return None
    y = train_df["spike_event"].to_numpy(dtype=float)
    x_raw, feat_names = _prepare_X_v2(train_df)
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


def score_rows_v2(model: LogisticV1Model, df: pd.DataFrame) -> np.ndarray:
    x_raw, _ = _prepare_X_v2(df)
    for c in model.feature_names:
        if c not in x_raw.columns:
            x_raw[c] = 0.0
    x = x_raw[list(model.feature_names)].to_numpy(dtype=float)
    x_s = (x - model.mean) / model.std
    p = _sigmoid(x_s @ model.weights + model.bias)
    return np.clip(p, 0.0, 1.0)


def build_enriched_panel(
    borrow_symbols: dict[str, list[dict]],
    *,
    horizon_days: int = HORIZON_DAYS_DEFAULT,
    label_variant: str = "L2",
    as_of_max: str | None = None,
) -> pd.DataFrame:
    rows: list[dict] = []
    for sym, hist in (borrow_symbols or {}).items():
        if not hist:
            continue
        s = _symbol_history_frame(hist)
        if len(s) < 12:
            continue
        s = apply_spike_labels(s, horizon_days=horizon_days, label_variant=label_variant)
        obs_count = int(s["borrow_current"].notna().sum())
        recent_borrows = s["borrow_current"].dropna().tail(60)
        usable = s.iloc[:-horizon_days].copy() if len(s) > horizon_days else s.iloc[0:0].copy()
        usable = usable.dropna(subset=["spike_event", "borrow_current", "med60"])
        if variant_needs_p99(label_variant):
            usable = usable.dropna(subset=["p99_180"])
        for _, row in usable.iterrows():
            rec: dict[str, Any] = {
                "symbol": str(sym).upper(),
                "date": row["date"].strftime("%Y-%m-%d"),
                "spike_event": float(row["spike_event"]),
                "obs_count": obs_count,
                "scoring_eligible": obs_count >= MIN_OBS_FOR_SCORING,
            }
            for c in FEATURE_COLS:
                v = row.get(c)
                rec[c] = float(v) if v is not None and pd.notna(v) and np.isfinite(v) else 0.0
            if not recent_borrows.empty and pd.notna(row.get("borrow_current")):
                rec["borrow_pctile_60"] = float((recent_borrows <= float(row["borrow_current"])).mean())
            else:
                rec["borrow_pctile_60"] = 0.0
            rows.append(rec)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = join_supply_to_panel(df)
    if as_of_max:
        df = df[df["date"] <= pd.Timestamp(as_of_max)].reset_index(drop=True)
    return df.dropna(subset=["date"]).sort_values(["date", "symbol"]).reset_index(drop=True)


def walk_forward_replay_model(
    panel: pd.DataFrame,
    *,
    fit_fn: Callable[[pd.DataFrame], LogisticV1Model | None],
    score_fn: Callable[[LogisticV1Model, pd.DataFrame], np.ndarray],
    model_name: str = "logistic",
    min_train_rows: int = 200,
    refit_cadence_days: int = 7,
    min_eval_dates: int = 5,
    calibrate: bool = False,
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
    calibrator: IsotonicCalibrator | None = None
    results: list[pd.DataFrame] = []
    for eval_date in unique_dates:
        if eval_date in refit_dates:
            train = panel[panel["date"] < eval_date]
            if len(train) >= min_train_rows:
                model = fit_fn(train)
                if calibrate and model is not None:
                    p_train = score_fn(model, train)
                    y_train = train["spike_event"].to_numpy(dtype=float)
                    calibrator = fit_isotonic_calibrator(p_train, y_train)
        if model is None:
            continue
        eval_rows = panel[panel["date"] == eval_date]
        if eval_rows.empty:
            continue
        p = score_fn(model, eval_rows)
        chunk = eval_rows.copy()
        chunk["p_replay"] = np.round(p, 6)
        chunk["model"] = model_name
        if calibrator is not None:
            p_cal = calibrator.transform_array(p)
            chunk["p_replay_calibrated"] = np.round(p_cal, 6)
            chunk["alert_tier"] = [alert_tier(float(x)) for x in p_cal]
            chunk["risk_band"] = chunk["alert_tier"]
        else:
            chunk["p_replay_calibrated"] = chunk["p_replay"]
            chunk["risk_band"] = [risk_band(float(x)) for x in p]
            chunk["alert_tier"] = chunk["risk_band"]
        chunk["pred_date"] = chunk["date"].dt.strftime("%Y-%m-%d")
        chunk["y_spike"] = chunk["spike_event"].astype(int)
        chunk["model_train_rows"] = model.train_rows
        results.append(chunk)

    if not results:
        return pd.DataFrame()
    return pd.concat(results, ignore_index=True)


def _ece_simple(ps: np.ndarray, ys: np.ndarray, n_bins: int = 10) -> float | None:
    if len(ps) < 10:
        return None
    df = pd.DataFrame({"p": ps, "y": ys})
    df["bin"] = pd.cut(df["p"], bins=n_bins, labels=False, include_lowest=True)
    ece = 0.0
    n = len(df)
    for _, g in df.groupby("bin", observed=False):
        if g.empty:
            continue
        w = len(g) / n
        ece += w * abs(float(g["p"].mean()) - float(g["y"].mean()))
    return round(ece, 6)


def build_extended_risk_payload(
    borrow_history_symbols: dict[str, list[dict]],
    as_of_date: str,
    *,
    horizon_days: int = HORIZON_DAYS_DEFAULT,
) -> dict[str, Any]:
    from borrow_spike_model import build_borrow_spike_risk_payload

    base = build_borrow_spike_risk_payload(borrow_history_symbols, as_of_date, horizon_days=horizon_days)
    panel_l2 = build_enriched_panel(borrow_history_symbols, horizon_days=horizon_days, label_variant="L2")
    v2_model: LogisticV1Model | None = None
    calibrator: IsotonicCalibrator | None = None
    v2_eval: dict[str, Any] = {"status": "insufficient_training_data"}

    if not panel_l2.empty and len(panel_l2) >= 40:
        dates = sorted(panel_l2["date"].dropna().unique())
        if len(dates) >= 10:
            split_idx = max(1, int(len(dates) * 0.80))
            eval_start = dates[split_idx - 1]
            train = panel_l2[panel_l2["date"] < eval_start]
            ev = panel_l2[panel_l2["date"] >= eval_start]
            v2_model = fit_logistic_v2(train)
            if v2_model is not None and not ev.empty:
                p_raw = score_rows_v2(v2_model, ev)
                y_ev = ev["spike_event"].to_numpy(dtype=float)
                calibrator = fit_isotonic_calibrator(p_raw, y_ev)
                p_cal = calibrator.transform_array(p_raw) if calibrator else p_raw
                v2_eval = {
                    "status": "ok",
                    "label_variant": "L2",
                    "eval_rows": int(len(ev)),
                    "eval_positives": int((y_ev > 0.5).sum()),
                    "ece_raw": _ece_simple(p_raw, y_ev),
                    "ece_calibrated": _ece_simple(p_cal, y_ev),
                }
        if v2_model is None:
            v2_model = fit_logistic_v2(panel_l2)
        if v2_model is not None and calibrator is None:
            p_all = score_rows_v2(v2_model, panel_l2)
            y_all = panel_l2["spike_event"].to_numpy(dtype=float)
            calibrator = fit_isotonic_calibrator(p_all, y_all)

    symbols = dict(base.get("symbols") or {})
    for sym, hist in (borrow_history_symbols or {}).items():
        sym_u = str(sym).upper()
        if sym_u not in symbols:
            continue
        s = _symbol_history_frame(hist)
        if len(s) < 12:
            continue
        latest = s.iloc[-1]
        lf = {c: latest.get(c) for c in FEATURE_COLS}
        recent_borrows = s["borrow_current"].dropna().tail(60)
        if not recent_borrows.empty and pd.notna(latest.get("borrow_current")):
            lf["borrow_pctile_60"] = float((recent_borrows <= float(latest["borrow_current"])).mean())
        else:
            lf["borrow_pctile_60"] = 0.0
        supply = latest_supply_features_for_symbol(
            sym_u,
            as_of_date,
            hist_shares_available=float(latest.get("shares_available") or 0),
        )
        lf.update(supply)
        if v2_model is not None and symbols[sym_u].get("scoring_eligible"):
            p2 = float(score_rows_v2(v2_model, pd.DataFrame([lf]))[0])
            p2c = calibrator.transform(p2) if calibrator else p2
            symbols[sym_u]["p_spike_5d_l2"] = round(p2, 6)
            symbols[sym_u]["p_spike_5d_l2_calibrated"] = round(p2c, 6)
            symbols[sym_u]["alert_tier"] = alert_tier(p2c)
            symbols[sym_u]["supply_data_grade"] = supply.get("supply_data_grade")
            symbols[sym_u]["risk_band"] = symbols[sym_u]["alert_tier"]
            _attach_boosting_scores(symbols[sym_u], sym_u, lf)
        else:
            symbols[sym_u]["p_spike_5d_l2"] = None
            symbols[sym_u]["p_spike_5d_l2_calibrated"] = None
            symbols[sym_u]["alert_tier"] = "none"

    base["symbols"] = symbols
    base["models"] = {
        "logistic_v1_l0": base.get("model") or {},
        "logistic_v2_l2": {
            "name": "logistic_v2",
            "label_variant": "L2",
            "features": FEATURE_COLS_V2,
            "status": "ok" if v2_model else "fit_failed",
            "train_rows": v2_model.train_rows if v2_model else 0,
            "positives": v2_model.positives if v2_model else 0,
        },
    }
    base["calibration"] = {
        "method": "isotonic_bins",
        "alert_tier_thresholds": ALERT_TIER_THRESHOLDS,
        "fitted": calibrator is not None,
        "v2_eval": v2_eval,
    }
    base["label_definitions"] = {
        "headline_l0": LABEL_VARIANTS["L0"],
        "model_dev_l2": LABEL_VARIANTS["L2"],
    }
    return base

#!/usr/bin/env python3
"""Analyze borrow predictor panel: lead-lag ranks, OLS R², block ablation, logistic spikes."""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from borrow_spike_model import (  # noqa: E402
    FEATURE_COLS,
    LogisticV1Model,
    _fit_logistic_l2,
    _sigmoid,
    risk_band,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
PANEL_PARQUET = DATA_DIR / "borrow_predictor_panel.parquet"
LEADLAG_JSON = DATA_DIR / "borrow_predictor_leadlag.json"
IMPORTANCE_JSON = DATA_DIR / "borrow_feature_importance.json"
SUMMARY_JSON = DATA_DIR / "borrow_predictor_study_summary.json"

TARGET_HORIZONS = (1, 3, 5, 10)
PRIMARY_SPIKE_TARGET = "y_spike_5"
PRECISION_AT_K = 10

BORROW_BLOCK = [
    "borrow_current",
    "borrow_z60",
    "borrow_slope5",
    "borrow_vol10",
    "borrow_pctile_60",
]
SUPPLY_BLOCK = [
    "shares_available",
    "shares_drop1",
    "shares_drop3",
    "shares_drop5",
    "utilization_proxy",
    "avail_to_adv",
]
SCALE_BLOCK = ["log_aum", "turnover_20d", "prem_disc_bps"]
FLOAT_BLOCK = ["tradable_float_shares", "etf_aum_over_float", "rebalance_pct_adv"]
PEER_BLOCK = ["peer_borrow_z_mean", "peer_shares_drop3_mean", "peer_shares_avail_sum"]
SCREENER_BLOCK = [
    "delta",
    "leverage",
    "net_edge_p50",
    "gross_decay_annual",
    "forecast_vol_underlying_annual",
]

FEATURE_BLOCKS: dict[str, list[str]] = {
    "borrow_only": BORROW_BLOCK,
    "borrow_plus_supply": BORROW_BLOCK + SUPPLY_BLOCK,
    "borrow_plus_scale": BORROW_BLOCK + SUPPLY_BLOCK + SCALE_BLOCK,
    "borrow_plus_float": BORROW_BLOCK + SUPPLY_BLOCK + SCALE_BLOCK + FLOAT_BLOCK,
    "borrow_plus_peer": BORROW_BLOCK + SUPPLY_BLOCK + SCALE_BLOCK + FLOAT_BLOCK + PEER_BLOCK,
    "full_plus_screener": (
        BORROW_BLOCK + SUPPLY_BLOCK + SCALE_BLOCK + FLOAT_BLOCK + PEER_BLOCK + SCREENER_BLOCK
    ),
}

V1_FEATURES = list(FEATURE_COLS)  # production logistic_v1 baseline


def _load_panel(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df.dropna(subset=["date"]).sort_values(["date", "symbol"]).reset_index(drop=True)


def _feature_list(panel: pd.DataFrame) -> list[str]:
    candidates = (
        BORROW_BLOCK
        + SUPPLY_BLOCK
        + SCALE_BLOCK
        + FLOAT_BLOCK
        + PEER_BLOCK
        + SCREENER_BLOCK
    )
    return [c for c in candidates if c in panel.columns]


def _target_list() -> list[str]:
    out: list[str] = []
    for h in TARGET_HORIZONS:
        out.extend([f"delta_borrow_{h}", f"max_borrow_jump_{h}", f"y_spike_{h}"])
    return out


def cross_sectional_rank_corr(
    panel: pd.DataFrame,
    feature: str,
    target: str,
    *,
    min_names: int = 8,
) -> float | None:
    if feature not in panel.columns or target not in panel.columns:
        return None
    corrs: list[float] = []
    for _, grp in panel.groupby("date"):
        sub = grp[[feature, target]].dropna()
        if len(sub) < min_names:
            continue
        if sub[feature].nunique() < 2 or sub[target].nunique() < 2:
            continue
        rf = sub[feature].rank(method="average")
        rt = sub[target].rank(method="average")
        c = rf.corr(rt)
        if c is not None and np.isfinite(c):
            corrs.append(float(c))
    if not corrs:
        return None
    return float(np.mean(corrs))


def univariate_r2(x: np.ndarray, y: np.ndarray) -> float | None:
    mask = np.isfinite(x) & np.isfinite(y)
    if int(mask.sum()) < 30:
        return None
    xv = x[mask]
    yv = y[mask]
    if np.std(xv) < 1e-12:
        return None
    x_mean = float(xv.mean())
    y_mean = float(yv.mean())
    ss_xx = float(np.sum((xv - x_mean) ** 2))
    if ss_xx < 1e-12:
        return None
    beta = float(np.sum((xv - x_mean) * (yv - y_mean)) / ss_xx)
    alpha = y_mean - beta * x_mean
    yhat = alpha + beta * xv
    ss_tot = float(np.sum((yv - y_mean) ** 2))
    if ss_tot < 1e-12:
        return None
    ss_res = float(np.sum((yv - yhat) ** 2))
    return float(1.0 - ss_res / ss_tot)


def _auroc(ps: list[float], ys: list[float]) -> float | None:
    if not ps or len(set(int(y) for y in ys)) < 2:
        return None
    pairs = sorted(zip(ps, ys), key=lambda x: x[0], reverse=True)
    n_pos = sum(ys)
    n_neg = len(ys) - n_pos
    if n_pos == 0 or n_neg == 0:
        return None
    tp = fp = 0
    prev_p = None
    auc = 0.0
    tpr_prev = fpr_prev = 0.0
    for p, y in pairs:
        if prev_p is not None and p != prev_p:
            tpr = tp / n_pos
            fpr = fp / n_neg
            auc += (fpr - fpr_prev) * (tpr + tpr_prev) / 2.0
            tpr_prev, fpr_prev = tpr, fpr
        if y > 0.5:
            tp += 1
        else:
            fp += 1
        prev_p = p
    tpr = tp / n_pos
    fpr = fp / n_neg
    auc += (fpr - fpr_prev) * (tpr + tpr_prev) / 2.0
    return float(auc)


def _precision_at_k_by_date(df: pd.DataFrame, p_col: str, y_col: str, k: int) -> float | None:
    if df.empty:
        return None
    hits = total = 0
    for _, grp in df.groupby("date"):
        g = grp.sort_values(p_col, ascending=False).head(k)
        if g.empty:
            continue
        hits += int(g[y_col].sum())
        total += len(g)
    if total == 0:
        return None
    return float(hits / total)


def _prepare_block_X(df: pd.DataFrame, feature_cols: list[str]) -> tuple[pd.DataFrame, list[str]]:
    cols = [c for c in feature_cols if c in df.columns]
    if not cols:
        return pd.DataFrame(), []
    x = df[cols].copy()
    if "shares_available" in x.columns and "log_shares" not in x.columns:
        x["log_shares"] = np.log1p(x["shares_available"].clip(lower=0))
        if "shares_available" in feature_cols:
            x = x.drop(columns=["shares_available"], errors="ignore")
    x = x.replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return x, list(x.columns)


def fit_logistic_features(
    train_df: pd.DataFrame,
    feature_cols: list[str],
    *,
    target_col: str = PRIMARY_SPIKE_TARGET,
    min_rows: int = 80,
) -> LogisticV1Model | None:
    if train_df.empty or target_col not in train_df.columns or len(train_df) < min_rows:
        return None
    work = train_df.dropna(subset=[target_col])
    if len(work) < min_rows:
        return None
    y = work[target_col].to_numpy(dtype=float)
    x_raw, feat_names = _prepare_block_X(work, feature_cols)
    if not feat_names:
        return None
    x_train = x_raw.to_numpy(dtype=float)
    mean_v = x_train.mean(axis=0)
    std_v = x_train.std(axis=0)
    std_v = np.where(std_v < 1e-9, 1.0, std_v)
    x_train_s = (x_train - mean_v) / std_v
    pos_n = int((y > 0.5).sum())
    neg_n = int((y <= 0.5).sum())
    if pos_n == 0 or neg_n == 0:
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
        train_rows=int(len(work)),
        positives=pos_n,
        negatives=neg_n,
        positive_weight=round(pos_w, 4),
    )


def score_block(model: LogisticV1Model, df: pd.DataFrame) -> np.ndarray:
    feat_cols = list(model.feature_names)
    x_raw = df[feat_cols].copy() if all(c in df.columns for c in feat_cols) else pd.DataFrame(index=df.index)
    if "log_shares" in feat_cols and "log_shares" not in x_raw.columns:
        sh = df["shares_available"] if "shares_available" in df.columns else 0.0
        x_raw["log_shares"] = np.log1p(pd.to_numeric(sh, errors="coerce").fillna(0).clip(lower=0))
    for c in feat_cols:
        if c not in x_raw.columns:
            x_raw[c] = 0.0
    x_raw = x_raw[feat_cols].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    x = x_raw.to_numpy(dtype=float)
    x_s = (x - model.mean) / model.std
    p = _sigmoid(x_s @ model.weights + model.bias)
    return np.clip(p, 0.0, 1.0)


def _time_split(panel: pd.DataFrame, holdout_frac: float = 0.20) -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = sorted(panel["date"].dropna().unique())
    if len(dates) < 10:
        return panel.iloc[0:0], panel.iloc[0:0]
    split_idx = max(1, int(len(dates) * (1.0 - holdout_frac)))
    cutoff = dates[split_idx - 1]
    train = panel[panel["date"] < cutoff]
    test = panel[panel["date"] >= cutoff]
    return train, test


def leadlag_matrix(panel: pd.DataFrame) -> dict[str, Any]:
    features = _feature_list(panel)
    targets = [t for t in _target_list() if t in panel.columns]
    matrix: dict[str, dict[str, float | None]] = {}
    for feat in features:
        matrix[feat] = {}
        for tgt in targets:
            matrix[feat][tgt] = cross_sectional_rank_corr(panel, feat, tgt)
    return {
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "method": "cross_sectional_spearman_mean",
        "features": features,
        "targets": targets,
        "matrix": matrix,
    }


def univariate_r2_table(panel: pd.DataFrame) -> dict[str, dict[str, float | None]]:
    features = _feature_list(panel)
    targets = [t for t in _target_list() if t in panel.columns]
    out: dict[str, dict[str, float | None]] = {}
    for feat in features:
        out[feat] = {}
        x = panel[feat].to_numpy(dtype=float)
        for tgt in targets:
            y = panel[tgt].to_numpy(dtype=float)
            r2 = univariate_r2(x, y)
            out[feat][tgt] = round(r2, 6) if r2 is not None else None
    return out


def block_ablation(panel: pd.DataFrame, *, target_col: str = PRIMARY_SPIKE_TARGET) -> dict[str, Any]:
    work = panel.dropna(subset=[target_col]).copy()
    if work.empty:
        return {"status": "empty", "blocks": {}}
    train, test = _time_split(work)
    if train.empty or test.empty:
        return {"status": "insufficient_dates", "blocks": {}}

    base_rate = float(train[target_col].mean())
    results: dict[str, Any] = {}
    baseline_auroc: float | None = None

    for block_name, cols in FEATURE_BLOCKS.items():
        model = fit_logistic_features(train, cols, target_col=target_col)
        if model is None:
            results[block_name] = {"status": "fit_failed", "features": cols}
            continue
        x_test, _ = _prepare_block_X(test, cols)
        eval_df = test.copy()
        for c in model.feature_names:
            if c in x_test.columns:
                eval_df[c] = x_test[c].values
        p = score_block(model, eval_df)
        y = test[target_col].to_numpy(dtype=float)
        auroc = _auroc([float(x) for x in p], [float(x) for x in y])
        prec10 = _precision_at_k_by_date(
            test.assign(_p=p),
            "_p",
            target_col,
            PRECISION_AT_K,
        )
        lift = None
        if base_rate > 0 and prec10 is not None:
            lift = round(prec10 / base_rate, 4)
        entry = {
            "status": "ok",
            "features": list(model.feature_names),
            "train_rows": model.train_rows,
            "test_rows": int(len(test)),
            "test_positives": int((y > 0.5).sum()),
            "auroc": round(auroc, 6) if auroc is not None else None,
            f"precision_at_{PRECISION_AT_K}": round(prec10, 6) if prec10 is not None else None,
            "precision_lift_vs_base": lift,
            "base_rate_train": round(base_rate, 6),
        }
        if block_name == "borrow_only":
            baseline_auroc = auroc
        if baseline_auroc is not None and auroc is not None and block_name != "borrow_only":
            entry["auroc_delta_vs_borrow_only"] = round(auroc - baseline_auroc, 6)
        results[block_name] = entry

    # Production v1 feature set for reference
    v1_model = fit_logistic_features(train, V1_FEATURES, target_col=target_col)
    if v1_model is not None:
        x_test, _ = _prepare_block_X(test, V1_FEATURES)
        eval_df = test.copy()
        for c in v1_model.feature_names:
            if c in x_test.columns:
                eval_df[c] = x_test[c].values
        p = score_block(v1_model, eval_df)
        y = test[target_col].to_numpy(dtype=float)
        results["logistic_v1_reference"] = {
            "status": "ok",
            "features": list(v1_model.feature_names),
            "auroc": round(_auroc([float(x) for x in p], [float(x) for x in y]) or 0.0, 6),
            f"precision_at_{PRECISION_AT_K}": round(
                _precision_at_k_by_date(test.assign(_p=p), "_p", target_col, PRECISION_AT_K) or 0.0,
                6,
            ),
        }

    return {
        "status": "ok",
        "target": target_col,
        "split": "out_of_time_80_20",
        "blocks": results,
    }


def _top_predictors(leadlag: dict[str, Any], r2_table: dict[str, dict[str, float | None]]) -> dict[str, list[dict]]:
    matrix = leadlag.get("matrix") or {}
    by_horizon: dict[str, list[dict]] = {}
    for h in TARGET_HORIZONS:
        spike_tgt = f"y_spike_{h}"
        jump_tgt = f"max_borrow_jump_{h}"
        scored: list[tuple[str, float]] = []
        for feat, row in matrix.items():
            vals = []
            for tgt in (spike_tgt, jump_tgt):
                v = row.get(tgt)
                if v is not None and np.isfinite(v):
                    vals.append(abs(float(v)))
            if vals:
                scored.append((feat, float(np.mean(vals))))
        scored.sort(key=lambda x: x[1], reverse=True)
        by_horizon[f"h{h}"] = [
            {"feature": f, "mean_abs_rank_corr": round(s, 6)} for f, s in scored[:8]
        ]
    # Also surface best univariate R² for spike_5
    if r2_table:
        spike5 = [(f, r2_table[f].get(PRIMARY_SPIKE_TARGET)) for f in r2_table]
        spike5 = [(f, v) for f, v in spike5 if v is not None]
        spike5.sort(key=lambda x: x[1], reverse=True)
        by_horizon["univariate_r2_y_spike_5"] = [
            {"feature": f, "r2": v} for f, v in spike5[:8]
        ]
    return by_horizon


def _recommended_v2_features(
    leadlag: dict[str, Any],
    importance: dict[str, Any],
) -> list[str]:
    matrix = leadlag.get("matrix") or {}
    spike5_scores: list[tuple[str, float]] = []
    for feat, row in matrix.items():
        v = row.get(PRIMARY_SPIKE_TARGET)
        if v is not None and np.isfinite(v):
            spike5_scores.append((feat, abs(float(v))))
    spike5_scores.sort(key=lambda x: x[1], reverse=True)

    blocks = (importance.get("blocks") or {})
    block_order = sorted(
        blocks.items(),
        key=lambda kv: (kv[1].get("auroc") or 0.0),
        reverse=True,
    )
    block_feats: list[str] = []
    for name, info in block_order:
        if name == "borrow_only":
            continue
        for f in info.get("features") or []:
            if f not in block_feats:
                block_feats.append(f)

    recommended: list[str] = []
    for f, _ in spike5_scores:
        if f not in recommended:
            recommended.append(f)
    for f in block_feats:
        if f not in recommended:
            recommended.append(f)
    # Always keep production borrow dynamics
    for f in BORROW_BLOCK:
        if f not in recommended:
            recommended.append(f)
    return recommended[:20]


def build_study_summary(
    panel: pd.DataFrame,
    leadlag: dict[str, Any],
    importance: dict[str, Any],
    r2_table: dict[str, dict[str, float | None]],
) -> dict[str, Any]:
    top_by_horizon = _top_predictors(leadlag, r2_table)
    recommended = _recommended_v2_features(leadlag, importance)
    blocks = importance.get("blocks") or {}
    best_block = None
    best_auroc = -1.0
    for name, info in blocks.items():
        if name == "logistic_v1_reference":
            continue
        a = info.get("auroc")
        if a is not None and float(a) > best_auroc:
            best_auroc = float(a)
            best_block = name

    borrow_only = blocks.get("borrow_only") or {}
    full = blocks.get("full_plus_screener") or {}
    auroc_lift = None
    if borrow_only.get("auroc") is not None and full.get("auroc") is not None:
        auroc_lift = round(float(full["auroc"]) - float(borrow_only["auroc"]), 6)

    peer_lift = None
    peer = blocks.get("borrow_plus_peer") or {}
    if borrow_only.get("auroc") is not None and peer.get("auroc") is not None:
        peer_lift = round(float(peer["auroc"]) - float(borrow_only["auroc"]), 6)

    supply_lift = None
    supply = blocks.get("borrow_plus_supply") or {}
    if borrow_only.get("auroc") is not None and supply.get("auroc") is not None:
        supply_lift = round(float(supply["auroc"]) - float(borrow_only["auroc"]), 6)

    findings: list[str] = []
    if supply_lift is not None and supply_lift > 0.01:
        findings.append(
            f"Supply block lifts AUROC by {supply_lift:.3f} vs borrow-only baseline."
        )
    if peer_lift is not None and peer_lift > 0.005:
        findings.append(
            f"Peer-underlying basket adds {peer_lift:.3f} AUROC over float block."
        )
    if auroc_lift is not None and auroc_lift > 0.01:
        findings.append(
            f"Full panel (+screener) AUROC delta vs borrow-only: {auroc_lift:.3f}."
        )
    if top_by_horizon.get("h5"):
        top3 = ", ".join(x["feature"] for x in top_by_horizon["h5"][:3])
        findings.append(f"Top rank-corr predictors at h=5: {top3}.")
    if not findings:
        findings.append("Borrow dynamics dominate; extended blocks show modest incremental lift.")

    guidance = (
        "Use recommended_v2_features for logistic_v2 candidate set. "
        "Prioritize supply (utilization_proxy, shares_drop3) and peer_borrow_z_mean "
        "when re-running borrow_spike_eval; keep L0 labels and out-of-time splits."
    )
    if best_block:
        guidance += f" Best ablation block: {best_block} (AUROC {best_auroc:.4f})."

    return {
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "panel_rows": int(len(panel)),
        "panel_symbols": int(panel["symbol"].nunique()) if not panel.empty else 0,
        "recommended_v2_features": recommended,
        "top_predictors_by_horizon": top_by_horizon,
        "guidance_for_spike_model": guidance,
        "key_findings": findings,
        "best_block_ablation": best_block,
        "block_auroc_lifts": {
            "supply_vs_borrow_only": supply_lift,
            "peer_vs_borrow_only": peer_lift,
            "full_vs_borrow_only": auroc_lift,
        },
    }


def run_analysis(panel: pd.DataFrame) -> tuple[dict, dict, dict]:
    leadlag = leadlag_matrix(panel)
    r2_table = univariate_r2_table(panel)
    importance = {
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "univariate_r2": r2_table,
        **block_ablation(panel),
    }
    summary = build_study_summary(panel, leadlag, importance, r2_table)
    return leadlag, importance, summary


def write_outputs(
    leadlag: dict[str, Any],
    importance: dict[str, Any],
    summary: dict[str, Any],
    *,
    leadlag_path: Path = LEADLAG_JSON,
    importance_path: Path = IMPORTANCE_JSON,
    summary_path: Path = SUMMARY_JSON,
) -> None:
    for path in (leadlag_path, importance_path, summary_path):
        path.parent.mkdir(parents=True, exist_ok=True)
    leadlag_path.write_text(json.dumps(leadlag, indent=2), encoding="utf-8")
    importance_path.write_text(json.dumps(importance, indent=2), encoding="utf-8")
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze borrow predictor panel.")
    parser.add_argument("--repo-root", type=str, default=str(REPO_ROOT))
    parser.add_argument("--panel-path", type=str, default=None)
    parser.add_argument("--fail-if-no-panel", action="store_true")
    args = parser.parse_args()

    repo_root = Path(args.repo_root)
    panel_path = Path(args.panel_path) if args.panel_path else repo_root / "data" / "borrow_predictor_panel.parquet"
    panel = _load_panel(panel_path)
    if panel.empty:
        if args.fail_if_no_panel:
            raise SystemExit(f"panel not found or empty: {panel_path}")
        print(f"panel not found or empty: {panel_path}")
        return

    leadlag, importance, summary = run_analysis(panel)
    write_outputs(
        leadlag,
        importance,
        summary,
        leadlag_path=repo_root / "data" / "borrow_predictor_leadlag.json",
        importance_path=repo_root / "data" / "borrow_feature_importance.json",
        summary_path=repo_root / "data" / "borrow_predictor_study_summary.json",
    )
    print(
        f"analyzed {len(panel)} rows; "
        f"best block={summary.get('best_block_ablation')}; "
        f"recommended_v2={len(summary.get('recommended_v2_features') or [])} features"
    )


if __name__ == "__main__":
    main()

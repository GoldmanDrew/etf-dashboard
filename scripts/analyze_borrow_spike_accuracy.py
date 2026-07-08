#!/usr/bin/env python3
"""Borrow spike accuracy metrics: replay panel + live production predictions."""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from borrow_spike_model import LABEL_VARIANTS, risk_band  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
REPLAY_PARQUET = DATA_DIR / "borrow_spike_replay_panel.parquet"
REPLAY_L2_PARQUET = DATA_DIR / "borrow_spike_replay_l2_panel.parquet"
REALIZED_JSONL = DATA_DIR / "borrow_spike_realized.jsonl"
EVAL_JSON = DATA_DIR / "borrow_spike_eval.json"
FORECAST_JSON = DATA_DIR / "borrow_forecast_latest.json"
TRACKING_JSON = DATA_DIR / "borrow_spike_tracking.json"
PREDICTOR_STUDY_SUMMARY_JSON = DATA_DIR / "borrow_predictor_study_summary.json"
PREDICTOR_FEATURE_IMPORTANCE_JSON = DATA_DIR / "borrow_feature_importance.json"
PREDICTOR_LEADLAG_JSON = DATA_DIR / "borrow_predictor_leadlag.json"

# CI floor: require precision@10 >= 2x base rate when enough positives exist.
PRECISION_AT_K = (5, 10, 20)
CALIBRATION_BINS = 10
DEFAULT_MIN_POSITIVES_FOR_GATE = 5
DEFAULT_PRECISION_LIFT_FLOOR = 2.0


def _sigmoid_log_loss(p: float, y: float) -> float:
    p = min(1.0 - 1e-9, max(1e-9, p))
    return -(y * math.log(p) + (1.0 - y) * math.log(1.0 - p))


def _load_realized_jsonl(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    rows: list[dict] = []
    for ln in path.read_text(encoding="utf-8").splitlines():
        ln = ln.strip()
        if not ln:
            continue
        try:
            rows.append(json.loads(ln))
        except json.JSONDecodeError:
            continue
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


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


def _pr_auc(ps: list[float], ys: list[float]) -> float | None:
    if not ps or sum(ys) == 0:
        return None
    pairs = sorted(zip(ps, ys), key=lambda x: x[0], reverse=True)
    n_pos = sum(ys)
    tp = 0
    prev_p = None
    auc = 0.0
    prec_prev = recl_prev = 0.0
    for i, (p, y) in enumerate(pairs, start=1):
        if y > 0.5:
            tp += 1
        prec = tp / i
        recl = tp / n_pos
        if prev_p is not None and p != prev_p:
            auc += (recl - recl_prev) * (prec + prec_prev) / 2.0
            prec_prev, recl_prev = prec, recl
        prev_p = p
    auc += (1.0 - recl_prev) * (prec + prec_prev) / 2.0
    return float(auc)


def _precision_at_k_by_date(df: pd.DataFrame, p_col: str, y_col: str, k: int) -> float | None:
    if df.empty or p_col not in df.columns or y_col not in df.columns:
        return None
    date_col = "pred_date" if "pred_date" in df.columns else "date"
    if date_col not in df.columns:
        return None
    hits = 0
    total = 0
    for _, grp in df.groupby(date_col):
        g = grp.sort_values(p_col, ascending=False).head(k)
        if g.empty:
            continue
        hits += int(g[y_col].sum())
        total += len(g)
    if total == 0:
        return None
    return float(hits / total)


def _lift_top_decile(df: pd.DataFrame, p_col: str, y_col: str) -> float | None:
    if df.empty or len(df) < 20:
        return None
    base = float(df[y_col].mean())
    if base <= 0:
        return None
    cutoff = df[p_col].quantile(0.90)
    top = df[df[p_col] >= cutoff]
    if top.empty:
        return None
    return float(top[y_col].mean() / base)


def _calibration_bins(ps: list[float], ys: list[float], n_bins: int = 10) -> list[dict]:
    if not ps:
        return []
    df = pd.DataFrame({"p": ps, "y": ys})
    df["bin"] = pd.cut(df["p"], bins=n_bins, labels=False, include_lowest=True)
    out: list[dict] = []
    for b, grp in df.groupby("bin", observed=False):
        if grp.empty:
            continue
        out.append({
            "bin": int(b) if pd.notna(b) else -1,
            "count": int(len(grp)),
            "avg_pred": round(float(grp["p"].mean()), 6),
            "realized_rate": round(float(grp["y"].mean()), 6),
        })
    return out


def _ece_mce(bins: list[dict]) -> tuple[float | None, float | None]:
    if not bins:
        return None, None
    n = sum(b["count"] for b in bins)
    if n == 0:
        return None, None
    errs = []
    for b in bins:
        w = b["count"] / n
        errs.append(w * abs(b["avg_pred"] - b["realized_rate"]))
    ece = float(sum(errs))
    mce = float(max(abs(b["avg_pred"] - b["realized_rate"]) for b in bins))
    return round(ece, 6), round(mce, 6)


def _band_calibration(df: pd.DataFrame, p_col: str, y_col: str) -> list[dict]:
    if df.empty:
        return []
    bands = []
    for _, row in df.iterrows():
        p = float(row[p_col])
        bands.append((p, float(row[y_col]), risk_band(p)))
    by_band: dict[str, list[tuple[float, float]]] = {}
    for p, y, b in bands:
        by_band.setdefault(b, []).append((p, y))
    calib = []
    for bname in sorted(by_band):
        pairs = by_band[bname]
        calib.append({
            "band": bname,
            "count": len(pairs),
            "avg_pred": round(sum(x[0] for x in pairs) / len(pairs), 6),
            "realized_rate": round(sum(x[1] for x in pairs) / len(pairs), 6),
        })
    return calib


def compute_metrics_df(
    df: pd.DataFrame,
    *,
    p_col: str = "p_replay",
    y_col: str = "y_spike",
    source: str = "replay",
) -> dict[str, Any]:
    if df.empty:
        return {"source": source, "n_rows": 0, "status": "empty"}
    work = df.copy()
    work = work[work[p_col].notna() & work[y_col].notna()]
    if work.empty:
        return {"source": source, "n_rows": 0, "status": "no_scored_rows"}
    ps = [float(x) for x in work[p_col]]
    ys = [float(x) for x in work[y_col]]
    n = len(ps)
    pos_rate = sum(ys) / n
    brier = sum((ps[i] - ys[i]) ** 2 for i in range(n)) / n
    base_brier = pos_rate * (1 - pos_rate)
    brier_skill = 1.0 - (brier / base_brier) if base_brier > 1e-12 else None
    log_loss = sum(_sigmoid_log_loss(ps[i], ys[i]) for i in range(n)) / n
    cal_bins = _calibration_bins(ps, ys, CALIBRATION_BINS)
    ece, mce = _ece_mce(cal_bins)
    prec_at_k = {}
    for k in PRECISION_AT_K:
        v = _precision_at_k_by_date(work, p_col, y_col, k)
        if v is not None:
            prec_at_k[f"precision_at_{k}"] = round(v, 6)
    capture = None
    if pos_rate > 0:
        pos_df = work[work[y_col] > 0.5]
        if not pos_df.empty:
            capture = float((pos_df[p_col] >= 0.10).mean())
    metrics: dict[str, Any] = {
        "source": source,
        "status": "ok",
        "n_rows": n,
        "positive_rate": round(pos_rate, 6),
        "positives": int(sum(ys)),
        "brier_score": round(brier, 6),
        "brier_skill_score": round(brier_skill, 6) if brier_skill is not None else None,
        "log_loss": round(log_loss, 6),
        "auroc": round(_auroc(ps, ys), 6) if _auroc(ps, ys) is not None else None,
        "pr_auc": round(_pr_auc(ps, ys), 6) if _pr_auc(ps, ys) is not None else None,
        "lift_top_decile": round(_lift_top_decile(work, p_col, y_col), 6)
        if _lift_top_decile(work, p_col, y_col) is not None
        else None,
        "capture_rate_p_ge_0p10": round(capture, 6) if capture is not None else None,
        "precision_at_k": prec_at_k,
        "calibration_bins": cal_bins,
        "ece": ece,
        "mce": mce,
        "calibration_by_band": _band_calibration(work, p_col, y_col),
    }
    if prec_at_k.get("precision_at_10") is not None and pos_rate > 0:
        metrics["precision_at_10_lift_vs_base"] = round(
            prec_at_k["precision_at_10"] / pos_rate,
            4,
        )
    return metrics


def _load_json_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _top_leadlag_entries(leadlag: dict[str, Any], *, target: str = "y_spike_5", limit: int = 8) -> list[dict]:
    matrix = leadlag.get("matrix") or {}
    scored: list[tuple[str, float]] = []
    for feat, row in matrix.items():
        if not isinstance(row, dict):
            continue
        v = row.get(target)
        if v is None:
            continue
        try:
            fv = float(v)
        except (TypeError, ValueError):
            continue
        if math.isfinite(fv):
            scored.append((str(feat), abs(fv)))
    scored.sort(key=lambda x: x[1], reverse=True)
    out: list[dict] = []
    for feat, score in scored[:limit]:
        raw = (matrix.get(feat) or {}).get(target)
        out.append({
            "feature": feat,
            "target": target,
            "rank_corr": round(float(raw), 6) if raw is not None else None,
            "abs_rank_corr": round(score, 6),
        })
    return out


def _compact_block_ablation(importance: dict[str, Any]) -> dict[str, Any]:
    blocks = importance.get("blocks") or {}
    compact: dict[str, Any] = {}
    for name, info in blocks.items():
        if not isinstance(info, dict):
            continue
        compact[name] = {
            k: info.get(k)
            for k in (
                "status",
                "auroc",
                "precision_at_10",
                "precision_lift_vs_base",
                "auroc_delta_vs_borrow_only",
                "test_positives",
            )
            if k in info
        }
    return {
        "build_time": importance.get("build_time"),
        "target": importance.get("target"),
        "split": importance.get("split"),
        "blocks": compact,
    }


def _load_predictor_study_guidance(data_dir: Path) -> dict[str, Any] | None:
    summary = _load_json_file(data_dir / PREDICTOR_STUDY_SUMMARY_JSON.name)
    importance = _load_json_file(data_dir / PREDICTOR_FEATURE_IMPORTANCE_JSON.name)
    leadlag = _load_json_file(data_dir / PREDICTOR_LEADLAG_JSON.name)
    if not summary and not importance and not leadlag:
        return None
    guidance: dict[str, Any] = {
        "source": "borrow_predictor_study",
        "top_leadlag_y_spike_5": _top_leadlag_entries(leadlag),
    }
    if summary:
        guidance["study_summary"] = {
            "build_time": summary.get("build_time"),
            "panel_rows": summary.get("panel_rows"),
            "panel_symbols": summary.get("panel_symbols"),
            "recommended_v2_features": summary.get("recommended_v2_features") or [],
            "guidance_for_spike_model": summary.get("guidance_for_spike_model"),
            "key_findings": summary.get("key_findings") or [],
            "best_block_ablation": summary.get("best_block_ablation"),
            "block_auroc_lifts": summary.get("block_auroc_lifts") or {},
            "top_predictors_h5": (summary.get("top_predictors_by_horizon") or {}).get("h5") or [],
        }
    if importance:
        guidance["feature_importance"] = _compact_block_ablation(importance)
    return guidance


def _spike_autopsy(replay_l2: pd.DataFrame, *, limit: int = 25) -> list[dict]:
    if replay_l2.empty or "y_spike" not in replay_l2.columns:
        return []
    p_col = "p_replay_calibrated" if "p_replay_calibrated" in replay_l2.columns else "p_replay"
    spikes = replay_l2[replay_l2["y_spike"] > 0.5].copy()
    if spikes.empty:
        return []
    spikes = spikes.sort_values("pred_date", ascending=False).head(limit)
    out = []
    for _, row in spikes.iterrows():
        p = float(row.get(p_col) or row.get("p_replay") or 0)
        out.append({
            "pred_date": str(row.get("pred_date", ""))[:10],
            "symbol": str(row.get("symbol", "")),
            "y_spike": 1,
            "p_calibrated": round(p, 6),
            "alert_tier": str(row.get("alert_tier") or "unknown"),
            "borrow_current": round(float(row.get("borrow_current") or 0), 6),
            "utilization_proxy": round(float(row.get("utilization_proxy") or 0), 4),
            "flagged_before": bool(p >= 0.10),
        })
    return out


def _model_comparison(replay_l0: pd.DataFrame, replay_l2: pd.DataFrame) -> dict[str, Any]:
    m0 = compute_metrics_df(replay_l0, p_col="p_replay", y_col="y_spike", source="v1_l0") if not replay_l0.empty else {}
    p2 = "p_replay_calibrated" if (not replay_l2.empty and "p_replay_calibrated" in replay_l2.columns) else "p_replay"
    m2 = compute_metrics_df(replay_l2, p_col=p2, y_col="y_spike", source="v2_l2") if not replay_l2.empty else {}
    a0 = m0.get("auroc")
    a2 = m2.get("auroc")
    delta = None
    if a0 is not None and a2 is not None:
        delta = round(float(a2) - float(a0), 6)
    return {
        "v1_l0_auroc": a0,
        "v2_l2_auroc": a2,
        "v2_auroc_delta": delta,
        "v1_l0_positives": m0.get("positives"),
        "v2_l2_positives": m2.get("positives"),
        "v2_ece": m2.get("ece"),
        "v2_elevated_realized": next(
            (b.get("realized_rate") for b in (m2.get("calibration_by_band") or []) if b.get("band") == "elevated"),
            None,
        ),
        "recommendation": (
            "Use v2 calibrated L2 for alert tiers; keep v1 L0 p_spike_5d as legacy headline."
            if delta is not None and delta >= 0
            else "Continue monitoring v2; v1 remains primary until v2 AUROC stable."
        ),
    }


def _findings_summary(
    replay_l0: dict,
    replay_l2: dict,
    comparison: dict,
    forecast: dict,
) -> dict[str, Any]:
    return {
        "best_estimate_borrow": (
            "Combine (1) current borrow level, (2) v2 calibrated L2 stress score for spike risk, "
            "and (3) delta_borrow_5d_p50 OLS forecast for directional drift. "
            "Level/momentum features explain ~5% of forward borrow change; spike L0 remains too rare for calibrated probabilities."
        ),
        "spike_headline": (
            f"L0 catastrophic spikes: {replay_l0.get('positives', 0)} in {replay_l0.get('n_rows', 0)} replay rows — "
            "use rank/decile, not raw P≥30%."
        ),
        "stress_headline": (
            f"L2 relative stress: {replay_l2.get('positives', 0)} events — v2+calibration for watch/elevated/high tiers."
        ),
        "forecast_headline": forecast.get("interpretation") or "Forecast lane pending.",
        "model_choice": comparison.get("recommendation"),
    }


def _recent_flagged_outcomes(df: pd.DataFrame, *, p_col: str, limit: int = 30) -> list[dict]:
    if df.empty:
        return []
    work = df.copy()
    date_col = "pred_date" if "pred_date" in work.columns else "date"
    work = work.sort_values([date_col, p_col], ascending=[False, False])
    flagged = work[work[p_col] >= 0.10].head(limit)
    out = []
    for _, row in flagged.iterrows():
        out.append({
            "pred_date": str(row.get(date_col, ""))[:10],
            "symbol": str(row.get("symbol", "")),
            "p": round(float(row[p_col]), 6),
            "y": int(row.get("y_spike", row.get("y", 0))),
            "risk_band": risk_band(float(row[p_col])),
        })
    return out


def build_eval_payload(repo_root: Path) -> dict[str, Any]:
    data_dir = repo_root / "data"
    replay_path = data_dir / "borrow_spike_replay_panel.parquet"
    summary_path = data_dir / "borrow_spike_replay_summary.json"
    realized_path = data_dir / "borrow_spike_realized.jsonl"
    metrics_path = data_dir / "borrow_spike_metrics.json"

    replay_df = pd.DataFrame()
    replay_l2_df = pd.DataFrame()
    if replay_path.exists():
        replay_df = pd.read_parquet(replay_path)
    replay_l2_path = data_dir / "borrow_spike_replay_l2_panel.parquet"
    if replay_l2_path.exists():
        replay_l2_df = pd.read_parquet(replay_l2_path)

    replay_metrics = compute_metrics_df(replay_df, p_col="p_replay", y_col="y_spike", source="replay_l0")
    p_l2_col = "p_replay_calibrated" if (not replay_l2_df.empty and "p_replay_calibrated" in replay_l2_df.columns) else "p_replay"
    replay_l2_metrics = (
        compute_metrics_df(replay_l2_df, p_col=p_l2_col, y_col="y_spike", source="replay_l2")
        if not replay_l2_df.empty
        else {"source": "replay_l2", "status": "empty"}
    )
    model_comparison = _model_comparison(replay_df, replay_l2_df)

    live_df = _load_realized_jsonl(realized_path)
    live_metrics = {"source": "live_predictions", "n_rows": 0, "status": "empty"}
    if not live_df.empty and "p_spike" in live_df.columns and "y_spike" in live_df.columns:
        live_metrics = compute_metrics_df(live_df, p_col="p_spike", y_col="y_spike", source="live_predictions")

    label_grid: dict = {}
    if summary_path.exists():
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            label_grid = summary.get("label_grid") or {}
        except json.JSONDecodeError:
            pass

    replay_summary = {}
    if summary_path.exists():
        try:
            replay_summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass

    l2_replay_window = (replay_summary.get("l2_replay") or {}) if replay_summary else {}

    forecast_payload: dict = {}
    fc_path = data_dir / "borrow_forecast_latest.json"
    if fc_path.exists():
        try:
            forecast_payload = json.loads(fc_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass

    prod_metrics = {}
    if metrics_path.exists():
        try:
            prod_metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass

    payload: dict[str, Any] = {
        "as_of": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "label": LABEL_VARIANTS["L0"],
        "horizon_observations": 5,
        "replay_window": {
            "start": replay_summary.get("replay_date_min"),
            "end": replay_summary.get("replay_date_max"),
            "rows": replay_summary.get("replay_rows"),
            "positives": replay_summary.get("replay_positives"),
        },
        "replay_window_l2": {
            "start": l2_replay_window.get("replay_date_min"),
            "end": l2_replay_window.get("replay_date_max"),
            "rows": l2_replay_window.get("replay_rows"),
            "positives": l2_replay_window.get("replay_positives"),
        },
        "metrics": {
            "replay": replay_metrics,
            "replay_l0": replay_metrics,
            "replay_l2": replay_l2_metrics,
            "live_predictions": live_metrics,
            "production_rollup": prod_metrics,
        },
        "model_comparison": model_comparison,
        "label_grid_summary": label_grid or replay_summary.get("label_grid") or {},
        "recent_high_risk": _recent_flagged_outcomes(replay_df, p_col="p_replay", limit=30),
        "spike_autopsy_l2": _spike_autopsy(replay_l2_df, limit=25),
        "borrow_forecast": {
            "n_symbols": forecast_payload.get("n_symbols"),
            "method": forecast_payload.get("method"),
            "interpretation": forecast_payload.get("interpretation"),
        },
        "gate": {
            "precision_at_10_lift_floor": DEFAULT_PRECISION_LIFT_FLOOR,
            "min_positives_for_gate": DEFAULT_MIN_POSITIVES_FOR_GATE,
            "primary_eval_label": "L2",
            "trader_headline_label": "L0",
        },
    }
    payload["findings_summary"] = _findings_summary(
        replay_metrics,
        replay_l2_metrics if isinstance(replay_l2_metrics, dict) else {},
        model_comparison,
        forecast_payload,
    )
    guidance = _load_predictor_study_guidance(data_dir)
    if guidance is not None:
        payload["predictor_study_guidance"] = guidance
    return payload


def check_gate(payload: dict[str, Any], *, lift_floor: float, min_positives: int) -> tuple[bool, str]:
    replay = (payload.get("metrics") or {}).get("replay_l2") or (payload.get("metrics") or {}).get("replay") or {}
    positives = int(replay.get("positives") or 0)
    if positives < min_positives:
        return True, f"skip gate: only {positives} replay positives (need {min_positives})"
    lift = replay.get("precision_at_10_lift_vs_base")
    if lift is None:
        return True, "skip gate: precision_at_10_lift_vs_base unavailable"
    if float(lift) >= lift_floor:
        return True, f"gate pass: precision@10 lift {lift:.2f}x >= {lift_floor}x"
    return False, f"gate fail: precision@10 lift {lift:.2f}x < {lift_floor}x"


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze borrow spike predictor accuracy.")
    parser.add_argument("--repo-root", type=str, default=str(REPO_ROOT))
    parser.add_argument("--fail-below-floor", action="store_true", help="Exit 1 if precision@10 lift below floor")
    parser.add_argument("--lift-floor", type=float, default=DEFAULT_PRECISION_LIFT_FLOOR)
    parser.add_argument("--min-positives", type=int, default=DEFAULT_MIN_POSITIVES_FOR_GATE)
    args = parser.parse_args()
    repo_root = Path(args.repo_root)
    payload = build_eval_payload(repo_root)
    out_path = repo_root / "data" / "borrow_spike_eval.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    replay_m = payload["metrics"]["replay"]
    print(
        f"[OK] borrow_spike_eval.json: replay_rows={replay_m.get('n_rows')} "
        f"positives={replay_m.get('positives')} "
        f"auroc={replay_m.get('auroc')} "
        f"precision@10={replay_m.get('precision_at_k', {}).get('precision_at_10')}",
    )
    if args.fail_below_floor:
        ok, msg = check_gate(payload, lift_floor=args.lift_floor, min_positives=args.min_positives)
        print(f"[GATE] {msg}")
        if not ok:
            raise SystemExit(1)


if __name__ == "__main__":
    main()

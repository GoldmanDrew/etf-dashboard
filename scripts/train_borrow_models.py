#!/usr/bin/env python3
"""Unified borrow ML training, replay, registry, and production scoring."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from borrow_boosting_model import (  # noqa: E402
    BoostingBundle,
    build_boosting_panel_from_predictor,
    build_drift_forecast_from_panel,
    evaluate_drift_replay,
    fit_boosting_bundle,
    load_bundle,
    save_bundle,
    score_drift,
    score_spike,
    walk_forward_replay_boosting,
)
from borrow_cnn_model import fit_numpy_cnn, load_cnn, save_cnn, score_cnn, walk_forward_replay_cnn  # noqa: E402
from borrow_model_common import default_registry, shrink_delta  # noqa: E402
from borrow_spike_v2 import alert_tier, fit_isotonic_calibrator  # noqa: E402
from build_borrow_predictor_panel import build_borrow_predictor_panel  # noqa: E402
from build_borrow_sequence_panel import build_sequence_panel  # noqa: E402
from forecast_borrow_level import build_forecast_payload  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"


def _load_panel(repo_root: Path) -> pd.DataFrame:
    path = repo_root / "data" / "borrow_predictor_panel.parquet"
    if path.exists():
        df = pd.read_parquet(path)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        return df
    return build_borrow_predictor_panel(repo_root=repo_root)


def _precision_at_k_by_date(df: pd.DataFrame, p_col: str, y_col: str, k: int = 10) -> float | None:
    if df.empty:
        return None
    date_col = "pred_date" if "pred_date" in df.columns else "date"
    hits = total = 0
    for _, grp in df.groupby(date_col):
        g = grp.sort_values(p_col, ascending=False).head(k)
        hits += int(g[y_col].sum())
        total += len(g)
    return float(hits / total) if total else None


def _drift_gate_pass(boosting_drift: dict, ols_r2: float | None) -> bool:
    drift_r2 = boosting_drift.get("r2")
    if drift_r2 is not None and ols_r2 is not None and drift_r2 >= 1.1 * ols_r2:
        return True
    return boosting_drift.get("mae") is not None and boosting_drift.get("status") == "ok"


def build_registry(
    *,
    boosting_spike_metrics: dict,
    boosting_drift_metrics: dict,
    cnn_spike_metrics: dict,
    cnn_drift_metrics: dict,
    ols_forecast: dict,
    drift_gate_pass: bool,
    backend: str,
) -> dict[str, Any]:
    from borrow_model_common import BORROW_OPS_POLICY

    reg = default_registry()
    reg["build_time"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    reg["policy"] = BORROW_OPS_POLICY
    reg["boosting"] = {
        "backend": backend,
        "spike_replay": boosting_spike_metrics,
        "drift_replay": boosting_drift_metrics,
        "drift_gate_pass": drift_gate_pass,
    }
    reg["cnn"] = {
        "spike_replay": cnn_spike_metrics,
        "drift_replay": cnn_drift_metrics,
    }
    reg["spike_l2"]["winner"] = "logistic_v2"
    reg["spike_l2"]["shadow"] = "boosting"
    reg["spike_l2"]["method"] = "logistic_v2_l2_isotonic"
    if drift_gate_pass and backend != "unavailable":
        reg["drift"]["winner"] = "boosting"
        reg["drift"]["method"] = f"boosting_{backend}_huber"
        reg["drift"]["artifact"] = "data/borrow_boosting_bundle.pkl"
    else:
        reg["drift"]["winner"] = "ols"
        reg["drift"]["method"] = ols_forecast.get("method") or "pooled_ols_top_features_shrunk"
    reg["cnn_artifact"] = "data/borrow_cnn_model.pkl"
    return reg


def _build_training_panel(repo_root: Path) -> pd.DataFrame:
    """Predictor features + L2 spike_event labels for boosting replay."""
    from borrow_spike_v2 import build_enriched_panel

    data = repo_root / "data"
    panel = _load_panel(repo_root)
    if panel.empty:
        return panel
    panel = panel.copy()
    panel["date"] = pd.to_datetime(panel["date"], errors="coerce").dt.normalize()

    borrow_path = data / "borrow_history.json"
    if not borrow_path.exists():
        return build_boosting_panel_from_predictor(panel)
    with borrow_path.open("r", encoding="utf-8") as f:
        syms = (json.load(f).get("symbols") or {})
    l2 = build_enriched_panel(syms, label_variant="L2")
    if l2.empty:
        return build_boosting_panel_from_predictor(panel)
    l2 = l2[["date", "symbol", "spike_event"]].copy()
    l2["date"] = pd.to_datetime(l2["date"], errors="coerce").dt.normalize()
    merged = panel.merge(l2, on=["date", "symbol"], how="left", suffixes=("_l0", ""))
    if "spike_event_l0" in merged.columns:
        merged = merged.drop(columns=["spike_event_l0"], errors="ignore")
    merged["spike_event"] = merged["spike_event"].where(
        merged["spike_event"].notna(),
        merged.get("y_spike_5"),
    )
    merged = merged.dropna(subset=["spike_event", "delta_borrow_5"])
    return build_boosting_panel_from_predictor(merged)


def train_and_replay(repo_root: Path, *, skip_cnn: bool = False) -> dict[str, Any]:
    data = repo_root / "data"
    panel = _build_training_panel(repo_root)
    if panel.empty:
        return {"status": "empty_panel"}

    spike_replay, drift_replay = walk_forward_replay_boosting(panel, calibrate=True)
    if not spike_replay.empty:
        spike_replay.to_parquet(data / "borrow_boosting_replay_l2_panel.parquet", index=False)
    if not drift_replay.empty:
        drift_replay.to_parquet(data / "borrow_boosting_replay_drift_panel.parquet", index=False)

    bundle = fit_boosting_bundle(panel)
    backend = bundle.backend if bundle else "unavailable"
    if bundle is not None:
        save_bundle(bundle, data / "borrow_boosting_bundle.pkl")
        X, _ = panel.copy(), None
        if bundle.spike_model is not None:
            p_all = score_spike(bundle, panel)
            y_all = panel["spike_event" if "spike_event" in panel.columns else "y_spike_5"].to_numpy(dtype=float)
            cal = fit_isotonic_calibrator(p_all, y_all)
            bundle = BoostingBundle(
                backend=bundle.backend,
                drift_model=bundle.drift_model,
                spike_model=bundle.spike_model,
                feature_names=bundle.feature_names,
                calibrator=cal,
                train_rows=bundle.train_rows,
                drift_train_rows=bundle.drift_train_rows,
                spike_positives=bundle.spike_positives,
            )
            save_bundle(bundle, data / "borrow_boosting_bundle.pkl")

    cnn_spike_metrics: dict = {"status": "skipped"}
    cnn_drift_metrics: dict = {"status": "skipped"}
    if not skip_cnn:
        seq_panel = build_sequence_panel(repo_root=repo_root, flat_panel=panel)
        if not seq_panel.empty:
            seq_panel.to_parquet(data / "borrow_sequence_panel.parquet", index=False)
            cnn_spike, cnn_drift = walk_forward_replay_cnn(seq_panel, epochs=15)
            if not cnn_spike.empty:
                cnn_spike.to_parquet(data / "borrow_cnn_replay_l2_panel.parquet", index=False)
            if not cnn_drift.empty:
                cnn_drift.to_parquet(data / "borrow_cnn_replay_drift_panel.parquet", index=False)
            cnn_model = fit_numpy_cnn(seq_panel, epochs=20)
            if cnn_model is not None:
                save_cnn(cnn_model, data / "borrow_cnn_model.pkl")
            if not cnn_spike.empty:
                base = float(cnn_spike["y_spike"].mean())
                prec10 = _precision_at_k_by_date(cnn_spike, "p_replay_calibrated", "y_spike", 10)
                cnn_spike_metrics = {
                    "status": "ok",
                    "n_rows": int(len(cnn_spike)),
                    "positives": int(cnn_spike["y_spike"].sum()),
                    "positive_rate": round(base, 6),
                    "precision_at_10": round(prec10, 6) if prec10 is not None else None,
                }
            if not cnn_drift.empty:
                cnn_drift_metrics = evaluate_drift_replay(cnn_drift)

    boosting_spike_metrics: dict = {"status": "empty"}
    if not spike_replay.empty:
        p_col = "p_replay_calibrated" if "p_replay_calibrated" in spike_replay.columns else "p_replay"
        base = float(spike_replay["y_spike"].mean())
        prec10 = _precision_at_k_by_date(spike_replay, p_col, "y_spike", 10)
        boosting_spike_metrics = {
            "status": "ok",
            "n_rows": int(len(spike_replay)),
            "positives": int(spike_replay["y_spike"].sum()),
            "positive_rate": round(base, 6),
            "precision_at_10": round(prec10, 6) if prec10 is not None else None,
            "precision_at_10_lift": round(prec10 / base, 4) if prec10 and base > 0 else None,
        }

    boosting_drift_metrics = evaluate_drift_replay(drift_replay)
    ols_payload = build_forecast_payload(repo_root)
    ols_r2 = None
    if bundle and bundle.drift_model is not None:
        y = panel[panel["delta_borrow_5"].notna()]["delta_borrow_5"].to_numpy(dtype=float)
        if len(y) > 50:
            sub = panel[panel["delta_borrow_5"].notna()]
            pred = score_drift(bundle, sub)
            from borrow_model_common import drift_metrics

            boosting_drift_metrics["full_panel"] = drift_metrics(y, pred)

    v2_path = data / "borrow_spike_replay_l2_panel.parquet"
    v2_prec10 = None
    if v2_path.exists():
        v2 = pd.read_parquet(v2_path)
        pcol = "p_replay_calibrated" if "p_replay_calibrated" in v2.columns else "p_replay"
        v2_prec10 = _precision_at_k_by_date(v2, pcol, "y_spike", 10)

    drift_gate = _drift_gate_pass(boosting_drift_metrics, ols_r2)
    registry = build_registry(
        boosting_spike_metrics=boosting_spike_metrics,
        boosting_drift_metrics=boosting_drift_metrics,
        cnn_spike_metrics=cnn_spike_metrics,
        cnn_drift_metrics=cnn_drift_metrics,
        ols_forecast=ols_payload,
        drift_gate_pass=drift_gate,
        backend=backend,
    )
    with (data / "borrow_model_registry.json").open("w", encoding="utf-8") as f:
        json.dump(registry, f, indent=2)

    return {
        "status": "ok",
        "backend": backend,
        "drift_gate_pass": drift_gate,
        "boosting_spike": boosting_spike_metrics,
        "boosting_drift": boosting_drift_metrics,
        "cnn_spike": cnn_spike_metrics,
        "cnn_drift": cnn_drift_metrics,
    }


def score_production_boosting(repo_root: Path) -> dict[str, Any]:
    """Score latest rows for borrow_spike_risk + forecast artifacts."""
    bundle = load_bundle(repo_root / "data" / "borrow_boosting_bundle.pkl")
    panel = _load_panel(repo_root)
    if bundle is None or panel.empty:
        return {"spike_by_symbol": {}, "forecast_by_symbol": {}}

    panel = panel.copy()
    panel["date"] = pd.to_datetime(panel["date"], errors="coerce")
    latest = panel.sort_values("date").groupby("symbol", as_index=False).tail(1)

    spike_by: dict[str, dict] = {}
    if bundle.spike_model is not None:
        p_raw = score_spike(bundle, latest)
        for i, (_, row) in enumerate(latest.iterrows()):
            sym = str(row["symbol"]).upper()
            p = float(p_raw[i])
            p_cal = bundle.calibrator.transform(p) if bundle.calibrator else p
            spike_by[sym] = {
                "p_spike_5d_l2_boosting": round(p, 6),
                "p_spike_5d_l2_boosting_calibrated": round(p_cal, 6),
                "alert_tier_boosting": alert_tier(p_cal),
            }

    forecast_by = build_drift_forecast_from_panel(panel, bundle)
    return {"spike_by_symbol": spike_by, "forecast_by_symbol": forecast_by, "backend": bundle.backend}


def main() -> None:
    parser = argparse.ArgumentParser(description="Train borrow ML models and update registry.")
    parser.add_argument("--repo-root", type=str, default=str(REPO_ROOT))
    parser.add_argument("--skip-cnn", action="store_true", help="Skip CNN replay (faster)")
    parser.add_argument("--score-only", action="store_true", help="Production score only")
    args = parser.parse_args()
    repo_root = Path(args.repo_root)
    if args.score_only:
        out = score_production_boosting(repo_root)
        out_path = repo_root / "data" / "borrow_ml_scores_latest.json"
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
        print(f"[OK] borrow_ml_scores_latest.json: {len(out.get('spike_by_symbol') or {})} symbols")
        return
    result = train_and_replay(repo_root, skip_cnn=args.skip_cnn)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

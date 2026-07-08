#!/usr/bin/env python3
"""Compare OLS, logistic v1/v2, boosting, and CNN borrow models."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from analyze_borrow_spike_accuracy import compute_metrics_df  # noqa: E402
from borrow_boosting_model import evaluate_drift_replay  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def build_model_eval_payload(repo_root: Path) -> dict[str, Any]:
    data = repo_root / "data"
    models: dict[str, Any] = {}

    replay_l0 = _load_parquet(data / "borrow_spike_replay_panel.parquet")
    replay_l2 = _load_parquet(data / "borrow_spike_replay_l2_panel.parquet")
    boost_l2 = _load_parquet(data / "borrow_boosting_replay_l2_panel.parquet")
    boost_drift = _load_parquet(data / "borrow_boosting_replay_drift_panel.parquet")
    cnn_l2 = _load_parquet(data / "borrow_cnn_replay_l2_panel.parquet")
    cnn_drift = _load_parquet(data / "borrow_cnn_replay_drift_panel.parquet")

    if not replay_l0.empty:
        models["logistic_v1_l0"] = compute_metrics_df(replay_l0, p_col="p_replay", y_col="y_spike")
    if not replay_l2.empty:
        pcol = "p_replay_calibrated" if "p_replay_calibrated" in replay_l2.columns else "p_replay"
        models["logistic_v2_l2"] = compute_metrics_df(replay_l2, p_col=pcol, y_col="y_spike")
    if not boost_l2.empty:
        pcol = "p_replay_calibrated" if "p_replay_calibrated" in boost_l2.columns else "p_replay"
        models["boosting_l2"] = compute_metrics_df(boost_l2, p_col=pcol, y_col="y_spike")
    if not boost_drift.empty:
        models["boosting_drift"] = evaluate_drift_replay(boost_drift)
    if not cnn_l2.empty:
        pcol = "p_replay_calibrated" if "p_replay_calibrated" in cnn_l2.columns else "p_replay"
        models["cnn_l2"] = compute_metrics_df(cnn_l2, p_col=pcol, y_col="y_spike")
    if not cnn_drift.empty:
        models["cnn_drift"] = evaluate_drift_replay(cnn_drift)

    registry: dict = {}
    reg_path = data / "borrow_model_registry.json"
    if reg_path.exists():
        try:
            registry = json.loads(reg_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass

    v2_prec = (models.get("logistic_v2_l2") or {}).get("precision_at_k", {}).get("precision_at_10")
    boost_prec = (models.get("boosting_l2") or {}).get("precision_at_k", {}).get("precision_at_10")
    cnn_prec = (models.get("cnn_l2") or {}).get("precision_at_k", {}).get("precision_at_10")

    winner_spike_production = (registry.get("spike_l2") or {}).get("winner") or "logistic_v2"
    winner_spike_research = "logistic_v2_l2"
    if boost_prec is not None and v2_prec is not None and boost_prec > v2_prec:
        winner_spike_research = "boosting_l2"
    if cnn_prec is not None and boost_prec is not None and cnn_prec > boost_prec:
        winner_spike_research = "cnn_l2"

    winner_drift = registry.get("drift", {}).get("winner") or "ols"
    drift_gate_pass = registry.get("boosting", {}).get("drift_gate_pass")
    if drift_gate_pass is None:
        drift_gate_pass = registry.get("boosting", {}).get("gate_pass")

    return {
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "models": models,
        "winners": {
            "spike_l2_production": winner_spike_production,
            "spike_l2_research_best": winner_spike_research,
            "drift_production": winner_drift,
        },
        "registry": registry,
        "comparison_notes": {
            "primary_spike_metric": "precision_at_10 on L2 walk-forward replay",
            "primary_drift_metric": "MAE / R² on delta_borrow_5",
            "policy": registry.get("policy") or "v2_spike_boosting_drift",
            "drift_gate_pass": drift_gate_pass,
            "v2_precision_at_10": v2_prec,
            "boosting_precision_at_10": boost_prec,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build borrow model comparison eval JSON.")
    parser.add_argument("--repo-root", type=str, default=str(REPO_ROOT))
    args = parser.parse_args()
    repo_root = Path(args.repo_root)
    payload = build_model_eval_payload(repo_root)
    out = repo_root / "data" / "borrow_model_eval_latest.json"
    with out.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"[OK] borrow_model_eval_latest.json: {len(payload.get('models') or {})} models")


if __name__ == "__main__":
    main()

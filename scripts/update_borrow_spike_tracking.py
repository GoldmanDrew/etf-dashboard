#!/usr/bin/env python3
"""Auto-generated milestone tracker for borrow spike / forecast program."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from borrow_live_calibration import build_live_calibration_monitor  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent


def _status(current: float, target: float, *, higher_is_better: bool = True) -> str:
    if higher_is_better:
        if current >= target:
            return "done"
        if current >= target * 0.5:
            return "in_progress"
        return "blocked"
    if current <= target:
        return "done"
    if current <= target * 2:
        return "in_progress"
    return "blocked"


def build_tracking_payload(repo_root: Path) -> dict[str, Any]:
    data = repo_root / "data"
    eval_path = data / "borrow_spike_eval.json"
    eval_payload: dict = {}
    if eval_path.exists():
        try:
            eval_payload = json.loads(eval_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass

    replay_l0 = (eval_payload.get("metrics") or {}).get("replay_l0") or eval_payload.get("metrics", {}).get("replay") or {}
    replay_l2 = (eval_payload.get("metrics") or {}).get("replay_l2") or {}
    live = (eval_payload.get("metrics") or {}).get("live_predictions") or {}
    v2cmp = eval_payload.get("model_comparison") or {}
    forecast = {}
    fc_path = data / "borrow_forecast_latest.json"
    if fc_path.exists():
        try:
            forecast = json.loads(fc_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            pass

    pred_dir = data / "borrow_spike_predictions"
    n_pred_days = len(list(pred_dir.glob("*.json"))) if pred_dir.exists() else 0

    l0_pos = int(replay_l0.get("positives") or 0)
    l2_pos = int(replay_l2.get("positives") or 0)
    util_cov = float((eval_payload.get("predictor_study_guidance") or {}).get("study_summary", {}).get("panel_rows") or 0)
    # approximate from panel meta if present
    meta_path = data / "borrow_predictor_panel_meta.json"
    util_pct = 0.42
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            util_pct = float((meta.get("feature_coverage") or {}).get("utilization_proxy") or 0.42)
        except json.JSONDecodeError:
            pass

    reg_path = data / "borrow_model_registry.json"
    gate_pass = False
    if reg_path.exists():
        try:
            gate_pass = bool(json.loads(reg_path.read_text(encoding="utf-8")).get("boosting", {}).get("gate_pass"))
        except json.JSONDecodeError:
            pass

    live_cal = build_live_calibration_monitor(data / "borrow_spike_realized.jsonl")
    metrics_path = data / "borrow_spike_metrics.json"
    if metrics_path.exists():
        try:
            m_payload = json.loads(metrics_path.read_text(encoding="utf-8"))
            live_cal = m_payload.get("live_calibration_monitor") or live_cal
        except json.JSONDecodeError:
            pass

    milestones = [
        {
            "id": "l0_replay_positives_20",
            "description": "Enough L0 catastrophic spikes for stable eval",
            "current": l0_pos,
            "target": 20,
            "status": _status(l0_pos, 20),
        },
        {
            "id": "l2_replay_positives_100",
            "description": "L2 relative-stress label positives for model dev gates",
            "current": l2_pos,
            "target": 100,
            "status": _status(l2_pos, 100),
        },
        {
            "id": "live_prediction_days_60",
            "description": "Daily prediction archive depth",
            "current": n_pred_days,
            "target": 60,
            "status": _status(n_pred_days, 60),
        },
        {
            "id": "utilization_coverage_0p8",
            "description": "Supply feature coverage for logistic_v2",
            "current": round(util_pct, 4),
            "target": 0.8,
            "status": _status(util_pct, 0.8),
        },
        {
            "id": "l2_elevated_band_hit_rate",
            "description": "Calibrated elevated tier realized rate > 0",
            "current": float(
                next(
                    (b.get("realized_rate") or 0 for b in (replay_l2.get("calibration_by_band") or []) if b.get("band") == "elevated"),
                    0,
                )
            ),
            "target": 0.05,
            "status": _status(
                float(
                    next(
                        (b.get("realized_rate") or 0 for b in (replay_l2.get("calibration_by_band") or []) if b.get("band") == "elevated"),
                        0,
                    )
                ),
                0.05,
            ),
        },
        {
            "id": "v2_beats_v1_auroc",
            "description": "logistic_v2 L2 AUROC >= v1 L0 AUROC on replay",
            "current": 1 if (v2cmp.get("v2_auroc_delta") or 0) >= 0 else 0,
            "target": 1,
            "status": "done" if (v2cmp.get("v2_auroc_delta") or 0) >= 0 else "in_progress",
        },
        {
            "id": "boosting_gate_pass",
            "description": "Boosting L2 passes precision@10 lift gate vs base rate",
            "current": 1 if gate_pass else 0,
            "target": 1,
            "status": "done" if gate_pass else "in_progress",
        },
        {
            "id": "live_elevated_hit_rate_60d",
            "description": "Rolling 60d elevated/high tier L2 realized rate",
            "current": float(live_cal.get("elevated_strict_hit_rate") or 0),
            "target": 0.05,
            "status": _status(float(live_cal.get("elevated_strict_hit_rate") or 0), 0.05)
            if live_cal.get("elevated_strict_hit_rate") is not None
            else "in_progress",
        },
    ]

    next_actions: list[str] = []
    for m in milestones:
        if m["status"] == "blocked":
            next_actions.append(f"Unblock {m['id']}: {m['current']} vs target {m['target']}")
    if live_cal.get("alert"):
        next_actions.insert(0, live_cal["alert"])
    if util_pct < 0.8:
        next_actions.append(f"Raise utilization coverage: {util_pct:.1%} vs target 80%")
    if not next_actions:
        next_actions.append("Maintain nightly pipeline; monitor live vs replay drift weekly.")

    return {
        "as_of": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "program_phase": "v2_shipped_monitoring",
        "milestones": milestones,
        "gates": eval_payload.get("gate") or {},
        "model_comparison": v2cmp,
        "live_track": {
            "n_rows": live.get("n_rows"),
            "positives": live.get("positives"),
            "prediction_snapshot_days": n_pred_days,
        },
        "forecast_lane": {
            "n_symbols": forecast.get("n_symbols"),
            "method": forecast.get("method"),
            "registry_winner": forecast.get("registry_winner"),
        },
        "borrow_ml_registry": gate_pass,
        "live_calibration_monitor": live_cal,
        "next_actions": next_actions[:8],
        "findings_summary": eval_payload.get("findings_summary"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Update borrow spike program tracking JSON.")
    parser.add_argument("--repo-root", type=str, default=str(REPO_ROOT))
    args = parser.parse_args()
    repo_root = Path(args.repo_root)
    payload = build_tracking_payload(repo_root)
    out = repo_root / "data" / "borrow_spike_tracking.json"
    with out.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    blocked = sum(1 for m in payload["milestones"] if m["status"] == "blocked")
    print(f"[OK] borrow_spike_tracking.json: {blocked} blocked milestones")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""End-to-end borrow spike + forecast pipeline (Programs A/B phases 0–5)."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"


def _run(cmd: list[str]) -> None:
    print(f"[RUN] {' '.join(cmd)}")
    subprocess.run(cmd, check=True, cwd=str(REPO_ROOT))


def run_dual_replay(repo_root: Path) -> dict:
    from borrow_spike_model import (
        HORIZON_DAYS_DEFAULT,
        LABEL_VARIANTS,
        build_panel_from_history,
        walk_forward_replay,
    )
    from borrow_spike_v2 import (
        build_enriched_panel,
        fit_logistic_v2,
        score_rows_v2,
        walk_forward_replay_model,
    )

    hist_path = repo_root / "data" / "borrow_history.json"
    with hist_path.open("r", encoding="utf-8") as f:
        borrow_symbols = (json.load(f).get("symbols") or {})

    # L0 + v1
    panel_l0 = build_panel_from_history(borrow_symbols, label_variant="L0")
    replay_l0 = walk_forward_replay(panel_l0, refit_cadence_days=7)
    replay_l0["model"] = "logistic_v1_l0"
    replay_l0["label_variant"] = "L0"

    # L2 + v2 calibrated
    panel_l2 = build_enriched_panel(borrow_symbols, label_variant="L2")
    replay_l2 = walk_forward_replay_model(
        panel_l2,
        fit_fn=fit_logistic_v2,
        score_fn=score_rows_v2,
        model_name="logistic_v2_l2",
        calibrate=True,
        refit_cadence_days=7,
    )
    replay_l2["label_variant"] = "L2"

    data_dir = repo_root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    if not replay_l0.empty:
        replay_l0.to_parquet(data_dir / "borrow_spike_replay_panel.parquet", index=False)
    if not replay_l2.empty:
        replay_l2.to_parquet(data_dir / "borrow_spike_replay_l2_panel.parquet", index=False)

    summary = {
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "label_variant": "L0",
        "label": LABEL_VARIANTS["L0"],
        "horizon_days": HORIZON_DAYS_DEFAULT,
        "refit_cadence_days": 7,
        "panel_rows": int(len(panel_l0)),
        "replay_rows": int(len(replay_l0)),
        "replay_positives": int(replay_l0["y_spike"].sum()) if not replay_l0.empty else 0,
        "replay_date_min": str(replay_l0["pred_date"].min()) if not replay_l0.empty else None,
        "replay_date_max": str(replay_l0["pred_date"].max()) if not replay_l0.empty else None,
        "symbols_scored": int(replay_l0["symbol"].nunique()) if not replay_l0.empty else 0,
        "l2_replay": {
            "panel_rows": int(len(panel_l2)),
            "replay_rows": int(len(replay_l2)),
            "replay_positives": int(replay_l2["y_spike"].sum()) if not replay_l2.empty else 0,
            "replay_date_min": str(replay_l2["pred_date"].min()) if not replay_l2.empty else None,
            "replay_date_max": str(replay_l2["pred_date"].max()) if not replay_l2.empty else None,
        },
    }
    with (data_dir / "borrow_spike_replay_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    # Label grid (quick counts only via subprocess if needed)
    grid = {}
    for variant in ("L0", "L1", "L2", "L3"):
        p = build_panel_from_history(borrow_symbols, label_variant=variant)
        grid[variant] = {
            "panel_rows": int(len(p)),
            "positives": int(p["spike_event"].sum()) if not p.empty and "spike_event" in p.columns else 0,
        }
    with (data_dir / "borrow_spike_replay_summary.json").open("w", encoding="utf-8") as f:
        json.dump({**summary, "label_grid": grid}, f, indent=2)

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Run full borrow spike evaluation pipeline.")
    parser.add_argument("--repo-root", type=str, default=str(REPO_ROOT))
    parser.add_argument("--skip-scorer", action="store_true")
    args = parser.parse_args()
    repo_root = Path(args.repo_root)
    py = sys.executable

    run_dual_replay(repo_root)

    if not args.skip_scorer:
        _run([py, "scripts/score_borrow_spikes.py", "--rescore-all", "--repo-root", str(repo_root)])

    _run([py, "scripts/forecast_borrow_level.py", "--repo-root", str(repo_root)])
    _run([py, "scripts/analyze_borrow_spike_accuracy.py", "--repo-root", str(repo_root)])
    _run([py, "scripts/update_borrow_spike_tracking.py", "--repo-root", str(repo_root)])

    print("[OK] borrow spike pipeline complete")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Walk-forward replay of borrow spike logistic_v1 against borrow history."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from borrow_spike_model import (  # noqa: E402
    HORIZON_DAYS_DEFAULT,
    LABEL_VARIANTS,
    build_panel_from_history,
    walk_forward_replay,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
BORROW_HISTORY_FILE = DATA_DIR / "borrow_history.json"
REPLAY_PARQUET = DATA_DIR / "borrow_spike_replay_panel.parquet"
REPLAY_SUMMARY_JSON = DATA_DIR / "borrow_spike_replay_summary.json"


def _load_history(path: Path) -> dict[str, list[dict]]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    syms = payload.get("symbols") or {}
    return syms if isinstance(syms, dict) else {}


def run_replay(
    *,
    repo_root: Path,
    horizon_days: int = HORIZON_DAYS_DEFAULT,
    label_variant: str = "L0",
    refit_cadence_days: int = 7,
    min_train_rows: int = 200,
    as_of_max: str | None = None,
) -> tuple[pd.DataFrame, dict]:
    hist_path = repo_root / "data" / "borrow_history.json"
    borrow_symbols = _load_history(hist_path)
    panel = build_panel_from_history(
        borrow_symbols,
        horizon_days=horizon_days,
        label_variant=label_variant,
        as_of_max=as_of_max,
    )
    replay = walk_forward_replay(
        panel,
        min_train_rows=min_train_rows,
        refit_cadence_days=refit_cadence_days,
    )
    summary = {
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "label_variant": label_variant,
        "label": LABEL_VARIANTS.get(label_variant, {}),
        "horizon_days": horizon_days,
        "refit_cadence_days": refit_cadence_days,
        "panel_rows": int(len(panel)),
        "replay_rows": int(len(replay)),
        "replay_positives": int(replay["y_spike"].sum()) if not replay.empty and "y_spike" in replay.columns else 0,
        "replay_date_min": str(replay["pred_date"].min()) if not replay.empty else None,
        "replay_date_max": str(replay["pred_date"].max()) if not replay.empty else None,
        "symbols_scored": int(replay["symbol"].nunique()) if not replay.empty else 0,
    }
    return replay, summary


def run_label_grid(
    *,
    repo_root: Path,
    horizon_days: int = HORIZON_DAYS_DEFAULT,
    refit_cadence_days: int = 7,
) -> dict[str, dict]:
    grid: dict[str, dict] = {}
    for variant in ("L0", "L1", "L2", "L3"):
        _, summary = run_replay(
            repo_root=repo_root,
            horizon_days=horizon_days,
            label_variant=variant,
            refit_cadence_days=refit_cadence_days,
        )
        grid[variant] = summary
    return grid


def main() -> None:
    parser = argparse.ArgumentParser(description="Walk-forward replay of borrow spike model.")
    parser.add_argument("--repo-root", type=str, default=str(REPO_ROOT))
    parser.add_argument("--horizon-days", type=int, default=HORIZON_DAYS_DEFAULT)
    parser.add_argument("--label-variant", type=str, default="L0", choices=tuple(LABEL_VARIANTS))
    parser.add_argument("--refit-cadence-days", type=int, default=7)
    parser.add_argument("--min-train-rows", type=int, default=200)
    parser.add_argument("--as-of-max", type=str, default=None)
    parser.add_argument("--label-grid", action="store_true", help="Summarize all label variants (no parquet write)")
    args = parser.parse_args()
    repo_root = Path(args.repo_root)

    if args.label_grid:
        grid = run_label_grid(
            repo_root=repo_root,
            horizon_days=args.horizon_days,
            refit_cadence_days=args.refit_cadence_days,
        )
        out = {
            "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "label_grid": grid,
        }
        REPLAY_SUMMARY_JSON.parent.mkdir(parents=True, exist_ok=True)
        with REPLAY_SUMMARY_JSON.open("w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
        print(f"[OK] label grid -> {REPLAY_SUMMARY_JSON}")
        return

    replay, summary = run_replay(
        repo_root=repo_root,
        horizon_days=args.horizon_days,
        label_variant=args.label_variant,
        refit_cadence_days=args.refit_cadence_days,
        min_train_rows=args.min_train_rows,
        as_of_max=args.as_of_max,
    )
    data_dir = repo_root / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = data_dir / "borrow_spike_replay_panel.parquet"
    if replay.empty:
        print("[WARN] replay produced zero rows")
    else:
        replay.to_parquet(parquet_path, index=False)
        print(f"[OK] replay panel -> {parquet_path} ({len(replay)} rows)")
    summary_path = data_dir / "borrow_spike_replay_summary.json"
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(
        f"[OK] replay summary: rows={summary.get('replay_rows')} "
        f"positives={summary.get('replay_positives')}",
    )


if __name__ == "__main__":
    main()

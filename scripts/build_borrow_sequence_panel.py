#!/usr/bin/env python3
"""Build per-row sequence tensors for borrow CNN / sequence models."""
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

from borrow_model_common import SEQUENCE_CHANNELS, SEQUENCE_WINDOW
from borrow_spike_model import _symbol_history_frame
from build_borrow_predictor_panel import (
    REPO_ROOT,
    build_borrow_predictor_panel,
    _load_borrow_history,
)

SEQUENCE_PARQUET = REPO_ROOT / "data" / "borrow_sequence_panel.parquet"
SEQUENCE_META_JSON = REPO_ROOT / "data" / "borrow_sequence_panel_meta.json"


def _history_sequences(hist: list[dict], window: int = SEQUENCE_WINDOW) -> list[dict]:
    s = _symbol_history_frame(hist)
    if len(s) < window + 5:
        return []
    s = s.sort_values("date").reset_index(drop=True)
    rows: list[dict] = []
    for i in range(window - 1, len(s)):
        win = s.iloc[i - window + 1 : i + 1]
        end = s.iloc[i]
        seq = {}
        for ch in SEQUENCE_CHANNELS:
            if ch in win.columns:
                seq[ch] = [float(x) if pd.notna(x) and np.isfinite(x) else 0.0 for x in win[ch]]
            else:
                seq[ch] = [0.0] * window
        gaps = win["date"].diff().dt.days.dropna()
        obs_delta = float(gaps.median()) if not gaps.empty else 1.0
        rows.append(
            {
                "date": end["date"],
                "symbol": None,
                "obs_idx_end": int(i),
                "obs_delta_median": obs_delta,
                **{f"seq_{ch}": seq[ch] for ch in SEQUENCE_CHANNELS},
            }
        )
    return rows


def build_sequence_panel(
    *,
    repo_root: Path = REPO_ROOT,
    flat_panel: pd.DataFrame | None = None,
    window: int = SEQUENCE_WINDOW,
) -> pd.DataFrame:
    borrow_symbols = _load_borrow_history(repo_root / "data" / "borrow_history.json")
    if flat_panel is None:
        flat_panel = build_borrow_predictor_panel(repo_root=repo_root)
    if flat_panel.empty:
        return pd.DataFrame()

    flat = flat_panel.copy()
    flat["date"] = pd.to_datetime(flat["date"], errors="coerce").dt.normalize()

    seq_frames: list[pd.DataFrame] = []
    for sym, hist in (borrow_symbols or {}).items():
        sym_u = str(sym).upper()
        seq_rows = _history_sequences(hist, window=window)
        if not seq_rows:
            continue
        sdf = pd.DataFrame(seq_rows)
        sdf["symbol"] = sym_u
        sdf["date"] = pd.to_datetime(sdf["date"], errors="coerce").dt.normalize()
        seq_frames.append(sdf)

    if not seq_frames:
        return pd.DataFrame()

    seq_panel = pd.concat(seq_frames, ignore_index=True)
    merge_cols = [c for c in flat.columns if c not in ("date", "symbol")]
    merged = seq_panel.merge(
        flat,
        on=["date", "symbol"],
        how="inner",
        suffixes=("", "_flat"),
    )
    return merged.sort_values(["date", "symbol"]).reset_index(drop=True)


def sequence_meta(panel: pd.DataFrame, window: int = SEQUENCE_WINDOW) -> dict[str, Any]:
    return {
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "grain": ["date", "symbol"],
        "rows": int(len(panel)),
        "symbols": int(panel["symbol"].nunique()) if not panel.empty else 0,
        "window": window,
        "channels": SEQUENCE_CHANNELS,
        "date_min": str(panel["date"].min().date()) if not panel.empty else None,
        "date_max": str(panel["date"].max().date()) if not panel.empty else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build borrow sequence panel for CNN models.")
    parser.add_argument("--repo-root", type=str, default=str(REPO_ROOT))
    args = parser.parse_args()
    repo_root = Path(args.repo_root)
    panel = build_sequence_panel(repo_root=repo_root)
    if panel.empty:
        print("borrow sequence panel is empty")
        return
    out = repo_root / "data" / "borrow_sequence_panel.parquet"
    meta = repo_root / "data" / "borrow_sequence_panel_meta.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(out, index=False)
    meta.write_text(json.dumps(sequence_meta(panel), indent=2), encoding="utf-8")
    print(f"wrote {len(panel)} rows -> {out}")


if __name__ == "__main__":
    main()

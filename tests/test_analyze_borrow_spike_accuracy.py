"""Tests for borrow spike accuracy analysis."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from analyze_borrow_spike_accuracy import (  # noqa: E402
    build_eval_payload,
    check_gate,
    compute_metrics_df,
)


def test_compute_metrics_df_perfect_ranking():
    df = pd.DataFrame({
        "pred_date": ["2026-01-01", "2026-01-01", "2026-01-02", "2026-01-02"],
        "p_replay": [0.95, 0.05, 0.90, 0.10],
        "y_spike": [1, 0, 1, 0],
    })
    m = compute_metrics_df(df, p_col="p_replay", y_col="y_spike")
    assert m["status"] == "ok"
    assert m["n_rows"] == 4
    assert m["auroc"] == pytest.approx(1.0, abs=0.01)


def test_compute_metrics_df_empty():
    m = compute_metrics_df(pd.DataFrame(), p_col="p_replay", y_col="y_spike")
    assert m["status"] == "empty"


def test_check_gate_skips_low_positives():
    payload = {"metrics": {"replay": {"positives": 2, "precision_at_10_lift_vs_base": 0.5}}}
    ok, msg = check_gate(payload, lift_floor=2.0, min_positives=5)
    assert ok is True
    assert "skip gate" in msg


def test_check_gate_fail():
    payload = {"metrics": {"replay": {"positives": 10, "precision_at_10_lift_vs_base": 1.0}}}
    ok, msg = check_gate(payload, lift_floor=2.0, min_positives=5)
    assert ok is False
    assert "fail" in msg


def test_build_eval_payload_smoke(tmp_path: Path):
    data = tmp_path / "data"
    data.mkdir()
    replay = pd.DataFrame({
        "pred_date": ["2026-01-01", "2026-01-01"],
        "symbol": ["A", "B"],
        "p_replay": [0.2, 0.05],
        "y_spike": [0, 0],
    })
    replay.to_parquet(data / "borrow_spike_replay_panel.parquet", index=False)
    payload = build_eval_payload(tmp_path)
    assert "metrics" in payload
    assert "replay" in payload["metrics"]


def test_eval_json_dump_rejects_non_finite(tmp_path: Path):
    from borrow_model_common import sanitize_for_json

    payload = {"metrics": {"cnn_drift": {"rmse": float("inf"), "r2": float("-inf"), "mae": float("nan")}}}
    clean = sanitize_for_json(payload)
    text = json.dumps(clean, allow_nan=False)
    assert "Infinity" not in text
    assert "NaN" not in text
    parsed = json.loads(text)
    assert parsed["metrics"]["cnn_drift"]["rmse"] is None

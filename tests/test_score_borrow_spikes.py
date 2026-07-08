"""Tests for borrow-spike prediction scoring."""
from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import score_borrow_spikes as sbs  # noqa: E402
from build_data import compute_borrow_spike_event_by_date  # noqa: E402


def _aaa_hist() -> list[dict]:
    hist = [
        {"date": f"2025-06-{i+1:02d}", "borrow_current": 0.10, "shares_available": 1e6}
        for i in range(30)
    ]
    hist.extend([
        {"date": f"2026-01-{i+1:02d}", "borrow_current": 0.10, "shares_available": 1e6}
        for i in range(31)
    ])
    hist.extend([
        {"date": d, "borrow_current": 0.1, "shares_available": 1e6}
        for d in (
            "2026-03-14", "2026-03-15", "2026-03-16", "2026-03-17",
            "2026-03-18", "2026-03-19", "2026-03-20",
        )
    ])
    return hist


def test_compute_spike_label_detects_large_forward_jump():
    start = date(2025, 6, 1)
    hist = [
        {
            "date": (start + timedelta(days=i)).isoformat(),
            "borrow_current": 0.10,
            "shares_available": 1_000_000,
        }
        for i in range(200)
    ]
    for j in range(195, 200):
        hist[j]["borrow_current"] = 2.0
    labels = compute_borrow_spike_event_by_date(hist, horizon_days=5)
    assert 1.0 in labels.values()


def test_score_prediction_file_emits_row(tmp_path: Path):
    pred = tmp_path / "2026-03-15.json"
    pred.write_text(
        json.dumps({
            "as_of": "2026-03-15",
            "horizon_days": 5,
            "symbols": {
                "ZZZ": {
                    "p_spike_5d": 0.15,
                    "risk_band": "elevated",
                    "scoring_eligible": True,
                },
            },
        }),
        encoding="utf-8",
    )
    rows = sbs.score_prediction_file(
        pred, {"ZZZ": _aaa_hist()}, horizon_days=5, require_mature=False,
    )
    assert len(rows) == 1
    assert rows[0]["symbol"] == "ZZZ"
    assert rows[0]["y_spike"] in (0, 1)
    assert "borrow_at_pred" in rows[0]


def test_mature_label_gate_skips_recent(tmp_path: Path):
    pred = tmp_path / "2026-06-01.json"
    pred.write_text(
        json.dumps({
            "as_of": "2026-06-01",
            "horizon_days": 5,
            "symbols": {"ZZZ": {"p_spike_5d": 0.2, "scoring_eligible": True}},
        }),
        encoding="utf-8",
    )
    hist = [{"date": "2026-05-01", "borrow_current": 0.1, "shares_available": 1e6}] * 40
    rows = sbs.score_prediction_file(
        pred, {"ZZZ": hist}, horizon_days=5, require_mature=True, today=date(2026, 6, 3),
    )
    assert rows == []


def test_rescore_all_rewrites_jsonl(tmp_path: Path):
    data = tmp_path / "data"
    pred_dir = data / "borrow_spike_predictions"
    pred_dir.mkdir(parents=True)
    (pred_dir / "2026-03-15.json").write_text(
        json.dumps({
            "as_of": "2026-03-15",
            "horizon_days": 5,
            "symbols": {"AAA": {"p_spike_5d": 0.2, "scoring_eligible": True}},
        }),
        encoding="utf-8",
    )
    (data / "borrow_history.json").write_text(
        json.dumps({"symbols": {"AAA": _aaa_hist()}}),
        encoding="utf-8",
    )
    m1 = sbs.score_from_repo(tmp_path, dedupe=True, rescore_all=True, require_mature=False)
    assert m1["n_rows"] == 1


def test_score_from_repo_skips_duplicate_keys(tmp_path: Path):
    data = tmp_path / "data"
    pred_dir = data / "borrow_spike_predictions"
    pred_dir.mkdir(parents=True)
    (pred_dir / "2026-03-15.json").write_text(
        json.dumps({
            "as_of": "2026-03-15",
            "horizon_days": 5,
            "symbols": {"AAA": {"p_spike_5d": 0.2, "scoring_eligible": True}},
        }),
        encoding="utf-8",
    )
    (data / "borrow_history.json").write_text(
        json.dumps({"symbols": {"AAA": _aaa_hist()}}),
        encoding="utf-8",
    )
    m1 = sbs.score_from_repo(tmp_path, dedupe=True, require_mature=False)
    m2 = sbs.score_from_repo(tmp_path, dedupe=True, require_mature=False)
    assert m2["n_rows"] == m1["n_rows"]

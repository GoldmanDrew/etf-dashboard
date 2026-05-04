"""Tests for borrow-spike prediction scoring."""
from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import score_borrow_spikes as sbs  # noqa: E402
from build_data import compute_borrow_spike_event_by_date  # noqa: E402


def test_compute_spike_label_detects_large_forward_jump():
    """Flat borrow then a large jump within the next 5 observations -> spike label 1."""
    start = date(2025, 6, 1)
    hist = [
        {
            "date": (start + timedelta(days=i)).isoformat(),
            "borrow_current": 0.10,
            "shares_available": 1_000_000,
        }
        for i in range(200)
    ]
    # Last 5 observations jump to 2.0 — exceeds max(1, 3*med60, p99) and jump > 0.25
    for j in range(195, 200):
        hist[j]["borrow_current"] = 2.0
    labels = compute_borrow_spike_event_by_date(hist, horizon_days=5)
    assert 1.0 in labels.values()


def test_score_prediction_file_emits_row(tmp_path: Path):
    pred = tmp_path / "2026-03-10.json"
    pred.write_text(
        json.dumps({
            "as_of": "2026-03-10",
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
    zzz_hist = [
        {"date": f"2026-01-{i+1:02d}", "borrow_current": 0.10, "shares_available": 1e6}
        for i in range(31)
    ]
    zzz_hist.extend([
        {"date": "2026-03-09", "borrow_current": 0.11, "shares_available": 1e6},
        {"date": "2026-03-10", "borrow_current": 0.11, "shares_available": 1e6},
        {"date": "2026-03-11", "borrow_current": 0.12, "shares_available": 1e6},
    ])
    borrow = {"ZZZ": zzz_hist}
    rows = sbs.score_prediction_file(pred, borrow, horizon_days=5)
    assert len(rows) == 1
    assert rows[0]["symbol"] == "ZZZ"
    assert rows[0]["pred_date"] == "2026-03-10"
    assert rows[0]["y_spike"] in (0, 1)
    assert rows[0]["p_spike"] == 0.15


def test_score_from_repo_skips_duplicate_keys(tmp_path: Path):
    data = tmp_path / "data"
    pred_dir = data / "borrow_spike_predictions"
    pred_dir.mkdir(parents=True)
    (pred_dir / "2026-03-15.json").write_text(
        json.dumps({
            "as_of": "2026-03-15",
            "horizon_days": 5,
            "symbols": {"AAA": {"p_spike_5d": 0.2, "risk_band": "low", "scoring_eligible": True}},
        }),
        encoding="utf-8",
    )
    aaa_hist = [
        {"date": f"2026-01-{i+1:02d}", "borrow_current": 0.10, "shares_available": 1e6}
        for i in range(31)
    ]
    aaa_hist.extend([
        {"date": "2026-03-14", "borrow_current": 0.1, "shares_available": 1e6},
        {"date": "2026-03-15", "borrow_current": 0.1, "shares_available": 1e6},
        {"date": "2026-03-16", "borrow_current": 0.1, "shares_available": 1e6},
    ])
    (data / "borrow_history.json").write_text(
        json.dumps({"symbols": {"AAA": aaa_hist}}),
        encoding="utf-8",
    )
    m1 = sbs.score_from_repo(tmp_path, dedupe=True)
    m2 = sbs.score_from_repo(tmp_path, dedupe=True)
    assert m2["n_rows"] == m1["n_rows"]

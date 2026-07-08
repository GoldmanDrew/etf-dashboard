"""Tests for borrow sequence panel builder."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from build_borrow_sequence_panel import _history_sequences, SEQUENCE_WINDOW  # noqa: E402


def test_history_sequences_shape():
    hist = []
    borrow = 0.1
    dates = pd.date_range("2026-01-01", periods=40, freq="D")
    for i, d in enumerate(dates):
        hist.append(
            {
                "date": d.strftime("%Y-%m-%d"),
                "borrow_current": borrow + i * 0.001,
                "shares_available": 50000 - i * 100,
            }
        )
    rows = _history_sequences(hist, window=SEQUENCE_WINDOW)
    assert len(rows) > 0
    assert len(rows[0]["seq_borrow_current"]) == SEQUENCE_WINDOW


def test_sequence_panel_no_future_in_window():
    """Last point in window must be the row end date observation."""
    hist = []
    dates = pd.date_range("2026-01-01", periods=35, freq="D")
    for i, d in enumerate(dates):
        hist.append({"date": d.strftime("%Y-%m-%d"), "borrow_current": 0.05 + i * 0.001, "shares_available": 1000})
    rows = _history_sequences(hist, window=8)
    last = rows[-1]
    assert last["obs_idx_end"] == 34
    assert last["seq_borrow_current"][-1] == hist[-1]["borrow_current"]

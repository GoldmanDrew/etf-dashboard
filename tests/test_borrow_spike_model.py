"""Tests for borrow_spike_model core."""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from borrow_spike_model import (  # noqa: E402
    LABEL_VARIANTS,
    apply_spike_labels,
    build_borrow_spike_risk_payload,
    build_panel_from_history,
    compute_borrow_spike_event_by_date,
    walk_forward_replay,
)


def _flat_hist(borrow: float = 0.10, n: int = 200) -> list[dict]:
    start = date(2025, 1, 1)
    return [
        {
            "date": (start + timedelta(days=i)).isoformat(),
            "borrow_current": borrow,
            "shares_available": 1_000_000,
        }
        for i in range(n)
    ]


def test_l0_label_detects_large_forward_jump():
    hist = _flat_hist()
    for j in range(195, 200):
        hist[j]["borrow_current"] = 2.0
    labels = compute_borrow_spike_event_by_date(hist, horizon_days=5, label_variant="L0")
    assert 1.0 in labels.values()


def test_l1_label_more_sensitive_than_l0():
    hist = _flat_hist(borrow=0.10, n=120)
    hist[115]["borrow_current"] = 0.50
    hist[116]["borrow_current"] = 0.55
    hist[117]["borrow_current"] = 0.60
    hist[118]["borrow_current"] = 0.65
    hist[119]["borrow_current"] = 0.70
    l0 = compute_borrow_spike_event_by_date(hist, horizon_days=5, label_variant="L0")
    l1 = compute_borrow_spike_event_by_date(hist, horizon_days=5, label_variant="L1")
    assert sum(v == 1.0 for v in l0.values() if v is not None) <= sum(
        v == 1.0 for v in l1.values() if v is not None
    )


def test_label_variants_defined():
    assert set(LABEL_VARIANTS) >= {"L0", "L1", "L2", "L3"}


def test_build_panel_from_history():
    syms = {"AAA": _flat_hist(n=80)}
    panel = build_panel_from_history(syms, horizon_days=5, label_variant="L0")
    assert not panel.empty
    assert "spike_event" in panel.columns
    assert panel["symbol"].iloc[0] == "AAA"


def test_walk_forward_replay_produces_scores():
    syms = {f"S{i}": _flat_hist(n=100) for i in range(5)}
    panel = build_panel_from_history(syms, horizon_days=5)
    replay = walk_forward_replay(panel, min_train_rows=50, refit_cadence_days=14)
    assert not replay.empty
    assert "p_replay" in replay.columns
    assert replay["p_replay"].between(0, 1).all()


def test_build_borrow_spike_risk_payload_smoke():
    syms = {f"X{i}": _flat_hist(n=70) for i in range(3)}
    payload = build_borrow_spike_risk_payload(syms, "2026-07-04")
    assert payload["as_of"] == "2026-07-04"
    assert "symbols" in payload
    assert payload["model"]["name"] == "logistic_v1"


def test_apply_spike_labels_preserves_dates():
    s = pd.DataFrame(_flat_hist(n=30))
    s["date"] = pd.to_datetime(s["date"])
    out = apply_spike_labels(s, horizon_days=5, label_variant="L0")
    assert len(out) == len(s)

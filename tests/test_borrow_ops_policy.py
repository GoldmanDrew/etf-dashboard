"""Tests for scripts/borrow_ops_policy.py."""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))

from borrow_ops_policy import (  # noqa: E402
    BORROW_OPS_POLICY,
    compute_borrow_ops_fields,
    effective_net_edge_for_sizing,
    merge_borrow_ops_into_short_edge_map,
)


def test_effective_net_edge_uses_stress_when_elevated():
    rec = {
        "net_edge_p50_annual": 0.12,
        "net_edge_stress_p50_annual": 0.05,
        "borrow_spike_alert_tier": "elevated",
    }
    assert effective_net_edge_for_sizing(rec) == 0.05


def test_effective_net_edge_headline_when_low_tier():
    rec = {
        "net_edge_p50_annual": 0.12,
        "net_edge_stress_p50_annual": 0.05,
        "borrow_spike_alert_tier": "low",
    }
    assert effective_net_edge_for_sizing(rec) == 0.12


def test_compute_borrow_ops_spike_block_and_disagree():
    rec = {
        "borrow_spike_alert_tier": "watch",
        "borrow_spike_alert_tier_boosting": "elevated",
        "borrow_forecast_delta_5d_p50": 0.03,
        "borrow_spike_scoring_eligible": True,
        "net_edge_p50_annual": 0.10,
    }
    ops = compute_borrow_ops_fields(rec)
    assert ops["borrow_ops_policy"] == BORROW_OPS_POLICY
    assert ops["borrow_ops_spike_block"] is False
    assert ops["borrow_ops_spike_watch"] is True
    assert ops["borrow_ops_model_disagree"] is True
    assert ops["borrow_ops_drift_tightening"] is True


def test_compute_borrow_ops_elevated_blocks():
    rec = {
        "borrow_spike_alert_tier": "high",
        "borrow_spike_scoring_eligible": True,
        "net_edge_p50_annual": 0.08,
        "net_edge_stress_p50_annual": 0.02,
    }
    ops = compute_borrow_ops_fields(rec)
    assert ops["borrow_ops_spike_block"] is True
    assert ops["borrow_ops_effective_net_edge_p50"] == 0.02


def test_merge_borrow_ops_into_short_edge_map():
    short_edge = {
        "__asof__": "2026-07-08",
        "TQQY": {"net_edge_p50_annual": 0.15},
    }
    dash = [{
        "symbol": "TQQY",
        "borrow_spike_alert_tier": "elevated",
        "borrow_spike_alert_tier_boosting": "high",
        "borrow_spike_scoring_eligible": True,
        "borrow_forecast_delta_5d_p50": 0.04,
        "net_edge_p50_annual": 0.15,
        "net_edge_stress_p50_annual": 0.09,
    }]
    merged = merge_borrow_ops_into_short_edge_map(short_edge, dash)
    assert merged["TQQY"]["borrow_ops_spike_block"] is True
    assert merged["TQQY"]["borrow_ops_model_disagree"] is True
    assert merged["__asof__"] == "2026-07-08"

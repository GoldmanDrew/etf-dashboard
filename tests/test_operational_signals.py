"""Tests for scripts/operational_signals.py."""
from __future__ import annotations

import json
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))

from operational_signals import enrich_records_with_operational_signals  # noqa: E402


def test_enrich_borrow_spike_forecast_and_moc_flow(tmp_path):
    records = [{"symbol": "TQQQ", "bucket": "bucket_1_high_beta"}]
    borrow = {
        "symbols": {
            "TQQQ": {
                "p_spike_5d": 0.18,
                "p_spike_5d_l2_calibrated": 0.14,
                "alert_tier": "elevated",
                "risk_band": "elevated",
                "quality_band": "strong",
                "scoring_eligible": True,
            }
        }
    }
    forecast_path = tmp_path / "borrow_forecast_latest.json"
    forecast_path.write_text(
        json.dumps(
            {
                "by_symbol": {
                    "TQQQ": {
                        "delta_borrow_5d_p50": 0.05,
                        "borrow_forecast_5d_p50": 0.23,
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    enrich_records_with_operational_signals(
        records, borrow_spike_risk=borrow, data_dir=tmp_path
    )
    rec = records[0]
    assert rec["borrow_spike_p_5d"] == 0.18
    assert rec["borrow_spike_p_5d_l2_calibrated"] == 0.14
    assert rec["borrow_spike_alert_tier"] == "elevated"
    assert rec["borrow_forecast_delta_5d_p50"] == 0.05
    assert rec["borrow_forecast_5d_p50"] == 0.23

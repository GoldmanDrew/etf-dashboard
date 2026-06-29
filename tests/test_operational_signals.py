"""Tests for scripts/operational_signals.py."""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))

from operational_signals import enrich_records_with_operational_signals  # noqa: E402


def test_enrich_borrow_spike_and_moc_flow():
    records = [{"symbol": "TQQQ", "bucket": "bucket_1_high_beta"}]
    borrow = {
        "symbols": {
            "TQQQ": {
                "p_spike_5d": 0.18,
                "risk_band": "medium",
                "quality_band": "strong",
                "scoring_eligible": True,
            }
        }
    }
    enrich_records_with_operational_signals(records, borrow_spike_risk=borrow, data_dir=Path("/nonexistent"))
    assert records[0]["borrow_spike_p_5d"] == 0.18
    assert records[0]["borrow_spike_risk_band"] == "medium"

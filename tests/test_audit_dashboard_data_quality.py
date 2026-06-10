"""Tests for scripts/audit_dashboard_data_quality.py."""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))

from audit_dashboard_data_quality import audit_dashboard  # noqa: E402


def test_audit_rejects_full_60d_with_insufficient_obs():
    errors, warnings = audit_dashboard(
        {
            "records": [
                {
                    "symbol": "CBRG",
                    "realized_pair_gross_60d": -0.2,
                    "realized_pair_gross_60d_obs": 13,
                    "realized_pair_gross_60d_sufficient": False,
                }
            ]
        }
    )
    assert any("full realized_pair_gross_60d" in msg for msg in errors)
    assert warnings == []


def test_audit_accepts_partial_60d_and_partial_vol_label():
    errors, _warnings = audit_dashboard(
        {
            "records": [
                {
                    "symbol": "CBRG",
                    "realized_pair_gross_partial": -0.2,
                    "realized_pair_gross_60d_obs": 13,
                    "realized_pair_gross_60d_sufficient": False,
                    "expected_decay_available": False,
                    "vol_etf_annual_obs": 17,
                    "vol_etf_annual_effective_label": "partial 17 obs",
                    "vol_etf_annual_window": "12M",
                }
            ]
        }
    )
    assert errors == []


def test_audit_rejects_missing_expected_value_when_available():
    errors, _warnings = audit_dashboard(
        {
            "records": [
                {
                    "symbol": "CBRX",
                    "expected_decay_available": True,
                    "expected_gross_decay_annual": None,
                    "expected_gross_decay_p50_annual": None,
                    "expected_pair_pnl_p50_annual": None,
                }
            ]
        }
    )
    assert any("expected_decay_available=true" in msg for msg in errors)

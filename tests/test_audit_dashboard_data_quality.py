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


def test_audit_rejects_full_60d_crossing_lifecycle_gap():
    old_rows = [
        {
            "date": f"2023-04-{day:02d}",
            "close_price": 30.0,
            "underlying_adj_close": 75.0,
            "source_provider": "yahoo_bootstrap",
        }
        for day in range(1, 29)
    ]
    new_rows = [
        {
            "date": f"2026-06-{day:02d}",
            "close_price": 15.0,
            "underlying_adj_close": 120.0,
            "source_provider": "merged",
        }
        for day in range(2, 9)
    ]
    errors, _warnings = audit_dashboard(
        {
            "records": [
                {
                    "symbol": "ONG",
                    "realized_pair_gross_60d": 5.0,
                    "realized_pair_gross_60d_obs": 30,
                    "realized_pair_gross_60d_sufficient": True,
                }
            ]
        },
        metrics_by_symbol={"ONG": old_rows + new_rows},
    )
    assert any("crosses" in msg and "metrics gap" in msg for msg in errors)

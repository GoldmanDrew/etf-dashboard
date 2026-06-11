"""Tests for scripts/audit_dashboard_data_quality.py."""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))

from audit_dashboard_data_quality import (  # noqa: E402
    audit_dashboard,
    audit_fabricated_adj_basis,
    audit_stale_price_feeds,
)


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


_QBTZ_CORP = {
    "events": [
        {
            "type": "reverse_split",
            "ticker": "QBTZ",
            "execution_date": "2026-03-23",
            "ratio_from": 3.0,
            "ratio_to": 1.0,
        }
    ]
}


def test_audit_flags_fabricated_adj_cliff():
    rows = [
        {"date": "2026-03-19", "close_price": 43.98, "etf_adj_close": 131.94},
        {"date": "2026-03-20", "close_price": 45.63, "etf_adj_close": 136.89},
        {"date": "2026-03-23", "close_price": 41.73, "etf_adj_close": 13.91},
        {"date": "2026-03-24", "close_price": 44.30, "etf_adj_close": 14.77},
    ]
    errors = audit_fabricated_adj_basis({"QBTZ": rows}, _QBTZ_CORP)
    assert any("fabricated etf_adj_close cliff" in msg for msg in errors)


def test_audit_accepts_clean_adj_basis():
    rows = [
        {"date": "2026-03-20", "close_price": 45.63, "etf_adj_close": 45.63},
        {"date": "2026-03-23", "close_price": 41.73, "etf_adj_close": 41.73},
    ]
    assert audit_fabricated_adj_basis({"QBTZ": rows}, _QBTZ_CORP) == []


def test_audit_stale_feed_warns_on_carry_forward_tail():
    real = [
        {"date": f"2026-06-{d:02d}", "source_provider": "polygon"}
        for d in range(1, 4)
    ]
    cf = [
        {"date": f"2026-06-{d:02d}", "source_provider": "carry_forward"}
        for d in range(4, 9)
    ]
    fresh = [
        {"date": f"2026-06-{d:02d}", "source_provider": "polygon"}
        for d in range(1, 9)
    ]
    errors, warnings = audit_stale_price_feeds({"QBTZ": real + cf, "SOXL": fresh})
    assert any("carry_forward" in msg and "QBTZ" in msg for msg in warnings)
    assert errors == []


def test_audit_stale_feed_fails_on_systemic_stall():
    stalled = [{"date": "2026-05-20", "source_provider": "polygon"}]
    fresh = [{"date": "2026-06-08", "source_provider": "polygon"}]
    errors, _warnings = audit_stale_price_feeds(
        {"AAA": stalled, "BBB": stalled, "CCC": stalled, "DDD": fresh}
    )
    assert any("systemic ingest stall" in msg for msg in errors)

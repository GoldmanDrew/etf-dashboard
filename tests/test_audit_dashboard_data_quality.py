"""Tests for scripts/audit_dashboard_data_quality.py."""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))

from audit_dashboard_data_quality import (  # noqa: E402
    audit_dashboard,
    audit_fabricated_adj_basis,
    audit_metrics_calendar,
    audit_metrics_asof_alignment,
    audit_stale_price_feeds,
    audit_underlying_adj_cliffs,
    audit_vrp_publication,
)


def test_audit_rejects_full_60d_with_insufficient_obs():
    errors, warnings = audit_dashboard(
        {
            "records": [
                {
                    "symbol": "CBRG",
                    "realized_pair_gross_20d": -0.2,
                    "realized_pair_gross_20d_obs": 13,
                    "realized_pair_gross_20d_sufficient": False,
                }
            ]
        }
    )
    assert any("full realized_pair_gross_20d" in msg for msg in errors)
    assert warnings == []


def test_audit_accepts_partial_60d_and_partial_vol_label():
    errors, _warnings = audit_dashboard(
        {
            "records": [
                {
                    "symbol": "CBRG",
                    "realized_pair_gross_partial": -0.2,
                    "realized_pair_gross_20d_obs": 13,
                    "realized_pair_gross_20d_sufficient": False,
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
                    "realized_pair_gross_20d": 5.0,
                    "realized_pair_gross_20d_obs": 30,
                    "realized_pair_gross_20d_sufficient": True,
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


def test_audit_stale_feed_errors_on_cf_tail_with_market():
    real = [{"date": "2026-06-01", "source_provider": "polygon", "close_price": 10.0, "underlying_adj_close": 20.0}]
    cf = [
        {
            "date": f"2026-06-{d:02d}",
            "source_provider": "carry_forward",
            "stale_kind": "carry_forward",
            "close_price": 11.0,
            "underlying_adj_close": 21.0,
        }
        for d in range(2, 6)
    ]
    errors, _warnings = audit_stale_price_feeds({"RGTZ": real + cf})
    assert any("market_backed" in msg and "RGTZ" in msg for msg in errors)


def test_audit_stale_feed_fails_on_systemic_stall():
    stalled = [{"date": "2026-05-20", "source_provider": "polygon"}]
    fresh = [{"date": "2026-06-08", "source_provider": "polygon"}]
    errors, _warnings = audit_stale_price_feeds(
        {"AAA": stalled, "BBB": stalled, "CCC": stalled, "DDD": fresh}
    )
    assert any("systemic ingest stall" in msg for msg in errors)


def test_audit_metrics_calendar_rejects_juneteenth_rows():
    rows = {
        "AAPU": [
            {"date": "2026-06-18", "source_provider": "direxion"},
            {"date": "2026-06-19", "source_provider": "carry_forward"},
        ]
    }
    errors = audit_metrics_calendar(rows)
    assert any("2026-06-19" in msg and "non-NYSE session" in msg for msg in errors)


def test_asof_audit_rejects_eligible_carry_forward_premium_discount():
    errors, _warnings = audit_metrics_asof_alignment({
        "TEST": [{
            "date": "2026-07-10",
            "nav": 10.0,
            "close_price": 9.0,
            "source_provider": "carry_forward",
            "source_url": "carry_forward://TEST?from=2026-07-09",
            "stale_kind": "carry_forward",
            "issuer_asof_date": "2026-07-09",
            "market_asof_date": "2026-07-10",
            "premium_discount_eligible": True,
        }]
    })
    assert any("premium_discount_eligible=true" in msg for msg in errors)


def test_vrp_audit_blocks_expired_grade_d_rows_without_failing():
    errors, warnings = audit_vrp_publication({
        "rows": [{
            "yb_etf": "TEST",
            "expiry": "2020-01-01",
            "data_grade": "D",
            "actionable": False,
            "publication_status": "expired_holdings",
            "quote_sync": {"sync_ok": False},
        }]
    })
    assert errors == []
    assert any("TEST" in msg and "blocked" in msg for msg in warnings)


def test_audit_schema_v4_requires_fof_provenance_and_stats_status():
    bare = {
        "schema_v": 4,
        "records": [
            {
                "symbol": "YBTY",
                "product_class": "income_yieldboost_fof",
                "gross_decay_annual": 0.12,
                "expected_decay_available": True,
                "expected_pair_pnl_p50_annual": 0.20,
            }
        ],
    }
    errors, _ = audit_dashboard(bare)
    assert any("gross_decay_annual has no provenance source" in msg for msg in errors)
    assert any("missing stats_status" in msg for msg in errors)

    stamped = {
        "schema_v": 4,
        "records": [
            {
                "symbol": "YBTY",
                "product_class": "income_yieldboost_fof",
                "gross_decay_annual": 0.12,
                "gross_decay_annual_source": "fof_realized_pair",
                "expected_decay_available": True,
                "expected_pair_pnl_p50_annual": 0.20,
                "stats_status": {
                    "borrow_current": "provider_missing",
                    "gross_decay_annual": "valid",
                    "realized_pair_gross_20d": "insufficient_history",
                    "expected_pair_pnl_p50_annual": "valid",
                    "net_edge_p50_annual": "provider_missing",
                    "forecast_vol_underlying_annual": "provider_missing",
                },
            }
        ],
    }
    errors2, _ = audit_dashboard(stamped)
    assert not any("YBTY" in msg and ("provenance" in msg or "stats_status" in msg) for msg in errors2)


def test_audit_underlying_cliff_message_mentions_declared_coverage_gap():
    rows = {
        "LCDL": [
            {"date": "2025-05-13", "underlying_adj_close": 26.5},
            {"date": "2025-05-14", "underlying_adj_close": 276.0},
            {"date": "2025-05-22", "underlying_adj_close": 26.6},
        ]
    }
    corp = {
        "events": [
            {
                "type": "reverse_split",
                "ticker": "LCID",
                "execution_date": "2025-09-02",
                "ratio_from": 10.0,
                "ratio_to": 1.0,
            }
        ]
    }
    errors = audit_underlying_adj_cliffs(rows, corp, etf_to_underlying={"LCDL": "LCID"})
    assert errors
    assert any("do not cover this date" in msg for msg in errors)
    assert any("Yahoo und island" in msg for msg in errors)

"""Tests for freshness helpers and YB underlying refresh selection."""
from __future__ import annotations

import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from build_data import (  # noqa: E402
    _order_yieldboost_refresh_symbols,
    _pick_yieldboost_underlyings_to_refresh,
    _yieldboost_targeted_refresh_symbols,
    load_monitored_options_symbols,
    prune_unmonitored_options_cache,
)
from freshness_diagnostics import _check_options  # noqa: E402
from ingest_etf_metrics import (  # noqa: E402
    extend_metrics_session_coverage,
    prune_expired_carry_forward_rows,
    repair_nav_only_partial_aum,
)


def test_yieldboost_targeted_refresh_symbols_keeps_underlyings():
    targets = {"SOXL": [47.89], "NUGT": [35.07]}
    held = {"SOXL": {"2026-05-27"}}
    sleeves, underlyings = _yieldboost_targeted_refresh_symbols(
        ["SOXL", "NUGT", "SOXX", "GDX"],
        target_strikes_by_sleeve=targets,
        held_expiries_by_sleeve=held,
    )
    assert sleeves == ["NUGT", "SOXL"]
    assert underlyings == ["GDX", "SOXX"]


def test_pick_underlyings_all_mode():
    prior = {
        "AMD": {"updated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z")},
    }
    picked, skipped = _pick_yieldboost_underlyings_to_refresh(
        ["AMD", "SOXX"],
        prior,
        refresh_mode="all",
    )
    assert picked == ["AMD", "SOXX"]
    assert skipped == []


def test_pick_underlyings_stale_mode_prioritizes_old():
    old = (datetime.now(UTC) - timedelta(hours=10)).isoformat().replace("+00:00", "Z")
    fresh = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    prior = {
        "AMD": {"updated_at": old},
        "SOXX": {"updated_at": fresh},
        "NVDA": {"updated_at": old},
    }
    picked, skipped = _pick_yieldboost_underlyings_to_refresh(
        ["AMD", "SOXX", "NVDA"],
        prior,
        refresh_mode="stale",
        stale_hours=4,
        cap=2,
    )
    assert len(picked) == 2
    assert "SOXX" not in picked
    assert "SOXX" in skipped


def test_order_yieldboost_refresh_puts_stale_underlyings_before_sleeves():
    old = (datetime.now(UTC) - timedelta(days=5)).isoformat().replace("+00:00", "Z")
    fresh = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    prior = {
        "AMD": {"updated_at": old},
        "SOXX": {"updated_at": old},
        "SOXL": {"updated_at": fresh},
        "AMDL": {"updated_at": fresh},
    }
    order = _order_yieldboost_refresh_symbols(
        ["AMD", "SOXX"],
        ["SOXL", "AMDL"],
        prior,
    )
    assert order.index("AMD") < order.index("SOXL")
    assert order.index("SOXX") < order.index("AMDL")
    assert set(order[:2]) == {"AMD", "SOXX"}


def test_load_monitored_options_symbols_from_records():
    records = [
        {"symbol": "SOXL", "underlying": "SOXX", "bucket": "bucket_3_inverse"},
        {"symbol": "SPY", "underlying": "SPY", "bucket": "bucket_1"},
    ]
    monitored = load_monitored_options_symbols(records)
    assert "SOXL" in monitored
    assert "SOXX" in monitored


def test_prune_unmonitored_options_cache():
    cache = {
        "symbols": {
            "SOXL": {"updated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z")},
            "AAL": {"updated_at": "2026-01-01T00:00:00Z"},
        }
    }
    n = prune_unmonitored_options_cache(cache, monitored={"SOXL", "SOXX"})
    assert n == 1
    assert "AAL" not in cache["symbols"]
    assert cache["symbols_count"] == 1


def test_check_options_ignores_orphan_stale_symbols(monkeypatch):
    old_ts = (datetime.now(UTC) - timedelta(days=60)).isoformat().replace("+00:00", "Z")
    fresh_ts = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    cache = {
        "symbols": {
            "AAL": {"updated_at": old_ts},
            "SOXL": {"updated_at": fresh_ts},
        },
        "yieldboost_underlyings_refreshed": ["SOXX"],
    }
    monkeypatch.setattr(
        "freshness_diagnostics._freshness_enforced_options_symbols",
        lambda cache: {"SOXL", "SOXX"},
    )
    block, violations = _check_options(cache, max_underlying_hours=48.0)
    assert not any("AAL" in v for v in violations)
    assert block["oldest_enforced_symbol"] == "SOXL"


def test_extend_metrics_session_coverage_adds_issuer_session_extend():
    df = pd.DataFrame([
        {
            "date": pd.Timestamp("2026-05-28"),
            "ticker": "NOW",
            "nav": 10.0,
            "aum": 100.0,
            "shares_outstanding": 10.0,
            "status": "ok",
            "stale": False,
            "stale_age_bdays": None,
            "stale_kind": None,
            "source_provider": "granite_shares",
            "source_url": "x",
            "ingested_at_utc": pd.Timestamp("2026-05-28T12:00:00Z"),
        },
    ])
    out = extend_metrics_session_coverage(
        df,
        session_date=date(2026, 5, 29),
        tickers=["NOW"],
        max_lag_bdays=2,
    )
    session_rows = out[out["date"] == pd.Timestamp("2026-05-29")]
    assert len(session_rows) == 1
    assert session_rows.iloc[0]["stale_kind"] == "issuer_session_extend"


def test_prune_expired_carry_forward_rows():
    df = pd.DataFrame([
        {
            "date": pd.Timestamp("2026-05-29"),
            "ticker": "XYZ",
            "nav": 10.0,
            "aum": 100.0,
            "shares_outstanding": 10.0,
            "status": "ok",
            "stale": True,
            "stale_age_bdays": 5,
            "stale_kind": "carry_forward",
            "source_provider": "carry_forward",
            "source_url": "carry_forward://XYZ",
            "ingested_at_utc": pd.Timestamp("2026-05-29T12:00:00Z"),
        },
    ])
    out, n = prune_expired_carry_forward_rows(df, max_stale_bdays=3)
    assert n == 1
    assert out.iloc[0]["status"] == "missing"
    assert pd.isna(out.iloc[0]["nav"])


def test_repair_nav_only_partial_aum(monkeypatch):
    df = pd.DataFrame([
        {
            "date": pd.Timestamp("2026-05-29"),
            "ticker": "BTCU",
            "nav": 25.0,
            "aum": None,
            "shares_outstanding": None,
            "status": "partial",
            "stale": False,
            "stale_age_bdays": None,
            "stale_kind": None,
            "source_provider": "merged",
            "source_url": "x",
            "ingested_at_utc": pd.Timestamp("2026-05-29T12:00:00Z"),
        },
    ])
    mock_yf = MagicMock()
    mock_yf._enabled = True
    mock_res = MagicMock(aum=500_000_000.0, shares_outstanding=20_000_000.0)
    mock_yf.fetch_for_date.return_value = mock_res
    monkeypatch.setattr("etf_providers.YFinanceProvider", lambda: mock_yf)
    out, n = repair_nav_only_partial_aum(df)
    assert n == 1
    assert float(out.iloc[0]["aum"]) == 500_000_000.0
    assert out.iloc[0]["status"] == "ok"

"""Tests for freshness helpers and YB underlying refresh selection."""
from __future__ import annotations

import json
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
from freshness_diagnostics import _check_options, _market_age_minutes  # noqa: E402
from ingest_etf_metrics import (  # noqa: E402
    STALE_KIND_MARKET_BACKED,
    extend_metrics_session_coverage,
    overlay_row_session_market_fields,
    promote_carry_forward_rows_with_market,
    prune_expired_carry_forward_rows,
    repair_nav_only_partial_aum,
    stamp_metric_asof_metadata,
)
from etf_providers import STALE_KIND_ISSUER_LAG  # noqa: E402


def test_carry_forward_market_overlay_uses_row_session_and_blocks_premium_discount():
    rows = pd.DataFrame([{
        "date": date(2026, 7, 10),
        "ticker": "TEST",
        "nav": 10.0,
        "close_price": 8.0,
        "shares_traded": None,
        "etf_adj_close": None,
        "underlying_adj_close": None,
        "source_provider": "carry_forward",
        "source_url": "carry_forward://TEST?from=2026-07-09",
        "stale_kind": "carry_forward",
    }])
    close = pd.DataFrame([{
        "date": date(2026, 7, 10), "ticker": "TEST",
        "close_price": 9.5, "shares_traded": 1234,
    }])
    adj = pd.DataFrame([{
        "date": date(2026, 7, 10), "ticker": "TEST", "etf_adj_close": 9.5,
    }])
    und = pd.DataFrame([{
        "date": date(2026, 7, 10), "ticker": "UND", "underlying_adj_close": 101.0,
    }])
    out = overlay_row_session_market_fields(
        rows,
        close_df=close,
        etf_adj_df=adj,
        underlying_df=und,
        etf_to_underlying={"TEST": "UND"},
    )
    out = stamp_metric_asof_metadata(out)
    row = out.iloc[0]
    assert row["close_price"] == 9.5
    assert row["shares_traded"] == 1234
    assert row["etf_adj_close"] == 9.5
    assert row["underlying_adj_close"] == 101.0
    assert row["issuer_asof_date"] == "2026-07-09"
    assert row["market_asof_date"] == "2026-07-10"
    assert bool(row["premium_discount_eligible"]) is False

    promoted, n = promote_carry_forward_rows_with_market(out)
    assert n == 1
    promoted = stamp_metric_asof_metadata(promoted)
    prow = promoted.iloc[0]
    assert prow["stale_kind"] == STALE_KIND_MARKET_BACKED
    assert prow["source_provider"] == "market_backed"
    assert bool(prow["premium_discount_eligible"]) is False
    assert prow["premium_discount_status"] == "issuer_stale"


def test_stamp_blocks_issuer_lag_and_implausible_prem_disc():
    """Frozen issuer NAV vs live close must not look like a tradeable premium."""
    rows = pd.DataFrame([
        {
            "date": date(2026, 5, 19),
            "ticker": "APHU",
            "nav": 29.65,
            "close_price": 14.84,
            "source_provider": "rex_shares",
            "source_url": "https://www.rexshares.com/APHU/#as_of=2026-05-19",
            "stale_kind": STALE_KIND_ISSUER_LAG,
            "issuer_asof_date": "2026-05-19",
            "market_asof_date": "2026-05-19",
        },
        {
            "date": date(2026, 6, 26),
            "ticker": "MIC",
            "nav": 14.4528,
            "close_price": 21.67,
            "source_provider": "merged",
            "source_url": "https://example/#as_of=2026-06-26",
            "stale_kind": None,
            "issuer_asof_date": "2026-06-26",
            "market_asof_date": "2026-06-26",
        },
        {
            "date": date(2026, 7, 14),
            "ticker": "SNDQ",
            "nav": 2.6829,
            "close_price": 2.68,
            "source_provider": "tradr_axs",
            "source_url": "https://example/#as_of=2026-07-14",
            "stale_kind": None,
            "issuer_asof_date": "2026-07-14",
            "market_asof_date": "2026-07-14",
        },
    ])
    out = stamp_metric_asof_metadata(rows)
    aphu = out.loc[out["ticker"] == "APHU"].iloc[0]
    mic = out.loc[out["ticker"] == "MIC"].iloc[0]
    sndq = out.loc[out["ticker"] == "SNDQ"].iloc[0]
    assert bool(aphu["premium_discount_eligible"]) is False
    assert aphu["premium_discount_status"] == "issuer_stale"
    assert bool(mic["premium_discount_eligible"]) is False
    assert mic["premium_discount_status"] == "split_basis_mismatch"
    assert bool(sndq["premium_discount_eligible"]) is True
    assert sndq["premium_discount_status"] == "valid"


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
        "SOXL": {"updated_at": fresh, "options": [{"strike": 47.0}]},
        "AMDL": {"updated_at": fresh, "options": []},
    }
    order = _order_yieldboost_refresh_symbols(
        ["AMD", "SOXX"],
        ["SOXL", "AMDL"],
        prior,
    )
    assert order.index("AMD") < order.index("SOXL")
    assert order.index("SOXX") < order.index("AMDL")
    assert set(order[:2]) == {"AMD", "SOXX"}
    # Empty-chain sleeves refresh before populated ones.
    assert order.index("AMDL") < order.index("SOXL")


def test_load_executed_delisted_off_universe_excludes_orphans_only():
    from ingest_etf_metrics import load_executed_delisted_off_universe

    payload = {
        "events": [
            {
                "type": "delisting",
                "status": "executed",
                "ticker": "ENPX",
                "execution_date": "2026-04-21",
            },
            {
                "type": "delisting",
                "status": "executed",
                "ticker": "SPY",
                "execution_date": "2026-04-21",
            },
        ]
    }
    path = Path(__file__).resolve().parent / "_tmp_corp_actions.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    try:
        off = load_executed_delisted_off_universe(
            universe={"AAPU"},
            corporate_actions_path=path,
            as_of=date(2026, 6, 2),
        )
    finally:
        path.unlink(missing_ok=True)
    assert off == {"ENPX", "SPY"}


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


def test_market_age_excludes_weekend_days():
    now = datetime(2026, 7, 13, 14, 0, tzinfo=UTC)
    assert _market_age_minutes("2026-07-11T10:00:00Z", now) == 14 * 60


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
    session_rows = out[out["date"] == date(2026, 5, 29)]
    assert len(session_rows) == 1
    assert session_rows.iloc[0]["stale_kind"] == "issuer_session_extend"
    assert isinstance(session_rows.iloc[0]["date"], date)


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

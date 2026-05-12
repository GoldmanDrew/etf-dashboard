"""Offline tests for ``recover_rex_nav_from_git_history`` (no git/Yahoo network)."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import recover_rex_nav_from_git_history as rec  # noqa: E402


def test_parse_as_of_iso():
    assert rec.parse_as_of_from_url("https://x/EOSU/#as_of=2026-05-06") == date(2026, 5, 6)
    assert rec.parse_as_of_from_url("a|b|https://w/#as_of=2026-04-22") == date(2026, 4, 22)
    assert rec.parse_as_of_from_url(None) is None
    assert rec.parse_as_of_from_url("") is None
    assert rec.parse_as_of_from_url("yfinance://EOSU") is None


def _store_row(**overrides):
    base = {
        "date": date(2026, 5, 4),
        "ticker": "EOSU",
        "nav": 33.33,
        "aum": 8_491_884.06,
        "shares_outstanding": 254_782.0,
        "shares_traded": 39_400.0,
        "close_price": 33.33,
        "underlying_adj_close": 6.18,
        "stale": False,
        "stale_age_bdays": None,
        "source_provider": "rex_shares",
        "source_url": "https://www.rexshares.com/EOSU/#as_of=2026-05-04",
        "ingested_at_utc": "2026-05-12T13:00:00+00:00",
        "status": "ok",
    }
    base.update(overrides)
    return base


def test_merge_history_inserts_missing_session():
    df = pd.DataFrame([_store_row()])
    sessions = {
        ("EOSU", date(2026, 5, 6)): {
            "nav": 35.32,
            "aum": 8_645_700.24,
            "shares_outstanding": 244_782.0,
            "source_url": "https://www.rexshares.com/EOSU/#as_of=2026-05-06",
            "from_sha": "deadbeef",
            "from_commit_iso": "2026-05-09T22:50:29+00:00",
        },
    }
    out, report = rec.merge_history_into_store(df, sessions)
    assert len(out) == 2
    new_row = out[(out["ticker"] == "EOSU") & (out["date"] == date(2026, 5, 6))].iloc[0]
    assert abs(float(new_row["nav"]) - 35.32) < 1e-6
    assert abs(float(new_row["aum"]) - 8_645_700.24) < 1e-3
    assert int(new_row["shares_outstanding"]) == 244_782
    assert new_row["source_provider"] == "rex_shares_history"
    assert new_row["status"] == "ok"
    assert any(r["action"] == "insert_new_row" for r in report)


def test_merge_history_fills_null_nav():
    df = pd.DataFrame([
        _store_row(
            date=date(2026, 5, 6),
            nav=None, aum=None, shares_outstanding=None,
            source_provider="yahoo_bootstrap",
            source_url=None,
            status="missing",
        ),
    ])
    sessions = {
        ("EOSU", date(2026, 5, 6)): {
            "nav": 35.32,
            "aum": 8_645_700.24,
            "shares_outstanding": 244_782.0,
            "source_url": "https://www.rexshares.com/EOSU/#as_of=2026-05-06",
            "from_sha": "deadbeef",
            "from_commit_iso": "2026-05-09T22:50:29+00:00",
        },
    }
    out, report = rec.merge_history_into_store(df, sessions)
    assert len(out) == 1
    row = out.iloc[0]
    assert abs(float(row["nav"]) - 35.32) < 1e-6
    assert abs(float(row["aum"]) - 8_645_700.24) < 1e-3
    assert int(row["shares_outstanding"]) == 244_782
    assert row["status"] == "ok"
    assert any(r["action"] == "update_existing_row" for r in report)


def test_merge_history_does_not_override_present_nav_with_rex_url():
    """If store already has a positive NAV from issuer feed, do not overwrite."""
    df = pd.DataFrame([
        _store_row(date=date(2026, 5, 7), nav=33.52,
                   source_url="https://www.rexshares.com/EOSU/#as_of=2026-05-07"),
    ])
    sessions = {
        ("EOSU", date(2026, 5, 7)): {
            "nav": 99.99,
            "aum": None,
            "shares_outstanding": None,
            "source_url": "https://www.rexshares.com/EOSU/#as_of=2026-05-07",
            "from_sha": "abcd",
            "from_commit_iso": "2026-05-08T00:00:00+00:00",
        },
    }
    out, _ = rec.merge_history_into_store(df, sessions)
    assert len(out) == 1
    assert abs(float(out.iloc[0]["nav"]) - 33.52) < 1e-6


def test_merge_history_preserves_merged_provider_when_rex_url_present():
    df = pd.DataFrame([
        _store_row(
            date=date(2026, 5, 5),
            ticker="SOLX",
            source_provider="merged",
            source_url=("https://www.rexshares.com/SOLX/#as_of=2026-05-05|"
                        "yfinance://SOLX|polygon://SOLX"),
            nav=8.62, aum=None, shares_outstanding=None, close_price=8.61,
        ),
    ])
    sessions = {
        ("SOLX", date(2026, 5, 5)): {
            "nav": 8.62,
            "aum": None,
            "shares_outstanding": None,
            "source_url": "https://www.rexshares.com/SOLX/#as_of=2026-05-05",
            "from_sha": "deadbeef",
            "from_commit_iso": "2026-05-09T00:00:00+00:00",
        },
    }
    out, report = rec.merge_history_into_store(df, sessions)
    assert len(out) == 1
    assert out.iloc[0]["source_provider"] == "merged"
    assert "rexshares.com" in str(out.iloc[0]["source_url"])
    update_reports = [r for r in report if r["action"] == "update_existing_row"]
    if update_reports:
        assert "source_provider" not in update_reports[0].get("changed", {})

"""Offline tests for ``migrate_etf_metrics_valuation_dates`` (no Yahoo network)."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import migrate_etf_metrics_valuation_dates as mig  # noqa: E402


def test_parse_valuation_date_iso_and_mdy():
    assert mig.parse_valuation_date_from_url("https://x/EOSU/#as_of=2026-04-28") == date(2026, 4, 28)
    assert mig.parse_valuation_date_from_url("a|b#as_of=04/28/2026") == date(2026, 4, 28)
    assert mig.parse_valuation_date_from_url("u#nav_date=2025-01-02") == date(2025, 1, 2)
    assert mig.parse_valuation_date_from_url("u#ticker=SSO&date=2026-04-20") == date(2026, 4, 20)


def _row(**kwargs):
    base = {
        "date": date(2026, 4, 29),
        "ticker": "EOSU",
        "nav": 33.52,
        "aum": 8.2e6,
        "shares_outstanding": 244_782.0,
        "shares_traded": 100.0,
        "close_price": 33.52,
        "underlying_adj_close": 6.0,
        "stale": True,
        "stale_age_bdays": 1,
        "source_provider": "rex_shares",
        "source_url": "https://www.rexshares.com/EOSU/#as_of=2026-04-28",
        "ingested_at_utc": "2026-04-30T00:00:00+00:00",
        "status": "ok",
    }
    base.update(kwargs)
    return base


def test_apply_url_migrations_relabel_in_place():
    df = pd.DataFrame([_row()])
    rep: list[dict] = []
    n_r, n_m = mig.apply_url_migrations(df, rep, since=None, tickers=None)
    assert n_r == 1 and n_m == 0
    assert df.iloc[0]["date"] == date(2026, 4, 28)
    assert pd.isna(df.iloc[0]["close_price"]) or df.iloc[0]["close_price"] is None
    assert len(rep) == 1


def test_apply_url_migrations_merge_into_existing():
    wrong = _row()
    target = _row(
        date=date(2026, 4, 28),
        nav=10.0,
        close_price=33.52,
        source_provider="yahoo_bootstrap",
        source_url=None,
        status="missing",
    )
    df = pd.DataFrame([target, wrong])
    rep: list[dict] = []
    n_r, n_m = mig.apply_url_migrations(df, rep, since=None, tickers=None)
    assert n_m == 1 and n_r == 0
    assert len(df) == 1
    row = df.iloc[0]
    assert row["date"] == date(2026, 4, 28)
    assert abs(float(row["nav"]) - 33.52) < 0.01


def test_fix_carry_forward_clears_close():
    df = pd.DataFrame(
        [
            {
                "date": date(2026, 5, 8),
                "ticker": "EOSU",
                "nav": 33.5,
                "aum": 8e6,
                "shares_outstanding": 244_000.0,
                "shares_traded": 50.0,
                "close_price": 50.4,
                "underlying_adj_close": None,
                "stale": True,
                "stale_age_bdays": 1,
                "source_provider": "carry_forward",
                "source_url": "carry_forward://EOSU?from=2026-05-07",
                "ingested_at_utc": "2026-05-09T00:00:00+00:00",
                "status": "ok",
            },
        ],
    )
    rep: list[dict] = []
    n = mig.fix_carry_forward_closes(df, rep)
    assert n == 1
    assert df.iloc[0]["close_price"] is None or pd.isna(df.iloc[0]["close_price"])
    assert df.iloc[0]["shares_traded"] is None or pd.isna(df.iloc[0]["shares_traded"])

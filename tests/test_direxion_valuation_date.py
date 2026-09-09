"""
Direxion's holdings file for trade date T is valued at the T-1 close, so its
``TradeDate`` names the session the basket is *for*, not the session the NAV
describes. Every other issuer in etf_providers stamps the session it valued.

Taking TradeDate at face value cost us twice:

* The nightly run fires pre-open, by which time the file has rolled to today.
  The row landed a session ahead of its own NAV, was flagged ``issuer_early``,
  and ``browser_metrics_frame`` dropped it -- which is why all 35 Direxion funds
  appeared in the published bundle only on Mondays and Fridays, and why a "20
  trading day" realized-decay window for SOXL spanned 113 calendar days.
* The ~1/3 of rows that did survive carried the same one-session lag while
  marked ``premium_discount_eligible``, so their premium/discount was measuring
  a date mismatch: -52.7 bps median for direxion against ~0 bps for yfinance.

Measured over 1575 rows / 35 funds in data/etf_metrics_daily.csv:

    provider    median |nav/close-1|   median |nav/prev_close-1|
    direxion               2.767%                 0.133%
    proshares              0.092%                 2.336%
    polygon                0.000%                 4.930%
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from etf_providers import STALE_KIND_ISSUER_EARLY, STALE_KIND_ISSUER_LAG, DirexionProvider  # noqa: E402


@pytest.fixture
def provider():
    # __new__ skips the network session that __init__ builds; _resolve_valuation
    # is pure date arithmetic.
    return DirexionProvider.__new__(DirexionProvider)


def test_file_not_yet_rolled_keeps_the_session(provider):
    """Saturday run for Friday: the file still says Friday and is valued there."""
    d, stale, age, kind = provider._resolve_valuation(date(2026, 8, 14), date(2026, 8, 14))
    assert (d, stale, kind) == (date(2026, 8, 14), False, None)


def test_file_rolled_one_session_lands_on_the_session_it_valued(provider):
    """
    The regression. Tuesday pre-open, file says Wednesday-eve... no: file says
    Tuesday while we ingest Monday. Its NAV is Monday's close, so the row is
    Monday's -- and it is not stale, so it stays in the browser bundle and keeps
    its premium/discount eligibility.
    """
    d, stale, age, kind = provider._resolve_valuation(date(2026, 8, 18), date(2026, 8, 17))
    assert d == date(2026, 8, 17)
    assert stale is False
    assert kind is None, "a rolled file is expected, not stale"


def test_weekend_gap_is_one_session_not_three_days(provider):
    """Monday's file against Friday's session is one NYSE session, not three."""
    d, _, _, kind = provider._resolve_valuation(date(2026, 8, 17), date(2026, 8, 14))
    assert (d, kind) == (date(2026, 8, 14), None)


def test_file_far_ahead_is_left_alone(provider):
    """
    The holdings URL always serves the current file, so a backfill for an old
    date must never have today's NAV stamped onto it. More than one session
    ahead keeps the old, cautious behaviour.
    """
    d, _, _, kind = provider._resolve_valuation(date(2026, 8, 28), date(2026, 8, 17))
    assert d == date(2026, 8, 28)
    assert kind == STALE_KIND_ISSUER_EARLY


def test_lagging_file_still_reports_lag(provider):
    d, stale, age, kind = provider._resolve_valuation(date(2026, 8, 10), date(2026, 8, 17))
    assert d == date(2026, 8, 10)
    assert stale is True
    assert kind == STALE_KIND_ISSUER_LAG
    assert age and age > 0


def test_missing_trade_date_falls_back_to_the_target_session(provider):
    assert provider._resolve_valuation(None, date(2026, 8, 17))[0] == date(2026, 8, 17)

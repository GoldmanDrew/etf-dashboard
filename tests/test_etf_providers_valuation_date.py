"""Valuation-session ``date`` on provider rows (NAV vs Yahoo close alignment)."""
from __future__ import annotations

from datetime import date

import pytest

import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from etf_providers import (
    GraniteSharesProvider,
    REXSharesProvider,
    YieldMaxProvider,
    merge_provider_attempts,
    ProviderResult,
    SKIP_SESSION_DATE_ANCHOR_PROVIDERS,
)


def test_skip_anchor_provider_set_includes_merged():
    assert "merged" in SKIP_SESSION_DATE_ANCHOR_PROVIDERS
    assert "rex_shares" in SKIP_SESSION_DATE_ANCHOR_PROVIDERS


def test_granite_product_id_from_catalog_table_row():
    html = """
    <span data-id="1159" data-type="leveraged" class="etf-table-cell pId">
        <a href="/etfs/gou/" title="GOU"><span>GOU</span></a>
    </span>
    <span data-id="1122" data-type="leveraged" data-underlying="Intel Corp"
          class="etf-table-cell pId">
        <a href="/etfs/intw/" title="INTW">
            <span class="etf-table-cell--ticker__symbol">INTW</span>
        </a>
    </span>
    """
    assert GraniteSharesProvider._product_id_from_page(html, "intw", "INTW") == "1122"


def test_merge_provider_attempts_prefers_polygon_when_yfinance_nav_diverges():
    """INTW-style bug: stale yfinance totalAssets + last_price must not beat polygon close."""
    yfin = ProviderResult(
        date=date(2026, 5, 7),
        ticker="INTW",
        nav=366.20,
        aum=431_105_568.0,
        shares_outstanding=1_990_001.0,
        source_provider="yfinance",
        source_url="yfinance://INTW",
        status="ok",
        stale=False,
        stale_age_bdays=None,
    )
    poly = ProviderResult(
        date=date(2026, 5, 7),
        ticker="INTW",
        nav=285.72,
        aum=285.72 * 1_177_225.0,
        shares_outstanding=1_177_225.0,
        source_provider="polygon",
        source_url="polygon://INTW",
        status="ok",
        stale=False,
        stale_age_bdays=None,
    )
    m = merge_provider_attempts([yfin, poly], "INTW", date(2026, 5, 7))
    assert abs(m.nav - 285.72) < 0.02
    assert m.aum is not None and abs(m.aum - 431_105_568.0) > 1e6


def test_merge_provider_attempts_uses_nav_source_row_date():
    rex = ProviderResult(
        date=date(2024, 1, 2),
        ticker="EOSU",
        nav=10.0,
        aum=1e9,
        shares_outstanding=1e8,
        source_provider="rex_shares",
        source_url="https://rex.example/#as_of=2024-01-02",
        status="ok",
        stale=False,
        stale_age_bdays=None,
    )
    poly = ProviderResult(
        date=date(2024, 1, 5),
        ticker="EOSU",
        nav=12.0,
        aum=2e9,
        shares_outstanding=2e8,
        source_provider="polygon",
        source_url="polygon://EOSU",
        status="ok",
        stale=False,
        stale_age_bdays=None,
    )
    m = merge_provider_attempts([poly, rex], "EOSU", date(2024, 1, 5))
    assert m.date == date(2024, 1, 2)
    assert m.source_provider == "merged"
    assert abs(m.nav - 10.0) < 1e-6


@pytest.fixture
def rex_provider():
    return REXSharesProvider(session=None)


def test_rex_fetch_uses_html_as_of_session_date(monkeypatch, rex_provider):
    html = """
    <div class="t-col t-label">Fund Assets</div><div class="t-col t-data">$8,205,092.64</div>
    <div class="t-col t-label">Shares Outstanding</div><div class="t-col t-data">244,782</div>
    <div class="t-col t-label">Closing Price</div><div class="t-col t-data">$33.52</div>
    <div class="t-col t-label">As of</div><div class="t-col t-data">04/28/2026</div>
    """

    class Resp:
        status_code = 200
        text = html

    monkeypatch.setattr("etf_providers._get", lambda *a, **k: Resp())
    as_of = date(2026, 4, 29)
    r = rex_provider.fetch_for_date("EOSU", as_of)
    assert r.date == date(2026, 4, 28)
    assert r.source_provider == "rex_shares"
    assert r.stale is True


@pytest.fixture
def yieldmax_provider():
    return YieldMaxProvider(session=None)


def test_yieldmax_fetch_uses_html_as_of_session_date(monkeypatch, yieldmax_provider):
    html = """
    <div class="fund-label">As of:</div><div class="fund-value">04/16/2026</div>
    <div class="fund-label">Net Assets:</div><div class="fund-value">$1.15B</div>
    <div class="fund-label">NAV:</div><div class="fund-value">$23.41</div>
    <div class="fund-label">Shares Outstanding:</div><div class="fund-value">49,294,959</div>
    """

    class Resp:
        status_code = 200
        text = html

    monkeypatch.setattr("etf_providers._get", lambda *a, **k: Resp())
    as_of = date(2026, 4, 17)
    r = yieldmax_provider.fetch_for_date("NVDY", as_of)
    assert r.date == date(2026, 4, 16)
    assert r.source_provider == "yieldmax"
    assert r.stale is True

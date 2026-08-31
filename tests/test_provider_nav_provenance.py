"""NAV provenance on fallback providers, and the issuer-slug / REX-repair guards.

Regression cover for the 2026-08-30 staleness sweep:
  * yfinance ``navPrice`` is the only real NAV for funds with no issuer scraper;
    a swallowed ``.info`` failure used to leave ``nav = close`` unmarked, which
    reads downstream as a fund trading at a flat 0% premium.
  * polygon has no NAV concept at all — its ``nav`` is the exchange close.
  * rexshares.com serves one live NAV whatever session is asked for, so the tail
    repair must never back-stamp it onto older rows.
  * dead issuer slugs in ``KNOWN_TICKERS`` claim a ticker before any fallback.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import etf_providers as ep  # noqa: E402
from etf_providers import (  # noqa: E402
    STALE_KIND_MARKET_BACKED,
    GraniteSharesProvider,
    PolygonProvider,
    REXSharesProvider,
    YFinanceProvider,
)

AS_OF = date(2026, 8, 28)


class _FakeTicker:
    def __init__(self, info, raises=0):
        self._info = info
        self._raises = raises
        self.info_calls = 0

    @property
    def info(self):
        self.info_calls += 1
        if self.info_calls <= self._raises:
            raise RuntimeError("429 Too Many Requests")
        return self._info

    @property
    def fast_info(self):
        return object()


def _yf_provider(monkeypatch, ticker_obj, close):
    prov = YFinanceProvider.__new__(YFinanceProvider)
    prov._last_call = 0.0
    prov._min_int = 0.0
    prov._enabled = True
    prov._yf = type("YF", (), {"Ticker": staticmethod(lambda t: ticker_obj)})
    monkeypatch.setattr(prov, "_fetch_close_on", lambda t, d: close)
    monkeypatch.setattr(ep.time, "sleep", lambda *_: None)
    return prov


def test_yfinance_navprice_is_a_real_nav(monkeypatch):
    tk = _FakeTicker({"navPrice": 39.6854, "netAssets": 216_739_712.0, "sharesOutstanding": 5_461_000})
    res = _yf_provider(monkeypatch, tk, close=40.83).fetch_for_date("UVIX", AS_OF)
    assert res.nav == pytest.approx(39.6854)
    assert res.market_close == pytest.approx(40.83)
    assert res.stale is False
    assert res.stale_kind is None
    assert res.aum == pytest.approx(216_739_712.0)
    assert res.status == "ok"


def test_yfinance_without_navprice_is_flagged_market_backed(monkeypatch):
    """No navPrice means the 'NAV' is the close — it must not look like a real NAV."""
    tk = _FakeTicker({"sharesOutstanding": 5_461_000})
    res = _yf_provider(monkeypatch, tk, close=40.83).fetch_for_date("UVIX", AS_OF)
    assert res.nav == pytest.approx(40.83)
    assert res.stale is True
    assert res.stale_kind == STALE_KIND_MARKET_BACKED
    assert "#nav=close" in res.source_url


def test_yfinance_info_is_retried_before_giving_up(monkeypatch):
    """`.info` is the fragile call and the only navPrice source; retry it."""
    tk = _FakeTicker({"navPrice": 12.5}, raises=2)
    res = _yf_provider(monkeypatch, tk, close=12.0).fetch_for_date("JEPI", AS_OF)
    assert tk.info_calls == 3
    assert res.nav == pytest.approx(12.5)
    assert res.stale_kind is None


def test_yfinance_info_exhausted_falls_back_but_stays_flagged(monkeypatch):
    tk = _FakeTicker({"navPrice": 12.5}, raises=99)
    res = _yf_provider(monkeypatch, tk, close=12.0).fetch_for_date("JEPI", AS_OF)
    assert tk.info_calls == ep._YF_INFO_RETRIES
    assert res.nav == pytest.approx(12.0)
    assert res.stale_kind == STALE_KIND_MARKET_BACKED


def test_polygon_nav_is_the_close_and_says_so(monkeypatch):
    prov = PolygonProvider.__new__(PolygonProvider)
    prov.api_key = "k"
    prov._meta_cache = {}
    prov._price_cache = {}
    monkeypatch.setattr(prov, "_close", lambda t, d: 27.12)
    monkeypatch.setattr(prov, "_meta", lambda t: (1_000_000.0, None))
    res = prov.fetch_for_date("ETHU", AS_OF)
    assert res.nav == pytest.approx(27.12)
    assert res.market_close == pytest.approx(27.12)
    assert res.stale is True
    assert res.stale_kind == STALE_KIND_MARKET_BACKED


def test_get_retries_403_with_browser_headers():
    seen: list[dict] = []

    class _Resp:
        def __init__(self, code):
            self.status_code = code
            self.text = ""

    class _Session:
        timeout_sec = 5

        def get(self, url, timeout=None, headers=None):
            seen.append(dict(headers or {}))
            return _Resp(403 if len(seen) == 1 else 200)

    r = ep._get(_Session(), "https://www.direxion.com/holdings/TSLL.csv")
    assert r.status_code == 200
    assert len(seen) == 2
    assert seen[1]["Sec-Fetch-Mode"] == "navigate"
    assert seen[1]["Referer"] == "https://www.direxion.com/"


def test_get_does_not_touch_headers_on_success():
    """rexshares.com serves a slim shell to a full Chrome profile — never default to it."""
    seen: list[dict | None] = []

    class _Resp:
        status_code = 200
        text = ""

    class _Session:
        timeout_sec = 5

        def get(self, url, timeout=None, headers=None):
            seen.append(headers)
            return _Resp()

    ep._get(_Session(), "https://www.rexshares.com/mstu/")
    assert seen == [None]


@pytest.mark.parametrize("dead", ["ADBU", "HUTG", "SPOG", "TTXD", "LOFF"])
def test_dead_granite_slugs_are_not_claimed(dead):
    assert dead not in GraniteSharesProvider.KNOWN_TICKERS


@pytest.mark.parametrize("live", ["AMYY", "TSYY", "YBST", "CRY"])
def test_live_granite_slugs_survive(live):
    assert live in GraniteSharesProvider.KNOWN_TICKERS


@pytest.mark.parametrize("dead", ["BITX", "ETHZ", "SOLX", "XRPK", "PLTI"])
def test_dead_rex_slugs_are_not_claimed(dead):
    assert dead not in REXSharesProvider.KNOWN_TICKERS


@pytest.mark.parametrize("live", ["MSTU", "NVDX", "NVDQ", "ROBN", "XRPR"])
def test_live_rex_slugs_survive(live):
    assert live in REXSharesProvider.KNOWN_TICKERS


def _rex_frame():
    return pd.DataFrame(
        {
            "date": [date(2026, 8, 25), date(2026, 8, 26), date(2026, 8, 27)],
            "ticker": ["MSTU"] * 3,
            "nav": [28.71, 30.10, 31.00],
            "close_price": [28.86, 30.20, 31.10],
            "source_provider": ["rex_shares"] * 3,
        }
    )


def test_rex_repair_only_patches_the_issuer_as_of_session(monkeypatch):
    """The live page has no history: patching every date froze 45 days to one NAV."""
    import repair_rex_session_nav_close as rr

    live = ep.ProviderResult(
        date=date(2026, 8, 27), ticker="MSTU", nav=35.32, aum=5.9e8,
        shares_outstanding=16_796_897, source_provider="rex_shares",
        source_url="https://www.rexshares.com/mstu/#as_of=2026-08-27",
        status="ok", market_close=35.34,
    )
    monkeypatch.setattr(
        rr, "REXSharesProvider", lambda *a, **k: type("P", (), {"fetch_for_date": staticmethod(lambda s, d: live)})()
    )
    out, n = rr.repair_rex_rows(_rex_frame(), lookback_days=45, apply=False)

    assert n == 1
    by_date = {r.date: r for r in out.itertuples()}
    assert by_date[date(2026, 8, 27)].nav == pytest.approx(35.32)
    assert by_date[date(2026, 8, 27)].close_price == pytest.approx(35.34)
    # Older sessions keep their own NAV/close.
    assert by_date[date(2026, 8, 25)].nav == pytest.approx(28.71)
    assert by_date[date(2026, 8, 26)].nav == pytest.approx(30.10)
    assert by_date[date(2026, 8, 25)].close_price == pytest.approx(28.86)


def _pr(provider, nav, status, **kw):
    return ep.ProviderResult(
        date=AS_OF, ticker="XYZ", nav=nav, aum=kw.pop("aum", None),
        shares_outstanding=kw.pop("shares", None), source_provider=provider,
        source_url=f"{provider}://XYZ", status=status, **kw
    )


def test_merge_keeps_issuer_nav_unflagged_when_fallback_is_market_backed():
    """A discarded polygon attempt must not stamp market_backed on an issuer NAV."""
    issuer = _pr("granite_shares", 20.5, "partial", aum=6.2e7)
    poly = _pr("polygon", 20.3, "partial", aum=6.0e7, shares=3.0e6,
               stale=True, stale_age_bdays=0, stale_kind=STALE_KIND_MARKET_BACKED)
    merged = ep.merge_provider_attempts([issuer, poly], "XYZ", AS_OF)
    assert merged.nav == pytest.approx(20.5)
    assert merged.stale is False
    assert merged.stale_kind is None


def test_merge_flags_market_backed_when_nav_came_from_fallback():
    poly = _pr("polygon", 20.3, "partial", aum=6.0e7, shares=3.0e6,
               stale=True, stale_age_bdays=0, stale_kind=STALE_KIND_MARKET_BACKED)
    merged = ep.merge_provider_attempts([poly], "XYZ", AS_OF)
    assert merged.nav == pytest.approx(20.3)
    assert merged.stale is True
    assert merged.stale_kind == STALE_KIND_MARKET_BACKED

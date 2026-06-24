"""NAV vs market close / premium-discount ingest behavior."""
from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

import pandas as pd
import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from etf_providers import (  # noqa: E402
    REXSharesProvider,
    _prem_disc_pct,
    resolve_issuer_nav,
)
from ingest_etf_metrics import (  # noqa: E402
    compute_prem_disc_health,
    merge_close_prices,
)


def test_resolve_issuer_nav_prefers_published_nav():
    nav = resolve_issuer_nav(
        published_nav=12.12,
        aum=14_116_842.67,
        shares=1_169_581,
        market_close=12.07,
    )
    assert nav is not None and abs(nav - 12.12) < 1e-6


def test_prem_disc_pct_smup_example():
    pct = _prem_disc_pct(12.12, 12.07)
    assert pct is not None
    assert abs(pct - (-0.4125)) < 0.05


def test_rex_fetch_parses_nav_close_and_prem(monkeypatch):
    html = """
    <div class="t-col t-label">NAV</div><div class="t-col t-data">$12.12</div>
    <div class="t-col t-label">Closing Price</div><div class="t-col t-data">$12.07</div>
    <div class="t-col t-label">Discount/Premium</div><div class="t-col t-data">-0.380000%</div>
    <div class="t-col t-label">Fund Assets</div><div class="t-col t-data">$14,116,842.67</div>
    <div class="t-col t-label">Shares Outstanding</div><div class="t-col t-data">1,169,581</div>
    <div class="t-col t-label">As of</div><div class="t-col t-data">06/01/2026</div>
    """

    class Resp:
        status_code = 200
        text = html

    monkeypatch.setattr("etf_providers._get", lambda *a, **k: Resp())
    r = REXSharesProvider(session=None).fetch_for_date("SMUP", date(2026, 6, 2))
    assert abs(r.nav - 12.12) < 0.01
    assert r.market_close is not None and abs(r.market_close - 12.07) < 0.01
    assert r.issuer_prem_disc_pct is not None
    assert abs(r.issuer_prem_disc_pct - (-0.38)) < 0.05
    implied = 14_116_842.67 / 1_169_581
    assert abs(r.nav - implied) > 0.03


def test_merge_close_prices_prefers_existing_issuer_close():
    base = pd.DataFrame([
        {
            "date": date(2026, 6, 1),
            "ticker": "SMUP",
            "nav": 12.12,
            "close_price": 12.07,
            "source_url": "https://www.rexshares.com/SMUP/#as_of=2026-06-01",
        },
    ])
    yahoo = pd.DataFrame([
        {"date": date(2026, 6, 1), "ticker": "SMUP", "close_price": 12.12, "shares_traded": 1000},
    ])
    out = merge_close_prices(base, yahoo)
    assert float(out.iloc[0]["close_price"]) == 12.07


def test_compute_prem_disc_health_lockstep():
    latest = pd.DataFrame([
        {"ticker": "A", "nav": 10.0, "close_price": 10.0, "source_provider": "rex_shares", "aum": 100, "shares_outstanding": 10},
        {"ticker": "B", "nav": 10.0, "close_price": 10.05, "source_provider": "rex_shares", "aum": 100, "shares_outstanding": 10},
    ])
    h = compute_prem_disc_health(latest, lockstep_bps=1.0)
    assert h["rows_with_nav_and_close"] == 2
    assert h["lockstep_nav_close_count"] == 1


def test_compute_prem_disc_health_tracks_material_rex_implied_nav_divergence():
    latest = pd.DataFrame(
        [
            {
                "ticker": "DIREXION",
                "nav": 1.0,
                "close_price": 1.0,
                "source_provider": "direxion",
                "aum": 2_000.0,
                "shares_outstanding": 1_000.0,
            },
            {
                "ticker": "STALE_REX",
                "nav": 1.0,
                "close_price": 1.0,
                "source_provider": "rex_shares",
                "aum": 2_000.0,
                "shares_outstanding": 1_000.0,
                "stale": True,
            },
            {
                "ticker": "ROUNDED",
                "nav": 10.0,
                "close_price": 10.0,
                "source_provider": "rex_shares",
                "aum": 100_400.0,
                "shares_outstanding": 10_000.0,
            },
            {
                "ticker": "MATERIAL",
                "nav": 10.0,
                "close_price": 10.0,
                "source_provider": "rex_shares",
                "aum": 99_000.0,
                "shares_outstanding": 10_000.0,
            },
        ]
    )

    h = compute_prem_disc_health(latest)

    assert h["rex_nav_vs_implied_div_bps_gt5"] == 2
    assert h["rex_nav_vs_implied_div_bps_gt50"] == 1
    assert h["rex_nav_vs_implied_div_bps_max"] > 90.0

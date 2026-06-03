"""Regression: repair_close_price_split_basis_mismatch fixes Yahoo split-day close basis."""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import ingest_etf_metrics as iem  # noqa: E402


def _row(
    d: str,
    *,
    nav: float,
    sh: float,
    close: float,
    aum: float,
    ticker: str = "BAIG",
) -> dict:
    return {
        "date": d,
        "ticker": ticker,
        "nav": nav,
        "aum": aum,
        "shares_outstanding": sh,
        "shares_traded": None,
        "close_price": close,
        "underlying_adj_close": None,
        "stale": False,
        "stale_age_bdays": None,
        "source_provider": "x",
        "source_url": "",
        "ingested_at_utc": "2026-05-06T00:00:00Z",
        "status": "ok",
    }


def test_reverse_split_baig_production_wrong_shares_polygon_hint(tmp_path: Path):
    """BAIG May 2026: NAV post-split while Yahoo close stale; shares wrong so TNA not flat.

    corporate_actions execution_date 2026-05-05 places a ±1d hint on 2026-05-04.
    """
    ca = tmp_path / "ca.json"
    ca.write_text(
        json.dumps(
            {
                "events": [
                    {
                        "type": "reverse_split",
                        "ticker": "BAIG",
                        "execution_date": "2026-05-05",
                        "ratio_from": 10.0,
                        "ratio_to": 1.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    df = pd.DataFrame(
        [
            _row("2026-05-01", nav=3.72, sh=2_933_537.0, close=3.72, aum=10_912_757.0),
            _row("2026-05-04", nav=37.89, sh=4_330_000.0, close=3.79, aum=164_000_000.0),
        ]
    )
    out, n = iem.repair_close_price_split_basis_mismatch(df, corporate_actions_path=ca)
    assert n == 1
    may4 = out.loc[out["date"] == date(2026, 5, 4), "close_price"].iloc[0]
    assert abs(float(may4) - 37.9) < 0.06
    assert abs(float(may4) / 37.89 - 1.0) < 0.02


def test_reverse_split_baig_style_repair():
    aum = 10_912_757.0
    df = pd.DataFrame(
        [
            _row("2026-05-01", nav=3.72, sh=2_933_537.0, close=3.72, aum=aum),
            _row("2026-05-04", nav=34.89, sh=312_776.0, close=3.79, aum=aum),
            _row("2026-05-05", nav=37.51, sh=290_891.0, close=37.51, aum=aum),
        ]
    )
    out, n = iem.repair_close_price_split_basis_mismatch(df, corporate_actions_path=None)
    assert n == 1
    may4 = out.loc[out["date"] == date(2026, 5, 4), "close_price"].iloc[0]
    assert abs(float(may4) - 37.9) < 0.05
    assert abs(float(may4) / 34.89 - 1.0) < 0.12


def test_forward_split_repair():
    aum = 100_000_000.0
    df = pd.DataFrame(
        [
            _row("2026-04-01", nav=100.0, sh=1_000_000.0, close=100.0, aum=aum, ticker="FWDS"),
            _row("2026-04-02", nav=20.0, sh=5_000_000.0, close=100.0, aum=aum, ticker="FWDS"),
        ]
    )
    out, n = iem.repair_close_price_split_basis_mismatch(df)
    assert n == 1
    fixed = float(out.iloc[1]["close_price"])
    assert abs(fixed - 20.0) < 1e-6


def test_idempotent_when_close_matches_nav():
    aum = 10_912_757.0
    df = pd.DataFrame(
        [
            _row("2026-05-01", nav=3.72, sh=2_933_537.0, close=3.72, aum=aum),
            _row("2026-05-04", nav=34.89, sh=312_776.0, close=37.9, aum=aum),
        ]
    )
    out, n = iem.repair_close_price_split_basis_mismatch(df)
    assert n == 0
    assert float(out.iloc[1]["close_price"]) == 37.9


def test_double_run_is_noop():
    aum = 10_912_757.0
    df = pd.DataFrame(
        [
            _row("2026-05-01", nav=3.72, sh=2_933_537.0, close=3.72, aum=aum),
            _row("2026-05-04", nav=34.89, sh=312_776.0, close=3.79, aum=aum),
        ]
    )
    once, n1 = iem.repair_close_price_split_basis_mismatch(df)
    twice, n2 = iem.repair_close_price_split_basis_mismatch(once)
    assert n1 == 1
    assert n2 == 0


def test_corporate_actions_hint_snaps_ratio(tmp_path: Path):
    ca = tmp_path / "ca.json"
    ca.write_text(
        json.dumps(
            {
                "events": [
                    {
                        "type": "reverse_split",
                        "ticker": "HNT",
                        "execution_date": "2026-05-05",
                        "ratio_from": 10,
                        "ratio_to": 1,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    aum = 50_000_000.0
    df = pd.DataFrame(
        [
            _row("2026-05-04", nav=5.0, sh=10_000_000.0, close=5.0, aum=aum, ticker="HNT"),
            _row("2026-05-05", nav=48.0, sh=1_000_000.0, close=4.9, aum=aum, ticker="HNT"),
        ]
    )
    out, n = iem.repair_close_price_split_basis_mismatch(df, corporate_actions_path=ca)
    assert n == 1
    assert abs(float(out.iloc[1]["close_price"]) / 48.0 - 1.0) < 0.05


def test_no_repair_on_normal_drift():
    df = pd.DataFrame(
        [
            _row("2026-04-01", nav=10.0, sh=1e6, close=10.0, aum=10e6, ticker="ZZZ"),
            _row("2026-04-02", nav=10.1, sh=1e6, close=10.05, aum=10.1e6, ticker="ZZZ"),
        ]
    )
    out, n = iem.repair_close_price_split_basis_mismatch(df)
    assert n == 0
    assert float(out.iloc[1]["close_price"]) == 10.05


def test_merge_close_prices_attaches_yahoo_volume_as_shares_traded():
    base = pd.DataFrame([
        _row("2026-04-01", nav=10.0, sh=1e6, close=9.9, aum=10e6, ticker="VOL"),
    ])
    base["date"] = pd.to_datetime(base["date"]).dt.date
    close_df = pd.DataFrame([
        {
            "date": date(2026, 4, 1),
            "ticker": "VOL",
            "close_price": 10.05,
            "shares_traded": 123456,
        }
    ])
    out = iem.merge_close_prices(base, close_df)
    got = out.iloc[0]
    # Issuer close is kept; Yahoo only supplies volume when close already set.
    assert float(got["close_price"]) == 9.9
    assert int(got["shares_traded"]) == 123456


def test_merge_close_prices_yahoo_fills_missing_close():
    base = pd.DataFrame([
        _row("2026-04-01", nav=10.0, sh=1e6, close=None, aum=10e6, ticker="VOL"),
    ])
    base["date"] = pd.to_datetime(base["date"]).dt.date
    close_df = pd.DataFrame([
        {"date": date(2026, 4, 1), "ticker": "VOL", "close_price": 10.05, "shares_traded": 99},
    ])
    out = iem.merge_close_prices(base, close_df)
    assert float(out.iloc[0]["close_price"]) == 10.05

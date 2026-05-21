"""Tests for Granite YieldBOOST holdings XLS parsing and put-spread pairing."""
from __future__ import annotations

from datetime import date
from io import BytesIO

import pandas as pd
import pytest

import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from yieldboost_holdings import (  # noqa: E402
    format_occ_ticker,
    granite_xls_rows_to_holdings,
    pair_put_spreads_from_holdings,
    parse_granite_option_description,
    resolve_sleeve_ticker,
)


def test_parse_granite_option_description_cwy():
    parsed = parse_granite_option_description("2CWVX 05/26/2026 P27.89")
    assert parsed is not None
    assert parsed.root == "2CWVX"
    assert parsed.expiry == date(2026, 5, 26)
    assert parsed.put_call == "P"
    assert parsed.strike == pytest.approx(27.89)


def test_resolve_sleeve_ticker():
    assert resolve_sleeve_ticker("2MSTU", "MSTR") == "MSTU"
    assert resolve_sleeve_ticker("2CWVX", "CRWV") == "CRWV"
    assert resolve_sleeve_ticker("2TSLL", "TSLA") == "TSLL"


def test_format_occ_ticker():
    occ = format_occ_ticker("MSTU", date(2026, 5, 22), "P", 7.03)
    assert occ == "MSTU260522P00007030"


def _cwy_fixture_df() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "Position Date": "2026-05-20",
            "ETF Ticker": "CWY",
            "Ticker/Cusip": "USD",
            "Security Description": "US Dollars",
            "Shares/Par": 264422.14438,
            "Asset Group": "CU",
            "Mat/Exp Date": pd.NaT,
            "Market/Notional Value": 264422.14438,
            "Percentage Weighting": 0.412182,
        },
        {
            "Position Date": "2026-05-20",
            "ETF Ticker": "CWY",
            "Ticker/Cusip": "2CWVX 05/26/2026 P27.89",
            "Security Description": "2CWVX 05/26/2026 P27.89",
            "Shares/Par": 240.0,
            "Asset Group": "O",
            "Mat/Exp Date": "2026-05-26",
            "Market/Notional Value": 56796.72,
            "Percentage Weighting": 0.088535,
        },
        {
            "Position Date": "2026-05-20",
            "ETF Ticker": "CWY",
            "Ticker/Cusip": "2CWVX 05/26/2026 P29.44",
            "Security Description": "2CWVX 05/26/2026 P29.44",
            "Shares/Par": -240.0,
            "Asset Group": "O",
            "Mat/Exp Date": "2026-05-26",
            "Market/Notional Value": -77171.928,
            "Percentage Weighting": -0.120296,
        },
    ])


def test_granite_xls_rows_to_holdings():
    rows = granite_xls_rows_to_holdings(
        _cwy_fixture_df(),
        etf_ticker="CWY",
        fallback_as_of=date(2026, 5, 19),
        underlying="CRWV",
        source_url="https://example.com/cwy.xls",
    )
    opts = [r for r in rows if r["security_type"] == "OPTION_PUT"]
    assert len(opts) == 2
    longs = [r for r in opts if r["option_side"] == "long"]
    shorts = [r for r in opts if r["option_side"] == "short"]
    assert len(longs) == 1 and len(shorts) == 1
    assert longs[0]["option_strike"] == pytest.approx(27.89)
    assert shorts[0]["option_strike"] == pytest.approx(29.44)
    assert longs[0]["position_ticker"].startswith("CRWV")


def test_pair_put_spreads_from_holdings():
    rows = granite_xls_rows_to_holdings(
        _cwy_fixture_df(),
        etf_ticker="CWY",
        fallback_as_of=date(2026, 5, 19),
        underlying="CRWV",
        source_url="https://example.com/cwy.xls",
    )
    hdf = pd.DataFrame(rows)
    spreads = pair_put_spreads_from_holdings(
        hdf, underlying_by_etf={"CWY": "CRWV"}, as_of=date(2026, 5, 20),
    )
    assert len(spreads) == 1
    s = spreads[0]
    assert s.yb_etf == "CWY"
    assert s.strike_long == pytest.approx(27.89)
    assert s.strike_short == pytest.approx(29.44)
    assert s.sleeve_2x_etf == "CRWV"
    assert s.is_front is True

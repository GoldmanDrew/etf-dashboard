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
    build_occ_symbol_index,
    build_vrp_live_payload,
    build_yieldboost_rv_map,
    extract_rv_30d_annual,
    format_occ_ticker,
    held_contract_needs_occ_quote,
    load_yieldboost_front_contracts,
    load_yieldboost_held_expiries_by_sleeve,
    granite_xls_rows_to_holdings,
    held_strike_band,
    infer_etf_ticker_from_source_url,
    load_sleeve_by_yb_from_screener,
    load_yieldboost_target_strikes_by_sleeve,
    load_yieldboost_sleeve_symbols_from_spreads,
    load_yieldboost_underlying_symbols_from_spreads,
    normalize_holdings_dataframe,
    normalize_occ_symbol,
    pair_put_spreads_from_holdings,
    parse_granite_option_description,
    resolve_iv_source,
    resolve_occ_ticker_for_contract,
    resolve_sleeve_ticker,
    spreads_json_to_put_spread_legs,
)


def test_extract_rv_30d_annual_dashboard_shape():
    stats = {
        "1M": {"etf": 1.361367, "underlying": 0.693201},
        "6M": {"etf": 1.556064, "underlying": 0.8},
    }
    assert extract_rv_30d_annual(stats) == pytest.approx(1.361367)


def test_extract_rv_30d_annual_yahoo_window_shape():
    stats = {
        "1M": {"vol_annual": 0.95, "ewma_vol_annual": 1.1},
    }
    assert extract_rv_30d_annual(stats) == pytest.approx(0.95)


def test_build_yieldboost_rv_map_prefers_yahoo_over_dashboard():
    rv = build_yieldboost_rv_map(
        dashboard_records=[{"symbol": "MSTU", "realized_vol": {"1M": {"etf": 1.0}}}],
        realized_vol_by_symbol={"MSTU": {"1M": {"vol_annual": 1.25}}},
    )
    assert rv["MSTU"] == pytest.approx(1.25)


def test_build_vrp_live_payload_populates_rv_and_vrp():
    spread = pair_put_spreads_from_holdings(
        pd.DataFrame([{
            "as_of_date": "2026-05-21",
            "etf_ticker": "MTYY",
            "position_ticker": "MSTU260522P00007030",
            "security_type": "OPTION_PUT",
            "shares": 670.0,
            "option_root": "2MSTU",
            "option_expiry": "2026-05-22",
            "option_strike": 7.03,
            "option_put_call": "P",
            "option_side": "long",
        }, {
            "as_of_date": "2026-05-21",
            "etf_ticker": "MTYY",
            "position_ticker": "MSTU260522P00007420",
            "security_type": "OPTION_PUT",
            "shares": -670.0,
            "option_root": "2MSTU",
            "option_expiry": "2026-05-22",
            "option_strike": 7.42,
            "option_put_call": "P",
            "option_side": "short",
        }]),
        underlying_by_etf={"MTYY": "MSTR"},
        as_of=date(2026, 5, 21),
    )
    assert len(spread) == 1
    options_cache = {
        "symbols": {
            "MSTU": {
                "spot": 6.66,
                "updated_at": "2026-05-22T00:00:00Z",
                "options": [
                    {
                        "expiration_date": "2026-05-22",
                        "contract_type": "put",
                        "strike_price": 7.03,
                        "iv": 2.2666,
                        "mid": 0.45,
                    },
                    {
                        "expiration_date": "2026-05-22",
                        "contract_type": "put",
                        "strike_price": 7.42,
                        "iv": 2.2314,
                        "mid": 0.85,
                    },
                ],
            },
        },
    }
    rv_map = {"MSTU": 1.36, "MSTR": 0.69}
    payload = build_vrp_live_payload(spread, options_cache, rv_map=rv_map)
    row = payload["rows"][0]
    assert row["rv_30d_2x"] == pytest.approx(1.36)
    assert row["iv_put_long"] == pytest.approx(2.2666)
    assert row["vrp_vol_2x"] == pytest.approx((2.2666 + 2.2314) / 2 - 1.36)
    assert row["iv_source"] == "holdings_exact"


def test_resolve_iv_source_labels():
    assert resolve_iv_source(
        {"matched": True, "exact_strike": True, "expiry_in_chain": True, "iv": 1.0},
        {"matched": True, "exact_strike": True, "expiry_in_chain": True, "iv": 1.1},
    ) == "holdings_exact"
    assert resolve_iv_source(
        {"matched": True, "exact_strike": False, "expiry_in_chain": True, "iv": 1.0},
        {"matched": True, "exact_strike": True, "expiry_in_chain": True, "iv": 1.1},
    ) == "holdings_nearest_strike"
    assert resolve_iv_source(
        {"matched": False, "exact_strike": False, "expiry_in_chain": False, "iv": None},
        {"matched": False, "exact_strike": False, "expiry_in_chain": False, "iv": None},
    ) == "holdings_missing_chain"


def test_held_contract_needs_occ_quote_when_nearest_iv_without_exact_mid():
    rows = [
        {
            "expiration_date": "2026-05-22",
            "contract_type": "put",
            "strike_price": 35.0,
            "iv": 0.82,
            "mid": 0.15,
        },
    ]
    assert held_contract_needs_occ_quote(
        rows, expiry=date(2026, 5, 22), strike=34.86, put_call="P",
    )


def test_held_contract_needs_occ_quote_when_exact_iv_without_mid():
    rows = [
        {
            "expiration_date": "2026-05-22",
            "contract_type": "put",
            "strike_price": 34.86,
            "iv": 0.82,
            "mid": None,
        },
    ]
    assert held_contract_needs_occ_quote(
        rows, expiry=date(2026, 5, 22), strike=34.86, put_call="P",
    )


def test_build_vrp_live_payload_missing_chain_iv_source():
    spread = pair_put_spreads_from_holdings(
        pd.DataFrame([{
            "as_of_date": "2026-05-21",
            "etf_ticker": "AZYY",
            "position_ticker": "AMZZ260522P00034860",
            "security_type": "OPTION_PUT",
            "shares": 390.0,
            "option_root": "2AMZZ",
            "option_expiry": "2026-05-22",
            "option_strike": 34.86,
            "option_put_call": "P",
            "option_side": "long",
        }, {
            "as_of_date": "2026-05-21",
            "etf_ticker": "AZYY",
            "position_ticker": "AMZZ260522P00036790",
            "security_type": "OPTION_PUT",
            "shares": -390.0,
            "option_root": "2AMZZ",
            "option_expiry": "2026-05-22",
            "option_strike": 36.79,
            "option_put_call": "P",
            "option_side": "short",
        }]),
        underlying_by_etf={"AZYY": "AMZN"},
        as_of=date(2026, 5, 22),
    )
    options_cache = {
        "symbols": {
            "AMZZ": {
                "spot": 39.8,
                "options": [{
                    "expiration_date": "2026-06-18",
                    "contract_type": "put",
                    "strike_price": 35.0,
                    "iv": 0.62,
                    "mid": 0.95,
                }],
            },
        },
    }
    payload = build_vrp_live_payload(spread, options_cache, rv_map={"AMZZ": 0.44})
    row = payload["rows"][0]
    assert row["iv_put_long"] is None
    assert row["iv_source"] == "holdings_missing_chain"


def test_spreads_json_to_put_spread_legs_recomputes_front_per_etf():
    payload = {
        "spreads": [
            {
                "yb_etf": "CWY",
                "underlying": "CRWV",
                "option_root": "2CWVX",
                "sleeve_2x_etf": "CRWV",
                "expiry": "2026-05-26",
                "strike_long": 27.89,
                "strike_short": 29.44,
                "qty": 240.0,
                "holdings_as_of": "2026-05-21",
                "is_front": False,
            },
            {
                "yb_etf": "AMYY",
                "underlying": "AMD",
                "option_root": "2AMDL",
                "sleeve_2x_etf": "AMDL",
                "expiry": "2026-05-22",
                "strike_long": 43.29,
                "strike_short": 45.7,
                "qty": 890.0,
                "holdings_as_of": "2026-05-21",
                "is_front": True,
            },
        ],
    }
    legs = spreads_json_to_put_spread_legs(payload)
    cwy = next(s for s in legs if s.yb_etf == "CWY")
    amyy = next(s for s in legs if s.yb_etf == "AMYY")
    assert cwy.is_front is True
    assert amyy.is_front is True


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
    assert resolve_sleeve_ticker("2HIMZ", "HIMS", yb_etf="HMYY") == "HIMZ"
    assert resolve_sleeve_ticker("2SPXL", "SPY", yb_etf="YSPY") == "SPXL"
    assert resolve_sleeve_ticker("2NUGT", "GDX", yb_etf="NUGY") == "NUGT"
    assert resolve_sleeve_ticker("2FAS", "XLF", yb_etf="FINY") == "FAS"
    assert resolve_sleeve_ticker("2ROBN", "HOOD", yb_etf="HOYY") == "ROBN"
    assert resolve_sleeve_ticker("2AMDL", "AMD", yb_etf="AMYY") == "AMDL"


@pytest.mark.parametrize(
    ("root", "underlying", "yb_etf", "expected"),
    [
        ("2AMDL", "AMD", "AMYY", "AMDL"),
        ("2AMZZ", "AMZN", "AZYY", "AMZZ"),
        ("2BABX", "BABA", "BBYY", "BABX"),
        ("2BITX", "IBIT", "XBTY", "BITX"),
        ("2CONL", "COIN", "COYY", "CONL"),
        ("2CRCA", "CRCL", "CRY", "CRCA"),
        ("2CWVX", "CRWV", "CWY", "CRWV"),
        ("2ETHU", "ETHA", "XEY", "ETHU"),
        ("2FAS", "XLF", "FINY", "FAS"),
        ("2FBL", "META", "FBYY", "FBL"),
        ("2HIMZ", "HIMS", "HMYY", "HIMZ"),
        ("2IONL", "IONQ", "IOYY", "IONL"),
        ("2LABU", "XBI", "BIOY", "LABU"),
        ("2MRAL", "MARA", "MAAY", "MRAL"),
        ("2MSTU", "MSTR", "MTYY", "MSTU"),
        ("2MULL", "MU", "MUYY", "MULL"),
        ("2NUGT", "GDX", "NUGY", "NUGT"),
        ("2NVDL", "NVDA", "NVYY", "NVDL"),
        ("2PTIR", "PLTR", "PLYY", "PTIR"),
        ("2QBTX", "QBTS", "QBY", "QBTX"),
        ("2RGTX", "RGTI", "RGYY", "RGTX"),
        ("2RIOX", "RIOT", "RTYY", "RIOX"),
        ("2ROBN", "HOOD", "HOYY", "ROBN"),
        ("2SMCX", "SMCI", "SMYY", "SMCX"),
        ("2SOXL", "SOXX", "SEMY", "SOXL"),
        ("2SPXL", "SPY", "YSPY", "SPXL"),
        ("2TECL", "XLK", "TECY", "TECL"),
        ("2TMF", "TLT", "FIYY", "TMF"),
        ("2TQQQ", "QQQ", "TQQY", "TQQQ"),
        ("2TSLL", "TSLA", "TSYY", "TSLL"),
        ("2TSLR", "TSLA", "TSYY", "TSLR"),
        ("2TSMU", "TSM", "TMYY", "TSMU"),
    ],
)
def test_resolve_sleeve_ticker_all_yieldboost_roots(root, underlying, yb_etf, expected):
    assert resolve_sleeve_ticker(root, underlying, yb_etf=yb_etf) == expected


def test_pair_put_spreads_front_is_per_yb_etf():
    """Each YB ETF marks its own nearest expiry as front (not global min)."""
    rows = granite_xls_rows_to_holdings(
        _cwy_fixture_df(),
        etf_ticker="CWY",
        fallback_as_of=date(2026, 5, 19),
        underlying="CRWV",
        source_url="https://example.com/cwy.xls",
    )
    bioy_opts = pd.DataFrame([
        {
            "as_of_date": "2026-05-21",
            "etf_ticker": "BIOY",
            "position_ticker": "LABU260526P00144560",
            "security_type": "OPTION_PUT",
            "shares": 50.0,
            "option_root": "2LABU",
            "option_expiry": "2026-05-26",
            "option_strike": 144.56,
            "option_put_call": "P",
            "option_side": "long",
        },
        {
            "as_of_date": "2026-05-21",
            "etf_ticker": "BIOY",
            "position_ticker": "LABU260526P00152590",
            "security_type": "OPTION_PUT",
            "shares": -50.0,
            "option_root": "2LABU",
            "option_expiry": "2026-05-26",
            "option_strike": 152.59,
            "option_put_call": "P",
            "option_side": "short",
        },
    ])
    hdf = pd.concat([pd.DataFrame(rows), bioy_opts], ignore_index=True)
    spreads = pair_put_spreads_from_holdings(
        hdf,
        underlying_by_etf={"CWY": "CRWV", "BIOY": "XBI"},
        as_of=date(2026, 5, 21),
    )
    cwy = [s for s in spreads if s.yb_etf == "CWY"]
    bioy = [s for s in spreads if s.yb_etf == "BIOY"]
    assert cwy and bioy
    assert all(s.is_front for s in cwy)
    assert all(s.is_front for s in bioy)


def test_load_yieldboost_sleeve_symbols_from_spreads_sleeves_only():
    spreads = Path(__file__).resolve().parents[1] / "data" / "yieldboost_put_spreads_latest.json"
    if not spreads.exists():
        pytest.skip("spreads file missing")
    sleeves = load_yieldboost_sleeve_symbols_from_spreads(spreads, front_only=True)
    assert "MSTU" in sleeves
    assert "MTYY" not in sleeves
    assert "MSTR" not in sleeves
    assert len(sleeves) <= 32


def test_load_yieldboost_underlying_symbols_from_spreads():
    spreads = Path(__file__).resolve().parents[1] / "data" / "yieldboost_put_spreads_latest.json"
    if not spreads.exists():
        pytest.skip("spreads file missing")
    underlyings = load_yieldboost_underlying_symbols_from_spreads(spreads, front_only=True)
    assert "MSTR" in underlyings
    assert "MSTU" not in underlyings
    assert len(underlyings) <= 32


def test_held_strike_band_includes_itm_puts():
    """MTYY front spread: strikes 7.03/7.42 vs spot ~6.66 must stay in band."""
    lo, hi = held_strike_band([7.03, 7.42], spot_value=6.66)
    assert lo <= 7.03
    assert hi >= 7.42


def test_load_yieldboost_target_strikes_by_sleeve():
    target = Path(__file__).resolve().parents[1] / "data" / "yieldboost_options_target.json"
    if not target.exists():
        pytest.skip("yieldboost_options_target.json not present")
    by_sleeve = load_yieldboost_target_strikes_by_sleeve(target)
    assert "MSTU" in by_sleeve
    assert len(by_sleeve["MSTU"]) >= 2


def test_load_sleeve_by_yb_from_screener_unique_underlyings():
    screener = Path(__file__).resolve().parents[1] / "data" / "etf_screened_today.csv"
    if not screener.exists():
        pytest.skip("screener csv not present")
    mapping = load_sleeve_by_yb_from_screener(screener)
    assert mapping.get("HMYY") == "HIMZ"
    assert mapping.get("YSPY") == "SPXL"
    assert "AMYY" not in mapping  # AMD has multiple letfs; root picks AMDL


def test_format_occ_ticker_amzz():
    occ = format_occ_ticker("AMZZ", date(2026, 5, 22), "P", 34.86)
    assert occ == "AMZZ260522P00034860"


def test_normalize_occ_symbol_strips_prefix_and_pads_strike():
    assert normalize_occ_symbol("O:AMZZ260522P34860") == "AMZZ260522P00034860"
    assert normalize_occ_symbol("MSTU260526P6160") == "MSTU260526P00006160"


def test_resolve_occ_ticker_prefers_chain_symbol():
    chain = [{
        "ticker": "MSTU260526P00006150",
        "expiration_date": "2026-05-26",
        "contract_type": "put",
        "strike_price": 6.16,
        "iv": 1.2,
    }]
    occ = resolve_occ_ticker_for_contract("MSTU", date(2026, 5, 26), 6.16, "P", chain)
    assert occ == "MSTU260526P00006150"


def test_build_occ_symbol_index_matches_padded_variants():
    pending = [{"occ": "MSTU260526P00006160", "sleeve": "MSTU"}]
    idx = build_occ_symbol_index(pending)
    assert idx["MSTU260526P00006160"]["sleeve"] == "MSTU"
    assert idx["MSTU260526P6160"]["sleeve"] == "MSTU"


def test_held_contract_needs_occ_quote_when_expiry_missing():
    rows = [
        {
            "expiration_date": "2026-06-18",
            "contract_type": "put",
            "strike_price": 35.0,
            "iv": 0.5,
            "mid": 1.0,
        },
    ]
    assert held_contract_needs_occ_quote(
        rows, expiry=date(2026, 5, 22), strike=34.86, put_call="P",
    )


def test_held_contract_needs_occ_quote_false_when_iv_present():
    rows = [
        {
            "expiration_date": "2026-05-22",
            "contract_type": "put",
            "strike_price": 34.86,
            "iv": 0.82,
            "mid": 0.15,
        },
    ]
    assert not held_contract_needs_occ_quote(
        rows, expiry=date(2026, 5, 22), strike=34.86, put_call="P",
    )


def test_load_yieldboost_held_expiries_by_sleeve():
    target = Path(__file__).resolve().parents[1] / "data" / "yieldboost_options_target.json"
    if not target.exists():
        pytest.skip("target file missing")
    by_sleeve = load_yieldboost_held_expiries_by_sleeve(target, front_only=True)
    assert "AMZZ" in by_sleeve
    assert len(by_sleeve["AMZZ"]) >= 1


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


def test_infer_etf_ticker_from_legacy_csv_without_column():
    legacy = pd.DataFrame([{
        "as_of_date": "2026-05-21",
        "position_ticker": "MSTU260522P00007030",
        "security_type": "OPTION_PUT",
        "shares": 670.0,
        "option_root": "2MSTU",
        "option_expiry": "2026-05-22",
        "option_strike": 7.03,
        "option_put_call": "P",
        "option_side": "long",
        "source_url": "https://www.graniteshares.com/media/vhfpp1fl/mtyy_holdings_file_20260520.xls",
    }])
    norm = normalize_holdings_dataframe(legacy)
    assert not norm.empty
    assert norm.iloc[0]["etf_ticker"] == "MTYY"
    assert infer_etf_ticker_from_source_url(legacy.iloc[0]["source_url"]) == "MTYY"


def test_normalize_holdings_missing_etf_ticker_returns_empty():
    legacy = pd.DataFrame([{
        "as_of_date": "2026-04-21",
        "position_ticker": "USD",
        "security_type": "CASH",
        "shares": 1.0,
    }])
    assert normalize_holdings_dataframe(legacy).empty
    legacy = pd.DataFrame([{
        "as_of_date": "2026-04-21",
        "position_ticker": "USD",
        "security_type": "CASH",
        "shares": 1.0,
    }])
    assert normalize_holdings_dataframe(legacy).empty
    assert pair_put_spreads_from_holdings(legacy) == []


def test_spreads_json_to_put_spread_legs_repairs_legacy_sleeve():
    payload = {
        "spreads": [{
            "yb_etf": "HMYY",
            "underlying": "HIMS",
            "option_root": "2HIMZ",
            "sleeve_2x_etf": "HIMS",
            "expiry": "2026-05-22",
            "strike_long": 25.88,
            "strike_short": 27.32,
            "qty": 30.0,
            "holdings_as_of": "2026-05-21",
            "is_front": True,
        }],
    }
    legs = spreads_json_to_put_spread_legs(payload)
    assert len(legs) == 1
    assert legs[0].sleeve_2x_etf == "HIMZ"


def test_spreads_json_to_put_spread_legs_roundtrip():
    rows = granite_xls_rows_to_holdings(
        _cwy_fixture_df(),
        etf_ticker="CWY",
        fallback_as_of=date(2026, 5, 19),
        underlying="CRWV",
        source_url="https://example.com/cwy.xls",
    )
    spreads = pair_put_spreads_from_holdings(
        pd.DataFrame(rows), underlying_by_etf={"CWY": "CRWV"}, as_of=date(2026, 5, 20),
    )
    payload = {
        "spreads": [{
            "yb_etf": spreads[0].yb_etf,
            "sleeve_2x_etf": spreads[0].sleeve_2x_etf,
            "underlying": spreads[0].underlying,
            "option_root": spreads[0].option_root,
            "expiry": spreads[0].expiry.isoformat(),
            "strike_long": spreads[0].strike_long,
            "strike_short": spreads[0].strike_short,
            "qty": spreads[0].qty,
            "holdings_as_of": spreads[0].holdings_as_of.isoformat(),
            "is_front": True,
        }],
    }
    legs = spreads_json_to_put_spread_legs(payload)
    assert len(legs) == 1
    assert legs[0].strike_long == pytest.approx(27.89)

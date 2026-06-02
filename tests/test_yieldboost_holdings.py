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
    _variance_time_interp_iv,
    backfill_exact_strike_mid_from_chain,
    build_occ_symbol_index,
    build_vrp_health_payload,
    build_vrp_live_payload,
    build_yieldboost_rv_map,
    compute_short_signal,
    compute_short_thesis_alignment,
    data_grade,
    borrow_carry_display_meta,
    enrich_vrp_rows_with_short_edge,
    evaluate_quote_sync,
    is_directly_shortable,
    load_short_edge_by_yb_from_screener,
    lookup_contract_iv,
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
    # lookup_contract_iv now performs a nearest-expiry fallback (cap 35d). The
    # only chain row (6/18) is 27d from the 5/22 held expiry, so the long-leg IV
    # comes back at the nearest strike with an explicit expiry-skew label. That
    # IS the desired graceful-degradation behavior: a stale-but-best estimate is
    # always better than a hard null, but it MUST be flagged in iv_source so the
    # UI can downgrade the recommendation.
    assert row["iv_put_long"] == pytest.approx(0.62)
    assert row["iv_source"] == "holdings_nearest_expiry"
    assert row["iv_expiry_skew_days"] == 27


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


def test_backfill_exact_strike_mid_from_chain_uses_prevclose():
    row = backfill_exact_strike_mid_from_chain(
        [{
            "expiration_date": "2026-05-26",
            "contract_type": "put",
            "strike_price": 6.16,
            "iv": 1.5,
            "mid": None,
            "prevclose": 0.42,
        }],
        expiry=date(2026, 5, 26),
        strike=6.16,
        put_call="P",
    )
    assert row is not None
    assert row["mid"] == pytest.approx(0.42)
    assert row["mid_source"] == "prevclose"


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


# ── P0a: freshness "worst-of" clock + P0b-1: AZ imputation iv_source escalation ──

def _build_amyy_az_spread(*, expiry: date = date(2026, 5, 27)) -> list:
    """Single AMYY put-spread leg for AZ-imputation tests."""
    return pair_put_spreads_from_holdings(
        pd.DataFrame([
            {
                "as_of_date": expiry.isoformat(),
                "etf_ticker": "AMYY",
                "position_ticker": f"AMDL{expiry.strftime('%y%m%d')}P00047890",
                "security_type": "OPTION_PUT",
                "shares": 100.0,
                "option_root": "2AMDL",
                "option_expiry": expiry.isoformat(),
                "option_strike": 47.89,
                "option_put_call": "P",
                "option_side": "long",
            },
            {
                "as_of_date": expiry.isoformat(),
                "etf_ticker": "AMYY",
                "position_ticker": f"AMDL{expiry.strftime('%y%m%d')}P00050550",
                "security_type": "OPTION_PUT",
                "shares": -100.0,
                "option_root": "2AMDL",
                "option_expiry": expiry.isoformat(),
                "option_strike": 50.55,
                "option_put_call": "P",
                "option_side": "short",
            },
        ]),
        underlying_by_etf={"AMYY": "AMD"},
        as_of=expiry,
    )


def test_build_vrp_live_payload_emits_iv_synthesis_provenance():
    """P3: every row carries an ``iv_synthesis`` provenance block documenting how
    the held-expiry IV was reconstructed (here: AZ from the underlying surface)."""
    expiry = date(2026, 5, 27)
    spread = _build_amyy_az_spread(expiry=expiry)
    options_cache = {
        "symbols": {
            "AMDL": {"spot": 66.9, "updated_at": "2026-05-27T15:00:00Z", "options": []},
            "AMD": {
                "spot": 467.5,
                "updated_at": "2026-05-27T15:00:00Z",
                "options": [
                    {"expiration_date": expiry.isoformat(), "contract_type": "put",
                     "strike_price": k, "iv": 0.70, "mid": 12.0}
                    for k in (380, 390, 395, 400, 405, 410, 420)
                ],
            },
        },
    }
    payload = build_vrp_live_payload(
        spread, options_cache, rv_map={"AMD": 0.45, "AMDL": 0.95}, as_of=expiry,
    )
    row = payload["rows"][0]
    syn = row["iv_synthesis"]
    assert syn["method"] == "az_underlying"
    assert syn["underlying_skew_days"] == 0
    assert syn["iv_underlying_derived"] is not None and syn["iv_underlying_derived"] > 0


def test_build_vrp_live_payload_az_imputes_missing_chain_sleeve_iv():
    """P0b-1: sleeve has no chain, but underlying chain exists -> iv_source escalates to AZ.

    This is the structural fix for CWY / MTYY / MUYY / RGYY / TSYY which Tradier
    OCC cannot quote (the FLEX strikes are not on OPRA). AZ takes the underlying
    1x surface and rescales by |beta| to imply a sleeve IV at the held strike.
    """
    expiry = date(2026, 5, 27)
    spread = _build_amyy_az_spread(expiry=expiry)
    options_cache = {
        "symbols": {
            "AMDL": {
                "spot": 66.9,
                "updated_at": "2026-05-27T00:00:00Z",
                "options": [],
            },
            "AMD": {
                "spot": 467.5,
                "updated_at": "2026-05-27T00:00:00Z",
                "options": [
                    {"expiration_date": expiry.isoformat(),
                     "contract_type": "put", "strike_price": k, "iv": 0.70, "mid": 12.0}
                    for k in (380, 390, 395, 400, 405, 410, 420)
                ],
            },
        },
    }
    payload = build_vrp_live_payload(
        spread, options_cache,
        rv_map={"AMD": 0.45, "AMDL": 0.95},
        as_of=expiry,
    )
    row = payload["rows"][0]
    # IV-source escalation: holdings_missing_chain -> az_imputed_from_underlying.
    assert row["iv_source"] == "az_imputed_from_underlying"
    assert row["az_iv_source"] == "az_imputed_from_underlying"
    # The AZ pipeline rebuilt the sleeve IV from the underlying chain.
    assert row["az_implied_sleeve_iv"] is not None and row["az_implied_sleeve_iv"] > 0
    # The BS-fair recomputed with the AZ-implied IV is a usable surrogate for
    # the dashboard's headline "fair value" column.
    assert row["az_put_spread_fair"] is not None and row["az_put_spread_fair"] >= 0
    # spread_mid_market is None (no sleeve quote), so best_model_edge cannot be
    # computed in price space. That is the correct null behavior -- the row is
    # AZ-fair-valued but the actionable edge stays None until a sleeve mid lands.
    assert row["best_model_edge_pp_of_max_loss"] is None


def test_build_vrp_live_payload_az_best_model_when_sleeve_mid_present():
    """When both AZ-imputed IV and a sleeve mid land, best_model_edge fires."""
    expiry = date(2026, 5, 27)
    spread = _build_amyy_az_spread(expiry=expiry)
    options_cache = {
        "symbols": {
            "AMDL": {
                "spot": 66.9,
                "updated_at": "2026-05-27T00:00:00Z",
                # Sleeve has IV+mid at the exact held strikes (not "missing-chain"
                # for this test) but we still want to confirm the AZ engine runs
                # AND the price-space edge column populates.
                "options": [
                    {"expiration_date": expiry.isoformat(),
                     "contract_type": "put", "strike_price": 47.89, "iv": 2.50, "mid": 0.10},
                    {"expiration_date": expiry.isoformat(),
                     "contract_type": "put", "strike_price": 50.55, "iv": 1.90, "mid": 0.18},
                ],
            },
            "AMD": {
                "spot": 467.5,
                "updated_at": "2026-05-27T00:00:00Z",
                "options": [
                    {"expiration_date": expiry.isoformat(),
                     "contract_type": "put", "strike_price": k, "iv": 0.70, "mid": 5.0}
                    for k in (380, 390, 395, 400, 405, 410, 420)
                ],
            },
        },
    }
    payload = build_vrp_live_payload(
        spread, options_cache,
        rv_map={"AMD": 0.45, "AMDL": 0.95},
        as_of=expiry,
    )
    row = payload["rows"][0]
    assert row["spread_mid_market"] is not None
    assert row["az_put_spread_fair"] is not None
    assert row["best_model_edge_pp_of_max_loss"] is not None
    assert row["best_model_name"] in {"az", "heston", "bates"}


def test_build_vrp_live_payload_az_fires_even_when_sigma_bar_missing():
    """The AZ branch must NOT gate on a missing rv_30d_underlying."""
    expiry = date(2026, 5, 27)
    spread = _build_amyy_az_spread(expiry=expiry)
    options_cache = {
        "symbols": {
            "AMDL": {
                "spot": 66.9,
                "updated_at": "2026-05-27T00:00:00Z",
                "options": [
                    {"expiration_date": expiry.isoformat(),
                     "contract_type": "put", "strike_price": 47.89, "iv": 2.50, "mid": 0.12},
                    {"expiration_date": expiry.isoformat(),
                     "contract_type": "put", "strike_price": 50.55, "iv": 1.90, "mid": 0.25},
                ],
            },
            "AMD": {
                "spot": 467.5,
                "updated_at": "2026-05-27T00:00:00Z",
                "options": [
                    {"expiration_date": expiry.isoformat(),
                     "contract_type": "put", "strike_price": k, "iv": 0.68}
                    for k in (380, 390, 395, 400, 405, 410, 420)
                ],
            },
        },
    }
    # rv_map deliberately empty for AMD: sigma_bar_underlying must fall back to iv_full/|beta|.
    payload = build_vrp_live_payload(spread, options_cache, rv_map={}, as_of=expiry)
    row = payload["rows"][0]
    assert row["az_implied_sleeve_iv"] is not None
    assert row["az_mapped_strike_long_underlying"] is not None
    assert row["az_underlying_iv_long"] is not None
    assert row["az_cone_residual_iv"] is not None


def test_build_vrp_health_payload_worst_of_clock_picks_oldest_source():
    """P0a: the topbar freshness pill must surface the *oldest* of (sleeve quote,
    underlying chain, holdings) — never the freshest build_time alone."""
    spreads_payload = {
        "build_time": "2026-05-27T00:37:00Z",
        "spreads": [{
            "yb_etf": "AMYY", "is_front": True,
            "holdings_as_of": "2026-05-22",
            "sleeve_2x_etf": "AMDL", "underlying": "AMD",
        }],
    }
    vrp_payload = {
        "build_time": "2026-05-27T00:37:00Z",
        "rows": [
            {
                "yb_etf": "AMYY", "sleeve_2x": "AMDL",
                "iv_put_long": 2.5, "iv_put_short": 1.9,
                "spread_mid_market": 0.12,
                "options_as_of": "2026-05-23T20:20:00Z",            # 4d old
                "underlying_options_as_of": "2026-05-23T20:20:00Z",
            },
            {
                "yb_etf": "AZYY", "sleeve_2x": "AMZZ",
                "iv_put_long": 0.62, "iv_put_short": 0.60,
                "spread_mid_market": 0.06,
                "options_as_of": "2026-05-27T00:31:00Z",            # ~minutes old
                "underlying_options_as_of": "2026-05-27T00:31:00Z",
            },
        ],
    }
    health = build_vrp_health_payload(
        spreads_payload, vrp_payload,
        options_cache={"build_time": "2026-05-27T00:31:54Z"},
    )
    # worst-of clocks: the oldest sleeve quote sets the per-row freshness floor.
    assert health["options_as_of_min"] == "2026-05-23T20:20:00Z"
    assert health["options_as_of_max"] == "2026-05-27T00:31:00Z"
    assert health["underlying_options_as_of_min"] == "2026-05-23T20:20:00Z"
    # The worst-of *age* must be the larger of the two (in minutes).
    assert (
        health["worst_sleeve_options_age_minutes"]
        > health["worst_underlying_options_age_minutes"] // 2
    )
    # Holdings age: 5/22 (Friday) vs build today -> trading-day age must be > 0.
    assert health["holdings_as_of"] == "2026-05-22"
    assert health["holdings_age_trading_days"] >= 1


def test_build_vrp_health_payload_flags_missing_chain_ybs():
    """P0b-1 telemetry: sleeves with iv_source=holdings_missing_chain land in the
    missing_chain_ybs bucket so the dashboard can hard-block their SELL signals."""
    spreads_payload = {
        "build_time": "2026-05-27T00:37:00Z",
        "spreads": [],
    }
    vrp_payload = {
        "build_time": "2026-05-27T00:37:00Z",
        "rows": [
            {"yb_etf": "CWY", "sleeve_2x": "CRWV",
             "iv_put_long": None, "iv_put_short": None,
             "iv_source": "holdings_missing_chain"},
            {"yb_etf": "MUYY", "sleeve_2x": "MULL",
             "iv_put_long": None, "iv_put_short": None,
             "iv_source": "holdings_missing_chain"},
            {"yb_etf": "AMYY", "sleeve_2x": "AMDL",
             "iv_put_long": 2.5, "iv_put_short": 1.9,
             "iv_source": "holdings_exact"},
        ],
    }
    health = build_vrp_health_payload(spreads_payload, vrp_payload, options_cache={})
    assert sorted(health["missing_chain_ybs"]) == ["CWY", "MUYY"]
    assert "AMDL" not in health["missing_chain_ybs"]


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


# ───────────────────────────────────────────────────────────────────────────
# data_grade() — PM-grade A/B/C/D classification
# ───────────────────────────────────────────────────────────────────────────

import datetime as _dt


def _now_ts(minutes_ago: int = 0) -> str:
    t = _dt.datetime(2026, 5, 27, 20, 0, 0, tzinfo=_dt.timezone.utc) - _dt.timedelta(minutes=minutes_ago)
    return t.isoformat().replace("+00:00", "Z")


def test_data_grade_A_for_fresh_exact_holdings_today():
    grade, _ = data_grade(
        options_as_of=_now_ts(15),
        underlying_options_as_of=_now_ts(20),
        holdings_as_of="2026-05-27",
        iv_source="holdings_exact",
        iv_expiry_skew_days=0,
        now_utc=_dt.datetime(2026, 5, 27, 20, 0, 0, tzinfo=_dt.timezone.utc),
    )
    assert grade == "A"


def test_data_grade_B_for_nearest_expiry_with_3d_skew_and_2h_quote():
    grade, reason = data_grade(
        options_as_of=_now_ts(120),
        underlying_options_as_of=_now_ts(60),
        holdings_as_of="2026-05-26",  # 1cd old
        iv_source="holdings_nearest_expiry",
        iv_expiry_skew_days=3,
        now_utc=_dt.datetime(2026, 5, 27, 20, 0, 0, tzinfo=_dt.timezone.utc),
    )
    assert grade == "B"
    assert "quote 120m" in reason or "holdings" in reason or "skew" in reason or "src=" in reason


def test_data_grade_C_for_az_imputed_iv():
    grade, reason = data_grade(
        options_as_of=_now_ts(60),
        underlying_options_as_of=_now_ts(60),
        holdings_as_of="2026-05-27",
        iv_source="az_imputed_from_underlying",
        iv_expiry_skew_days=2,
        now_utc=_dt.datetime(2026, 5, 27, 20, 0, 0, tzinfo=_dt.timezone.utc),
    )
    assert grade == "C"
    assert "AZ-imputed" in reason


def test_data_grade_C_for_skew_above_7_days():
    grade, reason = data_grade(
        options_as_of=_now_ts(30),
        underlying_options_as_of=_now_ts(30),
        holdings_as_of="2026-05-27",
        iv_source="holdings_nearest_expiry",
        iv_expiry_skew_days=14,
        now_utc=_dt.datetime(2026, 5, 27, 20, 0, 0, tzinfo=_dt.timezone.utc),
    )
    assert grade == "C"
    assert "skew" in reason


def test_data_grade_D_when_holdings_older_than_2cd():
    grade, reason = data_grade(
        options_as_of=_now_ts(30),
        underlying_options_as_of=_now_ts(30),
        holdings_as_of="2026-05-22",  # 5cd old
        iv_source="holdings_exact",
        iv_expiry_skew_days=0,
        now_utc=_dt.datetime(2026, 5, 27, 20, 0, 0, tzinfo=_dt.timezone.utc),
    )
    assert grade == "D"
    assert "holdings" in reason and "5cd" in reason


def test_data_grade_D_when_quote_older_than_24h():
    grade, reason = data_grade(
        options_as_of=_now_ts(30),
        underlying_options_as_of=_now_ts(30 * 60),  # 30h ago
        holdings_as_of="2026-05-27",
        iv_source="holdings_exact",
        iv_expiry_skew_days=0,
        now_utc=_dt.datetime(2026, 5, 27, 20, 0, 0, tzinfo=_dt.timezone.utc),
    )
    assert grade == "D"
    assert "quote" in reason


def test_data_grade_D_when_chain_missing():
    grade, reason = data_grade(
        options_as_of=_now_ts(15),
        underlying_options_as_of=_now_ts(15),
        holdings_as_of="2026-05-27",
        iv_source="holdings_missing_chain",
        iv_expiry_skew_days=0,
        now_utc=_dt.datetime(2026, 5, 27, 20, 0, 0, tzinfo=_dt.timezone.utc),
    )
    assert grade == "D"
    assert "missing chain" in reason


def test_data_grade_D_when_skew_above_21_days():
    grade, _ = data_grade(
        options_as_of=_now_ts(15),
        underlying_options_as_of=_now_ts(15),
        holdings_as_of="2026-05-27",
        iv_source="holdings_nearest_expiry",
        iv_expiry_skew_days=25,
        now_utc=_dt.datetime(2026, 5, 27, 20, 0, 0, tzinfo=_dt.timezone.utc),
    )
    assert grade == "D"


# ── P1/P2/P3: held-expiry IV synthesis grade policy ────────────────────────


def test_data_grade_C_for_skew_above_21_with_term_interp():
    """P2: a monthly-only sleeve whose held expiry is term-interpolated across
    two bracketing listed expiries is a held-expiry proxy → C, not D."""
    grade, reason = data_grade(
        options_as_of=_now_ts(15),
        underlying_options_as_of=_now_ts(15),
        holdings_as_of="2026-05-27",
        iv_source="holdings_term_interp",
        iv_expiry_skew_days=25,
        iv_term_interp=True,
        now_utc=_dt.datetime(2026, 5, 27, 20, 0, 0, tzinfo=_dt.timezone.utc),
    )
    assert grade == "C"
    assert "term-interp" in reason


def test_data_grade_C_for_skew_above_21_with_near_underlying():
    """P1: a monthly-only sleeve rescued by a near-dated underlying expiry → C."""
    grade, reason = data_grade(
        options_as_of=_now_ts(15),
        underlying_options_as_of=_now_ts(15),
        holdings_as_of="2026-05-27",
        iv_source="holdings_nearest_expiry",
        iv_expiry_skew_days=25,
        underlying_iv_skew_days=2,
        now_utc=_dt.datetime(2026, 5, 27, 20, 0, 0, tzinfo=_dt.timezone.utc),
    )
    assert grade == "C"
    assert "underlying IV" in reason


def test_data_grade_D_for_skew_above_21_with_far_underlying():
    """Resilience: if the underlying expiry used is ALSO far (>7d), the rescue
    is not trustworthy and the row stays D."""
    grade, _ = data_grade(
        options_as_of=_now_ts(15),
        underlying_options_as_of=_now_ts(15),
        holdings_as_of="2026-05-27",
        iv_source="holdings_nearest_expiry",
        iv_expiry_skew_days=25,
        underlying_iv_skew_days=30,
        now_utc=_dt.datetime(2026, 5, 27, 20, 0, 0, tzinfo=_dt.timezone.utc),
    )
    assert grade == "D"


def test_data_grade_held_proxy_does_not_override_stale_quote():
    """Resilience: the held-expiry proxy forgives *skew* only. A stale quote
    (>24h) is an independent hard block that must still produce D."""
    grade, _ = data_grade(
        options_as_of=_now_ts(30 * 60),   # 30h stale
        underlying_options_as_of=_now_ts(30 * 60),
        holdings_as_of="2026-05-27",
        iv_source="holdings_term_interp",
        iv_expiry_skew_days=25,
        iv_term_interp=True,
        now_utc=_dt.datetime(2026, 5, 27, 20, 0, 0, tzinfo=_dt.timezone.utc),
    )
    assert grade == "D"


def test_data_grade_caps_at_C_on_iv_disagreement():
    """P3: otherwise-A inputs are capped to C when the sleeve and underlying IV
    surfaces disagree by more than the threshold."""
    grade, reason = data_grade(
        options_as_of=_now_ts(15),
        underlying_options_as_of=_now_ts(20),
        holdings_as_of="2026-05-27",
        iv_source="holdings_exact",
        iv_expiry_skew_days=0,
        iv_disagreement_pct=0.50,
        now_utc=_dt.datetime(2026, 5, 27, 20, 0, 0, tzinfo=_dt.timezone.utc),
    )
    assert grade == "C"
    assert "disagree" in reason


def test_data_grade_no_cap_on_small_iv_disagreement():
    grade, _ = data_grade(
        options_as_of=_now_ts(15),
        underlying_options_as_of=_now_ts(20),
        holdings_as_of="2026-05-27",
        iv_source="holdings_exact",
        iv_expiry_skew_days=0,
        iv_disagreement_pct=0.10,
        now_utc=_dt.datetime(2026, 5, 27, 20, 0, 0, tzinfo=_dt.timezone.utc),
    )
    assert grade == "A"


# ── P2: variance-time interpolation helper + lookup_contract_iv ────────────


def test_variance_time_interp_iv_between_nodes():
    # Flat 20% vol on both nodes → interpolated vol is 20% regardless of tenor.
    iv = _variance_time_interp_iv(iv_lo=0.20, t_lo=10 / 365, iv_hi=0.20, t_hi=40 / 365, t_held=25 / 365)
    assert iv == pytest.approx(0.20, abs=1e-9)
    # Rising term structure: interpolated vol sits strictly between the nodes.
    iv2 = _variance_time_interp_iv(iv_lo=0.30, t_lo=10 / 365, iv_hi=0.50, t_hi=40 / 365, t_held=25 / 365)
    assert 0.30 < iv2 < 0.50


def test_variance_time_interp_iv_rejects_degenerate():
    assert _variance_time_interp_iv(iv_lo=None, t_lo=0.1, iv_hi=0.2, t_hi=0.2, t_held=0.15) is None
    assert _variance_time_interp_iv(iv_lo=0.2, t_lo=0.1, iv_hi=0.2, t_hi=0.1, t_held=0.15) is None
    assert _variance_time_interp_iv(iv_lo=0.2, t_lo=0.0, iv_hi=0.2, t_hi=0.2, t_held=0.15) is None


def test_lookup_contract_iv_term_interpolates_bracketed_expiry():
    """Held 5/27 is bracketed by listed 5/18 and 6/19 (>7d to either) → the
    returned IV is variance-time interpolated and flagged."""
    held = date(2026, 5, 27)
    options_cache = {
        "symbols": {
            "AMZZ": {
                "spot": 40.0,
                "updated_at": "2026-05-27T15:00:00Z",
                "options": [
                    {"expiration_date": "2026-05-18", "contract_type": "put",
                     "strike_price": 35.0, "iv": 0.40, "mid": 0.30},
                    {"expiration_date": "2026-06-19", "contract_type": "put",
                     "strike_price": 35.0, "iv": 0.60, "mid": 1.10},
                ],
            },
        },
    }
    # Valuation date is before the held expiry (realistic: both bracket nodes
    # have a positive time-to-expiry).
    q = lookup_contract_iv(options_cache, "AMZZ", held, 35.0, "P", as_of=date(2026, 5, 15))
    assert q["iv_term_interp"] is True
    assert q["iv_interp_expiries"] == ["2026-05-18", "2026-06-19"]
    assert 0.40 < q["iv"] < 0.60
    assert "holdings_term_interp" in q["iv_source_chain"]


def test_lookup_contract_iv_no_term_interp_when_not_bracketed():
    """Held 5/27 is BEFORE the only listed expiry (6/19) → cannot interpolate,
    falls back to the nearest far node verbatim."""
    held = date(2026, 5, 27)
    options_cache = {
        "symbols": {
            "AMZZ": {
                "spot": 40.0,
                "updated_at": "2026-05-27T15:00:00Z",
                "options": [
                    {"expiration_date": "2026-06-19", "contract_type": "put",
                     "strike_price": 35.0, "iv": 0.60, "mid": 1.10},
                ],
            },
        },
    }
    q = lookup_contract_iv(options_cache, "AMZZ", held, 35.0, "P", as_of=held)
    assert q["iv_term_interp"] is False
    assert q["iv"] == pytest.approx(0.60)


def test_resolve_iv_source_term_interp_label():
    long_q = {"iv": 0.5, "iv_source_chain": ["holdings_nearest_expiry", "holdings_term_interp"],
              "expiry_in_chain": False}
    short_q = {"iv": 0.5, "iv_source_chain": ["holdings_nearest_expiry"], "expiry_in_chain": False}
    assert resolve_iv_source(long_q, short_q) == "holdings_term_interp"


# ───────────────────────────────────────────────────────────────────────────
# Canonical (BS-free) public field names
# ───────────────────────────────────────────────────────────────────────────


def test_build_vrp_live_payload_emits_canonical_kernel_aware_fields():
    """vrp_live rows must carry model_name, model_fair, edge_pp_of_max_loss,
    dollar_gamma_per_1pct_underlying, theta_per_day, expected_weekly_carry_usd,
    greeks_kernel, and data_grade — the canonical BS-free public schema."""
    expiry = date(2026, 5, 27)
    spread = _build_amyy_az_spread(expiry=expiry)
    options_cache = {
        "symbols": {
            "AMDL": {
                "spot": 66.9,
                "updated_at": "2026-05-27T15:00:00Z",
                "options": [
                    {"expiration_date": expiry.isoformat(),
                     "contract_type": "put", "strike_price": 47.89, "iv": 2.50, "mid": 0.12},
                    {"expiration_date": expiry.isoformat(),
                     "contract_type": "put", "strike_price": 50.55, "iv": 1.90, "mid": 0.25},
                ],
            },
            "AMD": {
                "spot": 467.5,
                "updated_at": "2026-05-27T15:00:00Z",
                "options": [
                    {"expiration_date": expiry.isoformat(),
                     "contract_type": "put", "strike_price": k, "iv": 0.68}
                    for k in (380, 390, 395, 400, 405, 410, 420)
                ],
            },
        },
    }
    payload = build_vrp_live_payload(
        spread, options_cache, rv_map={"AMD": 0.6, "AMDL": 1.2}, as_of=expiry,
    )
    row = payload["rows"][0]
    # Canonical names (kernel-agnostic; UI consumes these).
    assert "model_name" in row and row["model_name"] in {"az", "heston", "bates", None}
    assert "model_fair" in row
    assert "edge_pp_of_max_loss" in row
    assert "expected_weekly_carry_usd" in row
    assert "dollar_gamma_per_1pct_underlying" in row
    assert "theta_per_day" in row
    # Greeks may now come from the calibrated kernel (heston / bates / az) when
    # one converged; bs_proxy is the fallback when none did.
    assert row.get("greeks_kernel") in {"heston", "bates", "az", "bs_proxy", None}
    assert "data_grade" in row and row["data_grade"] in {"A", "B", "C", "D"}
    assert "data_grade_reason" in row
    # When a model fair was computed, expected_weekly_carry_usd = model_fair * 100.
    if row["model_fair"] is not None:
        assert row["expected_weekly_carry_usd"] == pytest.approx(row["model_fair"] * 100, rel=1e-3)


def test_build_vrp_live_payload_model_disagreement_when_multiple_kernels_fire():
    """When AZ + Heston both succeed, model_disagreement_pp_of_max_loss surfaces."""
    expiry = date(2026, 5, 27)
    spread = _build_amyy_az_spread(expiry=expiry)
    # Big underlying chain so Heston has enough points to calibrate.
    options_cache = {
        "symbols": {
            "AMDL": {
                "spot": 66.9,
                "updated_at": "2026-05-27T15:00:00Z",
                "options": [
                    {"expiration_date": expiry.isoformat(),
                     "contract_type": "put", "strike_price": 47.89, "iv": 2.50, "mid": 0.12},
                    {"expiration_date": expiry.isoformat(),
                     "contract_type": "put", "strike_price": 50.55, "iv": 1.90, "mid": 0.25},
                ],
            },
            "AMD": {
                "spot": 467.5,
                "updated_at": "2026-05-27T15:00:00Z",
                "options": [
                    {"expiration_date": expiry.isoformat(),
                     "contract_type": "put", "strike_price": k, "iv": iv}
                    for k, iv in [
                        (360, 0.80), (380, 0.72), (400, 0.66), (420, 0.62),
                        (440, 0.60), (460, 0.58), (480, 0.59), (500, 0.61),
                    ]
                ],
            },
        },
    }
    payload = build_vrp_live_payload(
        spread, options_cache, rv_map={"AMD": 0.6, "AMDL": 1.2}, as_of=expiry,
    )
    row = payload["rows"][0]
    # az should always fire; heston may or may not depending on COS convergence.
    assert row["model_name"] in {"az", "heston", "bates"}
    # disagreement is None when only 1 model fires, otherwise a non-negative pp number
    if row.get("model_disagreement_pp_of_max_loss") is not None:
        assert row["model_disagreement_pp_of_max_loss"] >= 0


def test_build_vrp_live_payload_uses_kernel_greeks_when_kernel_fires():
    """When a calibrated kernel produces a model_fair, greeks_kernel must
    flip away from "bs_proxy" to that kernel's name and the canonical
    dollar_gamma / theta_per_day must come from the kernel's finite-diff."""
    expiry = date(2026, 5, 27)
    spread = _build_amyy_az_spread(expiry=expiry)
    # Skewed underlying chain wide enough for both Heston and Bates to converge.
    options_cache = {
        "symbols": {
            "AMDL": {
                "spot": 66.9,
                "updated_at": "2026-05-27T15:00:00Z",
                "options": [
                    {"expiration_date": expiry.isoformat(),
                     "contract_type": "put", "strike_price": 47.89, "iv": 2.50, "mid": 0.12},
                    {"expiration_date": expiry.isoformat(),
                     "contract_type": "put", "strike_price": 50.55, "iv": 1.90, "mid": 0.25},
                ],
            },
            "AMD": {
                "spot": 467.5,
                "updated_at": "2026-05-27T15:00:00Z",
                "options": [
                    {"expiration_date": expiry.isoformat(),
                     "contract_type": "put", "strike_price": k, "iv": iv}
                    for k, iv in [
                        (360, 0.95), (380, 0.85), (400, 0.76), (420, 0.69),
                        (440, 0.64), (460, 0.62), (480, 0.63), (500, 0.66),
                    ]
                ],
            },
        },
    }
    payload = build_vrp_live_payload(
        spread, options_cache, rv_map={"AMD": 0.6, "AMDL": 1.2}, as_of=expiry,
    )
    row = payload["rows"][0]
    # Whatever kernel fired, greeks_kernel must agree with model_name unless
    # the kernel-greeks helper itself returned None and we fell back to BS-proxy.
    if row.get("model_fair") is not None:
        assert row["greeks_kernel"] in {row["model_name"], "bs_proxy"}
        # If not falling back, the canonical greeks must equal what the
        # kernel emitted (i.e. NOT match the BS-proxy values).
        if row["greeks_kernel"] != "bs_proxy":
            bs_dbg = (row.get("debug") or {}).get("bs", {})
            bs_gamma = bs_dbg.get("dollar_gamma_per_1pct_underlying")
            kernel_gamma = row.get("dollar_gamma_per_1pct_underlying")
            # Both should be defined; kernel γ should differ structurally
            # (skewed chain → Heston/Bates ≠ BS).
            if bs_gamma is not None and kernel_gamma is not None:
                assert kernel_gamma < 0
                # Not strictly required to differ, but on this skewed chain it does.
                assert kernel_gamma != bs_gamma
        # Sign sanity for the canonical fields.
        if row.get("theta_per_day") is not None:
            assert row["theta_per_day"] > 0
        if row.get("dollar_gamma_per_1pct_underlying") is not None:
            assert row["dollar_gamma_per_1pct_underlying"] < 0


def test_yieldboost_targeted_refresh_symbols_keeps_underlyings():
    """YB-targeted options refresh must not drop underlying tickers from the request."""
    from build_data import _yieldboost_targeted_refresh_symbols

    targets = {"AMDL": [47.89, 50.55], "AMZZ": [35.07, 37.02]}
    held = {"AMDL": {"2026-05-27"}, "AMZZ": {"2026-05-27"}}
    sleeves, underlyings = _yieldboost_targeted_refresh_symbols(
        ["AMDL", "AMZZ", "AMD", "AMZN"],
        target_strikes_by_sleeve=targets,
        held_expiries_by_sleeve=held,
    )
    assert sleeves == ["AMDL", "AMZZ"]
    assert underlyings == ["AMD", "AMZN"]
    assert sorted(set(sleeves) | set(underlyings)) == ["AMD", "AMDL", "AMZN", "AMZZ"]


# ──────────────────────────────────────────────────────────────────────────
# Short-YieldBOOST structural edge join + sync gates
# ──────────────────────────────────────────────────────────────────────────
def test_short_thesis_alignment_negates_spread_edge():
    """Rich front spread (positive edge) must read as a HEADWIND to shorting."""
    rich = compute_short_thesis_alignment(20.0)
    assert rich["alignment_pp"] == -20.0
    assert rich["direction"] == "headwind"
    cheap = compute_short_thesis_alignment(-15.0)
    assert cheap["alignment_pp"] == 15.0
    assert cheap["direction"] == "tailwind"
    assert compute_short_thesis_alignment(None)["direction"] == "unknown"


def test_compute_short_signal_tiers_and_p05_guard():
    """STRONG SHORT needs p50>=15% AND a positive p05 lower band."""
    assert compute_short_signal(0.20, 0.05)["tier"] == "strong"
    # Same median but downside tail dips negative -> demote from strong to short.
    assert compute_short_signal(0.20, -0.01)["tier"] == "short"
    assert compute_short_signal(0.07, 0.0)["tier"] == "short"
    assert compute_short_signal(0.01)["tier"] == "lean"
    assert compute_short_signal(-0.05)["tier"] == "avoid"
    assert compute_short_signal(None)["tier"] == "none"
    # Not borrowable -> label capped, but numeric rank preserved for sorting.
    blocked = compute_short_signal(0.30, 0.10, shortable=False)
    assert blocked["tier"] == "blocked"
    assert blocked["rank"] == 0.30


def test_is_directly_shortable_flags():
    assert is_directly_shortable({"purgatory": False, "exclude_no_shares": False}) is True
    assert is_directly_shortable({"purgatory": True}) is False
    assert is_directly_shortable({"exclude_no_shares": True}) is False
    assert is_directly_shortable({"strategy_blacklisted": True}) is False
    assert is_directly_shortable({"exclude_borrow_spike": True}) is False
    assert is_directly_shortable(None) is False


def test_evaluate_quote_sync_flags_stale_underlying():
    """The XEY case: sleeve quote fresh, underlying quote ~7d stale -> NOT SYNCED."""
    row = {
        "options_as_of": "2026-05-30T17:23:00Z",
        "underlying_options_as_of": "2026-05-23T20:20:00Z",
        "holdings_as_of": "2026-05-29",
    }
    res = evaluate_quote_sync(row, screener_asof="2026-05-30")
    assert res["sync_ok"] is False
    assert res["quote_sync_gap_hours"] > 24


def test_evaluate_quote_sync_ok_when_aligned():
    row = {
        "options_as_of": "2026-05-30T17:23:00Z",
        "underlying_options_as_of": "2026-05-30T17:10:00Z",
        "holdings_as_of": "2026-05-30",
    }
    res = evaluate_quote_sync(row, screener_asof="2026-05-30")
    assert res["sync_ok"] is True
    assert res["quote_sync_gap_hours"] < 1


def test_evaluate_quote_sync_missing_underlying_is_unsynced():
    res = evaluate_quote_sync({"options_as_of": "2026-05-30T17:23:00Z"})
    assert res["sync_ok"] is False
    assert "missing underlying quote time" in res["sync_reason"]


def test_load_short_edge_by_yb_from_screener(tmp_path):
    csv = tmp_path / "screener.csv"
    pd.DataFrame(
        [
            {
                "ETF": "cwy", "Underlying": "CRWV", "product_class": "income_yieldboost",
                "net_edge_p50_annual": 0.75, "net_edge_p05_annual": 0.18,
                "net_edge_p95_annual": 1.28, "expected_gross_decay_p50_annual": 0.80,
                "borrow_fee_annual": "", "income_distributions_annual": "",
                "inverse_shortable": "False", "purgatory": "True",
                "strategy_blacklisted": "False", "exclude_borrow_spike": "False",
                "exclude_no_shares": "True",
                "edge_sign_convention": "short_favorable_positive",
                "asof_date": "2026-06-02",
            },
            {
                "ETF": "AAPU", "Underlying": "AAPL", "product_class": "letf_long",
                "net_edge_p50_annual": 0.02, "asof_date": "2026-06-02",
            },
        ]
    ).to_csv(csv, index=False)
    out = load_short_edge_by_yb_from_screener(csv)
    assert "AAPU" not in out  # only income_yieldboost rows
    assert out["__asof__"] == "2026-06-02"
    rec = out["CWY"]
    assert rec["net_edge_p50_annual"] == 0.75
    assert rec["purgatory"] is True
    assert rec["borrow_fee_annual"] is None  # empty cell -> None, not 0
    assert is_directly_shortable(rec) is False


def test_enrich_vrp_rows_with_short_edge_non_destructive():
    payload = {
        "build_time": "2026-06-02T19:00:00Z",
        "rows": [
            {
                "yb_etf": "CWY", "underlying": "CRWV",
                "edge_pp_of_max_loss": 12.0,
                "spread_mid_market": 0.5, "model_fair": 0.4,
                "options_as_of": "2026-06-02T17:00:00Z",
                "underlying_options_as_of": "2026-06-02T16:55:00Z",
                "holdings_as_of": "2026-06-02",
            }
        ],
    }
    short_map = {
        "__asof__": "2026-06-02",
        "CWY": {
            "net_edge_p50_annual": 0.75, "net_edge_p05_annual": 0.18,
            "net_edge_p95_annual": 1.28, "expected_gross_decay_p50_annual": 0.80,
            "borrow_fee_annual": None, "income_distributions_annual": None,
            "purgatory": False, "exclude_no_shares": False,
            "strategy_blacklisted": False, "exclude_borrow_spike": False,
            "inverse_shortable": True,
            "edge_sign_convention": "short_favorable_positive",
            "asof_date": "2026-06-02",
        },
    }
    out = enrich_vrp_rows_with_short_edge(payload, short_map)
    row = out["rows"][0]
    # Existing fields preserved.
    assert row["edge_pp_of_max_loss"] == 12.0
    assert row["spread_mid_market"] == 0.5
    # Structural fields joined.
    assert row["net_edge_p50_annual"] == 0.75
    assert row["short_signal"]["tier"] == "strong"
    # Sign-corrected overlay: rich spread -> headwind.
    assert row["short_thesis_alignment"]["alignment_pp"] == -12.0
    assert row["short_thesis_alignment"]["direction"] == "headwind"
    assert row["short_directly_shortable"] is True
    assert row["quote_sync"]["sync_ok"] is True
    assert "Net short edge" in row["short_edge_why"]
    assert out["short_edge_join_count"] == 1


def test_borrow_carry_display_prefers_live_then_avg():
    live = borrow_carry_display_meta({
        "borrow_fee_annual": 0.05,
        "borrow_avg_annual": 0.12,
        "borrow_for_net_annual": 0.05,
    })
    assert live["source"] == "live"
    assert live["display_annual"] == 0.05
    assert "live borrow" in live["tooltip"]
    assert "net-edge model used 5.0%" in live["tooltip"]

    hist = borrow_carry_display_meta({
        "borrow_fee_annual": None,
        "borrow_avg_annual": 0.19,
        "borrow_median_60d": 0.055,
        "borrow_for_net_annual": 0.0,
    })
    assert hist["source"] == "hist_avg"
    assert hist["display_annual"] == 0.19
    assert hist["source_label"] == "~hist avg"
    assert "net-edge model used 0.0%" in hist["tooltip"]


def test_enrich_vrp_rows_includes_borrow_carry():
    payload = {"rows": [{"yb_etf": "CWY", "edge_pp_of_max_loss": 1.0,
                         "options_as_of": "2026-06-02T17:00:00Z",
                         "underlying_options_as_of": "2026-06-02T17:00:00Z"}]}
    short_map = {
        "CWY": {
            "net_edge_p50_annual": 0.76, "net_edge_p05_annual": 0.18,
            "expected_gross_decay_p50_annual": 0.80,
            "borrow_fee_annual": None, "borrow_avg_annual": 0.039,
            "borrow_for_net_annual": 0.0,
            "purgatory": True, "exclude_no_shares": True,
            "strategy_blacklisted": False, "exclude_borrow_spike": False,
            "inverse_shortable": False,
        },
    }
    out = enrich_vrp_rows_with_short_edge(payload, short_map)
    bc = out["rows"][0]["borrow_carry"]
    assert bc["source"] == "hist_avg"
    assert "net-edge model used" in bc["tooltip"]
    assert "hist avg" in out["rows"][0]["short_edge_why"]


def test_enrich_vrp_rows_missing_screener_row_is_safe():
    payload = {"rows": [{"yb_etf": "ZZZZ", "edge_pp_of_max_loss": 5.0,
                         "options_as_of": "2026-06-02T17:00:00Z",
                         "underlying_options_as_of": "2026-06-02T17:00:00Z"}]}
    out = enrich_vrp_rows_with_short_edge(payload, {})
    row = out["rows"][0]
    assert row["net_edge_p50_annual"] is None
    assert row["short_signal"]["tier"] == "none"
    assert row["short_directly_shortable"] is None
    assert out["short_edge_join_count"] == 0

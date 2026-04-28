"""Unit tests for the multi-model NAV forecaster."""
from __future__ import annotations

import math
import sys
from datetime import datetime, timezone
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import forecast_nav as fn  # noqa: E402


# ---------------------------------------------------------------------------
# Pure arithmetic
# ---------------------------------------------------------------------------

def test_compute_v1_letf_2x_no_ter():
    nav = fn.compute_v1(nav_anchor=10.0, beta=2.0, und_ret=0.01, ter_daily=0.0)
    assert abs(nav - 10.0 * math.exp(0.02)) < 1e-9


def test_compute_v1_inverse_3x_with_ter():
    nav = fn.compute_v1(nav_anchor=20.0, beta=-3.0, und_ret=0.01, ter_daily=4e-5)
    assert abs(nav - 20.0 * math.exp(-0.03) * (1 - 4e-5)) < 1e-9


def test_v2_zero_drag_at_beta_one_and_zero():
    """Vol-drag coefficient (beta^2 - beta)/2 vanishes at beta in {0, 1}."""
    base = fn.compute_v1(10.0, 1.0, 0.0, 0.0)
    v2 = fn.compute_v2_ito(10.0, 1.0, 0.0, sigma_annual=1.0, dt_years=1.0, ter_daily=0.0)
    assert abs(v2 - base) < 1e-9
    base0 = fn.compute_v1(10.0, 0.0, 0.0, 0.0)
    v2_0 = fn.compute_v2_ito(10.0, 0.0, 0.0, sigma_annual=1.0, dt_years=1.0, ter_daily=0.0)
    assert abs(v2_0 - base0) < 1e-9


def test_v2_drag_pulls_letf_down_inverse_down():
    """For |beta|>1, vol drag should pull both LETFs and inverse ETFs lower."""
    sigma, dt = 0.5, 1.0 / 252.0
    # 2x LETF, no underlying move:
    v1 = fn.compute_v1(10.0, 2.0, 0.0, 0.0)
    v2 = fn.compute_v2_ito(10.0, 2.0, 0.0, sigma, dt, 0.0)
    assert v2 < v1
    # -3x inverse:
    v1_inv = fn.compute_v1(10.0, -3.0, 0.0, 0.0)
    v2_inv = fn.compute_v2_ito(10.0, -3.0, 0.0, sigma, dt, 0.0)
    assert v2_inv < v1_inv


def test_v2_drag_matches_closed_form():
    sigma, dt, beta = 0.5, 1.0 / 252.0, 2.0
    v1 = fn.compute_v1(10.0, beta, 0.01, 0.0)
    v2 = fn.compute_v2_ito(10.0, beta, 0.01, sigma, dt, 0.0)
    expected_drag = math.exp(-((beta ** 2 - beta) / 2.0) * sigma ** 2 * dt)
    assert abs(v2 - v1 * expected_drag) < 1e-9


# ---------------------------------------------------------------------------
# OCC parsing + option lookup
# ---------------------------------------------------------------------------

def test_parse_occ_standard_call():
    out = fn.parse_occ("AAPL260619C00250000")
    assert out == {"root": "AAPL", "expiry": "2026-06-19", "strike": 250.0, "type": "call"}


def test_parse_occ_with_space_and_prefix():
    out = fn.parse_occ("2AI 260515P00009010")
    assert out == {"root": "AI", "expiry": "2026-05-15", "strike": 9.010, "type": "put"}


def test_parse_occ_returns_none_on_garbage():
    assert fn.parse_occ("not-an-option") is None
    assert fn.parse_occ(None) is None
    assert fn.parse_occ("") is None


def test_lookup_option_mark_finds_match():
    cache = {
        "symbols": {
            "AI": {
                "spot": 10.0,
                "options": [
                    {"expiration_date": "2026-05-15", "contract_type": "put",
                     "strike_price": 9.01, "mid": 0.7164, "iv": 0.85, "delta": -0.4,
                     "ticker": "AI260515P00009010"},
                    {"expiration_date": "2026-05-15", "contract_type": "call",
                     "strike_price": 9.01, "mid": 0.5151, "iv": 0.85, "delta": 0.55,
                     "ticker": "AI260515C00009010"},
                ],
            }
        }
    }
    parsed = fn.parse_occ("2AI 260515P00009010")
    mid, meta = fn.lookup_option_mark(parsed, cache)
    assert mid == 0.7164
    assert meta["matched"] is True
    assert meta["iv"] == 0.85


def test_lookup_option_mark_misses_unknown_strike():
    cache = {"symbols": {"AI": {"options": [
        {"expiration_date": "2026-05-15", "contract_type": "put",
         "strike_price": 9.01, "mid": 0.7164},
    ]}}}
    parsed = fn.parse_occ("AI 260515P00010000")  # different strike
    mid, meta = fn.lookup_option_mark(parsed, cache)
    assert mid is None
    assert meta["matched"] is False


# ---------------------------------------------------------------------------
# mark_holdings
# ---------------------------------------------------------------------------

def _options_cache(spots: dict, chains: dict | None = None) -> dict:
    syms: dict[str, dict] = {}
    for k, v in spots.items():
        syms[k.upper()] = {"spot": v, "cache_age_seconds": 30}
    for k, opts in (chains or {}).items():
        syms.setdefault(k.upper(), {}).setdefault("spot", spots.get(k))
        syms[k.upper()]["options"] = opts
    return {"symbols": syms}


def test_mark_holdings_letf_swap_only_delta_scales_with_underlying():
    """delta_mv should equal sum(shares * (spot_now - spot_anchor)) for swap legs."""
    legs = [
        {"security_type": "SWAP", "position_ticker": None,
         "shares": 100.0, "price": 100.0, "market_value": 10_000.0},
        {"security_type": "SWAP", "position_ticker": None,
         "shares": 50.0, "price": 100.0, "market_value": 5_000.0},
        {"security_type": "CASH", "position_ticker": None,
         "shares": 1000.0, "price": 1.0, "market_value": 1_000.0},
    ]
    out = fn.mark_holdings(
        legs, fallback_underlying="AAPL",
        options_cache=_options_cache({"AAPL": 110.0}),
        price_options=False,
    )
    # 150 shares * (110 - 100) = 1,500 ; cash leg contributes 0.
    assert abs(out["delta_mv"] - 1_500.0) < 1e-6
    assert out["equity_legs_priced"] == 2
    assert out["equity_legs_total"] == 2
    assert out["legs_total"] == 3
    # Cash legs count as priced (their delta is known to be zero).
    assert out["legs_priced"] == 3


def test_mark_holdings_zero_delta_when_spot_unchanged():
    legs = [
        {"security_type": "SWAP", "position_ticker": None,
         "shares": 1000.0, "price": 267.61, "market_value": 267_610.0},
    ]
    out = fn.mark_holdings(
        legs, fallback_underlying="AAPL",
        options_cache=_options_cache({"AAPL": 267.61}),
        price_options=False,
    )
    assert abs(out["delta_mv"]) < 1e-6


def test_mark_holdings_equity_unpriced_when_spot_missing():
    legs = [
        {"security_type": "SWAP", "position_ticker": None,
         "shares": 100.0, "price": 100.0, "market_value": 10_000.0},
        {"security_type": "CASH", "shares": 1000.0, "price": 1.0, "market_value": 1_000.0},
    ]
    out = fn.mark_holdings(
        legs, fallback_underlying="AAPL",
        options_cache={"symbols": {}}, price_options=False,
    )
    assert out["delta_mv"] == 0.0
    assert out["equity_legs_priced"] == 0


def test_mark_holdings_options_zero_delta_in_v3_mode():
    """v3 (price_options=False) leaves option legs at zero delta."""
    legs = [
        {"security_type": "OPTION_PUT", "position_ticker": "AI 260515P00009010",
         "shares": -28200.0, "market_value": -2_020_248.0},
        {"security_type": "CASH", "shares": 1000.0, "market_value": 1_000.0},
    ]
    out = fn.mark_holdings(
        legs, fallback_underlying="AI",
        options_cache=_options_cache({}, {"AI": [
            {"expiration_date": "2026-05-15", "contract_type": "put",
             "strike_price": 9.01, "mid": 1.50},
        ]}),
        price_options=False,
    )
    assert out["option_legs_total"] == 1
    assert out["option_legs_priced"] == 0
    assert out["delta_mv"] == 0.0


def test_mark_holdings_options_repriced_in_yieldboost_mode():
    # mid_anchor implied by anchor MV: -2,020,248 / (-28200 * 100) = 0.7164
    # mid_now = 0.50 -> delta per option = -28200 * (0.50 - 0.7164) * 100 = +610,248
    legs = [
        {"security_type": "OPTION_PUT", "position_ticker": "AI 260515P00009010",
         "shares": -28200.0, "market_value": -2_020_248.0},
        {"security_type": "CASH", "shares": 1000.0, "market_value": 1_000.0},
    ]
    out = fn.mark_holdings(
        legs, fallback_underlying="AI",
        options_cache=_options_cache({}, {"AI": [
            {"expiration_date": "2026-05-15", "contract_type": "put",
             "strike_price": 9.01, "mid": 0.50, "iv": 0.85},
        ]}),
        price_options=True,
    )
    expected = -28200.0 * (0.50 - 0.7164) * 100.0
    assert abs(out["delta_mv"] - expected) < 1.0
    assert out["option_legs_priced"] == 1


# ---------------------------------------------------------------------------
# build_forecasts_for_symbol + dispatcher
# ---------------------------------------------------------------------------

def _ts():
    return datetime(2026, 4, 28, 13, 35, tzinfo=timezone.utc)


def _anchor(nav=10.0, und=100.0, asof="2026-04-25", shares=1_000_000.0):
    return {
        "nav_close": nav, "und_close": und, "as_of_date": asof,
        "shares_outstanding": shares,
    }


def test_build_forecasts_letf_emits_v1_v2_v3_when_inputs_present():
    rec = {
        "symbol": "TSLL", "underlying": "TSLA", "beta": 2.0, "product_class": "letf",
        "forecast_vol_underlying_annual": 0.6,
    }
    options = _options_cache({"TSLA": 102.0, "TSLL": 10.45})
    # Anchor: nav=10, shares=1M -> AUM=10M. 2x leverage -> ~200k AAPL shares
    # exposure (200k * 100 = 20M = 2x AUM). Use a single SWAP leg with the
    # right size so v3's nav_hat lands close to v1's.
    holdings = [
        {"security_type": "SWAP", "position_ticker": "TSLA",
         "shares": 200_000.0, "price": 100.0, "market_value": 20_000_000.0},
        {"security_type": "CASH", "shares": 5_000_000.0,
         "price": 1.0, "market_value": 5_000_000.0},
    ]
    rows, default = fn.build_forecasts_for_symbol(rec, _anchor(), options, holdings, _ts())
    by_model = {r.model: r for r in rows}
    assert {"delta_v1", "delta_v2_ito", "delta_v3_swap_mark", "yieldboost_putspread_v1"} <= set(by_model)
    # v1: 10 * exp(2 * log(102/100)) ~ 10.40
    assert by_model["delta_v1"].confidence == "high"
    assert 10.39 < by_model["delta_v1"].nav_hat < 10.41
    # v2 includes vol drag, so very slightly below v1 for an LETF.
    assert by_model["delta_v2_ito"].nav_hat is not None
    assert by_model["delta_v2_ito"].nav_hat < by_model["delta_v1"].nav_hat
    # v3: nav_anchor + delta_mv / shares = 10 + 200000*(102-100)/1e6 = 10.40
    v3 = by_model["delta_v3_swap_mark"]
    assert v3.confidence == "high"
    assert 10.39 < v3.nav_hat < 10.41
    # Dispatcher prefers v3 since its sanity ratio is ~1.04 (well within bounds).
    assert default == "delta_v3_swap_mark"
    assert v3.is_default is True


def test_build_forecasts_yieldboost_routes_to_yb_when_options_priced():
    rec = {
        "symbol": "AIYY", "underlying": "AI", "beta": 0.5,
        "product_class": "income_yieldboost", "is_yieldboost": True,
        "forecast_vol_underlying_annual": 0.85,
    }
    options = _options_cache(
        {"AI": 9.5, "AIYY": 12.0},
        # Tiny option mid move so the put-spread delta stays within sanity.
        {"AI": [
            {"expiration_date": "2026-05-15", "contract_type": "put",
             "strike_price": 9.01, "mid": 0.7100, "iv": 0.85},
        ]},
    )
    holdings = [
        {"security_type": "TREASURY", "position_ticker": None,
         "shares": 100_000.0, "price": 100.0, "market_value": 10_000_000.0},
        # mid_anchor implied = -7,000,000 / (-100000 * 100) = 0.70
        # mid_now = 0.71  -> delta = -100000 * 0.01 * 100 = -100,000
        # nav_hat = 12 + (-100,000)/1e6 = 11.90  (well within sanity envelope)
        {"security_type": "OPTION_PUT", "position_ticker": "2AI 260515P00009010",
         "shares": -100_000.0, "price": 0.0, "market_value": -7_000_000.0},
    ]
    anchor = _anchor(nav=12.0, und=9.5, shares=1_000_000.0)
    rows, default = fn.build_forecasts_for_symbol(rec, anchor, options, holdings, _ts())
    by_model = {r.model: r for r in rows}
    # Beta-only models should be na for yieldboost (income product class).
    assert by_model["delta_v1"].confidence == "na"
    assert by_model["delta_v2_ito"].confidence == "na"
    yb = by_model["yieldboost_putspread_v1"]
    assert yb.confidence in ("high", "medium")
    assert yb.option_legs_priced == 1
    assert 11.85 < yb.nav_hat < 11.95
    assert default == "yieldboost_putspread_v1"


def test_build_forecasts_falls_through_to_v2_when_holdings_missing():
    rec = {
        "symbol": "TSLL", "underlying": "TSLA", "beta": 2.0, "product_class": "letf",
        "forecast_vol_underlying_annual": 0.6,
    }
    options = _options_cache({"TSLA": 100.0, "TSLL": 10.0})
    rows, default = fn.build_forecasts_for_symbol(
        rec, _anchor(), options, holdings_legs=None, ts_utc=_ts(),
    )
    by_model = {r.model: r for r in rows}
    assert by_model["delta_v3_swap_mark"].confidence == "na"
    assert by_model["yieldboost_putspread_v1"].confidence == "na"
    assert default == "delta_v2_ito"


def test_build_forecasts_yieldboost_with_no_holdings_is_na():
    rec = {
        "symbol": "AIYY", "underlying": "AI", "beta": 0.5,
        "product_class": "income_yieldboost", "is_yieldboost": True,
    }
    options = _options_cache({"AI": 9.5})
    rows, default = fn.build_forecasts_for_symbol(rec, _anchor(), options, None, _ts())
    by_model = {r.model: r for r in rows}
    assert by_model["yieldboost_putspread_v1"].confidence == "na"
    assert by_model["delta_v3_swap_mark"].confidence == "na"
    assert default is None


def _record(model: str, nav_hat: float, *, conf="high", nav_anchor=10.0):
    return fn.ForecastRecord(
        ts="t", symbol="X", model=model, is_default=False, confidence=conf,
        product_class="letf", und_symbol="U", und_spot_t=1.0, und_spot_anchor=1.0,
        und_anchor_date="2026-04-25", und_spot_age_sec=10.0, beta=2.0, ter_daily=0.0,
        nav_anchor=nav_anchor, nav_anchor_date="2026-04-25", nav_hat=nav_hat,
        etf_last=None, etf_last_ts=None, premium_bp=None, notes=None,
    )


def test_select_default_model_sanity_envelope_rejects_blowup():
    """v3 outside [0.5, 2.0] x anchor -> dispatcher falls through to v1."""
    sane = _record("delta_v1", 10.5)
    blown = _record("delta_v3_swap_mark", 999.0)
    assert fn.select_default_model([sane, blown], product_class="letf", is_yieldboost=False) == "delta_v1"


def test_select_default_model_returns_none_when_all_violate():
    """If every candidate fails sanity, return None instead of a known-wrong NAV."""
    blown_v1 = _record("delta_v1", 999.0)
    blown_v3 = _record("delta_v3_swap_mark", 0.001)
    assert fn.select_default_model([blown_v1, blown_v3], product_class="letf", is_yieldboost=False) is None


def test_dt_years_floor_one_trading_day():
    ts = datetime(2026, 4, 25, 14, 0, tzinfo=timezone.utc)
    assert fn._dt_years("2026-04-25", ts) >= 1.0 / 252.0 - 1e-12


def test_dt_years_overnight_is_one_business_day():
    ts = datetime(2026, 4, 28, 14, 0, tzinfo=timezone.utc)
    # 2026-04-27 (Mon) to 2026-04-28 (Tue) = 1 business day.
    dt = fn._dt_years("2026-04-27", ts)
    assert abs(dt - 1.0 / 252.0) < 1e-9

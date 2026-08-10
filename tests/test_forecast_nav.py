"""Unit tests for the multi-model NAV forecaster."""
from __future__ import annotations

import math
import sys
from datetime import date, datetime, timezone
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
    assert default == "yieldboost_putspread_v2"  # v2 outranks v1 since 2026-08-07


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
        und_anchor_date="2026-04-25", und_spot_age_sec=10.0, delta=2.0, ter_daily=0.0,
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


def _stale_options_entry(spot: float) -> dict:
    return {
        "spot": spot,
        "cache_age_seconds": 9_999_999.0,
        "stale": True,
        "source": "cache",
        "updated_at": "2026-04-08T00:00:00Z",
    }


def test_options_quote_unreliable():
    assert fn._options_quote_unreliable(None, 10.0) is False
    assert fn._options_quote_unreliable({"stale": True}, 10.0) is True
    assert fn._options_quote_unreliable({"stale": False}, float(fn.SPOT_FRESH_SECONDS) + 1.0) is True
    assert fn._options_quote_unreliable({"stale": False}, 60.0) is False


def test_metrics_row_covers_anchor_iso_compare():
    assert fn._metrics_row_covers_anchor({"date": "2026-05-08"}, "2026-05-07") is True
    assert fn._metrics_row_covers_anchor({"date": "2026-05-06"}, "2026-05-07") is False
    assert fn._metrics_row_covers_anchor({"date": "2026-05-07"}, "2026-05-07") is True
    assert fn._metrics_row_covers_anchor(None, "2026-05-07") is False


def test_build_forecasts_stale_underlying_uses_metrics_fallback():
    """Zombie EOSE spot must not halve delta_v1 when metrics has the underlier."""
    rec = {
        "symbol": "EOSU", "underlying": "EOSE", "beta": 2.0, "product_class": "letf",
        "forecast_vol_underlying_annual": 0.6,
    }
    anchor = _anchor(nav=35.32, und=6.36, asof="2026-05-07")
    options = {
        "symbols": {
            "EOSE": _stale_options_entry(4.505),
            "EOSU": {"spot": 33.5, "cache_age_seconds": 30.0, "stale": False},
        }
    }
    metrics = {
        "date": "2026-05-08",
        "underlying_adj_close": 6.36,
        "close_price": 33.52,
    }
    rows, _ = fn.build_forecasts_for_symbol(
        rec,
        anchor,
        options,
        None,
        _ts(),
        None,
        metrics,
    )
    v1 = next(r for r in rows if r.model == "delta_v1")
    assert v1.confidence != "na"
    assert v1.und_spot_t is not None
    assert abs(float(v1.und_spot_t) - 6.36) < 1e-3
    assert v1.nav_hat is not None
    assert 35.0 < v1.nav_hat < 35.5
    assert v1.notes and "underlying_spot_via_etf_metrics" in v1.notes


def test_build_forecasts_stale_underlying_no_usable_metrics_is_na():
    rec = {
        "symbol": "EOSU", "underlying": "EOSE", "beta": 2.0, "product_class": "letf",
        "forecast_vol_underlying_annual": 0.6,
    }
    anchor = _anchor(nav=35.32, und=6.36, asof="2026-05-07")
    options = {"symbols": {"EOSE": _stale_options_entry(4.505)}}
    metrics = {"date": "2026-05-06", "underlying_adj_close": 6.50}
    rows, _ = fn.build_forecasts_for_symbol(
        rec,
        anchor,
        options,
        None,
        _ts(),
        None,
        metrics,
    )
    v1 = next(r for r in rows if r.model == "delta_v1")
    assert v1.confidence == "na"


def test_metrics_fallback_underlying_same_session_uses_anchor():
    """Same ``date`` as anchor: never substitute a conflicting metrics underlier."""
    fb = fn._metrics_fallback_underlying_price(
        {"date": "2026-05-08", "underlying_adj_close": 6.36},
        "2026-05-08",
        8.01,
    )
    assert fb == 8.01


def test_build_forecasts_stale_underlying_same_day_prefers_anchor_und():
    rec = {
        "symbol": "EOSU", "underlying": "EOSE", "beta": 2.0, "product_class": "letf",
        "forecast_vol_underlying_annual": 0.6,
    }
    anchor = _anchor(nav=33.52, und=8.01, asof="2026-05-08")
    options = {
        "symbols": {
            "EOSE": _stale_options_entry(4.505),
            "EOSU": {"spot": 33.5, "cache_age_seconds": 30.0, "stale": False},
        }
    }
    metrics = {
        "date": "2026-05-08",
        "underlying_adj_close": 6.36,
        "close_price": 33.52,
    }
    rows, _ = fn.build_forecasts_for_symbol(
        rec,
        anchor,
        options,
        None,
        _ts(),
        None,
        metrics,
    )
    v1 = next(r for r in rows if r.model == "delta_v1")
    assert v1.und_spot_t is not None
    assert abs(float(v1.und_spot_t) - 8.01) < 1e-6
    assert v1.nav_hat is not None
    assert 33.0 < v1.nav_hat < 34.0


# ---------------------------------------------------------------------------
# yieldboost_putspread_v2: BS marks for FLEX legs no vendor chain quotes
# ---------------------------------------------------------------------------

def _flex_cache(spot=28.0, iv_lo=0.60, iv_hi=0.80):
    """Listed monthly chain (integer strikes) around a FLEX target."""
    return {
        "symbols": {
            "AMZZ": {
                "spot": spot,
                "options": [
                    {"expiration_date": "2026-08-21", "contract_type": "put",
                     "strike_price": 34.0, "iv": iv_lo, "mid": 6.0},
                    {"expiration_date": "2026-08-21", "contract_type": "put",
                     "strike_price": 36.0, "iv": iv_hi, "mid": 8.0},
                ],
            }
        }
    }


_NOW = datetime(2026, 8, 7, 18, 0, tzinfo=timezone.utc)


def test_ring_sigma_is_one_number_for_every_strike_in_the_ring():
    """The spread-cancellation property, pinned.

    A BS mark on an unquotable FLEX leg is only tolerable because a put SPREAD
    carries offsetting vega — which requires BOTH legs to price off the SAME
    sigma. Per-strike interpolation broke that (2026-08-10: XBTY long legs
    drew 1.40/1.61, short legs 3.85, modelled NAV moved -6.2% on a +0.85%
    underlier day).
    """
    cache = _flex_cache()
    ivs = {
        fn.model_option_mark(
            fn.parse_occ(occ), cache, now_utc=_NOW,
        )[1]["iv"]
        for occ in ("AMZZ260812P00034000", "AMZZ260812P00035000",
                    "AMZZ260812P00036000", "AMZZ260812P00040000")
    }
    assert len(ivs) == 1, f"one sigma per ring, got {ivs}"
    assert abs(ivs.pop() - 0.70) < 1e-9, "median of 0.60/0.80"


def test_ring_sigma_prefers_quoted_contracts_over_zero_bid_artifacts():
    """An IV solved off a 0.00 x 0.01 book is a solver artifact, not a view."""
    cache = _flex_cache()
    opts = cache["symbols"]["AMZZ"]["options"]
    for o in opts:
        o["bid"] = 0.0                      # both existing strikes unquoted
    opts.append({"expiration_date": "2026-08-21", "contract_type": "put",
                 "strike_price": 35.0, "iv": 0.55, "mid": 7.0, "bid": 6.9})
    sigma, meta = fn.ring_sigma(cache["symbols"]["AMZZ"], "put",
                                date(2026, 8, 12))
    assert abs(sigma - 0.55) < 1e-9, "only the quoted contract should count"
    assert meta["iv_source"] == "quoted"
    assert meta["iv_ring_used"] == 1


def test_ring_sigma_drops_out_of_band_strikes_entirely():
    """AMDL carried 0.063 and 4.769 on ADJACENT strikes (a 76x step).

    Both are outside the sanity band, so they never reach the median at all.
    """
    cache = _flex_cache()
    cache["symbols"]["AMZZ"]["options"] += [
        {"expiration_date": "2026-08-21", "contract_type": "put",
         "strike_price": 35.0, "iv": 4.769, "mid": 9.0},
        {"expiration_date": "2026-08-21", "contract_type": "put",
         "strike_price": 35.5, "iv": 0.0063, "mid": 0.1},
    ]
    sigma, meta = fn.ring_sigma(cache["symbols"]["AMZZ"], "put",
                                date(2026, 8, 12))
    assert abs(sigma - 0.70) < 1e-9, "median of the two in-band strikes"
    assert meta["iv_ring_used"] == 2 and meta["iv_ring_n"] == 4


def test_ring_sigma_median_resists_an_in_band_outlier():
    """Even inside the band, one high strike must not drag the ring."""
    cache = _flex_cache()
    cache["symbols"]["AMZZ"]["options"].append(
        {"expiration_date": "2026-08-21", "contract_type": "put",
         "strike_price": 35.0, "iv": 2.9, "mid": 9.0}
    )
    sigma, _meta = fn.ring_sigma(cache["symbols"]["AMZZ"], "put",
                                 date(2026, 8, 12))
    assert abs(sigma - 0.80) < 1e-9, "median of 0.60/0.80/2.90, not the 1.43 mean"


def test_ring_sigma_clamps_an_entirely_out_of_band_ring():
    """Every contract an artifact: clamp, don't drop the leg.

    Returning None here would take option coverage to zero and push the whole
    row to ``na`` — a bounded vol still cancels across the spread.
    """
    cache = _flex_cache(iv_lo=6.0, iv_hi=8.0)
    sigma, meta = fn.ring_sigma(cache["symbols"]["AMZZ"], "put",
                                date(2026, 8, 12))
    assert sigma == fn.IV_SANITY_MAX
    assert meta["iv_source"] == "clamped"


def test_model_option_mark_expired_leg_is_intrinsic():
    parsed = fn.parse_occ("AMZZ260807P00036000")   # expires 'today'
    mid, meta = fn.model_option_mark(parsed, _flex_cache(spot=28.0), now_utc=_NOW)
    assert mid is not None
    assert abs(mid - 8.0) < 1e-6, "T=0 put = max(K - S, 0)"


def test_model_option_mark_fails_closed_without_surface():
    cache = {"symbols": {"AMZZ": {"spot": 28.0, "options": []}}}
    parsed = fn.parse_occ("AMZZ260812P00035000")
    mid, meta = fn.model_option_mark(parsed, cache, now_utc=_NOW)
    assert mid is None and meta["reason"] == "no listed IV surface"


def _flex_leg(occ="AMZZ260812P00035000", shares=100.0, mv=50_000.0):
    return {"security_type": "OPTION_PUT", "position_ticker": occ,
            "shares": shares, "market_value": mv, "price": None}


def test_mark_holdings_models_flex_leg_only_when_asked():
    """v1 (model_options=False) must keep its exact old behaviour."""
    legs = [_flex_leg()]
    v1 = fn.mark_holdings(legs, fallback_underlying="AMZN",
                          options_cache=_flex_cache(), price_options=True)
    assert v1["option_legs_priced"] == 0
    assert any(s.startswith("opt-no-quote") for s in v1["skipped"])
    v2 = fn.mark_holdings(legs, fallback_underlying="AMZN",
                          options_cache=_flex_cache(), price_options=True,
                          model_options=True, ts_utc=_NOW)
    assert v2["option_legs_priced"] == 1
    assert v2["option_legs_modeled"] == 1
    assert v2["skipped"] == []


def _yb_inputs(anchor_date="2026-08-06"):
    return {
        "symbol": "AZYY",
        "product_class": "income_yieldboost",
        "shares_outstanding": 200_000.0,
        "nav_anchor": 14.40,
        "nav_anchor_date": anchor_date,
        "und_symbol": "AMZN",
        "und_anchor": None,
        "und_spot_t": None,
        "und_spot_age_sec": None,
        "delta": 0.4,
        "etf_last": 14.43,
        "etf_last_ts": "test",
        "distribution_applied": 0.0,
    }


def test_v2_confidence_caps_at_medium_when_a_leg_is_modeled():
    rec = fn.build_yieldboost_putspread_v2(
        _yb_inputs(), "2026-08-07T18:00:00Z", 4e-5, [_flex_leg()],
        _flex_cache(), ts_utc=_NOW,
    )
    assert rec.nav_hat is not None
    assert rec.confidence == "medium"
    assert "BS-modeled" in rec.notes


def test_stale_anchor_degrades_then_refuses_but_always_emits():
    fresh = fn.build_yieldboost_putspread_v2(
        _yb_inputs("2026-08-06"), "ts", 4e-5, [_flex_leg()], _flex_cache(), ts_utc=_NOW)
    assert fresh.confidence == "medium"          # modeled leg, fresh anchor
    old = fn.build_yieldboost_putspread_v2(
        _yb_inputs("2026-07-28"), "ts", 4e-5, [_flex_leg()], _flex_cache(), ts_utc=_NOW)
    assert old.confidence == "na", "8 bdays stale -> refused"
    assert old.nav_hat is not None, "row still emitted with its level for diagnostics"
    assert "bdays old" in old.notes


def test_dispatcher_prefers_v2_for_yieldboost():
    rows = [
        fn.build_yieldboost_putspread_v1(
            _yb_inputs(), "ts", 4e-5, [_flex_leg()], _flex_cache(), ts_utc=_NOW),
        fn.build_yieldboost_putspread_v2(
            _yb_inputs(), "ts", 4e-5, [_flex_leg()], _flex_cache(), ts_utc=_NOW),
    ]
    picked = fn.select_default_model(rows, "income_yieldboost", True)
    assert picked == "yieldboost_putspread_v2"


# ---------------------------------------------------------------------------
# Distributions: list-shaped file + weekly inference (both were dead)
# ---------------------------------------------------------------------------

def _weekly_events(last="2026-07-31", amounts=(0.102, 0.100, 0.101, 0.094)):
    from datetime import date as _date, timedelta as _td
    d = _date.fromisoformat(last)
    evs = []
    for i, amt in enumerate(reversed(amounts)):
        evs.append({"ex_date": (d - _td(days=7 * i)).isoformat(), "amount": amt})
    return list(reversed(evs))


def test_distribution_loader_reads_bare_list_shape(tmp_path, monkeypatch):
    """The production file's by_symbol values are bare LISTS; the loader only
    handled dict shapes, so the ex-date adjustment never fired even when the
    env gate was on."""
    import json as _json
    p = tmp_path / "dist.json"
    p.write_text(_json.dumps({"by_symbol": {"AZYY": _weekly_events()}}), encoding="utf-8")
    monkeypatch.setattr(fn, "DISTRIBUTIONS_PATH_ENV", str(p))
    out = fn._load_distributions_for_today("2026-07-31")
    assert out == {"AZYY": 0.094}, "recorded event on its ex-date"


def test_weekly_inference_predicts_the_unrecorded_ex_date(tmp_path, monkeypatch):
    """The file is history-only — today's event appears only after it happened.
    A stable weekly cadence one step past the last record infers the median."""
    import json as _json
    p = tmp_path / "dist.json"
    p.write_text(_json.dumps({"by_symbol": {"AZYY": _weekly_events()}}), encoding="utf-8")
    monkeypatch.setattr(fn, "DISTRIBUTIONS_PATH_ENV", str(p))
    out = fn._load_distributions_for_today("2026-08-07")   # last + 7d
    assert out == {"AZYY": 0.1005}, "median of trailing four amounts"
    assert fn._load_distributions_for_today("2026-08-05") == {}, "off-cadence day: nothing"
    assert fn._load_distributions_for_today("2026-08-21") == {}, "two steps out: nothing"


def test_weekly_inference_refuses_irregular_cadence():
    from datetime import date as _date
    evs = _weekly_events()
    evs[1]["ex_date"] = "2026-07-05"   # breaks the 7d rhythm
    assert fn._infer_weekly_ex_amount(evs, _date(2026, 8, 7)) is None


# ---------------------------------------------------------------------------
# T-bill accretion
# ---------------------------------------------------------------------------

def _tbill_leg(price=0.9922, face=800_000.0, as_of="2026-08-01",
               name="US TBill 10/22/2026"):
    return {"security_type": "TREASURY", "security_name": name,
            "price": price, "shares": face, "market_value": face * price,
            "as_of_date": as_of}


def test_tbill_accretes_linearly_toward_par():
    ts = datetime(2026, 8, 7, 18, 0, tzinfo=timezone.utc)   # 6 days after as_of
    delta = fn._tbill_accrual_delta(_tbill_leg(), ts)
    days_to_mat = (datetime(2026, 10, 22).date() - datetime(2026, 8, 1).date()).days
    expect = 800_000.0 * (1.0 - 0.9922) * (6 / days_to_mat)
    assert abs(delta - expect) < 1e-6
    assert delta > 0


def test_tbill_same_day_and_garbage_are_zero_or_none():
    ts = datetime(2026, 8, 1, 18, 0, tzinfo=timezone.utc)
    assert fn._tbill_accrual_delta(_tbill_leg(), ts) == 0.0
    assert fn._tbill_accrual_delta(_tbill_leg(name="US Dollars"), ts) is None
    assert fn._tbill_accrual_delta(_tbill_leg(price=1.2), ts) is None


def test_mark_holdings_counts_tbill_accrual_in_delta():
    ts = datetime(2026, 8, 7, 18, 0, tzinfo=timezone.utc)
    marked = fn.mark_holdings(
        [_tbill_leg()], fallback_underlying="AMZN",
        options_cache={"symbols": {}}, price_options=True,
        model_options=True, ts_utc=ts,
    )
    assert marked["delta_mv"] > 0
    assert marked["legs_priced"] == 1

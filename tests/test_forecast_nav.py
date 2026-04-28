"""Unit tests for the delta_v1 NAV forecaster."""
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

def test_compute_nav_hat_letf_2x_no_ter():
    nav = fn.compute_nav_hat(nav_anchor=10.0, beta=2.0, und_ret=0.01, ter_daily=0.0)
    expected = 10.0 * math.exp(0.02)
    assert abs(nav - expected) < 1e-9


def test_compute_nav_hat_inverse_3x_with_ter():
    nav = fn.compute_nav_hat(nav_anchor=20.0, beta=-3.0, und_ret=0.01, ter_daily=0.00004)
    expected = 20.0 * math.exp(-0.03) * (1.0 - 0.00004)
    assert abs(nav - expected) < 1e-9


def test_ter_daily_default_is_one_pct_over_252():
    assert abs(fn._ter_daily_for("UNKNOWN") - 0.0098 / 252.0) < 1e-12


# ---------------------------------------------------------------------------
# Confidence routing
# ---------------------------------------------------------------------------

def _anchor(nav=10.0, und=100.0, asof="2026-04-25"):
    return {"nav_close": nav, "und_close": und, "as_of_date": asof}


def test_classify_letf_high_when_fresh_spot():
    rec = {"symbol": "TSLL", "underlying": "TSLA", "beta": 2.0, "product_class": "letf"}
    conf, _ = fn._classify_confidence(rec, _anchor(), und_spot_age_sec=60)
    assert conf == "high"


def test_classify_letf_medium_when_spot_stale():
    rec = {"symbol": "TSLL", "underlying": "TSLA", "beta": 2.0, "product_class": "letf"}
    conf, notes = fn._classify_confidence(rec, _anchor(), und_spot_age_sec=4_000)
    assert conf == "medium"
    assert any("stale" in n for n in notes)


def test_classify_inverse_high():
    rec = {"symbol": "SQQQ", "underlying": "QQQ", "beta": -3.0, "product_class": "inverse"}
    assert fn._classify_confidence(rec, _anchor(), 0)[0] == "high"


def test_classify_volatility_etp_medium():
    rec = {"symbol": "UVIX", "underlying": "VIX", "beta": 2.0, "product_class": "volatility_etp"}
    assert fn._classify_confidence(rec, _anchor(), 0)[0] == "medium"


def test_classify_yieldboost_na():
    rec = {
        "symbol": "TSYY", "underlying": "TSLA", "beta": 0.5,
        "product_class": "income_yieldboost", "is_yieldboost": True,
    }
    conf, notes = fn._classify_confidence(rec, _anchor(), 30)
    assert conf == "na"
    assert any("yieldboost" in n for n in notes)


def test_classify_missing_anchor_na():
    rec = {"symbol": "TSLL", "underlying": "TSLA", "beta": 2.0, "product_class": "letf"}
    assert fn._classify_confidence(rec, None, 30)[0] == "na"


def test_classify_missing_beta_na():
    rec = {"symbol": "TSLL", "underlying": "TSLA", "beta": None, "product_class": "letf"}
    assert fn._classify_confidence(rec, _anchor(), 30)[0] == "na"


def test_classify_passive_low_beta_medium():
    rec = {"symbol": "VOO", "underlying": "SPX", "beta": 1.0, "product_class": "passive_low_beta"}
    assert fn._classify_confidence(rec, _anchor(), 0)[0] == "medium"


# ---------------------------------------------------------------------------
# End-to-end build_forecast
# ---------------------------------------------------------------------------

def _ts():
    return datetime(2026, 4, 28, 13, 35, tzinfo=timezone.utc)


def test_build_forecast_letf_round_trip():
    rec = {"symbol": "TSLL", "underlying": "TSLA", "beta": 2.0, "product_class": "letf"}
    anchor = _anchor(nav=10.0, und=100.0)
    options = {
        "symbols": {
            "TSLA": {"spot": 102.0, "cache_age_seconds": 30, "updated_at": "2026-04-28T13:30:00Z"},
            "TSLL": {"spot": 10.45, "cache_age_seconds": 30, "updated_at": "2026-04-28T13:30:00Z"},
        }
    }
    f = fn.build_forecast(rec, anchor, options, _ts())
    assert f is not None
    assert f.symbol == "TSLL"
    assert f.confidence == "high"
    # 2x leverage on +2% underlying => roughly +4% NAV (minus tiny TER)
    assert 10.4 < f.nav_hat < 10.42
    assert f.premium_bp is not None
    # etf_last (10.45) > nav_hat (~10.41) => positive premium
    assert f.premium_bp > 0


def test_build_forecast_inverse_falls_with_underlying():
    rec = {"symbol": "SQQQ", "underlying": "QQQ", "beta": -3.0, "product_class": "inverse"}
    anchor = _anchor(nav=20.0, und=400.0)
    options = {
        "symbols": {
            "QQQ": {"spot": 408.0, "cache_age_seconds": 10},
            "SQQQ": {"spot": 18.7, "cache_age_seconds": 10},
        }
    }
    f = fn.build_forecast(rec, anchor, options, _ts())
    assert f is not None
    assert f.confidence == "high"
    assert f.beta == -3.0
    # QQQ +2% => SQQQ NAV ~ -6%
    assert 18.79 < f.nav_hat < 18.85


def test_build_forecast_yieldboost_returns_record_with_na():
    rec = {
        "symbol": "TSYY", "underlying": "TSLA", "beta": 0.5,
        "product_class": "income_yieldboost", "is_yieldboost": True,
    }
    options = {
        "symbols": {
            "TSLA": {"spot": 102.0, "cache_age_seconds": 5},
        }
    }
    f = fn.build_forecast(rec, _anchor(nav=15.0), options, _ts())
    assert f is not None
    assert f.confidence == "na"
    assert f.nav_hat is None
    assert f.premium_bp is None
    assert "yieldboost" in (f.notes or "")


def test_build_forecast_handles_missing_underlying_spot():
    rec = {"symbol": "TSLL", "underlying": "TSLA", "beta": 2.0, "product_class": "letf"}
    options = {"symbols": {}}  # no TSLA spot
    f = fn.build_forecast(rec, _anchor(), options, _ts())
    assert f is not None
    assert f.confidence == "na"
    assert f.nav_hat is None


def test_build_forecast_premium_bp_sign():
    rec = {"symbol": "TSLL", "underlying": "TSLA", "beta": 2.0, "product_class": "letf"}
    options = {
        "symbols": {
            "TSLA": {"spot": 100.0, "cache_age_seconds": 10},  # zero return
            "TSLL": {"spot": 9.99, "cache_age_seconds": 10},   # ETF below model
        }
    }
    f = fn.build_forecast(rec, _anchor(nav=10.0, und=100.0), options, _ts())
    assert f.confidence == "high"
    # nav_hat ~ 10 * 1.0 * (1-tiny ter) ~ 9.9996; etf_last 9.99 => slightly negative premium
    assert f.premium_bp is not None
    assert f.premium_bp < 0

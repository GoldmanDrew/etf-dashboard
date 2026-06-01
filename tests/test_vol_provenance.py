"""Tests for realized-vol headline provenance fields in build_data."""
from __future__ import annotations

import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from build_data import (  # noqa: E402
    _build_forecast_vol_etf_fields,
    _build_forecast_vol_fields,
    _vol_annual_source,
)


def test_vol_annual_source_yahoo_panel():
    assert _vol_annual_source(True, 0.45, 0.52) == "yahoo_realized_vol"


def test_vol_annual_source_screener_csv():
    assert _vol_annual_source(False, 0.45, 0.45) == "screener_csv"


def test_vol_annual_source_missing_final():
    assert _vol_annual_source(True, 0.45, None) is None
    assert _vol_annual_source(False, None, float("nan")) is None


def test_vol_annual_source_no_csv_fallback():
    assert _vol_annual_source(False, None, 0.40) is None


def test_build_forecast_vol_etf_fields_prefers_robust_6m():
    out = _build_forecast_vol_etf_fields(
        realized_vol={
            "6M": {
                "etf": 0.50,
                "etf_ewma": 0.48,
                "etf_robust_ewma": 0.42,
            }
        },
        vol_etf_csv=0.55,
    )
    assert out["forecast_vol_etf_annual"] == 0.42
    assert out["forecast_vol_etf_source"] == "robust_ewma_6m"
    assert out["forecast_vol_etf_window"] == "6M"


def test_build_forecast_vol_etf_fields_falls_back_to_screener_csv():
    out = _build_forecast_vol_etf_fields(realized_vol={}, vol_etf_csv=0.61)
    assert out["forecast_vol_etf_annual"] == 0.61
    assert out["forecast_vol_etf_source"] == "screener_csv"
    assert out["forecast_vol_etf_window"] is None


def test_build_forecast_vol_fields_blend():
    out = _build_forecast_vol_fields(
        beta=2.0,
        expected_decay=0.10,
        is_yieldboost=False,
        realized_vol={
            "6M": {
                "underlying_robust_ewma": 0.40,
                "underlying_ewma": 0.45,
                "underlying": 0.50,
            }
        },
    )
    assert out["forecast_vol_source"] == "50_50_model_robust_ewma"
    assert out["forecast_vol_underlying_annual"] is not None
    assert out["forecast_vol_underlying_annual"] > 0

"""Tests for FoF forward scenario grid and calibration rollup."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from yieldboost_fof_forward import (  # noqa: E402
    build_fof_income_distribution_calibration,
    build_fof_pair_scenario_grid,
    weighted_child_forecast_vol,
)


def _child_with_grid(sym: str, *, sigma: float, beta: float, p50: float) -> dict:
    return {
        "forecast_vol_underlying_annual": sigma,
        "delta": beta,
        "expected_gross_decay_p50_annual": p50,
        "income_distribution_calibration": {
            "blended_ratio_used": 0.65,
            "fund_ratio_median": 0.60,
            "events_used": 10,
            "events_total": 12,
            "cross_fund_ratio": 0.65,
            "events_recent": [
                {"ex_date": "2026-01-10", "amount": 0.42, "yield_frac": 0.03, "ratio": 0.62, "nav_at_ex": 14.0},
            ],
        },
    }


def test_weighted_child_forecast_vol():
    basket = {
        "children": [
            {"yb_etf": "SMYY", "weight_pct": 60.0},
            {"yb_etf": "IOYY", "weight_pct": 40.0},
        ],
    }
    child_records = {
        "SMYY": {"forecast_vol_underlying_annual": 0.80},
        "IOYY": {"forecast_vol_underlying_annual": 0.60},
    }
    out = weighted_child_forecast_vol(basket, child_records, basket_vol_fallback=0.55)
    assert out["forecast_vol_underlying_annual"] == pytest.approx(0.72, abs=0.001)
    assert out["forecast_vol_source"] == "fof_basket_weighted_child_forecast"


def test_build_fof_income_distribution_calibration():
    basket = {
        "children": [
            {"yb_etf": "SMYY", "weight_pct": 50.0},
            {"yb_etf": "IOYY", "weight_pct": 50.0},
        ],
    }
    child_records = {
        "SMYY": _child_with_grid("SMYY", sigma=0.7, beta=0.5, p50=0.4),
        "IOYY": _child_with_grid("IOYY", sigma=0.6, beta=0.4, p50=0.3),
    }
    out = build_fof_income_distribution_calibration(basket, child_records)
    assert out is not None
    assert out["source"] == "fof_weighted_child_rollup"
    assert out["blended_ratio_used"] == pytest.approx(0.65)
    assert out["events_used"] == 20
    assert len(out["events_recent"]) == 2


def test_build_fof_pair_scenario_grid_shape_and_anchor():
    basket = {
        "children": [{"yb_etf": "SMYY", "weight_pct": 100.0}],
        "cash_pct": 5.0,
    }
    child_records = {
        "SMYY": _child_with_grid("SMYY", sigma=0.70, beta=0.48, p50=0.35),
    }
    anchor = 0.31
    grid = build_fof_pair_scenario_grid(
        basket,
        child_records,
        anchor_p50=anchor,
        expense_ratio_annual=0.01,
        cash_drag_annual=0.0,
    )
    assert grid is not None
    assert grid["engine"] == "fof_weighted_child_structural"
    assert len(grid["p50_log_grid"]) == 5
    assert len(grid["p50_log_grid"][0]) == 5
    center = grid["p50_log_grid"][2][2]
    assert center == pytest.approx(anchor, abs=0.002)

"""Tests for FoF basket TR / chart payloads."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from yieldboost_fof_basket_series import (  # noqa: E402
    build_basket_tr_series,
    build_fof_chart_payload,
    build_fof_static_scenario_grid,
    build_synthetic_fof_nav_series,
    enrich_fof_dashboard_extras,
    resolve_fof_price_series,
    series_has_price_anomaly,
)


def _metrics_fixture() -> pd.DataFrame:
    rows = []
    nav = 10.0
    und = 100.0
    for i in range(30):
        d = f"2026-01-{i + 2:02d}"
        nav *= 0.998
        und *= 1.002
        rows.append({"date": d, "ticker": "YBTY", "nav": round(nav, 4)})
        rows.append({
            "date": d,
            "ticker": "SMYY",
            "nav": round(nav * 0.95, 4),
            "etf_adj_close": round(nav * 0.95, 4),
            "underlying_adj_close": round(und, 4),
        })
    return pd.DataFrame(rows)


def test_build_synthetic_fof_nav_series():
    metrics = _metrics_fixture()
    history = [{
        "as_of": "2026-01-01",
        "children": [{"yb_etf": "SMYY", "weight_pct": 100.0}],
        "underlying_weights": {"SMCI": 1.0},
    }]
    s = build_synthetic_fof_nav_series(history, metrics)
    assert len(s) >= 20
    assert float(s.iloc[0]) == pytest.approx(100.0, rel=1e-3)


def test_rebalance_does_not_spike_synthetic_index():
    """Weight changes must not create level jumps (old level-mix bug)."""
    metrics = pd.DataFrame([
        {"date": "2026-01-02", "ticker": "SMYY", "nav": 10.0, "underlying_adj_close": 100.0},
        {"date": "2026-01-03", "ticker": "SMYY", "nav": 10.1, "underlying_adj_close": 101.0},
        {"date": "2026-01-06", "ticker": "SMYY", "nav": 10.2, "underlying_adj_close": 102.0},
        {"date": "2026-01-02", "ticker": "IOYY", "nav": 20.0, "underlying_adj_close": 200.0},
        {"date": "2026-01-03", "ticker": "IOYY", "nav": 20.2, "underlying_adj_close": 202.0},
        {"date": "2026-01-06", "ticker": "IOYY", "nav": 20.4, "underlying_adj_close": 204.0},
    ])
    history = [
        {
            "as_of": "2026-01-01",
            "children": [{"yb_etf": "SMYY", "weight_pct": 100.0}],
            "underlying_weights": {"SMCI": 1.0},
        },
        {
            "as_of": "2026-01-04",
            "children": [
                {"yb_etf": "SMYY", "weight_pct": 50.0},
                {"yb_etf": "IOYY", "weight_pct": 50.0},
            ],
            "underlying_weights": {"SMCI": 0.5, "IONQ": 0.5},
        },
    ]
    s = build_synthetic_fof_nav_series(history, metrics)
    rets = s.pct_change().dropna()
    assert rets.abs().max() < 0.05


def test_build_basket_tr_series():
    metrics = _metrics_fixture()
    history = [{
        "as_of": "2026-01-01",
        "underlying_weights": {"SMCI": 1.0},
        "children": [{"yb_etf": "SMYY", "weight_pct": 100.0}],
    }]
    b = build_basket_tr_series(history, metrics)
    assert len(b) >= 20
    assert float(b.iloc[0]) == pytest.approx(100.0, rel=1e-3)


def test_build_fof_chart_payload_spread_identity():
    metrics = _metrics_fixture()
    fof = metrics[metrics.ticker == "YBTY"].sort_values("date")
    fof_s = pd.Series(fof.nav.values, index=fof.date.astype(str))
    basket = build_basket_tr_series([{
        "as_of": "2026-01-01",
        "underlying_weights": {"SMCI": 1.0},
        "children": [{"yb_etf": "SMYY", "weight_pct": 100.0}],
    }], metrics)
    out = build_fof_chart_payload(fof_s, basket, window_days=21)
    assert out["ok"] is True
    assert out["fof_end_pct"] is not None
    assert out["spread_end_pct"] == pytest.approx(
        out["basket_end_pct"] - out["fof_end_pct"],
        abs=1e-4,
    )
    assert len(out.get("all_dates") or []) >= 5


def test_series_has_price_anomaly():
    s = pd.Series([100.0, 101.0, 300.0], index=["2026-01-01", "2026-01-02", "2026-01-03"])
    assert series_has_price_anomaly(s) is True
    s_ok = pd.Series([100.0, 101.0, 102.0], index=["2026-01-01", "2026-01-02", "2026-01-03"])
    assert series_has_price_anomaly(s_ok) is False


def test_resolve_fof_price_series_prefers_compounded():
    metrics = _metrics_fixture()
    poison = metrics.copy()
    idx = poison.index[poison["ticker"] == "YBTY"][-1]
    poison.at[idx, "nav"] = float(poison.at[idx, "nav"]) * 0.05
    history = [{
        "as_of": "2026-01-01",
        "children": [{"yb_etf": "SMYY", "weight_pct": 100.0}],
        "underlying_weights": {"SMCI": 1.0},
    }]
    stored = pd.Series(
        poison.loc[poison["ticker"] == "YBTY", "nav"].values,
        index=poison.loc[poison["ticker"] == "YBTY", "date"].astype(str),
    )
    assert series_has_price_anomaly(stored) is True
    resolved = resolve_fof_price_series("YBTY", history, poison)
    assert series_has_price_anomaly(resolved) is False


def test_enrich_fof_dashboard_extras():
    metrics = _metrics_fixture()
    history = [{
        "as_of": "2026-01-01",
        "underlying_weights": {"SMCI": 1.0},
        "children": [{"yb_etf": "SMYY", "weight_pct": 100.0}],
    }]
    out = enrich_fof_dashboard_extras(
        "YBTY",
        history_snaps=history,
        metrics=metrics,
        forward_p50=0.35,
        effective_beta=0.5,
        borrow_annual=0.06,
    )
    assert out["fof_chart"]["ok"] is True
    assert out["fof_scenario_grid"]["ok"] is True
    assert out["fof_chart"].get("all_dates")


def test_static_scenario_grid():
    grid = build_fof_static_scenario_grid(
        forward_p50=0.4,
        effective_beta=0.7,
        borrow_annual=0.05,
        basket_vol=0.6,
    )
    assert grid["ok"] is True
    assert len(grid["rows"]) == 5

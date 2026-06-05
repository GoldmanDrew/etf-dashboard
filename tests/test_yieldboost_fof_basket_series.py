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
        rows.append({"date": d, "ticker": "SMYY", "etf_adj_close": round(nav * 0.95, 4), "underlying_adj_close": round(und, 4)})
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


def test_build_basket_tr_series():
    metrics = _metrics_fixture()
    history = [{
        "as_of": "2026-01-01",
        "underlying_weights": {"SMCI": 1.0},
        "children": [{"yb_etf": "SMYY", "weight_pct": 100.0}],
    }]
    b = build_basket_tr_series(history, metrics)
    assert len(b) >= 20


def test_build_fof_chart_payload():
    metrics = _metrics_fixture()
    fof = metrics[metrics.ticker == "YBTY"].sort_values("date")
    fof_s = pd.Series(fof.nav.values, index=fof.date.astype(str))
    basket = build_basket_tr_series([{
        "as_of": "2026-01-01",
        "underlying_weights": {"SMCI": 1.0},
        "children": [{"yb_etf": "SMYY", "weight_pct": 100.0}],
    }], metrics)
    out = build_fof_chart_payload(fof_s, basket)
    assert out["ok"] is True
    assert len(out["dates"]) >= 5
    assert out["fof_end_pct"] is not None


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


def test_static_scenario_grid():
    grid = build_fof_static_scenario_grid(
        forward_p50=0.4,
        effective_beta=0.7,
        borrow_annual=0.05,
        basket_vol=0.6,
    )
    assert grid["ok"] is True
    assert len(grid["rows"]) == 5

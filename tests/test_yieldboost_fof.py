"""Tests for YieldBOOST fund-of-funds (YBTY/YBST) dashboard rollup."""
from __future__ import annotations

import math
import sys
from pathlib import Path

import pandas as pd
import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from yieldboost_fof_forward import (  # noqa: E402
    bootstrap_fof_net_edge,
    weighted_child_nav_decay_forward,
    weighted_child_pair_pnl_blend,
)
from yieldboost_fof_holdings import (  # noqa: E402
    build_fof_holdings_history,
    build_fof_holdings_payload,
    extract_fof_children_from_holdings,
    infer_yb_child_ticker,
)
from yieldboost_fof_pair_pnl import (  # noqa: E402
    build_fof_dashboard_record,
    compute_fof_realized_pair_metrics,
    weighted_child_forward_metrics,
)


def _yb_metrics_fixture() -> pd.DataFrame:
    """YBTY NAV + SMYY underlying panel (60 aligned days)."""
    rows = []
    nav = 10.0
    und = 100.0
    dates = []
    for d in range(2, 67):
        day = f"2026-01-{d:02d}" if d <= 31 else f"2026-02-{d - 31:02d}"
        dates.append(day)
        nav *= 0.998
        und *= 1.002
        rows.append({"date": day, "ticker": "YBTY", "nav": round(nav, 4)})
        rows.append({"date": day, "ticker": "SMYY", "underlying_adj_close": round(und, 4)})
    return pd.DataFrame(rows)


def test_infer_yb_child_from_ticker():
    assert infer_yb_child_ticker(position_ticker="SMYY") == "SMYY"
    assert infer_yb_child_ticker(security_name="GraniteShares YieldBOOST SMCI ETF") == "SMYY"
    assert infer_yb_child_ticker(security_name="US Dollars") is None


def test_extract_fof_children_from_holdings():
    hdf = pd.DataFrame([
        {
            "as_of_date": "2026-06-04",
            "etf_ticker": "YBTY",
            "position_ticker": "SMYY",
            "security_name": "GraniteShares YieldBOOST SMCI ETF",
            "security_type": "ETF",
            "weight_pct": 19.83,
            "market_value": 551819.0,
            "shares": 62420.0,
        },
        {
            "as_of_date": "2026-06-04",
            "etf_ticker": "YBTY",
            "position_ticker": "IOYY",
            "security_name": "GraniteShares YieldBOOST IONQ ETF",
            "security_type": "ETF",
            "weight_pct": 19.21,
            "market_value": 534471.0,
            "shares": 60320.0,
        },
        {
            "as_of_date": "2026-06-04",
            "etf_ticker": "YBTY",
            "position_ticker": None,
            "security_name": "US Dollars",
            "security_type": "CASH",
            "weight_pct": 5.19,
            "market_value": 144530.0,
            "shares": 144530.0,
        },
    ])
    snap = extract_fof_children_from_holdings(hdf, "YBTY")
    assert snap is not None
    assert snap["n_children"] == 2
    assert abs(sum(snap["underlying_weights"].values()) - 1.0) < 0.02
    assert "SMCI" in snap["underlying_weights"]
    assert "IONQ" in snap["underlying_weights"]
    assert snap["cash_pct"] == pytest.approx(5.19)


def test_weighted_child_nav_decay_forward_v2():
    basket = {
        "children": [
            {"yb_etf": "SMYY", "weight_pct": 50.0},
            {"yb_etf": "IOYY", "weight_pct": 50.0},
        ],
        "cash_pct": 10.0,
    }
    child_records = {
        "SMYY": {
            "expected_gross_decay_p50_annual": 0.40,
            "expected_gross_decay_p10_annual": 0.30,
            "expected_gross_decay_p90_annual": 0.50,
            "delta": 0.5,
        },
        "IOYY": {
            "expected_gross_decay_p50_annual": 0.20,
            "expected_gross_decay_p10_annual": 0.10,
            "expected_gross_decay_p90_annual": 0.30,
            "delta": 0.4,
        },
    }
    out = weighted_child_nav_decay_forward(basket, child_records, expense_ratio_annual=0.01)
    # Invested 90% × avg(0.40, 0.20) = 0.27 gross sleeve, minus 1% ER → 0.26
    assert out["expected_gross_decay_p50_annual"] == pytest.approx(0.26, abs=0.001)
    assert out["effective_beta"] == pytest.approx(0.405, abs=0.001)
    assert out["cash_pct"] == pytest.approx(10.0)


def test_weighted_child_pair_pnl_blend_legacy():
    basket = {
        "children": [
            {"yb_etf": "SMYY", "weight_pct": 50.0},
            {"yb_etf": "IOYY", "weight_pct": 50.0},
        ],
    }
    child_records = {
        "SMYY": {"expected_pair_pnl_p50_annual": 0.20},
        "IOYY": {"expected_pair_pnl_p50_annual": 0.10},
    }
    out = weighted_child_pair_pnl_blend(basket, child_records)
    assert out["child_pair_p50_annual"] == pytest.approx(0.15)


def test_weighted_child_forward_metrics_wrapper():
    basket = {
        "children": [{"yb_etf": "SMYY", "weight_pct": 100.0}],
        "cash_pct": 0.0,
    }
    child_records = {
        "SMYY": {
            "expected_gross_decay_p50_annual": 0.35,
            "expected_gross_decay_p10_annual": 0.25,
            "expected_gross_decay_p90_annual": 0.45,
            "delta": 0.48,
        },
    }
    out = weighted_child_forward_metrics(basket, child_records)
    assert out["expected_gross_decay_p50_annual"] == pytest.approx(0.34, abs=0.001)


def test_compute_fof_realized_pair_metrics_synthetic():
    metrics = pd.DataFrame([
        {"date": "2026-01-02", "ticker": "YBTY", "nav": 10.0},
        {"date": "2026-01-03", "ticker": "YBTY", "nav": 9.9},
        {"date": "2026-01-06", "ticker": "YBTY", "nav": 9.85},
        {"date": "2026-01-07", "ticker": "YBTY", "nav": 9.80},
        {"date": "2026-01-08", "ticker": "YBTY", "nav": 9.75},
        {"date": "2026-01-02", "ticker": "SMYY", "underlying_adj_close": 100.0},
        {"date": "2026-01-03", "ticker": "SMYY", "underlying_adj_close": 101.0},
        {"date": "2026-01-06", "ticker": "SMYY", "underlying_adj_close": 102.0},
        {"date": "2026-01-07", "ticker": "SMYY", "underlying_adj_close": 103.0},
        {"date": "2026-01-08", "ticker": "SMYY", "underlying_adj_close": 104.0},
    ])
    history = [{
        "as_of": "2026-01-01",
        "underlying_weights": {"SMCI": 1.0},
        "cash_pct": 0.0,
        "children": [{"yb_etf": "SMYY", "underlying": "SMCI", "weight_pct": 100.0}],
    }]
    out = compute_fof_realized_pair_metrics("YBTY", history, metrics, borrow_annual=0.05)
    assert out["ok"] is True
    assert out["n_days"] >= 3
    assert math.isfinite(out["gross_decay_annual"])
    assert out["cash_pct"] == pytest.approx(0.0)
    assert len(out.get("daily_drags") or []) >= 3


def test_compute_fof_realized_pair_metrics_60d():
    metrics = _yb_metrics_fixture()
    history = [{
        "as_of": "2026-01-01",
        "underlying_weights": {"SMCI": 1.0},
        "cash_pct": 0.0,
        "children": [{"yb_etf": "SMYY", "underlying": "SMCI", "weight_pct": 100.0}],
    }]
    out = compute_fof_realized_pair_metrics("YBTY", history, metrics, borrow_annual=0.05)
    assert out["ok"] is True
    assert out["n_days"] >= 20
    h60 = next((h for h in out["horizons"] if h["days"] == 60), None)
    assert h60 is not None


def test_bootstrap_fof_net_edge():
    drags = [0.001] * 30
    out = bootstrap_fof_net_edge(
        drags,
        borrow_annual=0.06,
        forward_p50=0.25,
        forward_p10=0.15,
        forward_p90=0.35,
    )
    assert out["net_edge_p50_annual"] is not None
    assert out["net_edge_p05_annual"] <= out["net_edge_p95_annual"]
    assert out["net_edge_hist_json"]
    assert abs(out["net_edge_p50_annual"]) < 5.0
    assert abs(out["gross_anchor_target_annual"]) < 5.0


def test_bootstrap_fof_net_edge_not_inflated_by_252():
    """Regression: posterior shift must stay in annual log units (not ×252 twice)."""
    drags = [0.0006] * 120
    out = bootstrap_fof_net_edge(
        drags,
        borrow_annual=0.05,
        forward_p50=0.40,
        forward_p10=0.40,
        forward_p90=0.40,
    )
    assert out["net_edge_p50_annual"] == pytest.approx(0.35, abs=0.05)
    assert out["gross_anchor_target_annual"] == pytest.approx(0.40, abs=0.01)


def test_build_fof_dashboard_record_v2():
    basket = {
        "symbol": "YBTY",
        "as_of": "2026-06-04",
        "children": [{"yb_etf": "SMYY", "underlying": "SMCI", "weight_pct": 100.0}],
        "underlying_weights": {"SMCI": 1.0},
        "cash_pct": 0.0,
        "n_children": 1,
    }
    child_records = {
        "SMYY": {
            "expected_gross_decay_p50_annual": 0.35,
            "expected_gross_decay_p10_annual": 0.25,
            "expected_gross_decay_p90_annual": 0.45,
            "expected_pair_pnl_p50_annual": 0.70,
            "delta": 0.48,
        },
    }
    metrics = pd.DataFrame([
        {"date": "2026-01-02", "ticker": "YBTY", "nav": 10.0},
        {"date": "2026-01-03", "ticker": "YBTY", "nav": 9.9},
        {"date": "2026-01-06", "ticker": "YBTY", "nav": 9.85},
        {"date": "2026-01-07", "ticker": "YBTY", "nav": 9.80},
        {"date": "2026-01-08", "ticker": "YBTY", "nav": 9.75},
        {"date": "2026-01-09", "ticker": "YBTY", "nav": 9.70},
        {"date": "2026-01-02", "ticker": "SMYY", "underlying_adj_close": 100.0},
        {"date": "2026-01-03", "ticker": "SMYY", "underlying_adj_close": 101.0},
        {"date": "2026-01-06", "ticker": "SMYY", "underlying_adj_close": 102.0},
        {"date": "2026-01-07", "ticker": "SMYY", "underlying_adj_close": 103.0},
        {"date": "2026-01-08", "ticker": "SMYY", "underlying_adj_close": 104.0},
        {"date": "2026-01-09", "ticker": "SMYY", "underlying_adj_close": 105.0},
    ])
    rec = build_fof_dashboard_record(
        "YBTY",
        basket=basket,
        history_snaps=[basket],
        child_records=child_records,
        metrics=metrics,
        borrow_current=0.06,
        shares_available=100000,
    )
    assert rec is not None
    assert rec["product_class"] == "income_yieldboost_fof"
    assert rec["decomposition_note"] == "fof_synthetic_dashboard_v2"
    assert rec["expected_pair_pnl_basis"] == "fof_weighted_child_nav_decay"
    assert rec["expected_pair_pnl_p50_annual"] == pytest.approx(0.34, abs=0.001)
    assert rec["fof_forward_meta"]["child_pair_p50_blend"] == pytest.approx(0.70)
    assert rec["gross_decay_annual"] is not None
    assert rec["net_edge_p50_annual"] is not None
    assert rec["net_edge_p05_annual"] is not None


def test_build_fof_holdings_payload_history():
    hdf = pd.DataFrame([
        {"as_of_date": "2026-05-01", "etf_ticker": "YBST", "position_ticker": "AMYY", "security_type": "ETF",
         "security_name": "AMYY", "weight_pct": 50.0, "market_value": 100.0},
        {"as_of_date": "2026-06-01", "etf_ticker": "YBST", "position_ticker": "AMYY", "security_type": "ETF",
         "security_name": "AMYY", "weight_pct": 40.0, "market_value": 90.0},
        {"as_of_date": "2026-06-01", "etf_ticker": "YBST", "position_ticker": "NVYY", "security_type": "ETF",
         "security_name": "NVYY", "weight_pct": 60.0, "market_value": 110.0},
    ])
    hist = build_fof_holdings_history(hdf)
    assert len(hist["YBST"]) == 2
    payload = build_fof_holdings_payload(hdf)
    assert "YBST" in payload["latest"]

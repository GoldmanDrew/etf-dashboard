"""Tests for YieldBOOST fund-of-funds (YBTY/YBST) dashboard rollup."""
from __future__ import annotations

import math
import sys
from pathlib import Path

import pandas as pd
import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from yieldboost_fof_holdings import (
    build_fof_holdings_history,
    build_fof_holdings_payload,
    extract_fof_children_from_holdings,
    infer_yb_child_ticker,
)
from yieldboost_fof_pair_pnl import (
    build_fof_dashboard_record,
    compute_fof_realized_pair_metrics,
    weighted_child_forward_metrics,
)


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


def test_weighted_child_forward_metrics():
    basket = {
        "children": [
            {"yb_etf": "SMYY", "weight_pct": 50.0},
            {"yb_etf": "IOYY", "weight_pct": 50.0},
        ],
    }
    child_records = {
        "SMYY": {"expected_pair_pnl_p50_annual": 0.20, "net_edge_p50_annual": 0.15, "delta": 0.5},
        "IOYY": {"expected_pair_pnl_p50_annual": 0.10, "net_edge_p50_annual": 0.05, "delta": 0.4},
    }
    out = weighted_child_forward_metrics(basket, child_records)
    assert out["expected_pair_pnl_p50_annual"] == pytest.approx(0.15)
    assert out["net_edge_p50_annual"] == pytest.approx(0.10)
    assert out["effective_beta"] == pytest.approx(0.45)


def test_compute_fof_realized_pair_metrics_synthetic():
    metrics = pd.DataFrame([
        {"date": "2026-01-02", "ticker": "YBTY", "nav": 10.0, "underlying": "SMCI", "underlying_adj_close": 100.0},
        {"date": "2026-01-03", "ticker": "YBTY", "nav": 9.9, "underlying": "SMCI", "underlying_adj_close": 101.0},
        {"date": "2026-01-06", "ticker": "YBTY", "nav": 9.85, "underlying": "SMCI", "underlying_adj_close": 102.0},
        {"date": "2026-01-07", "ticker": "YBTY", "nav": 9.80, "underlying": "SMCI", "underlying_adj_close": 103.0},
        {"date": "2026-01-08", "ticker": "YBTY", "nav": 9.75, "underlying": "SMCI", "underlying_adj_close": 104.0},
    ])
    history = [{
        "as_of": "2026-01-01",
        "underlying_weights": {"SMCI": 1.0},
        "children": [{"yb_etf": "SMYY", "underlying": "SMCI", "weight_pct": 100.0}],
    }]
    out = compute_fof_realized_pair_metrics("YBTY", history, metrics, borrow_annual=0.05)
    assert out["ok"] is True
    assert out["n_days"] >= 3
    assert math.isfinite(out["gross_decay_annual"])


def test_build_fof_dashboard_record():
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
            "expected_pair_pnl_p50_annual": 0.12,
            "expected_pair_pnl_p10_annual": 0.08,
            "expected_pair_pnl_p90_annual": 0.16,
            "net_edge_p50_annual": 0.10,
            "delta": 0.48,
        },
    }
    metrics = pd.DataFrame([
        {"date": "2026-01-02", "ticker": "YBTY", "nav": 10.0, "underlying": "SMCI", "underlying_adj_close": 100.0},
        {"date": "2026-01-03", "ticker": "YBTY", "nav": 9.9, "underlying": "SMCI", "underlying_adj_close": 101.0},
        {"date": "2026-01-06", "ticker": "YBTY", "nav": 9.85, "underlying": "SMCI", "underlying_adj_close": 102.0},
        {"date": "2026-01-07", "ticker": "YBTY", "nav": 9.80, "underlying": "SMCI", "underlying_adj_close": 103.0},
        {"date": "2026-01-08", "ticker": "YBTY", "nav": 9.75, "underlying": "SMCI", "underlying_adj_close": 104.0},
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
    assert rec["is_dashboard_synthetic"] is True
    assert rec["expected_pair_pnl_p50_annual"] == pytest.approx(0.12)
    assert rec["gross_decay_annual"] is not None


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

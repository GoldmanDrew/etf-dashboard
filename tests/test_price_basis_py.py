"""Tests for scripts/price_basis.py (parity with assets/price_basis.js)."""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))

from price_basis import (  # noqa: E402
    build_tr_series_from_metrics,
    detect_split_boundary,
    max_abs_log_return,
    resolve_split_context,
)
from split_adjustments import filter_splits_needing_close_basis_fix as sa_filter


def test_filter_skips_continuous_yahoo_mtyy():
    dated = [
        (dt.date(2026, 5, 28), 24.0, 24.0),
        (dt.date(2026, 6, 1), 23.604, 23.604),
        (dt.date(2026, 6, 2), 22.99, 22.99),
    ]
    events = [(dt.date(2026, 6, 2), 6.0)]
    assert sa_filter(dated, events) == []
    pts = [(d, c) for d, c, _ in dated]
    ctx = resolve_split_context(pts, events)
    assert ctx["mode"] == "continuous"


def test_discrete_split_scales_pre_split_not_inflated_nav_tr():
    rows = [
        {"date": "2026-05-28", "close_price": 4.0, "nav_total_return": 264, "underlying_adj_close": 150},
        {"date": "2026-06-02", "close_price": 22.99, "etf_adj_close": 22.99, "nav_total_return": 23.02, "underlying_adj_close": 136},
    ]
    tr = build_tr_series_from_metrics(rows, [(dt.date(2026, 6, 2), 6.0)])
    pre = next(r for r in tr if r["date"] == "2026-05-28")
    assert pre["tr_etf_px"] < 30
    assert 20 < pre["tr_etf_px"] < 30


def test_mtyy_issuer_decay_gross_sane_band():
    rows = []
    for i in range(55):
        day = str(10 + (i % 20)).zfill(2)
        rows.append(
            {
                "date": f"2026-04-{day}",
                "close_price": 4.3 - i * 0.002,
                "etf_adj_close": 4.2 - i * 0.002,
                "nav_total_return": 4.35 - i * 0.002,
                "underlying_adj_close": 170 - i * 0.1,
            }
        )
    rows.extend(
        [
            {"date": "2026-05-28", "close_price": 4.0, "nav_total_return": 4.12, "underlying_adj_close": 151.64},
            {"date": "2026-06-01", "close_price": 23.604, "etf_adj_close": 23.604, "underlying_adj_close": 136.08},
            {"date": "2026-06-02", "close_price": 22.99, "etf_adj_close": 22.99, "underlying_adj_close": 136.08},
        ]
    )
    tr = build_tr_series_from_metrics(rows, [(dt.date(2026, 6, 2), 6.0)])
    assert len(tr) >= 57
    pre = next(r for r in tr if r["date"] == "2026-05-28")
    post = next(r for r in tr if r["date"] == "2026-06-02")
    assert pre["tr_etf_px"] > 20
    assert post["tr_etf_px"] == pytest.approx(22.99, rel=1e-3)


def test_detect_split_boundary_on_price_jump():
    pts = [
        (dt.date(2026, 5, 28), 4.0),
        (dt.date(2026, 6, 1), 23.6),
    ]
    assert detect_split_boundary(pts, 6.0) == dt.date(2026, 6, 1)


def test_forward_split_zero_price_return_when_normalized():
    rows = [
        {"date": "2026-04-01", "close_price": 100, "etf_adj_close": 100, "underlying_adj_close": 50},
        {"date": "2026-04-02", "close_price": 100 / 3, "etf_adj_close": 100 / 3, "underlying_adj_close": 50},
    ]
    tr = build_tr_series_from_metrics(rows, [(dt.date(2026, 4, 2), 1 / 3)])
    assert abs(tr[0]["tr_etf_px"] - tr[1]["tr_etf_px"]) < 0.02


def test_mtyy_real_metrics_no_nov_cliff():
    """Regression: pre-split adj must map consistently (no close-threshold cliff)."""
    import pandas as pd

    p = Path(__file__).resolve().parent.parent / "data" / "etf_metrics_daily.parquet"
    if not p.exists():
        pytest.skip("etf_metrics_daily.parquet not present")
    df = pd.read_parquet(p)
    m = df[df["ticker"] == "MTYY"].copy()
    m["date"] = m["date"].astype(str).str[:10]
    rows = m.sort_values("date").to_dict("records")
    tr = build_tr_series_from_metrics(rows, [(dt.date(2026, 6, 2), 6.0)])
    max_jump, jump_date = max_abs_log_return(tr, "tr_etf_px")
    assert max_jump < 0.15, f"ETF TR cliff on {jump_date}: log jump {max_jump}"
    if len(tr) >= 120:
        import math

        drags = []
        beta = 0.5
        for i in range(1, len(tr)):
            r_u = math.log(tr[i]["tr_und_px"] / tr[i - 1]["tr_und_px"])
            r_l = math.log(tr[i]["tr_etf_px"] / tr[i - 1]["tr_etf_px"])
            drags.append(beta * r_u - r_l)
        gross_120 = math.expm1(sum(drags[-120:]))
        assert -0.5 < gross_120 < 0.75, f"120d gross out of band: {gross_120}"

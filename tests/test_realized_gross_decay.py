"""Tests for scripts/realized_gross_decay.py."""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))

from realized_gross_decay import (  # noqa: E402
    REALIZED_PAIR_GROSS_20D_HORIZON,
    build_daily_log_drag_series,
    compute_gross_decay_annual,
    compute_horizon_period_returns,
    compute_realized_pair_gross_20d,
    latest_contiguous_metrics_segment,
    realized_pair_gross_20d_fields,
    _period_borrow_log,
)
from price_basis import build_tr_series_from_metrics  # noqa: E402


def _flat_joint_rows(n: int, *, etf_drift: float = -0.005, und_drift: float = 0.0):
    rows = []
    ep, up = 100.0, 50.0
    base = dt.date(2024, 1, 2)
    for i in range(n):
        d = base + dt.timedelta(days=i)
        rows.append(
            {
                "date": d.isoformat(),
                "close_price": ep,
                "etf_adj_close": ep,
                "underlying_adj_close": up,
            }
        )
        ep *= 1 + etf_drift
        up *= 1 + und_drift
    return rows


def test_compute_horizon_period_returns_60d():
    joint = _flat_joint_rows(80, etf_drift=-0.005, und_drift=0.001)
    tr = build_tr_series_from_metrics(joint, [])
    daily = build_daily_log_drag_series(tr, 2.0)
    out = compute_horizon_period_returns(daily, [60], borrow_annual=0.252)
    h60 = out["horizons"][0]
    assert h60["horizon_days"] == 60
    assert h60["obs"] == 60
    assert h60["sufficient"] is True
    assert h60["gross_log"] is not None and h60["gross_log"] > 0
    assert h60["net_simple"] < h60["gross_simple"]
    assert abs(h60["net_log"] - (h60["gross_log"] - _period_borrow_log(0.252, 60))) < 1e-12


def test_realized_pair_gross_20d_fields():
    fields = realized_pair_gross_20d_fields(
        {
            "gross_simple": 0.05,
            "gross_log": 0.04879,
            "net_simple": 0.04,
            "obs": 60,
            "sufficient": True,
            "start_date": "2026-03-01",
            "end_date": "2026-06-01",
        }
    )
    assert fields["realized_pair_gross_20d"] == 0.05
    assert fields["realized_pair_gross_20d_sufficient"] is True
    assert fields["realized_pair_net_20d"] == 0.04


def test_realized_pair_gross_20d_fields_partial_window_not_full_metric():
    fields = realized_pair_gross_20d_fields(
        {
            "gross_simple": -0.2056,
            "gross_log": -0.2299,
            "net_simple": -0.21,
            "obs": 13,
            "sufficient": False,
            "start_date": "2026-05-20",
            "end_date": "2026-06-09",
        }
    )
    assert "realized_pair_gross_20d" not in fields
    assert fields["realized_pair_gross_partial"] == -0.2056
    assert fields["realized_pair_gross_20d_obs"] == 13
    assert fields["realized_pair_gross_20d_sufficient"] is False


def test_compute_realized_pair_gross_20d_skips_carry_forward_rows():
    joint = _flat_joint_rows(15)
    joint[-1] = {**joint[-1], "source_url": "carry_forward://stale-etf-row"}
    out = compute_realized_pair_gross_20d(joint, 2.0, [], borrow_annual=0.1)
    assert out is not None
    assert out["realized_pair_gross_20d_obs"] == 13
    assert out["realized_pair_gross_20d_sufficient"] is False
    assert "realized_pair_gross_20d" not in out
    assert "realized_pair_gross_partial" in out


def test_latest_contiguous_segment_cuts_ticker_reuse_gap():
    old_rows = _flat_joint_rows(65, etf_drift=0.0, und_drift=0.001)
    old_rows = [{**r, "source_provider": "yahoo_bootstrap"} for r in old_rows]
    new_rows = [
        {
            "date": (dt.date(2026, 6, 2) + dt.timedelta(days=i)).isoformat(),
            "close_price": 17.0 + i,
            "etf_adj_close": 17.0 + i,
            "underlying_adj_close": 128.0 + i,
            "source_provider": "merged",
        }
        for i in range(5)
    ]
    segment = latest_contiguous_metrics_segment(old_rows + new_rows)
    assert [r["date"] for r in segment] == [r["date"] for r in new_rows]


def test_ticker_reuse_gap_makes_60d_partial_and_gross_unavailable():
    old_rows = _flat_joint_rows(70, etf_drift=0.0, und_drift=0.002)
    new_rows = [
        {
            "date": (dt.date(2026, 6, 2) + dt.timedelta(days=i)).isoformat(),
            "close_price": 17.0 + i,
            "etf_adj_close": 17.0 + i,
            "underlying_adj_close": 128.0 + i,
        }
        for i in range(5)
    ]
    rows = old_rows + new_rows
    annual = compute_gross_decay_annual(rows, beta=2.0, split_events=[], min_obs=40)
    pair = compute_realized_pair_gross_20d(rows, beta=2.0, split_events=[], borrow_annual=0.1)
    assert annual is None
    assert pair is not None
    assert pair["realized_pair_gross_20d_obs"] == 4
    assert pair["realized_pair_gross_20d_sufficient"] is False
    assert "realized_pair_gross_20d" not in pair
    assert "realized_pair_gross_partial" in pair


def test_build_daily_log_drag_skips_orphan_leg_jumps():
    """Bad underlying backfill (~2×) or pre-split ETF prints must not dominate 20d decay."""
    tr = [
        {"date": "2026-06-20", "tr_etf_px": 16.0, "tr_und_px": 228.0},
        {"date": "2026-06-22", "tr_etf_px": 15.9, "tr_und_px": 228.11},
        {"date": "2026-06-24", "tr_etf_px": 15.78, "tr_und_px": 454.84},
        {"date": "2026-06-25", "tr_etf_px": 16.31, "tr_und_px": 462.48},
    ]
    daily = build_daily_log_drag_series(tr, 2.0)
    assert len(daily) == 2
    assert [d["date"] for d in daily] == ["2026-06-22", "2026-06-25"]
    assert all(abs(d["drag"]) < 0.05 for d in daily)

    tr_etf_cliff = [
        {"date": "2026-06-21", "tr_etf_px": 100.0, "tr_und_px": 300.0},
        {"date": "2026-06-22", "tr_etf_px": 33.33, "tr_und_px": 301.0},
        {"date": "2026-06-23", "tr_etf_px": 32.0, "tr_und_px": 299.0},
    ]
    daily2 = build_daily_log_drag_series(tr_etf_cliff, 2.0)
    assert len(daily2) == 1
    assert daily2[0]["date"] == "2026-06-23"


def test_compute_realized_pair_gross_20d_from_metrics_rows():
    joint = _flat_joint_rows(REALIZED_PAIR_GROSS_20D_HORIZON + 5)
    out = compute_realized_pair_gross_20d(joint, 2.0, [], borrow_annual=0.1)
    assert out is not None
    assert out["realized_pair_gross_20d"] is not None
    assert out["realized_pair_gross_20d_obs"] == REALIZED_PAIR_GROSS_20D_HORIZON


def test_compute_gross_decay_aplx_fixture():
    rows = [
        {"date": "2026-03-05", "close_price": 16.94, "etf_adj_close": 5.647, "underlying_adj_close": 10.0},
        {"date": "2026-03-06", "close_price": 13.357, "etf_adj_close": 4.452, "underlying_adj_close": 10.0},
        {"date": "2026-03-09", "close_price": 15.377, "etf_adj_close": 5.126, "underlying_adj_close": 10.1},
        {"date": "2026-03-10", "close_price": 15.71, "etf_adj_close": 15.71, "underlying_adj_close": 10.2},
        {"date": "2026-03-11", "close_price": 17.08, "etf_adj_close": 17.08, "underlying_adj_close": 10.3},
    ]
    base = dt.date(2026, 1, 15)
    extended = []
    for i in range(40):
        d = base + dt.timedelta(days=i)
        extended.append(
            {
                "date": d.isoformat(),
                "close_price": 14.0 + i * 0.01,
                "etf_adj_close": 4.67 + i * 0.003,
                "underlying_adj_close": 9.5 + i * 0.01,
            }
        )
    extended.extend(rows)
    result = compute_gross_decay_annual(
        extended,
        beta=2.0,
        split_events=[(dt.date(2026, 3, 10), 1 / 3)],
        min_obs=35,
    )
    assert result is not None
    assert result["n_obs"] >= 35

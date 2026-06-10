"""Tests for scripts/price_basis.py (parity with assets/price_basis.js)."""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

import math
import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))

from price_basis import (  # noqa: E402
    build_tr_series_from_metrics,
    detect_split_boundary,
    max_abs_log_return,
    parse_split_events_from_corp,
    resolve_split_context,
)
from split_adjustments import (
    filter_splits_needing_close_basis_fix as sa_filter,
    match_split_to_price_jump,
)


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


def test_mtyy_staggered_adj_before_close_no_tr_cliff():
    """Issuer metrics: Yahoo adj switches ~2026-05-27, close jumps 2026-06-02."""
    rows = [
        {"date": "2026-05-21", "close_price": 4.23, "etf_adj_close": 4.166, "underlying_adj_close": 164.85},
        {"date": "2026-05-27", "close_price": 4.04, "etf_adj_close": 0.673, "underlying_adj_close": 154.2},
        {"date": "2026-05-28", "close_price": 4.0, "etf_adj_close": 0.667, "underlying_adj_close": 151.64},
        {"date": "2026-06-01", "close_price": 3.934, "etf_adj_close": 0.656, "underlying_adj_close": 149.78},
        {"date": "2026-06-02", "close_price": 22.99, "etf_adj_close": 3.832, "underlying_adj_close": 136.08},
        {"date": "2026-06-08", "close_price": 22.515, "etf_adj_close": 22.515, "underlying_adj_close": 130.0},
    ]
    events = [(dt.date(2026, 6, 2), 6.0)]
    pts = [(dt.date.fromisoformat(r["date"]), float(r["close_price"])) for r in rows]
    adj_by_date = {
        dt.date.fromisoformat(r["date"]): float(r["etf_adj_close"])
        for r in rows
        if r.get("etf_adj_close") is not None
    }
    ctx = resolve_split_context(pts, events, metric_rows=rows, adj_by_date=adj_by_date)
    assert ctx["mode"] == "staggered_reverse_adj_first"
    assert ctx["adj_boundary"] == dt.date(2026, 5, 27)
    assert ctx["boundary"] == dt.date(2026, 6, 2)
    tr = build_tr_series_from_metrics(rows, events)
    may21 = next(r for r in tr if r["date"] == "2026-05-21")
    may27 = next(r for r in tr if r["date"] == "2026-05-27")
    assert 24 < may21["tr_etf_px"] < 26
    assert 23 < may27["tr_etf_px"] < 26
    assert abs(may27["tr_etf_px"] / may21["tr_etf_px"] - 1.0) < 0.08
    max_lr, _ = max_abs_log_return(tr, "tr_etf_px")
    assert max_lr < 0.35


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


def test_aplz_reverse_split_declared_five_accepted():
    rows = [
        {
            "date": "2026-05-27",
            "close_price": 2.565,
            "etf_adj_close": 2.565,
            "nav": 2.5625,
            "shares_outstanding": 2405000,
            "underlying_adj_close": 10,
        },
        {
            "date": "2026-06-01",
            "close_price": 2.66,
            "etf_adj_close": 2.66,
            "nav": 2.6527,
            "shares_outstanding": 2420000,
            "underlying_adj_close": 10.1,
        },
        {
            "date": "2026-06-02",
            "close_price": 2.66,
            "etf_adj_close": 2.66,
            "nav": 2.6616,
            "shares_outstanding": 484000,
            "underlying_adj_close": 10.2,
        },
        {
            "date": "2026-06-03",
            "close_price": 15.0,
            "etf_adj_close": 15.0,
            "nav": 15.0602,
            "shares_outstanding": 484000,
            "underlying_adj_close": 11,
        },
    ]
    events = [(dt.date(2026, 6, 3), 5.0)]
    ctx = resolve_split_context(
        [(dt.date(2026, 5, 27), 2.565), (dt.date(2026, 6, 1), 2.66), (dt.date(2026, 6, 2), 2.66), (dt.date(2026, 6, 3), 15.0)],
        events,
        metric_rows=rows,
    )
    assert ctx["mode"] == "discrete_split"
    assert ctx["mult"] == pytest.approx(5.0)
    tr = build_tr_series_from_metrics(rows, events)
    max_jump, _ = max_abs_log_return(tr, "tr_etf_px")
    assert max_jump < 0.35
    pre = next(r for r in tr if r["date"] == "2026-05-27")
    assert 12 < pre["tr_etf_px"] < 14


def test_aplx_forward_split_adj_basis_switch():
    """APLX 3-for-1: continuous close, adj switches from back-adjusted to raw."""
    rows = [
        {"date": "2026-03-05", "close_price": 16.94, "etf_adj_close": 5.647, "underlying_adj_close": 10.0},
        {"date": "2026-03-06", "close_price": 13.357, "etf_adj_close": 4.452, "underlying_adj_close": 10.0},
        {"date": "2026-03-09", "close_price": 15.377, "etf_adj_close": 5.126, "underlying_adj_close": 10.1},
        {"date": "2026-03-10", "close_price": 15.71, "etf_adj_close": 15.71, "underlying_adj_close": 10.2},
        {"date": "2026-03-11", "close_price": 17.08, "etf_adj_close": 17.08, "underlying_adj_close": 10.3},
    ]
    events = [(dt.date(2026, 3, 10), 1 / 3)]
    ctx = resolve_split_context(
        [(dt.date.fromisoformat(r["date"]), r["close_price"]) for r in rows],
        events,
        metric_rows=rows,
        adj_by_date={
            dt.date.fromisoformat(r["date"]): float(r["etf_adj_close"])
            for r in rows
        },
    )
    assert ctx["mode"] == "adj_basis_switch"
    assert ctx["mult"] == pytest.approx(1 / 3)
    tr = build_tr_series_from_metrics(rows, events)
    max_jump, jump_date = max_abs_log_return(tr, "tr_etf_px")
    assert max_jump < 0.35, f"split cliff on {jump_date}: {max_jump}"
    pre = next(r for r in tr if r["date"] == "2026-03-09")
    post = next(r for r in tr if r["date"] == "2026-03-10")
    assert pre["tr_etf_px"] == pytest.approx(5.126, rel=1e-3)
    assert post["tr_etf_px"] == pytest.approx(15.71 / 3, rel=0.02)


def test_aplx_real_metrics_no_split_cliff():
    import pandas as pd

    p = Path(__file__).resolve().parent.parent / "data" / "etf_metrics_daily.parquet"
    if not p.exists():
        pytest.skip("etf_metrics_daily.parquet not present")
    df = pd.read_parquet(p)
    m = df[df["ticker"] == "APLX"].copy()
    m["date"] = m["date"].astype(str).str[:10]
    rows = m.sort_values("date").to_dict("records")
    tr = build_tr_series_from_metrics(rows, [(dt.date(2026, 3, 10), 1 / 3)])
    split_dates = {dt.date(2026, 3, 10), dt.date(2026, 3, 11)}
    max_near = 0.0
    for i in range(1, len(tr)):
        d0 = dt.date.fromisoformat(tr[i]["date"])
        if not any(abs((d0 - sd).days) <= 7 for sd in split_dates):
            continue
        lr = abs(math.log(tr[i]["tr_etf_px"] / tr[i - 1]["tr_etf_px"]))
        max_near = max(max_near, lr)
    assert max_near < 0.35, f"APLX TR cliff near split: log jump {max_near}"


def test_appx_mislabeled_reverse_uses_continuous_close_tr():
    """APPX 1-for-3 reverse label with forward adj-basis-switch Yahoo panel."""
    rows = [
        {"date": "2026-03-09", "close_price": 48.15, "etf_adj_close": 144.45, "underlying_adj_close": 10.0},
        {"date": "2026-03-10", "close_price": 40.67, "etf_adj_close": 40.67, "underlying_adj_close": 10.1},
        {"date": "2026-03-11", "close_price": 37.89, "etf_adj_close": 37.89, "underlying_adj_close": 10.2},
    ]
    base = dt.date(2026, 1, 15)
    extended = []
    for i in range(40):
        d = base + dt.timedelta(days=i)
        extended.append(
            {
                "date": d.isoformat(),
                "close_price": 45.0 + i * 0.01,
                "etf_adj_close": 135.0 + i * 0.03,
                "underlying_adj_close": 9.5 + i * 0.01,
            }
        )
    extended.extend(rows)
    events = [(dt.date(2026, 3, 10), 3.0)]  # mislabeled reverse 1-for-3
    ctx = resolve_split_context(
        [(dt.date.fromisoformat(r["date"]), r["close_price"]) for r in rows],
        events,
        metric_rows=rows,
        adj_by_date={
            dt.date.fromisoformat(r["date"]): float(r["etf_adj_close"]) for r in rows
        },
    )
    assert ctx["mode"] == "continuous_close_tr"
    tr = build_tr_series_from_metrics(extended, events)
    max_jump, jump_date = max_abs_log_return(tr, "tr_etf_px")
    assert max_jump < 0.35, f"APPX cliff on {jump_date}: {max_jump}"


def test_arcx_reverse_continuous_close_tr():
    rows = [
        {"date": "2025-12-01", "close_price": 36.9, "etf_adj_close": 184.5, "underlying_adj_close": 10.0},
        {"date": "2025-12-02", "close_price": 38.74, "etf_adj_close": 38.74, "underlying_adj_close": 10.1},
        {"date": "2025-12-03", "close_price": 45.8, "etf_adj_close": 45.8, "underlying_adj_close": 10.2},
    ]
    events = [(dt.date(2025, 12, 2), 5.0)]
    ctx = resolve_split_context(
        [(dt.date.fromisoformat(r["date"]), r["close_price"]) for r in rows],
        events,
        metric_rows=rows,
        adj_by_date={dt.date.fromisoformat(r["date"]): float(r["etf_adj_close"]) for r in rows},
    )
    assert ctx["mode"] == "continuous_close_tr"
    tr = build_tr_series_from_metrics(rows, events)
    max_jump, _ = max_abs_log_return(tr, "tr_etf_px")
    assert max_jump < 0.35


def test_match_split_to_price_jump_trusts_declared_mult():
    assert match_split_to_price_jump(5.64, 5.0) == pytest.approx(5.0)


def test_aplz_backfilled_adj_not_double_scaled():
    """Regression: ingest backfill maps adj to post-split basis; do not multiply again."""
    rows = [
        {
            "date": "2026-05-27",
            "close_price": 2.565,
            "etf_adj_close": 12.825,
            "nav": 2.5625,
            "shares_outstanding": 2405000,
            "underlying_adj_close": 10,
        },
        {
            "date": "2026-06-03",
            "close_price": 15.0,
            "etf_adj_close": 15.0,
            "nav": 15.0602,
            "shares_outstanding": 484000,
            "underlying_adj_close": 11,
        },
    ]
    events = [(dt.date(2026, 6, 3), 5.0)]
    tr = build_tr_series_from_metrics(rows, events)
    pre = next(r for r in tr if r["date"] == "2026-05-27")
    assert pre["tr_etf_px"] == pytest.approx(12.825, rel=1e-3)
    max_jump, _ = max_abs_log_return(tr, "tr_etf_px")
    assert max_jump < 0.35


def test_mtyy_partial_adj_backfill_no_oscillation():
    rows = [
        {
            "date": "2026-05-27",
            "close_price": 4.04,
            "etf_adj_close": 24.24,
            "underlying_adj_close": 154.2,
        },
        {
            "date": "2026-05-28",
            "close_price": 4.0,
            "etf_adj_close": 3.937,
            "underlying_adj_close": 151.64,
        },
        {
            "date": "2026-06-02",
            "close_price": 22.99,
            "etf_adj_close": 22.99,
            "underlying_adj_close": 136.08,
        },
    ]
    tr = build_tr_series_from_metrics(rows, [(dt.date(2026, 6, 2), 6.0)])
    by_date = {r["date"]: r["tr_etf_px"] for r in tr}
    assert by_date["2026-05-27"] == pytest.approx(24.24, rel=1e-3)
    assert by_date["2026-05-28"] == pytest.approx(23.622, rel=0.02)
    max_jump, _ = max_abs_log_return(tr, "tr_etf_px")
    assert max_jump < 0.15


def test_mtyy_real_metrics_no_nov_cliff():
    """Regression: no spurious TR cliff near the June 2026 reverse split."""
    import pandas as pd

    p = Path(__file__).resolve().parent.parent / "data" / "etf_metrics_daily.parquet"
    if not p.exists():
        pytest.skip("etf_metrics_daily.parquet not present")
    df = pd.read_parquet(p)
    m = df[df["ticker"] == "MTYY"].copy()
    m["date"] = m["date"].astype(str).str[:10]
    rows = m.sort_values("date").to_dict("records")
    split_eff = dt.date(2026, 6, 2)
    tr = build_tr_series_from_metrics(rows, [(split_eff, 6.0)])
    max_near = 0.0
    jump_date = None
    for i in range(1, len(tr)):
        d0 = dt.date.fromisoformat(tr[i]["date"])
        if abs((d0 - split_eff).days) > 2:
            continue
        lr = abs(math.log(tr[i]["tr_etf_px"] / tr[i - 1]["tr_etf_px"]))
        if lr > max_near:
            max_near = lr
            jump_date = tr[i]["date"]
    assert max_near < 0.35 or abs(max_near - math.log(6.0)) <= 0.25, (
        f"ETF TR cliff on {jump_date}: log jump {max_near}"
    )
    if len(tr) >= 120:
        drags = []
        beta = 0.5
        for i in range(1, len(tr)):
            r_u = math.log(tr[i]["tr_und_px"] / tr[i - 1]["tr_und_px"])
            r_l = math.log(tr[i]["tr_etf_px"] / tr[i - 1]["tr_etf_px"])
            drags.append(beta * r_u - r_l)
        gross_120 = math.expm1(sum(drags[-120:]))
        assert -0.5 < gross_120 < 0.75, f"120d gross out of band: {gross_120}"


def test_provider_basis_jump_before_declared_reverse_split_is_segment_scaled():
    """SMCL-style provider restatement: old rows are post-basis, AXS row flips early."""
    rows = [
        {"date": "2026-04-14", "close_price": 47.70, "etf_adj_close": 954.0, "underlying_adj_close": 27.20},
        {"date": "2026-04-15", "close_price": 47.80, "etf_adj_close": 956.0, "underlying_adj_close": 27.29},
        {
            "date": "2026-04-16",
            "close_price": 2.59,
            "etf_adj_close": 51.8,
            "underlying_adj_close": 28.40,
            "source_url": "https://axsetf.filepoint.live/assets/data/NSDEAXS2.04162026.csv",
        },
        {"date": "2026-04-17", "close_price": 2.61, "etf_adj_close": 52.2, "underlying_adj_close": 28.56},
        {"date": "2026-04-30", "close_price": 46.60, "etf_adj_close": 932.0, "underlying_adj_close": 27.40},
        {"date": "2026-05-01", "close_price": 45.63, "etf_adj_close": 2.2815, "underlying_adj_close": 27.09},
    ]
    tr = build_tr_series_from_metrics(rows, [(dt.date(2026, 5, 1), 20.0)])
    by_date = {r["date"]: r for r in tr}
    assert by_date["2026-04-15"]["tr_etf_px"] == pytest.approx(47.8, rel=0.03)
    assert by_date["2026-04-16"]["tr_etf_px"] == pytest.approx(51.8, rel=0.03)
    max_jump, jump_date = max_abs_log_return(tr, "tr_etf_px")
    assert max_jump < 0.35, f"basis cliff on {jump_date}: {max_jump}"


def test_oscillating_provider_basis_segments_before_split_are_normalized():
    """BULG/GEMG-style rows can flip old/new basis multiple times before ex-date."""
    rows = [
        {"date": "2026-04-15", "close_price": 36.40, "etf_adj_close": 728.0, "underlying_adj_close": 6.47},
        {"date": "2026-04-16", "close_price": 1.83, "etf_adj_close": 36.6, "underlying_adj_close": 6.48},
        {"date": "2026-04-17", "close_price": 1.955, "etf_adj_close": 39.1, "underlying_adj_close": 6.72},
        {"date": "2026-04-22", "close_price": 2.27, "etf_adj_close": 45.4, "underlying_adj_close": 7.27},
        {"date": "2026-04-23", "close_price": 41.16, "etf_adj_close": 823.2, "underlying_adj_close": 6.91},
        {"date": "2026-04-24", "close_price": 40.966, "etf_adj_close": 2.0483, "underlying_adj_close": 6.90},
    ]
    tr = build_tr_series_from_metrics(rows, [(dt.date(2026, 4, 24), 20.0)])
    by_date = {r["date"]: r["tr_etf_px"] for r in tr}
    assert by_date["2026-04-15"] == pytest.approx(36.4, rel=0.03)
    assert by_date["2026-04-17"] == pytest.approx(39.1, rel=0.03)
    assert by_date["2026-04-23"] == pytest.approx(41.16, rel=0.03)
    max_jump, jump_date = max_abs_log_return(tr, "tr_etf_px")
    assert max_jump < 0.35, f"oscillating basis cliff on {jump_date}: {max_jump}"


def test_real_metrics_recent_split_outliers_have_no_split_sized_tr_cliff():
    import pandas as pd

    p = Path(__file__).resolve().parent.parent / "data" / "etf_metrics_daily.parquet"
    if not p.exists():
        pytest.skip("etf_metrics_daily.parquet not present")
    df = pd.read_parquet(p)
    df["date"] = df["date"].astype(str).str[:10]
    df["ticker"] = df["ticker"].astype(str).str.upper()
    cases = {
        "SMCL": (dt.date(2026, 5, 1), 20.0),
        "MSTP": (dt.date(2026, 5, 1), 20.0),
        "CRWG": (dt.date(2026, 5, 5), 10.0),
        "DUOG": (dt.date(2026, 5, 5), 10.0),
        "MRAL": (dt.date(2026, 5, 1), 10.0),
        "FIGG": (dt.date(2026, 5, 5), 20.0),
        "BULG": (dt.date(2026, 4, 24), 20.0),
        "GEMG": (dt.date(2026, 4, 24), 20.0),
        "BMNG": (dt.date(2026, 5, 5), 20.0),
        "BAIG": (dt.date(2026, 5, 5), 10.0),
    }
    for sym, event in cases.items():
        rows = df[df["ticker"] == sym].sort_values("date").to_dict("records")
        if not rows:
            continue
        tr = build_tr_series_from_metrics(rows, [event])
        split_eff = event[0]
        max_jump = 0.0
        jump_date = None
        for i in range(1, len(tr)):
            d0 = dt.date.fromisoformat(str(tr[i]["date"])[:10])
            if abs((d0 - split_eff).days) > 45:
                continue
            if tr[i - 1]["tr_etf_px"] <= 0 or tr[i]["tr_etf_px"] <= 0:
                continue
            if tr[i - 1]["tr_und_px"] <= 0 or tr[i]["tr_und_px"] <= 0:
                continue
            if abs(math.log(tr[i]["tr_und_px"] / tr[i - 1]["tr_und_px"])) >= 0.25:
                continue
            lr = abs(math.log(tr[i]["tr_etf_px"] / tr[i - 1]["tr_etf_px"]))
            jump = math.exp(lr)
            split_mult = event[1] if event[1] >= 1 else 1 / event[1]
            if abs(jump / split_mult - 1.0) > 0.18:
                continue
            if lr > max_jump:
                max_jump = lr
                jump_date = tr[i]["date"]
        assert max_jump < 0.35, f"{sym} TR cliff on {jump_date}: {max_jump}"


def test_future_split_does_not_rewrite_old_split_sized_market_move():
    rows = [
        {"date": "2024-08-21", "close_price": 448.0, "etf_adj_close": 430.208099, "underlying_adj_close": 62.377998},
        {"date": "2024-08-22", "close_price": 753.200012, "etf_adj_close": 723.287415, "underlying_adj_close": 60.481998},
        {"date": "2024-08-23", "close_price": 773.200012, "etf_adj_close": 742.493103, "underlying_adj_close": 61.324001},
    ]
    ctx = resolve_split_context(
        [(dt.date.fromisoformat(r["date"]), r["close_price"]) for r in rows],
        [(dt.date(2026, 3, 19), 2.0)],
        metric_rows=rows,
        adj_by_date={dt.date.fromisoformat(r["date"]): float(r["etf_adj_close"]) for r in rows},
    )
    assert ctx["mode"] == "continuous"
    tr = build_tr_series_from_metrics(rows, [(dt.date(2026, 3, 19), 2.0)])
    assert tr[0]["tr_etf_px"] == pytest.approx(430.208099, rel=1e-6)

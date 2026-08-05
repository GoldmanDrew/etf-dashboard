"""Tests for scripts/realized_gross_decay.py."""
from __future__ import annotations

import datetime as dt
import math
import json
import sys
from pathlib import Path

import pandas as pd

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))

from realized_gross_decay import (  # noqa: E402
    MAX_PAIR_DRAG_GAP_DAYS,
    PAIR_DRAG_BASIS,
    PARTIAL_MIN_OBS,
    REALIZED_PAIR_GROSS_20D_HORIZON,
    _is_direction_violation,
    annualize_period_log_drag,
    best_estimate_gross_from_realized_pair_fields,
    build_daily_log_drag_series,
    build_daily_log_drag_series_with_meta,
    collapse_partial_horizons,
    compute_gross_decay_annual,
    compute_horizon_period_returns,
    compute_pair_track_quality,
    compute_realized_pair_gross_20d,
    latest_contiguous_metrics_segment,
    load_realized_pair_gross_20d_from_metrics,
    realized_pair_gross_20d_fields,
    _period_borrow_log,
)
from price_basis import build_tr_series_from_metrics  # noqa: E402


def test_loader_cliff_check_ignores_prior_reused_ticker_lifecycle(tmp_path):
    rows = []
    for i in range(4):
        rows.append({
            "date": dt.date(2018, 1, 2) + dt.timedelta(days=i),
            "ticker": "REUSE", "close_price": 10 + i,
            "etf_adj_close": 10 + i, "underlying_adj_close": 20 + i,
            "source_provider": "yahoo_bootstrap", "source_url": "", "stale_kind": "",
        })
    for i in range(25):
        rows.append({
            "date": dt.date(2026, 6, 1) + dt.timedelta(days=i),
            "ticker": "REUSE", "close_price": 30 * (1.001 ** i),
            "etf_adj_close": 30 * (1.001 ** i),
            "underlying_adj_close": 120 * (1.001 ** i),
            "source_provider": "issuer", "source_url": "", "stale_kind": "",
        })
    metrics = tmp_path / "metrics.parquet"
    pd.DataFrame(rows).to_parquet(metrics, index=False)
    corp = tmp_path / "corp.json"
    corp.write_text(json.dumps({"events": []}), encoding="utf-8")
    out = load_realized_pair_gross_20d_from_metrics(
        metrics,
        {"REUSE"},
        corp_actions_path=corp,
        beta_by_symbol={"REUSE": 2.0},
        underlying_by_symbol={"REUSE": "UND"},
    )
    assert out["REUSE"]["realized_pair_gross_20d_sufficient"] is True
    assert out["REUSE"].get("suppressed") is not True


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


def test_pair_drag_basis_is_log_drag_contract():
    assert PAIR_DRAG_BASIS == "beta_log_minus_etf_log"
    assert MAX_PAIR_DRAG_GAP_DAYS == 5


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


def test_period_gross_equals_endpoint_log_drag():
    joint = _flat_joint_rows(25, etf_drift=-0.01, und_drift=0.002)
    tr = build_tr_series_from_metrics(joint, [])
    daily = build_daily_log_drag_series(tr, 2.0)
    out = compute_horizon_period_returns(daily, [20], borrow_annual=0.0)
    row = out["horizons"][0]
    start = tr[-21]
    end = tr[-1]
    endpoint = 2.0 * math.log(end["tr_und_px"] / start["tr_und_px"]) - math.log(
        end["tr_etf_px"] / start["tr_etf_px"]
    )
    assert abs(row["gross_log"] - endpoint) < 1e-9


def test_perfect_simple_neg2x_large_day_flagged_convexity_not_zeroed():
    tr = [
        {"date": "2026-05-07", "tr_etf_px": 12.84, "tr_und_px": 78.58},
        {"date": "2026-05-08", "tr_etf_px": 4.05, "tr_und_px": 105.47},
    ]
    result = build_daily_log_drag_series_with_meta(tr, -2.0)
    series = result["series"]
    meta = result["meta"]
    assert len(series) == 1
    assert abs(series[0]["simple_pnl"]) < 0.01
    assert abs(series[0]["drag"]) > 0.3
    assert series[0]["convexity_day"] is True
    assert len(meta["convexity_days"]) == 1


def test_build_daily_log_drag_skips_calendar_gaps_over_5_days():
    tr = [
        {"date": "2026-05-28", "tr_etf_px": 1.73, "tr_und_px": 148.03},
        {"date": "2026-05-29", "tr_etf_px": 1.84, "tr_und_px": 143.48},
        {"date": "2026-06-16", "tr_etf_px": 2.83, "tr_und_px": 104.63},
        {"date": "2026-06-17", "tr_etf_px": 2.65, "tr_und_px": 107.98},
    ]
    result = build_daily_log_drag_series_with_meta(tr, -2.0)
    assert [d["date"] for d in result["series"]] == ["2026-05-29", "2026-06-17"]
    assert len(result["meta"]["skipped_gaps"]) == 1


def test_collapse_partial_horizons_dedupes():
    joint = _flat_joint_rows(26, etf_drift=-0.002, und_drift=0.001)
    tr = build_tr_series_from_metrics(joint, [])
    daily = build_daily_log_drag_series(tr, -2.0)
    raw = compute_horizon_period_returns(daily, [5, 20, 60, 120, 251], borrow_annual=0.1)
    collapsed = collapse_partial_horizons(raw)
    partials = [h for h in collapsed["horizons"] if not h.get("sufficient")]
    assert len(partials) == 1
    assert partials[0].get("available_history") is True


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
    assert annual is None  # only ~4 drag days after lifecycle cut (< PARTIAL_MIN_OBS)
    assert pair is not None
    assert pair["realized_pair_gross_20d_obs"] == 4
    assert pair["realized_pair_gross_20d_sufficient"] is False
    assert "realized_pair_gross_20d" not in pair
    assert "realized_pair_gross_partial" in pair


def test_partial_history_returns_annualized_best_estimate():
    """10–39 drag days → quality=partial annualized mean, not null."""
    rows = _flat_joint_rows(25, etf_drift=-0.001, und_drift=0.002)  # ~24 drag days
    result = compute_gross_decay_annual(rows, beta=2.0, split_events=[], min_obs=40)
    assert result is not None
    assert result["quality"] == "partial"
    assert result["n_obs"] >= PARTIAL_MIN_OBS
    assert result["n_obs"] < 40
    assert result["gross_decay_annual_source"] == "etf_metrics_daily_partial"
    assert result["gross_decay_annual"] > 0

    # Full panel still quality=full
    full = compute_gross_decay_annual(
        _flat_joint_rows(50, etf_drift=-0.001, und_drift=0.002),
        beta=2.0,
        split_events=[],
        min_obs=40,
    )
    assert full is not None
    assert full["quality"] == "full"
    assert full["gross_decay_annual_source"] == "etf_metrics_daily"


def test_best_estimate_from_20d_period_log():
    fields = {
        "realized_pair_gross_20d_log": 0.10,
        "realized_pair_gross_20d_obs": 20,
        "realized_pair_gross_20d_start_date": "2026-06-01",
        "realized_pair_gross_20d_end_date": "2026-06-29",
    }
    est = best_estimate_gross_from_realized_pair_fields(fields)
    assert est is not None
    assert est["quality"] == "partial"
    assert est["gross_decay_annual_source"] == "annualized_from_20d_period"
    assert abs(est["gross_decay_annual"] - annualize_period_log_drag(0.10, 20)) < 1e-9

    too_thin = best_estimate_gross_from_realized_pair_fields(
        {"realized_pair_gross_partial_log": 0.05, "realized_pair_gross_20d_obs": 5}
    )
    assert too_thin is None


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


def test_direction_violation_detects_wrong_way_letf_day():
    # +2x LETF should rise when the underlying rises; a large opposite move is impossible.
    assert _is_direction_violation(2.0, 0.10, -0.40) is True
    # Same-direction move is fine even when large.
    assert _is_direction_violation(2.0, 0.10, 0.20) is False
    # Small underlying moves never trip the filter (noise).
    assert _is_direction_violation(2.0, 0.005, -0.40) is False
    # Inverse: underlying up ⇒ ETF should fall.
    assert _is_direction_violation(-2.0, 0.10, 0.40) is True
    assert _is_direction_violation(-2.0, 0.10, -0.20) is False


def test_well_tracked_pair_excludes_direction_violation_day():
    """A single impossible print on a tracking LETF is dropped, not published."""
    tr = []
    etf, und = 100.0, 50.0
    base = dt.date(2024, 1, 2)
    for i in range(40):
        tr.append({"date": (base + dt.timedelta(days=i)).isoformat(), "tr_etf_px": etf, "tr_und_px": und})
        # Quiet 2x tracking days.
        und *= 1.002
        etf *= 1.004
    # Inject one impossible day that is NOT an orphan-leg cliff: both legs move,
    # but the ETF goes the wrong way by more than DIRECTION_MIN_GAP_LOG.
    bad_day = base + dt.timedelta(days=40)
    und_bad = und * math.exp(0.12)
    etf_bad = etf * math.exp(-0.25)
    tr.append({"date": bad_day.isoformat(), "tr_etf_px": etf_bad, "tr_und_px": und_bad})
    und, etf = und_bad, etf_bad
    for i in range(5):
        und *= 1.001
        etf *= 1.002
        tr.append(
            {
                "date": (bad_day + dt.timedelta(days=i + 1)).isoformat(),
                "tr_etf_px": etf,
                "tr_und_px": und,
            }
        )

    result = build_daily_log_drag_series_with_meta(tr, 2.0)
    meta = result["meta"]
    assert meta["pair_track"]["tracks_well"] is True
    assert any(v["date"] == bad_day.isoformat() for v in meta["direction_violations"])
    assert any(v["date"] == bad_day.isoformat() for v in meta["direction_violations_excluded"])
    assert bad_day.isoformat() not in {d["date"] for d in result["series"]}


def test_untracked_pair_with_violation_suppresses_20d():
    """Noise pair + impossible day → suppress rather than publish a number."""
    rows = []
    etf, und = 100.0, 50.0
    base = dt.date(2024, 1, 2)
    for i in range(25):
        # Uncorrelated legs so R² collapses.
        und *= 1.01 if i % 2 == 0 else 0.99
        etf *= 0.995 if i % 3 == 0 else 1.01
        rows.append(
            {
                "date": (base + dt.timedelta(days=i)).isoformat(),
                "close_price": etf,
                "etf_adj_close": etf,
                "underlying_adj_close": und,
            }
        )
    # Force one clear direction violation that is not an orphan-leg cliff.
    und *= math.exp(0.12)
    etf *= math.exp(-0.25)
    rows.append(
        {
            "date": (base + dt.timedelta(days=25)).isoformat(),
            "close_price": etf,
            "etf_adj_close": etf,
            "underlying_adj_close": und,
        }
    )
    for i in range(5):
        und *= 1.0
        etf *= 1.0
        rows.append(
            {
                "date": (base + dt.timedelta(days=26 + i)).isoformat(),
                "close_price": etf,
                "etf_adj_close": etf,
                "underlying_adj_close": und,
            }
        )
    out = compute_realized_pair_gross_20d(rows, 2.0, [])
    assert out is not None
    assert out.get("pair_untracked") is True
    assert out.get("suppressed") is True


def test_pair_track_quality_requires_r2_and_beta():
    good = [(0.01, 0.02), (0.02, 0.04), (-0.01, -0.02)] * 15
    track = compute_pair_track_quality(good, 2.0)
    assert track["tracks_well"] is True
    assert track["r2"] >= 0.90

    noise = [(0.01, -0.02), (-0.02, 0.03), (0.015, 0.001)] * 15
    bad = compute_pair_track_quality(noise, 2.0)
    assert bad["tracks_well"] is False

"""Tests for Layer A dual-h + membership clock + reason-typed cadence."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from bucket4.bucket4_dynamic_bt import (  # noqa: E402
    realized_hedge_ratio,
    run_bucket4_backtest_dynamic_h,
)
from bucket4.layer_a_parity import (  # noqa: E402
    compare_layer_a,
    inject_production_calendar_into_panel,
    membership_bounds_from_production,
    reason_dates,
    snap_dates_to_calendar,
)


def test_sanitize_panel_replaces_fabricated_etf_cliff():
    from bucket4.bucket4_price_loading import sanitize_panel_vs_session_close

    idx = pd.to_datetime(["2026-01-30", "2026-02-02", "2026-02-03"])
    panel = {
        "QBTZ": pd.DataFrame(
            {"a_px": [12.12, 37.23, 35.70], "b_px": [21.0, 21.0, 21.0]},
            index=idx,
        )
    }
    metrics = pd.DataFrame(
        {
            "date": idx,
            "ticker": ["QBTZ", "QBTZ", "QBTZ"],
            "close_price": [36.36, 37.23, 35.70],
            "etf_adj_close": [36.36, 37.23, 35.70],
            "underlying_adj_close": [21.0, 21.0, 21.0],
        }
    )
    out = sanitize_panel_vs_session_close(panel, metrics)
    assert float(out["QBTZ"].loc["2026-01-30", "a_px"]) == pytest.approx(36.36, rel=1e-6)
    assert float(out["QBTZ"]["a_px"].pct_change().abs().max()) < 0.05


def test_sanitize_panel_replaces_ratio_step_without_100pct_day():
    """APLZ-style: early history scaled ×5 vs close, late unscaled; day moves <100%."""
    from bucket4.bucket4_price_loading import sanitize_panel_vs_session_close

    idx = pd.bdate_range("2026-01-02", periods=25)
    close = np.linspace(10.0, 12.0, len(idx))
    # First half of panel is 5× close; second half matches close. Day-to-day
    # returns stay modest except a single ~20% step at the join (still <100%).
    a = close.copy()
    a[:12] = close[:12] * 5.0
    panel = {"APLZ": pd.DataFrame({"a_px": a, "b_px": np.full(len(idx), 50.0)}, index=idx)}
    metrics = pd.DataFrame(
        {
            "date": idx,
            "ticker": ["APLZ"] * len(idx),
            "close_price": close,
            "etf_adj_close": close,
            "underlying_adj_close": np.full(len(idx), 50.0),
        }
    )
    out = sanitize_panel_vs_session_close(panel, metrics)
    assert float(out["APLZ"].loc[idx[0], "a_px"]) == pytest.approx(float(close[0]), rel=1e-6)
    assert float(out["APLZ"].loc[idx[-1], "a_px"]) == pytest.approx(float(close[-1]), rel=1e-6)
    # No residual 5× prefix after sanitize
    ratio = out["APLZ"]["a_px"].astype(float) / close
    assert float(ratio.median()) == pytest.approx(1.0, abs=0.05)


def test_sanitize_panel_prefers_adj_when_close_has_reverse_split_cliff():
    """APLZ/BEZ/NBIZ: panel==close jumps on reverse-split day; etf_adj_close is continuous."""
    from bucket4.bucket4_price_loading import sanitize_panel_vs_session_close

    idx = pd.to_datetime(["2026-05-28", "2026-05-29", "2026-06-01", "2026-06-02"])
    close = np.array([2.51, 2.72, 13.30, 13.30])
    adj = np.array([12.55, 13.60, 13.30, 13.30])
    panel = {
        "APLZ": pd.DataFrame(
            {"a_px": close.copy(), "b_px": np.full(len(idx), 47.0)},
            index=idx,
        )
    }
    metrics = pd.DataFrame(
        {
            "date": idx,
            "ticker": ["APLZ"] * len(idx),
            "close_price": close,
            "etf_adj_close": adj,
            "underlying_adj_close": np.full(len(idx), 47.0),
        }
    )
    out = sanitize_panel_vs_session_close(panel, metrics)
    assert float(out["APLZ"].loc["2026-05-29", "a_px"]) == pytest.approx(13.60, rel=1e-6)
    assert float(out["APLZ"].loc["2026-06-01", "a_px"]) == pytest.approx(13.30, rel=1e-6)
    assert float(out["APLZ"]["a_px"].pct_change().abs().max()) < 0.10


def test_sanitize_panel_scales_tecs_style_split_sized_basis_jumps():
    """Provider restates a ~10× segment while underlying barely moves (TECS May-2026)."""
    from bucket4.bucket4_price_loading import sanitize_panel_split_sized_basis_jumps

    idx = pd.to_datetime(
        ["2026-05-15", "2026-05-19", "2026-05-20", "2026-05-21", "2026-05-22", "2026-05-28", "2026-05-29"]
    )
    a = np.array([85.5, 902.0, 841.0, 822.0, 79.6, 715.0, 66.7])
    b = np.array([176.0, 173.0, 177.0, 178.0, 180.0, 187.0, 191.0])
    panel = {"TECS": pd.DataFrame({"a_px": a, "b_px": b}, index=idx)}
    out = sanitize_panel_split_sized_basis_jumps(panel)
    px = out["TECS"]["a_px"].astype(float)
    # Spikes map onto a continuous basis — no residual ~10× day (raw max was ~10×).
    assert float(px.pct_change().abs().max()) < 0.35
    assert float(px.iloc[0]) == pytest.approx(85.5, rel=0.05)
    assert float(px.loc["2026-05-19"]) == pytest.approx(90.2, rel=0.05)
    assert float(px.iloc[-1]) == pytest.approx(66.7, rel=1e-6)


def test_equity_wipeout_stops_ghost_rebalances():
    idx = pd.bdate_range("2024-01-02", periods=12)
    # Sudden 3× ETF spike wipes a short book sized at unit equity.
    a = np.full(len(idx), 10.0)
    a[5:] = 40.0
    px = pd.DataFrame({"a_px": a, "b_px": np.full(len(idx), 20.0)}, index=idx)
    h = pd.Series(0.5, index=idx)
    bt = run_bucket4_backtest_dynamic_h(
        px,
        h,
        idx[::3],
        initial_capital=1.0,
        beta_a=-2.0,
        opt2_h_base=0.5,
    )
    wipe = bt[bt["rebalance_reason"] == "equity_wipeout"]
    assert len(wipe) >= 1
    after = bt.loc[wipe.index[0] :].iloc[1:]
    assert (after["gross_exposure"] == 0).all()
    assert not after["rebalance"].any()


def test_realized_hedge_ratio_matches_production_formula():
    assert realized_hedge_ratio(-1068.0, -1601.16, beta_abs=2.0) == pytest.approx(0.75, abs=1e-3)
    assert realized_hedge_ratio(-238.0, -2469.79, beta_abs=2.0) == pytest.approx(5.189, abs=1e-2)


def test_dynamic_bt_emits_target_and_realized_h():
    idx = pd.bdate_range("2024-01-02", periods=30)
    px = pd.DataFrame(
        {
            "a_px": np.linspace(50, 40, len(idx)),
            "b_px": np.linspace(100, 110, len(idx)),
        },
        index=idx,
    )
    h = pd.Series(0.45, index=idx)
    bt = run_bucket4_backtest_dynamic_h(
        px,
        h,
        idx[::7],
        initial_capital=1.0,
        beta_a=-2.0,
        opt2_h_base=0.45,
    )
    assert "h_target" in bt.columns and "h_realized" in bt.columns
    assert (bt["h_target"] == bt["h_used"]).all()
    # After enter, realized should be near target when freshly rebalanced
    first = bt.index[0]
    assert bt.loc[first, "rebalance_reason"] == "enter_membership"
    assert bt.loc[first, "h_realized"] == pytest.approx(0.45, abs=0.02)


def test_membership_hard_exit_flattens_and_tags_reason():
    idx = pd.bdate_range("2024-03-01", periods=20)
    px = pd.DataFrame(
        {
            "a_px": np.full(len(idx), 50.0),
            "b_px": np.full(len(idx), 100.0),
        },
        index=idx,
    )
    h = pd.Series(0.5, index=idx)
    start, end = idx[2], idx[10]
    bt = run_bucket4_backtest_dynamic_h(
        px,
        h,
        idx[::3],
        initial_capital=1.0,
        beta_a=-2.0,
        membership_start=start,
        membership_end=end,
        hard_exit=True,
        opt2_h_base=0.5,
        capital_mode="sleeve_dollars",
        target_gross_by_date={start: 1000.0},
        force_rebal_dates=[start],
    )
    assert bt.index.min() == start
    assert bt.index.max() == end
    assert bt.iloc[0]["rebalance_reason"] == "enter_membership"
    assert bt.iloc[-1]["rebalance_reason"] == "hard_exit"
    assert float(bt.iloc[-1]["a_shares"]) == 0.0
    assert float(bt.iloc[-1]["b_shares"]) == 0.0
    # Exit day return must not explode (cover accounting, not +short notionals)
    assert abs(float(bt.iloc[-1]["ret"])) < 0.5
    assert all(bt.loc[bt["rebalance_reason"] == "cadence_resize"].index < end)


def test_sleeve_dollars_pins_gross_and_forces_cadence():
    idx = pd.bdate_range("2024-01-02", periods=15)
    px = pd.DataFrame(
        {"a_px": np.linspace(50, 45, len(idx)), "b_px": np.linspace(100, 105, len(idx))},
        index=idx,
    )
    h = pd.Series(0.45, index=idx)
    enter, mid = idx[0], idx[7]
    bt = run_bucket4_backtest_dynamic_h(
        px,
        h,
        pd.DatetimeIndex([enter, mid]),
        initial_capital=2000.0,
        beta_a=-2.0,
        capital_mode="sleeve_dollars",
        target_gross_by_date={enter: 2000.0, mid: 5000.0},
        h_target_by_date={enter: 0.45, mid: 0.60},
        force_rebal_dates=[enter, mid],
        drift_threshold_share_of_gross=0.99,  # would skip mid without force
        opt2_h_base=0.45,
    )
    assert bt.loc[mid, "rebalance_reason"] == "cadence_resize"
    assert float(bt.loc[enter, "gross_exposure"]) == pytest.approx(2000.0, rel=0.02)
    assert float(bt.loc[mid, "gross_exposure"]) == pytest.approx(5000.0, rel=0.02)
    assert float(bt.loc[mid, "h_target"]) == pytest.approx(0.60, abs=1e-6)
    # NAV seed ≈ enter gross → ~1× leverage; daily |ret| should stay moderate
    assert float(bt["ret"].abs().max()) < 0.5


def test_snap_dates_to_calendar_next_session():
    snapped, mapping = snap_dates_to_calendar(
        ["2026-05-26", "2026-06-10"],
        ["2026-05-22", "2026-05-27", "2026-06-10"],
    )
    assert mapping["2026-05-26"] == "2026-05-27"
    assert mapping["2026-06-10"] == "2026-06-10"
    assert snapped == ["2026-05-27", "2026-06-10"]


def test_inject_production_calendar_interpolates_hole():
    idx = pd.to_datetime(["2026-05-22", "2026-05-27"])
    px = pd.DataFrame({"a_px": [3.51, 4.02], "b_px": [29.4, 27.48]}, index=idx)
    out = inject_production_calendar_into_panel(px, ["2026-05-26"])
    assert pd.Timestamp("2026-05-26") in out.index
    # 4/5 of the way from Fri→Wed across the long weekend span
    assert float(out.loc["2026-05-26", "a_px"]) == pytest.approx(3.918, abs=0.02)


def test_compare_layer_a_snaps_prod_cadence_dates():
    prod = {
        "dates": ["2026-05-22", "2026-05-26", "2026-05-27"],
        "etf_usd": [-100.0, -200.0, -200.0],
        "underlying_usd": [-180.0, -400.0, -400.0],
        "h_used": [0.9, 1.0, 1.0],
        "ret": [0.0, 0.01, 0.0],
        "equity": [1.0, 1.01, 1.01],
        "gross_exposure_dollars": [280.0, 600.0, 600.0],
        "rebalance": [1, 1, 0],
        "rebalance_reason": ["enter_membership", "cadence_resize", ""],
    }
    twin = {
        "dates": ["2026-05-22", "2026-05-27"],
        "etf_gross": [100.0, 200.0],
        "underlying_gross": [180.0, 400.0],
        "h_realized": [0.9, 1.0],
        "h_target": [0.9, 1.0],
        "h_used": [0.9, 1.0],
        "ret": [0.0, 0.0],
        "equity": [1.0, 1.0],
        "rebalance": [1, 1],
        "rebalance_reason": ["enter_membership", "cadence_resize"],
    }
    out = compare_layer_a(
        prod,
        twin,
        beta_abs=2.0,
        etf="QBTZ",
        isolation_mode=True,
        prod_date_snap={"2026-05-26": "2026-05-27"},
    )
    assert out["reasons"]["cadence_resize"]["jaccard"] == 1.0
    assert out["reasons"]["cadence_resize"]["both"] == ["2026-05-27"]


def test_membership_bounds_from_production_daily():
    daily = {
        "dates": ["2026-03-06", "2026-03-07", "2026-04-20"],
        "rebalance": [1, 0, 1],
        "rebalance_reason": ["enter_membership", "", "hard_exit"],
    }
    b = membership_bounds_from_production(daily)
    assert b["membership_start"] == "2026-03-06"
    assert b["membership_end"] == "2026-04-20"
    assert b["hard_exit"] is True


def test_membership_bounds_ignore_hard_exit_isolation():
    daily = {
        "dates": ["2026-03-06", "2026-03-07", "2026-04-20"],
        "rebalance": [1, 0, 1],
        "rebalance_reason": ["enter_membership", "", "hard_exit"],
    }
    b = membership_bounds_from_production(
        daily,
        ignore_hard_exit=True,
        isolation_end="2026-07-17",
    )
    assert b["membership_start"] == "2026-03-06"
    assert b["membership_end"] == "2026-07-17"
    assert b["hard_exit"] is False
    assert b["ignored_hard_exit"] is True
    assert b["isolation_mode"] is True


def test_compare_layer_a_reason_jaccard():
    prod = {
        "dates": ["2026-03-06", "2026-03-07", "2026-04-20"],
        "etf_usd": [-100.0, -110.0, 0.0],
        "underlying_usd": [-400.0, -420.0, 0.0],
        "h_used": [2.0, 1.909, None],
        "ret": [0.0, 0.01, -0.02],
        "equity": [1.0, 1.01, 0.99],
        "rebalance": [1, 0, 1],
        "rebalance_reason": ["enter_membership", "", "hard_exit"],
    }
    twin = {
        "dates": ["2026-03-06", "2026-03-07", "2026-04-20"],
        "etf_gross": [100.0, 110.0, 0.0],
        "underlying_gross": [400.0, 420.0, 0.0],
        "h_realized": [2.0, 1.909, float("nan")],
        "h_target": [0.45, 0.45, 0.45],
        "h_used": [0.45, 0.45, 0.45],
        "ret": [0.0, 0.01, -0.02],
        "equity": [1.0, 1.01, 0.99],
        "rebalance": [1, 0, 1],
        "rebalance_reason": ["enter_membership", "", "hard_exit"],
    }
    out = compare_layer_a(prod, twin, beta_abs=2.0, etf="APLZ")
    assert out["gates"]["enter_ok"] is True
    assert out["gates"]["hard_exit_ok"] is True
    assert reason_dates(twin, "enter_membership") == {"2026-03-06"}

    # Isolation: hard_exit gate waived; exit day excluded from overlap scoring
    twin_iso = {
        **twin,
        "rebalance": [1, 0, 0],
        "rebalance_reason": ["enter_membership", "", ""],
        "gross_exposure": [1.0, 1.0, 1.0],
    }
    prod_iso = {
        **prod,
        "gross_exposure_dollars": [8000.0, 7900.0, 0.0],
    }
    out_iso = compare_layer_a(prod_iso, twin_iso, beta_abs=2.0, etf="APLZ", isolation_mode=True)
    assert out_iso["gates"]["hard_exit_ok"] is True
    assert out_iso["overlap_end"] == "2026-03-07"

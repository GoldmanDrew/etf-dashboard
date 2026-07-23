from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from bucket4.bucket4_vol_shape_signals import get_pair_signal  # noqa: E402
from build_b4_inception_research import _effective_research_start  # noqa: E402
from build_bucket4_backtest import _pair_daily_payload  # noqa: E402


def test_signal_recomputes_expanding_vcr_median_and_shifts_one_day():
    idx = pd.bdate_range("2026-01-02", periods=5)
    history = {
        "TEST": pd.DataFrame(
            {
                "tr": [1.0] * 5,
                "tr_est": [1.0] * 5,
                "cadence_score": [1.0] * 5,
                "vcr": [0.10, 0.30, 0.20, 0.80, 0.40],
                # Deliberately leaked/full-sample baseline: must be ignored.
                "vcr_med": [99.0] * 5,
                "rv_daily": [0.2] * 5,
                "rv_weekly": [0.2] * 5,
            },
            index=idx,
        )
    }
    sig = get_pair_signal("TEST", "UND", idx, history=history, lookahead_shift=1)

    assert pd.isna(sig.iloc[0]["vcr_med"])
    assert sig.iloc[1]["vcr_med"] == pytest.approx(0.10)
    assert sig.iloc[2]["vcr_med"] == pytest.approx(0.20)
    assert sig.iloc[4]["vcr_med"] == pytest.approx(0.25)


def test_effective_research_start_from_inception_by_default():
    idx = pd.bdate_range("2025-01-02", periods=100)
    start = _effective_research_start(
        idx,
        start_floor="1900-01-01",
        warmup_bdays=60,
        first_borrow_date=idx[75],
        require_pit_borrow=True,
        trade_from_inception=True,
    )
    assert start == idx[0]


def test_effective_research_start_legacy_warmup_and_pit_borrow():
    idx = pd.bdate_range("2025-01-02", periods=100)
    start = _effective_research_start(
        idx,
        start_floor="1900-01-01",
        warmup_bdays=60,
        first_borrow_date=idx[75],
        require_pit_borrow=True,
        trade_from_inception=False,
    )
    assert start == idx[75]
    assert _effective_research_start(
        idx,
        start_floor="1900-01-01",
        warmup_bdays=60,
        first_borrow_date=None,
        require_pit_borrow=True,
        trade_from_inception=False,
    ) is None


def test_pair_payload_does_not_count_slippage_twice():
    idx = pd.bdate_range("2026-01-02", periods=2)
    bt = pd.DataFrame(
        {
            "a_shares": [-0.01, -0.01],
            "a_px": [50.0, 49.0],
            "b_shares": [-0.005, -0.005],
            "b_px": [100.0, 101.0],
            "ret": [0.0, 0.01],
            "equity": [1.0, 1.01],
            "drawdown": [0.0, 0.0],
            "h_used": [0.5, 0.5],
            "h_target": [0.5, 0.5],
            "h_realized": [0.5, 0.5],
            "rebalance": [True, False],
            "rebalance_scheduled": [True, False],
            "rebalance_reason": ["entry", ""],
            "borrow_cost": [0.0, 0.0],
            "financing_pnl": [0.0, 0.0],
            # All-in cost = commission + slippage.
            "rebalance_fee": [0.003, 0.0],
            "slippage_cost": [0.002, 0.0],
        },
        index=idx,
    )
    payload = _pair_daily_payload(bt)
    assert payload["tcost_cum"][-1] == pytest.approx(0.003)

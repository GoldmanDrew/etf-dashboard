"""Unit tests for B4 production-parity helpers (policy, smoothing, PIT borrow)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from bucket4.ls_algo_sizing import (  # noqa: E402
    opt2_cfg_from_policy,
    smooth_pair_weights_trim_only,
)
from bucket4.pit_inputs import (  # noqa: E402
    apply_pit_borrow_to_universe,
    borrow_asof,
    load_borrow_history,
)
from bucket4.bucket4_dynamic_bt import run_bucket4_backtest_dynamic_h  # noqa: E402


def test_policy_crash_rho_matches_production():
    policy = yaml.safe_load((REPO / "config" / "bucket4_backtest_policy.yml").read_text(encoding="utf-8"))
    opt2 = opt2_cfg_from_policy(policy)
    assert float(opt2["crash_budget"]["rho"]) == pytest.approx(0.0075)
    assert bool(opt2["crash_budget"]["scale_to_budget"]) is True
    assert float(opt2["borrow_ramp_lo"]) == pytest.approx(0.80)
    assert float(opt2["borrow_ramp_hi"]) == pytest.approx(1.20)
    assert bool(opt2["weight_smoothing"]["enabled"]) is True
    assert float(opt2["weight_smoothing"]["alpha"]) == pytest.approx(0.5)
    assert float(opt2["drift_threshold_share_of_gross"]) == pytest.approx(0.02)


def test_opt2_cfg_default_rho_not_legacy():
    cfg = opt2_cfg_from_policy({"inverse_decay_bucket4": {"rules": {"bucket4_weekly_opt2": {}}}})
    assert float(cfg["crash_budget"]["rho"]) == pytest.approx(0.0075)
    assert bool(cfg["crash_budget"]["scale_to_budget"]) is True


def test_smooth_trim_only_cuts_immediate_raises_ema():
    prev = {("A", "U"): 0.20}
    # Cut: take solved immediately
    cut = smooth_pair_weights_trim_only({("A", "U"): 0.10}, prev, alpha=0.5)
    assert cut[("A", "U")] == pytest.approx(0.10)
    # Raise: EMA
    up = smooth_pair_weights_trim_only({("A", "U"): 0.30}, prev, alpha=0.5)
    assert up[("A", "U")] == pytest.approx(0.25)


def test_pit_borrow_asof_and_gate(tmp_path: Path):
    hist_path = tmp_path / "borrow_history.json"
    hist_path.write_text(
        json.dumps(
            {
                "symbols": {
                    "FOO": [
                        {"date": "2024-01-05", "borrow_current": 0.10},
                        {"date": "2024-02-01", "borrow_current": 1.50},
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    hist = load_borrow_history(hist_path)
    assert borrow_asof(hist, "FOO", "2024-01-10") == pytest.approx(0.10)
    assert borrow_asof(hist, "FOO", "2024-02-10") == pytest.approx(1.50)
    uni = pd.DataFrame(
        {
            "ETF": ["FOO", "BAR"],
            "Underlying": ["X", "Y"],
            "borrow_current": [0.05, 0.05],
        }
    )
    gated = apply_pit_borrow_to_universe(uni, hist, "2024-02-10", max_borrow=1.20)
    assert list(gated["ETF"]) == ["BAR"]


def test_dynamic_bt_uses_borrow_series_and_drift():
    idx = pd.bdate_range("2024-01-02", periods=40)
    px = pd.DataFrame(
        {
            "a_px": 50 * np.cumprod(1 + np.full(len(idx), -0.001)),
            "b_px": 100 * np.cumprod(1 + np.full(len(idx), 0.0005)),
        },
        index=idx,
    )
    h = pd.Series(0.45, index=idx)
    # Rebalance every 5 days
    rb = idx[::5]
    borrow = pd.Series(0.50, index=idx)
    borrow.iloc[20:] = 2.0
    bt = run_bucket4_backtest_dynamic_h(
        px,
        h,
        rb,
        initial_capital=1.0,
        beta_a=-2.0,
        borrow_a_annual=0.10,
        borrow_a_series=borrow,
        fee_bps=1.0,
        slippage_bps=0.0,
        drift_threshold_share_of_gross=0.50,  # high → skip most
        force_rebalance_after_days=21,
        opt2_h_base=0.45,
    )
    assert "rebalance_skipped_below_drift" in bt.columns
    assert float(bt["borrow_cost"].sum()) > 0

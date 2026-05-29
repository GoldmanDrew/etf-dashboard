"""Unified YieldBOOST forward MC stamps expected_gross_decay from pair MC."""

from __future__ import annotations

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
SCRIPTS = os.path.join(REPO_ROOT, "scripts")
if SCRIPTS not in sys.path:
    sys.path.insert(0, SCRIPTS)

import income_schedule as ic  # noqa: E402
from build_data import _stamp_unified_yb_forward  # noqa: E402


def test_stamp_unified_yb_forward_mirrors_quantiles():
    pair = ic.simulate_weekly_compound_pair_pnl(
        sigma_annual=0.674,
        beta=0.372,
        capture_ratio=0.80,
        borrow_annual=0.05,
        n_paths=2000,
        seed=ic.stable_seed_from_symbol("MTYY"),
    )
    assert pair is not None
    rec: dict = {}
    _stamp_unified_yb_forward(
        rec,
        pair,
        sigma_forecast=0.674,
        capture_ratio=0.80,
        n_paths=2000,
    )
    assert rec["expected_pair_pnl_p50_annual"] == rec["expected_gross_decay_p50_annual"]
    assert rec["expected_pair_pnl_p10_annual"] == rec["expected_gross_decay_p10_annual"]
    assert rec["expected_gross_decay_dist_model"] == "yieldboost_weekly_compound_mc"
    assert rec["expected_pair_pnl_std_annual"] == round(float(pair["std_log"]), 6)

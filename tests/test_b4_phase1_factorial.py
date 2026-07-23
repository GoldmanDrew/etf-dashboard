from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from analyze_b4_phase1_factorial import _sign_test_pvalue, _trimmed_mean  # noqa: E402
from research_b4_phase1_factorial import _perf, _variant_policy  # noqa: E402


def _policy() -> dict:
    return {
        "inverse_decay_bucket4": {
            "rules": {
                "bucket4_weekly_opt2": {
                    "hedge_cadence_policy": {
                        "h_mid": 0.45, "k_vcr": 1.0, "h_min": 0.30, "h_max": 0.80,
                        "alpha": 0.25, "k_z": 0.20, "base_days": 14.0, "k_tr": -1.0,
                        "m_vcr": 2.5, "min_interval": 1, "max_interval": 21,
                    }
                }
            }
        }
    }


def test_variant_policy_is_pure_and_sets_exact_factorial_cell():
    base = _policy()
    out = _variant_policy(base, 0.75, 5)
    block = out["inverse_decay_bucket4"]["rules"]["bucket4_weekly_opt2"]["hedge_cadence_policy"]
    assert block["h_min"] == block["h_mid"] == block["h_max"] == pytest.approx(0.75)
    assert block["k_vcr"] == 0
    assert block["base_days"] == block["min_interval"] == block["max_interval"] == 5
    assert base["inverse_decay_bucket4"]["rules"]["bucket4_weekly_opt2"]["hedge_cadence_policy"]["k_vcr"] == 1.0


def test_perf_uses_unclipped_returns_and_reports_tail():
    out = _perf(pd.Series([0.0, 0.10, -0.20, 0.05]))
    assert out["cumulative_return"] == pytest.approx((1.1 * 0.8 * 1.05) - 1)
    assert out["max_drawdown"] == pytest.approx(-0.20)
    assert out["expected_shortfall_95_daily"] == pytest.approx(-0.20)


def test_robust_helpers():
    assert _trimmed_mean(pd.Series([-100.0, 1.0, 2.0, 3.0, 100.0]), 0.20) == pytest.approx(2.0)
    assert _sign_test_pvalue(18, 24) < 0.05

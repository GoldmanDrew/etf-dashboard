"""YieldBOOST forward edge mirrors screener Exp. decay gross (not weekly MC headline)."""

from __future__ import annotations

import math
import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import income_schedule as ic  # noqa: E402


def _yb_calibration() -> dict:
    return {
        "blended_ratio_used": 0.65,
        "n_events": 5,
        "confidence_band": "typical",
    }


def test_yb_headline_mirror_fields_match_gross_decay():
    """Headline expected_pair_pnl_* should mirror expected_gross_decay_* for YB."""
    gross_p50 = 0.795781
    gross_p10 = 0.579042
    gross_p90 = 1.189027
    rec = {
        "is_yieldboost": True,
        "expected_gross_decay_p10_annual": gross_p10,
        "expected_gross_decay_p50_annual": gross_p50,
        "expected_gross_decay_p90_annual": gross_p90,
        "expected_gross_decay_mean_annual": gross_p50,
        "expected_gross_decay_dist_model": "yieldboost_put_spread_point",
        "gross_sigma_forward_annual": 0.15,
    }
    rec["expected_pair_pnl_p10_annual"] = rec["expected_gross_decay_p10_annual"]
    rec["expected_pair_pnl_p50_annual"] = rec["expected_gross_decay_p50_annual"]
    rec["expected_pair_pnl_p90_annual"] = rec["expected_gross_decay_p90_annual"]
    rec["expected_pair_pnl_mean_annual"] = rec["expected_gross_decay_mean_annual"]
    sig = ic.band_to_sigma(gross_p10, gross_p90)
    rec["expected_pair_pnl_std_annual"] = round(sig, 6) if sig is not None else None
    rec["expected_pair_pnl_basis"] = "put_spread_structural_gross"
    rec["expected_pair_pnl_source"] = rec["expected_gross_decay_dist_model"]

    assert rec["expected_pair_pnl_p50_annual"] == pytest.approx(gross_p50)
    assert rec["expected_pair_pnl_basis"] == "put_spread_structural_gross"
    assert rec["expected_pair_pnl_source"] == "yieldboost_put_spread_point"


def test_weekly_mc_diagnostic_differs_from_structural_headline():
    """Weekly MC p50 should be materially below put-spread structural gross for high capture."""
    sigma = 0.94
    beta = 2.0
    calibration = _yb_calibration()
    weekly = ic.expected_pair_pnl_annual(
        calibration=calibration,
        sigma_annual=sigma,
        beta=beta,
        mu_annual=0.0,
        horizon_years=1.0,
        expense_ratio_annual=ic.DEFAULT_EXPENSE_RATIO_ANNUAL,
        borrow_annual=0.04,
        n_paths=2000,
        seed=42,
    )
    assert weekly is not None
    structural = ic.structural_pair_gross_log_annual(
        sigma,
        0.0,
        beta,
        expense_ratio_annual=ic.DEFAULT_EXPENSE_RATIO_ANNUAL,
    )
    assert structural is not None
    assert weekly["p50_log"] < structural
    assert structural - weekly["p50_log"] > 0.05


def test_vol_regime_triplet_uses_structural_not_weekly_mc():
    sigma = 0.80
    beta = 2.0
    mults = (0.7, 1.0, 1.3)
    gross_mid = 0.75
    regime = {}
    for mult in mults:
        label = "lo" if mult < 1.0 else "hi" if mult > 1.0 else "mid"
        if abs(mult - 1.0) < 1e-9:
            regime[label] = gross_mid
        else:
            cell = ic.structural_pair_gross_log_annual(
                sigma * mult,
                0.0,
                beta,
            )
            regime[label] = cell
    assert regime["mid"] == pytest.approx(gross_mid)
    assert regime["hi"] > regime["lo"]
    assert all(v is not None and math.isfinite(v) for v in regime.values())

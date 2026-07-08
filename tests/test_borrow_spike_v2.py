#!/usr/bin/env python3
"""Tests for logistic_v2, calibration, and tracking."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from borrow_spike_v2 import (  # noqa: E402
    alert_tier,
    fit_isotonic_calibrator,
    fit_logistic_v2,
    score_rows_v2,
)


def _synthetic_panel(n: int = 400, pos_rate: float = 0.08) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    y = (rng.random(n) < pos_rate).astype(float)
    df = pd.DataFrame({
        "spike_event": y,
        "borrow_current": rng.uniform(0.01, 0.15, n),
        "borrow_z60": rng.normal(0, 1, n),
        "borrow_slope5": rng.normal(0, 0.01, n),
        "borrow_vol10": rng.uniform(0, 0.02, n),
        "borrow_pctile_60": rng.uniform(0, 1, n),
        "shares_drop1": rng.uniform(0, 0.2, n),
        "shares_drop3": rng.uniform(0, 0.3, n),
        "shares_drop5": rng.uniform(0, 0.4, n),
        "near_zero_shares": (rng.random(n) < 0.05).astype(float),
        "utilization_proxy": rng.uniform(0, 0.9, n),
        "avail_to_adv": rng.uniform(0, 2, n),
        "log_aum": rng.uniform(10, 20, n),
        "turnover_20d": rng.uniform(0, 0.5, n),
    })
    return df


def test_fit_logistic_v2_produces_probs():
    df = _synthetic_panel()
    model = fit_logistic_v2(df)
    assert model is not None
    p = score_rows_v2(model, df.head(20))
    assert len(p) == 20
    assert np.all((p >= 0) & (p <= 1))


def test_isotonic_calibrator_monotone():
    ps = np.linspace(0.01, 0.5, 200)
    ys = (ps > 0.25).astype(float)
    ys[::17] = 1.0
    cal = fit_isotonic_calibrator(ps, ys)
    assert cal is not None
    out = [cal.transform(float(x)) for x in ps]
    for i in range(1, len(out)):
        assert out[i] >= out[i - 1] - 1e-9


def test_alert_tier_thresholds():
    assert alert_tier(0.01) == "low"
    assert alert_tier(0.06) == "watch"
    assert alert_tier(0.15) == "elevated"
    assert alert_tier(0.30) == "high"

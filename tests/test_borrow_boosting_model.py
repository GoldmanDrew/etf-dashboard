"""Tests for borrow boosting model (synthetic panel, no leakage)."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from borrow_boosting_model import (  # noqa: E402
    _backend_name,
    evaluate_drift_replay,
    fit_boosting_bundle,
    score_drift,
    score_spike,
    walk_forward_replay_boosting,
)
from borrow_model_common import drift_metrics, shrink_delta  # noqa: E402


def _synthetic_panel(n_dates: int = 40, n_syms: int = 8) -> pd.DataFrame:
    rows = []
    dates = pd.date_range("2026-01-01", periods=n_dates, freq="D")
    rng = np.random.default_rng(7)
    for sym_i in range(n_syms):
        sym = f"SYM{sym_i}"
        borrow = 0.05 + 0.01 * sym_i
        for d in dates:
            borrow += rng.normal(0, 0.002)
            rows.append(
                {
                    "date": d,
                    "symbol": sym,
                    "borrow_current": borrow,
                    "borrow_z60": borrow - 0.05,
                    "borrow_slope5": 0.001,
                    "borrow_vol10": 0.01,
                    "borrow_pctile_60": 0.5,
                    "shares_available": 100000,
                    "shares_drop3": 0.0,
                    "utilization_proxy": 0.2,
                    "log_aum": 10.0,
                    "delta": 2.0,
                    "obs_count": 40,
                }
            )
    df = pd.DataFrame(rows)
    df = df.sort_values(["symbol", "date"])
    df["delta_borrow_5"] = df.groupby("symbol")["borrow_current"].shift(-5) - df["borrow_current"]
    df["spike_event"] = (df["delta_borrow_5"] > 0.02).astype(float)
    df["y_spike_5"] = df["spike_event"]
    return df.dropna(subset=["delta_borrow_5"]).reset_index(drop=True)


def test_shrink_delta_thins_history():
    assert shrink_delta(0.10, 30) < 0.10
    assert shrink_delta(0.10, 60) == pytest.approx(0.10)


def test_drift_metrics_basic():
    y = np.linspace(-0.05, 0.1, 12)
    p = y + 0.01
    m = drift_metrics(y, p)
    assert m["mae"] is not None
    assert m["n"] == 12


@pytest.mark.skipif(_backend_name() == "unavailable", reason="no lightgbm/sklearn")
def test_fit_boosting_bundle_smoke():
    panel = _synthetic_panel()
    bundle = fit_boosting_bundle(panel)
    assert bundle is not None
    drift = score_drift(bundle, panel.head(5))
    spike = score_spike(bundle, panel.head(5))
    assert len(drift) == 5
    assert len(spike) == 5
    assert np.all((spike >= 0) & (spike <= 1))


@pytest.mark.skipif(_backend_name() == "unavailable", reason="no lightgbm/sklearn")
def test_walk_forward_replay_runs():
    panel = _synthetic_panel(n_dates=50, n_syms=10)
    spike_replay, drift_replay = walk_forward_replay_boosting(panel, min_train_rows=80)
    assert not spike_replay.empty or not drift_replay.empty


def test_evaluate_drift_replay_empty():
    assert evaluate_drift_replay(pd.DataFrame())["status"] == "empty"

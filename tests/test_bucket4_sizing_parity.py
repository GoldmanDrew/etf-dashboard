"""Tests for production B4 sizing bridge + cash-aware portfolio returns."""
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

from bucket4.ls_algo_sizing import (  # noqa: E402
    expand_weights_to_calendar,
    find_ls_algo,
    port_returns_with_cash,
)
from build_bucket4_backtest import build_backtest, port_returns  # noqa: E402


def test_port_returns_preserves_cash_residual():
    idx = pd.bdate_range("2024-01-02", periods=5)
    ret = pd.DataFrame({"A": 0.01, "B": -0.01}, index=idx)
    w = pd.Series({"A": 0.10, "B": 0.05})  # 15% deployed
    pr = port_returns(ret, w, renormalize=False)
    # Full-invested would be 0.01*2/3 + (-0.01)*1/3 ≈ 0.00333; cash path is 0.001
    assert pr.iloc[0] == pytest.approx(0.10 * 0.01 + 0.05 * (-0.01))


def test_port_returns_with_cash_matrix():
    idx = pd.bdate_range("2024-01-02", periods=4)
    ret = pd.DataFrame({"A": [0.02, 0.0, -0.01, 0.01], "B": [0.0, 0.01, 0.0, -0.02]}, index=idx)
    w = pd.DataFrame({"A": [0.2, 0.2, 0.1, 0.1], "B": [0.0, 0.1, 0.1, 0.05]}, index=idx)
    pr = port_returns_with_cash(ret, w)
    assert pr.iloc[0] == pytest.approx(0.2 * 0.02)
    assert pr.iloc[1] == pytest.approx(0.2 * 0.0 + 0.1 * 0.01)


def test_expand_weights_ffill():
    fri = pd.DatetimeIndex(["2024-01-05", "2024-01-12"])
    wdf = pd.DataFrame({"A": [0.2, 0.1]}, index=fri)
    cal = pd.bdate_range("2024-01-05", "2024-01-16")
    out = expand_weights_to_calendar(wdf, cal)
    assert out.loc["2024-01-08", "A"] == pytest.approx(0.2)
    assert out.loc["2024-01-15", "A"] == pytest.approx(0.1)


def test_find_ls_algo_local():
    ls = find_ls_algo()
    if ls is None:
        pytest.skip("ls-algo not checked out locally")
    assert (ls / "scripts" / "bucket4_backtest_api.py").is_file()


def test_builder_legacy_concentration_still_works():
    idx = pd.date_range("2023-01-03", periods=280, freq="B")
    rng = np.random.default_rng(7)
    und = 100 * np.cumprod(1 + rng.normal(0.0005, 0.01, len(idx)))
    etf = 50 * np.cumprod(1 + rng.normal(-0.0003, 0.02, len(idx)))
    px = pd.DataFrame({"a_px": etf, "b_px": und}, index=idx)
    panel = {"INV1": px, "INV2": px * 0.98}
    uni = pd.DataFrame(
        {
            "ETF": ["INV1", "INV2"],
            "Underlying": ["UND1", "UND2"],
            "bucket4_net_edge_annual": [0.50, 0.40],
            "borrow_current": [0.05, 0.08],
            "Delta": [-2.0, -2.0],
            "vol_underlying_annual": [0.60, 0.55],
            "production_candidate": [True, True],
            "gate_reason": ["", ""],
        }
    )
    policy = {
        "inverse_decay_bucket4": {
            "rules": {
                "bucket4_weekly_opt2": {
                    "fee_bps": 1.0,
                    "slippage_bps": 20.0,
                    "hedge_cadence_policy": {"h_mid": 0.45, "k_vcr": 1.0, "h_min": 0.3, "h_max": 0.8, "alpha": 0.0,
                                            "base_days": 14, "k_tr": 0.0, "m_vcr": 0.0, "min_interval": 5, "max_interval": 21,
                                            "cadence_signal_col": "tr"},
                },
                "ratchet": {"enabled": False},
                "concentration": {"enabled": False},
            }
        },
        "backtest": {"initial_capital": 1.0, "walk_forward": False},
    }
    built = build_backtest(
        uni,
        panel,
        policy,
        start="2023-06-01",
        min_days=40,
        warmup_bdays=10,
        signal_window=20,
        vol_history={},
        legacy_concentration=True,
    )
    assert built is not None
    assert built["sizing_method"] == "legacy_concentration"
    assert built["n_pairs"] >= 1
    assert abs(sum(built["default_weights"].values()) - 1.0) < 1e-6 or built["deployed_fraction"] > 0


@pytest.mark.skipif(find_ls_algo() is None, reason="ls-algo not available")
def test_builder_production_sizing_smoke():
    idx = pd.date_range("2023-01-03", periods=400, freq="B")
    rng = np.random.default_rng(11)
    panel = {}
    rows = []
    for i, (etf, und) in enumerate(
        [("INV1", "UND1"), ("INV2", "UND2"), ("INV3", "UND3"), ("INV4", "UND4"), ("INV5", "UND5")]
    ):
        und_px = 100 * np.cumprod(1 + rng.normal(0.0005, 0.02, len(idx)))
        und_px[-252:] = und_px[-252] * np.linspace(1.0, 2.5, 252)
        etf_px = 50 * np.cumprod(1 + rng.normal(-0.0004, 0.03, len(idx)))
        panel[etf] = pd.DataFrame({"a_px": etf_px, "b_px": und_px}, index=idx)
        rows.append(
            {
                "ETF": etf,
                "Underlying": und,
                "bucket4_net_edge_annual": 0.55 - 0.03 * i,
                "borrow_current": 0.05 + 0.01 * i,
                "Delta": -2.0,
                "vol_underlying_annual": 0.60,
                "production_candidate": True,
                "gate_reason": "",
            }
        )
    uni = pd.DataFrame(rows)
    # Write a temp screener for opt2 decay loader.
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        csv_path = Path(td) / "etf_screened_today.csv"
        sc = uni.copy()
        sc["net_edge_p50_annual"] = sc["bucket4_net_edge_annual"]
        sc.to_csv(csv_path, index=False)
        policy = {
            "inverse_decay_bucket4": {
                "rules": {
                    "bucket4_weekly_opt2": {
                        "fee_bps": 1.0,
                        "slippage_bps": 20.0,
                        "pf_min_pairs": 5,
                        "decay_borrow_quad": 0,
                        "borrow_linear_aversion": 1.5,
                        "borrow_uncertainty_penalty": 0.0,
                        "borrow_aversion_source": "spot",
                        "cov_penalty": 0.0,
                        "crash_budget": {
                            "enabled": True,
                            "rho": 0.0075,
                            # Research regime: freed dollars stay in cash (the
                            # production default is scale_to_budget=true).
                            "scale_to_budget": False,
                            "theta": 0.5,
                            "phi": 0.5,
                            "l_floor": 0.02,
                            "missing_policy": "book_quantile",
                            "missing_l_quantile": 0.75,
                        },
                        "hedge_cadence_policy": {
                            "h_mid": 0.45, "k_vcr": 1.0, "h_min": 0.3, "h_max": 0.8, "alpha": 0.0,
                            "base_days": 14, "k_tr": 0.0, "m_vcr": 0.0, "min_interval": 5, "max_interval": 21,
                            "cadence_signal_col": "tr",
                        },
                    },
                    "ratchet": {"enabled": False},
                }
            },
            "backtest": {
                "initial_capital": 1.0,
                "walk_forward": False,
                "sleeve_budget_usd": 100000.0,
            },
        }
        built = build_backtest(
            uni,
            panel,
            policy,
            start="2024-01-01",
            min_days=60,
            warmup_bdays=20,
            signal_window=20,
            vol_history={},
            screened_csv=csv_path,
            legacy_concentration=False,
        )
    assert built is not None
    assert built["sizing_method"] == "v6_opt2_crash_budget"
    assert built["cash_residual"] > 0.05
    assert sum(built["default_weights"].values()) <= 1.0 + 1e-6
    assert any(p.get("crash_budget_mult") is not None for p in built["pairs"])

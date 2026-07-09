"""Unit tests for Bucket 4 hedge cadence and backtest builder."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from bucket4.b4_ratchet_overlay import RatchetConfig, apply_inverse_ratchet  # noqa: E402
from bucket4.bucket4_hedge_cadence import (  # noqa: E402
    HedgeCadenceKnobs,
    build_h_series,
    build_rebal_dates,
    compute_pair_policy,
)
from bucket4.bucket4_sizing import concentration_scores  # noqa: E402
from build_bucket4_backtest import build_backtest, load_universe, score_weights  # noqa: E402


def test_compute_pair_policy_v7_higher_vcr_raises_h():
    knobs = HedgeCadenceKnobs(h_mid=0.45, k_vcr=1.0, h_min=0.30, h_max=0.80, alpha=0.0)
    lo = compute_pair_policy(1.0, 0.20, 0.25, knobs=knobs)
    hi = compute_pair_policy(1.0, 0.35, 0.25, knobs=knobs)
    assert hi.h > lo.h
    assert lo.h == pytest.approx(0.40, abs=1e-4)
    assert hi.h == pytest.approx(0.55, abs=1e-4)


def test_cadence_contrarian_k_tr_slower_when_tr_est_high():
    knobs = HedgeCadenceKnobs(base_days=14.0, k_tr=-1.0, m_vcr=0.0, min_interval=1, max_interval=21)
    calm = compute_pair_policy(0.90, 0.25, 0.25, knobs=knobs, cadence_signal_col="tr_est")
    hot = compute_pair_policy(1.20, 0.25, 0.25, knobs=knobs, cadence_signal_col="tr_est")
    assert hot.interval_days >= calm.interval_days


def test_build_h_series_ema_smooths():
    idx = pd.date_range("2024-01-02", periods=5, freq="B")
    sig = pd.DataFrame(
        {"tr": [1.0] * 5, "vcr": [0.20, 0.22, 0.24, 0.26, 0.28], "vcr_med": [0.25] * 5},
        index=idx,
    )
    knobs = HedgeCadenceKnobs(h_mid=0.45, k_vcr=1.0, h_min=0.30, h_max=0.80, alpha=0.5)
    h = build_h_series(sig, idx, knobs=knobs)
    assert len(h) == 5
    assert h.iloc[-1] != h.iloc[0]


def test_build_rebal_dates_steps_by_interval():
    idx = pd.date_range("2024-01-02", periods=30, freq="B")
    sig = pd.DataFrame({"tr": [1.0] * 30, "vcr": [0.25] * 30, "vcr_med": [0.25] * 30}, index=idx)
    knobs = HedgeCadenceKnobs(base_days=10.0, k_tr=0.0, m_vcr=0.0, min_interval=5, max_interval=5)
    rb, diag = build_rebal_dates(sig, idx, knobs=knobs, warmup_bdays=0)
    assert len(rb) >= 5
    assert diag["interval_days"].iloc[0] == 5


def test_ratchet_grow_only_floor():
    cfg = RatchetConfig(enabled=True, trim_enabled=False)
    res = apply_inverse_ratchet(
        0.10,
        held_usd=0.20,
        persisted_floor_usd=0.15,
        cfg=cfg,
    )
    assert res.inv_short_usd == pytest.approx(0.20)
    assert res.binding is True


def test_concentration_scores_prefers_high_edge_low_borrow():
    df = pd.DataFrame(
        {
            "ETF": ["A", "B"],
            "bucket4_net_edge_annual": [0.50, 0.20],
            "borrow_current": [0.05, 0.05],
            "vol_underlying_annual": [0.50, 0.50],
        }
    )
    scores = concentration_scores(df)
    assert scores.iloc[0] > scores.iloc[1]


def _synthetic_panel() -> dict[str, pd.DataFrame]:
    idx = pd.date_range("2023-01-03", periods=280, freq="B")
    rng = np.random.default_rng(42)
    und = 100 * np.cumprod(1 + rng.normal(0.0005, 0.01, len(idx)))
    etf = 50 * np.cumprod(1 + rng.normal(-0.0003, 0.02, len(idx)))
    px = pd.DataFrame({"a_px": etf, "b_px": und}, index=idx)
    return {"INV1": px, "INV2": px * 0.98}


def test_builder_on_synthetic_fixture(tmp_path):
    screener = tmp_path / "screen.csv"
    screener.write_text(
        "ETF,Underlying,Delta,bucket,bucket4_net_edge_annual,borrow_current,"
        "vol_underlying_annual,inverse_shortable,purgatory,strategy_blacklisted\n"
        "INV1,UND1,-2.0,bucket_4,0.45,0.05,0.60,True,False,False\n"
        "INV2,UND2,-1.8,bucket_4,0.35,0.08,0.55,True,False,False\n",
        encoding="utf-8",
    )
    policy = {
        "bucket_4": {"screener_buckets": ["bucket_4"], "rules": {"exclude_purgatory": True}},
        "inverse_decay_bucket4": {
            "rules": {
                "min_net_edge_annual": 0.30,
                "min_underlying_vol": 0.40,
                "excluded_etfs": [],
                "concentration": {"enabled": False},
                "cluster_caps": {},
                "bucket4_weekly_opt2": {
                    "slippage_bps": 0.0,
                    "fee_bps": 0.0,
                    "hedge_cadence_policy": {
                        "h_mid": 0.45,
                        "k_vcr": 1.0,
                        "h_min": 0.30,
                        "h_max": 0.80,
                        "alpha": 0.0,
                        "cadence_signal_col": "tr",
                        "base_days": 10.0,
                        "k_tr": 0.0,
                        "m_vcr": 0.0,
                        "min_interval": 5,
                        "max_interval": 10,
                    },
                },
                "ratchet": {"enabled": False},
            }
        },
        "backtest": {"start": "2023-10-01", "min_days": 15, "warmup_bdays": 5, "signal_window": 20},
    }

    uni = load_universe(screener, policy)
    assert len(uni) == 2
    _, w = score_weights(uni, policy)
    assert w.sum() == pytest.approx(1.0)

    built = build_backtest(
        uni,
        _synthetic_panel(),
        policy,
        start="2023-10-01",
        min_days=15,
        warmup_bdays=5,
        signal_window=20,
        vol_history={},
    )
    assert built is not None
    assert built["n_pairs"] == 2
    assert built["n_obs"] > 10
    assert len(built["port_daily_returns"]) == built["n_obs"]
    assert "h_state" in built
    assert built["default_weights"]
    assert set(built["pair_series"]) == {"INV1", "INV2"}
    inv1 = built["pair_series"]["INV1"]
    assert inv1["schema"] == "bucket4_pair.v1"
    assert len(inv1["daily"]["dates"]) == len(inv1["daily"]["ret"])
    assert "summary" in inv1
    assert "rebalance_log" in inv1

    out = tmp_path / "out.json"
    out.write_text(json.dumps(built), encoding="utf-8")
    loaded = json.loads(out.read_text(encoding="utf-8"))
    assert loaded["n_pairs"] == 2

"""Tests for borrow predictor analysis."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import analyze_borrow_predictors as abp  # noqa: E402


def _synthetic_panel(n_dates: int = 40, n_syms: int = 6) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    dates = pd.date_range("2024-01-01", periods=n_dates, freq="D")
    rows = []
    for di, d in enumerate(dates):
        for s in range(n_syms):
            sym = f"S{s:02d}"
            borrow = 0.05 + 0.002 * di + 0.01 * s + rng.normal(0, 0.01)
            shares = max(1000, 500_000 - di * 1000 - s * 500)
            spike = 1.0 if (di + s) % 7 == 0 or borrow > 0.14 else 0.0
            borrow_z = borrow / 0.05 + (0.5 if spike > 0.5 else 0.0)
            rows.append(
                {
                    "date": d,
                    "symbol": sym,
                    "borrow_current": borrow,
                    "borrow_z60": borrow_z,
                    "borrow_slope5": rng.normal(0, 0.001),
                    "borrow_vol10": abs(rng.normal(0, 0.002)),
                    "borrow_pctile_60": min(1.0, borrow / 0.15),
                    "shares_available": shares,
                    "shares_drop1": 0.01,
                    "shares_drop3": 0.02,
                    "shares_drop5": 0.03,
                    "utilization_proxy": 0.4,
                    "avail_to_adv": 2.0,
                    "log_aum": 17.0,
                    "turnover_20d": 100_000.0,
                    "prem_disc_bps": 10.0,
                    "tradable_float_shares": 5_000_000.0,
                    "etf_aum_over_float": 0.01,
                    "rebalance_pct_adv": 0.02,
                    "peer_borrow_z_mean": 1.1,
                    "peer_shares_drop3_mean": 0.015,
                    "peer_shares_avail_sum": 2_000_000.0,
                    "delta": 2.0,
                    "leverage": 2.0,
                    "net_edge_p50": 0.08,
                    "gross_decay_annual": 0.12,
                    "forecast_vol_underlying_annual": 0.45,
                    "y_spike_5": spike,
                    "y_spike_1": spike,
                    "y_spike_3": spike,
                    "y_spike_10": spike,
                    "delta_borrow_5": rng.normal(0, 0.01),
                    "max_borrow_jump_5": abs(rng.normal(0, 0.02)),
                }
            )
    return pd.DataFrame(rows)


def test_cross_sectional_rank_corr():
    panel = _synthetic_panel()
    c = abp.cross_sectional_rank_corr(panel, "borrow_z60", "y_spike_5", min_names=3)
    assert c is not None
    assert -1.0 <= c <= 1.0


def test_univariate_r2():
    x = np.linspace(0, 1, 100)
    y = 2 * x + 0.1
    r2 = abp.univariate_r2(x, y)
    assert r2 is not None
    assert r2 > 0.9


def test_block_ablation_runs():
    panel = _synthetic_panel(n_dates=60, n_syms=10)
    assert panel["y_spike_5"].sum() >= 5
    result = abp.block_ablation(panel)
    assert result["status"] == "ok"
    assert "borrow_only" in result["blocks"]
    assert result["blocks"]["borrow_only"]["status"] == "ok"


def test_run_analysis_writes_summary_fields(tmp_path: Path):
    panel = _synthetic_panel()
    leadlag, importance, summary = abp.run_analysis(panel)
    assert "matrix" in leadlag
    assert "blocks" in importance
    assert "recommended_v2_features" in summary
    assert "top_predictors_by_horizon" in summary
    assert "guidance_for_spike_model" in summary
    abp.write_outputs(
        leadlag,
        importance,
        summary,
        leadlag_path=tmp_path / "leadlag.json",
        importance_path=tmp_path / "importance.json",
        summary_path=tmp_path / "summary.json",
    )
    loaded = json.loads((tmp_path / "summary.json").read_text(encoding="utf-8"))
    assert isinstance(loaded["recommended_v2_features"], list)
    assert len(loaded["recommended_v2_features"]) > 0


def test_load_panel_missing_returns_empty(tmp_path: Path):
    panel = abp._load_panel(tmp_path / "missing.parquet")
    assert panel.empty

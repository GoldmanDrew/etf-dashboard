"""Tests for intraday remaining-model backtest."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import backtest_intraday_remaining as bt  # noqa: E402


def test_pick_recommended_defaults_to_none_when_baseline_wins(tmp_path):
    overall = {
        "baseline_est": {"n": 10, "mae_dollars": 100.0},
        "vol_remaining": {"n": 10, "mae_dollars": 105.0},
        "time_remaining": {"n": 10, "mae_dollars": 110.0},
    }
    assert bt.pick_recommended(overall) == "none"


def test_pick_recommended_selects_better_model(tmp_path):
    overall = {
        "baseline_est": {"n": 10, "mae_dollars": 100.0},
        "time_remaining": {"n": 10, "mae_dollars": 90.0},
    }
    assert bt.pick_recommended(overall) == "time_remaining"


def test_run_backtest_on_synthetic_snapshot(tmp_path):
    snap_dir = tmp_path / "snaps"
    snap_dir.mkdir()
    daily = tmp_path / "daily.parquet"

    rows = []
    for d, est, real, vol_pct in [
        ("2026-05-20", 100.0, 100.0, 0.2),
        ("2026-05-21", 200.0, 150.0, 0.5),
    ]:
        snap = {
            "minutes_to_close": 10,
            "by_underlying": {
                "SPY": {
                    "estimated_net_close_rebalance_dollars": est,
                    "underlying_dollar_adv_20d": 1_000_000.0,
                    "volume_so_far_pct_adv": vol_pct,
                },
            },
        }
        (snap_dir / f"{d}.jsonl").write_text(json.dumps(snap) + "\n", encoding="utf-8")
        rows.append({
            "date": d,
            "ticker": "UPRO",
            "underlying": "SPY",
            "included_in_aggregate": True,
            "rebalance_signed_dollars": real,
        })
    pd.DataFrame(rows).to_parquet(daily, index=False)

    out = tmp_path / "backtest.json"
    payload = bt.run_backtest(
        snapshot_dir=snap_dir,
        daily_parquet=daily,
        output_json=out,
    )
    assert payload["n_observations"] == 2
    assert out.exists()
    assert "models_overall" in payload

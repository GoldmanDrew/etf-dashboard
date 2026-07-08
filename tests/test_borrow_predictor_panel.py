"""Tests for borrow predictor panel builder."""
from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import build_borrow_predictor_panel as bpp  # noqa: E402


def _synthetic_borrow_history(tmp_path: Path, n_days: int = 80) -> Path:
    start = date(2025, 1, 1)
    hist_a = []
    hist_b = []
    for i in range(n_days):
        d = (start + timedelta(days=i)).isoformat()
        borrow_a = 0.05 + 0.001 * i + (0.5 if i >= n_days - 6 else 0.0)
        shares_a = max(1000, 1_000_000 - i * 5000)
        hist_a.append({"date": d, "borrow_current": borrow_a, "shares_available": shares_a})
        hist_b.append(
            {
                "date": d,
                "borrow_current": 0.08 + 0.0005 * i,
                "shares_available": 500_000 - i * 1000,
            }
        )
    payload = {"symbols": {"AAA": hist_a, "BBB": hist_b}, "meta": {}}
    path = tmp_path / "borrow_history.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _synthetic_metrics(tmp_path: Path, symbols: list[str], n_days: int = 80) -> Path:
    start = date(2025, 1, 1)
    rows = []
    for sym in symbols:
        for i in range(n_days):
            d = (start + timedelta(days=i)).isoformat()
            rows.append(
                {
                    "date": d,
                    "ticker": sym,
                    "nav": 25.0,
                    "aum": 50_000_000 + i * 10_000,
                    "shares_outstanding": 2_000_000,
                    "close_price": 25.1,
                    "shares_traded": 100_000 + i * 100,
                }
            )
    path = tmp_path / "etf_metrics_daily.parquet"
    pd.DataFrame(rows).to_parquet(path, index=False)
    return path


def _synthetic_screener(tmp_path: Path) -> Path:
    df = pd.DataFrame(
        [
            {
                "ETF": "AAA",
                "Underlying": "UND1",
                "Delta": 2.0,
                "Leverage": 2.0,
                "gross_decay_annual": 0.12,
                "net_edge_p50_annual": 0.08,
                "product_class": "letf",
                "bucket": 1,
                "vol_underlying_annual": 0.45,
            },
            {
                "ETF": "BBB",
                "Underlying": "UND1",
                "Delta": 1.8,
                "Leverage": 2.0,
                "gross_decay_annual": 0.10,
                "net_edge_p50_annual": 0.06,
                "product_class": "letf",
                "bucket": 1,
                "vol_underlying_annual": 0.40,
            },
        ]
    )
    path = tmp_path / "etf_screened_today.csv"
    df.to_csv(path, index=False)
    return path


def _synthetic_flows(tmp_path: Path, symbols: list[str], n_days: int = 80) -> Path:
    start = date(2025, 1, 1)
    rows = []
    for sym in symbols:
        for i in range(n_days):
            rows.append(
                {
                    "date": (start + timedelta(days=i)).isoformat(),
                    "ticker": sym,
                    "tradable_float_shares": 10_000_000,
                    "rebalance_pct_adv_20d": 0.02 + i * 0.0001,
                }
            )
    path = tmp_path / "letf_rebalance_flows_daily.parquet"
    pd.DataFrame(rows).to_parquet(path, index=False)
    return path


def test_build_panel_grain_and_targets(tmp_path: Path, monkeypatch):
    data = tmp_path / "data"
    data.mkdir()
    _synthetic_borrow_history(data)
    _synthetic_metrics(data, ["AAA", "BBB"])
    _synthetic_screener(data)
    _synthetic_flows(data, ["AAA", "BBB"])

    panel = bpp.build_borrow_predictor_panel(repo_root=tmp_path)
    assert not panel.empty
    assert {"date", "symbol"}.issubset(panel.columns)
    assert panel["symbol"].nunique() == 2
    for h in bpp.TARGET_HORIZONS:
        assert f"y_spike_{h}" in panel.columns
        assert f"delta_borrow_{h}" in panel.columns
        assert f"max_borrow_jump_{h}" in panel.columns


def test_peer_features_same_underlying(tmp_path: Path):
    data = tmp_path / "data"
    data.mkdir()
    _synthetic_borrow_history(data)
    _synthetic_metrics(data, ["AAA", "BBB"])
    _synthetic_screener(data)
    panel = bpp.build_borrow_predictor_panel(repo_root=tmp_path)
    assert "peer_borrow_z_mean" in panel.columns
    valid = panel["peer_borrow_z_mean"].dropna()
    assert not valid.empty


def test_write_panel_outputs(tmp_path: Path):
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2025-01-10"]),
            "symbol": ["ZZZ"],
            "borrow_current": [0.1],
            "y_spike_5": [0.0],
        }
    )
    out_p = tmp_path / "panel.parquet"
    out_m = tmp_path / "meta.json"
    bpp.write_panel_outputs(df, parquet_path=out_p, meta_path=out_m)
    assert out_p.exists()
    meta = json.loads(out_m.read_text(encoding="utf-8"))
    assert meta["rows"] == 1
    assert "feature_blocks" in meta

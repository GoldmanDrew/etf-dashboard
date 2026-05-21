"""Tests for --borrow-only partial refresh (no spike model side effects)."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import build_data as bd  # noqa: E402


@pytest.fixture
def borrow_only_tmp(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    monkeypatch.setattr(bd, "OUTPUT_DIR", data_dir)
    monkeypatch.setattr(bd, "OUTPUT_FILE", data_dir / "dashboard_data.json")
    monkeypatch.setattr(bd, "BORROW_HISTORY_FILE", data_dir / "borrow_history.json")
    monkeypatch.setattr(bd, "BORROW_SPIKE_RISK_FILE", data_dir / "borrow_spike_risk.json")
    monkeypatch.setattr(bd, "BORROW_SPIKE_PREDICTIONS_DIR", data_dir / "borrow_spike_predictions")
    return data_dir


def test_refresh_borrow_only_writes_dashboard_and_history_only(borrow_only_tmp):
    dashboard = {
        "records": [
            {
                "symbol": "TQQQ",
                "underlying": "QQQ",
                "bucket": "bucket_1_high_beta",
                "gross_decay_annual": 0.25,
                "borrow_current": 0.01,
            }
        ],
        "summary": {},
    }
    (borrow_only_tmp / "dashboard_data.json").write_text(json.dumps(dashboard), encoding="utf-8")

    ibkr = {
        "success": True,
        "borrow_map": {"TQQQ": 0.02},
        "fee_map": {"TQQQ": 0.02},
        "rebate_map": {"TQQQ": 0.0},
        "available_map": {"TQQQ": 500000},
    }

    with patch.object(bd, "try_fetch_ibkr_ftp", return_value=ibkr), patch.object(
        bd, "fetch_csv_from_github", side_effect=RuntimeError("skip csv")
    ):
        bd.refresh_borrow_only()

    assert (borrow_only_tmp / "dashboard_data.json").exists()
    assert (borrow_only_tmp / "borrow_history.json").exists()
    assert not (borrow_only_tmp / "borrow_spike_risk.json").exists()
    assert not (borrow_only_tmp / "borrow_spike_predictions").exists()

    out = json.loads((borrow_only_tmp / "dashboard_data.json").read_text(encoding="utf-8"))
    assert out["refresh_type"] == "borrow_only"
    assert out["records"][0]["borrow_current"] == 0.02
    assert out["records"][0]["borrow_source"] == "ibkr_ftp"

    hist = json.loads((borrow_only_tmp / "borrow_history.json").read_text(encoding="utf-8"))
    assert "TQQQ" in hist["symbols"]
    assert hist["meta"]["refresh_type"] == "borrow_only"

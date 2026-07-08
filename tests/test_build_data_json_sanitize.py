"""Ensure dashboard JSON never emits literal NaN/Infinity tokens."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import build_data as bd  # noqa: E402
from operational_signals import enrich_records_with_operational_signals  # noqa: E402


def test_sanitize_for_json_replaces_non_finite_floats():
    payload = {
        "a": float("nan"),
        "b": float("inf"),
        "c": np.float64("-inf"),
        "d": {"e": [1.0, float("nan"), "ok"]},
    }
    clean = bd._sanitize_for_json(payload)
    assert clean == {"a": None, "b": None, "c": None, "d": {"e": [1.0, None, "ok"]}}
    text = json.dumps(clean, allow_nan=False)
    assert "NaN" not in text
    assert "Infinity" not in text
    json.loads(text)


def test_enriched_records_dump_without_nan_tokens(tmp_path):
    records = [{
        "symbol": "BAD",
        "bucket": "bucket_2",
        "net_edge_p50_annual": float("nan"),
    }]
    forecast_path = tmp_path / "borrow_forecast_latest.json"
    forecast_path.write_text(
        json.dumps(
            {
                "method": "test",
                "by_symbol": {
                    "BAD": {
                        "delta_borrow_5d_p50": float("nan"),
                        "borrow_forecast_5d_p50": float("nan"),
                        "delta_borrow_5d_p25": float("inf"),
                        "delta_borrow_5d_p75": float("-inf"),
                    }
                },
            },
            allow_nan=True,
        ),
        encoding="utf-8",
    )
    spike = {
        "symbols": {
            "BAD": {
                "p_spike_5d": float("nan"),
                "p_spike_5d_l2_calibrated": 0.11,
                "alert_tier": "watch",
            }
        }
    }
    enrich_records_with_operational_signals(records, borrow_spike_risk=spike, data_dir=tmp_path)

    output = {"records": records, "summary": {}}
    text = json.dumps(bd._sanitize_for_json(output), allow_nan=False)
    assert ":NaN" not in text
    assert "Infinity" not in text
    parsed = json.loads(text)
    rec = parsed["records"][0]
    assert rec["borrow_forecast_5d_p50"] is None
    assert rec["borrow_forecast_delta_5d_p50"] is None
    assert rec["borrow_spike_p_5d"] is None
    assert rec["borrow_spike_p_5d_l2_calibrated"] == pytest.approx(0.11)


def test_refresh_borrow_only_output_is_valid_json(borrow_only_tmp, monkeypatch):
    """Borrow-only refresh must rewrite dashboard JSON without NaN tokens."""
    import build_data as bd_mod

    dashboard = {
        "records": [{
            "symbol": "TQQQ",
            "underlying": "QQQ",
            "bucket": "bucket_1_high_beta",
            "gross_decay_annual": 0.25,
            "borrow_current": 0.01,
        }],
        "summary": {},
    }
    forecast = {
        "method": "test",
        "by_symbol": {
            "TQQQ": {
                "delta_borrow_5d_p50": 0.01,
                "borrow_forecast_5d_p50": float("nan"),
            }
        },
    }
    (borrow_only_tmp / "dashboard_data.json").write_text(json.dumps(dashboard), encoding="utf-8")
    (borrow_only_tmp / "borrow_forecast_latest.json").write_text(
        json.dumps(forecast, allow_nan=True),
        encoding="utf-8",
    )

    ibkr = {
        "success": True,
        "borrow_map": {"TQQQ": 0.02},
        "fee_map": {"TQQQ": 0.02},
        "rebate_map": {"TQQQ": 0.0},
        "available_map": {"TQQQ": 500000},
    }

    from unittest.mock import patch

    with patch.object(bd_mod, "try_fetch_ibkr_ftp", return_value=ibkr), patch.object(
        bd_mod, "fetch_csv_from_github", side_effect=RuntimeError("skip csv")
    ):
        bd_mod.refresh_borrow_only()

    raw = (borrow_only_tmp / "dashboard_data.json").read_text(encoding="utf-8")
    assert ":NaN" not in raw
    assert "Infinity" not in raw
    out = json.loads(raw)
    assert out["records"][0]["borrow_forecast_5d_p50"] is None


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

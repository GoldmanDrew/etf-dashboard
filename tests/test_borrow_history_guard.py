"""Borrow history fail-safe and depth-regression guards."""
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


def _sample_history(n_days: int, sym: str = "TQQQ") -> dict[str, list[dict]]:
    rows = [
        {
            "date": f"2026-01-{str(i + 1).zfill(2)}",
            "borrow_current": 0.01 + i * 0.0001,
            "shares_available": 100_000,
        }
        for i in range(n_days)
    ]
    return {sym: rows}


@pytest.fixture
def borrow_guard_tmp(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    monkeypatch.setattr(bd, "OUTPUT_DIR", data_dir)
    monkeypatch.setattr(bd, "OUTPUT_FILE", data_dir / "dashboard_data.json")
    monkeypatch.setattr(bd, "BORROW_HISTORY_FILE", data_dir / "borrow_history.json")
    monkeypatch.setattr(bd, "BORROW_SPIKE_RISK_FILE", data_dir / "borrow_spike_risk.json")
    monkeypatch.setattr(bd, "BORROW_SPIKE_PREDICTIONS_DIR", data_dir / "borrow_spike_predictions")
    return data_dir


def test_build_borrow_history_falls_back_to_existing_on_api_failure(borrow_guard_tmp):
    deep = _sample_history(40)
    (borrow_guard_tmp / "borrow_history.json").write_text(
        json.dumps({"symbols": deep, "meta": {"build_time": "2026-01-01T00:00:00Z"}}),
        encoding="utf-8",
    )
    with patch.object(bd, "fetch_universe_commits", side_effect=RuntimeError("HTTP 403")):
        out = bd.build_borrow_history_from_commits({"TQQQ"})
    assert len(out["symbols"]["TQQQ"]) == 40
    assert out["meta"].get("fallback") == "existing_file"
    assert "403" in str(out["meta"].get("error", ""))


def test_write_borrow_history_blocks_depth_regression(borrow_guard_tmp):
    deep = _sample_history(60)
    shallow = _sample_history(1)
    (borrow_guard_tmp / "borrow_history.json").write_text(
        json.dumps({"symbols": deep, "meta": {}}),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="depth regression blocked"):
        bd._write_borrow_history_payload(
            {"symbols": shallow, "meta": {}},
            context="test",
        )


def test_borrow_stats_from_history_rows_counts_clean_points():
    rows = [
        {"date": "2026-06-01", "borrow_current": 0.05, "shares_available": 1000},
        {"date": "2026-06-02", "borrow_current": 0.0, "shares_available": 0},
        {"date": "2026-06-03", "borrow_current": 0.04, "shares_available": 2000},
    ]
    stats = bd._borrow_stats_from_history_rows(rows)
    assert stats["borrow_history_points_used"] == 2
    assert stats["borrow_avg_annual"] == pytest.approx(0.045, abs=1e-6)


def test_rebuild_borrow_stats_from_history_updates_dashboard(borrow_guard_tmp):
    hist = _sample_history(30, "TQQQ")
    (borrow_guard_tmp / "borrow_history.json").write_text(
        json.dumps({"symbols": hist, "meta": {}}),
        encoding="utf-8",
    )
    dashboard = {
        "records": [
            {
                "symbol": "TQQQ",
                "bucket": "bucket_1_high_beta",
                "gross_decay_annual": 0.2,
                "borrow_current": 0.02,
                "borrow_avg_annual": 0.02,
                "borrow_history_points_used": 1,
            }
        ],
        "summary": {},
    }
    (borrow_guard_tmp / "dashboard_data.json").write_text(
        json.dumps(dashboard),
        encoding="utf-8",
    )
    bd.rebuild_borrow_stats_from_history()
    out = json.loads((borrow_guard_tmp / "dashboard_data.json").read_text(encoding="utf-8"))
    rec = out["records"][0]
    assert rec["borrow_history_points_used"] == 30
    assert rec["borrow_avg_annual"] != 0.02
    assert (borrow_guard_tmp / "borrow_spike_risk.json").exists()

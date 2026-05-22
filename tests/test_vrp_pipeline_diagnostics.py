"""Tests for YieldBOOST VRP pipeline diagnostics."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from vrp_pipeline_diagnostics import run_diagnostics  # noqa: E402


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_passes_when_vrp_exists_without_spreads(tmp_path: Path):
    vrp = tmp_path / "vrp_live.json"
    _write_json(vrp, {"build_time": "2026-05-20T12:00:00Z", "rows": [], "row_count": 0})
    assert (
        run_diagnostics(
            spreads_path=tmp_path / "missing_spreads.json",
            vrp_path=vrp,
            require_vrp_file=True,
        )
        == 0
    )


def test_fails_when_spreads_exist_but_vrp_missing(tmp_path: Path):
    spreads = tmp_path / "yieldboost_put_spreads_latest.json"
    _write_json(
        spreads,
        {
            "build_time": "2026-05-20T12:00:00Z",
            "spread_count": 1,
            "front_count": 1,
            "spreads": [
                {
                    "yb_etf": "CWY",
                    "sleeve_2x_etf": "CRWV",
                    "underlying": "CRWV",
                    "expiry": "2026-05-26",
                    "strike_long": 27.89,
                    "strike_short": 29.44,
                    "holdings_as_of": "2026-05-20",
                    "is_front": True,
                }
            ],
        },
    )
    rc = run_diagnostics(
        spreads_path=spreads,
        vrp_path=tmp_path / "vrp_live.json",
        fail_on_missing_vrp_when_spreads=True,
    )
    assert rc == 2


def test_validates_matching_vrp_and_spreads(tmp_path: Path):
    spreads = tmp_path / "yieldboost_put_spreads_latest.json"
    vrp = tmp_path / "vrp_live.json"
    spread_row = {
        "yb_etf": "CWY",
        "sleeve_2x_etf": "CRWV",
        "underlying": "CRWV",
        "expiry": "2026-05-26",
        "strike_long": 27.89,
        "strike_short": 29.44,
        "holdings_as_of": "2026-05-20",
        "is_front": True,
    }
    _write_json(
        spreads,
        {
            "build_time": "2026-05-20T12:00:00Z",
            "spread_count": 1,
            "front_count": 1,
            "spreads": [spread_row],
        },
    )
    _write_json(
        vrp,
        {
            "build_time": "2026-05-20T12:05:00Z",
            "row_count": 1,
            "rows": [
                {
                    "yb_etf": "CWY",
                    "sleeve_2x": "CRWV",
                    "expiry": "2026-05-26",
                    "strike_long": 27.89,
                    "strike_short": 29.44,
                    "holdings_as_of": "2026-05-20",
                    "iv_put_long": 0.55,
                    "iv_put_short": 0.48,
                }
            ],
        },
    )
    assert (
        run_diagnostics(
            spreads_path=spreads,
            vrp_path=vrp,
            require_vrp_file=True,
            fail_on_missing_vrp_when_spreads=True,
        )
        == 0
    )


def test_warns_when_event_calendar_stale(tmp_path: Path):
    vrp = tmp_path / "vrp_live.json"
    cal = tmp_path / "event_calendar_combined.json"
    _write_json(
        cal,
        {
            "build_time": "2020-01-01T00:00:00Z",
            "item_count": 0,
            "items": [],
        },
    )
    _write_json(vrp, {"build_time": "2026-05-20T12:00:00Z", "rows": [], "row_count": 0})
    rc = run_diagnostics(
        spreads_path=tmp_path / "yieldboost_put_spreads_latest.json",
        vrp_path=vrp,
        event_calendar_path=cal,
        fail_on_stale_event_calendar=True,
    )
    assert rc == 2

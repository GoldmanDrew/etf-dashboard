"""Tests for scripts/ci_tick.py orchestrator."""
from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import ci_tick as ct  # noqa: E402


@pytest.fixture
def cfg():
    return json.loads(json.dumps(ct.DEFAULT_CONFIG))


def test_is_rth_weekday_session(cfg):
    noon_rth = datetime(2026, 5, 21, 15, 0, tzinfo=UTC)  # Thu
    assert ct.is_rth(noon_rth, cfg) is True
    night = datetime(2026, 5, 21, 3, 0, tzinfo=UTC)
    assert ct.is_rth(night, cfg) is False


def test_is_stale_when_never_run(cfg):
    now = datetime(2026, 5, 21, 15, 0, tzinfo=UTC)
    assert ct.is_stale("borrow", {}, cfg, now) is True


def test_is_not_stale_after_recent_run(cfg):
    now = datetime(2026, 5, 21, 15, 0, tzinfo=UTC)
    state = {"last_borrow_utc": "2026-05-21T14:40:00Z"}
    assert ct.is_stale("borrow", state, cfg, now) is False


def test_pick_auto_off_hours_only_borrow(cfg):
    now = datetime(2026, 5, 21, 4, 0, tzinfo=UTC)
    assert ct.pick_auto_task({}, cfg, now) == "borrow"


def test_pick_auto_off_hours_skips_fresh_borrow(cfg):
    now = datetime(2026, 5, 21, 4, 0, tzinfo=UTC)
    state = {"last_borrow_utc": now.isoformat().replace("+00:00", "Z")}
    assert ct.pick_auto_task(state, cfg, now) is None


def test_pick_auto_rth_returns_stale_task(cfg):
    now = datetime(2026, 5, 21, 15, 0, tzinfo=UTC)
    state = {"last_borrow_utc": "2026-05-21T10:00:00Z"}
    task = ct.pick_auto_task(state, cfg, now)
    assert task in {"borrow", "options", "yieldboost", "intraday", "nav"}


def test_commit_paths_includes_ci_state(tmp_path, cfg, monkeypatch):
    monkeypatch.setattr(ct, "ROOT", tmp_path)
    data = tmp_path / "data"
    data.mkdir()
    (data / "dashboard_data.json").write_text("{}", encoding="utf-8")
    (data / "ci_state.json").write_text("{}", encoding="utf-8")
    paths = ct.commit_paths(cfg, "borrow")
    assert "data/dashboard_data.json" in paths
    assert "data/ci_state.json" in paths


def test_main_skips_when_not_stale(tmp_path, monkeypatch, cfg):
    monkeypatch.setattr(ct, "ROOT", tmp_path)
    monkeypatch.setattr(ct, "CONFIG_PATH", tmp_path / "config" / "ci.yaml")
    monkeypatch.setattr(ct, "STATE_PATH", tmp_path / "data" / "ci_state.json")
    monkeypatch.setattr(ct, "load_config", lambda: cfg)
    now = datetime(2026, 5, 21, 15, 0, tzinfo=UTC)
    state = {
        "last_borrow_utc": now.isoformat().replace("+00:00", "Z"),
        "last_options_utc": now.isoformat().replace("+00:00", "Z"),
        "last_yieldboost_utc": now.isoformat().replace("+00:00", "Z"),
        "last_intraday_utc": now.isoformat().replace("+00:00", "Z"),
        "last_nav_utc": now.isoformat().replace("+00:00", "Z"),
    }
    monkeypatch.setattr(ct, "load_state", lambda: state)
    monkeypatch.setattr(ct, "save_state", lambda s: None)
    monkeypatch.setattr(ct, "pick_auto_task", lambda *a, **k: None)
    with patch.object(sys, "argv", ["ci_tick.py", "--mode", "auto"]):
        assert ct.main() == 0

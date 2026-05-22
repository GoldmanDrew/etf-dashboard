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


def test_intraday_rth_uses_shorter_cadence(cfg):
    now = datetime(2026, 5, 21, 15, 0, tzinfo=UTC)
    state = {"last_intraday_utc": "2026-05-21T14:56:00Z"}  # 4 min ago
    assert ct.is_stale("intraday", state, cfg, now, rth=False) is False
    assert ct.is_stale("intraday", state, cfg, now, rth=True) is False
    state = {"last_intraday_utc": "2026-05-21T14:50:00Z"}  # 10 min ago
    assert ct.is_stale("intraday", state, cfg, now, rth=False) is False
    assert ct.is_stale("intraday", state, cfg, now, rth=True) is True


def test_pick_auto_off_hours_only_borrow(cfg):
    now = datetime(2026, 5, 21, 4, 0, tzinfo=UTC)
    assert ct.pick_auto_tasks({}, cfg, now) == ["borrow"]
    assert ct.pick_auto_task({}, cfg, now) == "borrow"


def test_pick_auto_off_hours_skips_fresh_borrow(cfg):
    now = datetime(2026, 5, 21, 4, 0, tzinfo=UTC)
    state = {"last_borrow_utc": now.isoformat().replace("+00:00", "Z")}
    assert ct.pick_auto_tasks(state, cfg, now) == []
    assert ct.pick_auto_task(state, cfg, now) is None


def test_pick_auto_rth_intraday_fast_lane(cfg):
    """During RTH, stale intraday is always scheduled even when rotation slot is borrow."""
    now = datetime(2026, 5, 22, 15, 0, tzinfo=UTC)  # slot 0 -> borrow in rotation
    state = {
        "last_borrow_utc": "2026-05-22T10:00:00Z",
        "last_intraday_utc": "2026-05-22T14:00:00Z",  # 60m ago -> stale at 5m RTH cadence
    }
    tasks = ct.pick_auto_tasks(state, cfg, now)
    assert tasks[0] == "intraday"
    assert "borrow" in tasks


def test_pick_auto_rth_skips_fresh_intraday_but_runs_rotation(cfg):
    now = datetime(2026, 5, 22, 15, 0, tzinfo=UTC)
    state = {
        "last_borrow_utc": "2026-05-22T10:00:00Z",
        "last_intraday_utc": "2026-05-22T14:58:00Z",  # 2m ago -> fresh at 5m RTH cadence
    }
    tasks = ct.pick_auto_tasks(state, cfg, now)
    assert "intraday" not in tasks
    assert tasks == ["borrow"]


def test_pick_auto_rth_returns_stale_task(cfg):
    now = datetime(2026, 5, 21, 15, 0, tzinfo=UTC)
    state = {"last_borrow_utc": "2026-05-21T10:00:00Z"}
    tasks = ct.pick_auto_tasks(state, cfg, now)
    assert tasks
    assert tasks[0] in {"borrow", "options", "yieldboost", "intraday", "nav"}


def test_rth_day_simulation_intraday_runs_most_ticks(cfg):
    """Over a full RTH day with 15m ticks, intraday should run on most slots."""
    state: dict[str, str] = {}
    intraday_runs = 0
    total_rth_ticks = 0
    for hour in range(13, 23):
        for minute in (0, 15, 30, 45):
            now = datetime(2026, 5, 22, hour, minute, tzinfo=UTC)
            if not ct.is_rth(now, cfg):
                continue
            total_rth_ticks += 1
            tasks = ct.pick_auto_tasks(state, cfg, now)
            if "intraday" in tasks:
                intraday_runs += 1
            for task in tasks:
                key = "nav" if task == "nav" else task
                state[f"last_{key}_utc"] = now.isoformat().replace("+00:00", "Z")

    assert total_rth_ticks == 40
    assert intraday_runs >= 35


def test_commit_paths_includes_ci_state(tmp_path, cfg, monkeypatch):
    monkeypatch.setattr(ct, "ROOT", tmp_path)
    data = tmp_path / "data"
    data.mkdir()
    (data / "dashboard_data.json").write_text("{}", encoding="utf-8")
    (data / "ci_state.json").write_text("{}", encoding="utf-8")
    paths = ct.commit_paths(cfg, "borrow")
    assert "data/dashboard_data.json" in paths
    assert "data/ci_state.json" in paths


def test_merge_commit_paths_deduplicates(tmp_path, cfg, monkeypatch):
    monkeypatch.setattr(ct, "ROOT", tmp_path)
    data = tmp_path / "data"
    data.mkdir()
    (data / "ci_state.json").write_text("{}", encoding="utf-8")
    merged = ct._merge_commit_paths(cfg, ["borrow", "intraday"])
    assert merged.count("data/ci_state.json") == 1


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
    monkeypatch.setattr(ct, "pick_auto_tasks", lambda *a, **k: [])
    with patch.object(sys, "argv", ["ci_tick.py", "--mode", "auto"]):
        assert ct.main() == 0


def test_main_runs_multiple_auto_tasks(tmp_path, monkeypatch, cfg):
    monkeypatch.setattr(ct, "ROOT", tmp_path)
    monkeypatch.setattr(ct, "CONFIG_PATH", tmp_path / "config" / "ci.yaml")
    monkeypatch.setattr(ct, "STATE_PATH", tmp_path / "data" / "ci_state.json")
    monkeypatch.setattr(ct, "load_config", lambda: cfg)
    monkeypatch.setattr(ct, "load_state", lambda: {})
    saved: dict = {}

    def _save(state: dict) -> None:
        saved.update(state)

    monkeypatch.setattr(ct, "save_state", _save)
    executed: list[str] = []
    monkeypatch.setattr(ct, "execute_task", lambda task, config: executed.append(task))
    monkeypatch.setattr(
        ct,
        "pick_auto_tasks",
        lambda *a, **k: ["intraday", "borrow"],
    )
    monkeypatch.setattr(ct, "commit_paths", lambda config, task: [f"data/{task}.json", "data/ci_state.json"])
    with patch.object(sys, "argv", ["ci_tick.py", "--mode", "auto", "--force"]):
        assert ct.main() == 0
    assert executed == ["intraday", "borrow"]
    assert saved.get("last_intraday_utc")
    assert saved.get("last_borrow_utc")

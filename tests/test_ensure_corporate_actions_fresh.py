"""Tests for ensure_corporate_actions_fresh.py."""
from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from ensure_corporate_actions_fresh import _is_stale  # noqa: E402


def test_is_stale_missing_file(tmp_path: Path):
    assert _is_stale(tmp_path / "missing.json", 6.0) is True


def test_is_stale_fresh_file(tmp_path: Path):
    p = tmp_path / "corporate_actions.json"
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    p.write_text(json.dumps({"build_time": now}), encoding="utf-8")
    assert _is_stale(p, 6.0) is False


def test_is_stale_old_file(tmp_path: Path):
    p = tmp_path / "corporate_actions.json"
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=12)).isoformat()
    p.write_text(json.dumps({"build_time": old}), encoding="utf-8")
    assert _is_stale(p, 6.0) is True

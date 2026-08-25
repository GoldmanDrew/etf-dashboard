"""Tests for Bucket 6 watch deploy fetch + quality gate."""
from __future__ import annotations

import json
import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import fetch_bucket6_watch as fb6  # noqa: E402


def _artifact(barrier_sourced: int, funds: int = 8) -> dict:
    return {
        "schema": "b6-watch-1",
        "counts": {
            "funds": funds,
            "barrier_sourced": barrier_sourced,
            "autocall_imminent": 0,
        },
        "funds": [{"etf": "SCA", "underlying": "SMCI"}] if funds else [],
    }


def test_validate_rejects_degraded_barrier_sourced_zero():
    errors = fb6.validate_artifact(_artifact(0), min_funds=8, min_barrier_sourced=8)
    assert any("barrier_sourced=0" in e for e in errors)


def test_validate_accepts_full_barrier_sourcing():
    assert not fb6.validate_artifact(_artifact(8), min_funds=8, min_barrier_sourced=8)


def test_fetch_fail_soft_skips_write_on_gate_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(
        fb6,
        "fetch_remote_text",
        lambda **kw: (json.dumps(_artifact(0)), "http://example.com"),  # noqa: ARG005
    )
    out = tmp_path / "bucket6_watch.json"
    result = fb6.fetch_bucket6_watch(
        local_path=out,
        min_funds=8,
        min_barrier_sourced=8,
        fail_soft=True,
    )
    assert result["ok"] is False
    assert not out.exists()


def test_fetch_writes_when_gate_passes(tmp_path, monkeypatch):
    monkeypatch.setattr(
        fb6,
        "fetch_remote_text",
        lambda **kw: (json.dumps(_artifact(8)), "http://example.com"),  # noqa: ARG005
    )
    out = tmp_path / "bucket6_watch.json"
    result = fb6.fetch_bucket6_watch(
        local_path=out,
        min_funds=8,
        min_barrier_sourced=8,
    )
    assert result["ok"] is True
    assert result["written"] is True
    loaded = json.loads(out.read_text(encoding="utf-8"))
    assert loaded["counts"]["barrier_sourced"] == 8

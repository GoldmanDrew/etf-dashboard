"""Tests for Bucket 1 underlying views score → h ladder."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))

from bucket1_underlying_views import (  # noqa: E402
    build_payload,
    h_for_score,
    load_config,
    normalize_views_patch,
    resolve_view,
    save_views_config,
    write_views_artifact,
)


def test_default_ladder():
    assert h_for_score(-2) == pytest.approx(0.25)
    assert h_for_score(-1) == pytest.approx(0.625)
    assert h_for_score(0) == pytest.approx(1.0)
    assert h_for_score(1) == pytest.approx(1.25)
    assert h_for_score(2) == pytest.approx(1.50)


def test_invalid_score_falls_back_to_neutral():
    assert h_for_score(9) == pytest.approx(1.0)
    assert h_for_score(-9) == pytest.approx(1.0)


def test_clip_respects_bounds():
    assert h_for_score(2, score_to_h={2: 9.0}, h_min=0.25, h_max=1.75) == pytest.approx(1.75)
    assert h_for_score(-2, score_to_h={-2: 0.01}, h_min=0.25, h_max=1.75) == pytest.approx(0.25)


def test_missing_underlying_is_neutral():
    cfg = {
        "score_to_h": {-2: 0.25, -1: 0.625, 0: 1.0, 1: 1.25, 2: 1.5},
        "h_min": 0.25,
        "h_max": 1.75,
        "views": {},
    }
    row = resolve_view("NVDA", config=cfg)
    assert row["score"] == 0
    assert row["h"] == pytest.approx(1.0)
    assert row["from_config"] is False


def test_resolve_and_inherit_sleeves():
    cfg = {
        "schema": "bucket1_underlying_views.v1",
        "score_to_h": {-2: 0.25, -1: 0.625, 0: 1.0, 1: 1.25, 2: 1.5},
        "h_min": 0.25,
        "h_max": 1.75,
        "views": {
            "NVDA": {"score": 2, "note": "test", "updated": "2026-07-24"},
            "TSLA": {"score": -1},
        },
    }
    records = [
        {"symbol": "NVDX", "underlying": "NVDA", "bucket": "bucket_1_high_beta"},
        {"symbol": "NVDL", "underlying": "NVDA", "bucket": "bucket_1_high_beta"},
        {"symbol": "TSLL", "underlying": "TSLA", "bucket": "bucket_1_high_beta"},
        {"symbol": "SQQQ", "underlying": "QQQ", "bucket": "bucket_3_inverse"},
    ]
    payload = build_payload(config=cfg, records=records)
    nvda = payload["by_underlying"]["NVDA"]
    assert nvda["score"] == 2
    assert nvda["h"] == pytest.approx(1.5)
    assert nvda["sleeves"] == ["NVDL", "NVDX"]
    assert nvda["in_bucket1"] is True
    tsla = payload["by_underlying"]["TSLA"]
    assert tsla["score"] == -1
    assert tsla["h"] == pytest.approx(0.625)
    assert "QQQ" not in payload["by_underlying"]
    assert payload["n_active_views"] == 2


def test_load_repo_config():
    cfg = load_config()
    assert cfg["schema"].startswith("bucket1_underlying_views")
    assert h_for_score(0, score_to_h=cfg["score_to_h"]) == pytest.approx(1.0)


def test_normalize_views_patch_drops_neutral():
    out = normalize_views_patch(
        {
            "nvda": {"score": 2, "note": "bull"},
            "TSLA": {"score": 0, "note": ""},
            "AAPL": {"score": 0, "note": "watch"},
            "bad": {"score": 9},
        }
    )
    assert "NVDA" in out and out["NVDA"]["score"] == 2
    assert "TSLA" not in out
    assert out["AAPL"]["score"] == 0 and out["AAPL"]["note"] == "watch"
    assert "BAD" not in out  # invalid score → 0, empty note → dropped


def test_save_views_config_replace(tmp_path):
    cfg_path = tmp_path / "bucket1_underlying_views.yml"
    cfg_path.write_text(
        "schema: bucket1_underlying_views.v1\n"
        "score_to_h:\n  '-2': 0.25\n  '-1': 0.625\n  '0': 1.0\n  '1': 1.25\n  '2': 1.5\n"
        "h_min: 0.25\n"
        "h_max: 1.75\n"
        "views:\n  OLD: {score: 1, note: gone}\n",
        encoding="utf-8",
    )
    cfg = save_views_config(
        {"NVDA": {"score": -1, "note": "lean short"}},
        path=cfg_path,
        replace=True,
    )
    assert "OLD" not in cfg["views"]
    assert cfg["views"]["NVDA"]["score"] == -1
    assert cfg["h_min"] == pytest.approx(0.25)
    art = write_views_artifact(
        config=cfg,
        records=[{"symbol": "NVDX", "underlying": "NVDA", "bucket": "bucket_1_high_beta"}],
        out_path=tmp_path / "views.json",
    )
    assert art["by_underlying"]["NVDA"]["h"] == pytest.approx(0.625)
    assert art["n_active_views"] == 1

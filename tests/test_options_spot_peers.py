"""Tests for Trade Lab spot coverage: underlying peer symbol selection."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from build_data import select_symbols_for_polygon_cache  # noqa: E402


def _rec(symbol: str, underlying: str, bucket: str) -> dict:
    return {"symbol": symbol, "underlying": underlying, "bucket": bucket}


def test_select_symbols_includes_inverse_peers_on_same_underlying(monkeypatch):
    import build_data

    monkeypatch.setattr(build_data, "OPTIONS_INCLUDE_YIELDBOOST", False)
    records = [
        _rec("SMZ", "SMR", "bucket_3_inverse"),
        _rec("SMU", "SMR", "bucket_1_high_beta"),
        _rec("SMUP", "SMR", "bucket_1_high_beta"),
        _rec("TQQQ", "QQQ", "bucket_1_high_beta"),
    ]
    selected = build_data.select_symbols_for_polygon_cache(records)
    assert "SMZ" in selected
    assert "SMR" in selected
    if build_data.OPTIONS_INCLUDE_UNDERLYING_PEERS:
        assert "SMU" in selected
        assert "SMUP" in selected
    assert "TQQQ" not in selected


def test_select_symbols_peer_expansion_can_be_disabled(monkeypatch):
    import build_data

    monkeypatch.setattr(build_data, "OPTIONS_INCLUDE_UNDERLYING_PEERS", False)
    monkeypatch.setattr(build_data, "OPTIONS_INCLUDE_YIELDBOOST", False)
    records = [
        _rec("SMZ", "SMR", "bucket_3_inverse"),
        _rec("SMU", "SMR", "bucket_1_high_beta"),
    ]
    selected = build_data.select_symbols_for_polygon_cache(records)
    assert "SMZ" in selected
    assert "SMR" in selected
    assert "SMU" not in selected

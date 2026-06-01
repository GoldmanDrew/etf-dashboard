"""Tests for freshness helpers and YB underlying refresh selection."""
from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from build_data import (  # noqa: E402
    _order_yieldboost_refresh_symbols,
    _pick_yieldboost_underlyings_to_refresh,
    _yieldboost_targeted_refresh_symbols,
)


def test_yieldboost_targeted_refresh_symbols_keeps_underlyings():
    targets = {"SOXL": [47.89], "NUGT": [35.07]}
    held = {"SOXL": {"2026-05-27"}}
    sleeves, underlyings = _yieldboost_targeted_refresh_symbols(
        ["SOXL", "NUGT", "SOXX", "GDX"],
        target_strikes_by_sleeve=targets,
        held_expiries_by_sleeve=held,
    )
    assert sleeves == ["NUGT", "SOXL"]
    assert underlyings == ["GDX", "SOXX"]


def test_pick_underlyings_all_mode():
    prior = {
        "AMD": {"updated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z")},
    }
    picked, skipped = _pick_yieldboost_underlyings_to_refresh(
        ["AMD", "SOXX"],
        prior,
        refresh_mode="all",
    )
    assert picked == ["AMD", "SOXX"]
    assert skipped == []


def test_pick_underlyings_stale_mode_prioritizes_old():
    old = (datetime.now(UTC) - timedelta(hours=10)).isoformat().replace("+00:00", "Z")
    fresh = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    prior = {
        "AMD": {"updated_at": old},
        "SOXX": {"updated_at": fresh},
        "NVDA": {"updated_at": old},
    }
    picked, skipped = _pick_yieldboost_underlyings_to_refresh(
        ["AMD", "SOXX", "NVDA"],
        prior,
        refresh_mode="stale",
        stale_hours=4,
        cap=2,
    )
    assert len(picked) == 2
    assert "SOXX" not in picked
    assert "SOXX" in skipped


def test_order_yieldboost_refresh_puts_stale_underlyings_before_sleeves():
    old = (datetime.now(UTC) - timedelta(days=5)).isoformat().replace("+00:00", "Z")
    fresh = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    prior = {
        "AMD": {"updated_at": old},
        "SOXX": {"updated_at": old},
        "SOXL": {"updated_at": fresh},
        "AMDL": {"updated_at": fresh},
    }
    order = _order_yieldboost_refresh_symbols(
        ["AMD", "SOXX"],
        ["SOXL", "AMDL"],
        prior,
    )
    assert order.index("AMD") < order.index("SOXL")
    assert order.index("SOXX") < order.index("AMDL")
    assert order[:2] == ["AMD", "SOXX"]

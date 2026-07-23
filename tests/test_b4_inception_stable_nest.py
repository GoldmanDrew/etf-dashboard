"""Contract checks for Optimized-tab stable hedge nest."""
from __future__ import annotations

import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PAIR = REPO / "data" / "bucket4_pairs" / "MSTZ.json"


def test_mstz_has_frozen_stable_optimized_nest() -> None:
    assert PAIR.is_file(), "MSTZ pair shard missing — run build_b4_inception_research.py"
    payload = json.loads(PAIR.read_text(encoding="utf-8"))
    stable = payload.get("inception_research_stable")
    assert isinstance(stable, dict)
    assert stable.get("authoritative") is False
    assert stable.get("history_basis") == "inception_research_stable"
    stab = stable.get("stabilizer") or {}
    assert stab.get("name") == "deadband_005_slew_0025"
    assert stab.get("mode") == "deadband_slew"
    assert float(stab.get("band")) == 0.05
    assert float(stab.get("step")) == 0.025
    dates = (stable.get("daily") or {}).get("dates") or []
    assert len(dates) >= 2
    current = payload.get("inception_research")
    assert isinstance(current, dict)
    assert len((current.get("daily") or {}).get("dates") or []) >= 2

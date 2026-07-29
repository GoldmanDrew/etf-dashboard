"""Regression: production import must preserve dashboard research nests."""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from import_bucket4_production import preserve_research_nests  # noqa: E402


def _daily(n: int = 3) -> dict:
    return {
        "dates": [f"2026-01-0{i}" for i in range(1, n + 1)],
        "equity": [1.0] * n,
        "ret": [0.0] * n,
    }


def test_preserve_research_nests_from_existing_pair_shard(tmp_path: Path):
    pairs = tmp_path / "bucket4_pairs"
    pairs.mkdir()
    prior = {
        "etf": "NBIZ",
        "daily": _daily(2),
        "cash_residual_path": {
            "schema": "cash_residual_path.v1",
            "authoritative": False,
            "history_basis": "cash_residual_path",
            "daily": _daily(5),
            "summary": {"final_equity": 1.1},
        },
        "inception_research": {
            "authoritative": True,  # should be stamped false
            "history_basis": "inception_research",
            "daily": _daily(4),
            "summary": {},
        },
        "inception_research_stable": {
            "authoritative": False,
            "history_basis": "inception_research_stable",
            "daily": _daily(4),
            "summary": {},
        },
    }
    (pairs / "NBIZ.json").write_text(json.dumps(prior), encoding="utf-8")
    incoming = {"etf": "NBIZ", "daily": _daily(2), "summary": {"gate_reason": "production_ledger"}}
    restored = preserve_research_nests("NBIZ", incoming, pairs_dir=pairs)
    assert set(restored) == {
        "cash_residual_path",
        "inception_research",
        "inception_research_stable",
    }
    assert len(incoming["cash_residual_path"]["daily"]["dates"]) == 5
    assert incoming["inception_research"]["authoritative"] is False
    assert incoming["cash_residual_path"]["authoritative"] is False


def test_preserve_research_nests_hydrates_from_standalone(tmp_path: Path, monkeypatch):
    import import_bucket4_production as mod

    pairs = tmp_path / "bucket4_pairs"
    pairs.mkdir()
    cr = tmp_path / "bucket4_cash_residual_path"
    cr.mkdir()
    (cr / "CONI.json").write_text(
        json.dumps(
            {
                "schema": "cash_residual_path.v1",
                "etf": "CONI",
                "daily": _daily(6),
                "summary": {"final_equity": 1.2},
                "authoritative": False,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        mod,
        "STANDALONE_NEST_DIRS",
        {
            "cash_residual_path": cr,
            "inception_research": tmp_path / "missing_ir",
            "inception_research_stable": tmp_path / "missing_irs",
        },
    )
    incoming = {"etf": "CONI", "daily": _daily(2)}
    restored = preserve_research_nests("CONI", incoming, pairs_dir=pairs)
    assert restored == ["cash_residual_path"]
    assert len(incoming["cash_residual_path"]["daily"]["dates"]) == 6

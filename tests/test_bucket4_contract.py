from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import import_bucket4_production as importer  # noqa: E402


def _write(path: Path, payload: dict) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _contract(tmp_path: Path, *, dirty: bool = False) -> Path:
    root = tmp_path / "contract"
    book = {
        "initial_capital_usd": 100_000,
        "dates": ["2026-06-01", "2026-06-02"],
        "returns": [0, .001], "equity": [1, 1.001], "nav_usd": [100_000, 100_100],
        "daily_pnl_usd": [0, 100], "cumulative_pnl_usd": [0, 100],
        "borrow_cost_usd": [0, 1], "short_credit_usd": [0, .2], "margin_cost_usd": [0, .1], "txn_cost_usd": [0, 2],
        "summary": {"cagr": .1, "ann_vol": .2, "sharpe": .5, "max_drawdown": 0, "net_pnl_usd": 100, "final_nav_usd": 100_100},
    }
    pair = {
        "etf": "NBIZ", "underlying": "NBIS", "summary": {"actual_pnl_usd": 100},
        "daily": {"dates": ["2026-06-01", "2026-06-02"], "gross_exposure_dollars": [50_000, 48_000]},
    }
    hashes = {
        "book.json": _write(root / "book.json", book),
        "pairs/NBIZ.json": _write(root / "pairs" / "NBIZ.json", pair),
        "membership.json": _write(root / "membership.json", [
            {"ETF": "NBIZ", "Underlying": "NBIS", "lifecycle_state": "open", "block_reason": ""},
            {"ETF": "CBRZ", "Underlying": "CBRS", "lifecycle_state": "pending_entry", "block_reason": "awaiting_operator_or_execution"},
        ]),
    }
    manifest = {
        "schema": "bucket4_production_replay.v1", "authoritative": True,
        "generated_at_utc": "2026-06-03T00:00:00+00:00",
        "source": {"commit": "abc", "dirty": dirty, "working_tree_hash": "patch" if dirty else None},
        "run": {"start": "2026-06-01", "end": "2026-06-02"},
        "resolved_policy": {"b4_execution": "cadence", "execution_lag_sessions": 1},
        "resolved_policy_hash": "a" * 64, "output_hashes": hashes,
        "reconciliation": {"pair_to_sleeve": {"max_abs_after_usd": 0}, "book_max_abs_residual_usd": 0},
    }
    _write(root / "manifest.json", manifest)
    return root


def test_build_v4_payload_is_authoritative_and_disables_reblend(tmp_path: Path):
    root = _contract(tmp_path)
    manifest = importer.validate_contract(root)
    payload, shards = importer.build_dashboard_payload(root, manifest)
    assert payload["schema"] == "bucket4_backtest.v4"
    assert payload["authoritative"] is True
    assert payload["research_reblend_enabled"] is False
    assert payload["parity"]["production_execution_ledger"] is True
    assert payload["default_weights"]["NBIZ"] == pytest.approx(.48)
    assert payload["n_membership"] == 2
    assert payload["universes"]["pending_entry"]["pairs"] == ["CBRZ"]
    assert list(shards) == ["NBIZ"]


def test_dirty_source_fails_closed(tmp_path: Path):
    root = _contract(tmp_path, dirty=True)
    with pytest.raises(ValueError, match="dirty ls-algo source"):
        importer.validate_contract(root)
    assert importer.validate_contract(root, allow_dirty_source=True)["authoritative"] is True


def test_tampered_pair_fails_hash_validation(tmp_path: Path):
    root = _contract(tmp_path)
    (root / "pairs" / "NBIZ.json").write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="hash mismatch"):
        importer.validate_contract(root)


def test_stale_contract_fails_closed(tmp_path: Path):
    root = _contract(tmp_path)
    with pytest.raises(ValueError, match="stale"):
        importer.validate_contract(root, max_age_days=1)

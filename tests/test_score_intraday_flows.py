"""Tests for T+1 reconciliation of intraday LETF flow estimates."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import score_intraday_flows as scorer  # noqa: E402


def _write_snapshot(snap_dir: Path, date: str, rows: list[dict]) -> Path:
    snap_dir.mkdir(parents=True, exist_ok=True)
    p = snap_dir / f"{date}.jsonl"
    with p.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    return p


def _write_realised(realised_path: Path, date: str, by_und: dict[str, float]) -> None:
    payload = {
        "build_time": f"{date}T22:00:00Z",
        "latest_date": date,
        "by_underlying": {
            sym: {"date": date, "net_moc_dollars": dollars, "underlying": sym}
            for sym, dollars in by_und.items()
        },
    }
    realised_path.write_text(json.dumps(payload), encoding="utf-8")


def test_pick_close_estimate_prefers_smallest_minutes_to_close(tmp_path):
    p = _write_snapshot(
        tmp_path,
        "2026-05-19",
        [
            {"as_of": "T1", "minutes_to_close": 240, "by_underlying": {"SPY": {"estimated_net_close_rebalance_dollars": 1.0}}},
            {"as_of": "T2", "minutes_to_close": 30, "by_underlying": {"SPY": {"estimated_net_close_rebalance_dollars": 2.0}}},
            {"as_of": "T3", "minutes_to_close": 5, "by_underlying": {"SPY": {"estimated_net_close_rebalance_dollars": 3.0}}},
            {"as_of": "T4", "minutes_to_close": 100, "by_underlying": {"SPY": {"estimated_net_close_rebalance_dollars": 4.0}}},
        ],
    )
    est = scorer.pick_close_estimate(p, max_minutes=15)
    assert est is not None
    assert est["as_of"] == "T3"


def test_reconcile_one_day_writes_signed_error(tmp_path):
    snap_dir = tmp_path / "snaps"
    _write_snapshot(
        snap_dir,
        "2026-05-19",
        [
            {
                "as_of": "2026-05-19T19:55:00Z",
                "minutes_to_close": 5,
                "by_underlying": {
                    "SPY": {"estimated_net_close_rebalance_dollars": 110.0},
                    "QQQ": {"estimated_net_close_rebalance_dollars": -50.0},
                    "TSLA": {"estimated_net_close_rebalance_dollars": 25.0},
                },
            },
        ],
    )
    realised_path = tmp_path / "letf_rebalance_flows_latest.json"
    _write_realised(realised_path, "2026-05-19", {"SPY": 100.0, "QQQ": -40.0})  # TSLA missing realised

    rows = scorer.reconcile_one_day("2026-05-19", snap_dir, realised_path)
    by_und = {r["underlying"]: r for r in rows}
    assert "SPY" in by_und and "QQQ" in by_und
    assert "TSLA" not in by_und  # no realised ? skip
    assert by_und["SPY"]["estimate_dollars"] == pytest.approx(110.0)
    assert by_und["SPY"]["realized_dollars"] == pytest.approx(100.0)
    assert by_und["SPY"]["signed_error_dollars"] == pytest.approx(10.0)
    assert by_und["SPY"]["signed_error_pct"] == pytest.approx(0.1, rel=1e-9)
    assert by_und["QQQ"]["signed_error_pct"] == pytest.approx((-50.0 - (-40.0)) / -40.0, rel=1e-9)


def test_roll_metrics_marks_applied_after_min_observations(tmp_path):
    log = tmp_path / "letf_intraday_flow_realized.jsonl"
    rows = [
        {"trading_date": f"2026-05-{d:02d}", "underlying": "SPY", "estimate_dollars": 105.0, "realized_dollars": 100.0,
         "signed_error_dollars": 5.0, "signed_error_pct": 0.05}
        for d in range(1, 7)
    ]
    rows.append({"trading_date": "2026-05-07", "underlying": "QQQ", "estimate_dollars": 110.0, "realized_dollars": 100.0,
                 "signed_error_dollars": 10.0, "signed_error_pct": 0.1})
    with log.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")

    metrics = scorer.roll_metrics(log, window=30, min_obs=5)
    spy = metrics["by_underlying"]["SPY"]
    qqq = metrics["by_underlying"]["QQQ"]
    assert spy["n_observations"] == 6
    assert spy["mean_signed_error_pct"] == pytest.approx(0.05, rel=1e-9)
    assert spy["applied"] is True
    assert qqq["n_observations"] == 1
    assert qqq["applied"] is False


def test_main_no_data_writes_empty_metrics(tmp_path, monkeypatch):
    snap_dir = tmp_path / "snaps"
    realised_path = tmp_path / "realised.json"
    log = tmp_path / "log.jsonl"
    metrics_path = tmp_path / "metrics.json"

    rc = scorer.main.__wrapped__ if hasattr(scorer.main, "__wrapped__") else None  # noqa: F841
    # Drive main() via argv so we cover the CLI path end-to-end.
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "score_intraday_flows",
            "--snapshot-dir", str(snap_dir),
            "--realized-json", str(realised_path),
            "--realized-log", str(log),
            "--metrics-json", str(metrics_path),
        ],
    )
    rc = scorer.main()
    assert rc == 0
    assert metrics_path.exists()
    body = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert body["by_underlying"] == {}

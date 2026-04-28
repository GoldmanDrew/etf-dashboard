"""Unit tests for the NAV-forecast scorer / anchor builder."""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import score_nav_forecasts as sc  # noqa: E402


# ---------------------------------------------------------------------------
# collapse_last_per_symbol
# ---------------------------------------------------------------------------

def test_collapse_last_per_symbol_picks_latest_ts():
    rows = [
        {"symbol": "TSLL", "ts": "2026-04-28T13:00:00Z", "nav_hat": 10.0},
        {"symbol": "TSLL", "ts": "2026-04-28T15:30:00Z", "nav_hat": 10.4},
        {"symbol": "AAPU", "ts": "2026-04-28T14:00:00Z", "nav_hat": 31.7},
    ]
    out = sc.collapse_last_per_symbol(rows)
    assert out["TSLL"]["nav_hat"] == 10.4
    assert out["AAPU"]["nav_hat"] == 31.7


def test_collapse_skips_blank_symbol():
    rows = [{"symbol": "", "ts": "2026-04-28T13:00:00Z", "nav_hat": 1.0}]
    assert sc.collapse_last_per_symbol(rows) == {}


# ---------------------------------------------------------------------------
# score_one_day
# ---------------------------------------------------------------------------

def _snap(symbol="TSLL", nav_hat=10.41, conf="high", ts="2026-04-28T19:50:00Z", model="delta_v1"):
    return {
        "symbol": symbol, "ts": ts, "model": model, "confidence": conf,
        "nav_hat": nav_hat, "premium_bp": 5.0, "product_class": "letf",
    }


def test_score_one_day_basic_match():
    snap = [_snap()]
    metrics_latest = {
        "latest_date": "2026-04-28",
        "by_symbol": {"TSLL": {"date": "2026-04-28", "nav": 10.40, "close_price": 10.42}},
    }
    rows = sc.score_one_day(snap, metrics_latest, date(2026, 4, 28))
    assert len(rows) == 1
    r = rows[0]
    assert r["symbol"] == "TSLL"
    assert r["nav_actual"] == 10.40
    assert r["nav_hat_close"] == 10.41
    expected_bp = (10.41 - 10.40) / 10.40 * 1.0e4
    assert abs(r["err_bp"] - expected_bp) < 0.5
    assert r["abs_err_bp"] >= 0


def test_score_skips_na_confidence():
    snap = [_snap(symbol="ULTY", nav_hat=5.0, conf="na")]
    metrics_latest = {"by_symbol": {"ULTY": {"date": "2026-04-28", "nav": 5.05}}}
    assert sc.score_one_day(snap, metrics_latest, date(2026, 4, 28)) == []


def test_score_skips_when_metric_date_mismatches():
    """Defensive: don't score against a stale official NAV stamp."""
    snap = [_snap()]
    metrics_latest = {
        "by_symbol": {"TSLL": {"date": "2026-04-25", "nav": 10.40}},  # older
    }
    assert sc.score_one_day(snap, metrics_latest, date(2026, 4, 28)) == []


def test_score_skips_when_nav_actual_invalid():
    snap = [_snap()]
    metrics_latest = {
        "by_symbol": {"TSLL": {"date": "2026-04-28", "nav": None}},
    }
    assert sc.score_one_day(snap, metrics_latest, date(2026, 4, 28)) == []


# ---------------------------------------------------------------------------
# update_metrics_daily
# ---------------------------------------------------------------------------

def _write_realized_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def test_metrics_daily_summary_two_days(tmp_path: Path):
    rdir = tmp_path / "realized"
    _write_realized_jsonl(rdir / "2026-04-25.jsonl", [{
        "date": "2026-04-25", "symbol": "TSLL", "model": "delta_v1",
        "product_class": "letf", "nav_hat_close": 10.05, "nav_actual": 10.00,
        "err_bp": 50.0, "abs_err_bp": 50.0,
    }])
    _write_realized_jsonl(rdir / "2026-04-28.jsonl", [{
        "date": "2026-04-28", "symbol": "TSLL", "model": "delta_v1",
        "product_class": "letf", "nav_hat_close": 10.41, "nav_actual": 10.40,
        "err_bp": 9.6, "abs_err_bp": 9.6,
    }])
    summary = sc.update_metrics_daily(rdir)
    assert "TSLL" in summary
    s = summary["TSLL"]
    assert s["n"] == 2
    # median of [9.6, 50.0] = 29.8
    assert 29.0 < s["median_abs_err_bp"] < 30.6
    # within 10 bp: only one of two passes
    assert abs(s["hit_rate_within_10bp"] - 0.5) < 1e-9
    assert s["product_class"] == "letf"


def test_metrics_daily_skips_unknown_when_empty(tmp_path: Path):
    rdir = tmp_path / "realized"
    rdir.mkdir()
    assert sc.update_metrics_daily(rdir) == {}


# ---------------------------------------------------------------------------
# build_anchors (yfinance injectable)
# ---------------------------------------------------------------------------

def _records():
    return [
        {"symbol": "TSLL", "underlying": "TSLA", "beta": 2.0, "product_class": "letf"},
        {"symbol": "AAPU", "underlying": "AAPL", "beta": 2.0, "product_class": "letf"},
    ]


def test_build_anchors_uses_metric_date_and_und_close():
    metrics_latest = {
        "latest_date": "2026-04-28",
        "by_symbol": {
            "TSLL": {"date": "2026-04-28", "nav": 10.40, "close_price": 10.42},
            "AAPU": {"date": "2026-04-28", "nav": 31.50, "close_price": 31.48},
        },
    }
    captured: dict = {}

    def fake_und(unds, target):
        captured["unds"] = list(unds)
        captured["target"] = target
        return {"TSLA": 232.10, "AAPL": 271.06}

    out = sc.build_anchors(
        _records(), metrics_latest, date(2026, 4, 28),
        underlying_closes_fn=fake_und,
    )
    assert out["as_of_date"] == "2026-04-28"
    assert sorted(out["by_symbol"].keys()) == ["AAPU", "TSLL"]
    a = out["by_symbol"]["TSLL"]
    assert a["nav_close"] == 10.40
    assert a["und_close"] == 232.10
    assert a["beta"] == 2.0
    assert a["product_class"] == "letf"
    assert out["n_total"] == 2
    assert out["n_with_und_close"] == 2
    assert captured["unds"] == ["AAPL", "TSLA"]


def test_build_anchors_skips_when_metric_date_mismatches():
    metrics_latest = {
        "by_symbol": {
            "TSLL": {"date": "2026-04-25", "nav": 10.40},  # stale
            "AAPU": {"date": "2026-04-28", "nav": 31.50},  # current
        },
    }
    out = sc.build_anchors(
        _records(), metrics_latest, date(2026, 4, 28),
        underlying_closes_fn=lambda *_: {},
    )
    assert "TSLL" not in out["by_symbol"]
    assert "AAPU" in out["by_symbol"]
    assert out["by_symbol"]["AAPU"]["und_close"] is None

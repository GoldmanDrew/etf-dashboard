"""Unit tests for the multi-model NAV-forecast scorer / anchor builder."""
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
# collapse_last_per_symbol_model
# ---------------------------------------------------------------------------

def test_collapse_keys_by_symbol_model_and_picks_latest_ts():
    rows = [
        {"symbol": "TSLL", "model": "delta_v1",
         "ts": "2026-04-28T13:00:00Z", "nav_hat": 10.0},
        {"symbol": "TSLL", "model": "delta_v1",
         "ts": "2026-04-28T15:30:00Z", "nav_hat": 10.4},
        {"symbol": "TSLL", "model": "delta_v2_ito",
         "ts": "2026-04-28T15:30:00Z", "nav_hat": 10.45},
    ]
    out = sc.collapse_last_per_symbol_model(rows)
    assert out[("TSLL", "delta_v1")]["nav_hat"] == 10.4
    assert out[("TSLL", "delta_v2_ito")]["nav_hat"] == 10.45


def test_collapse_back_compat_treats_missing_model_as_v1():
    rows = [{"symbol": "AAPU", "ts": "2026-04-28T14:00:00Z", "nav_hat": 31.7}]
    out = sc.collapse_last_per_symbol_model(rows)
    assert out[("AAPU", "delta_v1")]["nav_hat"] == 31.7


# ---------------------------------------------------------------------------
# score_one_day
# ---------------------------------------------------------------------------

def _snap(symbol="TSLL", model="delta_v1", nav_hat=10.41, conf="high",
          ts="2026-04-28T19:50:00Z", is_default=True):
    return {
        "symbol": symbol, "ts": ts, "model": model, "confidence": conf,
        "nav_hat": nav_hat, "premium_bp": 5.0, "product_class": "letf",
        "is_default": is_default,
    }


def test_score_emits_one_row_per_symbol_model_pair():
    snap = [
        _snap(model="delta_v1", nav_hat=10.41, is_default=False),
        _snap(model="delta_v2_ito", nav_hat=10.40, is_default=True),
    ]
    metrics_latest = {
        "by_symbol": {"TSLL": {"date": "2026-04-28", "nav": 10.40, "close_price": 10.42}},
    }
    rows = sc.score_one_day(snap, metrics_latest, date(2026, 4, 28))
    by_model = {r["model"]: r for r in rows}
    assert set(by_model) == {"delta_v1", "delta_v2_ito"}
    assert by_model["delta_v2_ito"]["is_default"] is True
    assert by_model["delta_v1"]["is_default"] is False
    assert by_model["delta_v1"]["err_bp"] != 0
    assert by_model["delta_v2_ito"]["abs_err_bp"] == 0.0


def test_score_skips_na_confidence_per_model():
    snap = [_snap(model="delta_v1", conf="na", nav_hat=None)]
    metrics_latest = {"by_symbol": {"TSLL": {"date": "2026-04-28", "nav": 10.40}}}
    assert sc.score_one_day(snap, metrics_latest, date(2026, 4, 28)) == []


def test_score_skips_when_metric_date_mismatches():
    snap = [_snap(model="delta_v2_ito")]
    metrics_latest = {"by_symbol": {"TSLL": {"date": "2026-04-25", "nav": 10.40}}}
    assert sc.score_one_day(snap, metrics_latest, date(2026, 4, 28)) == []


# ---------------------------------------------------------------------------
# update_metrics_daily / update_history_panel
# ---------------------------------------------------------------------------

def _write_realized_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r) + "\n")


def test_metrics_daily_groups_by_symbol_and_model(tmp_path: Path):
    rdir = tmp_path / "realized"
    _write_realized_jsonl(rdir / "2026-04-25.jsonl", [
        {"date": "2026-04-25", "symbol": "TSLL", "model": "delta_v1",
         "product_class": "letf", "nav_hat_close": 10.05, "nav_actual": 10.00,
         "err_bp": 50.0, "abs_err_bp": 50.0},
        {"date": "2026-04-25", "symbol": "TSLL", "model": "delta_v2_ito",
         "product_class": "letf", "nav_hat_close": 10.01, "nav_actual": 10.00,
         "err_bp": 10.0, "abs_err_bp": 10.0},
    ])
    _write_realized_jsonl(rdir / "2026-04-28.jsonl", [
        {"date": "2026-04-28", "symbol": "TSLL", "model": "delta_v1",
         "product_class": "letf", "nav_hat_close": 10.41, "nav_actual": 10.40,
         "err_bp": 9.6, "abs_err_bp": 9.6},
        {"date": "2026-04-28", "symbol": "TSLL", "model": "delta_v2_ito",
         "product_class": "letf", "nav_hat_close": 10.40, "nav_actual": 10.40,
         "err_bp": 0.0, "abs_err_bp": 0.0},
    ])
    payload = sc.update_metrics_daily(rdir, default_model_for_symbol={"TSLL": "delta_v2_ito"})
    bm = payload["by_symbol_models"]["TSLL"]
    assert set(bm) == {"delta_v1", "delta_v2_ito"}
    assert bm["delta_v1"]["n"] == 2
    assert bm["delta_v2_ito"]["median_abs_err_bp"] == 5.0  # median of [10, 0]
    # Default routing surfaces v2 stats under by_symbol[SYM].
    assert payload["by_symbol"]["TSLL"]["model"] == "delta_v2_ito"
    assert payload["default_model_for_symbol"]["TSLL"] == "delta_v2_ito"


def test_metrics_daily_falls_back_to_preference_order(tmp_path: Path):
    rdir = tmp_path / "realized"
    _write_realized_jsonl(rdir / "2026-04-28.jsonl", [
        {"date": "2026-04-28", "symbol": "AAPU", "model": "delta_v1",
         "product_class": "letf", "nav_hat_close": 31.5, "nav_actual": 31.5,
         "err_bp": 0.0, "abs_err_bp": 0.0},
    ])
    payload = sc.update_metrics_daily(rdir)
    # No default_model_for_symbol passed -> fall back to v1 (only available).
    assert payload["by_symbol"]["AAPU"]["model"] == "delta_v1"


def test_history_panel_per_model_default(tmp_path: Path):
    rdir = tmp_path / "realized"
    _write_realized_jsonl(rdir / "2026-04-28.jsonl", [
        {"date": "2026-04-28", "symbol": "TSLL", "model": "delta_v3_swap_mark",
         "product_class": "letf", "nav_hat_close": 10.41, "nav_actual": 10.40,
         "close_actual": 10.42, "err_bp": 9.6, "abs_err_bp": 9.6},
        {"date": "2026-04-28", "symbol": "TSLL", "model": "delta_v2_ito",
         "product_class": "letf", "nav_hat_close": 10.40, "nav_actual": 10.40,
         "close_actual": 10.42, "err_bp": 0.0, "abs_err_bp": 0.0},
    ])
    payload = sc.update_history_panel(
        rdir, default_model_for_symbol={"TSLL": "delta_v3_swap_mark"},
    )
    assert "delta_v3_swap_mark" in payload["by_symbol_models"]["TSLL"]
    assert "delta_v2_ito" in payload["by_symbol_models"]["TSLL"]
    assert payload["by_symbol"]["TSLL"][0]["nav_hat_close"] == 10.41


# ---------------------------------------------------------------------------
# build_anchors (now captures shares_outstanding)
# ---------------------------------------------------------------------------

def _records():
    return [
        {"symbol": "TSLL", "underlying": "TSLA", "beta": 2.0, "product_class": "letf"},
        {"symbol": "AAPU", "underlying": "AAPL", "beta": 2.0, "product_class": "letf"},
    ]


def test_build_anchors_includes_shares_outstanding():
    metrics_latest = {
        "by_symbol": {
            "TSLL": {"date": "2026-04-28", "nav": 10.40, "close_price": 10.42,
                     "shares_outstanding": 5_000_000.0},
            "AAPU": {"date": "2026-04-28", "nav": 31.50, "close_price": 31.48,
                     "shares_outstanding": 6_000_000.0},
        },
    }
    out = sc.build_anchors(
        _records(), metrics_latest, date(2026, 4, 28),
        underlying_closes_fn=lambda *_: {"TSLA": 232.10, "AAPL": 271.06},
    )
    assert out["by_symbol"]["TSLL"]["shares_outstanding"] == 5_000_000.0
    assert out["by_symbol"]["AAPU"]["shares_outstanding"] == 6_000_000.0
    assert out["n_with_shares"] == 2
    assert out["n_with_und_close"] == 2


def test_build_anchors_skips_when_metric_date_mismatches():
    metrics_latest = {
        "by_symbol": {
            "TSLL": {"date": "2026-04-25", "nav": 10.40, "shares_outstanding": 5e6},
            "AAPU": {"date": "2026-04-28", "nav": 31.50, "shares_outstanding": 6e6},
        },
    }
    out = sc.build_anchors(
        _records(), metrics_latest, date(2026, 4, 28),
        underlying_closes_fn=lambda *_: {},
    )
    assert "TSLL" not in out["by_symbol"]
    assert "AAPU" in out["by_symbol"]
    assert out["by_symbol"]["AAPU"]["shares_outstanding"] == 6e6

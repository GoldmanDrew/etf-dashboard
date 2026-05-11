"""Tests for historical NAV-forecast stale spot repair."""
from __future__ import annotations

import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import repair_nav_forecast_stale_spots as repair  # noqa: E402


def test_recompute_beta_nav_hat_uses_next_session_underlying_adj_close():
    snap = {
        "model": "delta_v1",
        "nav_anchor": 35.32,
        "nav_anchor_date": "2026-05-07",
        "und_spot_anchor": 6.36,
        "und_spot_t": 4.505,
        "und_spot_age_sec": 2_600_000.0,
        "beta": 2.0,
        "ter_daily": 0.0,
        "notes": "spot stale 2600000s",
    }
    metrics = {"date": "2026-05-08", "underlying_adj_close": 6.36}
    nav_hat = repair.recompute_beta_nav_hat(snap, "2026-05-08", metrics)
    assert nav_hat == 35.32


def test_recompute_beta_nav_hat_same_session_uses_snapshot_anchor():
    snap = {
        "model": "delta_v1",
        "nav_anchor": 33.52,
        "nav_anchor_date": "2026-05-08",
        "und_spot_anchor": 8.01,
        "und_spot_t": 4.505,
        "und_spot_age_sec": 2_600_000.0,
        "beta": 2.0,
        "ter_daily": 0.0,
        "notes": "spot stale 2600000s",
    }
    # Same date as anchor has a conflicting metrics underlier; the repair must
    # use ``und_spot_anchor`` to avoid fabricating an intraday move.
    metrics = {"date": "2026-05-08", "underlying_adj_close": 6.36}
    nav_hat = repair.recompute_beta_nav_hat(snap, "2026-05-08", metrics)
    assert nav_hat == 33.52


def test_repair_realized_rows_recomputes_error_fields():
    snap = {
        "model": "delta_v1",
        "symbol": "EOSU",
        "nav_anchor": 35.32,
        "nav_anchor_date": "2026-05-07",
        "und_spot_anchor": 6.36,
        "und_spot_t": 4.505,
        "und_spot_age_sec": 2_600_000.0,
        "beta": 2.0,
        "ter_daily": 0.0,
        "notes": "spot stale 2600000s",
    }
    rows = [{
        "date": "2026-05-08",
        "symbol": "EOSU",
        "model": "delta_v1",
        "nav_hat_close": 17.7,
        "nav_actual": 35.32,
        "close_actual": 35.32,
        "err_bp": -5000.0,
        "abs_err_bp": 5000.0,
    }]
    fixed, n = repair.repair_realized_rows(
        rows,
        {("EOSU", "delta_v1"): snap},
        {("2026-05-08", "EOSU"): {"date": "2026-05-08", "underlying_adj_close": 6.36}},
    )
    assert n == 1
    assert fixed[0]["nav_hat_close"] == 35.32
    assert fixed[0]["err_bp"] == 0.0
    assert fixed[0]["abs_err_bp"] == 0.0

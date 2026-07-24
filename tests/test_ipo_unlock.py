"""Tests for IPO float-unlock calendar + σ/decay overlay."""
from __future__ import annotations

import json
import math
import sys
from datetime import date
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPTS))

from ipo_unlock_calendar import (  # noqa: E402
    add_nyse_trading_days,
    build_calendar,
    estimate_float_now,
)
from ipo_unlock_model import (  # noqa: E402
    compute_unlock_fields_for_record,
    enrich_records_with_ipo_unlock,
    ito_gross_annual,
    load_model_config,
)


def _seed() -> dict:
    return json.loads((REPO / "data" / "ipo_float_unlock_seed.json").read_text(encoding="utf-8"))


def test_spcx_t1_unlock_date_aug_6():
    cal = build_calendar(seed=_seed(), asof=date(2026, 7, 23))
    by = {u["underlying"]: u for u in cal["underlyings"]}
    spcx = by["SPCX"]
    assert spcx["is_ipo_float_unlock"] is True
    assert spcx["next_ipo_unlock_date"] == "2026-08-06"
    assert spcx["next_ipo_unlock_shares"] == pytest.approx(911_500_000)
    # Failed performance tranche excluded from supply path but present in tranches
    perf = next(t for t in spcx["tranches"] if t["tranche_id"] == "spcx_t1_perf_10pct")
    assert perf["condition_status"] == "failed"
    # Cum 30d includes T1 + d70 (~Aug 21)
    cum30 = spcx["cumulative_shares_eligible_by_horizon"]["30d"]
    assert cum30 >= 911_500_000


def test_cbrs_staircase_and_past_releases():
    cal = build_calendar(seed=_seed(), asof=date(2026, 7, 23))
    cbrs = next(u for u in cal["underlyings"] if u["underlying"] == "CBRS")
    assert cbrs["is_ipo_float_unlock"] is True
    # Next dated standard tranche should be Aug 19 (post-Q2 still unresolved)
    assert cbrs["next_ipo_unlock_date"] == "2026-08-19"
    assert cbrs["next_ipo_unlock_shares"] == pytest.approx(14_600_000)
    # Float now = IPO float + released day1/2/q1
    float_now = cbrs["float_now_estimate"]
    assert float_now > 30_000_000
    assert cbrs["pending_resolution_count"] >= 1  # post-Q2


def test_earnings_plus_two_trading_days():
    # Wed earnings → Fri unlock (Aug 4 2026 is Tuesday → Aug 6 Thursday)
    assert add_nyse_trading_days(date(2026, 8, 4), 2) == date(2026, 8, 6)


def test_ito_plus2_identity():
    assert ito_gross_annual(2.0, 0.739) == pytest.approx(0.739**2, rel=1e-9)
    assert ito_gross_annual(-2.0, 1.0) == pytest.approx(3.0, rel=1e-9)


def test_failed_trigger_excluded_from_model_path():
    cal = build_calendar(seed=_seed(), asof=date(2026, 7, 23))
    spcx = next(u for u in cal["underlyings"] if u["underlying"] == "SPCX")
    cfg = load_model_config()
    rec = {
        "symbol": "SPCU",
        "underlying": "SPCX",
        "product_class": "letf",
        "delta": 2.0,
        "forecast_vol_underlying_annual": 1.114,
        "gross_decay_annual": 0.12,
        "gross_realized_mean_annual": 0.12,
        "gross_blend_weight_forward": 0.6,
        "borrow_for_net_annual": 0.40,
    }
    fields = compute_unlock_fields_for_record(rec, spcx, asof=date(2026, 7, 23), cfg=cfg)
    assert fields["expected_gross_decay_unlock_p50_annual"] is not None
    # Cum eligible should not include failed 455.8M
    assert fields["ipo_unlock_cum_eligible_shares"] < 911_500_000 + 455_800_000 + 319_000_000 * 5


def test_enrich_records_flags_spcx_family():
    cal = build_calendar(seed=_seed(), asof=date(2026, 7, 23))
    records = [
        {
            "symbol": "SPCU",
            "underlying": "SPCX",
            "product_class": "letf",
            "delta": 2.0,
            "forecast_vol_underlying_annual": 1.114,
            "gross_decay_annual": 0.12,
            "borrow_current": 0.40,
        },
        {
            "symbol": "TQQQ",
            "underlying": "QQQ",
            "product_class": "letf",
            "delta": 3.0,
            "forecast_vol_underlying_annual": 0.25,
        },
    ]
    stats = enrich_records_with_ipo_unlock(
        records, calendar=cal, asof=date(2026, 7, 23)
    )
    assert stats["flagged"] == 1
    assert records[0]["is_ipo_float_unlock"] is True
    assert records[0]["forecast_vol_unlock_annual"] is not None
    assert "is_ipo_float_unlock" not in records[1] or not records[1].get("is_ipo_float_unlock")


def test_estimate_float_now_adds_released():
    und = {"free_float_shares_at_ipo": 30_000_000}
    past = [
        {"shares_eligible": 2_500_000, "condition_status": "unconditional"},
        {"shares_eligible": 2_500_000, "condition_status": "satisfied"},
        {"shares_eligible": 100_000_000, "condition_status": "failed"},
    ]
    assert estimate_float_now(und, past) == pytest.approx(35_000_000)


def test_unlock_sigma_moves_vs_base():
    cal = build_calendar(seed=_seed(), asof=date(2026, 7, 23))
    spcx = next(u for u in cal["underlyings"] if u["underlying"] == "SPCX")
    cfg = load_model_config()
    rec = {
        "symbol": "SPCU",
        "underlying": "SPCX",
        "product_class": "letf",
        "delta": 2.0,
        "forecast_vol_underlying_annual": 1.0,
        "gross_decay_annual": 0.5,
        "borrow_for_net_annual": 0.1,
    }
    fields = compute_unlock_fields_for_record(rec, spcx, asof=date(2026, 7, 23), cfg=cfg)
    assert fields["forecast_vol_unlock_annual"] > 0
    # With huge SPCX supply, event uplift and compression both fire — unlock σ should differ from base
    assert not math.isclose(fields["forecast_vol_unlock_annual"], 1.0, rel_tol=0, abs_tol=1e-6)
    assert fields["expected_gross_decay_unlock_p50_annual"] == pytest.approx(
        ito_gross_annual(2.0, fields["forecast_vol_unlock_annual"]), rel=1e-6
    )

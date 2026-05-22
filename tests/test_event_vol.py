"""Tests for event-vol decomposition (IV/RV de-eventing, calendar merge)."""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from event_vol import (  # noqa: E402
    CalendarEvent,
    enrich_vrp_row_with_events,
    estimate_event_var_for_window,
    events_for_underlying_in_window,
    fair_put_spread_mid_from_iv,
    forward_variance,
    merge_event_calendars,
    strip_iv_to_base,
)
from yieldboost_holdings import build_vrp_live_payload, pair_put_spreads_from_holdings  # noqa: E402

import pandas as pd  # noqa: E402


def test_strip_iv_to_base_removes_event_variance():
    iv_full = 2.0
    horizon = 7 / 252
    event_var = 0.05 ** 2
    base = strip_iv_to_base(iv_full, horizon, event_var)
    assert base is not None
    assert base < iv_full


def test_forward_variance_positive():
    fv = forward_variance(7 / 252, 1.0, 14 / 252, 1.2)
    assert fv is not None
    assert fv > 0


def test_merge_event_calendars_dedupes():
    known = {
        "items": [{
            "underlying": "NVDA",
            "event_type": "earnings",
            "event_date": "2026-05-28",
            "source": "yahoo_earnings",
        }],
    }
    inferred = {
        "items": [{
            "underlying": "CRWV",
            "event_type": "mystery",
            "window_start": "2026-05-30",
            "window_end": "2026-06-06",
            "source": "forward_straddle_excess",
        }],
    }
    merged = merge_event_calendars(known, inferred)
    assert merged["item_count"] == 2


def test_events_for_underlying_in_window():
    cal = merge_event_calendars({
        "items": [{
            "underlying": "MSTR",
            "event_type": "earnings",
            "event_date": "2026-05-29",
            "source": "yahoo_earnings",
        }],
    })
    hits = events_for_underlying_in_window(
        cal, "MSTR", date(2026, 5, 22), date(2026, 5, 30),
    )
    assert len(hits) == 1


def test_estimate_event_var_default_for_earnings():
    ev = CalendarEvent(
        underlying="NVDA",
        event_type="earnings",
        event_date=date(2026, 5, 28),
        source="yahoo_earnings",
    )
    est = estimate_event_var_for_window("NVDA", [ev], None)
    assert est["event_count"] == 1
    assert est["event_var_underlying"] > 0
    assert est["event_var_source"] == "earnings_default_5pct"


def test_fair_put_spread_mid_sane_magnitude():
    mid = fair_put_spread_mid_from_iv(
        spot=58.0,
        strike_long=43.0,
        strike_short=45.0,
        iv_base=1.0,
        horizon_years=7 / 252,
    )
    assert mid is not None
    assert 0 < mid < 5.0


def test_enrich_vrp_row_adds_base_fields():
    row = {
        "underlying": "MSTR",
        "sleeve_2x": "MSTU",
        "expiry": "2026-05-29",
        "iv_put_long": 2.0,
        "iv_put_short": 1.8,
        "rv_30d_2x": 1.2,
        "vrp_vol_2x": 0.7,
        "spot_2x": 7.0,
        "strike_long": 6.5,
        "strike_short": 6.8,
        "spread_mid_market": 0.4,
    }
    cal = merge_event_calendars({
        "items": [{
            "underlying": "MSTR",
            "event_type": "earnings",
            "event_date": "2026-05-28",
            "source": "yahoo_earnings",
            "historical_move_pct_mad": 0.07,
        }],
    })
    out = enrich_vrp_row_with_events(
        row,
        calendar=cal,
        rv_map_base={"MSTU": 1.0},
        as_of=date(2026, 5, 22),
    )
    assert out["iv_base_proxy"] is not None
    assert out["iv_base_proxy"] < 2.0
    assert out["vrp_vol_2x_base"] is not None
    assert out["event_count_in_window"] >= 1


def test_build_vrp_live_payload_with_event_calendar():
    spread = pair_put_spreads_from_holdings(
        pd.DataFrame([{
            "as_of_date": "2026-05-21",
            "etf_ticker": "MTYY",
            "security_type": "OPTION_PUT",
            "shares": 670.0,
            "option_root": "2MSTU",
            "option_expiry": "2026-05-29",
            "option_strike": 7.03,
            "option_put_call": "P",
            "option_side": "long",
        }, {
            "as_of_date": "2026-05-21",
            "etf_ticker": "MTYY",
            "security_type": "OPTION_PUT",
            "shares": -670.0,
            "option_root": "2MSTU",
            "option_expiry": "2026-05-29",
            "option_strike": 7.42,
            "option_put_call": "P",
            "option_side": "short",
        }]),
        underlying_by_etf={"MTYY": "MSTR"},
        as_of=date(2026, 5, 22),
    )
    options_cache = {
        "symbols": {
            "MSTU": {
                "spot": 6.66,
                "options": [
                    {
                        "expiration_date": "2026-05-29",
                        "contract_type": "put",
                        "strike_price": 7.03,
                        "iv": 2.0,
                        "mid": 0.45,
                    },
                    {
                        "expiration_date": "2026-05-29",
                        "contract_type": "put",
                        "strike_price": 7.42,
                        "iv": 1.8,
                        "mid": 0.85,
                    },
                ],
            },
        },
    }
    cal = merge_event_calendars({
        "items": [{
            "underlying": "MSTR",
            "event_type": "earnings",
            "event_date": "2026-05-28",
            "source": "yahoo_earnings",
        }],
    })
    payload = build_vrp_live_payload(
        spread,
        options_cache,
        rv_map={"MSTU": 1.36},
        rv_map_base={"MSTU": 1.1},
        event_calendar=cal,
        as_of=date(2026, 5, 22),
    )
    row = payload["rows"][0]
    assert row.get("iv_base_proxy") is not None
    assert row.get("vrp_vol_2x_base") is not None


def test_refresh_event_pipeline_skips_ingest_when_known_fresh(tmp_path, monkeypatch):
    import event_vol_decomposition as evd

    known_path = tmp_path / "event_calendar_known.json"
    known = {
        "build_time": "2099-01-01T00:00:00Z",
        "item_count": 1,
        "items": [{"underlying": "AMD", "event_type": "earnings", "event_date": "2099-02-01"}],
    }
    known_path.write_text(json.dumps(known), encoding="utf-8")
    monkeypatch.setattr(evd, "KNOWN_PATH", known_path)

    called = {"ingest": False}

    def _fake_build_known_calendar(**kwargs):
        called["ingest"] = True
        return known

    monkeypatch.setattr(
        "ingest_event_calendar.build_known_calendar",
        _fake_build_known_calendar,
    )
    result = evd.refresh_event_pipeline(write=False, known_max_age_hours=24.0)
    assert called["ingest"] is False
    assert result["combined"].get("item_count", 0) >= 1

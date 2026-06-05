"""Tests for split-aware chart return helpers and build_data window stats."""
from __future__ import annotations

import datetime as dt
import importlib.util
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SCRIPTS = REPO / "scripts"
sys.path.insert(0, str(SCRIPTS))

from split_adjustments import (  # noqa: E402
    cum_split_factor,
    dedupe_split_events,
    filter_splits_needing_close_basis_fix,
    infer_split_factor_end_to_live,
    load_split_hints_from_corporate_actions,
    merge_split_events,
)

BUILD_DATA_PATH = SCRIPTS / "build_data.py"
spec = importlib.util.spec_from_file_location("build_data", BUILD_DATA_PATH)
build_data = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(build_data)


def test_cum_split_factor_mtyy_post_split():
    split = dt.date(2026, 6, 2)
    events = [(split, 6.0)]
    end = dt.date(2026, 6, 1)
    asof = dt.date(2026, 6, 2)
    assert cum_split_factor(end, asof, events) == pytest.approx(6.0)
    assert cum_split_factor(asof, end, events) == pytest.approx(1 / 6.0)


def test_build_market_windows_mtyy_split_metadata():
    points = [
        (dt.date(2026, 5, 1), 4.709, 4.4097),
        (dt.date(2026, 6, 1), 3.934, 3.934),
    ]
    split = dt.date(2026, 6, 2)
    events = [(split, 6.0)]
    windows = build_data._build_market_windows(
        points,
        dividends=[(dt.date(2026, 5, 15), 0.371)],
        split_events=events,
        asof_calendar=split,
    )
    w = windows["1M"]
    assert w["price_return"] == pytest.approx((3.934 / 4.709) - 1, rel=1e-4)
    # No Yahoo bar spans the split → cannot verify a close jump → do not scale live spot.
    assert w["split_factor_end_to_asof"] == pytest.approx(1.0)
    assert w["dividend_yield"] == pytest.approx(0.371 / 4.709, rel=1e-4)


def test_split_factor_end_to_asof_applies_discontinuous_jump():
    from split_adjustments import split_factor_end_to_asof_safe

    points = [
        (dt.date(2026, 6, 1), 3.934, 3.934),
        (dt.date(2026, 6, 3), 23.604, 23.604),
    ]
    events = [(dt.date(2026, 6, 2), 6.0)]
    fac = split_factor_end_to_asof_safe(
        points, dt.date(2026, 6, 1), dt.date(2026, 6, 3), events,
    )
    assert fac == pytest.approx(6.0)


def test_live_return_mtyy_post_split_spot():
    """Post-split live spot must not show +385% when chained from window end."""
    start_close = 4.709
    end_close = 3.934
    stored_ret = (end_close / start_close) - 1.0
    live_spot = end_close * 6.0
    factor = infer_split_factor_end_to_live(live_spot, end_close, known_factor=6.0)
    live_on_end = live_spot / factor
    live_ret = (live_on_end / end_close) * (1 + stored_ret) - 1
    assert live_ret == pytest.approx(stored_ret, rel=1e-6)
    assert live_ret < 0


def test_live_return_mtyy_realistic_spot_not_plus_300pct():
    start_close = 4.709
    end_close = 3.934
    stored_ret = (end_close / start_close) - 1.0
    live_spot = 23.0
    factor = infer_split_factor_end_to_live(live_spot, end_close, known_factor=6.0)
    live_ret = (live_spot / factor / end_close) * (1 + stored_ret) - 1
    assert live_ret < 0
    assert live_ret > -0.25


def test_live_return_continuous_yahoo_ignores_stale_factor():
    end_close = 22.94
    live_spot = 23.0
    stored_ret = (22.94 / 64.08) - 1.0
    factor = infer_split_factor_end_to_live(live_spot, end_close, known_factor=6.0)
    assert factor == pytest.approx(1.0)
    live_ret = (live_spot / factor / end_close) * (1 + stored_ret) - 1
    assert live_ret == pytest.approx((23 / 22.94) * (1 + stored_ret) - 1, rel=1e-6)
    assert live_ret > -0.7 and live_ret < -0.5


def test_forward_split_within_window_normalizes_start():
    points = [
        (dt.date(2026, 4, 1), 100.0, 100.0),
        (dt.date(2026, 4, 2), 100.0 / 3.0, 100.0 / 3.0),
    ]
    events = [(dt.date(2026, 4, 2), 1 / 3)]
    windows = build_data._build_market_windows(
        points,
        dividends=[],
        split_events=events,
        asof_calendar=dt.date(2026, 4, 2),
    )
    w = windows["1M"]
    assert w["split_factor_start_to_end"] == pytest.approx(1 / 3)
    assert w["price_return"] == pytest.approx(0.0, abs=1e-6)


def test_corporate_actions_loads_mtyy_split():
    hints = load_split_hints_from_corporate_actions(REPO / "data" / "corporate_actions.json")
    assert "MTYY" in hints
    assert 6.0 in hints["MTYY"].values()


def test_merge_split_events_prefers_explicit():
    a = [(dt.date(2026, 6, 2), 6.0)]
    b = [(dt.date(2026, 6, 2), 5.0)]
    merged = merge_split_events(a, b)
    assert merged == [(dt.date(2026, 6, 2), 5.0)]


def test_dedupe_corp_hint_padding_triple_mtyy():
    hints = {
        dt.date(2026, 6, 1): 6.0,
        dt.date(2026, 6, 2): 6.0,
        dt.date(2026, 6, 3): 6.0,
    }
    merged = merge_split_events([(dt.date(2026, 6, 2), 6.0)], hints=hints)
    assert merged == [(dt.date(2026, 6, 3), 6.0)]


def test_filter_keeps_smup_style_reverse_split():
    points = [
        (dt.date(2026, 1, 23), 421.25, 421.25),
        (dt.date(2026, 1, 26), 36.25, 36.25),
    ]
    events = [(dt.date(2026, 1, 26), 0.1)]
    assert filter_splits_needing_close_basis_fix(points, events) == events


def test_filter_keeps_aplz_style_reverse_split_declared_five():
    points = [
        (dt.date(2026, 6, 2), 2.66, 2.66),
        (dt.date(2026, 6, 3), 15.0, 15.0),
    ]
    events = [(dt.date(2026, 6, 3), 5.0)]
    assert filter_splits_needing_close_basis_fix(points, events) == events


def test_match_split_to_price_jump_aplz():
    from split_adjustments import match_split_to_price_jump

    assert match_split_to_price_jump(15.0 / 2.66, 5.0) == pytest.approx(5.0)


def test_filter_skips_continuous_yahoo_mtyy():
    """Yahoo close is flat through MTYY 1-for-6 — do not apply mechanical ×6."""
    points = [
        (dt.date(2026, 5, 28), 24.0, 23.622),
        (dt.date(2026, 6, 1), 23.604, 23.604),
        (dt.date(2026, 6, 2), 22.99, 22.99),
        (dt.date(2026, 6, 3), 22.94, 22.94),
    ]
    events = [(dt.date(2026, 6, 2), 6.0)]
    assert filter_splits_needing_close_basis_fix(points, events) == []


def test_build_market_windows_mtyy_continuous_yahoo_6m():
    points = [
        (dt.date(2025, 12, 3), 64.08, 36.502),
        (dt.date(2026, 6, 3), 22.94, 22.94),
    ]
    windows = build_data._build_market_windows(
        points,
        dividends=[(dt.date(2026, 5, 15), 0.371)],
        split_events=[],
        asof_calendar=dt.date(2026, 6, 3),
    )
    w = windows["6M"]
    assert w["price_return"] == pytest.approx((22.94 / 64.08) - 1, rel=1e-3)
    assert w["adj_return"] == pytest.approx((22.94 / 36.502) - 1, rel=1e-3)
    assert w["end_close"] == pytest.approx(22.94, rel=1e-3)


def test_split_factor_end_to_asof_skips_continuous_mtyy():
    from split_adjustments import split_factor_end_to_asof_safe

    points = [
        (dt.date(2026, 6, 1), 23.604, 23.604),
        (dt.date(2026, 6, 2), 22.99, 22.99),
        (dt.date(2026, 6, 3), 22.94, 22.94),
    ]
    events = [(dt.date(2026, 6, 2), 6.0)]
    fac = split_factor_end_to_asof_safe(
        points, dt.date(2026, 6, 1), dt.date(2026, 6, 3), events,
    )
    assert fac == pytest.approx(1.0)

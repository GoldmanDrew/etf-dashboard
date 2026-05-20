"""Tests for refresh_underlying_spots merge / staleness logic."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import refresh_underlying_spots as refresh  # noqa: E402


def test_polygon_takes_priority_over_options_cache():
    underlyings = ["SPY", "TSLA"]
    polygon = {
        "SPY": {"last": 707.0, "prior_close": 700.0, "volume_so_far": 12e6, "as_of": "T", "stale": False, "source": "polygon_snapshot"},
    }
    options_cache = {
        "SPY": {"last": 706.5, "as_of": "U", "stale": False, "source": "options_cache"},
        "TSLA": {"last": 600.0, "as_of": "V", "stale": False, "source": "options_cache"},
    }
    metrics_priors = {"TSLA": {"prior_close": 590.0, "prior_close_date": "2026-05-19"}}
    out = refresh.merge_sources(underlyings, polygon=polygon, options_cache=options_cache, metrics_priors=metrics_priors)
    assert out["SPY"]["source"] == "polygon_snapshot"
    assert out["SPY"]["return_d1_so_far"] == pytest.approx(707.0 / 700.0 - 1.0, rel=1e-9)
    assert out["TSLA"]["source"] == "options_cache"
    assert out["TSLA"]["return_d1_so_far"] == pytest.approx(600.0 / 590.0 - 1.0, rel=1e-9)


def test_stale_options_cache_suppresses_return():
    """A stale spot (e.g. cache age > 1 day) is days old, so the implied
    "return vs today's prior close" is meaningless. The merge should keep the
    row but null out ``return_d1_so_far`` so downstream skips it."""
    out = refresh.merge_sources(
        ["AAPL"],
        polygon={},
        options_cache={"AAPL": {"last": 200.0, "as_of": "X", "stale": True, "source": "options_cache"}},
        metrics_priors={"AAPL": {"prior_close": 230.0, "prior_close_date": "2026-05-19"}},
    )
    assert out["AAPL"]["last"] == 200.0
    assert out["AAPL"]["stale"] is True
    assert out["AAPL"]["return_d1_so_far"] is None


def test_missing_prior_close_yields_no_return():
    out = refresh.merge_sources(
        ["NVDA"],
        polygon={"NVDA": {"last": 130.0, "prior_close": None, "volume_so_far": None, "as_of": "T", "stale": False, "source": "polygon_snapshot"}},
        options_cache={},
        metrics_priors={},  # nothing to fall back to
    )
    assert out["NVDA"]["return_d1_so_far"] is None


def test_polygon_sym_dot_conversion():
    assert refresh._polygon_sym("BRK-B") == "BRK.B"
    assert refresh._norm_sym("brk.b") == "BRK-B"


def test_build_payload_counts_sources():
    by_und = {
        "SPY": {"source": "polygon_snapshot", "stale": False, "return_d1_so_far": 0.01, "volume_so_far": 1e6},
        "AAPL": {"source": "options_cache", "stale": True, "return_d1_so_far": None, "volume_so_far": None},
        "BRK-B": {"source": "yfinance_fast_info", "stale": False, "return_d1_so_far": 0.0, "volume_so_far": None},
    }
    payload = refresh.build_payload(["SPY", "AAPL", "BRK-B", "FOO"], by_und)
    assert payload["n_underlyings_universe"] == 4
    assert payload["n_underlyings_priced"] == 3
    assert payload["sources"] == {"polygon_snapshot": 1, "options_cache": 1, "yfinance_fast_info": 1}
    assert payload["n_with_return"] == 2
    assert payload["n_with_volume"] == 1
    assert payload["n_stale"] == 1

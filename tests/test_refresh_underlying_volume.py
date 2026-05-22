"""Tests for decoupled intraday volume refresh."""
from __future__ import annotations

import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import refresh_underlying_volume as vol  # noqa: E402


def test_merge_volume_sources_respects_priority():
    merged = vol.merge_volume_sources(
        ["SPY", "QQQ"],
        tradier={"SPY": {"volume_so_far": 1_000.0, "source": "tradier_volume"}},
        polygon={"QQQ": {"volume_so_far": 2_000.0, "source": "polygon_day_agg"}},
        yfinance={"QQQ": {"volume_so_far": 9_999.0, "source": "yfinance_last_volume"}},
    )
    assert merged["SPY"]["volume_so_far"] == 1_000.0
    assert merged["SPY"]["source"] == "tradier_volume"
    assert merged["QQQ"]["volume_so_far"] == 2_000.0


def test_load_tier0_underlyings_filters_by_pct_adv(tmp_path):
    flow = tmp_path / "flow.json"
    flow.write_text(
        '{"by_underlying":{"SPY":{"estimated_net_close_rebalance_dollars":1000000,'
        '"underlying_dollar_adv_20d":100000000},"TINY":{"estimated_net_close_rebalance_dollars":1,'
        '"underlying_dollar_adv_20d":100000000}}}',
        encoding="utf-8",
    )
    tier0 = vol.load_tier0_underlyings(flow, pct_adv=0.005)
    assert tier0 == {"SPY"}

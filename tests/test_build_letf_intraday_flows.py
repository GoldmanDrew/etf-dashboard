"""Unit tests for the intraday LETF rebalance-flow estimator."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import build_letf_intraday_flows as intraday  # noqa: E402


def _universe() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "ticker": "UPRO",
            "underlying": "SPY",
            "leverage": 3.0,
            "product_class": "letf",
            "included_in_universe": True,
            "universe_exclusion_reason": None,
        },
        {
            "ticker": "SPXU",
            "underlying": "SPY",
            "leverage": -3.0,
            "product_class": "letf",
            "included_in_universe": True,
            "universe_exclusion_reason": None,
        },
        {
            # Excluded fund: should land in by_fund with quality_flag set,
            # but never count toward the per-underlying aggregate.
            "ticker": "TSLZ",
            "underlying": "TSLA",
            "leverage": -2.0,
            "product_class": "income_overlay",
            "included_in_universe": False,
            "universe_exclusion_reason": "income_overlay",
        },
    ])


def _latest_metrics() -> pd.DataFrame:
    return pd.DataFrame([
        {"ticker": "UPRO", "date": pd.Timestamp("2026-05-19"), "aum": 1_000_000_000.0, "nav": 70.0},
        {"ticker": "SPXU", "date": pd.Timestamp("2026-05-19"), "aum": 500_000_000.0, "nav": 9.0},
        {"ticker": "TSLZ", "date": pd.Timestamp("2026-05-19"), "aum": 50_000_000.0, "nav": 12.0},
    ])


def _spots() -> dict:
    return {
        "SPY": {
            "last": 707.07,
            "prior_close": 700.0,
            "return_d1_so_far": 0.01,
            "volume_so_far": 12_000_000.0,
            "as_of": "2026-05-20T19:55:00Z",
            "source": "polygon_snapshot",
            "stale": False,
        },
        "TSLA": {
            "last": 600.0,
            "prior_close": 600.0,
            "return_d1_so_far": 0.0,
            "as_of": "2026-05-20T19:55:00Z",
            "source": "polygon_snapshot",
            "stale": False,
        },
    }


def test_compute_fund_intraday_signs_match_avellaneda_zhang():
    fund_df = intraday.compute_fund_intraday(_universe(), _latest_metrics(), _spots())
    upro = fund_df[fund_df["ticker"].eq("UPRO")].iloc[0]
    spxu = fund_df[fund_df["ticker"].eq("SPXU")].iloc[0]
    # 3*(3-1)*1B*+1% = +$60M
    assert upro["estimated_close_rebalance_dollars"] == pytest.approx(60_000_000.0, rel=1e-9)
    # Inverse fund mechanics: -3 * (-3 - 1) * 500M * +1% = (+12) * 500M * 0.01 = +$60M.
    # Inverse LETFs buy the underlying on up days; the sign here matches the
    # Avellaneda-Zhang identity: L*(L-1) is +12 even for L = -3.
    assert spxu["estimated_close_rebalance_dollars"] == pytest.approx(60_000_000.0, rel=1e-9)


def test_excluded_fund_marked_but_not_aggregated():
    fund_df = intraday.compute_fund_intraday(_universe(), _latest_metrics(), _spots())
    excluded = fund_df[fund_df["ticker"].eq("TSLZ")].iloc[0]
    assert excluded["quality_flag"] == "income_overlay"
    assert excluded["included_in_aggregate"] == False  # noqa: E712 -- intentional bool comparison

    agg = intraday.aggregate_underlying(fund_df, adv_latest={}, bias_map={})
    # TSLA had no included funds, so it must be absent from the aggregate.
    assert "TSLA" not in agg["underlying"].tolist()


def test_aggregate_with_adv_and_bias_adjustment():
    fund_df = intraday.compute_fund_intraday(_universe(), _latest_metrics(), _spots())
    adv_latest = {
        "SPY": {"underlying_dollar_adv_20d": 30_000_000_000.0, "as_of_date": "2026-05-19"},
    }
    bias_map = {
        # Suppose historical signed error is +5 % (estimate consistently 5 %
        # over realised); adjusted = (1 - 0.05) ù estimate = 95 % ù est.
        "SPY": {"mean_signed_error_pct": 0.05, "n": 12},
    }
    agg = intraday.aggregate_underlying(fund_df, adv_latest=adv_latest, bias_map=bias_map)
    spy = agg[agg["underlying"].eq("SPY")].iloc[0]
    # Both UPRO and SPXU push +$60M each on a +1 % day ? net +$120M.
    assert spy["estimated_net_close_rebalance_dollars"] == pytest.approx(120_000_000.0, rel=1e-9)
    assert spy["estimated_close_rebalance_pct_adv_20d"] == pytest.approx(120_000_000.0 / 30_000_000_000.0, rel=1e-9)
    assert spy["bias_signed_error_pct"] == pytest.approx(0.05, rel=1e-9)
    # 120_000_000 ù (1 ? 0.05) = 114_000_000
    assert spy["estimated_close_rebalance_dollars_bias_adj"] == pytest.approx(114_000_000.0, rel=1e-9)


def test_stale_spot_drops_from_aggregate():
    spots = _spots()
    spots["SPY"] = {
        **spots["SPY"],
        "return_d1_so_far": None,  # stale path: refresh script suppresses ret
        "stale": True,
    }
    fund_df = intraday.compute_fund_intraday(_universe(), _latest_metrics(), spots)
    upro = fund_df[fund_df["ticker"].eq("UPRO")].iloc[0]
    assert upro["quality_flag"] == "stale_spot"
    assert upro["included_in_aggregate"] == False  # noqa: E712


def test_today_history_tail(tmp_path):
    snap_dir = tmp_path / "snaps"
    snap_dir.mkdir()
    p = snap_dir / "2026-05-20.jsonl"
    rows = [
        {"as_of": "2026-05-20T18:00:00Z", "minutes_to_close": 120, "by_underlying": {"SPY": {"estimated_net_close_rebalance_dollars": 100.0}}},
        {"as_of": "2026-05-20T19:00:00Z", "minutes_to_close": 60, "by_underlying": {"SPY": {"estimated_net_close_rebalance_dollars": 200.0}}},
        {"as_of": "2026-05-20T19:55:00Z", "minutes_to_close": 5, "by_underlying": {"SPY": {"estimated_net_close_rebalance_dollars": 250.0}}},
    ]
    with p.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")
    hist = intraday.read_today_history(snap_dir, "2026-05-20", n=2)
    assert len(hist) == 2
    assert hist[-1]["minutes_to_close"] == 5
    assert hist[-1]["net_by_underlying"]["SPY"] == pytest.approx(250.0)

"""Unit tests for leveraged-ETF close rebalance-flow estimates."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import build_letf_rebalance_flows as flows  # noqa: E402


def test_rebalance_notional_3x_up_day():
    # 3x with $1B AUM and +1% underlying return: 3*(3-1)*1B*1% = +$60M.
    assert flows.rebalance_notional(1_000_000_000.0, 3.0, 0.01) == 60_000_000.0


def test_rebalance_notional_inverse_2x_down_day():
    # -2x with $500M AUM and -2% return: (-2)*(-3)*500M*(-2%) = -$60M.
    assert flows.rebalance_notional(500_000_000.0, -2.0, -0.02) == -60_000_000.0


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
            "product_class": "inverse",
            "included_in_universe": True,
            "universe_exclusion_reason": None,
        },
        {
            "ticker": "YSPY",
            "underlying": "SPY",
            "leverage": 0.5,
            "product_class": "income_yieldboost",
            "included_in_universe": False,
            "universe_exclusion_reason": "income_overlay",
        },
        {
            "ticker": "SPY",
            "underlying": "SPY",
            "leverage": 1.0,
            "product_class": "passive_low_beta",
            "included_in_universe": False,
            "universe_exclusion_reason": "product_class:passive_low_beta",
        },
    ])


def _metrics() -> pd.DataFrame:
    rows = []
    for ticker, aum in {
        "UPRO": 1_000_000_000.0,
        "SPXU": 1_000_000_000.0,
        "YSPY": 100_000_000.0,
        "SPY": 500_000_000_000.0,
    }.items():
        rows.extend([
            {
                "date": pd.Timestamp("2026-05-18"),
                "ticker": ticker,
                "nav": 100.0,
                "aum": aum,
                "shares_outstanding": aum / 100.0,
                "underlying_adj_close": 100.0,
                "stale": False,
                "stale_age_bdays": 0,
                "source_provider": "unit",
                "status": "ok",
            },
            {
                "date": pd.Timestamp("2026-05-19"),
                "ticker": ticker,
                "nav": 101.0,
                "aum": aum * 1.01,
                "shares_outstanding": aum / 100.0,
                "underlying_adj_close": 101.0,
                "stale": False,
                "stale_age_bdays": 0,
                "source_provider": "unit",
                "status": "ok",
            },
        ])
    return pd.DataFrame(rows)


def test_long_and_inverse_flows_add_on_up_day():
    fund = flows.build_fund_flows(_universe(), _metrics())
    day = fund[fund["date"].eq("2026-05-19")]
    by_ticker = day.set_index("ticker")

    assert by_ticker.loc["UPRO", "rebalance_signed_dollars"] == pytest.approx(60_000_000.0)
    assert by_ticker.loc["SPXU", "rebalance_signed_dollars"] == pytest.approx(120_000_000.0)
    assert by_ticker.loc["YSPY", "quality_flag"] == "income_overlay"
    assert by_ticker.loc["SPY", "quality_flag"] == "product_class:passive_low_beta"

    agg = flows.build_underlying_aggregates(fund)
    spy = agg[(agg["date"].eq("2026-05-19")) & (agg["underlying"].eq("SPY"))].iloc[0]
    assert spy["net_moc_dollars"] == pytest.approx(180_000_000.0)
    assert spy["moc_buy_dollars"] == pytest.approx(180_000_000.0)
    assert spy["moc_sell_dollars"] == 0.0
    assert spy["n_funds"] == 2


def test_stale_prior_aum_is_not_aggregated():
    metrics = _metrics()
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "stale"] = True

    fund = flows.build_fund_flows(_universe(), metrics)
    upro = fund[(fund["date"].eq("2026-05-19")) & (fund["ticker"].eq("UPRO"))].iloc[0]
    assert upro["quality_flag"] == "stale_aum"
    assert bool(upro["included_in_aggregate"]) is False


def test_load_universe_excludes_yieldboost_and_uses_delta_when_leverage_missing(tmp_path: Path):
    csv_path = tmp_path / "universe.csv"
    csv_path.write_text(
        "ETF,Underlying,is_yieldboost,Delta,product_class\n"
        "AAPU,AAPL,False,2.0,letf\n"
        "YSPY,SPY,True,0.5,income_yieldboost\n",
        encoding="utf-8",
    )

    out = flows.load_universe(csv_path).set_index("ticker")
    assert out.loc["AAPU", "leverage"] == 2.0
    assert bool(out.loc["AAPU", "included_in_universe"]) is True
    assert bool(out.loc["YSPY", "included_in_universe"]) is False
    assert out.loc["YSPY", "universe_exclusion_reason"] == "income_overlay"

"""Unit tests for leveraged-ETF close rebalance-flow estimates."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
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


def test_partial_gap_aum_fill_avoids_missing_prior_aum():
    metrics = _metrics()
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "aum"] = 1_000_000_000.0
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "shares_outstanding"] = 10_000_000.0
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "status"] = "ok"
    partial = {
        "date": pd.Timestamp("2026-05-19"),
        "ticker": "UPRO",
        "nav": 101.0,
        "aum": np.nan,
        "shares_outstanding": np.nan,
        "underlying_adj_close": 101.0,
        "stale": False,
        "stale_age_bdays": 0,
        "source_provider": "merged",
        "status": "partial",
    }
    metrics = pd.concat([metrics, pd.DataFrame([partial])], ignore_index=True)
    metrics = metrics.sort_values(["ticker", "date"]).drop_duplicates(subset=["ticker", "date"], keep="last")

    fund = flows.build_fund_flows(_universe(), metrics)
    upro = fund[(fund["date"].eq("2026-05-19")) & (fund["ticker"].eq("UPRO"))].iloc[0]
    assert upro["quality_flag"] == "ok"
    assert float(upro["aum_prior_close"]) == pytest.approx(1_000_000_000.0)


def test_issuer_lag_prior_does_not_block_flow():
    metrics = _metrics()
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "stale"] = True
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "stale_age_bdays"] = 1
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "source_provider"] = "direxion"
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "stale_kind"] = "issuer_lag"

    fund = flows.build_fund_flows(_universe(), metrics)
    upro = fund[(fund["date"].eq("2026-05-19")) & (fund["ticker"].eq("UPRO"))].iloc[0]
    assert upro["quality_flag"] == "ok"
    assert bool(upro["included_in_aggregate"]) is True


def test_stale_prior_anchor_lag_blocks_flow():
    metrics = _metrics()
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "stale"] = True
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "stale_age_bdays"] = 1
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "source_provider"] = "polygon"
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "stale_kind"] = "anchor_lag"

    fund = flows.build_fund_flows(_universe(), metrics)
    upro = fund[(fund["date"].eq("2026-05-19")) & (fund["ticker"].eq("UPRO"))].iloc[0]
    assert upro["quality_flag"] == "stale_aum"
    assert bool(upro["included_in_aggregate"]) is False


def test_issuer_timing_stale_age_nonpositive_does_not_block_flow():
    metrics = _metrics()
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "stale"] = True
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "stale_age_bdays"] = -1
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "source_provider"] = "direxion"

    fund = flows.build_fund_flows(_universe(), metrics)
    upro = fund[(fund["date"].eq("2026-05-19")) & (fund["ticker"].eq("UPRO"))].iloc[0]
    assert upro["quality_flag"] == "ok"
    assert bool(upro["included_in_aggregate"]) is True


def test_carry_forward_prior_within_stale_budget_allows_flow():
    metrics = _metrics()
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "stale"] = True
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "stale_age_bdays"] = 1
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "source_provider"] = "carry_forward"
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "stale_kind"] = "carry_forward"

    fund = flows.build_fund_flows(_universe(), metrics)
    upro = fund[(fund["date"].eq("2026-05-19")) & (fund["ticker"].eq("UPRO"))].iloc[0]
    assert upro["quality_flag"] == "ok"
    assert bool(upro["included_in_aggregate"]) is True


def test_carry_forward_prior_older_than_stale_budget_blocks_flow():
    metrics = _metrics()
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "stale"] = True
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "stale_age_bdays"] = 4
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "source_provider"] = "carry_forward"
    metrics.loc[(metrics["ticker"].eq("UPRO")) & (metrics["date"].eq(pd.Timestamp("2026-05-18"))), "stale_kind"] = "carry_forward"

    fund = flows.build_fund_flows(_universe(), metrics, stale_bdays=3)
    upro = fund[(fund["date"].eq("2026-05-19")) & (fund["ticker"].eq("UPRO"))].iloc[0]
    assert upro["quality_flag"] == "stale_aum"


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


def test_compute_adv_panel_rolls_per_underlying():
    panel = pd.DataFrame([
        {"date": "2026-05-15", "underlying": "AAPL", "dollar_volume": 100.0},
        {"date": "2026-05-16", "underlying": "AAPL", "dollar_volume": 200.0},
        {"date": "2026-05-17", "underlying": "AAPL", "dollar_volume": 300.0},
        {"date": "2026-05-18", "underlying": "AAPL", "dollar_volume": 400.0},
        {"date": "2026-05-19", "underlying": "AAPL", "dollar_volume": 500.0},
        {"date": "2026-05-19", "underlying": "TSLA", "dollar_volume": 1_000.0},
    ])
    out = flows.compute_adv_panel(panel, window=3)
    aapl_19 = out[(out.date == "2026-05-19") & (out.underlying == "AAPL")].iloc[0]
    assert aapl_19.underlying_dollar_adv_20d == pytest.approx((300.0 + 400.0 + 500.0) / 3)
    tsla_19 = out[(out.date == "2026-05-19") & (out.underlying == "TSLA")].iloc[0]
    # min_periods is min(5, window) = 3 here, so a single observation -> NaN.
    assert pd.isna(tsla_19.underlying_dollar_adv_20d)


def test_compute_adv_panel_with_median_includes_median_column():
    panel = pd.DataFrame([
        {"date": "2026-05-15", "underlying": "AAPL", "dollar_volume": 100.0},
        {"date": "2026-05-16", "underlying": "AAPL", "dollar_volume": 200.0},
        {"date": "2026-05-17", "underlying": "AAPL", "dollar_volume": 300.0},
        {"date": "2026-05-18", "underlying": "AAPL", "dollar_volume": 400.0},
        {"date": "2026-05-19", "underlying": "AAPL", "dollar_volume": 10_000.0},
    ])
    out = flows.compute_adv_panel_with_median(panel, window=3)
    row = out[(out.date == "2026-05-19") & (out.underlying == "AAPL")].iloc[0]
    assert row.underlying_dollar_adv_20d == pytest.approx((300.0 + 400.0 + 10_000.0) / 3)
    assert row.underlying_dollar_median_adv_20d == pytest.approx(400.0)


def test_annotate_with_adv_attaches_pct_adv():
    fund_flows = pd.DataFrame([
        {
            "date": "2026-05-19",
            "ticker": "UPRO",
            "underlying": "SPY",
            "rebalance_signed_dollars": 60_000_000.0,
        },
    ])
    aggregates = pd.DataFrame([
        {
            "date": "2026-05-19",
            "underlying": "SPY",
            "net_moc_dollars": 60_000_000.0,
        },
    ])
    adv = pd.DataFrame([
        {"date": "2026-05-19", "underlying": "SPY", "underlying_dollar_adv_20d": 30_000_000_000.0},
    ])

    fund_flows_out, aggregates_out = flows.annotate_with_adv(fund_flows, aggregates, adv)
    assert fund_flows_out.loc[0, "rebalance_pct_adv_20d"] == pytest.approx(60_000_000.0 / 30_000_000_000.0)
    assert aggregates_out.loc[0, "net_moc_pct_adv_20d"] == pytest.approx(60_000_000.0 / 30_000_000_000.0)


def test_write_outputs_uses_per_underlying_latest(tmp_path: Path):
    fund_flows = pd.DataFrame([
        {
            "date": "2026-05-19", "ticker": "UPRO", "underlying": "SPY",
            "product_class": "letf", "leverage": 3.0,
            "aum_prior_close": 1_000_000_000.0, "nav_prior_close": 100.0,
            "shares_outstanding_prior_close": 10_000_000.0,
            "underlying_adj_close_prior": 100.0, "underlying_adj_close": 101.0,
            "underlying_return_d1": 0.01,
            "rebalance_signed_dollars": 60_000_000.0,
            "rebalance_abs_dollars": 60_000_000.0,
            "abs_rebalance_pct_prior_aum": 0.06,
            "rebalance_pct_adv_20d": 0.002,
            "underlying_dollar_adv_20d": 30_000_000_000.0,
            "included_in_aggregate": True, "quality_flag": "ok",
            "source_provider": "unit", "status": "ok",
        },
        {
            "date": "2026-05-18", "ticker": "APLZ", "underlying": "APLD",
            "product_class": "inverse", "leverage": -2.0,
            "aum_prior_close": 8_000_000.0, "nav_prior_close": 10.0,
            "shares_outstanding_prior_close": 800_000.0,
            "underlying_adj_close_prior": 50.0, "underlying_adj_close": 46.0,
            "underlying_return_d1": -0.08,
            "rebalance_signed_dollars": -3_840_000.0,
            "rebalance_abs_dollars": 3_840_000.0,
            "abs_rebalance_pct_prior_aum": 0.48,
            "rebalance_pct_adv_20d": -0.04,
            "underlying_dollar_adv_20d": 96_000_000.0,
            "included_in_aggregate": True, "quality_flag": "ok",
            "source_provider": "unit", "status": "ok",
        },
    ])
    aggregates = pd.DataFrame([
        {
            "date": "2026-05-19", "underlying": "SPY",
            "net_moc_dollars": 60_000_000.0, "gross_moc_dollars": 60_000_000.0,
            "moc_buy_dollars": 60_000_000.0, "moc_sell_dollars": 0.0,
            "total_letf_aum_prior_close": 1_000_000_000.0, "n_funds": 1,
            "underlying_return_d1": 0.01, "net_moc_pct_letf_aum": 0.06,
            "net_moc_5d_dollars": 60_000_000.0, "net_moc_20d_dollars": 60_000_000.0,
            "net_moc_60d_dollars": 60_000_000.0, "net_moc_z_60d": None,
            "underlying_dollar_adv_20d": 30_000_000_000.0,
            "net_moc_pct_adv_20d": 0.002,
        },
        {
            "date": "2026-05-18", "underlying": "APLD",
            "net_moc_dollars": -3_840_000.0, "gross_moc_dollars": 3_840_000.0,
            "moc_buy_dollars": 0.0, "moc_sell_dollars": 3_840_000.0,
            "total_letf_aum_prior_close": 8_000_000.0, "n_funds": 1,
            "underlying_return_d1": -0.08, "net_moc_pct_letf_aum": -0.48,
            "net_moc_5d_dollars": -3_840_000.0, "net_moc_20d_dollars": -3_840_000.0,
            "net_moc_60d_dollars": -3_840_000.0, "net_moc_z_60d": None,
            "underlying_dollar_adv_20d": 96_000_000.0,
            "net_moc_pct_adv_20d": -0.04,
        },
    ])

    daily_parquet = tmp_path / "letf_rebalance_flows_daily.parquet"
    daily_json = tmp_path / "letf_rebalance_flows_daily.json"
    latest_json = tmp_path / "letf_rebalance_flows_latest.json"

    flows.write_outputs(
        fund_flows, aggregates,
        daily_parquet=daily_parquet,
        daily_json=daily_json,
        latest_json=latest_json,
        json_days=20,
    )
    payload = __import__("json").loads(latest_json.read_text(encoding="utf-8"))
    assert payload["latest_date"] == "2026-05-19"
    assert "APLD" in payload["by_underlying"], "APLD must surface even when its latest agg is older"
    assert payload["by_underlying"]["APLD"]["date"] == "2026-05-18"
    assert payload["by_underlying"]["APLD"]["is_latest_global"] is False
    assert payload["by_underlying"]["SPY"]["is_latest_global"] is True
    assert payload["by_underlying"]["APLD"]["net_moc_pct_adv_20d"] == pytest.approx(-0.04)
    assert payload["by_underlying"]["SPY"]["underlying_dollar_adv_20d"] == pytest.approx(30_000_000_000.0)


def _yf_panel_legacy_multiindex() -> pd.DataFrame:
    idx = pd.to_datetime(["2026-05-16", "2026-05-19"])
    return pd.DataFrame(
        {
            ("SPY", "Close"): [500.0, 505.0],
            ("SPY", "Volume"): [80_000_000, 90_000_000],
            ("AAPL", "Close"): [200.0, 210.0],
            ("AAPL", "Volume"): [50_000_000, 55_000_000],
        },
        index=idx,
    )


def _yf_panel_price_first_multiindex() -> pd.DataFrame:
    idx = pd.to_datetime(["2026-05-16", "2026-05-19"])
    return pd.DataFrame(
        {
            ("Close", "SPY"): [500.0, 505.0],
            ("Volume", "SPY"): [80_000_000, 90_000_000],
            ("Close", "AAPL"): [200.0, 210.0],
            ("Volume", "AAPL"): [50_000_000, 55_000_000],
        },
        index=idx,
    )


def _yf_panel_flat_single_ticker() -> pd.DataFrame:
    idx = pd.DatetimeIndex(["2026-05-16", "2026-05-19"])
    return pd.DataFrame({"Close": [500.0, 505.0], "Volume": [80_000_000, 90_000_000]}, index=idx)


def test_extract_yf_close_volume_long_legacy_multiindex():
    out = flows._extract_yf_close_volume_long(_yf_panel_legacy_multiindex(), ["SPY", "AAPL"])
    assert set(out["underlying"]) == {"SPY", "AAPL"}
    assert set(out["date"]) == {"2026-05-16", "2026-05-19"}
    assert len(out) == 4
    spy = out[out["underlying"].eq("SPY")].sort_values("date")
    assert spy.iloc[-1]["close"] == pytest.approx(505.0)
    assert spy.iloc[-1]["volume"] == pytest.approx(90_000_000)


def test_extract_yf_close_volume_long_price_first_multiindex():
    out = flows._extract_yf_close_volume_long(_yf_panel_price_first_multiindex(), ["SPY", "AAPL"])
    assert set(out["underlying"]) == {"SPY", "AAPL"}
    spy = out[out["underlying"].eq("SPY")].sort_values("date")
    assert spy.iloc[-1]["close"] == pytest.approx(505.0)


def test_extract_yf_close_volume_long_flat_single_ticker():
    out = flows._extract_yf_close_volume_long(_yf_panel_flat_single_ticker(), ["SPY"])
    assert out["underlying"].tolist() == ["SPY", "SPY"]
    assert out.iloc[-1]["date"] == "2026-05-19"
    assert out.iloc[-1]["close"] == pytest.approx(505.0)


def test_fetch_underlying_volume_panel_uses_yfinance_download(monkeypatch):
    calls: list[list[str]] = []

    def fake_download(tickers, **_kwargs):
        calls.append(list(tickers))
        return _yf_panel_legacy_multiindex()

    monkeypatch.setitem(sys.modules, "yfinance", type("yf", (), {"download": staticmethod(fake_download)}))

    out = flows.fetch_underlying_volume_panel(["SPY", "AAPL"], lookback_days=5, batch_size=50)
    assert calls == [["AAPL", "SPY"]]
    assert set(out["underlying"]) == {"SPY", "AAPL"}
    assert "dollar_volume" in out.columns
    assert out.iloc[-1]["dollar_volume"] == pytest.approx(505.0 * 90_000_000)

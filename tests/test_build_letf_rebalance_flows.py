"""Unit tests for leveraged-ETF close rebalance-flow estimates."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from datetime import UTC, date, datetime

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


def test_annotate_with_adv_adds_tradable_float_ratios():
    fund = flows.build_fund_flows(_universe(), _metrics())
    agg = flows.build_underlying_aggregates(fund)
    adv = pd.DataFrame([
        {
            "date": "2026-05-19",
            "underlying": "SPY",
            "underlying_dollar_adv_20d": 30_000_000_000.0,
            "underlying_dollar_median_adv_20d": 28_000_000_000.0,
            "tradable_float_shares": 900_000_000.0,
            "shares_outstanding_underlying": 1_000_000_000.0,
            "tradable_float_dollars": 90_000_000_000.0,
            "tradable_float_source": "unit_float",
        }
    ])
    fund2, agg2 = flows.annotate_with_adv(fund, agg, adv)
    day = agg2[(agg2["date"].eq("2026-05-19")) & (agg2["underlying"].eq("SPY"))].iloc[0]

    assert day["net_moc_dollars"] == pytest.approx(180_000_000.0)
    assert day["net_moc_pct_tradable_float"] == pytest.approx(180_000_000.0 / 90_000_000_000.0)
    assert day["underlying_tradable_float_dollars"] == pytest.approx(90_000_000_000.0)
    upro = fund2[(fund2["date"].eq("2026-05-19")) & (fund2["ticker"].eq("UPRO"))].iloc[0]
    assert upro["rebalance_pct_tradable_float"] == pytest.approx(60_000_000.0 / 90_000_000_000.0)


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


def test_session_extend_enables_flow_on_global_session():
    from ingest_etf_metrics import extend_metrics_session_coverage

    metrics = pd.DataFrame([
        {
            "date": pd.Timestamp("2026-05-28"),
            "ticker": "NOWL",
            "nav": 5.66,
            "aum": 218_023_700.0,
            "shares_outstanding": 38_500_000.0,
            "underlying_adj_close": 100.0,
            "stale": False,
            "stale_age_bdays": 0,
            "stale_kind": "issuer_lag",
            "source_provider": "granite_shares",
            "status": "ok",
        },
    ])
    extended = extend_metrics_session_coverage(
        metrics,
        session_date=date(2026, 5, 29),
        tickers=["NOWL"],
        max_lag_bdays=2,
    )
    assert len(extended) == 2
    row = extended[extended["date"] == date(2026, 5, 29)].iloc[0]
    assert row["stale_kind"] == "issuer_session_extend"
    assert isinstance(row["date"], date)
    assert float(row["aum"]) == pytest.approx(218_023_700.0)


def test_build_fund_flows_accepts_python_date_after_session_extend():
    """Regression: nightly failed when session-extend left object-dtype dates.

    ``extend_metrics_session_coverage`` stores plain ``datetime.date`` values.
    ``build_fund_flows`` must coerce before formatting (not call ``.dt`` on object).
    """
    from ingest_etf_metrics import extend_metrics_session_coverage

    metrics = _metrics().copy()
    # Drop the later session so extend can synthesize it as python date.
    metrics = metrics[metrics["date"] < pd.Timestamp("2026-05-19")].reset_index(drop=True)
    extended = extend_metrics_session_coverage(
        metrics,
        session_date=date(2026, 5, 19),
        tickers=["UPRO", "SPXU", "YSPY", "SPY"],
        max_lag_bdays=2,
    )
    assert extended["date"].map(lambda d: isinstance(d, date)).all()

    # Patch underlying prices on the extended session so return is computable.
    mask = extended["date"].eq(date(2026, 5, 19))
    extended.loc[mask, "underlying_adj_close"] = 101.0
    extended.loc[mask, "nav"] = 101.0

    fund = flows.build_fund_flows(_universe(), extended)
    assert not fund.empty
    assert fund["date"].eq("2026-05-19").any()
    upro = fund[(fund["date"].eq("2026-05-19")) & (fund["ticker"].eq("UPRO"))].iloc[0]
    assert upro["quality_flag"] == "ok"
    assert float(upro["rebalance_signed_dollars"]) == pytest.approx(60_000_000.0)


def test_dates_as_yyyy_mm_dd_handles_mixed_date_types():
    s = pd.Series([date(2026, 5, 19), pd.Timestamp("2026-05-20"), "2026-05-21"])
    out = flows._dates_as_yyyy_mm_dd(s)
    assert out.tolist() == ["2026-05-19", "2026-05-20", "2026-05-21"]


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


def test_resolve_float_quality_etf_is_unreliable():
    res = flows.resolve_float_quality(
        float_shares_raw=None, shares_out=7_650_000.0, chosen_shares=7_650_000.0,
        float_dollars=4_848_000_000.0, adv=6_841_000_000.0, is_etf=True,
        source="yfinance_shares_outstanding",
    )
    assert res["reliable"] is False
    assert res["quality"] == "etf_elastic"


def test_resolve_float_quality_adv_exceeds_float_is_unreliable():
    # SOXX-like: ADV ($6.84B) > float MV ($4.85B) => cannot be a real free float.
    res = flows.resolve_float_quality(
        float_shares_raw=None, shares_out=7_650_000.0, chosen_shares=7_650_000.0,
        float_dollars=4_848_000_000.0, adv=6_841_000_000.0, is_etf=False,
        source="yfinance_shares_outstanding",
    )
    assert res["reliable"] is False
    assert res["quality"] == "adv_exceeds_float"


def test_resolve_float_quality_falls_back_when_floatshares_too_small():
    # BRK-B-like: floatShares 1.16M is implausibly small vs 1.4B shares out.
    res = flows.resolve_float_quality(
        float_shares_raw=1_158_277.0, shares_out=1_398_308_677.0,
        chosen_shares=1_158_277.0, float_dollars=575_513_087.0,
        adv=2_705_628_116.0, is_etf=False, source="yfinance_float_shares",
    )
    assert res["source"] == "yfinance_shares_outstanding_fallback"
    assert res["shares"] == pytest.approx(1_398_308_677.0)
    # price ~ $497 * 1.4B shares = ~$695B float MV, well above ADV => reliable.
    assert res["reliable"] is True
    assert res["dollars"] > 6.0e11


def test_resolve_float_quality_ok_single_name():
    res = flows.resolve_float_quality(
        float_shares_raw=2_625_438_772.0, shares_out=3_755_723_871.0,
        chosen_shares=2_625_438_772.0, float_dollars=1_088_231_230_980.0,
        adv=18_519_027_070.0, is_etf=False, source="yfinance_float_shares",
    )
    assert res["reliable"] is True
    assert res["quality"] == "ok"


def test_resolve_float_quality_missing():
    res = flows.resolve_float_quality(
        float_shares_raw=None, shares_out=None, chosen_shares=None,
        float_dollars=None, adv=2_676_000_000.0, is_etf=False, source=None,
    )
    assert res["reliable"] is False
    assert res["quality"] == "missing"


def test_session_state_for_date_past_is_final_future_is_forming():
    now = datetime(2026, 6, 30, 14, 56, tzinfo=UTC)  # 10:56 ET, before close
    assert flows.session_state_for_date("2026-06-29", now=now) == "final"
    assert flows.session_state_for_date("2026-06-30", now=now) == "forming"
    after_close = datetime(2026, 6, 30, 21, 0, tzinfo=UTC)  # ~17:00 ET (EDT)
    assert flows.session_state_for_date("2026-06-30", now=after_close) == "final"


def test_apply_float_quality_suppresses_unreliable_pct():
    df = pd.DataFrame([
        {  # ETF underlying -> suppressed
            "underlying": "SOXX", "net_moc_pct_tradable_float": 1.10,
            "underlying_tradable_float_dollars": 4.85e9, "tradable_float_shares": 7.65e6,
            "shares_outstanding_underlying": 7.65e6, "float_shares_raw": np.nan,
            "is_etf": True, "tradable_float_source": "yfinance_shares_outstanding",
            "underlying_dollar_adv_20d": 6.84e9,
        },
        {  # clean single name -> kept
            "underlying": "TSLA", "net_moc_pct_tradable_float": 4.8e-5,
            "underlying_tradable_float_dollars": 1.088e12, "tradable_float_shares": 2.6e9,
            "shares_outstanding_underlying": 3.7e9, "float_shares_raw": 2.6e9,
            "is_etf": False, "tradable_float_source": "yfinance_float_shares",
            "underlying_dollar_adv_20d": 1.85e10,
        },
    ])
    out = flows._apply_float_quality(df, pct_col="net_moc_pct_tradable_float").set_index("underlying")
    assert bool(out.loc["SOXX", "tradable_float_reliable"]) is False
    assert pd.isna(out.loc["SOXX", "net_moc_pct_tradable_float"])
    assert bool(out.loc["TSLA", "tradable_float_reliable"]) is True
    assert out.loc["TSLA", "net_moc_pct_tradable_float"] == pytest.approx(4.8e-5)


def test_annotate_with_adv_adds_auction_and_physical_stats():
    fund = flows.build_fund_flows(_universe(), _metrics())
    agg = flows.build_underlying_aggregates(fund)
    adv = pd.DataFrame([
        {"date": "2026-05-19", "underlying": "SPY", "underlying_dollar_adv_20d": 30_000_000_000.0},
    ])
    fund2, agg2 = flows.annotate_with_adv(fund, agg, adv)
    spy = agg2[(agg2["date"].eq("2026-05-19")) & (agg2["underlying"].eq("SPY"))].iloc[0]
    net = 180_000_000.0
    auction = 30_000_000_000.0 * flows._AUCTION_SHARE_OF_ADV
    assert spy["net_moc_pct_auction_volume"] == pytest.approx(net / auction)
    assert spy["net_moc_physical_dollars"] == pytest.approx(net * (1.0 - flows._SWAP_HEDGE_SHARE))
    assert spy["net_moc_pct_adv_physical"] == pytest.approx(
        net * (1.0 - flows._SWAP_HEDGE_SHARE) / 30_000_000_000.0
    )
    upro = fund2[(fund2["date"].eq("2026-05-19")) & (fund2["ticker"].eq("UPRO"))].iloc[0]
    assert upro["underlying_dollar_auction_est"] == pytest.approx(auction)
    assert upro["rebalance_pct_auction_volume"] == pytest.approx(60_000_000.0 / auction)


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

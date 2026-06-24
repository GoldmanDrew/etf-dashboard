"""Regression: repair_shares_vs_aum_nav fixes decimal-shifted share counts."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import ingest_etf_metrics as iem  # noqa: E402


def test_fetch_underlying_adj_close_batch_chunks_yfinance_calls(monkeypatch):
    """Large underlying sets must not rely on a single yfinance bulk download."""
    captured: list[list[str]] = []

    def fake_download(tickers, start, end, *, auto_adjust):
        captured.append(list(tickers))
        return None

    monkeypatch.setenv("ETF_METRICS_UNDERLYING_YF_CHUNK_SIZE", "2")
    monkeypatch.setattr(iem, "_yf_download_ohlcv", fake_download)
    syms = ["SPY", "QQQ", "IWM", "DIA", "VOO"]
    out = iem.fetch_underlying_adj_close_batch(
        syms, date(2026, 1, 1), date(2026, 1, 5),
    )
    assert out.empty
    assert len(captured) == 3
    assert [len(c) for c in captured] == [2, 2, 1]


def test_filter_metrics_to_nyse_sessions_drops_juneteenth():
    df = pd.DataFrame(
        [
            {"date": date(2026, 6, 18), "ticker": "AAA"},
            {"date": date(2026, 6, 19), "ticker": "AAA"},
            {"date": date(2026, 6, 20), "ticker": "AAA"},
        ]
    )
    out, n = iem.filter_metrics_to_nyse_sessions(df)
    assert n == 2
    assert out["date"].astype(str).tolist() == ["2026-06-18"]


def test_clear_issuer_early_market_fields():
    df = pd.DataFrame(
        [
            {
                "date": date(2026, 6, 24),
                "ticker": "AAPU",
                "stale_kind": "issuer_early",
                "close_price": 37.11,
                "etf_adj_close": 37.34,
                "shares_traded": 123.0,
                "underlying_adj_close": 297.1,
            }
        ]
    )
    out, n = iem.clear_issuer_early_market_fields(df)
    assert n == 1
    assert pd.isna(out.iloc[0]["close_price"])
    assert pd.isna(out.iloc[0]["shares_traded"])


def test_fetch_close_prices_batch_chunks_yfinance_calls(monkeypatch):
    """ETF close/volume batch must not rely on a single yfinance bulk download."""
    captured: list[list[str]] = []

    def fake_download(tickers, start, end, *, auto_adjust):
        captured.append(list(tickers))
        return None

    monkeypatch.setenv("ETF_METRICS_CLOSE_YF_CHUNK_SIZE", "2")
    monkeypatch.setattr(iem, "_yf_download_ohlcv", fake_download)
    syms = ["SPY", "QQQ", "IWM", "DIA", "VOO"]
    out = iem.fetch_close_prices_batch(
        syms, date(2026, 1, 1), date(2026, 1, 5),
    )
    assert out.empty
    assert len(captured) == 3
    assert [len(c) for c in captured] == [2, 2, 1]


def test_backfill_underlying_adj_close_gaps_fetches_per_underlying(monkeypatch):
    df = pd.DataFrame(
        [
            {
                "date": "2026-04-20",
                "ticker": "APLZ",
                "nav": 10.0,
                "aum": 1e8,
                "shares_outstanding": 1e7,
                "close_price": 10.1,
                "underlying_adj_close": None,
                "stale": False,
                "stale_age_bdays": None,
                "source_provider": "x",
                "source_url": "",
                "ingested_at_utc": "2026-04-27T00:00:00Z",
                "status": "ok",
            },
            {
                "date": "2026-04-21",
                "ticker": "APLZ",
                "nav": 10.1,
                "aum": 1e8,
                "shares_outstanding": 1e7,
                "close_price": 10.2,
                "underlying_adj_close": None,
                "stale": False,
                "stale_age_bdays": None,
                "source_provider": "x",
                "source_url": "",
                "ingested_at_utc": "2026-04-27T00:00:00Z",
                "status": "ok",
            },
        ]
    )
    calls: list[tuple[str, date, date]] = []

    def fake_fetch(syms, start, end):
        calls.append((tuple(syms), start, end))
        rows = []
        for i, d in enumerate(pd.date_range(start, end, freq="D")):
            rows.append(
                {"date": d.date(), "ticker": syms[0], "underlying_adj_close": 33.0 + i * 0.1},
            )
        return pd.DataFrame(rows)

    monkeypatch.setattr(iem, "fetch_underlying_adj_close_batch", fake_fetch)
    out = iem.backfill_underlying_adj_close_gaps(df, {"APLZ": "APLD"})
    assert len(calls) >= 1
    assert calls[0][0] == ("APLD",)
    assert float(out.iloc[0]["underlying_adj_close"]) > 32


def test_merge_underlying_adj_close_joins_on_underlying_ticker():
    df = pd.DataFrame(
        [
            {
                "date": "2026-04-20",
                "ticker": "SSO",
                "nav": 100.0,
                "aum": 1e9,
                "shares_outstanding": 1e7,
                "close_price": 101.0,
                "underlying_adj_close": None,
                "stale": False,
                "stale_age_bdays": None,
                "source_provider": "x",
                "source_url": "",
                "ingested_at_utc": "2026-04-27T00:00:00Z",
                "status": "ok",
            }
        ]
    )
    und_df = pd.DataFrame(
        [
            {"date": date.fromisoformat("2026-04-20"), "ticker": "SPY", "underlying_adj_close": 500.25},
        ]
    )
    out = iem.merge_underlying_adj_close(df, und_df, {"SSO": "SPY"})
    assert abs(float(out.iloc[0]["underlying_adj_close"]) - 500.25) < 1e-6


def test_repair_shares_decimal_shift():
    df = pd.DataFrame(
        [
            {
                "date": "2026-04-20",
                "ticker": "TEST",
                "nav": 6.58,
                "aum": 574_477.0,
                "shares_outstanding": 87_333_079.0,
                "close_price": None,
                "underlying_adj_close": None,
                "stale": False,
                "stale_age_bdays": None,
                "source_provider": "x",
                "source_url": "",
                "ingested_at_utc": "2026-04-27T00:00:00Z",
                "status": "ok",
            }
        ]
    )
    out, n = iem.repair_shares_vs_aum_nav(df)
    assert n == 1
    fixed = float(out.iloc[0]["shares_outstanding"])
    assert abs(fixed - (574_477.0 / 6.58)) < 1.0


def test_dedupe_metric_rows_prefers_complete_ok_non_stale():
    df = pd.DataFrame(
        [
            {
                "date": "2026-06-19",
                "ticker": "CRMU",
                "nav": 10.0,
                "aum": None,
                "shares_outstanding": None,
                "shares_traded": None,
                "close_price": 10.0,
                "etf_adj_close": None,
                "underlying_adj_close": None,
                "stale": True,
                "stale_age_bdays": 1,
                "stale_kind": "carry_forward",
                "source_provider": "carry_forward",
                "source_url": "",
                "ingested_at_utc": "2026-06-20T00:00:00Z",
                "status": "partial",
            },
            {
                "date": "2026-06-19",
                "ticker": "CRMU",
                "nav": 10.1,
                "aum": 1_010_000.0,
                "shares_outstanding": 100_000.0,
                "shares_traded": 1234,
                "close_price": 10.12,
                "etf_adj_close": 10.12,
                "underlying_adj_close": 50.0,
                "stale": False,
                "stale_age_bdays": None,
                "stale_kind": None,
                "source_provider": "polygon",
                "source_url": "",
                "ingested_at_utc": "2026-06-20T00:01:00Z",
                "status": "ok",
            },
        ]
    )
    out, removed = iem.dedupe_metric_rows(df, context="test")
    assert removed == 1
    assert len(out) == 1
    assert out.iloc[0]["source_provider"] == "polygon"
    iem.validate_df(out)


def test_merge_close_prices_aligns_yahoo_to_rex_as_of_before_calendar_date():
    """When ``#as_of`` lags the row calendar date, Yahoo close must match that session (EOSU)."""
    df = pd.DataFrame(
        [
            {
                "date": date(2026, 5, 8),
                "ticker": "EOSU",
                "nav": 33.52,
                "aum": 1.0,
                "shares_outstanding": 1.0,
                "close_price": None,
                "underlying_adj_close": None,
                "stale": True,
                "stale_age_bdays": 1,
                "source_provider": "rex_shares",
                "source_url": "https://www.rexshares.com/EOSU/#as_of=2026-05-07",
                "ingested_at_utc": "2026-05-10T00:00:00Z",
                "status": "ok",
            }
        ]
    )
    close_df = pd.DataFrame(
        [
            {"date": date(2026, 5, 7), "ticker": "EOSU", "close_price": 33.52, "shares_traded": 100.0},
            {"date": date(2026, 5, 8), "ticker": "EOSU", "close_price": 50.4, "shares_traded": 200.0},
        ]
    )
    out = iem.merge_close_prices(df, close_df)
    assert float(out.iloc[0]["close_price"]) == 33.52
    assert float(out.iloc[0]["shares_traded"]) == 100.0


def test_merge_underlying_adj_close_aligns_to_as_of_session():
    df = pd.DataFrame(
        [
            {
                "date": date(2026, 5, 8),
                "ticker": "EOSU",
                "nav": 33.52,
                "aum": 1.0,
                "shares_outstanding": 1.0,
                "close_price": None,
                "underlying_adj_close": None,
                "stale": True,
                "stale_age_bdays": 1,
                "source_provider": "rex_shares",
                "source_url": "https://www.rexshares.com/EOSU/#as_of=2026-05-07",
                "ingested_at_utc": "2026-05-10T00:00:00Z",
                "status": "ok",
            }
        ]
    )
    und_df = pd.DataFrame(
        [
            {"date": date(2026, 5, 7), "ticker": "EOSE", "underlying_adj_close": 6.36},
            {"date": date(2026, 5, 8), "ticker": "EOSE", "underlying_adj_close": 8.01},
        ]
    )
    out = iem.merge_underlying_adj_close(df, und_df, {"EOSU": "EOSE"})
    assert abs(float(out.iloc[0]["underlying_adj_close"]) - 6.36) < 1e-9


def test_validate_df_warns_on_misaligned_as_of(caplog):
    """Going-forward guard: warn (not raise) when row.date != #as_of session."""
    import logging

    df = pd.DataFrame(
        [
            {
                "date": date(2026, 5, 6),
                "ticker": "EOSU",
                "nav": 31.69,
                "aum": 7_757_141.58,
                "shares_outstanding": 244_782.0,
                "shares_traded": 20_300.0,
                "close_price": 31.689,
                "underlying_adj_close": 6.23,
                "stale": True,
                "stale_age_bdays": 1,
                "source_provider": "rex_shares",
                "source_url": "https://www.rexshares.com/EOSU/#as_of=2026-05-05",
                "ingested_at_utc": "2026-05-07T00:00:00Z",
                "status": "ok",
            },
        ]
    )
    caplog.set_level(logging.WARNING, logger=iem.LOGGER.name)
    iem.validate_df(df)
    msgs = [r.getMessage() for r in caplog.records]
    assert any("misaligned to #as_of" in m for m in msgs), msgs


def test_validate_df_skips_carry_forward_for_misalignment_warning(caplog):
    """``carry_forward://...?from=`` rows are not flagged as misaligned."""
    import logging

    df = pd.DataFrame(
        [
            {
                "date": date(2026, 5, 9),
                "ticker": "ZZZ",
                "nav": 10.0,
                "aum": 1e6,
                "shares_outstanding": 100_000.0,
                "shares_traded": 50.0,
                "close_price": None,
                "underlying_adj_close": None,
                "stale": True,
                "stale_age_bdays": 1,
                "source_provider": "carry_forward",
                "source_url": "carry_forward://ZZZ?from=2026-05-08",
                "ingested_at_utc": "2026-05-10T00:00:00Z",
                "status": "ok",
            },
        ]
    )
    caplog.set_level(logging.WARNING, logger=iem.LOGGER.name)
    iem.validate_df(df)
    msgs = [r.getMessage() for r in caplog.records]
    assert not any("misaligned to #as_of" in m for m in msgs), msgs


def test_merge_close_prices_carry_forward_from_session():
    df = pd.DataFrame(
        [
            {
                "date": date(2026, 5, 9),
                "ticker": "ZZZ",
                "nav": 10.0,
                "aum": 1e6,
                "shares_outstanding": 100_000.0,
                "close_price": None,
                "underlying_adj_close": None,
                "stale": True,
                "stale_age_bdays": 1,
                "source_provider": "carry_forward",
                "source_url": "carry_forward://ZZZ?from=2026-05-08",
                "ingested_at_utc": "2026-05-10T00:00:00Z",
                "status": "ok",
            }
        ]
    )
    close_df = pd.DataFrame(
        [
            {"date": date(2026, 5, 8), "ticker": "ZZZ", "close_price": 10.05, "shares_traded": 50.0},
            {"date": date(2026, 5, 9), "ticker": "ZZZ", "close_price": 11.0, "shares_traded": 60.0},
        ]
    )
    out = iem.merge_close_prices(df, close_df)
    assert float(out.iloc[0]["close_price"]) == 10.05


def test_backfill_shares_traded_gaps_fetches_volume(monkeypatch):
    df = pd.DataFrame(
        [
            {
                "date": date(2026, 5, 11),
                "ticker": "EOSU",
                "nav": 57.5,
                "aum": 1e8,
                "shares_outstanding": 1.7e6,
                "close_price": 57.62,
                "shares_traded": None,
                "underlying_adj_close": 100.0,
                "stale": False,
                "stale_age_bdays": None,
                "source_provider": "rex",
                "source_url": "https://www.rexshares.com/EOSU/#as_of=2026-05-11",
                "ingested_at_utc": "2026-05-12T00:00:00Z",
                "status": "ok",
            },
        ]
    )

    def fake_fetch(tickers, start, end):
        assert "EOSU" in tickers
        return pd.DataFrame(
            [
                {
                    "date": date(2026, 5, 11),
                    "ticker": "EOSU",
                    "close_price": 57.62,
                    "shares_traded": 147_100.0,
                },
            ]
        )

    monkeypatch.setattr(iem, "fetch_close_prices_batch", fake_fetch)
    out, n = iem.backfill_shares_traded_gaps(df)
    assert n == 1
    assert float(out.iloc[0]["shares_traded"]) == 147_100.0


def test_backfill_shares_traded_gaps_skips_when_disabled(monkeypatch):
    monkeypatch.setenv("ETF_METRICS_DISABLE_YFINANCE", "1")
    df = pd.DataFrame(
        [
            {
                "date": date(2026, 5, 11),
                "ticker": "EOSU",
                "nav": 1.0,
                "aum": 1.0,
                "shares_outstanding": 1.0,
                "close_price": 2.0,
                "shares_traded": None,
                "underlying_adj_close": None,
                "stale": False,
                "stale_age_bdays": None,
                "source_provider": "x",
                "source_url": "",
                "ingested_at_utc": "2026-05-12T00:00:00Z",
                "status": "ok",
            },
        ]
    )
    out, n = iem.backfill_shares_traded_gaps(df)
    assert n == 0
    assert out.iloc[0]["shares_traded"] is None or pd.isna(out.iloc[0]["shares_traded"])

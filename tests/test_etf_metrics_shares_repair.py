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

"""Parallel single-day ingest matches sequential output and preserves order."""
from __future__ import annotations

import os
import sys
import time
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from etf_providers import ProviderResult  # noqa: E402
import ingest_etf_metrics as iem  # noqa: E402


class _StubProvider:
    name = "stub"

    def __init__(self, delay_sec: float = 0.0):
        self.delay_sec = delay_sec

    def supports_ticker(self, ticker: str, as_of: date) -> bool:
        return True

    def fetch_for_date(self, ticker: str, as_of: date) -> ProviderResult:
        if self.delay_sec:
            time.sleep(self.delay_sec)
        nav = float(len(ticker) + as_of.day)
        return ProviderResult(
            as_of,
            ticker.upper(),
            nav,
            nav * 1_000_000,
            1_000_000.0,
            self.name,
            f"stub://{ticker}",
            "ok",
        )


@pytest.fixture(autouse=True)
def _reset_ingest_env(monkeypatch):
    monkeypatch.delenv("ETF_METRICS_INGEST_WORKERS", raising=False)
    monkeypatch.delenv("ETF_METRICS_INGEST_MIN_INTERVAL_SEC", raising=False)
    monkeypatch.setattr(iem, "_INGEST_WORKERS", 1, raising=False)
    monkeypatch.setattr(iem, "_INGEST_MIN_INTERVAL_SEC", 0.0, raising=False)
    monkeypatch.setattr(iem, "_ingest_last_slot", 0.0, raising=False)
    if hasattr(iem._thread_local, "providers"):
        del iem._thread_local.providers


def test_ingest_worker_count_defaults_to_one(monkeypatch):
    monkeypatch.delenv("ETF_METRICS_INGEST_WORKERS", raising=False)
    monkeypatch.setattr(iem, "_INGEST_WORKERS", max(1, int(os.getenv("ETF_METRICS_INGEST_WORKERS", "1"))))
    assert iem.ingest_worker_count() == 1


def test_parallel_single_day_matches_sequential(monkeypatch):
    tickers = ["AAA", "BBB", "CCC", "DDD", "EEE"]
    end = date(2026, 6, 30)
    stack = [_StubProvider(delay_sec=0.01)]

    monkeypatch.setattr(iem, "_INGEST_WORKERS", 1, raising=False)
    seq = iem.ingest(
        tickers,
        lookback_days=1,
        polygon_lookback_days=1,
        start_date=end,
        end_date=end,
        providers=stack,
    )

    monkeypatch.setattr(iem, "_INGEST_WORKERS", 4, raising=False)
    monkeypatch.setattr(iem, "build_default_stack", lambda: [_StubProvider(delay_sec=0.01)])
    if hasattr(iem._thread_local, "providers"):
        del iem._thread_local.providers
    par = iem.ingest(
        tickers,
        lookback_days=1,
        polygon_lookback_days=1,
        start_date=end,
        end_date=end,
        providers=None,
    )

    assert seq["ticker"].tolist() == par["ticker"].tolist() == tickers
    assert seq[["ticker", "nav", "status"]].equals(par[["ticker", "nav", "status"]])


def test_ingest_rate_limiter_serializes_slots(monkeypatch):
    monkeypatch.setattr(iem, "_INGEST_MIN_INTERVAL_SEC", 0.05, raising=False)
    t0 = time.monotonic()
    iem._ingest_rate_limit_wait()
    iem._ingest_rate_limit_wait()
    assert time.monotonic() - t0 >= 0.04

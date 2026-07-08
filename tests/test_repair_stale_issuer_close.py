from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from ingest_etf_metrics import merge_close_prices, repair_stale_issuer_close_from_market


def test_merge_close_prices_prefers_yahoo_when_issuer_lag():
    df = pd.DataFrame(
        [
            {
                "date": date(2026, 7, 1),
                "ticker": "CORD",
                "nav": 6.79,
                "close_price": 6.78,
                "stale_kind": "issuer_lag",
                "source_url": "https://www.rexshares.com/CORD/#as_of=2026-06-20",
            }
        ]
    )
    close_df = pd.DataFrame(
        [{"date": date(2026, 7, 1), "ticker": "CORD", "close_price": 7.12, "shares_traded": 1000}]
    )
    out = merge_close_prices(df, close_df)
    assert float(out.iloc[0]["close_price"]) == 7.12


def test_repair_stale_issuer_close_from_market_overwrites_frozen_close(monkeypatch):
    df = pd.DataFrame(
        [
            {
                "date": date(2026, 7, 1),
                "ticker": "CORD",
                "nav": 6.79,
                "close_price": 6.78,
                "stale": True,
                "stale_kind": "issuer_lag",
                "source_provider": "rex_shares",
            }
        ]
    )

    def _fake_batch(tickers, start, end):
        assert tickers == ["CORD"]
        return pd.DataFrame(
            [{"date": date(2026, 7, 1), "ticker": "CORD", "close_price": 7.05}]
        )

    monkeypatch.setattr(
        "ingest_etf_metrics.fetch_close_prices_batch",
        _fake_batch,
    )
    out, n = repair_stale_issuer_close_from_market(df, lookback_calendar_days=30)
    assert n == 1
    assert float(out.iloc[0]["close_price"]) == 7.05

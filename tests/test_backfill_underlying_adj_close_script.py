"""Smoke tests for ``scripts/backfill_underlying_adj_close.py``."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))


def test_backfill_script_noop_when_yfinance_disabled(monkeypatch):
    import backfill_underlying_adj_close as mod

    monkeypatch.setenv("ETF_METRICS_DISABLE_YFINANCE", "1")
    called = []

    def boom():
        called.append(1)
        raise AssertionError("should not load parquet")

    monkeypatch.setattr(mod, "load_existing", boom)
    mod.main()
    assert called == []


def test_backfill_script_writes_when_rows_added(tmp_path, monkeypatch):
    import backfill_underlying_adj_close as mod
    import ingest_etf_metrics as iem

    monkeypatch.delenv("ETF_METRICS_DISABLE_YFINANCE", raising=False)
    pq = tmp_path / "etf_metrics_daily.parquet"
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
        ]
    )
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df.to_parquet(pq, index=False)

    monkeypatch.setattr(iem, "PARQUET_PATH", pq)
    monkeypatch.setattr(mod, "PARQUET_PATH", pq)

    def fake_bf(frame, umap):
        out = frame.copy()
        out["underlying_adj_close"] = 99.0
        return out

    saved: list[pd.DataFrame] = []

    def capture_save(d):
        saved.append(d.copy())

    monkeypatch.setattr(mod, "load_existing", lambda: pd.read_parquet(pq))
    monkeypatch.setattr(mod, "load_universe_underlying_map", lambda: {"APLZ": "APLD"})
    monkeypatch.setattr(mod, "backfill_underlying_adj_close_gaps", fake_bf)
    monkeypatch.setattr(mod, "validate_df", lambda d: None)
    monkeypatch.setattr(mod, "save_outputs", capture_save)

    mod.main()
    assert len(saved) == 1
    assert float(saved[0].iloc[0]["underlying_adj_close"]) == 99.0

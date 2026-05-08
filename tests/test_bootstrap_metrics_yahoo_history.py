"""Offline tests for ``scripts/bootstrap_metrics_yahoo_history.py`` (Yahoo monkeypatched)."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import bootstrap_metrics_yahoo_history as bmh  # noqa: E402
import ingest_etf_metrics as iem  # noqa: E402


def test_build_bootstrap_frame_missing_joint_rows(monkeypatch):
    def fake_fetch_etf(syms, start, end):
        assert syms == ["ZZZ"]
        assert start == date(2020, 1, 1)
        assert end == date(2020, 1, 2)
        return pd.DataFrame(
            [
                {"date": date(2020, 1, 1), "close_price": 10.0},
                {"date": date(2020, 1, 2), "close_price": 10.5},
            ],
        )

    monkeypatch.setattr(bmh, "fetch_close_prices_batch", fake_fetch_etf)

    und_long = pd.DataFrame(
        [
            {"date": date(2020, 1, 1), "ticker": "SPY", "underlying_adj_close": 400.0},
            {"date": date(2020, 1, 2), "ticker": "SPY", "underlying_adj_close": 401.0},
        ],
    )
    out = bmh._build_bootstrap_frame("ZZZ", "SPY", date(2020, 1, 1), date(2020, 1, 2), und_long)
    assert len(out) == 2
    assert list(out["status"].unique()) == ["missing"]
    assert list(out["source_provider"].unique()) == ["yahoo_bootstrap"]
    assert out["close_price"].notna().all() and (out["close_price"] > 0).all()
    assert out["underlying_adj_close"].notna().all() and (out["underlying_adj_close"] > 0).all()
    assert "shares_traded" in out.columns


def test_bootstrap_rows_upsert_before_existing_min():
    """Bootstrap-style rows merge before existing issuer window."""
    ingested = "2026-01-01T00:00:00+00:00"

    def _row(d, ticker, *, close, und, status, src):
        return {
            "date": d,
            "ticker": ticker,
            "nav": 50.0 if status == "ok" else None,
            "aum": 1e8 if status == "ok" else None,
            "shares_outstanding": 1e7 if status == "ok" else None,
            "shares_traded": 1e6 if status == "ok" else None,
            "close_price": close,
            "underlying_adj_close": und,
            "stale": False,
            "stale_age_bdays": None,
            "source_provider": src,
            "source_url": None,
            "ingested_at_utc": ingested,
            "status": status,
        }

    existing = pd.DataFrame(
        [
            _row(date(2024, 6, 10), "AAA", close=100.0, und=400.0, status="ok", src="issuer"),
        ],
    )
    incoming = pd.DataFrame(
        [
            _row(date(2024, 6, 3), "AAA", close=99.0, und=395.0, status="missing", src="yahoo_bootstrap"),
            _row(date(2024, 6, 4), "AAA", close=99.5, und=396.0, status="missing", src="yahoo_bootstrap"),
        ],
    )
    merged = iem.upsert(existing, incoming)
    assert merged["date"].min() == date(2024, 6, 3)
    assert len(merged) == 3
    pre = merged.loc[merged["date"] == date(2024, 6, 3)].iloc[0]
    assert pre["status"] == "missing"
    assert pre["source_provider"] == "yahoo_bootstrap"

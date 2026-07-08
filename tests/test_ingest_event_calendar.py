"""Tests for earnings universe + Nasdaq-only event calendar ingest."""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

from earnings_universe import load_bucket_underlyings, load_bucket_underlying_etfs  # noqa: E402
from ingest_event_calendar import build_known_calendar  # noqa: E402


def _write_universe_csv(path: Path) -> None:
    df = pd.DataFrame([
        {"ETF": "AMYY", "Underlying": "AMD", "bucket": "bucket_2"},
        {"ETF": "MSTY", "Underlying": "MSTR", "bucket": "bucket_2"},
        {"ETF": "CONY", "Underlying": "COIN", "bucket": "bucket_4"},
        {"ETF": "TQQQ", "Underlying": "QQQ", "bucket": "bucket_1"},
        {"ETF": "SQQQ", "Underlying": "QQQ", "bucket": "bucket_3"},
    ])
    df.to_csv(path, index=False)


def test_load_bucket_underlyings_filters_b2_b4(tmp_path):
    csv_path = tmp_path / "etf_screened_today.csv"
    _write_universe_csv(csv_path)
    unds = load_bucket_underlyings(csv_path=csv_path)
    assert unds == ["AMD", "COIN", "MSTR"]
    etfs = load_bucket_underlying_etfs(csv_path=csv_path)
    assert etfs["AMD"] == ["AMYY"]
    assert etfs["COIN"] == ["CONY"]


def test_build_known_calendar_nasdaq_only_confirmed(tmp_path, monkeypatch):
    csv_path = tmp_path / "etf_screened_today.csv"
    _write_universe_csv(csv_path)
    seed_path = tmp_path / "seed.json"
    seed_path.write_text(json.dumps({"items": []}), encoding="utf-8")

    monkeypatch.setattr(
        "ingest_event_calendar.load_bucket_underlyings",
        lambda buckets=(): ["AMD", "MSTR", "COIN"],
    )
    monkeypatch.setattr(
        "ingest_event_calendar.fetch_nasdaq_earnings_window",
        lambda symbols, start=None, days=21: {
            "AMD": [date(2026, 7, 29)],
            "MSTR": [],
            "COIN": [],
        },
    )
    monkeypatch.setattr(
        "ingest_event_calendar._historical_earnings_moves",
        lambda *_args, **_kwargs: None,
    )

    payload = build_known_calendar(seed_path=seed_path, sleep_sec=0.0)
    assert payload["live_source"] == "nasdaq_only"
    assert payload["source_stats"]["nasdaq"] == 1
    assert "yahoo" not in payload["source_stats"]
    amd = next(i for i in payload["items"] if i["underlying"] == "AMD")
    assert amd["confirmation"] == "confirmed"
    assert amd["source"] == "nasdaq_earnings"


def test_build_known_calendar_seed_fallback_when_nasdaq_misses(tmp_path, monkeypatch):
    seed_path = tmp_path / "seed.json"
    seed_path.write_text(
        json.dumps({
            "items": [{
                "underlying": "MSTR",
                "event_date": "2026-07-30",
                "confirmation": "projected",
                "source": "seed_quarterly",
                "historical_move_pct_mad": 0.11,
            }],
        }),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        "ingest_event_calendar.load_bucket_underlyings",
        lambda buckets=(): ["MSTR"],
    )
    monkeypatch.setattr(
        "ingest_event_calendar.fetch_nasdaq_earnings_window",
        lambda symbols, start=None, days=21: {"MSTR": []},
    )
    monkeypatch.setattr(
        "ingest_event_calendar._historical_earnings_moves",
        lambda *_args, **_kwargs: None,
    )

    payload = build_known_calendar(seed_path=seed_path, sleep_sec=0.0)
    assert payload["source_stats"]["seed"] == 1
    row = payload["items"][0]
    assert row["underlying"] == "MSTR"
    assert row["event_date"] == "2026-07-30"
    assert row["confirmation"] == "projected"


def test_refresh_seed_from_nasdaq_updates_confirmed(tmp_path, monkeypatch):
    from refresh_earnings_seed import refresh_seed_from_nasdaq

    seed_path = tmp_path / "earnings_calendar_seed.json"
    seed_path.write_text(
        json.dumps({
            "items": [{
                "underlying": "AMD",
                "event_date": "2026-06-01",
                "confirmation": "projected",
                "source": "seed_quarterly",
                "historical_move_pct_mad": 0.065,
            }],
        }),
        encoding="utf-8",
    )
    monkeypatch.setattr("refresh_earnings_seed.SEED_PATH", seed_path)
    monkeypatch.setattr(
        "refresh_earnings_seed._underlying_universe",
        lambda: ["AMD", "COIN"],
    )
    monkeypatch.setattr(
        "refresh_earnings_seed.fetch_nasdaq_earnings_window",
        lambda symbols, start=None, days=21: {
            "AMD": [date(2026, 7, 29)],
            "COIN": [],
        },
    )

    payload = refresh_seed_from_nasdaq(dry_run=True)
    amd = next(i for i in payload["items"] if i["underlying"] == "AMD")
    assert amd["confirmation"] == "confirmed"
    assert amd["event_date"] == "2026-07-29"
    assert amd["historical_move_pct_mad"] == 0.065
    assert payload["refresh_stats"]["confirmed"] == 1

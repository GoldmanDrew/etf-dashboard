from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from ingest_etf_metrics import fill_missing_shares_outstanding_from_aum_nav
from market_calendar import next_nyse_session
from repair_metrics_staleness import (
    list_stale_tickers,
    resolve_targeted_catchup_sessions,
    tail_staleness_report,
)


def test_fill_missing_shares_outstanding_from_aum_nav():
    df = pd.DataFrame(
        [
            {
                "date": date(2026, 7, 1),
                "ticker": "TEST",
                "nav": 10.0,
                "aum": 1000.0,
                "shares_outstanding": None,
            }
        ]
    )
    out, n = fill_missing_shares_outstanding_from_aum_nav(df)
    assert n == 1
    assert float(out.iloc[0]["shares_outstanding"]) == 100.0


def test_resolve_targeted_catchup_sessions_skips_when_current():
    global_max = date(2026, 7, 8)
    df = pd.DataFrame(
        [
            {"date": global_max, "ticker": "AAA"},
            {"date": global_max, "ticker": "BBB"},
        ]
    )
    sessions = resolve_targeted_catchup_sessions(
        df,
        {"AAA", "BBB"},
        min_days_behind=2,
        end=global_max,
    )
    assert sessions == []


def test_resolve_targeted_catchup_sessions_extends_stale_tail():
    df = pd.DataFrame(
        [
            {"date": date(2026, 7, 2), "ticker": "STALE"},
            {"date": date(2026, 7, 8), "ticker": "FRESH"},
        ]
    )
    sessions = resolve_targeted_catchup_sessions(
        df,
        {"STALE", "FRESH"},
        min_days_behind=2,
        end=date(2026, 7, 8),
    )
    assert sessions
    assert sessions[0] == next_nyse_session(date(2026, 7, 2))
    assert sessions[-1] == date(2026, 7, 8)


def test_tail_staleness_report_counts_behind():
    df = pd.DataFrame(
        [
            {"date": date(2026, 7, 2), "ticker": "A"},
            {"date": date(2026, 7, 8), "ticker": "B"},
        ]
    )
    report = tail_staleness_report(df, {"A", "B"})
    assert report["universe_stale"] == 1
    assert report["worst"][0]["ticker"] == "A"
    stale = list_stale_tickers(df, {"A", "B"}, min_days_behind=2)
    assert len(stale) == 1

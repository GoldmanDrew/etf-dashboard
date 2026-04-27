"""Regression: repair_shares_vs_aum_nav fixes decimal-shifted share counts."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import ingest_etf_metrics as iem  # noqa: E402


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

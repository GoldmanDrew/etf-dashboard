"""Tests for borrow metrics supply join / utilization backfill."""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))

from borrow_metrics_enrichment import (  # noqa: E402
    join_supply_to_panel,
    utilization_coverage_fraction,
)


def test_merge_asof_backfills_utilization_on_staggered_dates():
    start = date(2025, 1, 1)
    panel_rows = []
    metric_rows = []
    for i in range(10):
        d = (start + timedelta(days=i * 2)).isoformat()
        panel_rows.append(
            {
                "date": d,
                "symbol": "AAA",
                "shares_available": 500_000 - i * 10_000,
                "borrow_current": 0.05,
            }
        )
        # Metrics only on even calendar days; borrow on staggered days still gets SO via asof.
        if i % 2 == 0:
            metric_rows.append(
                {
                    "date": d,
                    "ticker": "AAA",
                    "shares_outstanding": 2_000_000,
                    "nav": 25.0,
                    "aum": 50_000_000,
                    "close_price": 25.0,
                    "shares_traded": 100_000,
                }
            )
    panel = pd.DataFrame(panel_rows)
    panel["date"] = pd.to_datetime(panel["date"])
    metrics = pd.DataFrame(metric_rows)
    metrics["date"] = pd.to_datetime(metrics["date"])

    out = join_supply_to_panel(panel, metrics)
    cov = utilization_coverage_fraction(out)
    assert cov >= 0.8
    assert out["utilization_proxy"].notna().sum() >= 8


def test_utilization_zero_when_no_shares_outstanding():
    panel = pd.DataFrame(
        {
            "date": pd.to_datetime(["2025-01-01"]),
            "symbol": ["ZZZ"],
            "shares_available": [1000],
            "borrow_current": [0.1],
        }
    )
    out = join_supply_to_panel(panel, pd.DataFrame())
    assert out["supply_data_grade"].iloc[0] == "missing_metrics"

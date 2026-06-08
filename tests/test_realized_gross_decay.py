"""Tests for scripts/realized_gross_decay.py."""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))

from realized_gross_decay import compute_gross_decay_annual  # noqa: E402


def test_compute_gross_decay_aplx_fixture():
    rows = [
        {"date": "2026-03-05", "close_price": 16.94, "etf_adj_close": 5.647, "underlying_adj_close": 10.0},
        {"date": "2026-03-06", "close_price": 13.357, "etf_adj_close": 4.452, "underlying_adj_close": 10.0},
        {"date": "2026-03-09", "close_price": 15.377, "etf_adj_close": 5.126, "underlying_adj_close": 10.1},
        {"date": "2026-03-10", "close_price": 15.71, "etf_adj_close": 15.71, "underlying_adj_close": 10.2},
        {"date": "2026-03-11", "close_price": 17.08, "etf_adj_close": 17.08, "underlying_adj_close": 10.3},
    ]
    base = dt.date(2026, 1, 15)
    extended = []
    for i in range(40):
        d = base + dt.timedelta(days=i)
        extended.append(
            {
                "date": d.isoformat(),
                "close_price": 14.0 + i * 0.01,
                "etf_adj_close": 4.67 + i * 0.003,
                "underlying_adj_close": 9.5 + i * 0.01,
            }
        )
    extended.extend(rows)
    result = compute_gross_decay_annual(
        extended,
        beta=2.0,
        split_events=[(dt.date(2026, 3, 10), 1 / 3)],
        min_obs=35,
    )
    assert result is not None
    assert result["n_obs"] >= 35

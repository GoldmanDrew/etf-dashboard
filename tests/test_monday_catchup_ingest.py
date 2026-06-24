"""Monday catch-up date range for ingest_etf_metrics."""
from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from ingest_etf_metrics import previous_business_day, resolve_monday_catchup_range  # noqa: E402


def test_monday_catchup_range_covers_wed_through_fri():
    # Mon 2026-06-08 -> prior Fri 2026-06-05, start Wed 2026-06-03
    start, end = resolve_monday_catchup_range(date(2026, 6, 8), business_days=3)
    assert start == date(2026, 6, 3)
    assert end == date(2026, 6, 5)


def test_monday_catchup_single_day_when_bdays_one():
    start, end = resolve_monday_catchup_range(date(2026, 6, 8), business_days=1)
    assert start == end == date(2026, 6, 5)


def test_monday_catchup_rejects_non_monday():
    with pytest.raises(ValueError):
        resolve_monday_catchup_range(date(2026, 6, 10))


def test_previous_business_day_skips_juneteenth():
    assert previous_business_day(date(2026, 6, 20)) == date(2026, 6, 18)
    assert previous_business_day(date(2026, 6, 22)) == date(2026, 6, 18)

"""Tests for realized-vol headline provenance fields in build_data."""
from __future__ import annotations

import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from build_data import _vol_annual_source  # noqa: E402


def test_vol_annual_source_yahoo_panel():
    assert _vol_annual_source(True, 0.45, 0.52) == "yahoo_realized_vol"


def test_vol_annual_source_screener_csv():
    assert _vol_annual_source(False, 0.45, 0.45) == "screener_csv"


def test_vol_annual_source_missing_final():
    assert _vol_annual_source(True, 0.45, None) is None
    assert _vol_annual_source(False, None, float("nan")) is None


def test_vol_annual_source_no_csv_fallback():
    assert _vol_annual_source(False, None, 0.40) is None

from __future__ import annotations

from datetime import date

from pathlib import Path
import sys

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from market_calendar import (  # noqa: E402
    is_nyse_session,
    nyse_busday_count,
    nyse_sessions,
    previous_nyse_session,
)


def test_juneteenth_2026_is_not_a_session():
    assert is_nyse_session(date(2026, 6, 18))
    assert not is_nyse_session(date(2026, 6, 19))
    assert not is_nyse_session(date(2026, 6, 20))
    assert is_nyse_session(date(2026, 6, 22))


def test_previous_session_skips_juneteenth_and_weekend():
    assert previous_nyse_session(date(2026, 6, 20)) == date(2026, 6, 18)
    assert previous_nyse_session(date(2026, 6, 22)) == date(2026, 6, 18)


def test_sessions_and_counts_skip_market_holidays():
    assert nyse_sessions(date(2026, 6, 18), date(2026, 6, 22)) == [
        date(2026, 6, 18),
        date(2026, 6, 22),
    ]
    assert nyse_busday_count(date(2026, 6, 18), date(2026, 6, 22)) == 1
    assert nyse_busday_count(date(2026, 6, 18), date(2026, 6, 23)) == 2


def test_good_friday_2026_is_not_a_session():
    assert not is_nyse_session(date(2026, 4, 3))
    assert previous_nyse_session(date(2026, 4, 6)) == date(2026, 4, 2)

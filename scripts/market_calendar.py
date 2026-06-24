"""Small NYSE full-session calendar helpers used by data pipelines.

The dashboard only needs full-day session filtering and business-day counts for
daily artifacts.  Keeping this local avoids a runtime dependency on exchange
calendar packages in the GitHub Actions data jobs.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Iterable


def _as_date(value: object) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if hasattr(value, "date"):
        d = value.date()
        if isinstance(d, date):
            return d
    return date.fromisoformat(str(value)[:10])


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    d = date(year, month, 1)
    offset = (weekday - d.weekday()) % 7
    return d + timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        d = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        d = date(year, month + 1, 1) - timedelta(days=1)
    return d - timedelta(days=(d.weekday() - weekday) % 7)


def _observed_fixed(year: int, month: int, day: int) -> date:
    actual = date(year, month, day)
    if actual.weekday() == 5:
        return actual - timedelta(days=1)
    if actual.weekday() == 6:
        return actual + timedelta(days=1)
    return actual


def _easter_date(year: int) -> date:
    """Gregorian Easter Sunday, Meeus/Jones/Butcher algorithm."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


@lru_cache(maxsize=None)
def nyse_holidays(year: int) -> frozenset[date]:
    """Return observed NYSE full-day holidays for a calendar year."""
    days: set[date] = set()
    for y in (year - 1, year, year + 1):
        for observed in (
            _observed_fixed(y, 1, 1),
            _observed_fixed(y, 7, 4),
            _observed_fixed(y, 12, 25),
        ):
            if observed.year == year:
                days.add(observed)
        if y >= 2022:
            observed = _observed_fixed(y, 6, 19)
            if observed.year == year:
                days.add(observed)

    days.add(_nth_weekday(year, 1, 0, 3))   # Martin Luther King Jr. Day
    days.add(_nth_weekday(year, 2, 0, 3))   # Washington's Birthday
    days.add(_easter_date(year) - timedelta(days=2))  # Good Friday
    days.add(_last_weekday(year, 5, 0))     # Memorial Day
    days.add(_nth_weekday(year, 9, 0, 1))   # Labor Day
    days.add(_nth_weekday(year, 11, 3, 4))  # Thanksgiving Day
    return frozenset(days)


def is_nyse_session(value: object) -> bool:
    d = _as_date(value)
    return d.weekday() < 5 and d not in nyse_holidays(d.year)


def previous_nyse_session(value: object) -> date:
    d = _as_date(value) - timedelta(days=1)
    while not is_nyse_session(d):
        d -= timedelta(days=1)
    return d


def nyse_sessions(start: object, end: object) -> list[date]:
    """NYSE sessions in the inclusive range [start, end]."""
    s = _as_date(start)
    e = _as_date(end)
    if e < s:
        return []
    out: list[date] = []
    cur = s
    while cur <= e:
        if is_nyse_session(cur):
            out.append(cur)
        cur += timedelta(days=1)
    return out


def nyse_busday_count(start: object, end: object) -> int:
    """Count NYSE sessions in [start, end), matching numpy.busday_count shape."""
    s = _as_date(start)
    e = _as_date(end)
    if e == s:
        return 0
    sign = 1
    if e < s:
        s, e = e, s
        sign = -1
    n = 0
    cur = s
    while cur < e:
        if is_nyse_session(cur):
            n += 1
        cur += timedelta(days=1)
    return sign * n


def require_nyse_session(value: object, *, label: str = "date") -> date:
    d = _as_date(value)
    if not is_nyse_session(d):
        raise ValueError(f"{label} {d.isoformat()} is not an NYSE trading session")
    return d

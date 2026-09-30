"""Split-TR audit must not re-fail the nightly on the KEEX / SMCZ false cliffs."""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))

from audit_split_tr_quality import (  # noqa: E402
    _is_unexplained_split_basis_jump,
    audit_ticker,
)

KEEX_EVENTS = [
    (dt.date(2026, 3, 20), 2.0),
    (dt.date(2026, 9, 9), 0.25),
]
SMCZ_EVENTS = [
    (dt.date(2025, 12, 9), 8.0),
    (dt.date(2026, 9, 10), 2.0),
]


def test_keex_aug11_does_not_match_the_distant_two_for_one():
    """1.68x on a flat underlying, 29d before a 4-for-1, is not that split."""
    assert not _is_unexplained_split_basis_jump(
        0.521,
        -0.033,
        day=dt.date(2026, 8, 11),
        split_events=KEEX_EVENTS,
    )


def test_nearby_two_for_one_on_a_flat_underlying_still_blocks():
    assert _is_unexplained_split_basis_jump(
        0.67,
        0.01,
        day=dt.date(2026, 9, 8),
        split_events=[(dt.date(2026, 9, 9), 2.0)],
    )


def test_smcz_levered_session_near_split_is_explained_by_the_underlying():
    """Jun 22 was a real ~2x day (underlying log 0.24), not a restated basis."""
    assert not _is_unexplained_split_basis_jump(
        0.616,
        0.244,
        day=dt.date(2026, 6, 22),
        split_events=SMCZ_EVENTS,
    )


def test_stored_keex_and_smcz_rows_do_not_emit_those_cliffs():
    import pandas as pd

    parquet = Path(__file__).resolve().parent.parent / "data" / "etf_metrics_daily.parquet"
    if not parquet.exists():
        return
    df = pd.read_parquet(parquet)
    df["date"] = df["date"].astype(str).str[:10]
    df["ticker"] = df["ticker"].astype(str).str.upper()
    cases = {
        "KEEX": (KEEX_EVENTS, "2026-08-11"),
        "SMCZ": (SMCZ_EVENTS, "2026-06-22"),
    }
    for sym, (events, day) in cases.items():
        rows = df[df["ticker"] == sym].sort_values("date").to_dict("records")
        assert rows, sym
        failures = audit_ticker(
            sym,
            rows,
            events,
            corp_has_split=True,
            max_log_return=0.35,
        )
        assert not any(day in msg for msg in failures), failures

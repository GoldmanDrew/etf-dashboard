"""Tests for shifted etf_adj_close detection and the pair-leg alignment gate."""
from __future__ import annotations

import datetime as dt
import math
import sys
from pathlib import Path

import pandas as pd

SCRIPTS = Path(__file__).resolve().parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS))

from audit_pair_leg_alignment import (  # noqa: E402
    alignment_gate_errors,
    build_alignment_report,
    classify_ticker,
)
from price_basis import detect_shifted_etf_adj_close  # noqa: E402
from repair_shifted_etf_adj_close import find_shifted_sessions  # noqa: E402


def _aligned_rows(n: int = 40) -> list[dict]:
    rows = []
    close = 100.0
    base = dt.date(2026, 6, 1)
    for i in range(n):
        # Flat adjustment factor of 1.0 — close and adj move together.
        close *= 1.01 if i % 2 == 0 else 0.99
        rows.append(
            {
                "date": (base + dt.timedelta(days=i)).isoformat(),
                "close_price": close,
                "etf_adj_close": close,
                "underlying_adj_close": 50.0 * (1.001 ** i),
            }
        )
    return rows


def _shifted_rows(n: int = 40) -> list[dict]:
    """etf_adj_close[t] holds close_price[t+1] — the join-key asymmetry signature."""
    closes = []
    close = 100.0
    for i in range(n + 1):
        close *= 1.015 if i % 3 == 0 else 0.985
        closes.append(close)
    rows = []
    base = dt.date(2026, 6, 1)
    for i in range(n):
        rows.append(
            {
                "date": (base + dt.timedelta(days=i)).isoformat(),
                "close_price": closes[i],
                "etf_adj_close": closes[i + 1],
                "underlying_adj_close": 50.0 * (1.001 ** i),
            }
        )
    return rows


def test_detect_shifted_etf_adj_close_flags_lag():
    aligned = detect_shifted_etf_adj_close(_aligned_rows())
    assert aligned["shifted"] is False
    assert aligned["corr_aligned"] is not None and aligned["corr_aligned"] > 0.9

    shifted = detect_shifted_etf_adj_close(_shifted_rows())
    assert shifted["shifted"] is True
    assert shifted["corr_lagged"] > shifted["corr_aligned"]


def test_roughness_scan_localises_shifted_sessions():
    rows = _shifted_rows(30)
    dates = [dt.date.fromisoformat(r["date"]) for r in rows]
    closes = [float(r["close_price"]) for r in rows]
    adjs = [float(r["etf_adj_close"]) for r in rows]
    flagged = find_shifted_sessions(dates, closes, adjs)
    # Most of the interior should be flagged; ends may be skipped by the window.
    assert len(flagged) >= 10


def test_alignment_gate_fails_on_shifted_ticker():
    df = pd.DataFrame(
        [
            {**r, "ticker": "BAD"}
            for r in _shifted_rows(40)
        ]
        + [
            {**r, "ticker": "GOOD"}
            for r in _aligned_rows(40)
        ]
    )
    report = build_alignment_report(
        df,
        universe={"BAD", "GOOD"},
        betas={"BAD": 2.0, "GOOD": 2.0},
        since=dt.date(2026, 6, 1),
    )
    assert report["summary"]["shifted_n"] >= 1
    errors = alignment_gate_errors(report)
    assert errors, "gate should fail when a shifted ticker remains"
    assert any("BAD" in (s.get("ticker") or "") for s in report["shifted"]) or report["summary"][
        "shifted_n"
    ] >= 1


def test_alignment_gate_passes_when_clean():
    df = pd.DataFrame([{**r, "ticker": "GOOD"} for r in _aligned_rows(40)])
    report = build_alignment_report(
        df,
        universe={"GOOD"},
        betas={"GOOD": 2.0},
        since=dt.date(2026, 6, 1),
    )
    assert report["summary"]["shifted_n"] == 0
    assert report["summary"]["shifted_session_tickers_n"] == 0
    assert alignment_gate_errors(report) == []


def test_classify_ticker_reports_track_quality():
    rows = _aligned_rows(50)
    # Make a quiet 2x track so pair_tracks_well can fire.
    etf, und = 100.0, 50.0
    for i, r in enumerate(rows):
        und *= 1.002
        etf *= 1.004
        r["close_price"] = etf
        r["etf_adj_close"] = etf
        r["underlying_adj_close"] = und
    info = classify_ticker(rows, beta=2.0)
    assert info["shifted"] is False
    assert info["pair_tracks_well"] is True
    assert info["pair_track_r2"] is not None and info["pair_track_r2"] > 0.9

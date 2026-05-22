#!/usr/bin/env python3
"""Ingest known earnings dates for YieldBOOST underlyings."""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from build_data import YIELDBOOST_BUCKET2_PAIRS  # noqa: E402
from event_vol import (  # noqa: E402
    fetch_nasdaq_earnings_window,
    fetch_yahoo_earnings_dates,
    load_json_calendar,
)

DATA_DIR = SCRIPTS.parent / "data"
KNOWN_PATH = DATA_DIR / "event_calendar_known.json"
METRICS_PATH = DATA_DIR / "etf_metrics_daily.csv"


def _historical_earnings_moves(underlying: str, earnings_dates: list[date]) -> float | None:
    """Robust MAD of |log return| on earnings days from joint metrics."""
    if not METRICS_PATH.exists() or not earnings_dates:
        return None
    try:
        import pandas as pd
    except ImportError:
        return None

    try:
        df = pd.read_csv(METRICS_PATH, usecols=["ticker", "date", "underlying_adj_close"])
    except Exception:
        return None

    und = str(underlying).upper()
    sub = df[df["ticker"].astype(str).str.upper() == und].copy()
    if sub.empty:
        peers = df[df["ticker"].astype(str).str.upper() == und]
        if peers.empty:
            return None
    sub = df[df["ticker"].astype(str).str.upper() == und]
    if sub.empty:
        return None

    sub["date"] = pd.to_datetime(sub["date"], errors="coerce").dt.date
    sub = sub.sort_values("date")
    sub["px"] = pd.to_numeric(sub["underlying_adj_close"], errors="coerce")
    sub = sub.dropna(subset=["px"])
    if len(sub) < 5:
        return None

    px_by_date = {r.date: float(r.px) for _, r in sub.iterrows()}
    moves: list[float] = []
    for ed in earnings_dates:
        if ed not in px_by_date:
            continue
        prev_dates = [d for d in px_by_date if d < ed]
        if not prev_dates:
            continue
        prev = px_by_date[max(prev_dates)]
        cur = px_by_date[ed]
        if prev > 0 and cur > 0:
            moves.append(abs(__import__("math").log(cur / prev)))

    if len(moves) < 2:
        return None
    med = sorted(moves)[len(moves) // 2]
    mad = sorted([abs(x - med) for x in moves])[len(moves) // 2]
    return mad / 0.6745 if mad > 0 else med


def build_known_calendar(
    *,
    universe: list[tuple[str, str]] | None = None,
    sleep_sec: float = 0.15,
) -> dict:
    pairs = universe or sorted(YIELDBOOST_BUCKET2_PAIRS)
    underlyings = sorted({und.upper() for _, und in pairs})
    items: list[dict] = []
    today = date.today()

    nasdaq_future = fetch_nasdaq_earnings_window(underlyings, start=today, days=21)

    for und in underlyings:
        dates = nasdaq_future.get(und) or fetch_yahoo_earnings_dates(und)
        hist_dates = [d for d in dates if d <= today][-12:]
        hist_move = _historical_earnings_moves(und, hist_dates)
        future = [d for d in dates if d >= today][:2]
        for ed in future:
            item = {
                "underlying": und,
                "event_type": "earnings",
                "event_date": ed.isoformat(),
                "source": "nasdaq_earnings" if und in nasdaq_future else "yahoo_earnings",
            }
            if hist_move is not None:
                item["historical_move_pct_mad"] = round(hist_move, 6)
            items.append(item)
        time.sleep(sleep_sec)

    return {
        "build_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "item_count": len(items),
        "universe": underlyings,
        "items": items,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest YieldBOOST earnings calendar")
    parser.add_argument("--output", default=str(KNOWN_PATH))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    payload = build_known_calendar()
    print(f"Known events: {payload['item_count']} for {len(payload.get('universe', []))} underlyings")
    if args.dry_run:
        print(json.dumps(payload, indent=2)[:2000])
        return 0

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), allow_nan=False)
    print(f"Wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

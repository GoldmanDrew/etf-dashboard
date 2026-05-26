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
    EARNINGS_CADENCE_DAYS,
    fetch_nasdaq_earnings_window,
    fetch_yahoo_earnings_dates,
    load_earnings_seed,
    load_json_calendar,
    project_next_earnings_date,
)

DATA_DIR = SCRIPTS.parent / "data"
KNOWN_PATH = DATA_DIR / "event_calendar_known.json"
SEED_PATH = DATA_DIR / "earnings_calendar_seed.json"
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


def _stats_for_underlying(und: str) -> dict[str, int]:
    """Counter helper for source-attribution diagnostics."""
    return {
        "nasdaq": 0, "yahoo": 0, "seed": 0, "projected": 0, "missing": 0,
    }


def build_known_calendar(
    *,
    universe: list[tuple[str, str]] | None = None,
    sleep_sec: float = 0.15,
    seed_path: Path | None = None,
) -> dict:
    pairs = universe or sorted(YIELDBOOST_BUCKET2_PAIRS)
    underlyings = sorted({und.upper() for _, und in pairs})
    items: list[dict] = []
    today = date.today()

    # Pre-fetch seed JSON so we can fall back fast when live APIs return nothing.
    seed_by_underlying = load_earnings_seed(seed_path or SEED_PATH)
    nasdaq_future = fetch_nasdaq_earnings_window(underlyings, start=today, days=21)
    source_stats: dict[str, int] = {"nasdaq": 0, "yahoo": 0, "seed": 0, "projected": 0, "missing": 0}

    for und in underlyings:
        nasdaq_dates = nasdaq_future.get(und, [])
        yahoo_dates: list = []
        if not nasdaq_dates:
            yahoo_dates = fetch_yahoo_earnings_dates(und)

        merged_dates = sorted(set(nasdaq_dates + yahoo_dates))
        hist_dates = [d for d in merged_dates if d <= today][-12:]
        hist_move = _historical_earnings_moves(und, hist_dates)
        future = [d for d in merged_dates if d >= today][:2]

        # Tier 1: confirmed dates from Nasdaq or Yahoo.
        for ed in future:
            src = "nasdaq_earnings" if ed in nasdaq_dates else "yahoo_earnings"
            item = {
                "underlying": und,
                "event_type": "earnings",
                "event_date": ed.isoformat(),
                "source": src,
                "confirmation": "confirmed",
            }
            if hist_move is not None:
                item["historical_move_pct_mad"] = round(hist_move, 6)
            items.append(item)
            source_stats["nasdaq" if src == "nasdaq_earnings" else "yahoo"] += 1

        if future:
            time.sleep(sleep_sec)
            continue

        # Tier 2: seed override (committed JSON) -- typically hand-curated next
        # earnings dates for the YB universe, refreshed weekly.
        seed_rows = [r for r in seed_by_underlying.get(und, []) if r.get("event_date")]
        seed_future: list[date] = []
        for row in seed_rows:
            try:
                ed = datetime.strptime(str(row["event_date"]), "%Y-%m-%d").date()
            except ValueError:
                continue
            if ed >= today:
                seed_future.append(ed)
        seed_future.sort()
        if seed_future:
            for ed in seed_future[:2]:
                seed_row = next(
                    (r for r in seed_rows if str(r.get("event_date")) == ed.isoformat()),
                    {},
                )
                item = {
                    "underlying": und,
                    "event_type": "earnings",
                    "event_date": ed.isoformat(),
                    "source": str(seed_row.get("source") or "seed_quarterly"),
                    "confirmation": str(seed_row.get("confirmation") or "projected"),
                }
                seed_mad = seed_row.get("historical_move_pct_mad")
                if seed_mad is not None:
                    item["historical_move_pct_mad"] = float(seed_mad)
                elif hist_move is not None:
                    item["historical_move_pct_mad"] = round(hist_move, 6)
                items.append(item)
                source_stats["seed"] += 1
            time.sleep(sleep_sec)
            continue

        # Tier 3: project forward from the most recent historical earnings date.
        last_hist = hist_dates[-1] if hist_dates else None
        projected = (
            project_next_earnings_date(last_hist, today=today)
            if last_hist
            else None
        )
        if projected is not None:
            item = {
                "underlying": und,
                "event_type": "earnings",
                "event_date": projected.isoformat(),
                "source": "projected_quarterly",
                "confirmation": "projected",
                "projection_cadence_days": EARNINGS_CADENCE_DAYS,
            }
            if hist_move is not None:
                item["historical_move_pct_mad"] = round(hist_move, 6)
            items.append(item)
            source_stats["projected"] += 1
        else:
            source_stats["missing"] += 1
        time.sleep(sleep_sec)

    return {
        "build_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "item_count": len(items),
        "universe": underlyings,
        "items": items,
        "source_stats": source_stats,
        "seed_underlying_count": len(seed_by_underlying),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest YieldBOOST earnings calendar")
    parser.add_argument("--output", default=str(KNOWN_PATH))
    parser.add_argument("--seed", default=str(SEED_PATH), help="Seed JSON for fallback dates")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    payload = build_known_calendar(seed_path=Path(args.seed))
    stats = payload.get("source_stats") or {}
    print(
        f"Known events: {payload['item_count']} for {len(payload.get('universe', []))} underlyings "
        f"(nasdaq={stats.get('nasdaq', 0)}, yahoo={stats.get('yahoo', 0)}, "
        f"seed={stats.get('seed', 0)}, projected={stats.get('projected', 0)}, "
        f"missing={stats.get('missing', 0)})"
    )
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

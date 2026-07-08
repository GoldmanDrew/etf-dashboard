#!/usr/bin/env python3
"""Audit and refresh earnings_calendar_seed.json for B2 + B4 underlyings."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA = REPO_ROOT / "data"
SEED_PATH = DATA / "earnings_calendar_seed.json"
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from earnings_universe import load_bucket_underlyings  # noqa: E402
from event_vol import fetch_nasdaq_earnings_window, project_next_earnings_date  # noqa: E402

SEED_DESCRIPTION = (
    "Fallback next-earnings dates per bucket 2/4 underlying. Live Nasdaq dates are "
    "written on weekly --refresh; used when the nightly Nasdaq window returns nothing. "
    "'confirmation' = 'projected' means quarterly cadence — display with an amber badge."
)


def _load_seed() -> dict:
    if not SEED_PATH.exists():
        return {"updated_at": None, "items": []}
    try:
        return json.loads(SEED_PATH.read_text(encoding="utf-8")) or {"items": []}
    except Exception:
        return {"items": []}


def _underlying_universe() -> list[str]:
    return load_bucket_underlyings()


def _parse_event_date(raw: object) -> date | None:
    if not raw:
        return None
    try:
        return date.fromisoformat(str(raw)[:10])
    except ValueError:
        return None


def check_seed(*, horizon_days: int = 21) -> tuple[list[str], dict]:
    seed = _load_seed()
    items = seed.get("items") or []
    by_und: dict[str, dict] = {}
    for it in items:
        u = str(it.get("underlying") or "").upper()
        if u:
            by_und[u] = it

    today = date.today()
    horizon = today + timedelta(days=horizon_days)
    missing: list[str] = []
    stale: list[str] = []

    for und in _underlying_universe():
        it = by_und.get(und)
        if not it:
            missing.append(und)
            continue
        ev_d = _parse_event_date(it.get("event_date"))
        if ev_d is None:
            missing.append(und)
            continue
        if ev_d < today:
            stale.append(und)
        elif ev_d > horizon:
            stale.append(und)

    report = {
        "checked_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "horizon_days": horizon_days,
        "universe_count": len(_underlying_universe()),
        "seed_count": len(items),
        "missing_upcoming": missing,
        "stale_or_far": stale,
    }
    warnings = []
    if missing:
        warnings.append(
            f"earnings seed missing/upcoming for {len(missing)} underlyings: "
            f"{', '.join(missing[:8])}{'…' if len(missing) > 8 else ''}"
        )
    if stale:
        warnings.append(
            f"earnings seed stale/far for {len(stale)} underlyings: "
            f"{', '.join(stale[:8])}{'…' if len(stale) > 8 else ''}"
        )
    return warnings, report


def refresh_seed_from_nasdaq(
    *,
    horizon_days: int = 21,
    dry_run: bool = False,
) -> dict:
    """Weekly refresh: Nasdaq confirmed dates + quarterly projection for misses."""
    underlyings = _underlying_universe()
    today = date.today()
    seed = _load_seed()
    by_und: dict[str, dict] = {}
    for row in seed.get("items") or []:
        und = str(row.get("underlying") or "").upper()
        if und:
            by_und[und] = row

    nasdaq = fetch_nasdaq_earnings_window(underlyings, start=today, days=horizon_days)
    items: list[dict] = []
    stats = {"confirmed": 0, "projected": 0, "carried": 0, "new_missing": 0}

    for und in underlyings:
        nasdaq_future = sorted(d for d in nasdaq.get(und, []) if d >= today)
        old = by_und.get(und) or {}

        if nasdaq_future:
            ed = nasdaq_future[0]
            item = {
                "underlying": und,
                "event_date": ed.isoformat(),
                "confirmation": "confirmed",
                "source": "nasdaq_earnings",
            }
            mad = old.get("historical_move_pct_mad")
            if mad is not None:
                item["historical_move_pct_mad"] = float(mad)
            items.append(item)
            stats["confirmed"] += 1
            continue

        old_date = _parse_event_date(old.get("event_date"))
        if old_date is not None:
            projected = project_next_earnings_date(old_date, today=today)
            if projected is not None and projected >= today:
                item = {
                    "underlying": und,
                    "event_date": projected.isoformat(),
                    "confirmation": "projected",
                    "source": str(old.get("source") or "seed_quarterly"),
                }
                mad = old.get("historical_move_pct_mad")
                if mad is not None:
                    item["historical_move_pct_mad"] = float(mad)
                items.append(item)
                stats["projected"] += 1
                continue

        if old:
            items.append({**old, "underlying": und})
            stats["carried"] += 1
        else:
            stats["new_missing"] += 1

    payload = {
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "schema_version": 1,
        "description": SEED_DESCRIPTION,
        "universe_buckets": ["bucket_2", "bucket_4"],
        "live_source": "nasdaq_only",
        "refresh_stats": stats,
        "items": sorted(items, key=lambda x: str(x.get("underlying") or "")),
    }

    if not dry_run:
        SEED_PATH.parent.mkdir(parents=True, exist_ok=True)
        SEED_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    return payload


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit or refresh earnings seed for bucket 2 + bucket 4 underlyings",
    )
    parser.add_argument("--horizon-days", type=int, default=21)
    parser.add_argument("--refresh", action="store_true", help="Refresh seed from Nasdaq (weekly)")
    parser.add_argument("--dry-run", action="store_true", help="With --refresh, print stats only")
    parser.add_argument("--fail-on-missing", action="store_true")
    parser.add_argument("--report", type=Path, default=DATA / "earnings_seed_audit.json")
    args = parser.parse_args()

    if args.refresh:
        payload = refresh_seed_from_nasdaq(
            horizon_days=args.horizon_days,
            dry_run=args.dry_run,
        )
        stats = payload.get("refresh_stats") or {}
        print(
            f"Seed refresh: {len(payload.get('items') or [])} items "
            f"(confirmed={stats.get('confirmed', 0)}, projected={stats.get('projected', 0)}, "
            f"carried={stats.get('carried', 0)}, new_missing={stats.get('new_missing', 0)})"
        )
        if args.dry_run:
            print(json.dumps(payload, indent=2)[:2500])

    warnings, report = check_seed(horizon_days=args.horizon_days)
    if args.report and not args.dry_run:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2), encoding="utf-8")

    for w in warnings:
        print(f"[WARN] {w}", file=sys.stderr)
    if args.fail_on_missing and report.get("missing_upcoming"):
        return 1
    if not warnings and not args.refresh:
        print("earnings seed OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

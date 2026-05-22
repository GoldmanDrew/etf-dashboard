#!/usr/bin/env python3
"""Forward-straddle mystery event scanner + combined event calendar builder."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from build_data import YIELDBOOST_BUCKET2_PAIRS  # noqa: E402
from event_vol import (  # noqa: E402
    calendar_is_stale,
    load_json_calendar,
    merge_event_calendars,
    scan_mystery_events,
)

DATA_DIR = SCRIPTS.parent / "data"
OPTIONS_CACHE = DATA_DIR / "options_cache.json"
KNOWN_PATH = DATA_DIR / "event_calendar_known.json"
MACRO_PATH = DATA_DIR / "macro_event_calendar.json"
INFERRED_PATH = DATA_DIR / "event_calendar_inferred.json"
COMBINED_PATH = DATA_DIR / "event_calendar_combined.json"


def build_inferred_calendar(
    options_cache: dict,
    *,
    universe: list[str] | None = None,
    known: dict | None = None,
) -> dict:
    underlyings = universe or sorted({und.upper() for _, und in YIELDBOOST_BUCKET2_PAIRS})
    items: list[dict] = []
    for und in underlyings:
        items.extend(scan_mystery_events(und, options_cache, known_calendar=known))
    return {
        "build_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "item_count": len(items),
        "universe": underlyings,
        "items": items,
    }


def refresh_event_pipeline(
    *,
    options_cache_path: Path = OPTIONS_CACHE,
    write: bool = True,
    known_max_age_hours: float = 24.0,
) -> dict:
    """Refresh known earnings (when stale), mystery scanner, and combined calendar."""
    known = load_json_calendar(KNOWN_PATH)
    if calendar_is_stale(known, max_age_hours=known_max_age_hours):
        try:
            from ingest_event_calendar import build_known_calendar

            known = build_known_calendar()
            if write:
                KNOWN_PATH.parent.mkdir(parents=True, exist_ok=True)
                with KNOWN_PATH.open("w", encoding="utf-8") as f:
                    json.dump(known, f, separators=(",", ":"), allow_nan=False)
                print(f"  Refreshed {KNOWN_PATH.name} ({known.get('item_count', 0)} items)")
        except Exception as exc:
            print(f"  [WARN] Known event calendar ingest failed: {exc}")

    result = refresh_event_calendars(
        options_cache_path=options_cache_path,
        write=write,
        known=known,
    )
    result["known"] = known
    return result


def refresh_event_calendars(
    *,
    options_cache_path: Path = OPTIONS_CACHE,
    write: bool = True,
    known: dict | None = None,
) -> dict:
    options_cache: dict = {}
    if options_cache_path.exists():
        options_cache = json.loads(options_cache_path.read_text(encoding="utf-8"))

    if known is None:
        known = load_json_calendar(KNOWN_PATH)
    macro = load_json_calendar(MACRO_PATH)
    inferred = build_inferred_calendar(options_cache, known=known)

    combined = merge_event_calendars(known, inferred, macro)

    if write:
        INFERRED_PATH.parent.mkdir(parents=True, exist_ok=True)
        with INFERRED_PATH.open("w", encoding="utf-8") as f:
            json.dump(inferred, f, separators=(",", ":"), allow_nan=False)
        with COMBINED_PATH.open("w", encoding="utf-8") as f:
            json.dump(combined, f, separators=(",", ":"), allow_nan=False)

    return {"inferred": inferred, "combined": combined}


def main() -> int:
    parser = argparse.ArgumentParser(description="Event vol decomposition refresh")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    result = refresh_event_pipeline(write=not args.dry_run)
    inf = result["inferred"]
    comb = result["combined"]
    print(f"Inferred mystery events: {inf.get('item_count', 0)}")
    print(f"Combined calendar items: {comb.get('item_count', 0)}")
    if args.dry_run:
        print(json.dumps(comb, indent=2)[:3000])
    else:
        print(f"Wrote {INFERRED_PATH.name}, {COMBINED_PATH.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

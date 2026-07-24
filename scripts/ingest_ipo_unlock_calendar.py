#!/usr/bin/env python3
"""Build data/ipo_float_unlock_calendar.json from the curated seed."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

from ipo_unlock_calendar import (  # noqa: E402
    CALENDAR_PATH,
    DATA_DIR,
    SEED_PATH,
    build_calendar,
    write_calendar,
    _parse_date,
)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--asof", default=None, help="YYYY-MM-DD (default: today)")
    p.add_argument("--seed", type=Path, default=SEED_PATH)
    p.add_argument("--out", type=Path, default=CALENDAR_PATH)
    args = p.parse_args(argv)
    asof = _parse_date(args.asof) if args.asof else date.today()
    seed = json.loads(args.seed.read_text(encoding="utf-8"))
    cal = build_calendar(seed=seed, asof=asof, data_dir=DATA_DIR)
    write_calendar(cal, args.out)
    active = [u for u in cal.get("underlyings") or [] if u.get("is_ipo_float_unlock")]
    print(f"Wrote {args.out}")
    print(f"  asof={cal.get('asof_date')} underlyings={cal.get('underlying_count')} active={len(active)}")
    for u in active:
        print(
            f"  {u['underlying']}: next={u.get('next_ipo_unlock_date')} "
            f"shares={u.get('next_ipo_unlock_shares')} grade={u.get('data_grade')} "
            f"status={u.get('unlock_status')}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

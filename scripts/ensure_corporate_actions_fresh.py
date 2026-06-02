#!/usr/bin/env python3
"""Refresh corporate_actions.json when stale — keeps split metadata current for builds."""
from __future__ import annotations

import argparse
import datetime as dt
import subprocess
import sys
from pathlib import Path

from split_adjustments import corporate_actions_build_time, DEFAULT_CORPORATE_ACTIONS_PATH

REPO_ROOT = Path(__file__).resolve().parent.parent


def _is_stale(path: Path, max_age_hours: float) -> bool:
    if not path.exists():
        return True
    built = corporate_actions_build_time(path)
    if built is None:
        return True
    now = dt.datetime.now(dt.timezone.utc)
    if built.tzinfo is None:
        built = built.replace(tzinfo=dt.timezone.utc)
    age_h = (now - built).total_seconds() / 3600.0
    return age_h > max_age_hours


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--max-age-hours",
        type=float,
        default=6.0,
        help="Re-ingest when corporate_actions.json is older than this (default 6).",
    )
    parser.add_argument(
        "--path",
        type=Path,
        default=DEFAULT_CORPORATE_ACTIONS_PATH,
        help="Path to corporate_actions.json",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print whether ingest would run without executing.",
    )
    args = parser.parse_args()
    path = args.path
    if not _is_stale(path, args.max_age_hours):
        print(f"corporate_actions fresh (<={args.max_age_hours}h): {path}")
        return 0
    print(f"corporate_actions stale (>{args.max_age_hours}h): {path}")
    if args.dry_run:
        return 0
    cmd = [sys.executable, str(REPO_ROOT / "scripts" / "ingest_corporate_actions.py")]
    return subprocess.call(cmd, cwd=str(REPO_ROOT))


if __name__ == "__main__":
    raise SystemExit(main())

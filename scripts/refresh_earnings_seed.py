#!/usr/bin/env python3
"""Refresh earnings_calendar_seed.json when YB underlyings lack upcoming dates."""
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

from product_taxonomy import yieldboost_income_pairs  # noqa: E402


def _load_seed() -> dict:
    if not SEED_PATH.exists():
        return {"updated_at": None, "items": []}
    try:
        return json.loads(SEED_PATH.read_text(encoding="utf-8")) or {"items": []}
    except Exception:
        return {"items": []}


def _underlying_universe() -> set[str]:
    return {und for _sym, und in yieldboost_income_pairs()}


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

    for und in sorted(_underlying_universe()):
        it = by_und.get(und)
        if not it:
            missing.append(und)
            continue
        ev = str(it.get("event_date") or "")[:10]
        if len(ev) != 10:
            missing.append(und)
            continue
        try:
            ev_d = date.fromisoformat(ev)
        except ValueError:
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
        warnings.append(f"earnings seed missing/upcoming for {len(missing)} underlyings: {', '.join(missing[:8])}{'…' if len(missing) > 8 else ''}")
    if stale:
        warnings.append(f"earnings seed stale/far for {len(stale)} underlyings: {', '.join(stale[:8])}{'…' if len(stale) > 8 else ''}")
    return warnings, report


def main() -> int:
    parser = argparse.ArgumentParser(description="Check earnings seed freshness for YB underlyings")
    parser.add_argument("--horizon-days", type=int, default=21)
    parser.add_argument("--fail-on-missing", action="store_true")
    parser.add_argument("--report", type=Path, default=DATA / "earnings_seed_audit.json")
    args = parser.parse_args()

    warnings, report = check_seed(horizon_days=args.horizon_days)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2), encoding="utf-8")

    for w in warnings:
        print(f"[WARN] {w}", file=sys.stderr)
    if args.fail_on_missing and report.get("missing_upcoming"):
        return 1
    if not warnings:
        print("earnings seed OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

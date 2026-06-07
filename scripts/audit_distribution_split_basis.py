#!/usr/bin/env python3
"""Gate YieldBOOST distribution calibration after reverse splits."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from income_schedule import (  # noqa: E402
    MAX_BLENDED_RATIO_AFTER_SPLIT,
    MAX_INCOME_YIELD_ANNUAL,
    MAX_WEEKLY_YIELD_FRAC,
    POST_SPLIT_RUN_RATE_LOOKBACK_DAYS,
    build_income_calibration_row,
    load_nav_series_by_ticker,
)

REPO = _SCRIPTS.parent
DASHBOARD = REPO / "data" / "dashboard_data.json"
DISTRIBUTIONS = REPO / "data" / "etf_distributions.json"
METRICS = REPO / "data" / "etf_metrics_daily.csv"


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def audit_dashboard(
    *,
    today: dt.date | None = None,
    max_ratio: float = MAX_BLENDED_RATIO_AFTER_SPLIT,
    max_yield: float = MAX_INCOME_YIELD_ANNUAL,
) -> list[str]:
    today = today or dt.datetime.now(dt.UTC).date()
    payload = _load_json(DASHBOARD)
    failures: list[str] = []
    for rec in payload.get("records") or []:
        if not rec.get("is_yieldboost"):
            continue
        sym = str(rec.get("symbol") or "").strip().upper()
        cal = rec.get("income_distribution_calibration") or {}
        boundary_s = cal.get("split_boundary")
        if not boundary_s:
            continue
        try:
            boundary = dt.date.fromisoformat(str(boundary_s)[:10])
        except ValueError:
            continue
        if (today - boundary).days > POST_SPLIT_RUN_RATE_LOOKBACK_DAYS:
            continue
        br = cal.get("blended_ratio_used")
        if br is not None and float(br) > max_ratio:
            failures.append(
                f"{sym}: blended_ratio_used {float(br):.2f} > {max_ratio} "
                f"within {POST_SPLIT_RUN_RATE_LOOKBACK_DAYS}d of split {boundary_s}"
            )
        for ev in cal.get("events_recent") or []:
            yf = ev.get("yield_frac")
            ex = str(ev.get("ex_date") or "")[:10]
            if ex and ex < boundary_s and yf is not None and float(yf) > MAX_WEEKLY_YIELD_FRAC:
                failures.append(
                    f"{sym}: pre-split yield {float(yf):.3f} on {ex} "
                    f"(>{MAX_WEEKLY_YIELD_FRAC}) — split basis not normalized"
                )
    return failures


def audit_rebuild_tsyy(
    *,
    today: dt.date | None = None,
) -> list[str]:
    """Spot-check TSYY/COYY/MTYY calibration rebuild from raw distributions."""
    today = today or dt.date(2026, 6, 7)
    failures: list[str] = []
    dist = _load_json(DISTRIBUTIONS)
    by_symbol = dist.get("by_symbol") or {}
    nav_by = load_nav_series_by_ticker(METRICS)
    for sym, mult in (("TSYY", 8.0), ("COYY", 6.0), ("MTYY", 6.0)):
        events = by_symbol.get(sym) or []
        nav = nav_by.get(sym) or []
        if not events or not nav:
            continue
        block = build_income_calibration_row(
            sym,
            events,
            nav,
            current_sigma=0.7,
            today=today,
            split_events=[(dt.date(2026, 6, 2), mult)],
        )
        if block is None:
            failures.append(f"{sym}: calibration block is None")
            continue
        br = block.get("blended_ratio_used")
        if br is None or float(br) > MAX_BLENDED_RATIO_AFTER_SPLIT:
            failures.append(f"{sym}: rebuilt blended_ratio_used={br}")
        pre = [
            e for e in block.get("events_recent") or []
            if str(e.get("ex_date") or "") < "2026-06-02"
        ]
        for ev in pre[-3:]:
            yf = float(ev.get("yield_frac") or 0)
            if yf > MAX_WEEKLY_YIELD_FRAC:
                failures.append(
                    f"{sym}: rebuilt pre-split yield {yf:.3f} on {ev.get('ex_date')}"
                )
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit distribution split basis quality")
    parser.add_argument("--skip-rebuild", action="store_true")
    args = parser.parse_args()

    failures = audit_dashboard()
    if not args.skip_rebuild and METRICS.exists() and DISTRIBUTIONS.exists():
        failures.extend(audit_rebuild_tsyy())

    if failures:
        print("Distribution split basis failures:")
        for line in failures:
            print(f"  - {line}")
        return 1
    print("Distribution split basis audit: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

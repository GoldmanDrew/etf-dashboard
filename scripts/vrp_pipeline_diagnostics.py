#!/usr/bin/env python3
"""
YieldBOOST VRP pipeline diagnostics and CI quality gate.

Validates `yieldboost_put_spreads_latest.json` and `vrp_live.json` schema,
checks YieldBOOST universe coverage, and fails when spreads exist but VRP
artifacts are missing.

Usage:
  python scripts/vrp_pipeline_diagnostics.py
  python scripts/vrp_pipeline_diagnostics.py --require-vrp-file --fail-on-missing-vrp-when-spreads
  python scripts/vrp_pipeline_diagnostics.py --fail-on-missing-yb-coverage
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from build_data import YIELDBOOST_BUCKET2_PAIRS  # noqa: E402
from event_vol import calendar_is_stale  # noqa: E402

# YieldBOOST tickers without Granite option legs yet (document skips here).
YIELDBOOST_COVERAGE_SKIP: set[str] = set()

EVENT_CALENDAR_PATH = Path("data/event_calendar_combined.json")
OPTIONS_CACHE_PATH = Path("data/options_cache.json")
VRP_HEALTH_PATH = Path("data/vrp_health.json")
MIN_IV_COVERAGE_WARN = 0.50
MIN_IV_COVERAGE_FAIL = 0.80
MIN_MID_COVERAGE_FAIL = 0.50
MIN_OCC_FILL_RATIO = 0.80
EVENT_VRP_ROW_KEYS = (
    "iv_base_proxy",
    "iv_full_proxy",
    "rv_30d_2x_base",
    "vrp_vol_2x_base",
    "event_count_in_window",
)

VRP_REQUIRED_ROW_KEYS = (
    "yb_etf",
    "sleeve_2x",
    "expiry",
    "strike_long",
    "strike_short",
    "holdings_as_of",
)
SPREAD_REQUIRED_ROW_KEYS = (
    "yb_etf",
    "sleeve_2x_etf",
    "expiry",
    "strike_long",
    "strike_short",
    "holdings_as_of",
    "is_front",
)


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _front_spreads(payload: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not payload:
        return []
    rows = payload.get("spreads") or []
    if not isinstance(rows, list):
        return []
    return [r for r in rows if isinstance(r, dict) and r.get("is_front")]


def _validate_spreads_schema(payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    rows = payload.get("spreads")
    if not isinstance(rows, list):
        return ["spreads payload missing 'spreads' list"]
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            issues.append(f"spreads[{i}] is not an object")
            continue
        for key in SPREAD_REQUIRED_ROW_KEYS:
            if key not in row:
                issues.append(f"spreads[{i}] missing required key '{key}'")
    return issues


def _validate_vrp_schema(payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    rows = payload.get("rows")
    if not isinstance(rows, list):
        return ["vrp payload missing 'rows' list"]
    if "build_time" not in payload:
        issues.append("vrp payload missing 'build_time'")
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            issues.append(f"vrp rows[{i}] is not an object")
            continue
        for key in VRP_REQUIRED_ROW_KEYS:
            if key not in row:
                issues.append(f"vrp rows[{i}] missing required key '{key}'")
    return issues


def run_diagnostics(
    *,
    spreads_path: Path,
    vrp_path: Path,
    event_calendar_path: Path | None = None,
    options_cache_path: Path | None = None,
    vrp_health_path: Path | None = None,
    require_vrp_file: bool = False,
    fail_on_missing_vrp_when_spreads: bool = False,
    fail_on_missing_yb_coverage: bool = False,
    fail_on_stale_event_calendar: bool = False,
    fail_on_low_iv_coverage: bool = False,
    fail_on_low_mid_coverage: bool = False,
    min_iv_coverage_fail: float = MIN_IV_COVERAGE_FAIL,
    min_mid_coverage_fail: float = MIN_MID_COVERAGE_FAIL,
) -> int:
    exit_code = 0
    spreads_payload = _load_json(spreads_path)
    vrp_payload = _load_json(vrp_path)
    calendar_path = event_calendar_path or EVENT_CALENDAR_PATH
    cache_path = options_cache_path or OPTIONS_CACHE_PATH
    health_path = vrp_health_path or VRP_HEALTH_PATH
    event_calendar = _load_json(calendar_path)
    options_cache = _load_json(cache_path)
    vrp_health = _load_json(health_path)
    front = _front_spreads(spreads_payload)

    print(f"spreads_path: {spreads_path} exists={spreads_path.exists()}")
    print(f"vrp_path: {vrp_path} exists={vrp_path.exists()}")
    print(f"event_calendar_path: {calendar_path} exists={calendar_path.exists()}")
    print(f"options_cache_path: {cache_path} exists={cache_path.exists()}")
    print(f"vrp_health_path: {health_path} exists={health_path.exists()}")
    if spreads_payload:
        print(
            "spreads_summary:",
            {
                "spread_count": spreads_payload.get("spread_count"),
                "front_count": spreads_payload.get("front_count"),
                "build_time": spreads_payload.get("build_time"),
            },
        )
    if vrp_payload:
        print(
            "vrp_summary:",
            {
                "row_count": vrp_payload.get("row_count"),
                "build_time": vrp_payload.get("build_time"),
            },
        )
    if event_calendar:
        print(
            "event_calendar_summary:",
            {
                "item_count": event_calendar.get("item_count"),
                "build_time": event_calendar.get("build_time"),
                "stale": calendar_is_stale(event_calendar),
            },
        )
    elif calendar_path.exists():
        print("WARN event_calendar: file exists but could not be parsed")
    else:
        print("WARN event_calendar: combined calendar missing")

    if options_cache:
        print(
            "options_cache_summary:",
            {
                "build_time": options_cache.get("build_time"),
                "tradier_api_configured": options_cache.get("tradier_api_configured"),
                "yieldboost_targeted": options_cache.get("yieldboost_targeted"),
                "occ_requested": options_cache.get("yieldboost_occ_quotes_requested"),
                "occ_filled": options_cache.get("yieldboost_occ_quotes_filled"),
                "occ_skip_reason": options_cache.get("yieldboost_occ_skip_reason"),
            },
        )
        if options_cache.get("tradier_api_configured") is False:
            print(
                "WARN options_cache: tradier_api_configured=false — "
                "YieldBOOST OCC backfill did not run; held weekly expiries may show missing IV."
            )
        occ_req = options_cache.get("yieldboost_occ_quotes_requested")
        occ_fill = options_cache.get("yieldboost_occ_quotes_filled") or 0
        if isinstance(occ_req, int) and occ_req > 0 and occ_fill / occ_req < MIN_OCC_FILL_RATIO:
            print(
                f"WARN options_cache: OCC fill ratio {occ_fill}/{occ_req} below {MIN_OCC_FILL_RATIO:.0%}. "
                "Check held-leg quote logs for unmatched OCC symbols."
            )
    if vrp_health:
        print(
            "vrp_health_summary:",
            {
                "iv_coverage_front_pct": vrp_health.get("iv_coverage_front_pct"),
                "stale_sleeves_count": len(vrp_health.get("stale_sleeves") or []),
                "missing_chain_ybs": vrp_health.get("missing_chain_ybs"),
                "front_leg_quote_coverage": vrp_health.get("front_leg_quote_coverage"),
                "occ_supplement": vrp_health.get("occ_supplement"),
            },
        )

    if spreads_payload:
        for issue in _validate_spreads_schema(spreads_payload):
            print(f"WARN spreads schema: {issue}")
    if vrp_payload:
        for issue in _validate_vrp_schema(vrp_payload):
            print(f"WARN vrp schema: {issue}")

    if require_vrp_file and not vrp_path.exists():
        print(f"FATAL: required VRP file missing: {vrp_path}")
        return 2

    if fail_on_missing_vrp_when_spreads and front and not vrp_path.exists():
        print(
            "FATAL: yieldboost_put_spreads has front spreads but vrp_live.json is missing. "
            "Run build_data.py (refresh_yieldboost_vrp_files) and commit data/vrp_live.json."
        )
        return 2

    if fail_on_stale_event_calendar and calendar_is_stale(event_calendar):
        print(
            f"FATAL: event calendar stale or missing: {calendar_path}. "
            "Run ingest_event_calendar.py and event_vol_decomposition.py."
        )
        return 2

    if front and vrp_payload is not None:
        spread_ybs = {str(r.get("yb_etf") or "").upper() for r in front}
        vrp_ybs = {
            str(r.get("yb_etf") or "").upper()
            for r in (vrp_payload.get("rows") or [])
            if isinstance(r, dict)
        }
        missing_vrp_rows = sorted(yb for yb in spread_ybs if yb and yb not in vrp_ybs)
        if missing_vrp_rows:
            print(f"WARN front spreads without vrp rows: {missing_vrp_rows}")

    yb_universe = sorted({yb for yb, _ in YIELDBOOST_BUCKET2_PAIRS})
    spread_front_ybs = {str(r.get("yb_etf") or "").upper() for r in front}
    missing_front = [
        yb
        for yb in yb_universe
        if yb not in YIELDBOOST_COVERAGE_SKIP and yb not in spread_front_ybs
    ]
    if missing_front:
        msg = f"YieldBOOST tickers without front spread: {missing_front}"
        if fail_on_missing_yb_coverage:
            print(f"FATAL: {msg}")
            exit_code = 2
        else:
            print(f"WARN: {msg}")

    coverage: float | None = None
    if vrp_payload:
        rows = vrp_payload.get("rows") or []
        iv_rows = sum(
            1
            for r in rows
            if isinstance(r, dict)
            and r.get("iv_put_long") is not None
            and r.get("iv_put_short") is not None
        )
        coverage = iv_rows / len(rows) if rows else 0.0
        print(f"vrp_iv_coverage: {iv_rows}/{len(rows)} rows with both leg IVs ({coverage:.1%})")
        if rows and iv_rows == 0:
            print(
                "WARN: vrp_live.json has rows but no IV at held strikes - "
                "options refresh may be stale or sharded away from YieldBOOST sleeves."
            )
        elif rows and coverage < MIN_IV_COVERAGE_WARN:
            print(
                f"WARN: vrp IV coverage {coverage:.1%} below {MIN_IV_COVERAGE_WARN:.0%} — "
                "check yieldboost tick logs for tradier_api_configured and OCC supplement stats."
            )
        missing_chain = sorted({
            str(r.get("yb_etf") or "").upper()
            for r in rows
            if isinstance(r, dict) and r.get("iv_source") == "holdings_missing_chain" and r.get("yb_etf")
        })
        if missing_chain:
            print(f"WARN vrp rows with holdings_missing_chain: {missing_chain}")
        event_rows = sum(
            1
            for r in rows
            if isinstance(r, dict) and all(k in r for k in EVENT_VRP_ROW_KEYS)
        )
        print(f"vrp_event_field_coverage: {event_rows}/{len(rows)} rows with event-decomposed fields")
        if rows and event_rows == 0:
            print(
                "WARN: vrp_live.json rows missing event-decomposed fields - "
                "refresh_yieldboost_vrp_files may not be using event_calendar."
            )

    if coverage is None and vrp_health and vrp_health.get("iv_coverage_front_pct") is not None:
        coverage = float(vrp_health["iv_coverage_front_pct"])

    if coverage is not None and coverage < min_iv_coverage_fail:
        msg = (
            f"YieldBOOST front IV coverage {coverage:.1%} below {min_iv_coverage_fail:.0%} — "
            "check Market Hours yieldboost tick logs for OCC supplement / Tradier chain."
        )
        if fail_on_low_iv_coverage:
            print(f"FATAL: {msg}")
            exit_code = 2
        elif coverage < MIN_IV_COVERAGE_WARN:
            print(f"WARN: {msg}")

    mid_coverage = None
    if vrp_health:
        flc = vrp_health.get("front_leg_quote_coverage") or {}
        mid_coverage = flc.get("spread_mid_pct")
        if mid_coverage is None:
            mid_coverage = flc.get("leg_mid_pct")
    if mid_coverage is not None and mid_coverage < min_mid_coverage_fail:
        msg = (
            f"YieldBOOST spread mid coverage {mid_coverage:.1%} below {min_mid_coverage_fail:.0%} — "
            "held-leg Tradier quotes may be IV-only without bid/ask mids."
        )
        if fail_on_low_mid_coverage:
            print(f"FATAL: {msg}")
            exit_code = 2
        else:
            print(f"WARN: {msg}")

    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser(description="YieldBOOST VRP pipeline diagnostics")
    parser.add_argument(
        "--spreads-path",
        default="data/yieldboost_put_spreads_latest.json",
        help="Path to yieldboost_put_spreads_latest.json",
    )
    parser.add_argument(
        "--vrp-path",
        default="data/vrp_live.json",
        help="Path to vrp_live.json",
    )
    parser.add_argument(
        "--event-calendar-path",
        default=str(EVENT_CALENDAR_PATH),
        help="Path to event_calendar_combined.json",
    )
    parser.add_argument(
        "--options-cache-path",
        default=str(OPTIONS_CACHE_PATH),
        help="Path to options_cache.json",
    )
    parser.add_argument(
        "--vrp-health-path",
        default=str(VRP_HEALTH_PATH),
        help="Path to vrp_health.json",
    )
    parser.add_argument(
        "--require-vrp-file",
        action="store_true",
        help="Exit non-zero when vrp_live.json is missing",
    )
    parser.add_argument(
        "--fail-on-missing-vrp-when-spreads",
        action="store_true",
        help="Exit non-zero when spreads have front legs but vrp_live.json is missing",
    )
    parser.add_argument(
        "--fail-on-missing-yb-coverage",
        action="store_true",
        help="Exit non-zero when a YieldBOOST ticker lacks a front spread (minus documented skips)",
    )
    parser.add_argument(
        "--fail-on-stale-event-calendar",
        action="store_true",
        help="Exit non-zero when event_calendar_combined.json is missing or older than 24h",
    )
    parser.add_argument(
        "--fail-on-low-iv-coverage",
        action="store_true",
        help=f"Exit non-zero when IV row coverage is below {MIN_IV_COVERAGE_FAIL:.0%}",
    )
    parser.add_argument(
        "--fail-on-low-mid-coverage",
        action="store_true",
        help=f"Exit non-zero when spread mid coverage is below {MIN_MID_COVERAGE_FAIL:.0%}",
    )
    parser.add_argument(
        "--min-iv-coverage-fail",
        type=float,
        default=MIN_IV_COVERAGE_FAIL,
        help="IV coverage threshold for --fail-on-low-iv-coverage",
    )
    args = parser.parse_args()

    return run_diagnostics(
        spreads_path=Path(args.spreads_path),
        vrp_path=Path(args.vrp_path),
        event_calendar_path=Path(args.event_calendar_path),
        options_cache_path=Path(args.options_cache_path),
        vrp_health_path=Path(args.vrp_health_path),
        require_vrp_file=args.require_vrp_file,
        fail_on_missing_vrp_when_spreads=args.fail_on_missing_vrp_when_spreads,
        fail_on_missing_yb_coverage=args.fail_on_missing_yb_coverage,
        fail_on_stale_event_calendar=args.fail_on_stale_event_calendar,
        fail_on_low_iv_coverage=args.fail_on_low_iv_coverage,
        fail_on_low_mid_coverage=args.fail_on_low_mid_coverage,
        min_iv_coverage_fail=args.min_iv_coverage_fail,
    )


if __name__ == "__main__":
    raise SystemExit(main())

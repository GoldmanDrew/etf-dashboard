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

# YieldBOOST tickers without Granite option legs yet (document skips here).
YIELDBOOST_COVERAGE_SKIP: set[str] = set()

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
    require_vrp_file: bool = False,
    fail_on_missing_vrp_when_spreads: bool = False,
    fail_on_missing_yb_coverage: bool = False,
) -> int:
    exit_code = 0
    spreads_payload = _load_json(spreads_path)
    vrp_payload = _load_json(vrp_path)
    front = _front_spreads(spreads_payload)

    print(f"spreads_path: {spreads_path} exists={spreads_path.exists()}")
    print(f"vrp_path: {vrp_path} exists={vrp_path.exists()}")
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

    if vrp_payload:
        rows = vrp_payload.get("rows") or []
        iv_rows = sum(
            1
            for r in rows
            if isinstance(r, dict)
            and r.get("iv_put_long") is not None
            and r.get("iv_put_short") is not None
        )
        print(f"vrp_iv_coverage: {iv_rows}/{len(rows)} rows with both leg IVs")
        if rows and iv_rows == 0:
            print(
                "WARN: vrp_live.json has rows but no IV at held strikes - "
                "options refresh may be stale or sharded away from YieldBOOST sleeves."
            )

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
    args = parser.parse_args()

    return run_diagnostics(
        spreads_path=Path(args.spreads_path),
        vrp_path=Path(args.vrp_path),
        require_vrp_file=args.require_vrp_file,
        fail_on_missing_vrp_when_spreads=args.fail_on_missing_vrp_when_spreads,
        fail_on_missing_yb_coverage=args.fail_on_missing_yb_coverage,
    )


if __name__ == "__main__":
    raise SystemExit(main())

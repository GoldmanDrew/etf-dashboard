#!/usr/bin/env python3
"""Audit dashboard data for thin-history and artifact-quality regressions."""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parent.parent
DASHBOARD_JSON = REPO / "data" / "dashboard_data.json"
DEFAULT_JSON_GLOBS = ("data/*.json", "data/**/*.json")


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _has_conflict_marker(path: Path) -> bool:
    text = path.read_text(encoding="utf-8", errors="ignore")
    return any(marker in text for marker in ("<<<<<<<", "=======", ">>>>>>>"))


def _finite(v: Any) -> bool:
    try:
        return math.isfinite(float(v))
    except (TypeError, ValueError):
        return False


def audit_dashboard(payload: dict[str, Any]) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    records = payload.get("records")
    if not isinstance(records, list):
        return ["dashboard_data.json missing records[]"], warnings

    for row in records:
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol") or "?")

        obs = row.get("realized_pair_gross_60d_obs")
        sufficient = row.get("realized_pair_gross_60d_sufficient")
        if sufficient is False and row.get("realized_pair_gross_60d") is not None:
            errors.append(f"{sym}: full realized_pair_gross_60d set despite insufficient obs={obs}")
        if row.get("realized_pair_gross_partial") is not None and sufficient is not False:
            errors.append(f"{sym}: partial realized pair field set without insufficient flag")

        if row.get("expected_decay_available") is True:
            expected_values = (
                row.get("expected_pair_pnl_p50_annual"),
                row.get("expected_gross_decay_p50_annual"),
                row.get("expected_gross_decay_annual"),
            )
            if not any(_finite(v) for v in expected_values):
                errors.append(f"{sym}: expected_decay_available=true but no expected decay value")

        for leg in ("etf", "underlying"):
            obs_key = f"vol_{leg}_annual_obs"
            label_key = f"vol_{leg}_annual_effective_label"
            window_key = f"vol_{leg}_annual_window"
            obs_val = row.get(obs_key)
            label = str(row.get(label_key) or "")
            if isinstance(obs_val, (int, float)) and int(obs_val) < 60:
                if not label.startswith("partial"):
                    errors.append(f"{sym}: {obs_key}={obs_val} but label={label!r}")
                if row.get(window_key) in {"12M", "6M", "3M"}:
                    warnings.append(f"{sym}: thin {leg} vol history ({obs_val} obs) shown from {row.get(window_key)} slot")

    return errors, warnings


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit dashboard JSON data quality.")
    parser.add_argument("--dashboard", type=Path, default=DASHBOARD_JSON)
    parser.add_argument("--fail-on-warnings", action="store_true")
    args = parser.parse_args()

    errors: list[str] = []
    warnings: list[str] = []

    for pattern in DEFAULT_JSON_GLOBS:
        for path in REPO.glob(pattern):
            if path.is_file() and _has_conflict_marker(path):
                errors.append(f"{path.relative_to(REPO)} contains merge-conflict marker text")

    if not args.dashboard.exists():
        errors.append(f"{args.dashboard} missing")
    else:
        dash_errors, dash_warnings = audit_dashboard(_load_json(args.dashboard))
        errors.extend(dash_errors)
        warnings.extend(dash_warnings)

    if errors:
        print("Dashboard data quality failures:")
        for msg in errors[:80]:
            print(f"  - {msg}")
        if len(errors) > 80:
            print(f"  ... {len(errors) - 80} more")
    if warnings:
        print("Dashboard data quality warnings:")
        for msg in warnings[:40]:
            print(f"  - {msg}")
        if len(warnings) > 40:
            print(f"  ... {len(warnings) - 40} more")

    if errors or (warnings and args.fail_on_warnings):
        return 1
    print("Dashboard data quality OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

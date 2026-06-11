#!/usr/bin/env python3
"""Audit dashboard data for thin-history and artifact-quality regressions."""
from __future__ import annotations

import argparse
import json
import math
import sys
import datetime as dt
from pathlib import Path
from typing import Any


REPO = Path(__file__).resolve().parent.parent
DASHBOARD_JSON = REPO / "data" / "dashboard_data.json"
METRICS_PARQUET = REPO / "data" / "etf_metrics_daily.parquet"
DEFAULT_JSON_GLOBS = ("data/*.json", "data/**/*.json")
MAX_CONTIGUOUS_METRICS_GAP_DAYS = 45
HARD_LIFECYCLE_GAP_DAYS = 365


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _has_conflict_marker(path: Path) -> bool:
    text = path.read_text(encoding="utf-8", errors="ignore")
    return any(marker in text for marker in ("<<<<<<<", "=======", ">>>>>>>"))


def _finite(v: Any) -> bool:
    try:
        return math.isfinite(float(v))
    except (TypeError, ValueError):
        return False


def _metrics_row_has_usable_prices(row: dict[str, Any]) -> bool:
    if not str(row.get("date") or "")[:10]:
        return False
    source_url = str(row.get("source_url") or "")
    source_provider = str(row.get("source_provider") or "")
    stale_kind = str(row.get("stale_kind") or "")
    if (
        source_url.startswith("carry_forward://")
        or source_provider.lower().startswith("carry_forward")
        or stale_kind.lower() == "carry_forward"
    ):
        return False
    try:
        px = row.get("close_price") if row.get("close_price") is not None else row.get("nav")
        return float(px) > 0 and float(row.get("underlying_adj_close")) > 0
    except (TypeError, ValueError):
        return False


def _date(value: Any) -> dt.date | None:
    ds = str(value or "")[:10]
    if len(ds) != 10:
        return None
    try:
        return dt.date.fromisoformat(ds)
    except ValueError:
        return None


def _source_key(row: dict[str, Any]) -> str:
    return "|".join(
        str(row.get(k) or "").strip().lower()
        for k in ("source_provider", "source_url", "status")
    )


def _tail_window_lifecycle_gap(rows: list[dict[str, Any]], obs: int) -> int | None:
    usable = []
    for row in rows:
        if not _metrics_row_has_usable_prices(row):
            continue
        d0 = _date(row.get("date"))
        if d0 is not None:
            usable.append((d0, row))
    usable.sort(key=lambda x: x[0])
    if len(usable) < 2:
        return None
    returns_needed = max(1, int(obs or 0))
    tail = usable[-(returns_needed + 1):]
    if len(tail) < 2:
        return None
    max_bad_gap: int | None = None
    for i in range(1, len(tail)):
        gap = (tail[i][0] - tail[i - 1][0]).days
        prev_src = _source_key(tail[i - 1][1])
        cur_src = _source_key(tail[i][1])
        source_changed = bool(prev_src or cur_src) and prev_src != cur_src
        if gap > HARD_LIFECYCLE_GAP_DAYS or (
            gap > MAX_CONTIGUOUS_METRICS_GAP_DAYS and source_changed
        ):
            max_bad_gap = max(max_bad_gap or 0, gap)
    return max_bad_gap


def audit_dashboard(
    payload: dict[str, Any],
    *,
    metrics_by_symbol: dict[str, list[dict[str, Any]]] | None = None,
) -> tuple[list[str], list[str]]:
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
        if (
            metrics_by_symbol
            and row.get("realized_pair_gross_60d") is not None
            and isinstance(obs, (int, float))
        ):
            max_gap = _tail_window_lifecycle_gap(metrics_by_symbol.get(sym.upper(), []), int(obs))
            if max_gap is not None:
                errors.append(
                    f"{sym}: full realized_pair_gross_60d crosses {max_gap}d metrics gap"
                )

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

    metrics_by_symbol: dict[str, list[dict[str, Any]]] | None = None
    if METRICS_PARQUET.exists():
        try:
            import pandas as pd

            df = pd.read_parquet(METRICS_PARQUET)
            df["ticker"] = df["ticker"].astype(str).str.upper()
            metrics_by_symbol = {
                sym: sub.sort_values("date").to_dict("records")
                for sym, sub in df.groupby("ticker")
            }
        except Exception as exc:
            warnings.append(f"Could not load metrics gap audit input: {exc}")

    if not args.dashboard.exists():
        errors.append(f"{args.dashboard} missing")
    else:
        dash_errors, dash_warnings = audit_dashboard(
            _load_json(args.dashboard),
            metrics_by_symbol=metrics_by_symbol,
        )
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

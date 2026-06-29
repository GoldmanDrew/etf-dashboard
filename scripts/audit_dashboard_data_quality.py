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
_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from market_calendar import is_nyse_session  # noqa: E402

DASHBOARD_JSON = REPO / "data" / "dashboard_data.json"
METRICS_PARQUET = REPO / "data" / "etf_metrics_daily.parquet"
CORP_ACTIONS_JSON = REPO / "data" / "corporate_actions.json"
DEFAULT_JSON_GLOBS = ("data/*.json", "data/**/*.json")
MAX_CONTIGUOUS_METRICS_GAP_DAYS = 45
HARD_LIFECYCLE_GAP_DAYS = 365
STALE_TAIL_CARRY_FORWARD_ROWS = 3
STALE_FEED_MAX_LAG_BDAYS = 4
SYSTEMIC_STALE_FRACTION = 0.5


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


def _is_carry_forward_row(row: dict[str, Any]) -> bool:
    return (
        str(row.get("source_url") or "").startswith("carry_forward://")
        or str(row.get("source_provider") or "").lower().startswith("carry_forward")
        or str(row.get("stale_kind") or "").lower() == "carry_forward"
    )


def _business_days_between(d0: dt.date, d1: dt.date) -> int:
    if d1 <= d0:
        return 0
    days = 0
    cur = d0
    while cur < d1:
        cur += dt.timedelta(days=1)
        if cur.weekday() < 5:
            days += 1
    return days


def audit_fabricated_adj_basis(
    metrics_by_symbol: dict[str, list[dict[str, Any]]],
    corp_payload: dict[str, Any] | None,
) -> list[str]:
    """Fail when any ticker's etf_adj_close has a cliff no declared split explains."""
    try:
        from price_basis import find_fabricated_adj_cliffs, parse_split_events_from_corp
    except ImportError as exc:
        return [f"price_basis import failed for adj-basis audit: {exc}"]
    errors: list[str] = []
    for sym, rows in sorted(metrics_by_symbol.items()):
        events = parse_split_events_from_corp(corp_payload or {"events": []}, sym)
        cliffs = find_fabricated_adj_cliffs(rows, events)
        for cliff in cliffs[:2]:
            errors.append(
                f"{sym}: fabricated etf_adj_close cliff on {cliff['date']} "
                f"(x{cliff['factor']:.2f} vs close; no matching split)"
            )
    return errors


def audit_stale_price_feeds(
    metrics_by_symbol: dict[str, list[dict[str, Any]]],
) -> tuple[list[str], list[str]]:
    """Warn on per-ticker stale tails; fail when the whole ingest has stalled."""
    errors: list[str] = []
    warnings: list[str] = []
    last_real_by_sym: dict[str, dt.date | None] = {}
    for sym, rows in metrics_by_symbol.items():
        dated = sorted(
            ((d, r) for r in rows if (d := _date(r.get("date"))) is not None),
            key=lambda x: x[0],
        )
        if not dated:
            continue
        real_dates = [d for d, r in dated if not _is_carry_forward_row(r)]
        last_real_by_sym[sym] = real_dates[-1] if real_dates else None
        tail_cf = 0
        for _d, row in reversed(dated):
            if _is_carry_forward_row(row):
                tail_cf += 1
            else:
                break
        if tail_cf >= STALE_TAIL_CARRY_FORWARD_ROWS:
            warnings.append(
                f"{sym}: last {tail_cf} metrics rows are carry_forward "
                f"(last real data {real_dates[-1] if real_dates else 'never'})"
            )
    real_dates_all = [d for d in last_real_by_sym.values() if d is not None]
    if not real_dates_all:
        return errors, warnings
    max_real = max(real_dates_all)
    stale_syms = [
        sym
        for sym, d in last_real_by_sym.items()
        if d is None or _business_days_between(d, max_real) > STALE_FEED_MAX_LAG_BDAYS
    ]
    frac = len(stale_syms) / max(1, len(last_real_by_sym))
    if frac > SYSTEMIC_STALE_FRACTION:
        errors.append(
            f"systemic ingest stall: {len(stale_syms)}/{len(last_real_by_sym)} tickers "
            f"have no real metrics within {STALE_FEED_MAX_LAG_BDAYS} business days of {max_real}"
        )
    return errors, warnings


def audit_metrics_calendar(
    metrics_by_symbol: dict[str, list[dict[str, Any]]],
) -> list[str]:
    errors: list[str] = []
    by_date: dict[dt.date, int] = {}
    samples: dict[dt.date, list[str]] = {}
    for sym, rows in metrics_by_symbol.items():
        for row in rows:
            d = _date(row.get("date"))
            if d is None or is_nyse_session(d):
                continue
            by_date[d] = by_date.get(d, 0) + 1
            samples.setdefault(d, [])
            if len(samples[d]) < 8:
                samples[d].append(sym)
    for d, n in sorted(by_date.items()):
        errors.append(
            f"etf_metrics_daily has {n} row(s) on non-NYSE session {d} "
            f"(sample: {', '.join(samples.get(d, []))})"
        )
    return errors


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

        obs = row.get("realized_pair_gross_20d_obs")
        sufficient = row.get("realized_pair_gross_20d_sufficient")
        if sufficient is False and row.get("realized_pair_gross_20d") is not None:
            errors.append(f"{sym}: full realized_pair_gross_20d set despite insufficient obs={obs}")
        if row.get("realized_pair_gross_partial") is not None and sufficient is not False:
            errors.append(f"{sym}: partial realized pair field set without insufficient flag")
        if (
            metrics_by_symbol
            and row.get("realized_pair_gross_20d") is not None
            and isinstance(obs, (int, float))
        ):
            max_gap = _tail_window_lifecycle_gap(metrics_by_symbol.get(sym.upper(), []), int(obs))
            if max_gap is not None:
                errors.append(
                    f"{sym}: full realized_pair_gross_20d crosses {max_gap}d metrics gap"
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

    if metrics_by_symbol:
        corp_payload = None
        if CORP_ACTIONS_JSON.exists():
            try:
                corp_payload = _load_json(CORP_ACTIONS_JSON)
            except (OSError, json.JSONDecodeError) as exc:
                warnings.append(f"corporate_actions.json unreadable: {exc}")
        errors.extend(audit_fabricated_adj_basis(metrics_by_symbol, corp_payload))
        errors.extend(audit_metrics_calendar(metrics_by_symbol))
        stale_errors, stale_warnings = audit_stale_price_feeds(metrics_by_symbol)
        errors.extend(stale_errors)
        warnings.extend(stale_warnings)

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

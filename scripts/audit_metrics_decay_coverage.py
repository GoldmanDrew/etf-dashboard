#!/usr/bin/env python3
"""Audit joint Decay-usable coverage of ``etf_metrics_daily``.

Decay horizons end at the last non-carry_forward joint (ETF + underlying) day.
This report buckets every universe ticker so repair / CI can target CF tails and
missing underlyings.

Usage::

    python scripts/audit_metrics_decay_coverage.py
    python scripts/audit_metrics_decay_coverage.py --fail-on-tradeable-lag
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from ingest_etf_metrics import (  # noqa: E402
    PARQUET_PATH,
    UNIVERSE_CSV,
    load_universe_tickers,
)
from market_calendar import is_nyse_session, previous_nyse_session  # noqa: E402

DEFAULT_REPORT = ROOT / "data" / "metrics_decay_coverage_report.json"
DEFAULT_CSV = ROOT / "data" / "metrics_decay_coverage_summary.csv"
DASHBOARD_JSON = ROOT / "data" / "dashboard_data.json"
MAX_PAIR_DRAG_GAP_DAYS = 5
TRADEABLE_MAX_LAG_SESSIONS = 2
TRADEABLE_LAG_FRACTION_FAIL = 0.15

MARKET_BACKED_STALE_KIND = "market_backed_no_issuer_nav"


def _is_carry_forward_row(row: dict[str, Any] | pd.Series) -> bool:
    src = str(row.get("source_url") or "")
    prov = str(row.get("source_provider") or "").lower()
    stale = str(row.get("stale_kind") or "").lower()
    return (
        src.startswith("carry_forward://")
        or prov.startswith("carry_forward")
        or stale == "carry_forward"
    )


def is_decay_joint_usable(row: dict[str, Any] | pd.Series) -> bool:
    """Mirror ``realized_gross_decay._metrics_row_has_usable_prices``."""
    if not str(row.get("date") or "")[:10]:
        return False
    if _is_carry_forward_row(row):
        return False
    try:
        close_like = row.get("close_price") if row.get("close_price") is not None else row.get("nav")
        if float(close_like) <= 0:
            return False
        if float(row.get("underlying_adj_close")) <= 0:
            return False
    except (TypeError, ValueError):
        return False
    return True


def _as_date(value: Any) -> date | None:
    ds = str(value or "")[:10]
    if len(ds) != 10:
        return None
    try:
        return date.fromisoformat(ds)
    except ValueError:
        return None


def _nyse_sessions_between(d0: date, d1: date) -> int:
    """Count NYSE sessions strictly after d0 through d1 inclusive."""
    if d1 <= d0:
        return 0
    n = 0
    cur = d0 + timedelta(days=1)
    while cur <= d1:
        if is_nyse_session(cur):
            n += 1
        cur += timedelta(days=1)
    return n


def _cf_tail_length(rows: list[dict[str, Any]]) -> int:
    n = 0
    for row in reversed(rows):
        if _is_carry_forward_row(row):
            n += 1
        else:
            break
    return n


def _gap_gt5_count(usable_dates: list[date], *, lookback_days: int = 60) -> int:
    if len(usable_dates) < 2:
        return 0
    cutoff = usable_dates[-1] - timedelta(days=lookback_days)
    window = [d for d in usable_dates if d >= cutoff]
    n = 0
    for a, b in zip(window, window[1:]):
        if (b - a).days > MAX_PAIR_DRAG_GAP_DAYS:
            n += 1
    return n


def _load_tradeable_symbols() -> set[str]:
    """Bucket 1/3 + inverse + YieldBOOST (+ FoF) from dashboard when available."""
    if not DASHBOARD_JSON.exists():
        return set()
    try:
        payload = json.loads(DASHBOARD_JSON.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return set()
    out: set[str] = set()
    for row in payload.get("records") or payload.get("rows") or []:
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol") or "").strip().upper()
        if not sym:
            continue
        pc = str(row.get("product_class") or "")
        bucket = row.get("bucket")
        is_yb = bool(row.get("is_yieldboost")) or pc in {
            "income_yieldboost",
            "income_yieldboost_fof",
        }
        tradeable = (
            bucket in (1, 3, "1", "3", "bucket_1", "bucket_3")
            or pc in {"letf", "inverse", "volatility_etp", "income_yieldboost", "income_yieldboost_fof"}
            or is_yb
        )
        if tradeable:
            out.add(sym)
    return out


def classify_ticker(
    rows: list[dict[str, Any]],
    *,
    panel_max: date,
) -> dict[str, Any]:
    dated = sorted(
        (r for r in rows if _as_date(r.get("date")) is not None),
        key=lambda r: str(r.get("date") or "")[:10],
    )
    usable = [r for r in dated if is_decay_joint_usable(r)]
    usable_dates = [_as_date(r.get("date")) for r in usable]
    usable_dates = [d for d in usable_dates if d is not None]
    last_ju = usable_dates[-1] if usable_dates else None
    last_row = dated[-1] if dated else None
    last_non_cf = next(
        (r for r in reversed(dated) if not _is_carry_forward_row(r)),
        None,
    )
    lag = _nyse_sessions_between(last_ju, panel_max) if last_ju else 999
    cf_tail = _cf_tail_length(dated)

    # market_fixable: recent CF (or any CF) rows that already have close+und
    market_fixable = False
    for r in reversed(dated[-10:]):
        if not _is_carry_forward_row(r):
            continue
        try:
            close_like = r.get("close_price") if r.get("close_price") is not None else r.get("nav")
            if float(close_like or 0) > 0 and float(r.get("underlying_adj_close") or 0) > 0:
                market_fixable = True
                break
        except (TypeError, ValueError):
            continue

    has_any_und = any(
        _safe_pos(r.get("underlying_adj_close")) for r in dated[-30:]
    )
    if last_ju is None and not dated:
        bucket = "dead_listing"
    elif last_ju is None and not has_any_und:
        bucket = "needs_underlying"
    elif last_ju is None:
        bucket = "needs_issuer_or_listing"
    elif lag <= TRADEABLE_MAX_LAG_SESSIONS:
        bucket = "current"
    elif market_fixable:
        bucket = "market_fixable"
    elif not has_any_und:
        bucket = "needs_underlying"
    else:
        bucket = "needs_issuer_or_listing"

    last_issuer_ok = next(
        (
            r
            for r in reversed(dated)
            if str(r.get("status") or "") == "ok"
            and not _is_carry_forward_row(r)
            and str(r.get("stale_kind") or "").lower() != MARKET_BACKED_STALE_KIND
        ),
        None,
    )
    return {
        "ticker": str((last_row or {}).get("ticker") or "").upper(),
        "bucket": bucket,
        "panel_max": panel_max.isoformat(),
        "last_row_date": str((last_row or {}).get("date") or "")[:10] or None,
        "last_joint_usable": last_ju.isoformat() if last_ju else None,
        "sessions_behind": lag,
        "cf_tail_length": cf_tail,
        "gap_gt5d_last_60d": _gap_gt5_count(usable_dates),
        "joint_usable_n": len(usable),
        "market_fixable": market_fixable,
        "last_non_cf_provider": str((last_non_cf or {}).get("source_provider") or "") or None,
        "last_issuer_ok_provider": str((last_issuer_ok or {}).get("source_provider") or "") or None,
        "last_issuer_ok_date": str((last_issuer_ok or {}).get("date") or "")[:10] or None,
        "last_stale_kind": str((last_row or {}).get("stale_kind") or "") or None,
    }


def _safe_pos(v: Any) -> bool:
    try:
        return float(v) > 0
    except (TypeError, ValueError):
        return False


def build_coverage_report(
    df: pd.DataFrame,
    universe: set[str] | None = None,
    *,
    tradeable: set[str] | None = None,
) -> dict[str, Any]:
    if df.empty:
        return {
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "panel_max": None,
            "summary": {},
            "tickers": [],
        }
    work = df.copy()
    work["ticker"] = work["ticker"].astype(str).str.upper()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    panel_max_raw = work["date"].max()
    panel_max = date.fromisoformat(str(panel_max_raw)[:10])
    if not is_nyse_session(panel_max):
        panel_max = previous_nyse_session(panel_max)

    uni = universe or set(load_universe_tickers())
    tradeable = tradeable if tradeable is not None else _load_tradeable_symbols()

    rows_out: list[dict[str, Any]] = []
    for ticker, g in work.groupby("ticker"):
        if uni and ticker not in uni:
            continue
        recs = g.sort_values("date").to_dict("records")
        info = classify_ticker(recs, panel_max=panel_max)
        info["ticker"] = str(ticker).upper()
        info["tradeable"] = info["ticker"] in tradeable if tradeable else False
        rows_out.append(info)

    rows_out.sort(key=lambda r: (-int(r["sessions_behind"]), r["ticker"]))
    by_bucket: dict[str, int] = {}
    for r in rows_out:
        by_bucket[r["bucket"]] = by_bucket.get(r["bucket"], 0) + 1

    tradeable_rows = [r for r in rows_out if r.get("tradeable")]
    tradeable_lagging = [
        r for r in tradeable_rows if int(r["sessions_behind"]) > TRADEABLE_MAX_LAG_SESSIONS
    ]
    cf_tail_ge3_with_market = [
        r for r in rows_out if int(r["cf_tail_length"]) >= 3 and r.get("market_fixable")
    ]

    return {
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "panel_max": panel_max.isoformat(),
        "tradeable_max_lag_sessions": TRADEABLE_MAX_LAG_SESSIONS,
        "summary": {
            "universe_n": len(rows_out),
            "by_bucket": by_bucket,
            "tradeable_n": len(tradeable_rows),
            "tradeable_lagging_n": len(tradeable_lagging),
            "tradeable_lagging_fraction": (
                len(tradeable_lagging) / max(1, len(tradeable_rows))
            ),
            "cf_tail_ge3_market_fixable_n": len(cf_tail_ge3_with_market),
            "current_n": by_bucket.get("current", 0),
            "market_fixable_n": by_bucket.get("market_fixable", 0),
        },
        "tradeable_lagging": tradeable_lagging[:50],
        "cf_tail_ge3_market_fixable": [
            {"ticker": r["ticker"], "cf_tail_length": r["cf_tail_length"]}
            for r in cf_tail_ge3_with_market[:50]
        ],
        "tickers": rows_out,
    }


def coverage_gate_errors(report: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    summary = report.get("summary") or {}
    frac = float(summary.get("tradeable_lagging_fraction") or 0.0)
    lag_n = int(summary.get("tradeable_lagging_n") or 0)
    tradeable_n = int(summary.get("tradeable_n") or 0)
    if tradeable_n > 0 and frac > TRADEABLE_LAG_FRACTION_FAIL:
        errors.append(
            f"decay coverage: {lag_n}/{tradeable_n} tradeable tickers "
            f"(>{TRADEABLE_LAG_FRACTION_FAIL:.0%}) have joint-usable lag "
            f"> {TRADEABLE_MAX_LAG_SESSIONS} NYSE sessions of panel max "
            f"{report.get('panel_max')}"
        )
    cf_n = int(summary.get("cf_tail_ge3_market_fixable_n") or 0)
    if cf_n > 0:
        errors.append(
            f"decay coverage: {cf_n} ticker(s) have carry_forward tails ≥3 "
            f"with market close+underlying present (should be market_backed)"
        )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet", type=Path, default=PARQUET_PATH)
    parser.add_argument("--report-out", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--csv-out", type=Path, default=DEFAULT_CSV)
    parser.add_argument(
        "--fail-on-tradeable-lag",
        action="store_true",
        help="Exit 1 when tradeable Decay coverage gate fails",
    )
    args = parser.parse_args()

    if not args.parquet.exists():
        print(f"missing {args.parquet}", file=sys.stderr)
        return 2
    df = pd.read_parquet(args.parquet)
    universe = set(load_universe_tickers()) if UNIVERSE_CSV.exists() else None
    report = build_coverage_report(df, universe)
    args.report_out.parent.mkdir(parents=True, exist_ok=True)
    args.report_out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    pd.DataFrame(report["tickers"]).to_csv(args.csv_out, index=False)

    summary = report["summary"]
    print(
        f"panel_max={report['panel_max']} universe={summary.get('universe_n')} "
        f"current={summary.get('current_n')} market_fixable={summary.get('market_fixable_n')} "
        f"tradeable_lagging={summary.get('tradeable_lagging_n')}/"
        f"{summary.get('tradeable_n')} "
        f"cf_tail_ge3_market={summary.get('cf_tail_ge3_market_fixable_n')}"
    )
    print(f"wrote {args.report_out}")
    print(f"wrote {args.csv_out}")

    errors = coverage_gate_errors(report) if args.fail_on_tradeable_lag else []
    if errors:
        print("Decay coverage gate failures:")
        for msg in errors:
            print(f"  - {msg}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

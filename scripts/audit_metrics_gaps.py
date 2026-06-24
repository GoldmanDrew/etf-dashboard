#!/usr/bin/env python3
"""Scan ``etf_metrics_daily`` for nullable market-field gaps and suggest fill strategies.

Usage:
    python scripts/audit_metrics_gaps.py
    python scripts/audit_metrics_gaps.py --start 2026-06-01 --ticker CBRZ
    python scripts/audit_metrics_gaps.py --apply-easy-fixes --dry-run
    python scripts/audit_metrics_gaps.py --apply-easy-fixes
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from ingest_etf_metrics import (  # noqa: E402
    PARQUET_PATH,
    backfill_close_prices_polygon_gaps,
    backfill_etf_adj_close_from_close_gaps,
    backfill_etf_adj_close_gaps,
    backfill_shares_traded_gaps,
    collapse_redundant_consecutive_rows,
    enforce_status_consistency,
    load_existing,
    repair_close_price_split_basis_mismatch,
    save_outputs,
    validate_df,
)
from market_calendar import is_nyse_session  # noqa: E402

LOGGER = logging.getLogger("audit_metrics_gaps")

# Fields the dashboard treats as market overlays (issuer NAV/AUM/shares are separate).
MARKET_FIELDS: dict[str, str] = {
    "close_price": "yahoo_or_polygon_close",
    "shares_traded": "yahoo_or_polygon_volume",
    "etf_adj_close": "yahoo_adj_close_or_close_copy",
    "underlying_adj_close": "yahoo_underlying_adj",
}

EXPECTED_MARKET_GAP_KINDS = {"issuer_early"}

ISSUER_FIELDS: dict[str, str] = {
    "nav": "issuer_feed",
    "aum": "issuer_feed",
    "shares_outstanding": "issuer_feed_or_aum_div_nav",
}


def _is_missing(series: pd.Series) -> pd.Series:
    num = pd.to_numeric(series, errors="coerce")
    return num.isna() | (num <= 0)


def audit_gaps(
    df: pd.DataFrame,
    *,
    start: date | None = None,
    end: date | None = None,
    ticker: str | None = None,
    status: str = "ok",
    include_expected_market_gaps: bool = False,
) -> dict:
    work = df.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.date
    work["ticker"] = work["ticker"].astype(str).str.upper()
    if ticker:
        work = work[work["ticker"] == ticker.upper()]
    if start:
        work = work[work["date"] >= start]
    if end:
        work = work[work["date"] <= end]
    if status:
        work = work[work["status"].astype(str) == status]

    all_fields = {**ISSUER_FIELDS, **MARKET_FIELDS}
    for col in all_fields:
        if col not in work.columns:
            work[col] = None
    if "stale_kind" not in work.columns:
        work["stale_kind"] = None

    rows_scanned = len(work)
    expected_market_gap_mask = work["stale_kind"].astype(str).isin(EXPECTED_MARKET_GAP_KINDS)
    actionable_market_mask = (
        pd.Series(True, index=work.index)
        if include_expected_market_gaps
        else ~expected_market_gap_mask
    )
    non_session_counts = {
        str(k): int(v)
        for k, v in (
            work.loc[~work["date"].map(lambda d: bool(d and is_nyse_session(d)))]
            .groupby("date")
            .size()
            .sort_index()
            .items()
        )
    }
    by_field: dict[str, dict] = {}
    gap_rows: list[dict] = []

    for field, strategy in all_fields.items():
        miss_all = _is_missing(work[field])
        if field in MARKET_FIELDS:
            miss = miss_all & actionable_market_mask
            expected_missing = int((miss_all & expected_market_gap_mask).sum())
        else:
            miss = miss_all
            expected_missing = 0
        n = int(miss.sum())
        by_field[field] = {
            "missing_rows": n,
            "fill_strategy": strategy,
            "missing_pct": round(n / rows_scanned, 4) if rows_scanned else 0.0,
        }
        if expected_missing:
            by_field[field]["expected_missing_rows"] = expected_missing
        if n:
            sub = work.loc[miss, ["date", "ticker", field]].copy()
            sub["field"] = field
            sub["fill_strategy"] = strategy
            sub["date"] = sub["date"].astype(str)
            gap_rows.extend(sub.to_dict(orient="records"))

    # Easy-fill candidates: close present but derived fields null.
    close_ok = ~_is_missing(work["close_price"])
    easy: dict[str, int] = {}
    if "etf_adj_close" in work.columns:
        easy["etf_adj_close_from_close"] = int(
            (close_ok & actionable_market_mask & _is_missing(work["etf_adj_close"])).sum()
        )
    if "shares_traded" in work.columns:
        easy["shares_traded_yahoo_or_polygon"] = int(
            (close_ok & actionable_market_mask & _is_missing(work["shares_traded"])).sum()
        )
    sh = pd.to_numeric(work.get("shares_outstanding"), errors="coerce")
    nav = pd.to_numeric(work.get("nav"), errors="coerce")
    aum = pd.to_numeric(work.get("aum"), errors="coerce")
    implied = aum / nav
    easy["shares_outstanding_from_aum_nav"] = int(
        (_is_missing(work["shares_outstanding"]) & nav.notna() & (nav > 0) & aum.notna()).sum()
    )
    if easy["shares_outstanding_from_aum_nav"]:
        easy["shares_outstanding_implied_sample"] = (
            f"can derive {easy['shares_outstanding_from_aum_nav']} row(s) as aum/nav"
        )

    by_date = (
        work.assign(_any_gap=False)
    )
    miss_any = pd.Series(False, index=work.index)
    expected_miss_any = pd.Series(False, index=work.index)
    for field in MARKET_FIELDS:
        field_missing = _is_missing(work[field])
        miss_any |= field_missing & actionable_market_mask
        expected_miss_any |= field_missing & expected_market_gap_mask
    by_date_counts = {
        str(k): int(v)
        for k, v in (
            work.loc[miss_any]
            .groupby("date")
            .size()
            .sort_index()
            .items()
        )
    }

    top_tickers = (
        work.loc[miss_any]
        .groupby("ticker")
        .size()
        .sort_values(ascending=False)
        .head(25)
        .astype(int)
        .to_dict()
    )
    expected_by_date = {
        str(k): int(v)
        for k, v in (
            work.loc[expected_miss_any]
            .groupby("date")
            .size()
            .sort_index()
            .items()
        )
    }
    expected_by_kind = (
        work.loc[expected_miss_any, "stale_kind"]
        .astype(str)
        .value_counts()
        .astype(int)
        .to_dict()
    )

    return {
        "build_time": datetime.now(UTC).isoformat(),
        "rows_scanned": rows_scanned,
        "date_range": {
            "start": str(work["date"].min()) if rows_scanned else None,
            "end": str(work["date"].max()) if rows_scanned else None,
        },
        "ticker_filter": ticker.upper() if ticker else None,
        "status_filter": status,
        "non_session_rows_by_date": non_session_counts,
        "expected_market_gap_rows_by_date": expected_by_date,
        "expected_market_gap_rows_by_kind": expected_by_kind,
        "include_expected_market_gaps": include_expected_market_gaps,
        "by_field": by_field,
        "easy_fill_candidates": easy,
        "gap_rows_by_date": by_date_counts,
        "top_tickers_with_gaps": top_tickers,
        "sample_gaps": gap_rows[:200],
    }


def apply_easy_fixes(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Run local backfill helpers that do not need issuer feeds."""
    out = df.copy()
    summary: dict[str, int] = {}

    out = backfill_etf_adj_close_gaps(out)
    out, n_close_copy = backfill_etf_adj_close_from_close_gaps(out)
    summary["etf_adj_close_from_close"] = n_close_copy

    out, n_vol = backfill_shares_traded_gaps(out)
    summary["shares_traded"] = n_vol

    out, n_poly = backfill_close_prices_polygon_gaps(out)
    summary["polygon_fields"] = n_poly

    out, n_adj2 = backfill_etf_adj_close_from_close_gaps(out)
    summary["etf_adj_close_post_polygon"] = n_adj2

    return out, summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit nullable fields in etf_metrics_daily.")
    parser.add_argument("--start", default=None, help="YYYY-MM-DD lower bound")
    parser.add_argument("--end", default=None, help="YYYY-MM-DD upper bound")
    parser.add_argument("--ticker", default=None, help="Single ticker filter")
    parser.add_argument("--status", default="ok", help="Row status filter (default: ok)")
    parser.add_argument(
        "--include-expected-market-gaps",
        action="store_true",
        help="Include intentional issuer-early market blanks in actionable gap counts.",
    )
    parser.add_argument("--json-out", default=None, help="Write full report JSON here")
    parser.add_argument(
        "--apply-easy-fixes",
        action="store_true",
        help="Run Yahoo/Polygon/close-copy backfills and save parquet",
    )
    parser.add_argument("--dry-run", action="store_true", help="With --apply-easy-fixes, skip save")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if not PARQUET_PATH.exists():
        LOGGER.error("missing %s", PARQUET_PATH)
        sys.exit(1)

    df = load_existing()
    start = datetime.strptime(args.start, "%Y-%m-%d").date() if args.start else None
    end = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else None

    report = audit_gaps(
        df,
        start=start,
        end=end,
        ticker=args.ticker,
        status=args.status or None,
        include_expected_market_gaps=bool(args.include_expected_market_gaps),
    )

    print(json.dumps({k: v for k, v in report.items() if k != "sample_gaps"}, indent=2))
    if report.get("sample_gaps"):
        print(f"\n--- first {min(10, len(report['sample_gaps']))} gap row(s) ---")
        for row in report["sample_gaps"][:10]:
            print(row)

    if args.json_out:
        Path(args.json_out).write_text(json.dumps(report, indent=2), encoding="utf-8")
        LOGGER.info("wrote %s", args.json_out)

    if args.apply_easy_fixes:
        updated, fix_summary = apply_easy_fixes(df)
        LOGGER.info("easy-fix summary: %s", fix_summary)
        if args.dry_run:
            LOGGER.info("--dry-run: not saving")
            return
        updated = enforce_status_consistency(updated)
        updated, _ = collapse_redundant_consecutive_rows(updated)
        updated, _ = repair_close_price_split_basis_mismatch(updated)
        validate_df(updated)
        save_outputs(updated)
        LOGGER.info("saved %s (%d rows)", PARQUET_PATH, len(updated))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Catch up stale ``etf_metrics_daily`` tails and repair issuer-lagged closes.

Runs the same repair chain as ``nightly.yml`` plus optional multi-day provider
ingest when the store lags the global max session.

Usage::

    python scripts/repair_metrics_staleness.py --apply
    python scripts/repair_metrics_staleness.py --apply --catchup-days 14
    python scripts/repair_metrics_staleness.py --apply --targeted-catchup --min-days-behind 2
    python scripts/repair_metrics_staleness.py --apply --skip-ingest
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REPORT_PATH = ROOT / "data" / "metrics_staleness_report.json"
sys.path.insert(0, str(ROOT / "scripts"))

from ingest_etf_metrics import (  # noqa: E402
    PARQUET_PATH,
    backfill_etf_adj_close_from_close_gaps,
    backfill_etf_adj_close_gaps,
    backfill_underlying_adj_close_gaps,
    collapse_redundant_consecutive_rows,
    enforce_status_consistency,
    fill_missing_shares_outstanding_from_aum_nav,
    load_existing,
    load_universe_tickers,
    load_universe_underlying_map,
    repair_close_price_split_basis_mismatch,
    repair_stale_issuer_close_from_market,
    save_outputs,
    validate_df,
)
from market_calendar import is_nyse_session, next_nyse_session, nyse_sessions, previous_nyse_session  # noqa: E402

LOGGER = logging.getLogger("repair_metrics_staleness")


def _run(cmd: list[str], *, label: str) -> int:
    LOGGER.info("=== %s ===", label)
    LOGGER.info("cmd: %s", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=ROOT, check=False)
    if proc.returncode != 0:
        LOGGER.warning("%s exited %d", label, proc.returncode)
    return int(proc.returncode)


def list_stale_tickers(
    df: pd.DataFrame,
    universe: set[str],
    *,
    min_days_behind: int = 1,
    limit: int | None = None,
) -> list[dict]:
    if df.empty:
        return []
    work = df.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.normalize()
    global_max = work["date"].max()
    rows: list[dict] = []
    for ticker, g in work.groupby("ticker"):
        if ticker not in universe:
            continue
        last = g["date"].max()
        behind = int((global_max - last).days) if pd.notna(last) else 999
        if behind >= min_days_behind:
            rows.append({"ticker": ticker, "last_date": str(last.date()), "days_behind": behind})
    rows.sort(key=lambda x: (-x["days_behind"], x["ticker"]))
    if limit is not None:
        return rows[:limit]
    return rows


def tail_staleness_report(df: pd.DataFrame, universe: set[str], *, min_days_behind: int = 2) -> dict:
    stale = list_stale_tickers(df, universe, min_days_behind=min_days_behind)
    if df.empty:
        return {"global_max": None, "universe_stale": 0, "worst": []}
    global_max = pd.to_datetime(df["date"], errors="coerce").max()
    return {
        "global_max": str(global_max.date()),
        "universe_stale": len(stale),
        "worst": stale[:25],
    }


def resolve_catchup_range(catchup_days: int, *, end: date | None = None) -> tuple[date, date]:
    end_d = end or date.today()
    if is_nyse_session(end_d):
        end_session = end_d
    else:
        end_session = previous_nyse_session(end_d)
    sessions = nyse_sessions(end_session - timedelta(days=max(catchup_days * 2, 30)), end_session)
    if not sessions:
        return end_session, end_session
    want = sessions[-catchup_days:] if len(sessions) >= catchup_days else sessions
    return want[0], want[-1]


def resolve_targeted_catchup_sessions(
    df: pd.DataFrame,
    universe: set[str],
    *,
    min_days_behind: int,
    end: date | None = None,
) -> list[date]:
    """NYSE sessions to ingest: day after earliest stale tail through global max."""
    stale = list_stale_tickers(df, universe, min_days_behind=min_days_behind)
    if not stale:
        return []
    end_d = end or date.today()
    if is_nyse_session(end_d):
        end_session = end_d
    else:
        end_session = previous_nyse_session(end_d)
    earliest_last = min(date.fromisoformat(r["last_date"]) for r in stale)
    start_session = next_nyse_session(earliest_last)
    if start_session > end_session:
        return []
    return nyse_sessions(start_session, end_session)


def apply_tail_repairs(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    underlying_map = load_universe_underlying_map()
    out = df.copy()
    summary: dict[str, int] = {}

    out, n_stale = repair_stale_issuer_close_from_market(out)
    summary["stale_issuer_close"] = n_stale

    out = backfill_etf_adj_close_gaps(out)
    out, n_adj = backfill_etf_adj_close_from_close_gaps(out)
    summary["etf_adj_close_from_close"] = n_adj

    out = backfill_underlying_adj_close_gaps(out, underlying_map)
    after_und = int(pd.to_numeric(out["underlying_adj_close"], errors="coerce").notna().sum())
    before_und = int(pd.to_numeric(df["underlying_adj_close"], errors="coerce").notna().sum())
    summary["underlying_adj_close_delta"] = after_und - before_und

    out, n_shares = fill_missing_shares_outstanding_from_aum_nav(out)
    summary["shares_outstanding_from_aum_nav"] = n_shares

    out = enforce_status_consistency(out)
    out, _ = collapse_redundant_consecutive_rows(out)
    out, _ = repair_close_price_split_basis_mismatch(out)
    return out, summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Catch up stale etf_metrics_daily and repair issuer lag.")
    parser.add_argument("--apply", action="store_true", help="Write parquet/csv/json outputs")
    parser.add_argument("--skip-ingest", action="store_true", help="Skip multi-day provider ingest")
    parser.add_argument(
        "--minimal",
        action="store_true",
        help="Only run in-process tail repairs (no subprocess pipeline)",
    )
    parser.add_argument(
        "--targeted-catchup",
        action="store_true",
        help="Ingest only NYSE sessions needed to extend stale ticker tails",
    )
    parser.add_argument(
        "--min-days-behind",
        type=int,
        default=2,
        help="Min calendar days behind global max to count as stale (default 2)",
    )
    parser.add_argument("--catchup-days", type=int, default=10, help="NYSE sessions to re-ingest (default 10)")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD catch-up end (default: latest NYSE session)")
    parser.add_argument(
        "--report-out",
        default=None,
        help="Write before/after JSON report (default: data/metrics_staleness_report.json when --apply)",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if not PARQUET_PATH.exists():
        LOGGER.error("Missing %s", PARQUET_PATH)
        return 1

    end_arg = None
    if args.end_date:
        end_arg = date.fromisoformat(args.end_date)
    universe = set(load_universe_tickers())

    before = load_existing()
    before_report = tail_staleness_report(before, universe)
    LOGGER.info("Before: global_max=%s universe_stale=%d", before_report["global_max"], before_report["universe_stale"])

    ingest_sessions: list[date] = []
    if not args.minimal and not args.skip_ingest:
        if args.targeted_catchup:
            ingest_sessions = resolve_targeted_catchup_sessions(
                before,
                universe,
                min_days_behind=args.min_days_behind,
                end=end_arg,
            )
            if ingest_sessions:
                LOGGER.info(
                    "Targeted catchup: %d session(s) for tickers >=%d day(s) behind: %s .. %s",
                    len(ingest_sessions),
                    args.min_days_behind,
                    ingest_sessions[0],
                    ingest_sessions[-1],
                )
            else:
                LOGGER.info("Targeted catchup: no stale tickers need session ingest")
        else:
            start_d, end_d = resolve_catchup_range(args.catchup_days, end=end_arg)
            ingest_sessions = nyse_sessions(start_d, end_d)
            LOGGER.info("Single-day ingest for %d NYSE session(s): %s .. %s", len(ingest_sessions), start_d, end_d)

        for session in ingest_sessions:
            rc = _run(
                [
                    sys.executable,
                    "scripts/ingest_etf_metrics.py",
                    "--start-date",
                    session.isoformat(),
                    "--end-date",
                    session.isoformat(),
                    "--lookback-days",
                    "10",
                    "--polygon-lookback-days",
                    "5",
                ],
                label=f"ingest {session}",
            )
            if rc != 0:
                LOGGER.warning("Ingest %s returned %d; continuing", session, rc)

    if not args.minimal:
        _run(
            [sys.executable, "scripts/repair_rex_session_nav_close.py", "--lookback-days", "21", "--apply"],
            label="repair REX NAV/close",
        )
        _run([sys.executable, "scripts/backfill_underlying_adj_close.py"], label="backfill underlying adj")
        _run([sys.executable, "scripts/backfill_close_prices.py", "--chunk", "50"], label="backfill close prices")
        _run([sys.executable, "scripts/backfill_etf_adj_close.py"], label="backfill etf adj close")
        _run(
            [sys.executable, "scripts/audit_metrics_gaps.py", "--apply-easy-fixes"],
            label="audit easy fixes",
        )

    df = load_existing()
    repaired, fix_summary = apply_tail_repairs(df)
    LOGGER.info("Tail repair summary: %s", fix_summary)

    if args.apply:
        validate_df(repaired)
        save_outputs(repaired)
        LOGGER.info("Saved metrics store (%d rows)", len(repaired))
    else:
        LOGGER.info("Dry-run: pass --apply to persist tail repairs")

    after_df = repaired if args.apply else load_existing()
    after_report = tail_staleness_report(after_df, universe)
    stale_full = list_stale_tickers(after_df, universe, min_days_behind=args.min_days_behind, limit=50)
    report = {
        "build_time": datetime.now(UTC).isoformat(),
        "catchup_mode": "targeted" if args.targeted_catchup else ("full" if ingest_sessions else "none"),
        "ingest_sessions": [s.isoformat() for s in ingest_sessions],
        "min_days_behind": args.min_days_behind,
        "before": before_report,
        "after": after_report,
        "stale_tickers": stale_full,
        "tail_repairs": fix_summary,
        "applied": bool(args.apply),
    }
    LOGGER.info(
        "After: global_max=%s universe_stale=%d (was %d)",
        after_report["global_max"],
        after_report["universe_stale"],
        before_report["universe_stale"],
    )
    report_path = Path(args.report_out) if args.report_out else (DEFAULT_REPORT_PATH if args.apply else None)
    if report_path:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
        LOGGER.info("Wrote report %s", report_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

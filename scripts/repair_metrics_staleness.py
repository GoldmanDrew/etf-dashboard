#!/usr/bin/env python3
"""Catch up stale ``etf_metrics_daily`` tails and repair issuer-lagged closes.

Runs the same repair chain as ``nightly.yml`` plus optional multi-day provider
ingest when the store lags the global max session.

Usage::

    python scripts/repair_metrics_staleness.py --apply
    python scripts/repair_metrics_staleness.py --apply --catchup-days 14
    python scripts/repair_metrics_staleness.py --apply --skip-ingest
"""
from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from ingest_etf_metrics import (  # noqa: E402
    PARQUET_PATH,
    backfill_etf_adj_close_from_close_gaps,
    backfill_etf_adj_close_gaps,
    backfill_underlying_adj_close_gaps,
    collapse_redundant_consecutive_rows,
    enforce_status_consistency,
    load_existing,
    load_universe_tickers,
    load_universe_underlying_map,
    repair_close_price_split_basis_mismatch,
    repair_stale_issuer_close_from_market,
    save_outputs,
    validate_df,
)
from market_calendar import is_nyse_session, nyse_sessions, previous_nyse_session  # noqa: E402

LOGGER = logging.getLogger("repair_metrics_staleness")


def _run(cmd: list[str], *, label: str) -> int:
    LOGGER.info("=== %s ===", label)
    LOGGER.info("cmd: %s", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=ROOT, check=False)
    if proc.returncode != 0:
        LOGGER.warning("%s exited %d", label, proc.returncode)
    return int(proc.returncode)


def tail_staleness_report(df: pd.DataFrame, universe: set[str]) -> dict:
    if df.empty:
        return {"global_max": None, "universe_stale": 0, "worst": []}
    work = df.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.normalize()
    global_max = work["date"].max()
    rows = []
    for ticker, g in work.groupby("ticker"):
        if ticker not in universe:
            continue
        last = g["date"].max()
        behind = int((global_max - last).days) if pd.notna(last) else 999
        if behind > 0:
            rows.append({"ticker": ticker, "last_date": str(last.date()), "days_behind": behind})
    rows.sort(key=lambda x: (-x["days_behind"], x["ticker"]))
    return {
        "global_max": str(global_max.date()),
        "universe_stale": len(rows),
        "worst": rows[:25],
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
    parser.add_argument("--catchup-days", type=int, default=10, help="NYSE sessions to re-ingest (default 10)")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD catch-up end (default: latest NYSE session)")
    parser.add_argument("--report-out", default=None, help="Write before/after JSON report")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if not PARQUET_PATH.exists():
        LOGGER.error("Missing %s", PARQUET_PATH)
        return 1

    end_arg = None
    if args.end_date:
        end_arg = date.fromisoformat(args.end_date)
    start_d, end_d = resolve_catchup_range(args.catchup_days, end=end_arg)
    universe = set(load_universe_tickers())

    before = load_existing()
    before_report = tail_staleness_report(before, universe)
    LOGGER.info("Before: global_max=%s universe_stale=%d", before_report["global_max"], before_report["universe_stale"])

    if not args.minimal:
        if not args.skip_ingest:
            sessions = nyse_sessions(start_d, end_d)
            LOGGER.info("Single-day ingest for %d NYSE session(s): %s .. %s", len(sessions), start_d, end_d)
            for session in sessions:
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

    after_report = tail_staleness_report(repaired if args.apply else load_existing(), universe)
    report = {
        "catchup_range": [start_d.isoformat(), end_d.isoformat()],
        "before": before_report,
        "after": after_report,
        "tail_repairs": fix_summary,
        "applied": bool(args.apply),
    }
    LOGGER.info(
        "After: global_max=%s universe_stale=%d (was %d)",
        after_report["global_max"],
        after_report["universe_stale"],
        before_report["universe_stale"],
    )
    if args.report_out:
        Path(args.report_out).write_text(json.dumps(report, indent=2), encoding="utf-8")
        LOGGER.info("Wrote report %s", args.report_out)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

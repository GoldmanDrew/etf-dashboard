#!/usr/bin/env python3
"""Heal Decay-usable joint coverage in ``etf_metrics_daily``.

1. Rewrite ``carry_forward`` rows that already have session close (+ optional und)
   into ``market_backed_no_issuer_nav`` (Decay-usable).
2. Optionally backfill missing close / underlying via existing gap helpers.
3. Re-audit coverage and write ``data/metrics_decay_repair_report.json``.

Usage::

    python scripts/repair_metrics_decay_coverage.py
    python scripts/repair_metrics_decay_coverage.py --apply
    python scripts/repair_metrics_decay_coverage.py --apply --backfill-gaps
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from audit_metrics_decay_coverage import (  # noqa: E402
    DEFAULT_REPORT as COVERAGE_REPORT_PATH,
    build_coverage_report,
    coverage_gate_errors,
)
from ingest_etf_metrics import (  # noqa: E402
    PARQUET_PATH,
    backfill_etf_adj_close_from_close_gaps,
    backfill_underlying_adj_close_gaps,
    load_existing,
    load_universe_tickers,
    load_universe_underlying_map,
    promote_carry_forward_rows_with_market,
    save_outputs,
    stamp_metric_asof_metadata,
    validate_df,
)

LOGGER = logging.getLogger("repair_metrics_decay_coverage")
DEFAULT_REPAIR_REPORT = ROOT / "data" / "metrics_decay_repair_report.json"


def _is_cf(row: dict[str, Any]) -> bool:
    src = str(row.get("source_url") or "")
    prov = str(row.get("source_provider") or "").lower()
    stale = str(row.get("stale_kind") or "").lower()
    return (
        src.startswith("carry_forward://")
        or prov.startswith("carry_forward")
        or stale == "carry_forward"
    )


def repair_decay_coverage(
    df: pd.DataFrame,
    *,
    backfill_gaps: bool = False,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Promote CF→market_backed; optionally backfill close/und gaps."""
    before = build_coverage_report(df, set(load_universe_tickers()))
    work = df.copy()
    work, n_promoted = promote_carry_forward_rows_with_market(work)

    n_und = 0
    n_adj = 0
    if backfill_gaps:
        und_map = load_universe_underlying_map()
        work = backfill_underlying_adj_close_gaps(work, und_map)
        # Count rows that gained und after promote path — best-effort via re-promote
        work, n_promoted2 = promote_carry_forward_rows_with_market(work)
        n_promoted += n_promoted2
        work, n_adj = backfill_etf_adj_close_from_close_gaps(work)
        # backfill_underlying_adj_close_gaps mutates in place; expose a soft count
        n_und = int(
            pd.to_numeric(work.get("underlying_adj_close"), errors="coerce").gt(0).sum()
        )

    work = stamp_metric_asof_metadata(work)
    after = build_coverage_report(work, set(load_universe_tickers()))

    fixed = []
    before_by = {r["ticker"]: r for r in before.get("tickers") or []}
    for r in after.get("tickers") or []:
        prev = before_by.get(r["ticker"]) or {}
        if int(prev.get("sessions_behind") or 999) > int(r.get("sessions_behind") or 999):
            fixed.append(
                {
                    "ticker": r["ticker"],
                    "before_behind": prev.get("sessions_behind"),
                    "after_behind": r.get("sessions_behind"),
                    "before_bucket": prev.get("bucket"),
                    "after_bucket": r.get("bucket"),
                    "last_joint_usable": r.get("last_joint_usable"),
                }
            )

    still_blocked = [
        r
        for r in (after.get("tickers") or [])
        if r.get("bucket") not in {"current"} and int(r.get("sessions_behind") or 0) > 2
    ]

    report = {
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "rows_promoted_cf_to_market_backed": int(n_promoted),
        "backfill_gaps": bool(backfill_gaps),
        "etf_adj_from_close_fills": int(n_adj),
        "underlying_positive_rows_after": int(n_und) if backfill_gaps else None,
        "summary_before": before.get("summary"),
        "summary_after": after.get("summary"),
        "tickers_improved": fixed[:100],
        "still_blocked_n": len(still_blocked),
        "still_blocked_sample": [
            {
                "ticker": r["ticker"],
                "bucket": r["bucket"],
                "sessions_behind": r["sessions_behind"],
                "last_joint_usable": r["last_joint_usable"],
            }
            for r in still_blocked[:40]
        ],
        "coverage_gate_errors": coverage_gate_errors(after),
    }
    return work, report


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write repaired metrics store")
    parser.add_argument(
        "--backfill-gaps",
        action="store_true",
        help="Also run underlying / etf_adj gap backfills before promote",
    )
    parser.add_argument("--report-out", type=Path, default=DEFAULT_REPAIR_REPORT)
    parser.add_argument("--coverage-out", type=Path, default=COVERAGE_REPORT_PATH)
    parser.add_argument(
        "--fail-on-gate",
        action="store_true",
        help="Exit 1 when post-repair tradeable Decay coverage gate fails",
    )
    args = parser.parse_args()

    df = load_existing(PARQUET_PATH)
    if df.empty:
        LOGGER.error("empty metrics store at %s", PARQUET_PATH)
        return 2

    repaired, report = repair_decay_coverage(df, backfill_gaps=args.backfill_gaps)
    args.report_out.parent.mkdir(parents=True, exist_ok=True)
    args.report_out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    LOGGER.info(
        "promoted=%s improved=%s still_blocked=%s",
        report["rows_promoted_cf_to_market_backed"],
        len(report["tickers_improved"]),
        report["still_blocked_n"],
    )
    LOGGER.info("wrote %s", args.report_out)

    coverage = build_coverage_report(repaired, set(load_universe_tickers()))
    args.coverage_out.write_text(json.dumps(coverage, indent=2), encoding="utf-8")
    LOGGER.info("wrote %s", args.coverage_out)

    if args.apply:
        validate_df(repaired)
        save_outputs(repaired)
        LOGGER.info("saved repaired metrics to %s", PARQUET_PATH)
    else:
        LOGGER.info("dry-run only (pass --apply to write)")

    if args.fail_on_gate and report.get("coverage_gate_errors"):
        for msg in report["coverage_gate_errors"]:
            LOGGER.error("%s", msg)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

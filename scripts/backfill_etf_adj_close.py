#!/usr/bin/env python3
"""Backfill ``etf_adj_close`` on the canonical etf_metrics_daily store.

Daily ingest attaches Yahoo adjusted close for the run window; this script
fills historical gaps (rows with ``close_price`` but null ``etf_adj_close``).

Usage::

    python scripts/backfill_etf_adj_close.py
    python scripts/backfill_etf_adj_close.py --only-missing
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from ingest_etf_metrics import (  # noqa: E402
    PARQUET_PATH,
    backfill_etf_adj_close_from_close_gaps,
    backfill_etf_adj_close_gaps,
    backfill_split_adjusted_etf_adj_close,
    load_existing,
    repair_fabricated_etf_adj_basis,
    save_outputs,
    validate_df,
)

LOGGER = logging.getLogger("backfill_etf_adj_close")


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill etf_adj_close on etf_metrics_daily.")
    parser.add_argument(
        "--only-missing",
        action="store_true",
        default=True,
        help="Only fill rows where etf_adj_close is null (default).",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if not PARQUET_PATH.exists():
        LOGGER.error("Metrics store missing: %s", PARQUET_PATH)
        sys.exit(1)

    df = load_existing()
    if df.empty:
        LOGGER.warning("Empty metrics store; nothing to backfill.")
        return

    if "etf_adj_close" not in df.columns:
        df["etf_adj_close"] = None

    before = int(pd.to_numeric(df["etf_adj_close"], errors="coerce").notna().sum())
    updated = backfill_etf_adj_close_gaps(df)
    updated, n_copy = backfill_etf_adj_close_from_close_gaps(updated)
    if n_copy:
        LOGGER.info("etf_adj_close from close_price fallback: +%d", n_copy)
    updated = backfill_split_adjusted_etf_adj_close(updated)
    updated, n_fab = repair_fabricated_etf_adj_basis(updated)
    if n_fab:
        LOGGER.info("Repaired fabricated adj basis on %d row(s)", n_fab)
    after = int(pd.to_numeric(updated["etf_adj_close"], errors="coerce").notna().sum())
    LOGGER.info("etf_adj_close non-null: %d -> %d (+%d)", before, after, after - before)

    if after <= before and n_fab == 0 and args.only_missing:
        LOGGER.info("No new etf_adj_close values; skipping save.")
        return

    validate_df(updated)
    save_outputs(updated)
    LOGGER.info("Saved %s", PARQUET_PATH)


if __name__ == "__main__":
    main()

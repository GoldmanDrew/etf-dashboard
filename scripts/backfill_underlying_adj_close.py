#!/usr/bin/env python3
"""Fill ``underlying_adj_close`` gaps in ``etf_metrics_daily`` using Yahoo.

Run after ``ingest_etf_metrics.py`` or when the ingest job **skipped** the full
network pull (``ETF_METRICS_SKIP_IF_RECENT_HOURS``) — in that case the main
ingest never reaches ``backfill_underlying_adj_close_gaps`` on the merged store,
so historical nulls would otherwise persist forever.

Usage::

    python scripts/backfill_underlying_adj_close.py

Exits 0 even when nothing to do. Respects ``ETF_METRICS_DISABLE_YFINANCE=1``.
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from ingest_etf_metrics import (  # noqa: E402
    PARQUET_PATH,
    backfill_underlying_adj_close_gaps,
    load_existing,
    load_universe_underlying_map,
    save_outputs,
    validate_df,
)

LOGGER = logging.getLogger("backfill_underlying_adj_close")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if os.getenv("ETF_METRICS_DISABLE_YFINANCE", "").lower() in ("1", "true", "yes"):
        LOGGER.info("ETF_METRICS_DISABLE_YFINANCE set; exiting without changes")
        return
    if not PARQUET_PATH.exists():
        LOGGER.warning("Missing %s — nothing to backfill", PARQUET_PATH)
        return

    df = load_existing()
    if df.empty:
        LOGGER.info("Existing metrics empty; nothing to backfill")
        return

    underlying_map = load_universe_underlying_map()
    before = int(pd.to_numeric(df["underlying_adj_close"], errors="coerce").notna().sum())
    merged = backfill_underlying_adj_close_gaps(df.copy(), underlying_map)
    after = int(pd.to_numeric(merged["underlying_adj_close"], errors="coerce").notna().sum())

    if after <= before:
        LOGGER.info(
            "underlying_adj_close non-null rows: %d -> %d (no new fills); not writing",
            before,
            after,
        )
        return

    validate_df(merged)
    save_outputs(merged)
    LOGGER.info(
        "Wrote etf_metrics_daily.* after underlying_adj_close backfill (+ %d non-null rows)",
        after - before,
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Repair ETF metrics rows that are not completed NYSE sessions.

This is safe to run after a bad scheduled/manual ingest.  It removes weekend
and full-day holiday rows, clears incomplete market overlays from early issuer
rows, then rewrites the canonical metrics outputs through ``save_outputs``.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path
import sys

if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from ingest_etf_metrics import (  # noqa: E402
    clear_issuer_early_market_fields,
    filter_metrics_to_nyse_sessions,
    load_existing,
    save_outputs,
    validate_df,
)

LOGGER = logging.getLogger("repair_etf_metrics_calendar")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="report only; do not save")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    df = load_existing()
    before = len(df)
    repaired, n_non_session = filter_metrics_to_nyse_sessions(df)
    repaired, n_early = clear_issuer_early_market_fields(repaired)
    LOGGER.info(
        "calendar repair: rows_before=%d rows_after=%d non_session_removed=%d issuer_early_market_cleared=%d",
        before,
        len(repaired),
        n_non_session,
        n_early,
    )
    validate_df(repaired)
    if args.dry_run:
        return 0
    save_outputs(repaired)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

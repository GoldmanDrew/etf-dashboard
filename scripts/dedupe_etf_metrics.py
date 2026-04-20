#!/usr/bin/env python3
"""One-shot cleanup: remove redundant consecutive rows per ticker (same as ingest collapse).

Run from the repo root:
    python scripts/dedupe_etf_metrics.py

Uses ``collapse_redundant_consecutive_rows`` from ``ingest_etf_metrics`` so logic stays in one place.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from ingest_etf_metrics import (  # noqa: E402
    PARQUET_PATH,
    collapse_redundant_consecutive_rows,
    save_outputs,
)


def main() -> None:
    df = pd.read_parquet(PARQUET_PATH)
    before = len(df)
    cleaned, dropped = collapse_redundant_consecutive_rows(df)
    print(f"Input rows:  {before}")
    print(f"Dropped:     {dropped}")
    print(f"Output rows: {len(cleaned)}")

    removed_mask = ~df.set_index(["ticker", "date"]).index.isin(
        cleaned.set_index(["ticker", "date"]).index
    )
    removed = df.loc[removed_mask]
    if not removed.empty:
        print("\nRemoved rows by status / provider:")
        print(
            removed.groupby(["status", "source_provider"]).size()
            .reset_index(name="count")
            .to_string(index=False)
        )

    save_outputs(cleaned)
    print("\nRewrote: etf_metrics_daily.{parquet,csv,json}, etf_metrics_latest.json, etf_metrics_health.json")


if __name__ == "__main__":
    main()

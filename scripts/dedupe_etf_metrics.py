#!/usr/bin/env python3
"""One-shot cleanup: remove redundant consecutive rows per ticker.

A row is considered redundant when its (nav, aum, shares_outstanding) triple is
identical to the previous (kept) row for the same ticker.  NaNs are treated as
equal.  The earliest row in each "run" of identical values is preserved so the
dashboard continues to show the date a metric first appeared/was last
refreshed, while the redundant carry-forward rows are dropped.

Run from the repo root:
    python scripts/dedupe_etf_metrics.py
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from ingest_etf_metrics import PARQUET_PATH, save_outputs  # noqa: E402


def _eq(a, b) -> bool:
    if a is None and b is None:
        return True
    try:
        fa, fb = float(a), float(b)
        if math.isnan(fa) and math.isnan(fb):
            return True
        return fa == fb
    except (TypeError, ValueError):
        return a == b


def dedupe(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.sort_values(["ticker", "date"]).reset_index(drop=True)

    keep_mask = [True] * len(df)
    last_by_ticker: dict[str, tuple] = {}
    for i, row in df.iterrows():
        t = row["ticker"]
        triple = (row.get("nav"), row.get("aum"), row.get("shares_outstanding"))
        prev = last_by_ticker.get(t)
        if prev is not None and all(_eq(a, b) for a, b in zip(prev, triple)):
            keep_mask[i] = False
        else:
            last_by_ticker[t] = triple

    cleaned = df.loc[keep_mask].reset_index(drop=True)
    dropped = int(sum(1 for k in keep_mask if not k))
    return cleaned, dropped


def main() -> None:
    df = pd.read_parquet(PARQUET_PATH)
    before = len(df)
    cleaned, dropped = dedupe(df)
    print(f"Input rows:  {before}")
    print(f"Dropped:     {dropped}")
    print(f"Output rows: {len(cleaned)}")

    # Show per-status breakdown of what was removed
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

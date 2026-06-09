#!/usr/bin/env python3
"""
Backfill ``close_price`` on the canonical etf_metrics_daily store.

The dashboard gained a per-day closing-price column alongside NAV so the UI can
plot premium/discount to NAV. New ingest runs write ``close_price`` going
forward, but historical rows predate the column. This script fills them in one
pass using ``yfinance.download`` with a symbol list (single multi-ticker
request rather than one-per-ticker).

Usage:
    python scripts/backfill_close_prices.py
    python scripts/backfill_close_prices.py --chunk 50 --start 2024-01-01
    python scripts/backfill_close_prices.py --only-missing      # default
    python scripts/backfill_close_prices.py --force             # overwrite

The script is idempotent and by default leaves non-null close_price values
alone. Re-runs only hit Yahoo for rows still missing a close.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

# ``scripts/`` is on sys.path when invoked via ``python scripts/...``, but also
# support ``python -m scripts.backfill_close_prices`` by padding explicitly.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from ingest_etf_metrics import (  # noqa: E402
    CSV_PATH,
    JSON_PATH,
    LATEST_JSON_PATH,
    HEALTH_JSON_PATH,
    PARQUET_PATH,
    REQUIRED_COLUMNS,
    backfill_close_prices_polygon_gaps,
    collapse_redundant_consecutive_rows,
    enforce_status_consistency,
    load_existing,
    repair_close_price_split_basis_mismatch,
    save_outputs,
    validate_df,
)


LOGGER = logging.getLogger("backfill_close_prices")


def fetch_close_history(
    tickers: list[str],
    start: date,
    end: date,
) -> pd.DataFrame:
    """One batched yfinance call for the full (tickers × dates) matrix."""
    if not tickers:
        return pd.DataFrame(columns=["date", "ticker", "close_price"])
    try:
        import yfinance as yf
    except Exception as e:
        LOGGER.error("yfinance not available: %s", e)
        return pd.DataFrame(columns=["date", "ticker", "close_price"])

    start_s = start.isoformat()
    end_s = (end + timedelta(days=1)).isoformat()
    LOGGER.info(
        "yfinance batch: %d tickers, %s..%s", len(tickers), start_s, end_s,
    )
    try:
        raw = yf.download(
            tickers=list(tickers),
            start=start_s,
            end=end_s,
            interval="1d",
            auto_adjust=False,
            actions=False,
            group_by="ticker",
            threads=True,
            progress=False,
        )
    except Exception as e:
        LOGGER.warning("yfinance batch failed: %s", e)
        return pd.DataFrame(columns=["date", "ticker", "close_price"])

    if raw is None or raw.empty:
        LOGGER.warning("yfinance batch returned empty frame")
        return pd.DataFrame(columns=["date", "ticker", "close_price"])

    rows: list[dict] = []
    if isinstance(raw.columns, pd.MultiIndex):
        for t in tickers:
            if t not in raw.columns.get_level_values(0):
                continue
            try:
                sub = raw[t]
            except KeyError:
                continue
            if "Close" not in sub.columns:
                continue
            for idx, v in sub["Close"].dropna().items():
                try:
                    c = float(v)
                except (TypeError, ValueError):
                    continue
                if c > 0:
                    rows.append({
                        "date": idx.date() if hasattr(idx, "date") else idx,
                        "ticker": t.upper(),
                        "close_price": c,
                    })
    else:
        if "Close" in raw.columns and len(tickers) == 1:
            t = tickers[0].upper()
            for idx, v in raw["Close"].dropna().items():
                try:
                    c = float(v)
                except (TypeError, ValueError):
                    continue
                if c > 0:
                    rows.append({
                        "date": idx.date() if hasattr(idx, "date") else idx,
                        "ticker": t,
                        "close_price": c,
                    })

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out["ticker"] = out["ticker"].astype(str).str.upper()
    return out.drop_duplicates(subset=["date", "ticker"], keep="last")


def chunked(seq: list[str], n: int) -> list[list[str]]:
    return [seq[i : i + n] for i in range(0, len(seq), n)]


def backfill(
    existing: pd.DataFrame,
    *,
    only_missing: bool = True,
    start: date | None = None,
    end: date | None = None,
    chunk_size: int = 100,
) -> tuple[pd.DataFrame, dict]:
    """Return updated frame + summary dict; no I/O."""
    for c in REQUIRED_COLUMNS:
        if c not in existing.columns:
            existing[c] = None

    work = existing.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.date
    work["ticker"] = work["ticker"].astype(str).str.upper()

    data_start = work["date"].min()
    data_end = work["date"].max()
    start = start or data_start
    end = end or data_end
    if start is None or end is None:
        LOGGER.info("no rows in existing frame; nothing to backfill")
        return work, {"rows_updated": 0, "tickers": 0}

    # Decide which (ticker, date) pairs still need a close.
    close_existing = pd.to_numeric(work["close_price"], errors="coerce")
    mask_need = (work["date"] >= start) & (work["date"] <= end)
    if only_missing:
        mask_need &= close_existing.isna() | (close_existing <= 0)
    need = work.loc[mask_need]
    if need.empty:
        LOGGER.info("nothing needs backfilling (dates=%s..%s, only_missing=%s)", start, end, only_missing)
        return work, {"rows_updated": 0, "tickers": 0}

    tickers = sorted(need["ticker"].unique().tolist())
    LOGGER.info(
        "rows needing close_price: %d across %d tickers (dates %s..%s)",
        len(need), len(tickers), start, end,
    )

    # Fetch in ticker chunks to stay under Yahoo's URL-length cap.
    orig_close = pd.to_numeric(existing["close_price"], errors="coerce")
    orig_dates = pd.to_datetime(existing["date"], errors="coerce").dt.date
    orig_mask = (orig_dates >= start) & (orig_dates <= end)
    orig_filled = int((orig_mask & orig_close.notna() & (orig_close > 0)).sum())

    all_closes: list[pd.DataFrame] = []
    for batch in chunked(tickers, chunk_size):
        df = fetch_close_history(batch, start, end)
        if not df.empty:
            all_closes.append(df)
    closes_rows_fetched = 0
    if not all_closes:
        LOGGER.warning("no close_price rows returned from yfinance")
    else:
        closes = pd.concat(all_closes, ignore_index=True)
        closes = closes.drop_duplicates(subset=["date", "ticker"], keep="last")
        closes_rows_fetched = int(len(closes))

        merged = work.merge(
            closes.rename(columns={"close_price": "_close_new"}),
            on=["date", "ticker"],
            how="left",
        )
        new_num = pd.to_numeric(merged["_close_new"], errors="coerce")
        cur_num = pd.to_numeric(merged.get("close_price"), errors="coerce")
        if only_missing:
            take_new = cur_num.isna() | (cur_num <= 0)
            merged["close_price"] = np.where(take_new & new_num.notna(), new_num, cur_num)
        else:
            merged["close_price"] = new_num.combine_first(cur_num)
        merged = merged.drop(columns=["_close_new"])
        work = merged

    close_after_yf = pd.to_numeric(work["close_price"], errors="coerce")
    still_need = int(
        ((work["date"] >= start) & (work["date"] <= end) & (close_after_yf.isna() | (close_after_yf <= 0))).sum()
    )
    if still_need:
        LOGGER.info(
            "%d row(s) still missing close after yfinance; trying Polygon",
            still_need,
        )
        work, n_poly = backfill_close_prices_polygon_gaps(work, start=start, end=end)
        if n_poly:
            LOGGER.info("polygon backfill filled %d field(s)", n_poly)

    new_close = pd.to_numeric(work["close_price"], errors="coerce")
    new_mask = (work["date"] >= start) & (work["date"] <= end)
    new_filled = int((new_mask & new_close.notna() & (new_close > 0)).sum())
    rows_updated = max(0, new_filled - orig_filled)
    return work, {
        "rows_updated": rows_updated,
        "tickers": len(tickers),
        "closes_rows_fetched": closes_rows_fetched,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill close_price on etf_metrics_daily.")
    parser.add_argument("--start", default=None, help="YYYY-MM-DD lower bound (default: earliest date in file)")
    parser.add_argument("--end", default=None, help="YYYY-MM-DD upper bound (default: latest date in file)")
    parser.add_argument("--chunk", type=int, default=50, help="Ticker batch size for yfinance calls")
    parser.add_argument("--force", action="store_true", help="Overwrite existing close_price values")
    parser.add_argument("--dry-run", action="store_true", help="Do not write outputs")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if not PARQUET_PATH.exists():
        LOGGER.error("no existing etf_metrics_daily.parquet at %s; run the ingest first", PARQUET_PATH)
        sys.exit(1)

    existing = load_existing()
    if existing.empty:
        LOGGER.info("store empty; nothing to do")
        return

    start = datetime.strptime(args.start, "%Y-%m-%d").date() if args.start else None
    end = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else None

    updated, summary = backfill(
        existing,
        only_missing=not args.force,
        start=start,
        end=end,
        chunk_size=max(1, int(args.chunk)),
    )
    LOGGER.info("backfill summary: %s", summary)

    if args.dry_run:
        LOGGER.info("--dry-run: skipping writes")
        return

    updated = enforce_status_consistency(updated)
    updated, n_collapse = collapse_redundant_consecutive_rows(updated)
    if n_collapse:
        LOGGER.info("collapse_redundant dropped %d rows", n_collapse)
    updated, n_close_split = repair_close_price_split_basis_mismatch(updated)
    if n_close_split:
        LOGGER.info("repair_close_price_split_basis_mismatch: %d row(s)", n_close_split)
    validate_df(updated)
    save_outputs(updated)
    LOGGER.info(
        "wrote %s (%d rows), %s, %s",
        PARQUET_PATH, len(updated), CSV_PATH, JSON_PATH,
    )


if __name__ == "__main__":
    main()

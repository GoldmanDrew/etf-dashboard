#!/usr/bin/env python3
"""Insert Yahoo ETF + underlying adjusted closes *before* the earliest stored metrics row.

Daily ingest only appends recent trading days, so newly listed names can sit in
``etf_metrics_daily`` with a short window while Yahoo already has history from
listing. That truncated window also truncates the Chart page pair backtest.

This script, intended for CI (``continue-on-error: true``) or manual runs, fills
the gap from Yahoo's first trade date up to the day before each ticker's current
minimum stored date. Rows are ``status='missing'`` (no issuer NAV/AUM/shares)
but carry ``close_price`` + ``underlying_adj_close`` so the dashboard backtest
can run from true pair inception once merged.

Usage::

    python scripts/bootstrap_metrics_yahoo_history.py
    python scripts/bootstrap_metrics_yahoo_history.py --dry-run --max-tickers 20
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from ingest_etf_metrics import (  # noqa: E402
    JSON_PATH,
    PARQUET_PATH,
    REQUIRED_COLUMNS,
    collapse_redundant_consecutive_rows,
    enforce_status_consistency,
    fetch_close_prices_batch,
    fetch_underlying_adj_close_batch,
    load_existing,
    load_universe_tickers,
    load_universe_underlying_map,
    merge_underlying_adj_close,
    repair_close_price_split_basis_mismatch,
    save_outputs,
    upsert,
    validate_df,
)

LOGGER = logging.getLogger("bootstrap_metrics_yahoo")


def _first_trade_date(sym: str) -> date | None:
    """Best-effort first US session with price data from Yahoo."""
    try:
        import yfinance as yf
    except Exception as e:
        LOGGER.warning("yfinance unavailable (%s); cannot resolve listing for %s", e, sym)
        return None
    try:
        h = yf.Ticker(sym).history(period="max", auto_adjust=False, actions=False)
    except Exception as e:
        LOGGER.debug("history failed for %s: %s", sym, e)
        return None
    if h is None or h.empty:
        return None
    try:
        idx0 = h.index[0]
        return idx0.date() if hasattr(idx0, "date") else date.fromisoformat(str(idx0)[:10])
    except Exception:
        return None


def _build_bootstrap_frame(
    sym: str,
    und: str,
    start: date,
    end: date,
    und_close_long: pd.DataFrame,
) -> pd.DataFrame:
    """Rows for [start, end] inclusive with close + underlying_adj_close."""
    if start > end:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)
    etf_px = fetch_close_prices_batch([sym], start, end)
    if etf_px.empty:
        LOGGER.info("bootstrap: no Yahoo closes for %s %s..%s", sym, start, end)
        return pd.DataFrame(columns=REQUIRED_COLUMNS)
    etf_px = etf_px.copy()
    etf_px["date"] = pd.to_datetime(etf_px["date"], errors="coerce").dt.date
    etf_px["ticker"] = sym.upper()
    ingested = datetime.now(timezone.utc).isoformat()
    rows = []
    for _, r in etf_px.iterrows():
        rows.append({
            "date": r["date"],
            "ticker": sym.upper(),
            "nav": None,
            "aum": None,
            "shares_outstanding": None,
            "shares_traded": None,
            "close_price": float(r["close_price"]) if pd.notna(r["close_price"]) else None,
            "underlying_adj_close": None,
            "stale": False,
            "stale_age_bdays": None,
            "source_provider": "yahoo_bootstrap",
            "source_url": None,
            "ingested_at_utc": ingested,
            "status": "missing",
        })
    out = pd.DataFrame(rows)
    for c in REQUIRED_COLUMNS:
        if c not in out.columns:
            out[c] = None
    out = out[REQUIRED_COLUMNS]
    und_side = und_close_long.copy() if not und_close_long.empty else pd.DataFrame(
        columns=["date", "ticker", "underlying_adj_close"],
    )
    if not und_side.empty:
        und_side["date"] = pd.to_datetime(und_side["date"], errors="coerce").dt.date
        und_side = und_side.loc[und_side["ticker"].astype(str).str.upper() == und.upper()]
    out = merge_underlying_adj_close(
        out,
        und_side,
        {sym.upper(): und},
    )
    u = pd.to_numeric(out["underlying_adj_close"], errors="coerce")
    c = pd.to_numeric(out["close_price"], errors="coerce")
    keep = u.notna() & (u > 0) & c.notna() & (c > 0)
    out = out.loc[keep].reset_index(drop=True)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap Yahoo prices before earliest metrics row.")
    parser.add_argument("--max-tickers", type=int, default=100, help="Cap tickers processed per run (largest gaps first).")
    parser.add_argument("--min-gap-calendar-days", type=int, default=7, help="Skip if first store date is within this many days of Yahoo listing.")
    parser.add_argument("--dry-run", action="store_true", help="Compute work but do not write parquet/json/csv.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if not PARQUET_PATH.exists() and not JSON_PATH.exists():
        LOGGER.warning("No metrics store at %s — nothing to bootstrap", PARQUET_PATH)
        return

    existing = load_existing()
    if existing.empty:
        LOGGER.info("Empty metrics store; nothing to bootstrap")
        return

    und_map = load_universe_underlying_map()
    tickers = load_universe_tickers()
    existing["date"] = pd.to_datetime(existing["date"], errors="coerce").dt.date
    existing["ticker"] = existing["ticker"].astype(str).str.upper()

    gaps: list[tuple[int, str, str, date, date]] = []
    for sym in tickers:
        und = und_map.get(sym)
        if not und:
            continue
        sub = existing.loc[existing["ticker"] == sym.upper()]
        if sub.empty:
            continue
        min_d = sub["date"].min()
        list_d = _first_trade_date(sym)
        if list_d is None:
            continue
        cal_gap = (min_d - list_d).days
        if cal_gap < int(args.min_gap_calendar_days):
            continue
        end_fill = min_d - timedelta(days=1)
        if end_fill < list_d:
            continue
        gaps.append((cal_gap, sym.upper(), str(und).strip().upper(), list_d, end_fill))

    gaps.sort(key=lambda x: -x[0])
    picked = gaps[: max(1, int(args.max_tickers))]
    if not picked:
        LOGGER.info("No tickers with a Yahoo-vs-store date gap ≥ %d days", int(args.min_gap_calendar_days))
        return

    LOGGER.info("Bootstrapping %d ticker(s) with largest calendar gaps", len(picked))
    und_syms = sorted({p[2] for p in picked})
    global_start = min(p[3] for p in picked)
    global_end = max(p[4] for p in picked)
    und_raw = fetch_underlying_adj_close_batch(und_syms, global_start, global_end)
    if und_raw.empty:
        LOGGER.warning("Underlying Yahoo batch empty (%s..%s); aborting", global_start, global_end)
        return
    und_prefetch = und_raw.copy()
    und_prefetch["date"] = pd.to_datetime(und_prefetch["date"], errors="coerce").dt.date

    frames: list[pd.DataFrame] = []
    for _gap, sym, und, list_d, end_fill in picked:
        part = _build_bootstrap_frame(sym, und, list_d, end_fill, und_prefetch)
        if not part.empty:
            LOGGER.info("bootstrap %s: +%d row(s) %s..%s", sym, len(part), part["date"].min(), part["date"].max())
            frames.append(part)

    if not frames:
        LOGGER.info("No bootstrap rows produced")
        return

    incoming = pd.concat(frames, ignore_index=True)
    merged = upsert(existing, incoming)
    merged, n_collapse = collapse_redundant_consecutive_rows(merged)
    if n_collapse:
        LOGGER.info("post-bootstrap collapse removed %d redundant row(s)", n_collapse)
    merged = enforce_status_consistency(merged)
    merged, n_close_split = repair_close_price_split_basis_mismatch(merged)
    if n_close_split:
        LOGGER.info("close split repair touched %d row(s)", n_close_split)
    validate_df(merged)

    if args.dry_run:
        LOGGER.info("--dry-run: would write %d total rows (+%d incoming)", len(merged), len(incoming))
        return

    save_outputs(merged)
    LOGGER.info("Wrote metrics store after Yahoo bootstrap (+%d new row(s))", len(incoming))


if __name__ == "__main__":
    main()

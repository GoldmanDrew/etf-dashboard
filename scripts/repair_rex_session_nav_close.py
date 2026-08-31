#!/usr/bin/env python3
"""Re-scrape REX/T-REX session rows and repair NAV vs market close in metrics store.

Updates ``nav`` from issuer-published NAV (not AUM/shares implied) and ``close_price``
from issuer Closing Price for historical rows still sourced from ``rex_shares``.

Default: last 45 calendar days, dry-run unless ``--apply``.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from etf_providers import REXSharesProvider  # noqa: E402
from ingest_etf_metrics import (  # noqa: E402
    PARQUET_PATH,
    save_outputs,
    validate_df,
)

LOGGER = logging.getLogger("repair_rex_session_nav_close")


def repair_rex_rows(
    df: pd.DataFrame,
    *,
    lookback_days: int = 45,
    tickers: list[str] | None = None,
    apply: bool = False,
) -> tuple[pd.DataFrame, int]:
    if df.empty:
        return df, 0
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    max_d = out["date"].max()
    if max_d is None:
        return df, 0
    min_d = max_d - timedelta(days=max(1, lookback_days))
    mask = (
        (out["date"] >= min_d)
        & (out["source_provider"].astype(str).str.lower().str.contains("rex"))
    )
    if tickers:
        want = {str(t).strip().upper() for t in tickers}
        mask &= out["ticker"].astype(str).str.upper().isin(want)
    sub = out.loc[mask]
    if sub.empty:
        LOGGER.info("No rex_shares rows in window %s .. %s", min_d, max_d)
        return out, 0

    prov = REXSharesProvider()
    n_fixed = 0
    n_skipped_asof = 0
    for (sym, d), _ in sub.groupby(["ticker", "date"]):
        sym_s = str(sym).upper()
        d_val = d if isinstance(d, date) else pd.Timestamp(d).date()
        try:
            res = prov.fetch_for_date(sym_s, d_val)
        except Exception as exc:
            LOGGER.warning("%s %s fetch failed: %s", sym_s, d_val, exc)
            continue
        if res.status not in ("ok", "partial") or res.nav is None:
            continue
        # rexshares.com/{ticker}/ has no history: it serves one live NAV whatever
        # ``as_of`` we ask for. Writing that onto every date in the window flattened
        # 45 days of REX history to a single constant (33/34 REX funds showed one
        # unique NAV across August 2026) and back-stamped today's issuer close onto
        # older rows. Only patch the session the page itself is stamped "As Of".
        res_date = res.date if isinstance(res.date, date) else pd.Timestamp(res.date).date()
        if res_date != d_val:
            n_skipped_asof += 1
            LOGGER.debug(
                "%s %s skipped: issuer page is as-of %s, not this session", sym_s, d_val, res_date
            )
            continue
        ix = (out["ticker"].astype(str).str.upper() == sym_s) & (out["date"] == d_val)
        if not ix.any():
            continue
        old_nav = float(out.loc[ix, "nav"].iloc[0]) if pd.notna(out.loc[ix, "nav"].iloc[0]) else None
        old_close = (
            float(out.loc[ix, "close_price"].iloc[0])
            if "close_price" in out.columns and pd.notna(out.loc[ix, "close_price"].iloc[0])
            else None
        )
        out.loc[ix, "nav"] = res.nav
        if res.market_close is not None and not res.stale:
            out.loc[ix, "close_price"] = res.market_close
        if old_nav != res.nav or (res.market_close is not None and old_close != res.market_close):
            n_fixed += int(ix.sum())
            LOGGER.info(
                "%s %s nav %.4f -> %.4f close %s -> %s prem=%s",
                sym_s,
                d_val,
                old_nav or float("nan"),
                res.nav,
                old_close,
                res.market_close,
                res.issuer_prem_disc_pct,
            )

    if n_skipped_asof:
        LOGGER.info(
            "Skipped %d row(s) whose session does not match the issuer page as-of date",
            n_skipped_asof,
        )
    if apply and n_fixed:
        validate_df(out)
        save_outputs(out)
        LOGGER.info("Saved %d row patch(es) to %s", n_fixed, PARQUET_PATH)
    elif n_fixed:
        LOGGER.info("Dry-run: would patch %d row(s); re-run with --apply", n_fixed)
    return out, n_fixed


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    parser = argparse.ArgumentParser(description="Repair REX NAV vs market close in metrics parquet")
    parser.add_argument("--lookback-days", type=int, default=45)
    parser.add_argument("--tickers", type=str, default="", help="Comma-separated tickers (default: all REX in window)")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if not PARQUET_PATH.exists():
        LOGGER.error("Missing %s", PARQUET_PATH)
        return 1
    df = pd.read_parquet(PARQUET_PATH)
    tickers = [t.strip() for t in args.tickers.split(",") if t.strip()] or None
    _, n = repair_rex_rows(
        df,
        lookback_days=args.lookback_days,
        tickers=tickers,
        apply=args.apply,
    )
    return 0 if n >= 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

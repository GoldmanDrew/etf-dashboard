#!/usr/bin/env python3
"""Backfill Tradr/AXS NAV/AUM/shares from dated Filepoint CSVs.

Yahoo-bootstrap rows (and other provider_missing gaps) often have close but no
issuer NAV. Tradr keeps historical ``NSDEAXS2.MMDDYYYY.csv`` / holdings files
reachable — Wayback has zero captures for this host, so dated live URLs are the
recovery path.

Default is dry-run (JSON report). Pass ``--apply`` to merge into the metrics
store, run NAV-lagging split repair, restamp prem/disc, and ``save_outputs``.

Examples::

    python scripts/backfill_tradr_axs_dated_csvs.py --tickers NBIZ
    python scripts/backfill_tradr_axs_dated_csvs.py --tickers NBIZ --apply
    python scripts/backfill_tradr_axs_dated_csvs.py --since 2026-01-01 --apply
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
_SCRIPTS = _REPO / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from etf_providers import TradrAxsProvider  # noqa: E402
from ingest_etf_metrics import (  # noqa: E402
    REQUIRED_COLUMNS,
    enforce_status_consistency,
    load_existing,
    repair_nav_lagging_split_basis,
    save_outputs,
    stamp_metric_asof_metadata,
)
from market_calendar import nyse_sessions  # noqa: E402

LOGGER = logging.getLogger("backfill_tradr_axs_dated_csvs")


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    return date.fromisoformat(str(s)[:10])


def _candidate_rows(df: pd.DataFrame, *, tickers: set[str] | None, since: date | None) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out["ticker"] = out["ticker"].astype(str).str.upper()
    nav = pd.to_numeric(out.get("nav"), errors="coerce")
    aum = pd.to_numeric(out.get("aum"), errors="coerce")
    provider = out.get("source_provider", pd.Series(index=out.index, dtype=object)).astype(str).str.lower()
    status = out.get("premium_discount_status", pd.Series(index=out.index, dtype=object)).astype(str)
    kind = out.get("stale_kind", pd.Series(index=out.index, dtype=object)).astype(str).str.lower()
    missing_issuer = nav.isna() | (nav <= 0) | aum.isna() | (aum <= 0)
    bootstrap = provider.eq("yahoo_bootstrap") | status.eq("provider_missing")
    market_backed = (
        kind.eq("market_backed_no_issuer_nav")
        | provider.eq("market_backed")
        | status.eq("issuer_stale")
    )
    mask = missing_issuer & (bootstrap | provider.isin({"", "nan", "none"}))
    # Also refill tradr rows that somehow lost NAV, and refresh market-backed
    # carry rows when Filepoint still has the dated CSV (NBIZ Jul 15–16).
    mask = mask | (provider.eq("tradr_axs") & missing_issuer) | (
        out["ticker"].isin(tickers or set()) & market_backed
    ) if tickers else (mask | (provider.eq("tradr_axs") & missing_issuer) | market_backed)
    if tickers:
        mask &= out["ticker"].isin(tickers)
    if since is not None:
        mask &= out["date"].map(lambda d: d is not None and d >= since)
    return out.loc[mask].sort_values(["ticker", "date"])


def _tradr_universe(df: pd.DataFrame) -> set[str]:
    """Tickers that ever used tradr_axs, plus common bootstrap candidates we probe."""
    provider = df.get("source_provider", pd.Series(dtype=object)).astype(str).str.lower()
    known = set(df.loc[provider.eq("tradr_axs"), "ticker"].astype(str).str.upper())
    return known


def backfill(
    df: pd.DataFrame,
    *,
    tickers: set[str] | None = None,
    since: date | None = None,
    until: date | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out["ticker"] = out["ticker"].astype(str).str.upper()

    tradr_tickers = _tradr_universe(out)
    if tickers:
        tradr_tickers |= {t.upper() for t in tickers}

    candidates = _candidate_rows(out, tickers=tickers, since=since)
    # Also walk sessions for requested tickers even if no row yet (fill holes).
    report: dict[str, Any] = {
        "built_at": datetime.now(timezone.utc).isoformat(),
        "tickers": sorted(tickers) if tickers else sorted(tradr_tickers),
        "filled": [],
        "missing_on_filepoint": [],
        "skipped": [],
        "n_filled": 0,
    }
    if until is None:
        until = date.today()
    if since is None and not candidates.empty:
        since = min(candidates["date"])
    if since is None:
        since = date(2026, 1, 1)

    tradr = TradrAxsProvider()
    sessions = list(nyse_sessions(since, until))
    target_tickers = sorted(tickers) if tickers else sorted(tradr_tickers)
    key_index = {(r.date, r.ticker): i for i, r in out.iterrows()}

    for ds in sessions:
        # Warm ticker cache once per session.
        for ticker in target_tickers:
            if ticker not in tradr_tickers and tickers is None:
                continue
            if not tradr.supports_ticker(ticker, ds):
                continue
            # Only fill when existing row lacks issuer fields, or no row.
            ix = key_index.get((ds, ticker))
            if ix is not None:
                nav = pd.to_numeric(out.at[ix, "nav"], errors="coerce")
                aum = pd.to_numeric(out.at[ix, "aum"], errors="coerce")
                src_provider = str(out.at[ix, "source_provider"] or "").lower()
                kind = str(out.at[ix, "stale_kind"] or "").lower()
                status = str(out.at[ix, "premium_discount_status"] or "")
                needs_refresh = (
                    kind == "market_backed_no_issuer_nav"
                    or src_provider == "market_backed"
                    or status == "issuer_stale"
                )
                if (
                    pd.notna(nav)
                    and float(nav) > 0
                    and pd.notna(aum)
                    and float(aum) > 0
                    and not needs_refresh
                ):
                    continue
            result = tradr.fetch_for_date(ticker, ds)
            if result.status == "missing" or result.nav is None or float(result.nav) <= 0:
                report["missing_on_filepoint"].append({"date": ds.isoformat(), "ticker": ticker})
                continue
            payload = {
                "nav": float(result.nav) if result.nav is not None else None,
                "aum": float(result.aum) if result.aum is not None else None,
                "shares_outstanding": float(result.shares_outstanding) if result.shares_outstanding is not None else None,
                "source_provider": "tradr_axs",
                "source_url": result.source_url,
                "status": result.status,
                "stale": False,
                "stale_age_bdays": None,
                "stale_kind": None,
                "issuer_asof_date": ds.isoformat(),
            }
            if ix is None:
                row = {c: None for c in REQUIRED_COLUMNS if c in out.columns or c in REQUIRED_COLUMNS}
                row.update({"date": ds, "ticker": ticker, **payload})
                # Keep required schema columns present.
                for c in out.columns:
                    row.setdefault(c, None)
                out = pd.concat([out, pd.DataFrame([row])], ignore_index=True)
                key_index[(ds, ticker)] = out.index[-1]
            else:
                for k, v in payload.items():
                    if k in out.columns:
                        out.at[ix, k] = v
            report["filled"].append(
                {
                    "date": ds.isoformat(),
                    "ticker": ticker,
                    "nav": payload["nav"],
                    "aum": payload["aum"],
                    "shares_outstanding": payload["shares_outstanding"],
                }
            )

    report["n_filled"] = len(report["filled"])
    out = enforce_status_consistency(out)
    out, n_nav = repair_nav_lagging_split_basis(out)
    report["n_nav_lag_repairs"] = int(n_nav)
    out = stamp_metric_asof_metadata(out)
    return out, report


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tickers", default=None, help="Comma-separated ETF tickers (default: all tradr_axs)")
    parser.add_argument("--since", default=None, help="YYYY-MM-DD")
    parser.add_argument("--until", default=None, help="YYYY-MM-DD")
    parser.add_argument("--report", type=Path, default=_REPO / "data" / "runs" / "_tradr_axs_backfill_report.json")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    tickers = None
    if args.tickers:
        tickers = {t.strip().upper() for t in args.tickers.split(",") if t.strip()}
    since = _parse_date(args.since)
    until = _parse_date(args.until)

    df = load_existing()
    if df.empty:
        LOGGER.error("No metrics store found")
        return 1

    updated, report = backfill(df, tickers=tickers, since=since, until=until)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2), encoding="utf-8")
    LOGGER.info(
        "filled=%s nav_lag_repairs=%s report=%s",
        report["n_filled"],
        report.get("n_nav_lag_repairs"),
        args.report,
    )
    if not args.apply:
        LOGGER.info("Dry-run only; pass --apply to write metrics store")
        return 0

    save_outputs(updated)
    LOGGER.info("Applied backfill to metrics store")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

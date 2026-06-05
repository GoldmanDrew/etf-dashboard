#!/usr/bin/env python3
"""Backfill YBTY/YBST NAV/price history into etf_metrics_daily.

FoF tickers are dashboard-only (not in ls-algo screener history). This script
seeds Yahoo adjusted closes + optional Granite spot NAV so FoF realized pair
metrics can run.

Usage::

    python scripts/backfill_fof_metrics.py
    python scripts/backfill_fof_metrics.py --dry-run
"""
from __future__ import annotations

import argparse
import logging
import math
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from ingest_etf_metrics import (  # noqa: E402
    CSV_PATH,
    JSON_PATH,
    PARQUET_PATH,
    REQUIRED_COLUMNS,
    collapse_redundant_consecutive_rows,
    enforce_status_consistency,
    fetch_close_prices_batch,
    fetch_etf_adj_close_batch,
    load_existing,
    save_outputs,
    upsert,
    validate_df,
)
from yieldboost_fof_constants import YIELDBOOST_FOF_SYMBOLS
from yieldboost_fof_basket_series import build_synthetic_fof_nav_series
from yieldboost_fof_holdings import build_fof_holdings_history, load_holdings_frame

LOGGER = logging.getLogger("backfill_fof_metrics")
FOF_LISTING_FALLBACK = date(2025, 1, 2)
MIN_ROWS_TARGET = 20


def _first_trade_date(sym: str) -> date | None:
    try:
        import yfinance as yf
    except Exception as exc:
        LOGGER.warning("yfinance unavailable: %s", exc)
        return FOF_LISTING_FALLBACK
    try:
        h = yf.Ticker(sym).history(period="max", auto_adjust=False, actions=False)
    except Exception as exc:
        LOGGER.debug("history failed for %s: %s", sym, exc)
        return FOF_LISTING_FALLBACK
    if h is None or h.empty:
        return FOF_LISTING_FALLBACK
    try:
        idx0 = h.index[0]
        return idx0.date() if hasattr(idx0, "date") else date.fromisoformat(str(idx0)[:10])
    except Exception:
        return FOF_LISTING_FALLBACK


def _granite_nav_row(sym: str, as_of: date) -> dict | None:
    try:
        from etf_providers import GraniteSharesProvider

        prov = GraniteSharesProvider()
        res = prov.fetch_for_date(sym, as_of)
        if res.status not in {"ok", "partial"} or res.nav is None:
            return None
        nav = float(res.nav)
        if not (nav > 0):
            return None
        shares = float(res.shares_outstanding) if res.shares_outstanding else None
        aum = float(res.aum) if res.aum else (nav * shares if shares else None)
        return {
            "nav": nav,
            "aum": aum,
            "shares_outstanding": shares,
            "source_provider": res.source_provider or "granite_shares",
            "source_url": res.source_url,
            "status": res.status,
            "valuation_date": res.date,
        }
    except Exception as exc:
        LOGGER.debug("Granite fetch failed for %s: %s", sym, exc)
        return None


def build_fof_metrics_frame(
    sym: str,
    start: date,
    end: date,
) -> pd.DataFrame:
    """Yahoo close/adj rows for one FoF symbol; overlay Granite NAV when available."""
    sym_u = sym.upper()
    etf_px = fetch_close_prices_batch([sym_u], start, end)
    adj_px = fetch_etf_adj_close_batch([sym_u], start, end)
    if etf_px.empty and adj_px.empty:
        LOGGER.warning("No Yahoo prices for %s %s..%s — trying Granite spot NAV", sym_u, start, end)
        gran = _granite_nav_row(sym_u, end)
        if gran:
            ingested = datetime.now(timezone.utc).isoformat()
            rows = [{
                "date": end,
                "ticker": sym_u,
                "nav": gran["nav"],
                "aum": gran["aum"],
                "shares_outstanding": gran["shares_outstanding"],
                "shares_traded": None,
                "close_price": gran["nav"],
                "etf_adj_close": gran["nav"],
                "underlying_adj_close": None,
                "stale": False,
                "stale_age_bdays": None,
                "stale_kind": None,
                "source_provider": gran["source_provider"],
                "source_url": gran["source_url"],
                "ingested_at_utc": ingested,
                "status": "ok",
            }]
            out = pd.DataFrame(rows)
            for c in REQUIRED_COLUMNS:
                if c not in out.columns:
                    out[c] = None
            return out.reset_index(drop=True)
        return pd.DataFrame(columns=REQUIRED_COLUMNS)

    if not etf_px.empty:
        etf_px = etf_px.copy()
        etf_px["date"] = pd.to_datetime(etf_px["date"], errors="coerce").dt.date
    if not adj_px.empty:
        adj_px = adj_px.copy()
        adj_px["date"] = pd.to_datetime(adj_px["date"], errors="coerce").dt.date
        adj_map = dict(zip(adj_px["date"], adj_px["etf_adj_close"]))
    else:
        adj_map = {}

    dates = sorted(set(etf_px["date"].tolist()) if not etf_px.empty else set(adj_map.keys()))
    ingested = datetime.now(timezone.utc).isoformat()
    rows = []
    for d in dates:
        close = None
        if not etf_px.empty:
            hit = etf_px.loc[etf_px["date"] == d, "close_price"]
            if not hit.empty:
                close = float(hit.iloc[0])
        adj = adj_map.get(d)
        if adj is None and close is not None:
            adj = close
        if close is None and adj is not None:
            close = float(adj)

        gran = _granite_nav_row(sym_u, d)
        nav = gran["nav"] if gran else None
        status = gran["status"] if gran else "missing"
        provider = gran["source_provider"] if gran else "yahoo_fof_bootstrap"
        source_url = gran["source_url"] if gran else None

        # Reject Granite spot rows that disagree with Yahoo close scale (prevents chart spikes).
        if nav is not None and close is not None and close > 0:
            ratio = float(nav) / float(close)
            if ratio < 0.5 or ratio > 2.0:
                LOGGER.warning(
                    "%s %s: Granite NAV %.4f vs Yahoo close %.4f — skip issuer overlay",
                    sym_u,
                    d,
                    nav,
                    close,
                )
                nav = None
                status = "missing"
                provider = "yahoo_fof_bootstrap"

        rows.append({
            "date": d,
            "ticker": sym_u,
            "nav": nav,
            "aum": gran["aum"] if gran else None,
            "shares_outstanding": gran["shares_outstanding"] if gran else None,
            "shares_traded": None,
            "close_price": close,
            "etf_adj_close": adj,
            "underlying_adj_close": None,
            "stale": False,
            "stale_age_bdays": None,
            "stale_kind": None,
            "source_provider": provider,
            "source_url": source_url,
            "ingested_at_utc": ingested,
            "status": status if nav else "missing",
        })

    out = pd.DataFrame(rows)
    for c in REQUIRED_COLUMNS:
        if c not in out.columns:
            out[c] = None
    keep = pd.to_numeric(out["close_price"], errors="coerce").notna() | pd.to_numeric(out["nav"], errors="coerce").notna()
    return out.loc[keep].reset_index(drop=True)


def build_child_synthetic_fof_metrics_frame(
    sym_u: str,
    history_snaps: list[dict],
    metrics: pd.DataFrame,
) -> pd.DataFrame:
    """When Yahoo/Granite FoF history is thin, infer NAV from weighted child ETF adj closes."""
    synth = build_synthetic_fof_nav_series(history_snaps, metrics)
    if synth.empty or len(synth) < 5:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)
    ingested = datetime.now(timezone.utc).isoformat()
    rows = []
    for d, nav in synth.items():
        try:
            nav_f = float(nav)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(nav_f) or nav_f <= 0:
            continue
        rows.append({
            "date": pd.to_datetime(str(d)[:10]).date(),
            "ticker": sym_u,
            "nav": round(nav_f, 6),
            "aum": None,
            "shares_outstanding": None,
            "shares_traded": None,
            "close_price": round(nav_f, 6),
            "etf_adj_close": round(nav_f, 6),
            "underlying_adj_close": None,
            "stale": False,
            "stale_age_bdays": None,
            "stale_kind": None,
            "source_provider": "fof_child_synthetic",
            "source_url": None,
            "ingested_at_utc": ingested,
            "status": "partial",
        })
    if not rows:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)
    out = pd.DataFrame(rows)
    for c in REQUIRED_COLUMNS:
        if c not in out.columns:
            out[c] = None
    return out.reset_index(drop=True)


def _purge_fof_scale_anomalies(df: pd.DataFrame, sym_u: str) -> pd.DataFrame:
    """Drop FoF rows with impossible one-day level jumps (mixed synthetic + issuer spot)."""
    if df is None or df.empty:
        return df
    sub = df[df["ticker"] == sym_u].sort_values("date").copy()
    if len(sub) < 2:
        return df
    keep_dates: set[date] = set()
    prev_nav: float | None = None
    for _, row in sub.iterrows():
        nav = pd.to_numeric(row.get("nav"), errors="coerce")
        if nav is None or not math.isfinite(float(nav)) or float(nav) <= 0:
            continue
        nav_f = float(nav)
        if prev_nav is not None:
            ratio = nav_f / prev_nav
            if ratio > 1.0 + _MAX_DAILY_RETURN or ratio < 1.0 - _MAX_DAILY_RETURN:
                LOGGER.warning(
                    "%s %s: drop anomalous FoF NAV %.4f (prev %.4f, ratio %.3f)",
                    sym_u,
                    row["date"],
                    nav_f,
                    prev_nav,
                    ratio,
                )
                continue
        keep_dates.add(row["date"])
        prev_nav = nav_f
    if not keep_dates:
        return df
    drop_mask = (df["ticker"] == sym_u) & (~df["date"].isin(keep_dates))
    return df.loc[~drop_mask].reset_index(drop=True)


_MAX_DAILY_RETURN = 0.35


def merge_fof_metrics_into_store(
    existing: pd.DataFrame,
    *,
    end: date | None = None,
    min_rows: int = MIN_ROWS_TARGET,
    refresh_synthetic: bool = True,
) -> tuple[pd.DataFrame, dict[str, int], bool]:
    """Upsert FoF history; return merged frame, per-symbol row deltas, and whether store changed."""
    end = end or date.today()
    if existing is None or existing.empty:
        base = pd.DataFrame(columns=REQUIRED_COLUMNS)
    else:
        base = existing.copy()
        base["date"] = pd.to_datetime(base["date"], errors="coerce").dt.date
        base["ticker"] = base["ticker"].astype(str).str.upper()

    added: dict[str, int] = {}
    changed = False
    merged = base
    holdings_df = load_holdings_frame()
    history_by_sym = build_fof_holdings_history(holdings_df)
    for sym in YIELDBOOST_FOF_SYMBOLS:
        sym_u = sym.upper()
        sub = merged[merged["ticker"] == sym_u] if not merged.empty else pd.DataFrame()
        existing_n = len(sub)
        snaps = history_by_sym.get(sym_u) or []

        if refresh_synthetic and snaps:
            synth = build_child_synthetic_fof_metrics_frame(
                sym_u,
                snaps,
                merged if not merged.empty else metrics_from_store(existing),
            )
            if not synth.empty:
                if not sub.empty:
                    merged = merged[
                        ~((merged["ticker"] == sym_u) & (merged["source_provider"] == "fof_child_synthetic"))
                    ].reset_index(drop=True)
                merged = upsert(merged, synth)
                merged = _purge_fof_scale_anomalies(merged, sym_u)
                added[sym_u] = len(merged[merged["ticker"] == sym_u]) - existing_n
                changed = True
                LOGGER.info(
                    "%s: refreshed child-synthetic NAV → %d row(s) (%+d)",
                    sym_u,
                    len(merged[merged["ticker"] == sym_u]),
                    added[sym_u],
                )
                continue

        if existing_n >= min_rows:
            merged = _purge_fof_scale_anomalies(merged, sym_u)
            LOGGER.info("%s already has %d rows — skip", sym_u, existing_n)
            added[sym_u] = 0
            continue

        listing = _first_trade_date(sym_u) or FOF_LISTING_FALLBACK
        start = listing
        if not sub.empty:
            min_d = sub["date"].min()
            if min_d and min_d > start:
                start = min_d

        frame = build_fof_metrics_frame(sym_u, start, end)
        if len(frame) < min_rows and snaps:
            synth = build_child_synthetic_fof_metrics_frame(sym_u, snaps, merged if not merged.empty else metrics_from_store(existing))
            if not synth.empty:
                frame = upsert(frame, synth) if not frame.empty else synth
                LOGGER.info("%s: child-synthetic supplement → %d row(s)", sym_u, len(frame))

        if frame.empty:
            LOGGER.warning("%s: no bootstrap rows built", sym_u)
            added[sym_u] = 0
            continue

        before = len(merged)
        merged = upsert(merged, frame)
        merged = _purge_fof_scale_anomalies(merged, sym_u)
        added[sym_u] = len(merged) - before
        if added[sym_u] != 0:
            changed = True
        LOGGER.info("%s: upserted %d row(s), total now %d", sym_u, added[sym_u], len(merged[merged["ticker"] == sym_u]))

    return merged, added, changed


def metrics_from_store(existing: pd.DataFrame) -> pd.DataFrame:
    if existing is None or existing.empty:
        return pd.DataFrame()
    return existing.copy()


def bootstrap_fof_nav_history(
    existing: pd.DataFrame | None = None,
    *,
    save: bool = True,
) -> pd.DataFrame:
    """Called from ingest/build pipelines to ensure FoF metrics exist."""
    existing = existing if existing is not None else load_existing()
    merged, added, changed = merge_fof_metrics_into_store(existing)
    if save and changed:
        merged, _ = collapse_redundant_consecutive_rows(merged)
        merged = enforce_status_consistency(merged)
        validate_df(merged)
        save_outputs(merged)
    return merged


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill YBTY/YBST metrics history.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--min-rows", type=int, default=MIN_ROWS_TARGET)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    existing = load_existing()
    merged, added, changed = merge_fof_metrics_into_store(existing, min_rows=args.min_rows)
    LOGGER.info("Added rows: %s", added)

    if args.dry_run:
        for sym in YIELDBOOST_FOF_SYMBOLS:
            n = len(merged[merged["ticker"] == sym.upper()]) if not merged.empty else 0
            LOGGER.info("Would have %d rows for %s", n, sym)
        return

    if not changed:
        if not merged.empty and all(
            len(merged[merged["ticker"] == s.upper()]) >= args.min_rows
            for s in YIELDBOOST_FOF_SYMBOLS
        ):
            LOGGER.info("FoF metrics already satisfied — no write")
        else:
            LOGGER.warning("FoF bootstrap added no rows — check Yahoo/Granite connectivity")
        return

    merged, _ = collapse_redundant_consecutive_rows(merged)
    merged = enforce_status_consistency(merged)
    validate_df(merged)
    save_outputs(merged)
    LOGGER.info("Saved %s (%d total rows)", CSV_PATH, len(merged))


if __name__ == "__main__":
    main()

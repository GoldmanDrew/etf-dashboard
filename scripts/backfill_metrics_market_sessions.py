#!/usr/bin/env python3
"""Backfill missing NYSE sessions with market-backed Decay-usable rows.

Faster than full issuer ingest when the goal is joint close + underlying through
a recent session (e.g. 2026-07-15 / 2026-07-16).

Usage::

    python scripts/backfill_metrics_market_sessions.py --sessions 2026-07-15,2026-07-16 --apply
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from ingest_etf_metrics import (  # noqa: E402
    PARQUET_PATH,
    STALE_KIND_MARKET_BACKED,
    fetch_close_prices_batch,
    fetch_underlying_adj_close_batch,
    load_existing,
    load_universe_tickers,
    load_universe_underlying_map,
    promote_carry_forward_rows_with_market,
    save_outputs,
    stamp_metric_asof_metadata,
    upsert,
    validate_df,
)
from market_calendar import is_nyse_session, nyse_sessions  # noqa: E402

LOGGER = logging.getLogger("backfill_metrics_market_sessions")


def _parse_sessions(raw: str | None, start: str | None, end: str | None) -> list[date]:
    if raw:
        out: list[date] = []
        for part in raw.split(","):
            part = part.strip()
            if not part:
                continue
            d = date.fromisoformat(part)
            if is_nyse_session(d):
                out.append(d)
            else:
                LOGGER.warning("skip non-NYSE session %s", d)
        return sorted(set(out))
    if start and end:
        return nyse_sessions(date.fromisoformat(start), date.fromisoformat(end))
    raise SystemExit("pass --sessions YYYY-MM-DD,YYYY-MM-DD or --start/--end")


def build_market_backed_rows(
    sessions: list[date],
    *,
    existing: pd.DataFrame,
    tickers: list[str],
    und_map: dict[str, str],
) -> pd.DataFrame:
    if not sessions:
        return pd.DataFrame()
    start = min(sessions)
    end = max(sessions) + timedelta(days=1)  # yfinance end exclusive-ish
    close_df = fetch_close_prices_batch(tickers, start, end)
    und_syms = sorted({str(v).strip().upper() for v in und_map.values() if v and str(v).strip()})
    und_df = fetch_underlying_adj_close_batch(und_syms, start, end)
    LOGGER.info(
        "fetched close rows=%d und rows=%d for %s..%s",
        len(close_df),
        len(und_df),
        start,
        max(sessions),
    )

    # Last known issuer NAV per ticker (optional carry for Stats display).
    last_nav: dict[str, dict] = {}
    if not existing.empty:
        hist = existing.copy()
        hist["ticker"] = hist["ticker"].astype(str).str.upper()
        hist["date"] = pd.to_datetime(hist["date"], errors="coerce").dt.date
        hist = hist.sort_values("date")
        for sym, g in hist.groupby("ticker"):
            ok = g[pd.to_numeric(g.get("nav"), errors="coerce").gt(0)]
            if ok.empty:
                continue
            row = ok.iloc[-1]
            last_nav[str(sym)] = {
                "nav": float(row["nav"]) if pd.notna(row.get("nav")) else None,
                "aum": float(row["aum"]) if pd.notna(row.get("aum")) else None,
                "shares_outstanding": (
                    float(row["shares_outstanding"])
                    if pd.notna(row.get("shares_outstanding"))
                    else None
                ),
                "from": str(row["date"]),
            }

    close_df = close_df.copy()
    if not close_df.empty:
        close_df["date"] = pd.to_datetime(close_df["date"], errors="coerce").dt.date
        close_df["ticker"] = close_df["ticker"].astype(str).str.upper()
    und_by: dict[tuple[date, str], float] = {}
    if not und_df.empty:
        u = und_df.copy()
        u["date"] = pd.to_datetime(u["date"], errors="coerce").dt.date
        u["ticker"] = u["ticker"].astype(str).str.upper()
        for _, r in u.iterrows():
            try:
                px = float(r["underlying_adj_close"])
            except (TypeError, ValueError):
                continue
            if px > 0 and r["date"] in sessions:
                und_by[(r["date"], r["ticker"])] = px

    session_set = set(sessions)
    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    rows: list[dict] = []
    for _, r in close_df.iterrows():
        d = r["date"]
        if d not in session_set:
            continue
        sym = str(r["ticker"]).upper()
        try:
            close = float(r["close_price"])
        except (TypeError, ValueError):
            continue
        if not (close > 0):
            continue
        und_sym = str(und_map.get(sym) or "").strip().upper()
        und_px = und_by.get((d, und_sym)) if und_sym else None
        if und_px is None or not (und_px > 0):
            continue
        prior = last_nav.get(sym) or {}
        frm = prior.get("from") or d.isoformat()
        vol = r.get("shares_traded")
        try:
            vol_f = float(vol) if vol is not None and pd.notna(vol) else None
        except (TypeError, ValueError):
            vol_f = None
        rows.append(
            {
                "date": d,
                "ticker": sym,
                "nav": prior.get("nav"),
                "aum": prior.get("aum"),
                "shares_outstanding": prior.get("shares_outstanding"),
                "shares_traded": vol_f,
                "close_price": close,
                "etf_adj_close": close,
                "underlying_adj_close": und_px,
                "stale": True,
                "stale_age_bdays": None,
                "stale_kind": STALE_KIND_MARKET_BACKED,
                "source_provider": "market_backed",
                "source_url": (
                    f"market_backed://{sym}?from={frm}"
                    f"#session={d.isoformat()}#repaired=market_backed"
                ),
                "ingested_at_utc": now,
                "status": "partial",
                "issuer_asof_date": frm if isinstance(frm, str) else d.isoformat(),
                "market_asof_date": d.isoformat(),
                "premium_discount_eligible": False,
                "premium_discount_status": "issuer_stale",
            }
        )
    out = pd.DataFrame(rows)
    LOGGER.info("built %d market-backed joint rows across %d session(s)", len(out), len(sessions))
    return out


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sessions", default=None, help="Comma-separated YYYY-MM-DD NYSE sessions")
    parser.add_argument("--start", default=None)
    parser.add_argument("--end", default=None)
    parser.add_argument(
        "--missing-only",
        action="store_true",
        help="Only fetch tickers still missing joint rows on the target sessions",
    )
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    sessions = _parse_sessions(args.sessions, args.start, args.end)
    if not sessions:
        LOGGER.error("no sessions to backfill")
        return 2
    LOGGER.info("sessions: %s", ", ".join(d.isoformat() for d in sessions))

    tickers = load_universe_tickers()
    und_map = load_universe_underlying_map()
    existing = load_existing(PARQUET_PATH)
    if args.missing_only and not existing.empty:
        ex = existing.copy()
        ex["date"] = pd.to_datetime(ex["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        ex["ticker"] = ex["ticker"].astype(str).str.upper()
        uni = set(tickers)
        need: set[str] = set()
        for d in sessions:
            ds = d.isoformat()
            have = set(
                ex.loc[
                    (ex["date"] == ds)
                    & (ex["ticker"].isin(uni))
                    & pd.to_numeric(ex.get("close_price"), errors="coerce").gt(0)
                    & pd.to_numeric(ex.get("underlying_adj_close"), errors="coerce").gt(0),
                    "ticker",
                ]
            )
            need |= uni - have
        tickers = sorted(need)
        LOGGER.info("missing-only: %d ticker(s) still need joint rows", len(tickers))
        if not tickers:
            LOGGER.info("nothing missing")
            return 0
    incoming = build_market_backed_rows(
        sessions, existing=existing, tickers=tickers, und_map=und_map
    )
    if incoming.empty:
        LOGGER.error("no market-backed rows produced (Yahoo empty?)")
        return 1

    merged = upsert(existing, incoming)
    merged, n_prom = promote_carry_forward_rows_with_market(merged)
    if n_prom:
        LOGGER.info("also promoted %d legacy CF rows with market", n_prom)
    merged = stamp_metric_asof_metadata(merged)

    # Coverage summary for target sessions
    work = merged.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    work["ticker"] = work["ticker"].astype(str).str.upper()
    uni = set(tickers)
    for d in sessions:
        ds = d.isoformat()
        sub = work[(work["date"] == ds) & (work["ticker"].isin(uni))]
        joint = sub[
            pd.to_numeric(sub["close_price"], errors="coerce").gt(0)
            & pd.to_numeric(sub["underlying_adj_close"], errors="coerce").gt(0)
            & ~sub["source_provider"].astype(str).str.lower().eq("carry_forward")
            & ~sub["stale_kind"].astype(str).str.lower().eq("carry_forward")
        ]
        LOGGER.info(
            "%s: universe rows=%d joint_usable=%d / %d",
            ds,
            len(sub),
            joint["ticker"].nunique(),
            len(uni),
        )

    if args.apply:
        validate_df(merged)
        save_outputs(merged)
        LOGGER.info("saved %s (%d rows)", PARQUET_PATH, len(merged))
    else:
        LOGGER.info("dry-run only; pass --apply to write")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

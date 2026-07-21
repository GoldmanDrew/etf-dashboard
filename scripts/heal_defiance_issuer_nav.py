#!/usr/bin/env python3
"""Lightweight heal: overwrite latest market-backed Defiance tails with issuer NAV.

Fetches Defiance Fund Details for misrouted tickers and upserts only those rows
into ``etf_metrics_daily`` — avoids a full-universe Yahoo volume backfill.

Usage::

    python scripts/heal_defiance_issuer_nav.py --dry-run
    python scripts/heal_defiance_issuer_nav.py --apply
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from audit_defiance_issuer_routing import build_report  # noqa: E402
from etf_providers import DefianceProvider  # noqa: E402
from ingest_etf_metrics import (  # noqa: E402
    CSV_PATH,
    PARQUET_PATH,
    fetch_close_prices_batch,
    fetch_etf_adj_close_batch,
    fetch_underlying_adj_close_batch,
    load_existing,
    load_universe_underlying_map,
    merge_close_prices,
    merge_etf_adj_close,
    merge_underlying_adj_close,
    resolve_ingest_end_date,
    save_outputs,
    stamp_metric_asof_metadata,
    upsert,
    validate_df,
)
from market_calendar import is_nyse_session  # noqa: E402

LOGGER = logging.getLogger("heal_defiance_issuer_nav")


def candidate_tickers() -> list[str]:
    report = build_report(probe_catalog=True)
    mis = [r["ticker"] for r in report.get("misroutes") or []]
    if mis:
        return sorted(set(mis))
    path = PARQUET_PATH if PARQUET_PATH.exists() else CSV_PATH
    if not path.exists():
        return sorted(DefianceProvider.KNOWN_TICKERS)
    df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)
    df["ticker"] = df["ticker"].astype(str).str.upper()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    idx = df.groupby("ticker")["date"].idxmax()
    latest = df.loc[idx]
    out = []
    for _, row in latest.iterrows():
        t = str(row["ticker"])
        if t not in DefianceProvider.KNOWN_TICKERS:
            continue
        prov = str(row.get("source_provider") or "").lower()
        kind = str(row.get("stale_kind") or "").lower()
        if prov != "defiance" or kind == "market_backed_no_issuer_nav":
            out.append(t)
    return sorted(set(out))


def _provider_rows_to_frame(results: list, *, as_of: date) -> pd.DataFrame:
    rows = []
    now = datetime.now(timezone.utc).isoformat()
    for r in results:
        if r.status not in ("ok", "partial") or r.nav is None:
            continue
        row_date = r.date if isinstance(r.date, date) else as_of
        if not is_nyse_session(row_date):
            continue
        rows.append(
            {
                "date": row_date,
                "ticker": str(r.ticker).upper(),
                "nav": r.nav,
                "aum": r.aum,
                "shares_outstanding": r.shares_outstanding,
                "source_provider": "defiance",
                "source_url": r.source_url,
                "ingested_at_utc": now,
                "status": r.status,
                "stale": bool(r.stale),
                "stale_age_bdays": r.stale_age_bdays,
                "stale_kind": r.stale_kind,
                "close_price": r.market_close,
                "issuer_asof_date": row_date.isoformat(),
                "market_asof_date": row_date.isoformat() if r.market_close else None,
            }
        )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def apply_heal(tickers: list[str], *, as_of: date) -> dict:
    provider = DefianceProvider()
    results = []
    for t in tickers:
        try:
            results.append(provider.fetch_for_date(t, as_of))
        except Exception as exc:
            LOGGER.warning("fetch %s failed: %s", t, exc)
    incoming = _provider_rows_to_frame(results, as_of=as_of)
    if incoming.empty:
        return {"fetched_ok": 0, "upserted": 0}

    # Market overlays for the healed session(s) only.
    dates = sorted({d for d in incoming["date"].tolist() if d})
    start = min(dates)
    end = max(dates)
    tick_list = sorted(incoming["ticker"].unique().tolist())
    close_df = fetch_close_prices_batch(tick_list, start=start, end=end)
    if not close_df.empty:
        incoming = merge_close_prices(incoming, close_df)
    etf_adj_df = fetch_etf_adj_close_batch(tick_list, start=start, end=end)
    if not etf_adj_df.empty:
        incoming = merge_etf_adj_close(incoming, etf_adj_df)
    underlying_map = load_universe_underlying_map()
    und_syms = sorted(
        {
            str(underlying_map.get(t) or "").strip().upper()
            for t in tick_list
            if underlying_map.get(t)
        }
    )
    if und_syms:
        und_df = fetch_underlying_adj_close_batch(und_syms, start, end)
        if not und_df.empty:
            incoming = merge_underlying_adj_close(incoming, und_df, underlying_map)

    # Fresh issuer row: clear market_backed markers.
    incoming["stale_kind"] = incoming["stale_kind"]
    incoming.loc[incoming["stale"] != True, "stale_kind"] = None  # noqa: E712
    incoming["source_provider"] = "defiance"
    close_ok = pd.to_numeric(incoming.get("close_price"), errors="coerce").gt(0)
    incoming.loc[close_ok, "market_asof_date"] = incoming.loc[close_ok, "date"].astype(str)
    incoming["issuer_asof_date"] = incoming["date"].astype(str)
    aligned = incoming["issuer_asof_date"].astype(str).eq(incoming["market_asof_date"].astype(str))
    nav_ok = pd.to_numeric(incoming["nav"], errors="coerce").gt(0)
    incoming["premium_discount_eligible"] = nav_ok & close_ok & aligned & (incoming["stale"] != True)  # noqa: E712
    incoming["premium_discount_status"] = incoming["premium_discount_eligible"].map(
        lambda ok: "valid" if ok else "issuer_stale"
    )

    existing = load_existing()
    # Guardrail: a thin heal that advances panel max alone causes flow session-extend
    # to fabricate a sparse "global latest" and trip freshness gates. Warn loudly.
    if not existing.empty and "date" in existing.columns:
        ex = existing.copy()
        ex["date"] = pd.to_datetime(ex["date"], errors="coerce")
        by = ex.groupby(ex["date"].dt.normalize())["ticker"].nunique()
        if not by.empty:
            dense_n = int(by.max())
            heal_dates = pd.to_datetime(incoming["date"], errors="coerce")
            heal_max = heal_dates.max()
            if pd.notna(heal_max):
                on_heal = int(by.get(heal_max.normalize(), 0)) + int(incoming["ticker"].nunique())
                if on_heal < max(20, int(0.2 * dense_n)):
                    LOGGER.warning(
                        "Heal writes %s with ~%d tickers vs densest session %d — "
                        "flow builder will prefer the denser session for extend; "
                        "run a full metrics ingest for %s when possible",
                        heal_max.date(),
                        on_heal,
                        dense_n,
                        heal_max.date(),
                    )

    merged = upsert(existing, incoming)
    merged = stamp_metric_asof_metadata(merged)
    validate_df(merged)
    save_outputs(merged)
    return {"fetched_ok": int(len(incoming)), "upserted": int(len(incoming))}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="list candidates only")
    parser.add_argument("--apply", action="store_true", help="fetch Defiance + upsert metrics")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD probe as_of (default: ingest end)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if not args.dry_run and not args.apply:
        parser.error("pass --dry-run or --apply")

    tickers = candidate_tickers()
    as_of = date.fromisoformat(args.end_date) if args.end_date else resolve_ingest_end_date()
    LOGGER.info("Candidates=%d as_of=%s", len(tickers), as_of)
    if not tickers:
        LOGGER.info("Nothing to heal")
        return 0
    print(",".join(tickers))
    if args.dry_run:
        return 0

    stats = apply_heal(tickers, as_of=as_of)
    LOGGER.info("Heal complete: %s", stats)

    report = build_report(probe_catalog=False)
    still = [r["ticker"] for r in report.get("misroutes") or [] if r["ticker"] in set(tickers)]
    LOGGER.info(
        "Post-heal: still_misrouted=%d/%d sample=%s",
        len(still),
        len(tickers),
        ",".join(still[:15]),
    )
    report_path = ROOT / "data" / "defiance_issuer_routing_report.json"
    import json

    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    LOGGER.info("Wrote %s", report_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""One-time migration: align legacy ``etf_metrics_daily`` rows with valuation-session dates.

Before the ingest fix, REX/YieldMax/Granite (and similar) stamped issuer NAV on the
ingest calendar day while ``source_url`` carried the true ``#as_of`` / ``#nav_date``.
That paired Yahoo ``close_price`` one session off. This script:

1. Detects misalignment via ``source_url`` fragments vs row ``date``.
2. Merges issuer fields onto the correct ``(date, ticker)`` row or relabels in place.
3. Clears legacy ``carry_forward`` rows that still have ``close_price`` set.
4. Re-fetches Yahoo closes / underlyings for affected windows and runs the same
   post-process chain as ``ingest_etf_metrics.main()`` (minus holdings).

Default is **dry-run**: applies URL/carry-forward logic in memory and can write ``--report`` only — **no Yahoo calls, postprocess, or disk writes**. Pass ``--apply`` to refresh closes, run the ingest postprocess chain, and ``save_outputs``.

Examples::

    python scripts/migrate_etf_metrics_valuation_dates.py
    python scripts/migrate_etf_metrics_valuation_dates.py --tickers EOSU,NVDY --since 2024-01-01
    python scripts/migrate_etf_metrics_valuation_dates.py --apply --report /tmp/mig.json
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import re
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from ingest_etf_metrics import (  # noqa: E402
    CSV_PATH,
    JSON_PATH,
    PARQUET_PATH,
    backfill_underlying_adj_close_gaps,
    collapse_redundant_consecutive_rows,
    enforce_status_consistency,
    fetch_close_prices_batch,
    fetch_underlying_adj_close_batch,
    load_existing,
    load_universe_tickers,
    load_universe_underlying_map,
    merge_close_prices,
    merge_underlying_adj_close,
    repair_close_price_split_basis_mismatch,
    repair_shares_vs_aum_nav,
    save_outputs,
    validate_df,
)

LOGGER = logging.getLogger("migrate_etf_metrics_valuation_dates")

# Issuer rows whose ``source_url`` encodes an explicit valuation session date.
VALUATION_URL_PROVIDERS = frozenset({
    "rex_shares",
    "yieldmax",
    "granite_shares",
    "merged",
    "proshares",
    "roundhill",
})


def parse_valuation_date_from_url(url: str | None) -> date | None:
    """Best-effort first valuation date embedded in ``source_url`` (handles ``|``-merged)."""
    if not url or not isinstance(url, str):
        return None
    for part in url.split("|"):
        part = part.strip()
        if not part:
            continue
        m = re.search(r"#as_of=(\d{4}-\d{2}-\d{2})", part, re.I)
        if m:
            try:
                return date.fromisoformat(m.group(1))
            except ValueError:
                pass
        m = re.search(r"#as_of=(\d{1,2}/\d{1,2}/\d{4})", part)
        if m:
            try:
                return datetime.strptime(m.group(1), "%m/%d/%Y").date()
            except ValueError:
                pass
        m = re.search(r"#nav_date=(\d{4}-\d{2}-\d{2})", part, re.I)
        if m:
            try:
                return date.fromisoformat(m.group(1))
            except ValueError:
                pass
        m = re.search(r"[#&]date=(\d{4}-\d{2}-\d{2})", part, re.I)
        if m:
            try:
                return date.fromisoformat(m.group(1))
            except ValueError:
                pass
    return None


def fix_carry_forward_closes(df: pd.DataFrame, report: list[dict]) -> int:
    """Null ``close_price`` / ``shares_traded`` on legacy carry_forward rows that still have closes."""
    n = 0
    for i, row in df.iterrows():
        if str(row.get("source_provider") or "") != "carry_forward":
            continue
        ch = row.get("close_price")
        if ch is None or (isinstance(ch, float) and math.isnan(ch)):
            continue
        report.append({
            "ticker": str(row.get("ticker") or "").upper(),
            "old_date": str(row.get("date")),
            "new_date": str(row.get("date")),
            "reason": "carry_forward_clear_close",
            "source_url": row.get("source_url"),
        })
        df.at[i, "close_price"] = None
        df.at[i, "shares_traded"] = None
        n += 1
    return n


def apply_url_migrations(df: pd.DataFrame, report: list[dict], *, since: date | None, tickers: set[str] | None) -> tuple[int, int]:
    """Return (n_relabel, n_merge_delete). Mutates ``df`` in place (reset index first)."""
    df.reset_index(drop=True, inplace=True)
    n_relabel = 0
    n_merge_del = 0
    to_drop: set[int] = set()

    for i in range(len(df)):
        if i in to_drop:
            continue
        row = df.iloc[i]
        t = str(row.get("ticker") or "").strip().upper()
        if not t:
            continue
        if tickers is not None and t not in tickers:
            continue
        d_wrong = row["date"]
        if hasattr(d_wrong, "date"):
            d_wrong = d_wrong.date()  # type: ignore[union-attr]
        if since is not None and d_wrong < since:
            continue
        prov = str(row.get("source_provider") or "").strip().lower()
        if prov not in VALUATION_URL_PROVIDERS:
            continue
        vd = parse_valuation_date_from_url(str(row.get("source_url") or ""))
        if vd is None or vd == d_wrong:
            continue

        mask = (df["ticker"].astype(str).str.upper() == t) & (df["date"] == vd)
        if mask.any():
            j = int(df.index[mask][0])
            tgt = df.iloc[j]
            # Overlay issuer economics from misaligned row (trusted for session ``vd``).
            for col in ("nav", "aum", "shares_outstanding"):
                wv = row.get(col)
                if wv is not None and not (isinstance(wv, float) and math.isnan(float(wv))):
                    try:
                        if float(wv) > 0:
                            df.iat[j, df.columns.get_loc(col)] = float(wv)
                    except (TypeError, ValueError):
                        pass
            for col in ("source_provider", "source_url", "status", "stale", "stale_age_bdays", "ingested_at_utc"):
                if col in df.columns and pd.notna(row.get(col)):
                    df.iat[j, df.columns.get_loc(col)] = row.get(col)
            report.append({
                "ticker": t,
                "old_date": str(d_wrong),
                "new_date": str(vd),
                "reason": "merge_issuer_into_existing_valuation_row",
                "source_url": row.get("source_url"),
            })
            to_drop.add(i)
            n_merge_del += 1
        else:
            df.iat[i, df.columns.get_loc("date")] = vd
            df.iat[i, df.columns.get_loc("close_price")] = None
            if "shares_traded" in df.columns:
                df.iat[i, df.columns.get_loc("shares_traded")] = None
            if "underlying_adj_close" in df.columns:
                df.iat[i, df.columns.get_loc("underlying_adj_close")] = None
            report.append({
                "ticker": t,
                "old_date": str(d_wrong),
                "new_date": str(vd),
                "reason": "relabel_row_to_valuation_date",
                "source_url": row.get("source_url"),
            })
            n_relabel += 1

    if to_drop:
        df.drop(index=sorted(to_drop), inplace=True)
        df.reset_index(drop=True, inplace=True)
    return n_relabel, n_merge_del


def postprocess_like_ingest(df: pd.DataFrame, underlying_map: dict[str, str]) -> pd.DataFrame:
    """Match post-upsert cleanup in ``ingest_etf_metrics.main`` (no holdings phase)."""
    out = df.copy()
    if "ingested_at_utc" in out.columns:
        out["ingested_at_utc"] = pd.to_datetime(out["ingested_at_utc"], errors="coerce", utc=True)
        out = out.sort_values("ingested_at_utc").drop_duplicates(subset=["date", "ticker"], keep="last")
    out = out.sort_values(["date", "ticker"]).reset_index(drop=True)
    out, _ = collapse_redundant_consecutive_rows(out)
    out, n_sh = repair_shares_vs_aum_nav(out)
    if n_sh:
        out = enforce_status_consistency(out)
        out, _ = collapse_redundant_consecutive_rows(out)
    out, _ = repair_close_price_split_basis_mismatch(out)
    out = backfill_underlying_adj_close_gaps(out, underlying_map)
    validate_df(out)
    return out


def yahoo_refresh_window(
    df: pd.DataFrame,
    tickers: list[str],
    d_min: date,
    d_max: date,
) -> pd.DataFrame:
    if not tickers or d_min > d_max:
        return df
    out = df.copy()
    close_df = fetch_close_prices_batch(tickers, d_min, d_max)
    if not close_df.empty:
        out = merge_close_prices(out, close_df)
    umap = load_universe_underlying_map()
    und_set = sorted({str(umap.get(s, "")).strip().upper() for s in tickers if umap.get(s)})
    und_set = [u for u in und_set if u]
    if und_set:
        und_df = fetch_underlying_adj_close_batch(und_set, d_min, d_max)
        if not und_df.empty:
            out = merge_underlying_adj_close(out, und_df, umap)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write parquet/csv/json (default: dry-run).")
    parser.add_argument("--since", type=str, default=None, help="Only rows with date >= YYYY-MM-DD.")
    parser.add_argument("--tickers", type=str, default=None, help="Comma-separated uppercase tickers (subset).")
    parser.add_argument("--report", type=str, default=None, help="Write JSON report path.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    since = None
    if args.since:
        since = datetime.strptime(args.since.strip(), "%Y-%m-%d").date()
    tickers: set[str] | None = None
    if args.tickers:
        tickers = {x.strip().upper() for x in args.tickers.split(",") if x.strip()}

    if not PARQUET_PATH.exists() and not CSV_PATH.exists() and not Path(JSON_PATH).exists():
        LOGGER.error("No metrics store at %s / %s", PARQUET_PATH, CSV_PATH)
        sys.exit(1)

    df = load_existing()
    if df.empty:
        LOGGER.info("Empty store; nothing to migrate")
        return

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    report: list[dict] = []

    n_cf = fix_carry_forward_closes(df, report)
    n_rel, n_md = apply_url_migrations(df, report, since=since, tickers=tickers)

    touched_tickers = sorted({str(r["ticker"]).upper() for r in report if r.get("ticker")})
    if not touched_tickers and tickers:
        touched_tickers = sorted(tickers)

    dates = [r.get("old_date") for r in report if r.get("old_date")] + [r.get("new_date") for r in report if r.get("new_date")]
    d_parsed = []
    for d in dates:
        if d is None:
            continue
        try:
            d_parsed.append(datetime.strptime(str(d)[:10], "%Y-%m-%d").date())
        except ValueError:
            continue
    if d_parsed:
        d_min, d_max = min(d_parsed), max(d_parsed)
    else:
        d_min = df["date"].min()
        d_max = df["date"].max()

    universe = load_universe_tickers()
    y_tickers = touched_tickers if touched_tickers else [t for t in universe if not tickers or t in tickers]

    LOGGER.info(
        "Migration summary (dry_run=%s): carry_forward_fix=%d relabel=%d merge_drop=%d report_rows=%d yahoo_tickers=%d window=%s..%s",
        not args.apply,
        n_cf,
        n_rel,
        n_md,
        len(report),
        len(y_tickers) if (n_cf + n_rel + n_md) > 0 else 0,
        d_min,
        d_max,
    )

    if args.report:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        with open(args.report, "w", encoding="utf-8") as f:
            json.dump({"rows": report, "summary": {"carry_forward": n_cf, "relabel": n_rel, "merge_drop": n_md}}, f, indent=2)
            f.write("\n")

    if not args.apply:
        LOGGER.info(
            "Dry-run: in-memory mutations only (no Yahoo refresh, postprocess, or save). "
            "Re-run with --apply to refresh closes, validate, and write parquet/json/csv.",
        )
        return

    if n_cf + n_rel + n_md > 0 and y_tickers:
        df = yahoo_refresh_window(df, y_tickers, d_min, d_max)
    else:
        LOGGER.info("Skipping Yahoo batch (no mutations or no tickers).")
    underlying_map = load_universe_underlying_map()
    df = postprocess_like_ingest(df, underlying_map)
    save_outputs(df)
    LOGGER.info("Wrote store to %s", PARQUET_PATH)


if __name__ == "__main__":
    main()

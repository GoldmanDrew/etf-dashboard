#!/usr/bin/env python3
"""Heal ``etf_adj_close`` cells that were stored one session away from ``close_price``.

When market columns resolve on different session keys, ``etf_adj_close`` ends up
holding the *next* session's close while ``close_price`` and
``underlying_adj_close`` stay on the row session. Pair drag then evaluates
``beta * r_und[t] - r_etf[t+1]``, which is noise with a large negative bias on a
falling ETF, so realized decay publishes confident wrong (often negative) values.

Detection does not use an amplitude threshold. ``etf_adj_close / close_price`` is
a smooth, slowly-varying adjustment factor (flat between distributions and
splits). If the adjusted column is shifted, ``etf_adj_close[t] / close_price[t+1]``
is the smooth ratio instead. Comparing the roughness of the two candidate ratios
over a rolling window localises the affected sessions per ticker.

Repair nulls the affected cells and re-derives them from Yahoo (and Polygon as a
fallback) on the row session. ``backfill_etf_adj_close_from_close_gaps`` only
fills nulls, so the null step is required before any backfill can take effect.

Usage::

    python scripts/repair_shifted_etf_adj_close.py
    python scripts/repair_shifted_etf_adj_close.py --apply
    python scripts/repair_shifted_etf_adj_close.py --apply --since 2026-06-01
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from ingest_etf_metrics import (  # noqa: E402
    PARQUET_PATH,
    backfill_close_prices_polygon_gaps,
    backfill_etf_adj_close_from_close_gaps,
    fetch_etf_adj_close_batch,
    load_existing,
    save_outputs,
    stamp_metric_asof_metadata,
    validate_df,
)

LOGGER = logging.getLogger("repair_shifted_etf_adj_close")
DEFAULT_REPORT = ROOT / "data" / "metrics_shifted_adj_repair_report.json"

# Rolling window used to localise a shifted run. Long enough that the roughness
# statistic is stable, short enough to bracket a multi-week ingest incident.
WINDOW = 11
# The shifted ratio must be markedly smoother than the aligned one, and the
# aligned ratio must be genuinely rough — otherwise a flat tape trips the test.
ROUGHNESS_RATIO_MAX = 0.25
MIN_ALIGNED_ROUGHNESS = 0.01


def _roughness(values: list[float]) -> float:
    """Mean absolute first difference of a log-ratio series."""
    if len(values) < 2:
        return 0.0
    return sum(abs(values[i] - values[i - 1]) for i in range(1, len(values))) / (len(values) - 1)


def find_shifted_sessions(
    dates: list[date],
    closes: list[float],
    adjs: list[float],
) -> set[date]:
    """Sessions where ``etf_adj_close`` looks sampled one session ahead.

    Two passes. The rolling window locates a shifted *region*; a per-session pass
    then keeps only the sessions where the shifted ratio is locally smoother than
    the aligned one. Without the second pass the window bleeds up to ``WINDOW``
    healthy sessions past each end of the run, and nulling those would discard
    correct distribution-adjusted values.
    """
    n = len(dates)
    if n < WINDOW + 1:
        return set()
    aligned: list[float] = []
    shifted: list[float] = []
    for i in range(n - 1):
        aligned.append(math.log(adjs[i] / closes[i]))
        shifted.append(math.log(adjs[i] / closes[i + 1]))

    candidates: set[int] = set()
    for start in range(0, len(aligned) - WINDOW + 1):
        rough_a = _roughness(aligned[start : start + WINDOW])
        rough_s = _roughness(shifted[start : start + WINDOW])
        if rough_a < MIN_ALIGNED_ROUGHNESS:
            continue
        if rough_s <= ROUGHNESS_RATIO_MAX * rough_a:
            candidates.update(range(start, start + WINDOW))

    confirmed: set[date] = set()
    for i in sorted(candidates):
        steps_a: list[float] = []
        steps_s: list[float] = []
        for j in (i, i + 1):
            if 0 < j < len(aligned):
                steps_a.append(abs(aligned[j] - aligned[j - 1]))
                steps_s.append(abs(shifted[j] - shifted[j - 1]))
        if not steps_a:
            continue
        if min(steps_s) < min(steps_a):
            confirmed.add(dates[i])
    return confirmed


def scan(df: pd.DataFrame, *, since: date | None = None) -> dict[str, list[date]]:
    """Per-ticker sessions whose ``etf_adj_close`` is shifted."""
    work = df.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.date
    work["ticker"] = work["ticker"].astype(str).str.upper()
    hits: dict[str, list[date]] = {}
    for ticker, grp in work.groupby("ticker"):
        grp = grp.sort_values("date")
        close = pd.to_numeric(grp["close_price"], errors="coerce")
        adj = pd.to_numeric(grp.get("etf_adj_close"), errors="coerce")
        keep = close.gt(0) & adj.gt(0)
        if int(keep.sum()) < WINDOW + 1:
            continue
        sub = grp[keep]
        flagged = find_shifted_sessions(
            list(sub["date"]),
            [float(x) for x in close[keep]],
            [float(x) for x in adj[keep]],
        )
        if since is not None:
            flagged = {d for d in flagged if d >= since}
        if flagged:
            hits[str(ticker)] = sorted(flagged)
    return hits


def repair(
    df: pd.DataFrame,
    *,
    since: date | None = None,
    use_polygon: bool = True,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    hits = scan(df, since=since)
    work = df.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.date
    work["ticker"] = work["ticker"].astype(str).str.upper()

    n_nulled = 0
    if hits:
        pairs = {(t, d) for t, days in hits.items() for d in days}
        mask = pd.Series(
            [(t, d) in pairs for t, d in zip(work["ticker"], work["date"])],
            index=work.index,
        )
        n_nulled = int(mask.sum())
        work.loc[mask, "etf_adj_close"] = None

    n_refetched = 0
    if hits:
        tickers = sorted(hits)
        d_min = min(min(days) for days in hits.values())
        d_max = max(max(days) for days in hits.values())
        adj_df = fetch_etf_adj_close_batch(tickers, d_min, d_max)
        if not adj_df.empty:
            adj_df = adj_df.copy()
            adj_df["date"] = pd.to_datetime(adj_df["date"], errors="coerce").dt.date
            adj_df["ticker"] = adj_df["ticker"].astype(str).str.upper()
            adj_df = adj_df.drop_duplicates(subset=["date", "ticker"], keep="last")
            before = int(pd.to_numeric(work["etf_adj_close"], errors="coerce").notna().sum())
            work = work.merge(
                adj_df.rename(columns={"etf_adj_close": "_fresh_adj"}),
                on=["date", "ticker"],
                how="left",
            )
            work["etf_adj_close"] = pd.to_numeric(work["etf_adj_close"], errors="coerce").combine_first(
                pd.to_numeric(work.pop("_fresh_adj"), errors="coerce")
            )
            n_refetched = int(
                pd.to_numeric(work["etf_adj_close"], errors="coerce").notna().sum()
            ) - before

    if use_polygon and n_nulled:
        work, _n_poly = backfill_close_prices_polygon_gaps(work)
    work, n_from_close = backfill_etf_adj_close_from_close_gaps(work)
    work = stamp_metric_asof_metadata(work)

    residual = scan(work, since=since)
    report = {
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "since": since.isoformat() if since else None,
        "tickers_detected": len(hits),
        "sessions_detected": sum(len(v) for v in hits.values()),
        "cells_nulled": n_nulled,
        "cells_refetched_yahoo": max(0, n_refetched),
        "cells_filled_from_close": int(n_from_close),
        "tickers_residual": len(residual),
        "detected": {t: [d.isoformat() for d in days] for t, days in sorted(hits.items())},
        "residual": {t: [d.isoformat() for d in days] for t, days in sorted(residual.items())},
    }
    return work, report


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Write the repaired metrics store")
    parser.add_argument("--since", type=str, default=None, help="Only repair sessions on/after YYYY-MM-DD")
    parser.add_argument("--no-polygon", action="store_true", help="Skip the Polygon close backfill")
    parser.add_argument("--report-out", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()

    since: date | None = None
    if args.since:
        try:
            since = date.fromisoformat(str(args.since)[:10])
        except ValueError:
            LOGGER.error("bad --since value %r (expected YYYY-MM-DD)", args.since)
            return 2

    df = load_existing(PARQUET_PATH)
    if df.empty:
        LOGGER.error("empty metrics store at %s", PARQUET_PATH)
        return 2

    repaired, report = repair(df, since=since, use_polygon=not args.no_polygon)
    args.report_out.parent.mkdir(parents=True, exist_ok=True)
    args.report_out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    LOGGER.info(
        "detected=%s ticker(s) / %s session(s); nulled=%s refetched=%s from_close=%s residual=%s",
        report["tickers_detected"],
        report["sessions_detected"],
        report["cells_nulled"],
        report["cells_refetched_yahoo"],
        report["cells_filled_from_close"],
        report["tickers_residual"],
    )
    LOGGER.info("wrote %s", args.report_out)

    if args.apply:
        validate_df(repaired)
        save_outputs(repaired)
        LOGGER.info("saved repaired metrics to %s", PARQUET_PATH)
    else:
        LOGGER.info("dry-run only (pass --apply to write)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

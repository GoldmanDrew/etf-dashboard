#!/usr/bin/env python3
"""Fleet-wide split TR quality gate — flags spurious cliffs on reverse/forward splits."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import sys
from pathlib import Path

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from price_basis import (  # noqa: E402
    build_tr_series_from_metrics,
    max_abs_log_return,
    parse_split_events_from_corp,
    resolve_split_context,
)
from split_adjustments import (  # noqa: E402
    DEFAULT_CORPORATE_ACTIONS_PATH,
    etf_adj_close_needs_split_scaling,
    load_split_events_for_ticker,
)

REPO = _SCRIPTS.parent
METRICS_PARQUET = REPO / "data" / "etf_metrics_daily.parquet"
MAX_LOG_RETURN = 0.35
LOOKBACK_MONTHS = 18
SPLIT_WINDOW_DAYS = 7


def _load_corp_payload(path: Path) -> dict:
    if not path.exists():
        return {"events": []}
    return json.loads(path.read_text(encoding="utf-8"))


def _split_tickers(payload: dict, *, since: dt.date) -> set[str]:
    out: set[str] = set()
    for ev in payload.get("events") or []:
        if str(ev.get("type") or "") not in {"reverse_split", "forward_split"}:
            continue
        ed = str(ev.get("execution_date") or "")[:10]
        if len(ed) != 10:
            continue
        try:
            d0 = dt.date.fromisoformat(ed)
        except ValueError:
            continue
        if d0 >= since:
            out.add(str(ev.get("ticker") or "").strip().upper())
    return out


def audit_ticker(
    ticker: str,
    rows: list[dict],
    split_events: list[tuple[dt.date, float]],
    *,
    corp_has_split: bool,
    max_log_return: float,
) -> list[str]:
    failures: list[str] = []
    if not rows or not split_events:
        return failures

    tr = build_tr_series_from_metrics(rows, split_events)
    if len(tr) < 2:
        return failures

    split_dates = {eff for eff, _ in split_events}
    max_near = 0.0
    near_date: str | None = None
    for i in range(1, len(tr)):
        ds = str(tr[i].get("date") or "")[:10]
        if len(ds) != 10:
            continue
        try:
            d0 = dt.date.fromisoformat(ds)
        except ValueError:
            continue
        near_split = any(abs((d0 - eff).days) <= SPLIT_WINDOW_DAYS for eff in split_dates)
        if not near_split:
            continue
        try:
            a = float(tr[i - 1]["tr_etf_px"])
            b = float(tr[i]["tr_etf_px"])
        except (KeyError, TypeError, ValueError):
            continue
        if a <= 0 or b <= 0:
            continue
        lr = abs(math.log(b / a))
        if lr > max_near:
            max_near = lr
            near_date = ds
    if max_near > max_log_return:
        failures.append(
            f"{ticker}: max ETF TR daily log-return {max_near:.3f} "
            f"({math.expm1(max_near)*100:.0f}%) near split on {near_date}"
        )

    close_pts = []
    adj_equiv_close = True
    for row in rows:
        ds = str(row.get("date") or "")[:10]
        if len(ds) != 10:
            continue
        try:
            close = float(row.get("close_price") or row.get("nav") or 0)
            adj = float(row.get("etf_adj_close") or close)
        except (TypeError, ValueError):
            continue
        if close <= 0:
            continue
        d0 = dt.date.fromisoformat(ds)
        close_pts.append((d0, close))
        if not etf_adj_close_needs_split_scaling(close, adj):
            adj_equiv_close = False

    ctx = resolve_split_context(close_pts, split_events, metric_rows=rows)
    if (
        corp_has_split
        and adj_equiv_close
        and ctx.get("mode") == "continuous"
        and split_events
    ):
        failures.append(
            f"{ticker}: corp split present, etf_adj_close==close, but splitMode=continuous"
        )

    return failures


def _bucket_by_ticker(payload: dict) -> dict[str, str]:
    out: dict[str, str] = {}
    for ev in payload.get("events") or []:
        sym = str(ev.get("ticker") or "").strip().upper()
        bucket = str(ev.get("bucket") or "").strip()
        if sym and bucket:
            out[sym] = bucket
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit split-aware TR quality across universe.")
    parser.add_argument("--corp-actions", type=Path, default=DEFAULT_CORPORATE_ACTIONS_PATH)
    parser.add_argument("--metrics", type=Path, default=METRICS_PARQUET)
    parser.add_argument("--max-log-return", type=float, default=MAX_LOG_RETURN)
    parser.add_argument("--lookback-months", type=int, default=LOOKBACK_MONTHS)
    parser.add_argument(
        "--strict-bucket-1",
        action="store_true",
        help="Exit 1 only when bucket_1_high_delta tickers fail; other failures warn.",
    )
    args = parser.parse_args()

    max_log_return = float(args.max_log_return)

    if not args.metrics.exists():
        print(f"metrics store missing: {args.metrics}", file=sys.stderr)
        return 0

    payload = _load_corp_payload(args.corp_actions)
    bucket_by_ticker = _bucket_by_ticker(payload)
    since = dt.date.today() - dt.timedelta(days=int(args.lookback_months) * 31)
    tickers = _split_tickers(payload, since=since)
    if not tickers:
        print("No recent split tickers in corporate_actions.json")
        return 0

    df = pd.read_parquet(args.metrics)
    df["date"] = df["date"].astype(str).str[:10]
    df["ticker"] = df["ticker"].astype(str).str.upper()

    all_failures: list[str] = []
    for sym in sorted(tickers):
        sub = df[df["ticker"] == sym].sort_values("date")
        if sub.empty:
            continue
        rows = sub.to_dict("records")
        events = load_split_events_for_ticker(sym, args.corp_actions)
        if not events:
            events = parse_split_events_from_corp(payload, sym)
        all_failures.extend(
            audit_ticker(
                sym,
                rows,
                events,
                corp_has_split=bool(events),
                max_log_return=max_log_return,
            )
        )

    if all_failures:
        print("Split TR quality failures:")
        for msg in all_failures:
            print(f"  - {msg}")
        if args.strict_bucket_1:
            blocking = [
                msg
                for msg in all_failures
                if bucket_by_ticker.get(msg.split(":", 1)[0].strip()) == "bucket_1_high_delta"
            ]
            if blocking:
                print(f"\nBlocking ({len(blocking)} bucket_1_high_delta failure(s))")
                return 1
            print(f"\nWarn-only: {len(all_failures)} failure(s) outside bucket_1_high_delta")
            return 0
        return 1

    print(f"Split TR quality OK for {len(tickers)} ticker(s) with recent corp splits")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

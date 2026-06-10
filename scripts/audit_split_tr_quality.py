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
SPLIT_WINDOW_DAYS = 2
MAX_UNDERLYING_LOG_MOVE_FOR_CLIFF = 0.25
SPLIT_SIZED_CLIFF_EVENT_WINDOW_DAYS = 90


def _known_split_multipliers(split_events: list[tuple[dt.date, float]]) -> list[float]:
    out: list[float] = []
    for _d, mult in split_events or []:
        try:
            m = float(mult)
        except (TypeError, ValueError):
            continue
        if m <= 0:
            continue
        cand = m if m >= 1 else 1.0 / m
        if cand > 1.05 and not any(abs(cand / x - 1.0) <= 1e-6 for x in out):
            out.append(cand)
    return out


def _is_split_sized_jump(lr_abs: float, multipliers: list[float], *, rel_tol: float = 0.18) -> bool:
    if not (math.isfinite(lr_abs) and lr_abs > 0):
        return False
    jump = math.exp(lr_abs)
    return any(abs(jump / mult - 1.0) <= rel_tol for mult in multipliers)


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

    multipliers = _known_split_multipliers(split_events)
    max_any = 0.0
    max_any_date: str | None = None
    for i in range(1, len(tr)):
        try:
            e0 = float(tr[i - 1]["tr_etf_px"])
            e1 = float(tr[i]["tr_etf_px"])
            u0 = float(tr[i - 1]["tr_und_px"])
            u1 = float(tr[i]["tr_und_px"])
        except (KeyError, TypeError, ValueError):
            continue
        if min(e0, e1, u0, u1) <= 0:
            continue
        try:
            d_cur = dt.date.fromisoformat(str(tr[i].get("date") or "")[:10])
        except ValueError:
            continue
        if not any(
            abs((d_cur - eff).days) <= SPLIT_SIZED_CLIFF_EVENT_WINDOW_DAYS
            for eff, _mult in split_events
        ):
            continue
        lr_u = abs(math.log(u1 / u0))
        if lr_u >= MAX_UNDERLYING_LOG_MOVE_FOR_CLIFF:
            continue
        lr_e = abs(math.log(e1 / e0))
        if not _is_split_sized_jump(lr_e, multipliers):
            continue
        if lr_e > max_any:
            max_any = lr_e
            max_any_date = str(tr[i].get("date") or "")[:10]
    if max_any > max_log_return:
        failures.append(
            f"{ticker}: max unexplained ETF TR daily log-return {max_any:.3f} "
            f"({math.expm1(max_any)*100:.0f}%) on {max_any_date}"
        )

    close_pts = []
    adj_by_date: dict[dt.date, float] = {}
    adj_equiv_close = True
    had_basis_switch = False
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
        adj_by_date[d0] = adj
        if not etf_adj_close_needs_split_scaling(close, adj):
            adj_equiv_close = False
        ratio = adj / close
        if ratio > 1.4 or ratio < 0.75:
            had_basis_switch = True

    ctx = resolve_split_context(close_pts, split_events, metric_rows=rows, adj_by_date=adj_by_date)
    boundary = ctx.get("boundary")
    mode = ctx.get("mode") or "continuous"

    near_dates: set[dt.date] = set()
    if mode == "continuous" and not had_basis_switch:
        pass
    elif boundary is not None:
        try:
            bnd = boundary if isinstance(boundary, dt.date) else dt.date.fromisoformat(str(boundary)[:10])
            for delta in range(-SPLIT_WINDOW_DAYS, SPLIT_WINDOW_DAYS + 1):
                near_dates.add(bnd + dt.timedelta(days=delta))
        except ValueError:
            pass
    else:
        for eff, _ in split_events:
            for delta in range(-SPLIT_WINDOW_DAYS, SPLIT_WINDOW_DAYS + 1):
                near_dates.add(eff + dt.timedelta(days=delta))

    max_near = 0.0
    near_date: str | None = None
    if near_dates:
        for i in range(1, len(tr)):
            ds = str(tr[i].get("date") or "")[:10]
            if len(ds) != 10:
                continue
            try:
                d0 = dt.date.fromisoformat(ds)
            except ValueError:
                continue
            if d0 not in near_dates:
                continue
            try:
                a = float(tr[i - 1]["tr_etf_px"])
                b = float(tr[i]["tr_etf_px"])
            except (KeyError, TypeError, ValueError):
                continue
            if a <= 0 or b <= 0:
                continue
            try:
                u0 = float(tr[i - 1]["tr_und_px"])
                u1 = float(tr[i]["tr_und_px"])
            except (KeyError, TypeError, ValueError):
                u0 = u1 = 0.0
            if u0 > 0 and u1 > 0:
                lr_u = abs(math.log(u1 / u0))
                if lr_u >= 0.25:
                    continue
            lr = abs(math.log(b / a))
            if not _is_split_sized_jump(lr, multipliers):
                continue
            mult = float(ctx.get("mult") or 0)
            if mult > 0 and mode == "discrete_split" and mult >= 1.05:
                expected = math.log(mult)
                if abs(lr - expected) <= 0.25 or abs(lr + expected) <= 0.25:
                    continue
            if lr > max_near:
                max_near = lr
                near_date = ds
        if max_near > max_log_return:
            failures.append(
                f"{ticker}: max ETF TR daily log-return {max_near:.3f} "
                f"({math.expm1(max_near)*100:.0f}%) near split on {near_date}"
            )

    if (
        corp_has_split
        and adj_equiv_close
        and mode == "continuous"
        and split_events
        and had_basis_switch
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
        "--symbols",
        default="",
        help="Optional comma-separated ticker filter for targeted audits.",
    )
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
    wanted = {
        s.strip().upper()
        for s in str(args.symbols or "").split(",")
        if s.strip()
    }
    tickers = wanted or _split_tickers(payload, since=since)
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
        events = parse_split_events_from_corp(payload, sym)
        if not events:
            events = load_split_events_for_ticker(sym, args.corp_actions)
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

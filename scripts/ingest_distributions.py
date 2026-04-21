#!/usr/bin/env python3
"""Ingest per-ticker distribution (dividend) history for the dashboard universe.

Pulls the full inception-to-today dividend event list for every ticker in
``data/etf_screened_today.csv`` from the public Yahoo Finance chart API and
writes it to ``data/etf_distributions.json``:

    {
      "build_time": "2026-04-21T...",
      "range": "max",
      "by_symbol": {
        "MSTY": [
          { "ex_date": "2024-02-27", "amount": 1.42 },
          ...
        ],
        ...
      }
    }

The frontend consumes this alongside ``etf_metrics_daily.json`` to build a
distribution-adjusted ("total return") NAV series -- NAV on each date scaled
by the cumulative reinvestment factor of every prior ex-dividend event.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import time
from datetime import UTC, datetime, date
from pathlib import Path

import pandas as pd
import requests


LOGGER = logging.getLogger("etf_distributions_ingest")

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
UNIVERSE_CSV = DATA_DIR / "etf_screened_today.csv"
OUTPUT_PATH = DATA_DIR / "etf_distributions.json"

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
DEFAULT_RANGE = "max"
DEFAULT_TIMEOUT_SEC = int(os.getenv("ETF_DISTRIBUTIONS_HTTP_TIMEOUT_SEC", "20"))
DEFAULT_RETRY_TOTAL = int(os.getenv("ETF_DISTRIBUTIONS_HTTP_RETRY_TOTAL", "2"))
DEFAULT_SLEEP_BETWEEN_CALLS = float(os.getenv("ETF_DISTRIBUTIONS_SLEEP_SEC", "0.1"))


def _normalize_symbol(v: object) -> str:
    return str(v).strip().upper().replace(".", "-")


def load_universe_tickers(path: Path = UNIVERSE_CSV) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Universe CSV missing: {path}")
    df = pd.read_csv(path)
    if "ETF" not in df.columns:
        raise ValueError(f"Universe CSV missing ETF column: {path}")
    return sorted({_normalize_symbol(x) for x in df["ETF"].dropna().tolist()})


def fetch_distributions_for_symbol(
    session: requests.Session,
    symbol: str,
    *,
    range_value: str = DEFAULT_RANGE,
    timeout_sec: int = DEFAULT_TIMEOUT_SEC,
    retries: int = DEFAULT_RETRY_TOTAL,
) -> list[dict]:
    """Return a sorted list of {ex_date, amount} for the ticker (empty on failure)."""
    url = YAHOO_CHART_URL.format(symbol=symbol)
    params = {"range": range_value, "interval": "1d", "events": "div"}

    last_err: Exception | None = None
    for attempt in range(max(1, retries + 1)):
        try:
            resp = session.get(url, params=params, timeout=timeout_sec)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as e:  # noqa: BLE001 - network, logged below
            last_err = e
            time.sleep(0.4 * (attempt + 1))
            continue

        result = (payload.get("chart") or {}).get("result") or []
        if not result:
            return []
        events = (result[0].get("events") or {}).get("dividends") or {}
        if not isinstance(events, dict):
            return []
        out: list[dict] = []
        for ts_key, ev in events.items():
            amount = None
            ts = None
            if isinstance(ev, dict):
                amount = ev.get("amount")
                ts = ev.get("date", ts_key)
            else:
                ts = ts_key
            try:
                amt = float(amount)
                if not math.isfinite(amt) or amt <= 0:
                    continue
            except (TypeError, ValueError):
                continue
            try:
                ex_ts = int(ts)
                ex_date = datetime.fromtimestamp(ex_ts, UTC).date()
            except (TypeError, ValueError, OSError):
                continue
            out.append({"ex_date": ex_date.isoformat(), "amount": round(amt, 8)})
        # Stable chronological ordering; de-dupe identical (date, amount) entries.
        seen: set[tuple[str, float]] = set()
        uniq: list[dict] = []
        for d in sorted(out, key=lambda x: x["ex_date"]):
            key = (d["ex_date"], d["amount"])
            if key in seen:
                continue
            seen.add(key)
            uniq.append(d)
        return uniq

    LOGGER.warning("Yahoo distributions fetch failed for %s: %s", symbol, last_err)
    return []


def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "etf-dashboard-distributions/1.0 (+https://github.com/)",
            "Accept": "application/json, text/plain, */*",
        }
    )
    return s


def write_output(
    by_symbol: dict[str, list[dict]],
    *,
    path: Path = OUTPUT_PATH,
    range_value: str = DEFAULT_RANGE,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "build_time": datetime.now(UTC).isoformat(),
        "range": range_value,
        "symbols": len(by_symbol),
        "total_distributions": int(sum(len(v) for v in by_symbol.values())),
        "by_symbol": {sym: list(events) for sym, events in sorted(by_symbol.items())},
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), allow_nan=False)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pull inception-to-today distribution history for every ETF in the universe."
    )
    parser.add_argument("--range", default=DEFAULT_RANGE, help="Yahoo chart range (default: max)")
    parser.add_argument(
        "--symbols",
        default=None,
        help="Optional comma-separated subset; default is the full screened universe.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=DEFAULT_SLEEP_BETWEEN_CALLS,
        help="Seconds to sleep between successive ticker fetches.",
    )
    parser.add_argument(
        "--out",
        default=str(OUTPUT_PATH),
        help="Output JSON path.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if args.symbols:
        tickers = sorted({_normalize_symbol(x) for x in args.symbols.split(",") if x.strip()})
    else:
        tickers = load_universe_tickers()
    if not tickers:
        LOGGER.warning("No tickers to process; exiting.")
        return
    LOGGER.info("Fetching distributions for %d tickers (range=%s)", len(tickers), args.range)

    session = build_session()

    by_symbol: dict[str, list[dict]] = {}
    total_events = 0
    for i, sym in enumerate(tickers, start=1):
        events = fetch_distributions_for_symbol(session, sym, range_value=args.range)
        by_symbol[sym] = events
        total_events += len(events)
        if i % 50 == 0 or i == len(tickers):
            LOGGER.info(
                "  progress: %d/%d tickers, %d distribution events so far",
                i,
                len(tickers),
                total_events,
            )
        if args.sleep > 0 and i < len(tickers):
            time.sleep(args.sleep)

    out_path = Path(args.out)
    write_output(by_symbol, path=out_path, range_value=args.range)

    tickers_with_events = sum(1 for v in by_symbol.values() if v)
    LOGGER.info(
        "Wrote %s (tickers=%d, with_events=%d, events=%d)",
        out_path,
        len(by_symbol),
        tickers_with_events,
        total_events,
    )


if __name__ == "__main__":
    main()

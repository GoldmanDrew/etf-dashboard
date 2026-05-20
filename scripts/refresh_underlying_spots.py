#!/usr/bin/env python3
"""Pull intraday underlying spot + prior close for the LETF universe.

The output (``data/underlying_intraday_spot.json``) feeds
``scripts/build_letf_intraday_flows.py``, which estimates today's pending
LETF close rebalance ahead of the print.

Source priority (graceful fallback):

1. **Polygon snapshot** -- one batched ``/v2/snapshot/locale/us/markets/stocks/tickers``
   call per chunk of ~200 tickers. Returns ``lastTrade.p``, ``prevDay.c``,
   ``day.v``. This is the freshest source for stocks and the only one that
   exposes intraday volume.
2. **``data/options_cache.json``** -- already refreshed every 5 min by
   ``refresh-options.yml``. Carries ``spot`` for ~100 LETFs/underlyings on
   the options-trader covered list, but no prior-close.
3. **``yfinance.Ticker.fast_info``** -- per-symbol fallback for whatever is
   still missing; rate-limited and slow, so capped via ``--max-yfinance``.

Prior-close is sourced from Polygon's ``prevDay.c`` when present, else the
most recent ``etf_metrics_daily.underlying_adj_close`` row for that
underlying (issuer-published close, the EOD pipeline's own ground truth).

Usage::

    python scripts/refresh_underlying_spots.py
    python scripts/refresh_underlying_spots.py --max-yfinance 0   # CI: skip yf entirely
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

LOGGER = logging.getLogger("underlying_spots")

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
UNIVERSE_CSV = DATA_DIR / "etf_screened_today.csv"
OPTIONS_CACHE = DATA_DIR / "options_cache.json"
METRICS_PARQUET = DATA_DIR / "etf_metrics_daily.parquet"
OUTPUT_JSON = DATA_DIR / "underlying_intraday_spot.json"

POLYGON_BASE = "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
POLYGON_BATCH_SIZE = 200
POLYGON_RETRIES = 3
POLYGON_TIMEOUT_SEC = 12.0
POLYGON_BACKOFF_SEC = 1.5

# yfinance becomes the per-symbol fallback for anything Polygon + the options
# cache couldn't price -- usually long-tail single names. Capped because each
# call is a separate HTTP round-trip and free-tier limits bite hard.
DEFAULT_MAX_YFINANCE = 60


def _norm_sym(v: object) -> str:
    return str(v or "").strip().upper().replace(".", "-")


def _polygon_sym(v: str) -> str:
    """Polygon uses dotted class shares (BRK.B); we store BRK-B."""
    return str(v or "").strip().upper().replace("-", ".")


def _f(v: object) -> float | None:
    try:
        x = float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _json_clean(v: Any) -> Any:
    if isinstance(v, dict):
        return {str(k): _json_clean(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_json_clean(x) for x in v]
    if v is None:
        return None
    if isinstance(v, float) and not math.isfinite(v):
        return None
    return v


# ?? Universe / priors ?????????????????????????????????????????????????????


def load_universe_underlyings(path: Path = UNIVERSE_CSV) -> tuple[list[str], dict[str, str]]:
    """Return (sorted unique underlyings, ticker->underlying map)."""
    if not path.exists():
        LOGGER.warning("Universe CSV missing: %s", path)
        return [], {}
    df = pd.read_csv(path)
    if "Underlying" not in df.columns or "ETF" not in df.columns:
        return [], {}
    df = df[["ETF", "Underlying"]].dropna()
    df["ETF"] = df["ETF"].map(_norm_sym)
    df["Underlying"] = df["Underlying"].map(_norm_sym)
    df = df[df["ETF"].astype(bool) & df["Underlying"].astype(bool)]
    ticker_to_und = dict(zip(df["ETF"], df["Underlying"]))
    underlyings = sorted(set(df["Underlying"].tolist()))
    return underlyings, ticker_to_und


def load_prior_closes_from_metrics(
    path: Path = METRICS_PARQUET,
    *,
    ticker_to_und: dict[str, str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Pull most-recent ``underlying_adj_close`` per underlying from the metrics panel.

    ``etf_metrics_daily`` stores ``underlying_adj_close`` as the issuer-published
    close of *that fund's* underlying. We pivot to per-underlying by the
    universe's ETF?underlying map and take the last date.
    """
    if not path.exists() or not ticker_to_und:
        return {}
    try:
        df = pd.read_parquet(path, columns=["ticker", "date", "underlying_adj_close"])
    except Exception as exc:  # pragma: no cover - parquet engine fallback
        LOGGER.debug("parquet column subset failed (%s); reading full panel", exc)
        try:
            df = pd.read_parquet(path)
        except Exception as exc2:
            LOGGER.warning("failed to read %s: %s", path, exc2)
            return {}

    if df.empty:
        return {}
    df = df.dropna(subset=["underlying_adj_close"])
    if df.empty:
        return {}
    df["ticker"] = df["ticker"].map(_norm_sym)
    df["underlying"] = df["ticker"].map(lambda t: ticker_to_und.get(t))
    df = df.dropna(subset=["underlying"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    if df.empty:
        return {}
    df = df.sort_values(["underlying", "date"]).drop_duplicates(["underlying"], keep="last")
    out: dict[str, dict[str, Any]] = {}
    for _, r in df.iterrows():
        out[str(r["underlying"])] = {
            "prior_close": _f(r["underlying_adj_close"]),
            "prior_close_date": r["date"].strftime("%Y-%m-%d") if pd.notna(r["date"]) else None,
        }
    return out


# ?? Source 2: options_cache.json ????????????????????????????????????????


def load_options_cache_spots(path: Path = OPTIONS_CACHE) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        LOGGER.warning("failed to parse %s: %s", path, exc)
        return {}
    syms = payload.get("symbols") or {}
    out: dict[str, dict[str, Any]] = {}
    for sym, entry in syms.items():
        spot = _f(entry.get("spot"))
        if spot is None or spot <= 0:
            continue
        out[_norm_sym(sym)] = {
            "last": spot,
            "as_of": entry.get("updated_at"),
            "stale": bool(entry.get("stale")),
            "source": "options_cache",
        }
    return out


# ?? Source 1: Polygon snapshot ??????????????????????????????????????????


def fetch_polygon_snapshots(tickers: list[str], api_key: str) -> dict[str, dict[str, Any]]:
    if not api_key or not tickers:
        return {}
    try:
        import requests  # type: ignore
    except ImportError:  # pragma: no cover - requests is in requirements.txt
        LOGGER.warning("requests not installed; skipping Polygon snapshot")
        return {}

    session = requests.Session()
    session.headers.update({
        "User-Agent": "etf-dashboard-intraday-spots/1.0",
        "Accept": "application/json",
    })

    out: dict[str, dict[str, Any]] = {}
    for start in range(0, len(tickers), POLYGON_BATCH_SIZE):
        chunk = tickers[start:start + POLYGON_BATCH_SIZE]
        polygon_syms = [_polygon_sym(t) for t in chunk]
        params = {"tickers": ",".join(polygon_syms), "apiKey": api_key}

        for attempt in range(POLYGON_RETRIES):
            try:
                resp = session.get(POLYGON_BASE, params=params, timeout=POLYGON_TIMEOUT_SEC)
            except Exception as exc:
                LOGGER.warning("polygon snapshot exception (attempt %d/%d): %s",
                               attempt + 1, POLYGON_RETRIES, exc)
                time.sleep(min(8.0, POLYGON_BACKOFF_SEC * (attempt + 1)))
                continue

            if resp.status_code in (429, 502, 503, 504):
                LOGGER.warning("polygon snapshot status=%s on chunk[0:%d] (attempt %d/%d)",
                               resp.status_code, min(5, len(chunk)), attempt + 1, POLYGON_RETRIES)
                time.sleep(min(10.0, POLYGON_BACKOFF_SEC * (attempt + 1) + 1))
                continue

            if resp.status_code != 200:
                LOGGER.warning("polygon snapshot status=%s body=%s", resp.status_code, resp.text[:200])
                break

            try:
                data = resp.json()
            except Exception as exc:
                LOGGER.warning("polygon snapshot JSON decode failed: %s", exc)
                break

            tickers_obj = data.get("tickers") or []
            for t in tickers_obj:
                sym = _norm_sym(t.get("ticker"))
                if not sym:
                    continue
                last_trade = t.get("lastTrade") or {}
                day = t.get("day") or {}
                prev = t.get("prevDay") or {}
                last = _f(last_trade.get("p")) or _f(day.get("c")) or _f(prev.get("c"))
                prior_close = _f(prev.get("c"))
                volume = _f(day.get("v"))
                ts_ns = _f(last_trade.get("t"))
                as_of = None
                if ts_ns and ts_ns > 0:
                    try:
                        as_of = (
                            datetime.fromtimestamp(ts_ns / 1e9, UTC)
                            .isoformat()
                            .replace("+00:00", "Z")
                        )
                    except Exception:
                        as_of = None
                if last is None or last <= 0:
                    continue
                out[sym] = {
                    "last": last,
                    "prior_close": prior_close,
                    "volume_so_far": volume,
                    "as_of": as_of,
                    "stale": False,
                    "source": "polygon_snapshot",
                }
            break  # success or non-retryable; advance to next chunk

    return out


# ?? Source 3: yfinance fast_info ?????????????????????????????????????????


def fetch_yfinance_fast_info(missing: list[str]) -> dict[str, dict[str, Any]]:
    if not missing:
        return {}
    try:
        import yfinance as yf  # type: ignore
    except Exception as exc:  # pragma: no cover - import-time failure
        LOGGER.warning("yfinance unavailable: %s", exc)
        return {}

    out: dict[str, dict[str, Any]] = {}
    for sym in missing:
        try:
            t = yf.Ticker(sym)
            fi = t.fast_info
            last = _f(getattr(fi, "last_price", None))
            prev = _f(getattr(fi, "previous_close", None))
            if last is None or last <= 0:
                continue
            out[sym] = {
                "last": last,
                "prior_close": prev,
                "volume_so_far": _f(getattr(fi, "last_volume", None)),
                "as_of": None,
                "stale": False,
                "source": "yfinance_fast_info",
            }
        except Exception as exc:
            LOGGER.debug("yfinance fast_info failed for %s: %s", sym, exc)
            continue
    return out


# ?? Pipeline ?????????????????????????????????????????????????????????????


def merge_sources(
    underlyings: list[str],
    *,
    polygon: dict[str, dict[str, Any]],
    options_cache: dict[str, dict[str, Any]],
    metrics_priors: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for sym in underlyings:
        prior_meta = metrics_priors.get(sym, {})
        prior_close = prior_meta.get("prior_close")
        prior_date = prior_meta.get("prior_close_date")

        last: float | None = None
        as_of: str | None = None
        source: str | None = None
        stale = False
        volume_so_far: float | None = None

        if sym in polygon:
            row = polygon[sym]
            last = row.get("last")
            volume_so_far = row.get("volume_so_far")
            as_of = row.get("as_of")
            source = "polygon_snapshot"
            stale = bool(row.get("stale"))
            if row.get("prior_close"):
                prior_close = row["prior_close"]
        elif sym in options_cache:
            row = options_cache[sym]
            last = row.get("last")
            as_of = row.get("as_of")
            source = "options_cache"
            stale = bool(row.get("stale"))

        if last is None or last <= 0:
            continue
        ret = None
        # A stale options_cache spot can be days old, so the implied
        # "return vs today's prior close" is meaningless. Suppress to avoid
        # poisoning the intraday flow estimate; downstream surfaces this row
        # but skips it for aggregation.
        if prior_close and prior_close > 0 and not stale:
            ret = last / prior_close - 1.0
        out[sym] = {
            "last": last,
            "prior_close": prior_close,
            "prior_close_date": prior_date,
            "return_d1_so_far": ret,
            "volume_so_far": volume_so_far,
            "as_of": as_of,
            "source": source,
            "stale": stale,
        }
    return out


def build_payload(
    underlyings: list[str],
    by_und: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    counts = {"polygon_snapshot": 0, "options_cache": 0, "yfinance_fast_info": 0}
    stale = 0
    with_return = 0
    with_volume = 0
    for v in by_und.values():
        s = v.get("source")
        if s in counts:
            counts[s] += 1
        if v.get("stale"):
            stale += 1
        if v.get("return_d1_so_far") is not None:
            with_return += 1
        if v.get("volume_so_far") is not None:
            with_volume += 1
    return {
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "n_underlyings_universe": len(underlyings),
        "n_underlyings_priced": len(by_und),
        "n_with_return": with_return,
        "n_with_volume": with_volume,
        "n_stale": stale,
        "sources": counts,
        "by_underlying": {k: by_und[k] for k in sorted(by_und.keys())},
    }


def write_payload(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(_json_clean(payload), separators=(",", ":"), allow_nan=False, sort_keys=True),
        encoding="utf-8",
    )
    tmp.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh underlying intraday spots")
    parser.add_argument("--output", type=Path, default=OUTPUT_JSON)
    parser.add_argument("--universe", type=Path, default=UNIVERSE_CSV)
    parser.add_argument("--metrics-parquet", type=Path, default=METRICS_PARQUET)
    parser.add_argument("--options-cache", type=Path, default=OPTIONS_CACHE)
    parser.add_argument(
        "--max-yfinance",
        type=int,
        default=DEFAULT_MAX_YFINANCE,
        help="Cap yfinance fallback per-symbol fetches (0 disables yfinance entirely).",
    )
    parser.add_argument("--skip-polygon", action="store_true", help="Skip Polygon snapshot pull (debug / offline).")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(levelname)s:%(name)s:%(message)s",
    )

    underlyings, ticker_to_und = load_universe_underlyings(args.universe)
    if not underlyings:
        LOGGER.error("No underlyings loaded from %s", args.universe)
        return 1
    LOGGER.info("Universe: %d underlyings (from %d funds)", len(underlyings), len(ticker_to_und))

    metrics_priors = load_prior_closes_from_metrics(args.metrics_parquet, ticker_to_und=ticker_to_und)
    LOGGER.info("Prior closes from metrics for %d underlyings", len(metrics_priors))

    options_spots = load_options_cache_spots(args.options_cache)
    LOGGER.info("Options-cache spots for %d underlyings", len(options_spots))

    polygon_data: dict[str, dict[str, Any]] = {}
    if not args.skip_polygon:
        api_key = os.environ.get("POLYGON_API_KEY") or os.environ.get("POLYGON_IO_API_KEY") or ""
        if api_key:
            polygon_data = fetch_polygon_snapshots(underlyings, api_key)
            LOGGER.info("Polygon snapshots for %d/%d underlyings", len(polygon_data), len(underlyings))
        else:
            LOGGER.warning("POLYGON_API_KEY missing; relying on options_cache + yfinance fallbacks")

    by_und = merge_sources(
        underlyings,
        polygon=polygon_data,
        options_cache=options_spots,
        metrics_priors=metrics_priors,
    )

    if args.max_yfinance > 0:
        missing = [s for s in underlyings if s not in by_und]
        if missing:
            sample = missing[: int(args.max_yfinance)]
            LOGGER.info("yfinance fast_info fallback for %d/%d missing underlyings", len(sample), len(missing))
            yf_data = fetch_yfinance_fast_info(sample)
            for sym, row in yf_data.items():
                last = row.get("last")
                prior = row.get("prior_close") or metrics_priors.get(sym, {}).get("prior_close")
                ret = (last / prior - 1.0) if last and prior and prior > 0 else None
                by_und[sym] = {
                    "last": last,
                    "prior_close": prior,
                    "prior_close_date": metrics_priors.get(sym, {}).get("prior_close_date"),
                    "return_d1_so_far": ret,
                    "volume_so_far": row.get("volume_so_far"),
                    "as_of": row.get("as_of"),
                    "source": row.get("source") or "yfinance_fast_info",
                    "stale": bool(row.get("stale")),
                }

    payload = build_payload(underlyings, by_und)
    write_payload(args.output, payload)
    LOGGER.info(
        "wrote %s priced=%d/%d sources=%s with_return=%d with_volume=%d",
        args.output,
        payload["n_underlyings_priced"],
        payload["n_underlyings_universe"],
        payload["sources"],
        payload["n_with_return"],
        payload["n_with_volume"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

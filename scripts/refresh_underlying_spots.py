#!/usr/bin/env python3
"""Pull intraday underlying spot + prior close for the LETF universe.

The output (``data/underlying_intraday_spot.json``) feeds
``scripts/build_letf_intraday_flows.py``, which estimates today's pending
LETF close rebalance ahead of the print.

Source priority (graceful fallback):

1. **Tradier quotes** -- batched ``/markets/quotes`` (same path as
   ``build_data.py`` options refresh). Primary live spot source in CI.
2. **Polygon last trade** -- per-symbol ``/v2/last/trade/{sym}`` (entitled on
   stock plans that lack the batch snapshot endpoint).
3. **``data/options_cache.json``** -- only when the row is **not** marked
   stale. Stale cache rows are ignored entirely (they poison return math).
4. **``yfinance.Ticker.fast_info``** -- per-symbol fallback; capped via
   ``--max-yfinance``.

Prior-close comes from ``etf_metrics_daily.underlying_adj_close`` (issuer
close, the EOD pipeline's ground truth). Polygon ``prevDay`` is not used
because the batch snapshot endpoint is not entitled on our plan.

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
from collections import deque
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import pandas as pd

LOGGER = logging.getLogger("underlying_spots")

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
UNIVERSE_CSV = DATA_DIR / "etf_screened_today.csv"
OPTIONS_CACHE = DATA_DIR / "options_cache.json"
METRICS_PARQUET = DATA_DIR / "etf_metrics_daily.parquet"
OUTPUT_JSON = DATA_DIR / "underlying_intraday_spot.json"

TRADIER_TOKEN = os.environ.get("TRADIER_TOKEN", "").strip()
TRADIER_BASE_URL = os.environ.get("TRADIER_BASE_URL", "https://api.tradier.com/v1").strip().rstrip("/")
TRADIER_SPOT_MAX_SYMBOLS_PER_BATCH = int(os.environ.get("TRADIER_SPOT_MAX_SYMBOLS_PER_BATCH", "200"))
TRADIER_SPOT_MAX_REQUESTS = int(os.environ.get("TRADIER_SPOT_MAX_REQUESTS", "30"))

POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY") or os.environ.get("POLYGON_IO_API_KEY") or ""
POLYGON_MAX_REQUESTS_PER_MINUTE = int(os.environ.get("POLYGON_MAX_REQUESTS_PER_MINUTE", "25"))
POLYGON_MAX_TOTAL_REQUESTS = int(os.environ.get("POLYGON_MAX_TOTAL_REQUESTS", "250"))
POLYGON_RETRY_MAX_429 = int(os.environ.get("POLYGON_RETRY_MAX_429", "1"))
POLYGON_TIMEOUT_SEC = 15.0

DEFAULT_MAX_YFINANCE = 60

SOURCE_KEYS = ("tradier_spot", "polygon_last_trade", "options_cache", "yfinance_fast_info")


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


def _chunked(items: list[str], size: int) -> list[list[str]]:
    n = max(1, int(size))
    return [items[i:i + n] for i in range(0, len(items), n)]


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
    """Pull most-recent ``underlying_adj_close`` per underlying from the metrics panel."""
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


# ?? Source 3: options_cache.json ????????????????????????????????????????


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
        if bool(entry.get("stale")):
            continue
        spot = _f(entry.get("spot"))
        if spot is None or spot <= 0:
            continue
        out[_norm_sym(sym)] = {
            "last": spot,
            "as_of": entry.get("updated_at"),
            "stale": False,
            "source": "options_cache",
        }
    return out


# ?? Source 1: Tradier batched quotes ????????????????????????????????????


def fetch_tradier_spots(tickers: list[str], *, token: str = TRADIER_TOKEN) -> dict[str, dict[str, Any]]:
    if not token or not tickers:
        return {}
    try:
        import requests  # type: ignore
    except ImportError:  # pragma: no cover
        LOGGER.warning("requests not installed; skipping Tradier spot pull")
        return {}

    session = requests.Session()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": "etf-dashboard-intraday-spots/1.0",
    }
    out: dict[str, dict[str, Any]] = {}
    request_count = 0

    for batch in _chunked(tickers, TRADIER_SPOT_MAX_SYMBOLS_PER_BATCH):
        if request_count >= max(1, TRADIER_SPOT_MAX_REQUESTS):
            LOGGER.warning(
                "tradier capped at %d requests; %d symbols may use polygon fallback",
                TRADIER_SPOT_MAX_REQUESTS,
                len(tickers) - len(out),
            )
            break
        request_count += 1
        try:
            resp = session.post(
                f"{TRADIER_BASE_URL}/markets/quotes",
                headers=headers,
                data={"symbols": ",".join(batch)},
                timeout=20,
            )
        except Exception as exc:
            LOGGER.warning("tradier quotes exception: %s", exc)
            continue
        if not resp.ok:
            msg = ""
            try:
                payload = resp.json() or {}
                msg = payload.get("error") or payload.get("message") or ""
            except Exception:
                msg = ""
            LOGGER.warning("tradier quotes HTTP %s%s", resp.status_code, f" {msg}" if msg else "")
            continue
        payload = resp.json() or {}
        quotes = ((payload.get("quotes") or {}).get("quote")) or []
        if isinstance(quotes, dict):
            quotes = [quotes]
        for q in quotes:
            sym = _norm_sym(q.get("symbol"))
            if not sym:
                continue
            last = (
                _f(q.get("last"))
                or _f(q.get("close"))
                or _f(q.get("bid"))
                or _f(q.get("ask"))
            )
            if last is None or last <= 0:
                continue
            out[sym] = {
                "last": last,
                "as_of": None,
                "stale": False,
                "source": "tradier_spot",
            }
    return out


# ?? Source 2: Polygon last/trade ?????????????????????????????????????????


class _PolygonRateLimiter:
    def __init__(self) -> None:
        self.request_timestamps: deque[float] = deque()
        self.total_requests = 0

    def _rate_limit(self) -> None:
        if POLYGON_MAX_REQUESTS_PER_MINUTE <= 0:
            return
        now = time.monotonic()
        while self.request_timestamps and (now - self.request_timestamps[0]) >= 60.0:
            self.request_timestamps.popleft()
        if len(self.request_timestamps) < POLYGON_MAX_REQUESTS_PER_MINUTE:
            return
        wait_s = max(0.01, 60.0 - (now - self.request_timestamps[0]))
        time.sleep(wait_s)

    def get(self, session: Any, url: str) -> tuple[Any | None, str | None]:
        if POLYGON_MAX_TOTAL_REQUESTS > 0 and self.total_requests >= POLYGON_MAX_TOTAL_REQUESTS:
            return None, f"polygon request budget exceeded ({POLYGON_MAX_TOTAL_REQUESTS})"
        retries_left = max(0, POLYGON_RETRY_MAX_429)
        while True:
            self._rate_limit()
            try:
                resp = session.get(url, timeout=POLYGON_TIMEOUT_SEC)
            except Exception:
                return None, "polygon request exception"
            self.total_requests += 1
            self.request_timestamps.append(time.monotonic())
            if resp.status_code != 429:
                return resp, None
            if retries_left <= 0:
                return resp, "HTTP 429 rate limited"
            retry_after = resp.headers.get("Retry-After", "").strip()
            try:
                wait_s = float(retry_after) if retry_after else 2.0
            except ValueError:
                wait_s = 2.0
            time.sleep(min(max(wait_s, 0.5), 15.0))
            retries_left -= 1


def fetch_polygon_last_spots(
    tickers: list[str],
    api_key: str,
    *,
    limiter: _PolygonRateLimiter | None = None,
) -> dict[str, dict[str, Any]]:
    if not api_key or not tickers:
        return {}
    try:
        import requests  # type: ignore
    except ImportError:  # pragma: no cover
        LOGGER.warning("requests not installed; skipping Polygon last/trade")
        return {}

    session = requests.Session()
    session.headers.update({
        "User-Agent": "etf-dashboard-intraday-spots/1.0",
        "Accept": "application/json",
    })
    rl = limiter or _PolygonRateLimiter()
    out: dict[str, dict[str, Any]] = {}

    for sym in tickers:
        polygon_sym = _polygon_sym(sym)
        url = f"https://api.polygon.io/v2/last/trade/{polygon_sym}?{urlencode({'apiKey': api_key})}"
        resp, req_err = rl.get(session, url)
        if req_err and resp is None:
            if "budget exceeded" in (req_err or ""):
                LOGGER.warning(req_err)
                break
            continue
        if resp is None or not resp.ok:
            continue
        try:
            payload = resp.json() or {}
        except Exception:
            continue
        results = payload.get("results") or {}
        last = _f(results.get("p"))
        if last is None or last <= 0:
            continue
        ts_ns = _f(results.get("t"))
        as_of = None
        if ts_ns and ts_ns > 0:
            try:
                as_of = datetime.fromtimestamp(ts_ns / 1e9, UTC).isoformat().replace("+00:00", "Z")
            except Exception:
                as_of = None
        out[sym] = {
            "last": last,
            "as_of": as_of,
            "stale": False,
            "source": "polygon_last_trade",
        }
    return out


# ?? Source 4: yfinance fast_info ?????????????????????????????????????????


def fetch_yfinance_fast_info(missing: list[str]) -> dict[str, dict[str, Any]]:
    if not missing:
        return {}
    try:
        import yfinance as yf  # type: ignore
    except Exception as exc:  # pragma: no cover
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
    tradier: dict[str, dict[str, Any]],
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

        for bucket in (tradier, polygon, options_cache):
            if sym not in bucket:
                continue
            row = bucket[sym]
            last = row.get("last")
            as_of = row.get("as_of")
            source = str(row.get("source") or "")
            stale = bool(row.get("stale"))
            volume_so_far = row.get("volume_so_far")
            break

        if last is None or last <= 0:
            continue
        ret = None
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
    counts = {k: 0 for k in SOURCE_KEYS}
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
    parser.add_argument("--skip-tradier", action="store_true", help="Skip Tradier quotes (debug / offline).")
    parser.add_argument("--skip-polygon", action="store_true", help="Skip Polygon last/trade pull (debug / offline).")
    parser.add_argument(
        "--min-with-return",
        type=int,
        default=0,
        help="Exit non-zero when n_with_return is below this threshold (CI guard during RTH).",
    )
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
    LOGGER.info("Fresh options-cache spots for %d underlyings", len(options_spots))

    tradier_data: dict[str, dict[str, Any]] = {}
    if not args.skip_tradier:
        if TRADIER_TOKEN:
            tradier_data = fetch_tradier_spots(underlyings)
            LOGGER.info("Tradier spots for %d/%d underlyings", len(tradier_data), len(underlyings))
        else:
            LOGGER.warning("TRADIER_TOKEN missing; relying on polygon + options_cache + yfinance")

    polygon_data: dict[str, dict[str, Any]] = {}
    if not args.skip_polygon:
        api_key = POLYGON_API_KEY
        if api_key:
            missing_for_polygon = [s for s in underlyings if s not in tradier_data]
            if missing_for_polygon:
                polygon_data = fetch_polygon_last_spots(missing_for_polygon, api_key)
            LOGGER.info(
                "Polygon last/trade for %d/%d underlyings (%d after Tradier)",
                len(polygon_data),
                len(underlyings),
                len(tradier_data),
            )
        else:
            LOGGER.warning("POLYGON_API_KEY missing; relying on tradier + options_cache + yfinance")

    by_und = merge_sources(
        underlyings,
        tradier=tradier_data,
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

    min_with_return = int(args.min_with_return)
    if min_with_return > 0 and payload["n_with_return"] < min_with_return:
        LOGGER.error(
            "n_with_return=%d below --min-with-return=%d (priced=%d)",
            payload["n_with_return"],
            min_with_return,
            payload["n_underlyings_priced"],
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

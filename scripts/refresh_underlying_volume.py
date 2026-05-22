#!/usr/bin/env python3
"""Refresh intraday cumulative underlying share volume (decoupled from spot prices).

Writes ``data/underlying_intraday_volume.json`` for
``build_letf_intraday_flows.py``. Price refresh stays in
``refresh_underlying_spots.py`` so volume API failures do not block live spots.

Source priority:
  1. Tradier ``/markets/quotes`` ``volume`` field (batched, same as spots).
  2. Polygon ``/v2/aggs/ticker/{sym}/range/1/day/{today}/{today}`` (day bar ``v``).
  3. ``yfinance.Ticker.fast_info.last_volume`` (capped via ``--max-yfinance``).
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import time
from collections import deque
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import pandas as pd

LOGGER = logging.getLogger("underlying_volume")

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
UNIVERSE_CSV = DATA_DIR / "etf_screened_today.csv"
OUTPUT_JSON = DATA_DIR / "underlying_intraday_volume.json"
FLOW_LATEST = DATA_DIR / "letf_rebalance_flows_intraday_latest.json"

TRADIER_TOKEN = os.environ.get("TRADIER_TOKEN", "").strip()
TRADIER_BASE_URL = os.environ.get("TRADIER_BASE_URL", "https://api.tradier.com/v1").strip().rstrip("/")
TRADIER_MAX_SYMBOLS_PER_BATCH = int(os.environ.get("TRADIER_SPOT_MAX_SYMBOLS_PER_BATCH", "200"))
TRADIER_MAX_REQUESTS = int(os.environ.get("TRADIER_SPOT_MAX_REQUESTS", "30"))

POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY") or os.environ.get("POLYGON_IO_API_KEY") or ""
POLYGON_MAX_REQUESTS_PER_MINUTE = int(os.environ.get("POLYGON_MAX_REQUESTS_PER_MINUTE", "25"))
POLYGON_MAX_TOTAL_REQUESTS = int(os.environ.get("POLYGON_VOLUME_MAX_TOTAL_REQUESTS", "120"))

DEFAULT_MAX_YFINANCE = 40
TIER0_PCT_ADV = float(os.environ.get("VOLUME_TIER0_PCT_ADV", "0.005"))


def _f(v: object) -> float | None:
    try:
        x = float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _norm_sym(v: object) -> str:
    return str(v or "").strip().upper().replace(".", "-")


def _chunked(items: list[str], size: int) -> list[list[str]]:
    n = max(1, size)
    return [items[i:i + n] for i in range(0, len(items), n)]


def _json_clean(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _json_clean(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_clean(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj


def load_universe_underlyings(universe_path: Path) -> list[str]:
    if not universe_path.exists():
        return []
    df = pd.read_csv(universe_path)
    col = "Underlying" if "Underlying" in df.columns else "underlying"
    if col not in df.columns:
        return []
    syms = {_norm_sym(x) for x in df[col].dropna().tolist() if str(x).strip()}
    return sorted(s for s in syms if s)


def load_tier0_underlyings(flow_path: Path = FLOW_LATEST, *, pct_adv: float = TIER0_PCT_ADV) -> set[str]:
    """Underlyings where |est close| / ADV exceeds threshold (optional fetch subset)."""
    if not flow_path.exists():
        return set()
    try:
        payload = json.loads(flow_path.read_text(encoding="utf-8"))
    except Exception:
        return set()
    out: set[str] = set()
    for und, row in (payload.get("by_underlying") or {}).items():
        est = _f(row.get("estimated_net_close_rebalance_dollars"))
        adv = _f(row.get("underlying_dollar_adv_20d")) or _f(row.get("underlying_dollar_median_adv_20d"))
        if est is None or not adv or adv <= 0:
            continue
        if abs(est) / adv >= pct_adv:
            out.add(_norm_sym(und))
    return out


def fetch_tradier_volumes(tickers: list[str], token: str) -> dict[str, dict[str, Any]]:
    if not token or not tickers:
        return {}
    try:
        import requests  # type: ignore
    except ImportError:
        LOGGER.warning("requests not installed; skipping Tradier volume pull")
        return {}

    session = requests.Session()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": "etf-dashboard-intraday-volume/1.0",
    }
    out: dict[str, dict[str, Any]] = {}
    request_count = 0
    now_iso = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    for batch in _chunked(tickers, TRADIER_MAX_SYMBOLS_PER_BATCH):
        if request_count >= max(1, TRADIER_MAX_REQUESTS):
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
            continue
        payload = resp.json() or {}
        quotes = ((payload.get("quotes") or {}).get("quote")) or []
        if isinstance(quotes, dict):
            quotes = [quotes]
        for q in quotes:
            sym = _norm_sym(q.get("symbol"))
            if not sym:
                continue
            vol = _f(q.get("volume"))
            if vol is None or vol < 0:
                continue
            out[sym] = {
                "volume_so_far": vol,
                "as_of": now_iso,
                "source": "tradier_volume",
            }
    return out


class _PolygonRateLimiter:
    def __init__(self) -> None:
        self.request_timestamps: deque[float] = deque()
        self.total_requests = 0

    def get(self, session: Any, url: str) -> Any | None:
        if POLYGON_MAX_TOTAL_REQUESTS > 0 and self.total_requests >= POLYGON_MAX_TOTAL_REQUESTS:
            return None
        if POLYGON_MAX_REQUESTS_PER_MINUTE > 0:
            now = time.monotonic()
            while self.request_timestamps and (now - self.request_timestamps[0]) >= 60.0:
                self.request_timestamps.popleft()
            if len(self.request_timestamps) >= POLYGON_MAX_REQUESTS_PER_MINUTE:
                time.sleep(max(0.01, 60.0 - (now - self.request_timestamps[0])))
        try:
            resp = session.get(url, timeout=20)
        except Exception:
            return None
        self.total_requests += 1
        self.request_timestamps.append(time.monotonic())
        return resp if resp.ok else None


def _polygon_sym(sym: str) -> str:
    return sym.replace("-", ".")


def fetch_polygon_day_volumes(
    tickers: list[str],
    api_key: str,
    *,
    session_date: date | None = None,
) -> dict[str, dict[str, Any]]:
    if not api_key or not tickers:
        return {}
    try:
        import requests  # type: ignore
    except ImportError:
        return {}

    day = (session_date or datetime.now(UTC).date()).isoformat()
    session = requests.Session()
    session.headers.update({"User-Agent": "etf-dashboard-intraday-volume/1.0"})
    rl = _PolygonRateLimiter()
    out: dict[str, dict[str, Any]] = {}

    for sym in tickers:
        polygon_sym = _polygon_sym(sym)
        q = urlencode({"adjusted": "true", "apiKey": api_key})
        url = f"https://api.polygon.io/v2/aggs/ticker/{polygon_sym}/range/1/day/{day}/{day}?{q}"
        resp = rl.get(session, url)
        if resp is None:
            continue
        try:
            payload = resp.json() or {}
        except Exception:
            continue
        results = payload.get("results") or []
        if not results:
            continue
        bar = results[0]
        vol = _f(bar.get("v"))
        if vol is None or vol < 0:
            continue
        ts_ms = _f(bar.get("t"))
        as_of = None
        if ts_ms and ts_ms > 0:
            try:
                as_of = datetime.fromtimestamp(ts_ms / 1000.0, UTC).isoformat().replace("+00:00", "Z")
            except Exception:
                as_of = None
        out[sym] = {
            "volume_so_far": vol,
            "as_of": as_of,
            "source": "polygon_day_agg",
        }
    return out


def fetch_yfinance_volumes(missing: list[str]) -> dict[str, dict[str, Any]]:
    if not missing:
        return {}
    try:
        import yfinance as yf  # type: ignore
    except Exception as exc:
        LOGGER.warning("yfinance unavailable: %s", exc)
        return {}

    out: dict[str, dict[str, Any]] = {}
    now_iso = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    for sym in missing:
        try:
            fi = yf.Ticker(sym).fast_info
            vol = _f(getattr(fi, "last_volume", None))
            if vol is None or vol < 0:
                continue
            out[sym] = {
                "volume_so_far": vol,
                "as_of": now_iso,
                "source": "yfinance_last_volume",
            }
        except Exception:
            continue
    return out


def merge_volume_sources(
    underlyings: list[str],
    *,
    tradier: dict[str, dict[str, Any]],
    polygon: dict[str, dict[str, Any]],
    yfinance: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for sym in underlyings:
        for bucket in (tradier, polygon, yfinance):
            if sym not in bucket:
                continue
            row = bucket[sym]
            vol = _f(row.get("volume_so_far"))
            if vol is None:
                continue
            out[sym] = {
                "volume_so_far": vol,
                "as_of": row.get("as_of"),
                "source": row.get("source"),
            }
            break
    return out


def build_payload(underlyings: list[str], by_und: dict[str, dict[str, Any]]) -> dict[str, Any]:
    sources: dict[str, int] = {}
    for row in by_und.values():
        s = str(row.get("source") or "unknown")
        sources[s] = sources.get(s, 0) + 1
    return {
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "trading_date": datetime.now(UTC).date().isoformat(),
        "n_underlyings_universe": len(underlyings),
        "n_underlyings_with_volume": len(by_und),
        "sources": sources,
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
    parser = argparse.ArgumentParser(description="Refresh underlying intraday cumulative volume")
    parser.add_argument("--output", type=Path, default=OUTPUT_JSON)
    parser.add_argument("--universe", type=Path, default=UNIVERSE_CSV)
    parser.add_argument("--flow-latest", type=Path, default=FLOW_LATEST)
    parser.add_argument("--max-yfinance", type=int, default=DEFAULT_MAX_YFINANCE)
    parser.add_argument("--skip-tradier", action="store_true")
    parser.add_argument("--skip-polygon", action="store_true")
    parser.add_argument(
        "--tier0-only",
        action="store_true",
        help="Only fetch volume for underlyings above VOLUME_TIER0_PCT_ADV in latest flow.",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(levelname)s:%(name)s:%(message)s",
    )

    underlyings = load_universe_underlyings(args.universe)
    if not underlyings:
        LOGGER.error("No underlyings from %s", args.universe)
        return 1

    if args.tier0_only:
        tier0 = load_tier0_underlyings(args.flow_latest)
        if tier0:
            underlyings = [s for s in underlyings if s in tier0]
            LOGGER.info("Tier-0 filter: %d underlyings", len(underlyings))

    tradier_data: dict[str, dict[str, Any]] = {}
    if not args.skip_tradier and TRADIER_TOKEN:
        tradier_data = fetch_tradier_volumes(underlyings, TRADIER_TOKEN)
        LOGGER.info("Tradier volume for %d/%d", len(tradier_data), len(underlyings))
    elif not args.skip_tradier:
        LOGGER.warning("TRADIER_TOKEN missing")

    polygon_data: dict[str, dict[str, Any]] = {}
    if not args.skip_polygon and POLYGON_API_KEY:
        missing_poly = [s for s in underlyings if s not in tradier_data]
        if missing_poly:
            polygon_data = fetch_polygon_day_volumes(missing_poly, POLYGON_API_KEY)
        LOGGER.info("Polygon day agg volume for %d symbols", len(polygon_data))

    yf_data: dict[str, dict[str, Any]] = {}
    if int(args.max_yfinance) > 0:
        missing_yf = [s for s in underlyings if s not in tradier_data and s not in polygon_data]
        if missing_yf:
            sample = missing_yf[: int(args.max_yfinance)]
            yf_data = fetch_yfinance_volumes(sample)
            LOGGER.info("yfinance volume for %d/%d missing", len(yf_data), len(missing_yf))

    by_und = merge_volume_sources(
        underlyings,
        tradier=tradier_data,
        polygon=polygon_data,
        yfinance=yf_data,
    )
    payload = build_payload(underlyings, by_und)
    write_payload(args.output, payload)
    LOGGER.info(
        "wrote %s with_volume=%d/%d sources=%s",
        args.output,
        payload["n_underlyings_with_volume"],
        payload["n_underlyings_universe"],
        payload["sources"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

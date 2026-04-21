#!/usr/bin/env python3
"""Ingest corporate-action events + filtered news headlines for the dashboard universe.

Writes two artifacts, both consumed by the frontend's "News" tab:

    data/corporate_actions.json   -- structured events (splits, delistings, symbol
                                      changes, mergers/reorgs) sourced from Polygon
                                      reference endpoints; authoritative, pinned at
                                      the top of the News tab.
    data/etf_news.json            -- rolling headline feed derived from Polygon
                                      /v2/reference/news, classified by regex and
                                      filtered to corporate-action categories only
                                      (dividends/distributions explicitly excluded).

Design choice: every phase uses a **bulk** Polygon endpoint (no per-ticker loop) and
filters to our universe client-side.  This collapses a free-tier 5-req/min budget
from ~900 ticker-by-ticker calls to ~15-30 paginated bulk calls, which comfortably
fits the GitHub Actions timeout.

Pipeline phases:
    phase_1_splits          -- Polygon /v3/reference/splits bulk sweep (date-windowed),
                               filtered to our universe client-side
    phase_2_delistings      -- Polygon /v3/reference/tickers?active=false bulk sweep,
                               intersected with the **ever-known** universe so
                               already-delisted ETFs (which are gone from the
                               screener) still match
    phase_3_news            -- Polygon /v2/reference/news bulk sweep (date-windowed),
                               capped by page budget; each article's tickers[] array is
                               intersected with our universe and regex-classified
    phase_4_symbol_changes  -- Per-ticker /vX/reference/tickers/{t}/events?types=
                               ticker_change sweep for current universe +
                               underlyings + ever-known universe.  Caches responses
                               per ticker on disk with a configurable TTL so the
                               daily sweep is cheap even on free-tier Polygon.
                               Emits a ticker_alias_map.json side-output.
    phase_5_google_news     -- Google News RSS sweep (no API key) for issuer
                               press-release coverage of *announced but not yet
                               executed* liquidations / renames / splits.
                               Synthesizes ``status="pending"`` CorporateEvents
                               whose ids match Polygon's format, so once Polygon
                               confirms the event the two records merge cleanly.
    merge / dedupe          -- status-aware: executed beats pending, missing fields
                               back-fill across records; attach linked_event_id,
                               collapse duplicate headlines
    persist                 -- write corporate_actions.json, etf_news.json,
                               polygon_ticker_events_cache.json,
                               ticker_alias_map.json

Run manually::

    python scripts/ingest_corporate_actions.py --symbols MSTY,CONY,APLZ,SPY

Or via the scheduled workflow (``update-corporate-actions.yml``) which passes no
``--symbols`` to cover the full screened universe.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests


LOGGER = logging.getLogger("corporate_actions_ingest")

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
UNIVERSE_CSV = DATA_DIR / "etf_screened_today.csv"
# Historic metrics: every ticker we've ever tracked (NAV/AUM/shares).  Used to
# build an "ever-known" universe for delistings, since a delisted ETF drops out
# of `etf_screened_today.csv` before the next corp-actions sweep and would
# otherwise be filtered out of the Polygon intersection.
METRICS_PARQUET = DATA_DIR / "etf_metrics_daily.parquet"
METRICS_CSV = DATA_DIR / "etf_metrics_daily.csv"
OUT_EVENTS = DATA_DIR / "corporate_actions.json"
OUT_NEWS = DATA_DIR / "etf_news.json"
# Phase 4: per-ticker Polygon /vX/reference/tickers/{t}/events cache (TTL in
# days, default 14) so the daily run doesn't re-query every ticker.  The cache
# is checked into git alongside the other artifacts — its size is bounded by
# the ever-known universe (~450 tickers × a few events each ≈ tens of KB).
TICKER_EVENTS_CACHE_PATH = DATA_DIR / "polygon_ticker_events_cache.json"
# Stable alias map derived from accumulated symbol_change events: {old: {"new":..., "effective_date":...}}.
# Downstream consumers (universe normalizers, chart deep-links, future chains
# across multiple renames) can read this without re-parsing every event record.
TICKER_ALIAS_MAP_PATH = DATA_DIR / "ticker_alias_map.json"

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY") or os.getenv("POLYGON_IO_API_KEY") or ""
HTTP_TIMEOUT_SEC = int(os.getenv("ETF_METRICS_HTTP_TIMEOUT_SEC", "30"))
HTTP_RETRY_TOTAL = int(os.getenv("ETF_METRICS_HTTP_RETRY_TOTAL", "4"))
NEWS_WINDOW_DAYS = int(os.getenv("CORP_ACTIONS_NEWS_WINDOW_DAYS", "60"))
SPLITS_LOOKBACK_DAYS = int(os.getenv("CORP_ACTIONS_SPLITS_LOOKBACK_DAYS", "365"))
NEWS_PAGE_LIMIT = int(os.getenv("CORP_ACTIONS_NEWS_PAGE_LIMIT", "1000"))
# How many pages of bulk news to fetch before stopping.  At 1000 articles/page, 25
# pages covers ~25k articles which is typically more than 60 days of market news.
NEWS_MAX_PAGES = int(os.getenv("CORP_ACTIONS_NEWS_MAX_PAGES", "25"))
SPLITS_MAX_PAGES = int(os.getenv("CORP_ACTIONS_SPLITS_MAX_PAGES", "10"))
DELISTINGS_MAX_PAGES = int(os.getenv("CORP_ACTIONS_DELISTINGS_MAX_PAGES", "10"))
# Phase 4 (symbol changes) config.  Lookback is long (default 540d) so renames
# that happened before the next refresh still surface during the initial
# backfill period.  Cache TTL controls per-ticker re-query cadence.
SYMBOL_CHANGE_LOOKBACK_DAYS = int(os.getenv("CORP_ACTIONS_SYMBOL_CHANGE_LOOKBACK_DAYS", "540"))
TICKER_EVENTS_CACHE_TTL_DAYS = int(os.getenv("CORP_ACTIONS_TICKER_EVENTS_CACHE_TTL_DAYS", "14"))
# Hard ceiling on per-ticker calls we'll make in one run.  On the free tier
# (5/min) an unbounded initial sweep would take 100+ min and blow the workflow
# timeout.  Cache rollover across multiple runs lets us backfill over a few days.
SYMBOL_CHANGE_MAX_CALLS_PER_RUN = int(os.getenv("CORP_ACTIONS_SYMBOL_CHANGE_MAX_CALLS", "120"))
# Phase 5 (Google News RSS) config.  Independent of Polygon rate limits.  Set
# CORP_ACTIONS_ENABLE_GOOGLE_NEWS=0 to skip (e.g. if Google throttles a runner).
ENABLE_GOOGLE_NEWS = os.getenv("CORP_ACTIONS_ENABLE_GOOGLE_NEWS", "1") not in {"0", "false", "False", ""}
GOOGLE_NEWS_MAX_ARTICLES_PER_QUERY = int(os.getenv("CORP_ACTIONS_GOOGLE_NEWS_MAX_PER_QUERY", "100"))
GOOGLE_NEWS_WINDOW_DAYS = int(os.getenv("CORP_ACTIONS_GOOGLE_NEWS_WINDOW_DAYS", "45"))
# Polygon tier rate limit (req/min). Defaults to the free-tier cap of 5 so the
# script is safe on basic plans out-of-the-box. Bump via env for paid tiers:
#   Starter=100, Developer/Advanced=600+.
POLYGON_MAX_REQUESTS_PER_MINUTE = int(os.getenv("CORP_ACTIONS_POLYGON_REQS_PER_MIN", "5"))
# Hard cap on consecutive 429s before we bail out (prevents spending hours in a
# rate-limit wall).  Reset on any successful call.
MAX_CONSECUTIVE_RATE_LIMITS = int(os.getenv("CORP_ACTIONS_MAX_CONSEC_429", "15"))


class RateLimitExceeded(RuntimeError):
    """Raised when Polygon persistently 429s and we decide to abort the run."""


# Sliding-window of recent request timestamps (monotonic seconds) for rate limiting.
_REQUEST_TIMESTAMPS: deque[float] = deque()
# Global counter so we can short-circuit if Polygon is hard-rate-limiting us.
_CONSECUTIVE_429_STATE = {"count": 0}


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

CATEGORY_PRIORITY = {
    "delisting": 100,
    "reverse_split": 90,
    "symbol_change": 70,
    "merger": 60,
    "forward_split": 50,
    "other": 10,
}


@dataclass
class CorporateEvent:
    """Structured corporate-action event.  Surfaced in the pinned strip.

    Status semantics:

    * ``executed``  — event has happened (Polygon confirms delisting / split
      executed / ticker change completed).
    * ``pending``   — event announced but not yet executed (source is an issuer
      press-release, Google News RSS, SEC filing, etc.).  Rendered with an
      "Announced" pill on the frontend so users can plan around the upcoming
      date.  Once the event actually fires, a subsequent run promotes the row
      to ``executed`` by id-merge in :func:`dedupe_events`.

    ``prior_ticker`` is populated for ``symbol_change`` events (e.g. BITF → KEEL
    stores ``prior_ticker="BITF"`` alongside ``ticker="KEEL"``).
    """

    id: str
    type: str
    ticker: str
    execution_date: str | None = None
    announcement_date: str | None = None
    ratio_from: float | None = None
    ratio_to: float | None = None
    ratio_label: str | None = None
    status: str = "executed"
    source: str = "polygon"
    source_url: str | None = None
    bucket: str | None = None
    underlying: str | None = None
    headline: str | None = None
    summary: str | None = None
    prior_ticker: str | None = None


@dataclass
class NewsItem:
    """Classified news headline.  Surfaced in the rolling feed."""

    id: str
    tickers: list[str] = field(default_factory=list)
    category: str = "other"
    confidence: float = 0.7
    published_utc: str | None = None
    title: str | None = None
    summary: str | None = None
    url: str | None = None
    publisher: str | None = None
    image_url: str | None = None
    linked_event_id: str | None = None
    bucket: str | None = None


# ---------------------------------------------------------------------------
# Universe
# ---------------------------------------------------------------------------

def _norm(sym: object) -> str:
    return str(sym).strip().upper().replace(".", "-")


def _canonical_bucket(raw: object) -> str | None:
    """Map the universe CSV's short bucket name to the frontend's canonical one."""
    s = str(raw or "").strip().lower()
    if not s:
        return None
    mapping = {
        "bucket_1": "bucket_1_high_beta",
        "bucket_2": "bucket_2_low_beta",
        "bucket_3": "bucket_3_inverse",
        "bucket_4": "bucket_4_edge",
    }
    return mapping.get(s, s)


def load_universe() -> pd.DataFrame:
    """Return a DataFrame with columns [ticker, underlying, bucket]."""
    if not UNIVERSE_CSV.exists():
        raise FileNotFoundError(f"Universe CSV missing: {UNIVERSE_CSV}")
    df = pd.read_csv(UNIVERSE_CSV)
    if "ETF" not in df.columns:
        raise ValueError(f"Universe CSV missing ETF column: {UNIVERSE_CSV}")
    out = pd.DataFrame({
        "ticker": df["ETF"].map(_norm),
        "underlying": df.get("Underlying", pd.Series([None] * len(df))).map(
            lambda x: None if pd.isna(x) else str(x).upper()
        ),
        "bucket": df.get("bucket", pd.Series([None] * len(df))).map(_canonical_bucket),
    })
    out = out.dropna(subset=["ticker"]).drop_duplicates(subset=["ticker"])
    return out.sort_values("ticker").reset_index(drop=True)


def load_ever_known_universe(current_universe: set[str]) -> set[str]:
    """Return the union of every ticker we've ever tracked.

    Delistings-only helper: a delisted ETF is dropped from
    ``etf_screened_today.csv`` the day after it stops trading, so the current
    universe is structurally incapable of matching a Polygon delisting record
    for that ticker.  We widen the match set using:

      * the current screener (``current_universe``),
      * every ticker that has ever been written into
        ``data/etf_metrics_daily.parquet`` (historic NAV/AUM/shares),
      * every ticker already recorded as a delisting or split in the prior
        ``data/corporate_actions.json`` (so once we detect a delisting, it
        stays pinned even if Polygon later stops returning it in the
        ``active=false`` paginator).

    All sources are optional; missing files are skipped silently.  This is a
    pure superset — splits/news phases continue to use the narrow current
    universe so we don't accidentally surface noise for tickers we no longer
    track.
    """
    out: set[str] = {t for t in current_universe if t}

    parquet_path = METRICS_PARQUET
    try:
        if parquet_path.exists():
            df = pd.read_parquet(parquet_path)
            if "ticker" in df.columns:
                out.update({_norm(t) for t in df["ticker"].dropna().unique() if t})
    except Exception as e:  # noqa: BLE001 - parquet read is best-effort
        LOGGER.warning("ever-known universe: parquet load failed (%s); trying CSV fallback", e)
        try:
            if METRICS_CSV.exists():
                df = pd.read_csv(METRICS_CSV, usecols=["ticker"])
                out.update({_norm(t) for t in df["ticker"].dropna().unique() if t})
        except Exception as e2:  # noqa: BLE001
            LOGGER.warning("ever-known universe: CSV fallback also failed (%s)", e2)

    if OUT_EVENTS.exists():
        try:
            prior = json.loads(OUT_EVENTS.read_text(encoding="utf-8"))
            for ev in prior.get("events") or []:
                t = _norm(ev.get("ticker") or "")
                if t:
                    out.add(t)
        except Exception as e:  # noqa: BLE001
            LOGGER.warning("ever-known universe: could not read prior events (%s)", e)

    return out


def _load_prior_events_of_type(event_type: str) -> dict[str, CorporateEvent]:
    """Return ``{event_id: CorporateEvent}`` for every prior event matching ``event_type``.

    Shared helper for types that are considered immutable once seen (delistings,
    symbol_changes).  The merge happens upstream in ``main()`` via
    :func:`dedupe_events`, which promotes pending→executed and back-fills any
    missing fields.
    """
    out: dict[str, CorporateEvent] = {}
    if not OUT_EVENTS.exists():
        return out
    try:
        payload = json.loads(OUT_EVENTS.read_text(encoding="utf-8"))
    except Exception as e:  # noqa: BLE001
        LOGGER.warning("prior events (%s): could not read %s (%s)", event_type, OUT_EVENTS, e)
        return out
    fields = set(CorporateEvent.__dataclass_fields__.keys())
    for raw in payload.get("events") or []:
        if raw.get("type") != event_type:
            continue
        try:
            # Tolerate legacy rows written before new fields existed.
            kwargs = {k: raw.get(k) for k in fields}
            ev = CorporateEvent(**kwargs)
        except Exception:  # noqa: BLE001
            continue
        if ev.id:
            out[ev.id] = ev
    return out


def load_prior_delistings() -> dict[str, CorporateEvent]:
    """Back-compat wrapper around :func:`_load_prior_events_of_type`."""
    return _load_prior_events_of_type("delisting")


# ---------------------------------------------------------------------------
# Polygon helpers
# ---------------------------------------------------------------------------

def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "etf-dashboard-corporate-actions/1.0",
        "Accept": "application/json",
    })
    return s


def _rate_limit_polygon() -> None:
    """Token-bucket style gate: blocks until we're under POLYGON_MAX_REQUESTS_PER_MINUTE."""
    if POLYGON_MAX_REQUESTS_PER_MINUTE <= 0:
        return
    now = time.monotonic()
    while _REQUEST_TIMESTAMPS and (now - _REQUEST_TIMESTAMPS[0]) >= 60.0:
        _REQUEST_TIMESTAMPS.popleft()
    if len(_REQUEST_TIMESTAMPS) < POLYGON_MAX_REQUESTS_PER_MINUTE:
        return
    wait_s = max(0.05, 60.0 - (now - _REQUEST_TIMESTAMPS[0]) + 0.05)
    time.sleep(wait_s)


def _polygon_get(session: requests.Session, url: str, params: dict | None = None) -> dict | None:
    """GET with client-side rate limiting + Retry-After-aware 429 handling.

    Returns parsed JSON on success, or ``None`` on unrecoverable failure.  Raises
    :class:`RateLimitExceeded` if Polygon 429s us more than
    ``MAX_CONSECUTIVE_RATE_LIMITS`` calls in a row -- this is the signal to bail out
    rather than spend another hour spinning.
    """
    if not POLYGON_API_KEY:
        LOGGER.error("POLYGON_API_KEY is not set; corporate-actions ingest cannot run.")
        return None
    p = dict(params or {})
    p.setdefault("apiKey", POLYGON_API_KEY)

    last_status: int | None = None
    last_body_snippet: str | None = None
    last_exc: Exception | None = None

    for attempt in range(max(1, HTTP_RETRY_TOTAL + 1)):
        _rate_limit_polygon()
        try:
            resp = session.get(url, params=p, timeout=HTTP_TIMEOUT_SEC)
        except Exception as e:  # noqa: BLE001 - network-level, logged below
            last_exc = e
            time.sleep(0.4 * (attempt + 1))
            continue

        _REQUEST_TIMESTAMPS.append(time.monotonic())
        last_status = resp.status_code

        if resp.status_code == 429:
            _CONSECUTIVE_429_STATE["count"] += 1
            if _CONSECUTIVE_429_STATE["count"] > MAX_CONSECUTIVE_RATE_LIMITS:
                raise RateLimitExceeded(
                    f"Aborting: {MAX_CONSECUTIVE_RATE_LIMITS} consecutive HTTP 429s from Polygon. "
                    f"Your plan's rate limit is lower than CORP_ACTIONS_POLYGON_REQS_PER_MIN="
                    f"{POLYGON_MAX_REQUESTS_PER_MINUTE}. Lower that env var (free tier = 5) "
                    f"and re-run."
                )
            retry_after_hdr = resp.headers.get("Retry-After", "").strip()
            try:
                wait_s = float(retry_after_hdr) if retry_after_hdr else 2.0 * (attempt + 1)
            except ValueError:
                wait_s = 2.0 * (attempt + 1)
            wait_s = min(max(wait_s, 0.5), 20.0)
            LOGGER.debug("polygon 429 on %s; sleeping %.1fs (attempt %d/%d, consec_429=%d)",
                         url, wait_s, attempt + 1, HTTP_RETRY_TOTAL + 1,
                         _CONSECUTIVE_429_STATE["count"])
            time.sleep(wait_s)
            continue

        # Any other non-2xx gets logged and retried once, then we give up.
        if resp.status_code >= 400:
            _CONSECUTIVE_429_STATE["count"] = 0
            try:
                last_body_snippet = resp.text[:200]
            except Exception:  # noqa: BLE001
                last_body_snippet = None
            time.sleep(0.4 * (attempt + 1))
            continue

        # Success.
        _CONSECUTIVE_429_STATE["count"] = 0
        try:
            return resp.json()
        except Exception as e:  # noqa: BLE001 - bad JSON, treat as failure
            last_exc = e
            last_body_snippet = resp.text[:200] if hasattr(resp, "text") else None
            break

    LOGGER.warning(
        "polygon GET failed: url=%s status=%s exc=%s body=%s",
        url,
        last_status,
        repr(last_exc) if last_exc else None,
        (last_body_snippet or "")[:120],
    )
    return None


# ---------------------------------------------------------------------------
# Shared: bulk paginator for Polygon reference endpoints
# ---------------------------------------------------------------------------

def _bulk_paginate(
    session: requests.Session,
    initial_url: str,
    initial_params: dict,
    *,
    max_pages: int,
    phase_label: str,
) -> list[dict]:
    """Walk Polygon's `next_url` pagination up to ``max_pages`` and return all results."""
    out: list[dict] = []
    url: str | None = initial_url
    params: dict | None = dict(initial_params)
    for page_idx in range(1, max_pages + 1):
        if url is None:
            break
        payload = _polygon_get(session, url, params)
        if not payload:
            break
        results = payload.get("results") or []
        out.extend(results)
        next_url = payload.get("next_url")
        LOGGER.info(
            "  %s page %d/%d: +%d results (total=%d) next=%s",
            phase_label,
            page_idx,
            max_pages,
            len(results),
            len(out),
            "yes" if next_url else "no",
        )
        if not next_url:
            break
        # Polygon's next_url already embeds query params; re-attach apiKey.
        if "apiKey=" not in next_url:
            sep = "&" if "?" in next_url else "?"
            next_url = f"{next_url}{sep}apiKey={POLYGON_API_KEY}"
        url = next_url
        params = None
    else:
        LOGGER.warning(
            "%s: hit max_pages=%d; additional results may exist but were skipped.",
            phase_label,
            max_pages,
        )
    return out


# ---------------------------------------------------------------------------
# Phase 1 - splits (forward + reverse), bulk
# ---------------------------------------------------------------------------

def phase_1_splits(
    session: requests.Session,
    tickers: list[str],
    bucket_map: dict[str, str | None],
    underlying_map: dict[str, str | None],
) -> list[CorporateEvent]:
    """Fetch the market-wide split history over our lookback window in one sweep,
    then filter client-side to our universe.  On a 5-req/min plan this costs ~1-3
    calls vs ~440 calls for the per-ticker variant."""
    universe = set(tickers)
    cutoff = (datetime.now(UTC) - timedelta(days=SPLITS_LOOKBACK_DAYS)).date().isoformat()
    raw = _bulk_paginate(
        session,
        "https://api.polygon.io/v3/reference/splits",
        {
            "execution_date.gte": cutoff,
            "order": "desc",
            "limit": 1000,
            "sort": "execution_date",
        },
        max_pages=SPLITS_MAX_PAGES,
        phase_label="splits",
    )

    events: list[CorporateEvent] = []
    skipped_off_universe = 0
    for r in raw:
        sym = _norm(r.get("ticker"))
        if not sym or sym not in universe:
            skipped_off_universe += 1
            continue
        try:
            split_from = float(r.get("split_from") or 0)
            split_to = float(r.get("split_to") or 0)
        except (TypeError, ValueError):
            continue
        if split_from <= 0 or split_to <= 0:
            continue
        execution_date = r.get("execution_date")
        if not execution_date:
            continue
        is_reverse = split_from > split_to
        category = "reverse_split" if is_reverse else "forward_split"
        if is_reverse:
            ratio_n = split_from / split_to
            ratio_label = f"1-for-{ratio_n:g}"
        else:
            ratio_n = split_to / split_from
            ratio_label = f"{ratio_n:g}-for-1"
        headline = f"{sym} {ratio_label} {'reverse split' if is_reverse else 'stock split'} effective {execution_date}"
        events.append(
            CorporateEvent(
                id=f"polygon_split:{sym}:{execution_date}",
                type=category,
                ticker=sym,
                execution_date=execution_date,
                ratio_from=split_from,
                ratio_to=split_to,
                ratio_label=ratio_label,
                status="executed",
                source="polygon",
                source_url=f"https://polygon.io/stocks/{sym}",
                bucket=bucket_map.get(sym),
                underlying=underlying_map.get(sym),
                headline=headline,
                summary=(
                    f"Polygon reference feed reports a {ratio_label} "
                    f"{'reverse split (share consolidation)' if is_reverse else 'stock split'} "
                    f"for {sym} with execution date {execution_date}."
                ),
            )
        )
    LOGGER.info(
        "splits: kept=%d off_universe=%d (of %d raw splits across the market)",
        len(events),
        skipped_off_universe,
        len(raw),
    )
    return events


# ---------------------------------------------------------------------------
# Phase 2 - delistings
# ---------------------------------------------------------------------------

def phase_2_delistings(
    session: requests.Session,
    universe: set[str],
    bucket_map: dict[str, str | None],
    underlying_map: dict[str, str | None],
) -> list[CorporateEvent]:
    """Pull inactive tickers from Polygon and intersect with our ever-known universe.

    ``universe`` MUST be the broadened "ever-known" set (see
    :func:`load_ever_known_universe`) rather than just the current screener —
    delisted ETFs are dropped from the screener on day one, so the narrow
    current universe has a near-zero match rate for inactive tickers.

    We intentionally omit the ``type=ETF`` filter on Polygon's
    ``/v3/reference/tickers?active=false`` sweep because Polygon occasionally
    reclassifies a fund's type post-delisting (e.g. back to equity or to ``CS``
    when the CUSIP/SEDOL is retired).  The universe-intersection downstream
    guarantees we only keep tickers we actually tracked, so this broadening
    cannot introduce off-topic rows.
    """
    raw = _bulk_paginate(
        session,
        "https://api.polygon.io/v3/reference/tickers",
        {
            "market": "stocks",
            "active": "false",
            "order": "desc",
            "sort": "last_updated_utc",
            "limit": 1000,
        },
        max_pages=DELISTINGS_MAX_PAGES,
        phase_label="delistings",
    )

    events: list[CorporateEvent] = []
    skipped_off_universe = 0
    for r in raw:
        sym = _norm(r.get("ticker") or "")
        if sym not in universe:
            skipped_off_universe += 1
            continue
        delisted = r.get("delisted_utc") or r.get("last_updated_utc")
        date_str = None
        if delisted:
            try:
                date_str = datetime.fromisoformat(str(delisted).replace("Z", "+00:00")).date().isoformat()
            except ValueError:
                date_str = None
        events.append(
            CorporateEvent(
                id=f"polygon_delisting:{sym}:{date_str or 'unknown'}",
                type="delisting",
                ticker=sym,
                execution_date=date_str,
                status="executed",
                source="polygon",
                source_url=f"https://polygon.io/stocks/{sym}",
                bucket=bucket_map.get(sym),
                underlying=underlying_map.get(sym),
                headline=(
                    f"{sym} marked inactive by Polygon"
                    + (f" as of {date_str}" if date_str else "")
                ),
                summary=(
                    f"{r.get('name') or sym} (primary exchange {r.get('primary_exchange') or '—'}) "
                    "is no longer listed as an active ETF in the Polygon reference feed. "
                    "Verify against issuer and SEC filings before acting."
                ),
            )
        )
    LOGGER.info(
        "delistings: kept=%d off_universe=%d (of %d inactive tickers returned by Polygon)",
        len(events),
        skipped_off_universe,
        len(raw),
    )
    return events


# ---------------------------------------------------------------------------
# Phase 3 - news (classified, corporate-actions-only)
# ---------------------------------------------------------------------------

# Positive patterns per category.  Each is (regex, weight).  Weight feeds confidence.
POSITIVE_PATTERNS: dict[str, list[re.Pattern]] = {
    "reverse_split": [
        re.compile(r"\breverse[-\s]?split\b", re.I),
        re.compile(r"\b1[-\s]for[-\s]\d+\b", re.I),
        re.compile(r"\bshare[-\s]consolidation\b", re.I),
    ],
    "forward_split": [
        re.compile(r"\bstock[-\s]split\b", re.I),
        re.compile(r"\bforward[-\s]split\b", re.I),
        re.compile(r"\b\d+[-\s]for[-\s]1\b", re.I),
    ],
    "delisting": [
        re.compile(r"\bdelist", re.I),
        re.compile(r"\bliquidat", re.I),
        re.compile(r"\bwind[-\s]down\b", re.I),
        re.compile(r"\bfund\s+closure\b", re.I),
        re.compile(r"\bterminat(e|es|ed|ion|ing)\b", re.I),
        re.compile(r"\bceas\w*\s+trading\b", re.I),
        re.compile(r"\bplan\s+of\s+liquidation\b", re.I),
    ],
    "symbol_change": [
        re.compile(r"\bticker\s+change\b", re.I),
        re.compile(r"\bsymbol\s+change\b", re.I),
        re.compile(r"\brenam(e|ed|ing)\b", re.I),
        re.compile(r"\bre[-\s]brand", re.I),
    ],
    "merger": [
        re.compile(r"\bmerger\b", re.I),
        re.compile(r"\breorganization\b", re.I),
        re.compile(r"\bacquir(e|es|ed|ing|ition)\b", re.I),
    ],
}

# Negative (exclusion) patterns.  Any hit forces the item out of the feed,
# unless we later re-admit via linked structured event.
NEGATIVE_PATTERNS: list[re.Pattern] = [
    re.compile(r"\bdividend(s)?\b", re.I),
    re.compile(r"\bdistribution(s)?\b", re.I),
    re.compile(r"\bpayout\b", re.I),
    re.compile(r"\byield[-\s]announc", re.I),
    re.compile(r"\bearnings\b", re.I),
    re.compile(r"\banalyst(s)?\b", re.I),
    re.compile(r"\bprice\s+target", re.I),
    re.compile(r"\b(up|down)grade", re.I),
    re.compile(r"\bunusual\s+option", re.I),
]


# Fund-context anchors: a positive split match is only accepted if one of these
# tokens appears within the same headline+summary (prevents generic "MSTR splits
# analysts" etc. from firing on unrelated equities).
FUND_CONTEXT_PATTERNS = [
    re.compile(r"\bETF(s)?\b", re.I),
    re.compile(r"\bfund\b", re.I),
    re.compile(r"\bshares\b", re.I),
    re.compile(r"\bunit(s|holders?)?\b", re.I),
    re.compile(r"\bissuer\b", re.I),
    re.compile(r"\btrust\b", re.I),
]


def classify_text(text: str) -> tuple[str | None, float]:
    """Return (category, confidence) or (None, 0.0) if no corporate-action classification."""
    if not text:
        return None, 0.0

    # Reject if any negative pattern hits and no positive (positive may win below).
    neg_hits = sum(1 for p in NEGATIVE_PATTERNS if p.search(text))

    # Score positive patterns.
    scores: dict[str, int] = {}
    for cat, patterns in POSITIVE_PATTERNS.items():
        hits = sum(1 for p in patterns if p.search(text))
        if hits:
            scores[cat] = hits

    if not scores:
        return None, 0.0

    # Drop categories whose match could be spurious outside fund-context.  Ticker-symbol
    # changes are rare enough and ETF-skewed in our universe that we don't require the
    # extra anchor word (gains recall without meaningful precision loss).
    needs_context = {"reverse_split", "forward_split"}
    if any(cat in scores for cat in needs_context):
        has_ctx = any(p.search(text) for p in FUND_CONTEXT_PATTERNS)
        if not has_ctx:
            for c in list(needs_context):
                scores.pop(c, None)

    if not scores:
        return None, 0.0

    # Pick the highest-priority category that still has hits.
    cat = max(scores.keys(), key=lambda c: (CATEGORY_PRIORITY.get(c, 0), scores[c]))
    total_hits = scores[cat]
    confidence = min(0.95, 0.7 + 0.08 * total_hits)
    if neg_hits:
        # A co-mention of "dividend" alongside a split is possible (e.g. "final
        # distribution before delisting"), but we knock confidence down.
        confidence = max(0.55, confidence - 0.15 * neg_hits)
    return cat, round(confidence, 3)


def phase_3_news(
    session: requests.Session,
    tickers: list[str],
    bucket_map: dict[str, str | None],
) -> list[NewsItem]:
    """Bulk-fetch market-wide news over the lookback window, classify, and keep only
    articles mentioning a ticker in our universe that classify as a corporate action.

    Polygon's /v2/reference/news accepts an optional ``ticker`` filter -- we OMIT it
    so the endpoint returns all market news, and paginate up to ``NEWS_MAX_PAGES``
    newest-first.  Each article carries a ``tickers`` list we intersect with our
    universe client-side.  This drops the free-tier budget from ~440 calls to <=25.
    """
    universe = set(bucket_map.keys())
    since_utc = (datetime.now(UTC) - timedelta(days=NEWS_WINDOW_DAYS)).date().isoformat()
    raw = _bulk_paginate(
        session,
        "https://api.polygon.io/v2/reference/news",
        {
            "published_utc.gte": since_utc,
            "order": "desc",
            "limit": NEWS_PAGE_LIMIT,
            "sort": "published_utc",
        },
        max_pages=NEWS_MAX_PAGES,
        phase_label="news",
    )

    items: list[NewsItem] = []
    scanned_on_universe = 0
    for r in raw:
        raw_tickers = r.get("tickers") or []
        related = sorted({_norm(t) for t in raw_tickers if _norm(t) in universe})
        if not related:
            continue
        scanned_on_universe += 1
        title = (r.get("title") or "").strip()
        description = (r.get("description") or "").strip()
        text = f"{title}\n{description}"
        category, confidence = classify_text(text)
        if not category:
            continue
        art_id = r.get("id") or r.get("article_url") or f"{related[0]}:{r.get('published_utc')}"
        pub = r.get("publisher") or {}
        items.append(
            NewsItem(
                id=f"polygon_news:{art_id}",
                tickers=related,
                category=category,
                confidence=confidence,
                published_utc=r.get("published_utc"),
                title=title or None,
                summary=description or None,
                url=r.get("article_url"),
                publisher=(pub.get("name") if isinstance(pub, dict) else None),
                image_url=r.get("image_url"),
                bucket=bucket_map.get(related[0]),
            )
        )
    LOGGER.info(
        "news: raw_articles=%d on_universe=%d classified_corporate_actions=%d",
        len(raw),
        scanned_on_universe,
        len(items),
    )
    return items


# ---------------------------------------------------------------------------
# Phase 4 - symbol changes (per-ticker Polygon events, cached)
# ---------------------------------------------------------------------------

def _load_ticker_events_cache() -> dict:
    """Return ``{ticker: {"fetched_at":iso, "events":[...]}}`` from disk.

    Missing file or malformed JSON yields an empty dict — the sweep will just
    re-populate it on this run.
    """
    if not TICKER_EVENTS_CACHE_PATH.exists():
        return {}
    try:
        payload = json.loads(TICKER_EVENTS_CACHE_PATH.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception as e:  # noqa: BLE001
        LOGGER.warning("ticker-events cache: corrupt (%s); starting fresh", e)
        return {}


def _save_ticker_events_cache(cache: dict) -> None:
    TICKER_EVENTS_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    TICKER_EVENTS_CACHE_PATH.write_text(
        json.dumps(cache, separators=(",", ":"), sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )


def _cache_is_fresh(entry: dict | None, ttl_days: int) -> bool:
    if not entry:
        return False
    stamp = entry.get("fetched_at")
    if not stamp:
        return False
    try:
        then = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
    except ValueError:
        return False
    return (datetime.now(UTC) - then) < timedelta(days=ttl_days)


def _fetch_ticker_events(session: requests.Session, ticker: str) -> list[dict]:
    """Call Polygon /vX/reference/tickers/{t}/events?types=ticker_change.

    Returns the ``events`` array (possibly empty) or an empty list on any error.
    The endpoint resolves both the old and new symbol back to the same FIGI, so
    querying by either side of a rename surfaces the full chain.
    """
    url = f"https://api.polygon.io/vX/reference/tickers/{ticker}/events"
    payload = _polygon_get(session, url, {"types": "ticker_change"})
    if not payload:
        return []
    # Polygon wraps the result in results.events; be defensive about both shapes.
    results = payload.get("results") or {}
    if isinstance(results, list):
        return [r for r in results if isinstance(r, dict)]
    events = results.get("events") if isinstance(results, dict) else None
    if isinstance(events, list):
        return [e for e in events if isinstance(e, dict)]
    return []


def phase_4_symbol_changes(
    session: requests.Session,
    current_universe: set[str],
    underlyings: set[str],
    ever_known: set[str],
    bucket_map: dict[str, str | None],
    underlying_map: dict[str, str | None],
) -> tuple[list[CorporateEvent], dict[str, dict[str, str]]]:
    """Per-ticker Polygon ticker_change sweep with on-disk caching.

    Query budget: the free-tier Polygon cap (5/min) makes a full 480-ticker
    sweep take ~100 min, which exceeds the GitHub Actions timeout if run every
    6h.  We therefore:

      1. prefer cached entries that are younger than
         ``TICKER_EVENTS_CACHE_TTL_DAYS`` (default 14d) — these are free,
      2. fetch the stalest/never-seen tickers first, and
      3. hard-cap the network calls at ``SYMBOL_CHANGE_MAX_CALLS_PER_RUN``
         (default 120) per invocation, so the initial backfill spreads over
         a handful of daily runs but we never blow the timeout.

    Returns ``(events, alias_map)``.  ``alias_map`` is the accumulated
    ``{old: {"new":..., "effective_date":...}}`` persisted to
    :data:`TICKER_ALIAS_MAP_PATH`.
    """
    cache = _load_ticker_events_cache()
    now_iso = datetime.now(UTC).isoformat()
    cutoff = (datetime.now(UTC) - timedelta(days=SYMBOL_CHANGE_LOOKBACK_DAYS)).date().isoformat()

    # Query order: current universe first (most likely to be relevant to
    # today's screener), then current underlyings (catches BITF→KEEL-style
    # renames where the underlying is what rebranded, not the ETF), then
    # anything else we've ever seen.  Within each tier, prefer the stalest.
    tier_current = [t for t in sorted(current_universe) if t]
    tier_underlyings = [t for t in sorted(underlyings) if t and t not in current_universe]
    tier_historic = [t for t in sorted(ever_known) if t and t not in current_universe and t not in underlyings]
    ordered_tickers = tier_current + tier_underlyings + tier_historic

    def _staleness_key(t: str) -> tuple[int, str]:
        ent = cache.get(t)
        if not ent:
            return (0, t)  # never seen → highest priority
        return (1, str(ent.get("fetched_at") or ""))

    ordered_tickers.sort(key=_staleness_key)

    calls_made = 0
    cache_hits = 0
    new_events: list[CorporateEvent] = []
    alias_map: dict[str, dict[str, str]] = {}
    if TICKER_ALIAS_MAP_PATH.exists():
        try:
            alias_map = json.loads(TICKER_ALIAS_MAP_PATH.read_text(encoding="utf-8")) or {}
        except Exception:  # noqa: BLE001
            alias_map = {}

    for t in ordered_tickers:
        cached = cache.get(t)
        if _cache_is_fresh(cached, TICKER_EVENTS_CACHE_TTL_DAYS):
            events_raw = cached.get("events") or []
            cache_hits += 1
        else:
            if calls_made >= SYMBOL_CHANGE_MAX_CALLS_PER_RUN:
                # Out of call budget for this run; stale entries will refresh
                # on the next invocation.  Still use them if we have them.
                if cached:
                    events_raw = cached.get("events") or []
                else:
                    continue
            else:
                events_raw = _fetch_ticker_events(session, t)
                cache[t] = {"fetched_at": now_iso, "events": events_raw}
                calls_made += 1

        for ev in events_raw:
            if (ev.get("type") or "").lower() != "ticker_change":
                continue
            change = ev.get("ticker_change") or {}
            new_sym = _norm(change.get("ticker") or "")
            # Polygon's event model: `ticker_change.ticker` is the NEW symbol
            # after the rename; the queried ticker `t` is the OLD symbol when
            # the event pre-dates the rename, or vice-versa if we queried by
            # the new symbol.  Use composite_figi / date heuristics below to
            # disambiguate; for now store both sides and let dedupe_events
            # collapse duplicates when we eventually query the other side.
            date = ev.get("date")
            if not date or not new_sym:
                continue
            if str(date) < cutoff:
                continue
            old_sym = t if new_sym != t else _norm(ev.get("previous_ticker") or "")
            if not old_sym or old_sym == new_sym:
                continue

            event_id = f"polygon_symchange:{old_sym}:{new_sym}:{date}"
            headline = f"{old_sym} → {new_sym} effective {date}"
            summary = (
                f"Polygon ticker-change event: {old_sym} was renamed to {new_sym} "
                f"on {date}.  Downstream trades, chart deep-links, and borrow lookups "
                f"should be migrated to the new symbol."
            )
            new_events.append(
                CorporateEvent(
                    id=event_id,
                    type="symbol_change",
                    ticker=new_sym,
                    execution_date=str(date),
                    announcement_date=None,
                    status="executed",
                    source="polygon",
                    source_url=f"https://polygon.io/stocks/{new_sym}",
                    bucket=bucket_map.get(new_sym) or bucket_map.get(old_sym),
                    underlying=underlying_map.get(new_sym) or underlying_map.get(old_sym),
                    headline=headline,
                    summary=summary,
                    prior_ticker=old_sym,
                )
            )
            # Accumulate alias: only overwrite if the new record is newer.
            prev_alias = alias_map.get(old_sym) or {}
            if str(date) > (prev_alias.get("effective_date") or ""):
                alias_map[old_sym] = {"new": new_sym, "effective_date": str(date)}

    _save_ticker_events_cache(cache)
    TICKER_ALIAS_MAP_PATH.write_text(
        json.dumps(alias_map, separators=(",", ":"), sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )

    LOGGER.info(
        "symbol_changes: scanned=%d cache_hits=%d calls_made=%d new_events=%d (cap=%d)",
        len(ordered_tickers),
        cache_hits,
        calls_made,
        len(new_events),
        SYMBOL_CHANGE_MAX_CALLS_PER_RUN,
    )
    return new_events, alias_map


# ---------------------------------------------------------------------------
# Phase 5 - Google News RSS sweep (announced liquidations + renames)
# ---------------------------------------------------------------------------

# Queries to run against Google News RSS.  Each is mapped to a canonical event
# type so we can synthesize pending CorporateEvents straight from Google News.
GOOGLE_NEWS_QUERIES: list[tuple[str, str]] = [
    ('"ETF" "plan of liquidation"', "delisting"),
    ('"ETF" "to liquidate"', "delisting"),
    ('"ETF" "cease trading"', "delisting"),
    ('"ETF" "wind down"', "delisting"),
    ('"ETF" "fund termination"', "delisting"),
    ('"ETF" "fund closure"', "delisting"),
    ('"ETF" "ticker change"', "symbol_change"),
    ('"ETF" "symbol change"', "symbol_change"),
    ('"ETF" "reverse split"', "reverse_split"),
    ('"ETF" "stock split"', "forward_split"),
]

# Regex to extract ticker candidates from a news title or summary.  We bias
# toward 3-5 uppercase letters with a word boundary to avoid matching generic
# words like "ETF" itself (which would collide with a ticker of the same name).
_TICKER_CANDIDATE_RE = re.compile(r"(?<![A-Z0-9])([A-Z]{2,5})(?![a-z])")

# Words that look like tickers but are actually English/finance noise.  Not
# exhaustive — the universe-intersection downstream does the heavy lifting.
_TICKER_STOPWORDS = {
    "ETF", "ETFS", "NAV", "AUM", "USD", "USA", "CEO", "CFO", "SEC", "IRS",
    "API", "PDF", "CSV", "JSON", "RSS", "NYC", "NY", "LA", "FDIC", "SIPC",
    "NYSE", "NASDAQ", "AMEX", "BATS", "ARCA", "IPO", "NAV", "OTC", "PR",
    "LLC", "INC", "LTD", "CORP", "LP", "FUND", "TRUST", "PLAN", "NEW",
    "OLD", "DAILY", "WEEKLY", "YEAR", "DAY", "END", "MAX", "MIN", "ALL",
    "ANY", "THE", "AND", "FOR", "YET", "NOT", "ARE", "BUT", "HAS", "WAS",
    "WILL", "WERE", "BEEN", "HAVE", "WITH", "THAT", "THIS", "THAN", "FROM",
    "INTO", "OVER", "UNTO", "UPON", "SELL", "BUY", "LONG", "SHORT",
}


_EXECUTION_DATE_RE = re.compile(
    r"(?:on|effective(?:\s+as\s+of)?|by)\s+"
    r"(?P<month>January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(?P<day>\d{1,2}),?\s+(?P<year>20\d{2})",
    re.IGNORECASE,
)
_MONTH_LOOKUP = {m.lower(): i for i, m in enumerate(
    ["January","February","March","April","May","June","July","August","September","October","November","December"],
    start=1,
)}


def _extract_execution_date(text: str) -> str | None:
    m = _EXECUTION_DATE_RE.search(text or "")
    if not m:
        return None
    try:
        mon = _MONTH_LOOKUP[m.group("month").lower()]
        return f"{int(m.group('year')):04d}-{mon:02d}-{int(m.group('day')):02d}"
    except Exception:  # noqa: BLE001
        return None


def _extract_candidate_tickers(text: str) -> set[str]:
    if not text:
        return set()
    cands = _TICKER_CANDIDATE_RE.findall(text)
    return {c for c in cands if c not in _TICKER_STOPWORDS}


def _fetch_google_news_rss(session: requests.Session, query: str) -> list[dict]:
    """Return a list of ``{title, link, pub_date, description, source}`` dicts.

    Uses the public RSS endpoint (no API key).  Google caches aggressively and
    will often hand back 304/empty under heavy load; we treat all errors as a
    soft miss (empty list) so the rest of the pipeline still runs.
    """
    import urllib.parse as _urlparse
    import xml.etree.ElementTree as ET

    url = "https://news.google.com/rss/search?" + _urlparse.urlencode({
        "q": query,
        "hl": "en-US",
        "gl": "US",
        "ceid": "US:en",
    })
    try:
        resp = session.get(url, timeout=HTTP_TIMEOUT_SEC, headers={
            "User-Agent": "etf-dashboard-corporate-actions/1.0 (+https://goldmandrew.github.io/etf-dashboard)",
            "Accept": "application/rss+xml, application/xml, text/xml",
        })
    except Exception as e:  # noqa: BLE001
        LOGGER.warning("google-news RSS fetch failed (%s): %s", query, e)
        return []
    if resp.status_code != 200 or not resp.content:
        LOGGER.warning("google-news RSS non-200 (%s): status=%s", query, resp.status_code)
        return []
    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as e:
        LOGGER.warning("google-news RSS parse error (%s): %s", query, e)
        return []

    items: list[dict] = []
    for item in root.iter("item"):
        def _text(tag: str) -> str:
            el = item.find(tag)
            return (el.text or "").strip() if el is not None and el.text else ""

        title = _text("title")
        link = _text("link")
        pub_date = _text("pubDate")
        description = _text("description")
        # Google News wraps the source in a <source> element with a "url" attr.
        src_el = item.find("source")
        source_name = (src_el.text or "").strip() if (src_el is not None and src_el.text) else ""

        items.append({
            "title": title,
            "link": link,
            "pub_date": pub_date,
            "description": description,
            "source": source_name,
        })
        if len(items) >= GOOGLE_NEWS_MAX_ARTICLES_PER_QUERY:
            break
    return items


def _parse_rfc822(s: str) -> str | None:
    """Convert RFC-822 (RSS pubDate) to ISO-8601 UTC; returns None on failure."""
    if not s:
        return None
    from email.utils import parsedate_to_datetime
    try:
        dt = parsedate_to_datetime(s)
    except Exception:  # noqa: BLE001
        return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat()


def phase_5_google_news(
    session: requests.Session,
    universe: set[str],
    ever_known: set[str],
    alias_map: dict[str, dict[str, str]],
    bucket_map: dict[str, str | None],
    underlying_map: dict[str, str | None],
) -> tuple[list[CorporateEvent], list[NewsItem]]:
    """Fetch Google News RSS for forward-looking announcements.

    Synthesized output:

      * ``CorporateEvent`` rows with ``status="pending"`` and
        ``source="google_news"`` whenever an article clearly announces an
        upcoming liquidation / rename / split (execution_date parsed from the
        article text; if no date is found the row is still emitted so the
        pinned strip surfaces it, just without a date label).
      * ``NewsItem`` rows for every article that hits (both dated and undated),
        so the rolling headline feed includes press-release coverage that
        Polygon's retail-news index often misses.

    The ``alias_map`` input lets us resolve BITF → KEEL style chains when an
    article mentions the old symbol.
    """
    if not ENABLE_GOOGLE_NEWS:
        LOGGER.info("google-news phase: DISABLED via CORP_ACTIONS_ENABLE_GOOGLE_NEWS=0")
        return [], []

    match_universe = set(universe) | set(ever_known) | set(alias_map.keys())
    cutoff_iso = (datetime.now(UTC) - timedelta(days=GOOGLE_NEWS_WINDOW_DAYS)).isoformat()

    all_events: list[CorporateEvent] = []
    all_news: list[NewsItem] = []
    seen_article_ids: set[str] = set()
    total_articles_scanned = 0
    total_articles_kept = 0

    for query, default_category in GOOGLE_NEWS_QUERIES:
        LOGGER.info("google-news query: %r → default_category=%s", query, default_category)
        articles = _fetch_google_news_rss(session, query)
        total_articles_scanned += len(articles)
        for art in articles:
            title = art.get("title") or ""
            description = art.get("description") or ""
            text = f"{title}\n{description}"
            pub_iso = _parse_rfc822(art.get("pub_date") or "")
            if pub_iso and pub_iso < cutoff_iso:
                continue

            # 1) Extract candidate tickers from title+desc, resolve aliases.
            candidates = _extract_candidate_tickers(text)
            resolved: set[str] = set()
            for c in candidates:
                if c in match_universe:
                    resolved.add(c)
                # Alias chain: old ticker mentioned in article → map to new.
                alias = alias_map.get(c)
                if alias and alias.get("new"):
                    resolved.add(alias["new"])
            # Intersect with tickers we actually track (universe OR ever_known).
            related = sorted({t for t in resolved if t in (universe | ever_known)})
            if not related:
                continue

            # 2) Classify via the shared regex bank so we honour negative patterns
            # (dividend/distribution/earnings drop) but fall back to the query's
            # default category if the classifier doesn't fire.
            cat, conf = classify_text(text)
            if not cat:
                cat = default_category
                conf = 0.65  # lower confidence when inferred from query only
            if cat not in {"delisting", "symbol_change", "reverse_split", "forward_split", "merger"}:
                continue

            # 3) Stable id; skip near-duplicates from multiple queries.
            slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:60] or "untitled"
            art_id = f"gnews:{slug}:{(pub_iso or '')[:10]}:{related[0]}"
            if art_id in seen_article_ids:
                continue
            seen_article_ids.add(art_id)

            publisher = art.get("source") or None

            # 4) Always emit a NewsItem so the article shows up in the feed.
            all_news.append(
                NewsItem(
                    id=art_id,
                    tickers=related,
                    category=cat,
                    confidence=round(conf, 3),
                    published_utc=pub_iso,
                    title=title or None,
                    summary=description or None,
                    url=art.get("link") or None,
                    publisher=publisher,
                    image_url=None,
                    bucket=bucket_map.get(related[0]),
                )
            )

            # 5) If the article announces a delisting / symbol_change / split,
            # synthesize a pending CorporateEvent so the pinned strip shows it
            # before Polygon catches up.
            if cat in {"delisting", "symbol_change", "reverse_split", "forward_split", "merger"}:
                exec_date = _extract_execution_date(text)
                primary = related[0]
                event_id = (
                    f"polygon_delisting:{primary}:{exec_date or 'unknown'}"
                    if cat == "delisting"
                    # Re-use Polygon's id format for delistings so that once Polygon
                    # confirms the event, dedupe_events merges the two records.
                    else f"gnews_{cat}:{primary}:{exec_date or (pub_iso or '')[:10]}"
                )
                all_events.append(
                    CorporateEvent(
                        id=event_id,
                        type=cat,
                        ticker=primary,
                        execution_date=exec_date,
                        announcement_date=(pub_iso or "")[:10] or None,
                        status="pending",
                        source="google_news",
                        source_url=art.get("link") or None,
                        bucket=bucket_map.get(primary),
                        underlying=underlying_map.get(primary),
                        headline=title or None,
                        summary=description or None,
                    )
                )

            total_articles_kept += 1

    LOGGER.info(
        "google-news: queries=%d scanned=%d kept=%d → events=%d news=%d",
        len(GOOGLE_NEWS_QUERIES),
        total_articles_scanned,
        total_articles_kept,
        len(all_events),
        len(all_news),
    )
    return all_events, all_news


# ---------------------------------------------------------------------------
# Merge / dedupe
# ---------------------------------------------------------------------------

_STATUS_RANK = {"executed": 2, "pending": 1, "": 0, None: 0}


def _merge_event_pair(prev: CorporateEvent, cand: CorporateEvent) -> CorporateEvent:
    """Merge two events that share an ``id``, keeping the most authoritative fields.

    Rules:
      * Status: ``executed`` beats ``pending`` beats missing.  This is how a
        Polygon-confirmed delisting promotes a press-release-derived pending row.
      * Dates: a populated ``execution_date`` / ``announcement_date`` always beats
        a blank one (regardless of which record is "winning" on status).
      * ``prior_ticker``: preserved whenever either record has it populated
        (symbol_change chains keep their source metadata even after Polygon
        later confirms the rename).
      * ``headline`` / ``summary`` / ``source_url``: keep the longer non-empty
        value (press releases are usually richer than Polygon's stub text).
    """
    prev_rank = _STATUS_RANK.get(prev.status, 0)
    cand_rank = _STATUS_RANK.get(cand.status, 0)
    base = prev if prev_rank >= cand_rank else cand
    other = cand if base is prev else prev
    merged = CorporateEvent(**asdict(base))

    if not merged.execution_date and other.execution_date:
        merged.execution_date = other.execution_date
    if not merged.announcement_date and other.announcement_date:
        merged.announcement_date = other.announcement_date
    if not merged.prior_ticker and other.prior_ticker:
        merged.prior_ticker = other.prior_ticker
    if not merged.underlying and other.underlying:
        merged.underlying = other.underlying
    if not merged.bucket and other.bucket:
        merged.bucket = other.bucket
    if not merged.ratio_label and other.ratio_label:
        merged.ratio_label = other.ratio_label
        merged.ratio_from = merged.ratio_from or other.ratio_from
        merged.ratio_to = merged.ratio_to or other.ratio_to

    for attr in ("headline", "summary", "source_url"):
        new_val = getattr(other, attr) or ""
        cur_val = getattr(merged, attr) or ""
        if len(new_val) > len(cur_val):
            setattr(merged, attr, new_val)

    return merged


def dedupe_events(events: list[CorporateEvent]) -> list[CorporateEvent]:
    """Collapse by id.  When two records share an id, :func:`_merge_event_pair`
    promotes ``pending`` → ``executed`` and back-fills missing fields from the
    other record so the survivor is strictly richer than either input."""
    by_id: dict[str, CorporateEvent] = {}
    for ev in events:
        prev = by_id.get(ev.id)
        if prev is None:
            by_id[ev.id] = ev
            continue
        by_id[ev.id] = _merge_event_pair(prev, ev)
    out = list(by_id.values())

    def sort_key(ev: CorporateEvent) -> tuple[str, str]:
        return (ev.execution_date or "0000-00-00", ev.ticker)

    out.sort(key=sort_key, reverse=True)
    return out


def dedupe_news(items: list[NewsItem]) -> list[NewsItem]:
    """Collapse near-duplicate headlines (same normalized title + day + ticker set)."""

    def norm_title(t: str | None) -> str:
        s = (t or "").lower()
        s = re.sub(r"\s+", " ", s)
        s = re.sub(r"[^a-z0-9 ]", "", s)
        return s[:120]

    seen: dict[tuple[str, str, str], NewsItem] = {}
    for item in items:
        day = (item.published_utc or "")[:10]
        key = (norm_title(item.title), day, ",".join(sorted(item.tickers)))
        prev = seen.get(key)
        if prev is None:
            seen[key] = item
            continue
        # Keep the earliest publication; extend ticker coverage.
        merged_tickers = sorted(set(prev.tickers) | set(item.tickers))
        if (item.published_utc or "") < (prev.published_utc or ""):
            item.tickers = merged_tickers
            seen[key] = item
        else:
            prev.tickers = merged_tickers

    out = list(seen.values())
    out.sort(key=lambda x: (x.published_utc or ""), reverse=True)
    return out


def link_news_to_events(news: list[NewsItem], events: list[CorporateEvent]) -> None:
    """Attach linked_event_id to news items that correspond to a known structured event."""
    by_ticker: dict[str, list[CorporateEvent]] = {}
    for ev in events:
        by_ticker.setdefault(ev.ticker, []).append(ev)

    for item in news:
        if not item.published_utc:
            continue
        try:
            pub_date = datetime.fromisoformat(item.published_utc.replace("Z", "+00:00")).date()
        except ValueError:
            continue
        for t in item.tickers:
            candidates = by_ticker.get(t) or []
            for ev in candidates:
                if ev.type != item.category:
                    continue
                if not ev.execution_date:
                    continue
                try:
                    exec_date = datetime.fromisoformat(ev.execution_date).date()
                except ValueError:
                    continue
                if abs((exec_date - pub_date).days) <= 7:
                    item.linked_event_id = ev.id
                    break
            if item.linked_event_id:
                break


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def persist(events: list[CorporateEvent], news: list[NewsItem], universe_size: int) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUT_EVENTS.write_text(
        json.dumps(
            {
                "build_time": datetime.now(UTC).isoformat(),
                "universe_size": int(universe_size),
                "events": [asdict(e) for e in events],
            },
            separators=(",", ":"),
            allow_nan=False,
        ),
        encoding="utf-8",
    )
    OUT_NEWS.write_text(
        json.dumps(
            {
                "build_time": datetime.now(UTC).isoformat(),
                "window_days": NEWS_WINDOW_DAYS,
                "items": [asdict(n) for n in news],
            },
            separators=(",", ":"),
            allow_nan=False,
        ),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--symbols",
        default=None,
        help="Optional comma-separated subset; defaults to the full screened universe.",
    )
    parser.add_argument(
        "--skip-news",
        action="store_true",
        help="Only refresh the structured events file; leaves data/etf_news.json alone.",
    )
    parser.add_argument(
        "--skip-delistings",
        action="store_true",
        help="Skip the /v3/reference/tickers?active=false sweep.",
    )
    parser.add_argument(
        "--skip-symbol-changes",
        action="store_true",
        help="Skip the per-ticker /vX/reference/tickers/{t}/events sweep (Phase 4).",
    )
    parser.add_argument(
        "--skip-google-news",
        action="store_true",
        help="Skip the Google News RSS forward-looking announcements sweep (Phase 5).",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if not POLYGON_API_KEY:
        LOGGER.error("POLYGON_API_KEY (or POLYGON_IO_API_KEY) is not set.  Aborting.")
        raise SystemExit(2)

    uni = load_universe()
    if args.symbols:
        want = {_norm(x) for x in args.symbols.split(",") if x.strip()}
        uni = uni[uni["ticker"].isin(want)].reset_index(drop=True)
    if uni.empty:
        LOGGER.warning("Universe empty after filtering; nothing to do.")
        return

    tickers = uni["ticker"].tolist()
    bucket_map = dict(zip(uni["ticker"], uni["bucket"]))
    underlying_map = dict(zip(uni["ticker"], uni["underlying"]))
    LOGGER.info(
        "Starting corporate-actions ingest: tickers=%d news_window=%dd splits_lookback=%dd "
        "rate_limit=%d/min (bulk endpoints; ~30 calls expected total)",
        len(tickers),
        NEWS_WINDOW_DAYS,
        SPLITS_LOOKBACK_DAYS,
        POLYGON_MAX_REQUESTS_PER_MINUTE,
    )

    session = build_session()

    split_events: list[CorporateEvent] = []
    delisting_events: list[CorporateEvent] = []
    symbol_change_events: list[CorporateEvent] = []
    news: list[NewsItem] = []
    gnews_events: list[CorporateEvent] = []
    gnews_items: list[NewsItem] = []
    alias_map: dict[str, dict[str, str]] = {}
    # Precompute the ever-known universe once — Phase 2, Phase 4, and Phase 5
    # all need it and it's cheap-enough (a single parquet read + prior events).
    ever_known = load_ever_known_universe(set(tickers))
    LOGGER.info(
        "Ever-known universe: current=%d ever_known=%d (+%d historic/prior)",
        len(tickers), len(ever_known), len(ever_known) - len(tickers),
    )
    underlyings_set = {u for u in underlying_map.values() if u}
    aborted_reason: str | None = None

    try:
        LOGGER.info(
            "Phase 1: splits (rate_limit=%d req/min)",
            POLYGON_MAX_REQUESTS_PER_MINUTE,
        )
        split_events = phase_1_splits(session, tickers, bucket_map, underlying_map)

        if args.skip_delistings:
            LOGGER.info("Phase 2: delistings SKIPPED")
        else:
            LOGGER.info("Phase 2: delistings")
            delisting_events = phase_2_delistings(
                session, ever_known, bucket_map, underlying_map
            )

        if args.skip_news:
            LOGGER.info("Phase 3: news SKIPPED")
        else:
            LOGGER.info("Phase 3: news")
            news = phase_3_news(session, tickers, bucket_map)

        if args.skip_symbol_changes:
            LOGGER.info("Phase 4: symbol_changes SKIPPED")
        else:
            LOGGER.info(
                "Phase 4: symbol_changes (per-ticker ticker_change events, cache_ttl=%dd, call_cap=%d)",
                TICKER_EVENTS_CACHE_TTL_DAYS,
                SYMBOL_CHANGE_MAX_CALLS_PER_RUN,
            )
            symbol_change_events, alias_map = phase_4_symbol_changes(
                session,
                set(tickers),
                underlyings_set,
                ever_known,
                bucket_map,
                underlying_map,
            )

        if args.skip_google_news or not ENABLE_GOOGLE_NEWS:
            LOGGER.info("Phase 5: google_news SKIPPED")
        else:
            LOGGER.info(
                "Phase 5: google_news (window=%dd, %d queries)",
                GOOGLE_NEWS_WINDOW_DAYS,
                len(GOOGLE_NEWS_QUERIES),
            )
            gnews_events, gnews_items = phase_5_google_news(
                session,
                set(tickers),
                ever_known,
                alias_map,
                bucket_map,
                underlying_map,
            )
    except RateLimitExceeded as e:
        aborted_reason = str(e)
        LOGGER.error("Ingest aborted due to rate-limit wall: %s", e)

    # Delistings are immutable once confirmed — always merge in prior records so
    # that a ticker which fell out of Polygon's top-page `active=false` window
    # does not vanish from the News tab.  dedupe_events() preserves the record
    # with a populated execution_date, so a previously-detected delisting keeps
    # its original date even if the new run finds only a stale stub.
    prior_delistings = load_prior_delistings()
    if prior_delistings and not args.skip_delistings:
        LOGGER.info(
            "  merging %d prior delisting records (immutable pin)",
            len(prior_delistings),
        )
        delisting_events = list(delisting_events) + list(prior_delistings.values())

    # Symbol-change events are also immutable — preserve prior ones across runs
    # so that the per-ticker cache-TTL rollover doesn't accidentally drop a
    # rename we detected weeks ago (Phase 4 only emits events for freshly-queried
    # tickers within the lookback window, so cached tickers whose cache expires
    # might temporarily produce fewer rows).
    prior_symbol_changes = _load_prior_events_of_type("symbol_change")
    if prior_symbol_changes and not args.skip_symbol_changes:
        LOGGER.info(
            "  merging %d prior symbol_change records",
            len(prior_symbol_changes),
        )
        symbol_change_events = list(symbol_change_events) + list(prior_symbol_changes.values())

    all_events = (
        split_events
        + delisting_events
        + symbol_change_events
        + gnews_events
    )
    events = dedupe_events(all_events)
    final_delisting_count = sum(1 for e in events if e.type == "delisting")
    final_pending_delisting = sum(1 for e in events if e.type == "delisting" and e.status == "pending")
    final_split_count = sum(1 for e in events if e.type in {"reverse_split", "forward_split"})
    final_symchange_count = sum(1 for e in events if e.type == "symbol_change")
    LOGGER.info(
        "Events after dedupe: %d (splits=%d delistings=%d [pending=%d] symbol_changes=%d)",
        len(events),
        final_split_count,
        final_delisting_count,
        final_pending_delisting,
        final_symchange_count,
    )

    # Merge Polygon-news + Google-news items into the headline feed.
    news = list(news) + list(gnews_items)
    if news:
        news = dedupe_news(news)
        link_news_to_events(news, events)
    LOGGER.info(
        "News items after classify + dedupe: %d (polygon + google-news merged)",
        len(news),
    )

    if args.skip_news and OUT_NEWS.exists() and not news:
        # Preserve the existing news file if the caller asked to skip news.
        existing = json.loads(OUT_NEWS.read_text(encoding="utf-8"))
        existing_items = existing.get("items") or []
        persist(events, [NewsItem(**it) for it in existing_items], universe_size=len(tickers))
    else:
        persist(events, news, universe_size=len(tickers))

    LOGGER.info(
        "Wrote %s (events=%d) and %s (news=%d)",
        OUT_EVENTS,
        len(events),
        OUT_NEWS,
        len(news),
    )
    if aborted_reason:
        LOGGER.error(
            "Run partially persisted but aborted early; set CORP_ACTIONS_POLYGON_REQS_PER_MIN "
            "to match your Polygon plan (free=5, Starter=100, Developer/Advanced=unlimited -> "
            "e.g. 600) and re-run."
        )
        raise SystemExit(3)


if __name__ == "__main__":
    main()

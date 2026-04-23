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
from collections.abc import Iterable
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
# Fallback cap for phase-2 ticker-targeted checks when the bulk inactive sweep
# misses a universe symbol (e.g. delisted ETF still present in today's screener).
DELISTING_FALLBACK_MAX_CALLS = int(os.getenv("CORP_ACTIONS_DELISTING_FALLBACK_MAX_CALLS", "60"))
DELISTING_FALLBACK_SYMBOLS = {
    str(s).strip().upper().replace(".", "-")
    for s in os.getenv("CORP_ACTIONS_DELISTING_FALLBACK_SYMBOLS", "").split(",")
    if str(s).strip()
}
# Phase 4 (symbol changes) config.  Lookback is long (default 540d) so renames
# that happened before the next refresh still surface during the initial
# backfill period.  Cache TTL controls per-ticker re-query cadence.
SYMBOL_CHANGE_LOOKBACK_DAYS = int(os.getenv("CORP_ACTIONS_SYMBOL_CHANGE_LOOKBACK_DAYS", "540"))
TICKER_EVENTS_CACHE_TTL_DAYS = int(os.getenv("CORP_ACTIONS_TICKER_EVENTS_CACHE_TTL_DAYS", "14"))
# Hard ceiling on per-ticker calls we'll make in one run.  On the free tier
# (5/min) an unbounded initial sweep would take 100+ min and blow the workflow
# timeout.  Cache rollover across multiple runs lets us backfill over a few days.
#
# Budget math (free tier, 5 req/min = 12s/call):
#   60 calls × 12s = 12 min.  Plus ~6 min for phases 1-3 = 18 min.  Well
#   below the 30-min workflow timeout.  Paid tiers can raise this via env
#   (Starter 100/min -> 600 works in same time; Advanced -> no cap needed).
SYMBOL_CHANGE_MAX_CALLS_PER_RUN = int(os.getenv("CORP_ACTIONS_SYMBOL_CHANGE_MAX_CALLS", "60"))
# Wall-clock ceiling for Phase 4, independent of call count.  Guards against
# pathological pagination / retry-After storms that would otherwise eat the
# remainder of the workflow budget before the 404-fix above kicks in.
SYMBOL_CHANGE_WALL_CLOCK_BUDGET_SEC = int(
    os.getenv("CORP_ACTIONS_SYMBOL_CHANGE_WALL_CLOCK_SEC", "900")
)
# Phase 5 (Google News RSS) config.  Independent of Polygon rate limits.  Set
# CORP_ACTIONS_ENABLE_GOOGLE_NEWS=0 to skip (e.g. if Google throttles a runner).
ENABLE_GOOGLE_NEWS = os.getenv("CORP_ACTIONS_ENABLE_GOOGLE_NEWS", "1") not in {"0", "false", "False", ""}
GOOGLE_NEWS_MAX_ARTICLES_PER_QUERY = int(os.getenv("CORP_ACTIONS_GOOGLE_NEWS_MAX_PER_QUERY", "100"))
GOOGLE_NEWS_WINDOW_DAYS = int(os.getenv("CORP_ACTIONS_GOOGLE_NEWS_WINDOW_DAYS", "45"))
# Per-run cap on article-body fetches for ticker disambiguation.  The RSS
# title alone often doesn't contain a ticker (e.g. "T-REX 2X XRP Daily Target
# ETF to Liquidate" -- our actual ticker XRPK only appears in the body).  We
# fall back to fetching the article page when a high-signal headline has no
# resolved ticker.  Bounded at a conservative default to stay polite to
# publishers and avoid burning the workflow budget on low-value pages.
GOOGLE_NEWS_BODY_FETCH_BUDGET = int(os.getenv("CORP_ACTIONS_GOOGLE_NEWS_BODY_FETCH_BUDGET", "40"))
# Weak inferred (bare-token underlying expansion) matches are noisy.  Only
# admit them when classification confidence is high enough.
GOOGLE_NEWS_WEAK_INFERRED_MIN_CONF = float(
    os.getenv("CORP_ACTIONS_GOOGLE_NEWS_WEAK_INFERRED_MIN_CONF", "0.85")
)
# Cap characters of body we store in the classifier context.  Press releases
# are typically ~3-8kb of actual prose; everything beyond is navigation /
# footer and adds no signal.
GOOGLE_NEWS_BODY_MAX_CHARS = int(os.getenv("CORP_ACTIONS_GOOGLE_NEWS_BODY_MAX_CHARS", "20000"))
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
    # Matching diagnostics:
    #   explicit  - ETF ticker appears directly in article text
    #   high      - issuer-style ticker signal mapped via underlying->ETF
    #   inferred  - weak/bare token inferred mapping (high confidence only)
    match_tier: str | None = None
    source_count: int = 1
    source_publishers: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Universe
# ---------------------------------------------------------------------------

def _norm(sym: object) -> str:
    return str(sym).strip().upper().replace(".", "-")


def _underlying_symbols_as_set(values: Iterable[object]) -> set[str]:
    """Coerce screener ``Underlying`` values to a clean upper-case string set.

    CSV / pandas can surface ``float('nan')`` (truthy!), ``pd.NA``, or the
    literal ``\"nan\"`` string.  Those must never reach ``sorted()`` in Phase 4
    where ``float`` vs ``str`` raises ``TypeError``.
    """
    out: set[str] = set()
    for x in values:
        if x is None:
            continue
        if pd.api.types.is_scalar(x) and pd.isna(x):
            continue
        s = str(x).strip()
        if not s or s.lower() == "nan":
            continue
        n = _norm(s)
        if n:
            out.add(n)
    return out


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


def _load_current_universe() -> set[str]:
    """Fast helper: return the set of tickers currently in the active screener.

    Unlike :func:`load_universe` this does not enforce schema and never raises;
    missing file returns an empty set so delisting filtering degrades softly.
    """
    if not UNIVERSE_CSV.exists():
        return set()
    try:
        df = pd.read_csv(UNIVERSE_CSV)
    except Exception:
        return set()
    col = "ETF" if "ETF" in df.columns else ("ticker" if "ticker" in df.columns else None)
    if col is None:
        return set()
    return {_norm(x) for x in df[col].dropna().tolist()}


# Fund-name tokens that let us accept a Polygon inactive record whose ``type``
# is no longer ``ETF``.  ETFs occasionally get reclassified to ``CS`` / ``FUND``
# / blank after delisting, but their ``name`` almost always still contains one
# of these tokens.  Securities that *don't* match any of these are almost
# certainly ticker-reuse collisions (preferreds, warrants, "when-issued"
# stubs, old common stock) and must not be surfaced as ETF delistings.
_FUND_NAME_TOKENS = re.compile(
    r"\b(ETF|ETN|ETP|ETC|FUND|TRUST|SHARES|PORTFOLIO|INDEX|BULL|BEAR|"
    r"ULTRA|ULTRASHORT|ULTRAPRO|DAILY|INVERSE|LEVERED|LEVERAGED|"
    r"\dX|2X|3X|-?1X|WEEKLYPAY|YIELDMAX|DIREXION|PROSHARES|ROUNDHILL|"
    r"TRADR|DEFIANCE|KURV|REX|GRANITESHARES|T-REX)\b",
    re.I,
)
# Polygon ``type`` codes we accept as fund-like even without a name match.
_FUND_POLYGON_TYPES = {"ETF", "ETV", "ETN", "FUND", "CEF"}


def _polygon_ticker_looks_like_fund(rec: dict) -> bool:
    """Heuristic: does this Polygon reference record describe an ETF/fund?

    We need this because
    :func:`phase_2_delistings` intentionally drops the ``type=ETF`` filter on
    the Polygon inactive-tickers endpoint (to catch reclassified funds).  Without
    a safety net, that admits unrelated securities that happen to share a symbol
    with one of our tracked ETFs — see AAPW ("ADVANCE AUTO PARTS INC WI"),
    UNHW ("UNITEDHEALTH GROUP INCORPORATED W.I."), MSTP ("MBNK CP TST ... PF"),
    etc.

    A record passes if **either**

      * ``type`` is in :data:`_FUND_POLYGON_TYPES`, **or**
      * ``name`` contains an obvious fund-name token.

    When ``name`` / ``type`` are both absent we default to ``False`` — better to
    miss a rare reclassified fund than to surface a confidently wrong delisting.
    """
    t = str(rec.get("type") or "").strip().upper()
    if t in _FUND_POLYGON_TYPES:
        return True
    name = str(rec.get("name") or "").strip()
    if not name:
        return False
    return bool(_FUND_NAME_TOKENS.search(name))


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
        if not ev.id:
            continue
        # Legacy delisting ids embed the date (polygon_delisting:SYM:YYYY-MM-DD).
        # Normalize to the ticker-only form so a new run's emission (which uses
        # the new id) merges with any previously-stored record for the same
        # ticker rather than accumulating one card per observed date.
        if event_type == "delisting" and ev.id.startswith("polygon_delisting:"):
            parts = ev.id.split(":")
            if len(parts) >= 2:
                ev.id = f"polygon_delisting:{parts[1]}"
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

        # 4xx other than 429 = deterministic client error (bad ticker,
        # endpoint, params, auth).  Retrying just burns rate-limit slots
        # without changing the answer.  Log once and bail.  This is the
        # critical fix for Phase 4: Polygon returns 404 "no events found"
        # for ~90% of tickers, and previously we retried each 404 five
        # times through the 5 req/min gate (~60s per ticker), blowing the
        # workflow timeout.  Now a 404 costs a single call slot.
        if 400 <= resp.status_code < 500 and resp.status_code != 429:
            _CONSECUTIVE_429_STATE["count"] = 0
            try:
                last_body_snippet = resp.text[:200]
            except Exception:  # noqa: BLE001
                last_body_snippet = None
            # Don't spam INFO for expected "no events" responses on Phase 4.
            if resp.status_code == 404:
                LOGGER.debug("polygon 404 (no data) url=%s", url)
            else:
                LOGGER.warning(
                    "polygon GET %s -> %d (no retry on 4xx): %s",
                    url, resp.status_code, (last_body_snippet or "")[:120],
                )
            return None

        # 5xx — retryable server-side issue.  Bounded backoff then retry.
        if resp.status_code >= 500:
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

    # Current-live set: tickers that are in today's screener.  If a symbol is
    # still trading as an ETF for us, we cannot credibly flag it as delisted --
    # any Polygon inactive record for that symbol is almost certainly a
    # historical ticker-reuse (e.g. an old "Advance Auto Parts WI" stub sharing
    # the AAPW symbol with today's AAPL weekly-pay ETF).  We skip those
    # collisions unconditionally rather than emit a misleading delisting card.
    current_live = _load_current_universe() if UNIVERSE_CSV.exists() else set()

    events: list[CorporateEvent] = []
    seen_delisted_syms: set[str] = set()
    skipped_off_universe = 0
    skipped_live = 0
    skipped_live_symbols: set[str] = set()
    skipped_not_fund = 0
    for r in raw:
        sym = _norm(r.get("ticker") or "")
        if sym not in universe:
            skipped_off_universe += 1
            continue
        if sym in current_live:
            # Still in today's active screener -- Polygon is reporting an old
            # same-ticker security (ticker reuse), not our ETF.  Drop it.
            skipped_live += 1
            skipped_live_symbols.add(sym)
            continue
        if not _polygon_ticker_looks_like_fund(r):
            # Dropping the type=ETF filter on the bulk sweep admits all manner
            # of non-fund inactive securities (warrants, preferreds, old CSs,
            # "when-issued" stubs) that happen to share a symbol with a current
            # ETF in our ever-known set.  Require a fund-ish name or type so
            # those collisions don't surface as bogus delistings.
            skipped_not_fund += 1
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
                # id is ticker-only so repeat runs (Polygon sometimes nudges
                # last_updated_utc day-to-day) collapse to a single card.
                # _merge_event_pair's delisting-specific rule prefers the
                # earliest execution_date, which reflects the actual delisting
                # day rather than Polygon's metadata-touched day.
                id=f"polygon_delisting:{sym}",
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
        seen_delisted_syms.add(sym)

    fallback_hits = 0
    fallback_calls = 0
    fallback_base = set(skipped_live_symbols) | set(DELISTING_FALLBACK_SYMBOLS)
    pagination_likely_truncated = len(raw) >= (DELISTINGS_MAX_PAGES * 1000)
    if pagination_likely_truncated:
        # If the bulk scan is truncated at max_pages, proactively sample symbols
        # still in today's screener so true delistings don't hide behind pager
        # depth.  Call cap keeps this bounded.
        fallback_base |= {t for t in universe if t in current_live}
    fallback_candidates = sorted(
        {
            t
            for t in fallback_base
            if t and t in current_live and t in universe and t not in seen_delisted_syms
        }
    )
    # Ticker-targeted fallback: if a symbol is still in today's screener but
    # Polygon now reports it inactive at /v3/reference/tickers/{ticker}, keep
    # it as a delisting.  This catches lag between screener refresh and
    # reference-state changes (NNEX-like misses) without widening bulk pages.
    for sym in fallback_candidates:
        if fallback_calls >= DELISTING_FALLBACK_MAX_CALLS:
            break
        payload = _polygon_get(session, f"https://api.polygon.io/v3/reference/tickers/{sym}")
        fallback_calls += 1
        rec = (payload or {}).get("results") or {}
        if not isinstance(rec, dict):
            continue
        is_active = rec.get("active")
        if is_active is not False:
            continue
        if not _polygon_ticker_looks_like_fund(rec):
            continue
        delisted = rec.get("delisted_utc") or rec.get("last_updated_utc")
        date_str = None
        if delisted:
            try:
                date_str = datetime.fromisoformat(str(delisted).replace("Z", "+00:00")).date().isoformat()
            except ValueError:
                date_str = None
        events.append(
            CorporateEvent(
                id=f"polygon_delisting:{sym}",
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
                    f"{rec.get('name') or sym} (primary exchange {rec.get('primary_exchange') or '—'}) "
                    "is no longer listed as an active ETF in the Polygon reference feed. "
                    "Verify against issuer and SEC filings before acting."
                ),
            )
        )
        fallback_hits += 1
        seen_delisted_syms.add(sym)
    missed_universe_tickers = max(0, fallback_calls - fallback_hits)
    LOGGER.info(
        "delistings: kept=%d off_universe=%d skipped_live=%d skipped_not_fund=%d "
        "fallback_hits=%d fallback_calls=%d missed_universe_tickers=%d (of %d inactive tickers returned by Polygon)",
        len(events),
        skipped_off_universe,
        skipped_live,
        skipped_not_fund,
        fallback_hits,
        fallback_calls,
        missed_universe_tickers,
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
        re.compile(r"\bdelisted\b", re.I),
        re.compile(r"\bliquidat", re.I),
        re.compile(r"\bwind[-\s]down\b", re.I),
        re.compile(r"\bfund\s+closure\b", re.I),
        re.compile(r"\bterminat(e|es|ed|ion|ing)\b", re.I),
        re.compile(r"\bceas\w*\s+trading\b", re.I),
        re.compile(r"\bplan\s+of\s+liquidation\b", re.I),
        re.compile(r"\bno\s+longer\s+listed\b", re.I),
        re.compile(r"\bmarked\s+inactive\b", re.I),
        re.compile(r"\bexchange\s+delisting\b", re.I),
        re.compile(r"\bdelisting\s+notice\b", re.I),
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

# Titles that are clearly editorial / opinion, not an issuer press release, even
# if they match ``symbol change``-shaped phrasing in the text.
_GNEWS_OPINION_SYMBOL_CHANGE = re.compile(
    r"Jim Cramer|Shares Up Since|"
    r"should change (its|their) ticker|"
    r"change (its|their) ticker to\s+['\u2018\u2019]SELL|"
    r"Change Its Ticker To 'SELL'",
    re.IGNORECASE,
)


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
    # Hard-reject titles/summaries that are clearly commentary or podcasts but may
    # spuriously hit ``acquire`` / ``merger`` in their body (e.g. a Motley Fool
    # episode "Breaking down Jamie Dimon's letter" while also mentioning JEPQ).
    if re.search(r"\bBreaking Down Jamie\b", text, re.I):
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
         (default 60) per invocation, so the initial backfill spreads over
         a handful of daily runs but we never blow the timeout.

    Returns ``(events, alias_map)``.  ``alias_map`` is the accumulated
    ``{old: {"new":..., "effective_date":...}}`` persisted to
    :data:`TICKER_ALIAS_MAP_PATH`.
    """
    cache = _load_ticker_events_cache()
    now_iso = datetime.now(UTC).isoformat()
    cutoff = (datetime.now(UTC) - timedelta(days=SYMBOL_CHANGE_LOOKBACK_DAYS)).date().isoformat()

    # Query order (by rename probability, not alphabetical):
    #
    #   1. Underlyings first -- common-stock rebrands (BITF -> KEEL, FB -> META,
    #      FCEL -> PLUG, etc.) are where almost all real ticker_change events
    #      fire.  Polygon's endpoint returns events keyed to the underlying's
    #      FIGI, so querying the current symbol finds the full rename chain.
    #   2. Current ETF universe -- leveraged/inverse ETFs occasionally rename
    #      (MSTU -> MSTX, that sort of thing) but it's rare and Phase 2 already
    #      catches the delisting / relisting side effects.
    #   3. Historic / ever-known -- lowest priority; mostly a safety net so
    #      tickers that silently dropped out of the screener still get swept.
    #
    # Within each tier the staleness sort (cache_hit? ascending, then ticker)
    # ensures cold entries dominate the call budget instead of re-fetching
    # already-known tickers on every run.
    underlyings = _underlying_symbols_as_set(underlyings)
    tier_underlyings = [t for t in sorted(underlyings) if t and t not in current_universe]
    tier_current = [t for t in sorted(current_universe) if t]
    tier_historic = [t for t in sorted(ever_known) if t and t not in current_universe and t not in underlyings]
    ordered_tickers = tier_underlyings + tier_current + tier_historic
    # ``tier_rank`` preserves the deliberate tier ordering (underlyings → ETFs →
    # historic) *inside* the never-seen bucket.  A plain ``(0, ticker)`` sort key
    # accidentally sorted **all** never-seen symbols alphabetically across tiers,
    # so hundreds of ``A*`` ETF symbols starved out common-stock underlyings like
    # ``KEEL`` (BITF→KEEL) until dozens of runs had passed.
    tier_rank: dict[str, int] = {}
    for i, tier in enumerate((tier_underlyings, tier_current, tier_historic)):
        for sym in tier:
            tier_rank[sym] = i

    def _staleness_key(t: str) -> tuple:
        ent = cache.get(t)
        tr = tier_rank.get(t, 99)
        if not ent:
            return (0, tr, t)  # never seen → tier first, then alpha
        # Seen: refresh coldest cache first (ISO timestamps sort chronologically),
        # still respecting tier when timestamps tie.
        return (1, str(ent.get("fetched_at") or ""), tr, t)

    ordered_tickers.sort(key=_staleness_key)

    calls_made = 0
    cache_hits = 0
    wall_clock_deadline = time.monotonic() + SYMBOL_CHANGE_WALL_CLOCK_BUDGET_SEC
    wall_clock_abort = False
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
            if calls_made >= SYMBOL_CHANGE_MAX_CALLS_PER_RUN or time.monotonic() >= wall_clock_deadline:
                # Out of call budget or wall-clock budget for this run; stale
                # entries will refresh on the next invocation.  Still use them
                # if we have them.
                if time.monotonic() >= wall_clock_deadline and not wall_clock_abort:
                    LOGGER.warning(
                        "symbol_changes: wall-clock budget (%ds) exhausted after %d calls; "
                        "deferring remaining tickers to next run",
                        SYMBOL_CHANGE_WALL_CLOCK_BUDGET_SEC,
                        calls_made,
                    )
                    wall_clock_abort = True
                if cached:
                    events_raw = cached.get("events") or []
                else:
                    continue
            else:
                events_raw = _fetch_ticker_events(session, t)
                cache[t] = {"fetched_at": now_iso, "events": events_raw}
                calls_made += 1

        # Polygon documents each event's ``ticker_change.ticker`` as the symbol
        # **after** that event's effective date.  The prior symbol is usually NOT
        # exposed as ``previous_ticker`` in the JSON (docs only show nested
        # ``ticker``).  Infer ``old -> new`` by walking the timeline sorted oldest
        # first: for event i>0, old is the prior row's post-rename ticker.
        #
        # Single-event fallback: when we queried a **pre-rename** id and Polygon
        # returns one row whose ``ticker`` is already the new symbol, treat the
        # query symbol as the old side (``new_sym != _norm(t)``).
        tc_sorted = sorted(
            (e for e in events_raw if (e.get("type") or "").lower() == "ticker_change"),
            key=lambda e: (
                str(e.get("date") or ""),
                str(((e.get("ticker_change") or {}).get("ticker") or "")).upper(),
            ),
        )
        for idx, ev in enumerate(tc_sorted):
            change = ev.get("ticker_change") or {}
            new_sym = _norm(change.get("ticker") or "")
            date = ev.get("date")
            if not date or not new_sym:
                continue
            if str(date) < cutoff:
                continue

            old_sym = _norm(ev.get("previous_ticker") or change.get("previous_ticker") or "")
            if not old_sym and idx > 0:
                prev_change = tc_sorted[idx - 1].get("ticker_change") or {}
                old_sym = _norm(prev_change.get("ticker") or "")
            if not old_sym and idx == 0 and new_sym != _norm(t):
                old_sym = _norm(t)
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
    # Titles often say "T-REX" without repeating the "ETF" token both times;
    # this recovers 2X crypto product liquidations (XRPK / SOLX) missed by
    # generic "ETF … liquidate" matches.
    ('"T-REX" "liquidate"', "delisting"),
    ('"ETF" "cease trading"', "delisting"),
    ('"ETF" "wind down"', "delisting"),
    ('"ETF" "fund termination"', "delisting"),
    ('"ETF" "fund closure"', "delisting"),
    ('"ETF" "ticker change"', "symbol_change"),
    ('"ETF" "symbol change"', "symbol_change"),
    # Common-stock / issuer renames (BITF→KEEL, etc.) rarely contain the token
    # ``ETF`` in the Google News headline — dedicated queries recover them.
    ('"changes its ticker"', "symbol_change"),
    ('"will change" "ticker"', "symbol_change"),
    ('"change its ticker to"', "symbol_change"),
    ('"ETF" "reverse split"', "reverse_split"),
    ('"ETF" "stock split"', "forward_split"),
]

# Regex to extract ticker candidates from a news title or summary.  We bias
# toward 3-5 uppercase letters with a word boundary to avoid matching generic
# words like "ETF" itself (which would collide with a ticker of the same name).
_TICKER_CANDIDATE_RE = re.compile(r"(?<![A-Z0-9])([A-Z]{2,5})(?![a-z])")

# Parenthesized ticker, e.g. "...Income Strategy ETF (JEPY)".  This is how
# issuer press releases nearly always disambiguate the fund.  Matches with or
# without the "NASDAQ: "/"NYSE: "/"Cboe BZX Exchange, Inc: " prefix that some
# press releases include inside the parens.
_PARENTHESIZED_TICKER_RE = re.compile(
    r"\(\s*(?:(?:NASDAQ|NYSE|NYSE\s*ARCA|ARCA|CBOE|CBOE\s*BZX|BZX|BATS|AMEX|OTC|NASDAQGM|NASDAQGS|NASDAQCM)"
    r"(?:\s*Exchange)?(?:,\s*Inc\.?)?\s*:\s*)?([A-Z]{2,5})\s*\)"
)
# Exchange-prefixed ticker without parens, e.g. "Cboe BZX Exchange, Inc: XRPK".
_EXCHANGE_PREFIX_TICKER_RE = re.compile(
    r"\b(?:NASDAQ|NYSE|NYSE\s*ARCA|ARCA|CBOE|CBOE\s*BZX|BZX|BATS|AMEX|OTC)"
    r"(?:\s*Exchange)?(?:,\s*Inc\.?)?\s*:\s*([A-Z]{2,5})\b"
)
# Twitter-style ticker, e.g. "$XRPK".
_DOLLAR_TICKER_RE = re.compile(r"\$([A-Z]{2,5})\b")
# "ticker: XRPK" / "symbol: XRPK" — common in issuer press releases.
# Require a real ``ticker: XYZ`` / ``symbol:`` label, not the phrase
# "Change its Ticker to KEEL" (that produced false *label* matches).
_TICKER_LABEL_RE = re.compile(
    r"\b(?:ticker|symbol)\s*:\s*([A-Z]{2,5})\b",
    re.IGNORECASE,
)

# Underlying symbols that are also common English / headline tokens.  We still
# allow *explicit* or *high-signal* (paren/exchange) matches, but the weak
# (bare-word → underlying → ETF) path would otherwise turn ``TOP`` into TOPW
# (Leveraged Oil & Gas) on unrelated stories that mention the word in prose.
_AMBIGUOUS_BARE_ENV = {
    s.strip().upper()
    for s in os.getenv("CORP_ACTIONS_AMBIGUOUS_BARE", "TOP,AI").split(",")
    if s.strip()
}
AMBIGUOUS_BARE_FOR_WEAK_INFER: frozenset[str] = frozenset(
    _AMBIGUOUS_BARE_ENV if _AMBIGUOUS_BARE_ENV else {"TOP", "AI"}
)

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


def _extract_high_signal_tickers(text: str) -> set[str]:
    """Ticker-like tokens from issuer-style patterns (parens, exchange prefix, …)."""
    if not text:
        return set()
    out: set[str] = set()
    for regex in (
        _PARENTHESIZED_TICKER_RE,
        _EXCHANGE_PREFIX_TICKER_RE,
        _TICKER_LABEL_RE,
        _DOLLAR_TICKER_RE,
    ):
        for match in regex.findall(text):
            sym = (match if isinstance(match, str) else match[0]).upper()
            if sym and sym not in _TICKER_STOPWORDS:
                out.add(sym)
    return out


def _extract_bare_tickers(text: str) -> set[str]:
    """Loose ``WORD`` caps matches — very noisy in Google News HTML descriptions."""
    if not text:
        return set()
    out: set[str] = set()
    for match in _TICKER_CANDIDATE_RE.findall(text):
        sym = (match if isinstance(match, str) else match[0]).upper()
        if sym and sym not in _TICKER_STOPWORDS:
            out.add(sym)
    return out


def _extract_title_disclosed_etf_syms(
    title: str,
    etf_universe: set[str],
    alias_map: dict[str, dict[str, str]],
) -> set[str]:
    """Tickers the **headline** discloses (paren/exchange) that map to current screener ETFs.

    Strips the Google News **RSS description** from the signal path — it embeds
    related-ticker sidebars and once caused ``(TOPW)`` in HTML to crowd out
    *actual* fund tickers in the real headline.  We also follow ``alias_map``
    when the title names a pre-rename stock symbol (e.g. BITF → KEEL).
    """
    if not title:
        return set()
    out: set[str] = set()
    raw: set[str] = set()
    for regex in (
        _PARENTHESIZED_TICKER_RE,
        _EXCHANGE_PREFIX_TICKER_RE,
        _TICKER_LABEL_RE,
        _DOLLAR_TICKER_RE,
    ):
        for match in regex.findall(title):
            sym = (match if isinstance(match, str) else match[0]).upper()
            if sym and sym not in _TICKER_STOPWORDS:
                raw.add(sym)
    for match in _TICKER_CANDIDATE_RE.findall(title):
        sym = (match if isinstance(match, str) else match[0]).upper()
        if sym not in _TICKER_STOPWORDS:
            raw.add(sym)
    for sym in raw:
        n = _norm(sym)
        if n in etf_universe:
            out.add(n)
        elif alias_map and n in alias_map and alias_map[n].get("new"):
            n2 = _norm(alias_map[n]["new"])
            if n2 in etf_universe:
                out.add(n2)
    return out


def _raw_ticker_disclosures_in_title(title: str) -> set[str]:
    """(BMAX), (NYSE: Z), $Z — from the headline only, *not* restricted to the screener.

    When non-empty, any ETF resolved **only** from a fetched page body (sidebar
    "related" tickers) is treated as spurious if it does not appear in the
    title text — a pattern we saw for ``(BMAX)`` liquidate pieces that pulled
    unrelated symbols from syndicated HTML.
    """
    if not title:
        return set()
    out: set[str] = set()
    for regex in (
        _PARENTHESIZED_TICKER_RE,
        _EXCHANGE_PREFIX_TICKER_RE,
        _DOLLAR_TICKER_RE,
        _TICKER_LABEL_RE,
    ):
        for match in regex.findall(title):
            sym = (match if isinstance(match, str) else match[0]).upper()
            if sym and sym not in _TICKER_STOPWORDS:
                out.add(sym)
    return out


def _symbol_appears_in_title_text(title: str, sym: str) -> bool:
    """True if ``sym`` is visibly tied to the headline (word token or paren)."""
    if not title or not sym:
        return False
    s = _norm(str(sym).strip().upper())
    t = str(title)
    if not s:
        return False
    if f"({s})" in t or f"({s.upper()})" in t or f"({s.lower()})" in t:
        return True
    if re.search(rf"(?<![A-Za-z0-9.]){re.escape(s)}(?![A-Za-z0-9])", t, re.IGNORECASE):
        return True
    return False


def _extract_candidate_tickers(text: str) -> set[str]:
    """Union of high-signal + bare patterns (Polygon news paths, tests, etc.)."""
    return _extract_high_signal_tickers(text) | _extract_bare_tickers(text)


def _strip_html_to_plain(s: str | None, *, max_len: int = 2000) -> str | None:
    """Drop HTML tags from RSS snippets so the dashboard does not show raw ``<a>``."""
    if not s:
        return None
    t = re.sub(r"<[^>]+>", " ", str(s))
    t = re.sub(r"\s+", " ", t).strip()
    if not t:
        return None
    if len(t) > max_len:
        t = t[: max_len - 3].rstrip() + "..."
    return t


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


_HTML_TAG_RE = re.compile(r"<[^>]+>")
_HTML_WHITESPACE_RE = re.compile(r"\s+")
# High-signal phrases that justify spending a body-fetch slot on an article
# whose title alone failed to resolve any universe ticker.  Keep this tight:
# pure "rally"/"downgrade" etc. articles don't get the expensive lookup.
_BODY_FETCH_TRIGGER_RE = re.compile(
    r"\b(?:to\s+liquidate|plan\s+of\s+liquidation|will\s+liquidate|"
    r"wind\s+down|cease\s+trading|fund\s+(?:termination|closure)|"
    r"delisted|delisting\s+notice|exchange\s+delisting|no\s+longer\s+listed|marked\s+inactive|"
    r"ticker\s+change|symbol\s+change|changes\s+its\s+ticker|will\s+change\s+its\s+ticker|"
    r"change\s+its\s+ticker\s+to|reverse\s+stock\s+split|reverse\s+split|"
    r"forward\s+stock\s+split|share\s+consolidation)\b",
    re.IGNORECASE,
)


def _body_window_around_trigger(
    body: str,
    *,
    chars_before: int = 500,
    chars_after: int = 1500,
) -> str:
    """Return the slice of ``body`` centered on the first action-trigger phrase.

    Press-release pages nearly always embed the corporate-action announcement
    in the first few paragraphs; the bulk of the remaining bytes is
    navigation / sidebar / "related news" content which references dozens of
    unrelated tickers.  Limiting the ticker sweep to this tight window is
    what prevents a Morningstar sidebar blurb about Microsoft AI from being
    conflated with the T-REX liquidation that's actually on the page.

    Falls back to the first ~2000 chars when no trigger is found (shouldn't
    happen since we only invoke body fetch after a title/desc trigger hit,
    but is defensive in case the trigger phrase is title-only).
    """
    if not body:
        return ""
    match = _BODY_FETCH_TRIGGER_RE.search(body)
    if match is None:
        return body[: chars_before + chars_after]
    start = max(0, match.start() - chars_before)
    end = min(len(body), match.end() + chars_after)
    return body[start:end]


def _fetch_article_body(session: requests.Session, url: str) -> str:
    """Fetch an article page and return its cleaned plain-text body.

    Soft-fail on any error (bad URL, timeout, 403 paywall, non-HTML content-
    type) -- the caller just treats it as "no additional text" and moves on.
    We intentionally don't follow JS redirects or run a headless browser; the
    overwhelming majority of press releases are server-rendered, so a basic
    requests.get() fetches the entire body.
    """
    if not url:
        return ""
    try:
        # Many financial publishers (Morningstar, etc.) return ``202`` with an
        # empty body or a bot-wall stub unless the request looks like a normal
        # browser.  The minimal "compatible; …" UA worked for some hosts but
        # consistently failed here in CI, which meant Phase 5 could never resolve
        # tickers that only appear inside the article HTML (T-REX XRPK/SOLX).
        resp = session.get(
            url,
            timeout=HTTP_TIMEOUT_SEC,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/121.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            allow_redirects=True,
        )
    except Exception as e:  # noqa: BLE001
        LOGGER.debug("article body fetch failed %s: %s", url, e)
        return ""
    if resp.status_code != 200 or not resp.text:
        LOGGER.debug("article body non-200 %s: status=%s", url, resp.status_code)
        return ""
    # Bail early on clearly non-HTML (PDF press releases come back as
    # application/pdf and the regex pass below would produce garbage).
    ct = resp.headers.get("Content-Type", "")
    if ct and "html" not in ct.lower():
        return ""
    text = _HTML_TAG_RE.sub(" ", resp.text)
    text = _HTML_WHITESPACE_RE.sub(" ", text)
    if len(text) > GOOGLE_NEWS_BODY_MAX_CHARS:
        text = text[:GOOGLE_NEWS_BODY_MAX_CHARS]
    return text


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
    underlyings: set[str],
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
    article mentions the old symbol.  ``underlyings`` must include every
    screener ``Underlying`` symbol so headlines that only name the stock (KEEL)
    still intersect our tracked set (``ever_known`` is built from ETF ``ticker``
    rows in metrics parquet, not from the Underlying column).

    News rows and synthesized events only attach tickers that map to **current
    screener ETFs**: bare-word matches are taken from the **headline** (not RSS
    HTML descriptions) and underlyings expand to ETFs only when the match is
    high-signal or the bare token is long enough (>=3 chars) to avoid ``AI`` /
    ``TOP`` style noise.
    """
    if not ENABLE_GOOGLE_NEWS:
        LOGGER.info("google-news phase: DISABLED via CORP_ACTIONS_ENABLE_GOOGLE_NEWS=0")
        return [], []

    alias_new: set[str] = set()
    for v in (alias_map or {}).values():
        n = _norm(v.get("new") or "")
        if n:
            alias_new.add(n)
    underlyings = _underlying_symbols_as_set(underlyings)
    tracked: set[str] = (
        {_norm(t) for t in universe if t}
        | {_norm(t) for t in ever_known if t}
        | {_norm(t) for t in underlyings if t}
        | {_norm(t) for t in (alias_map or {}) if t}
        | alias_new
    )
    cutoff_iso = (datetime.now(UTC) - timedelta(days=GOOGLE_NEWS_WINDOW_DAYS)).isoformat()

    etf_universe: set[str] = {_norm(t) for t in universe if t}
    underlying_to_etfs: dict[str, set[str]] = {}
    for etf_raw, und in underlying_map.items():
        if und is None or (pd.api.types.is_scalar(und) and pd.isna(und)):
            continue
        u = _norm(str(und))
        if not u:
            continue
        underlying_to_etfs.setdefault(u, set()).add(_norm(str(etf_raw)))

    all_events: list[CorporateEvent] = []
    all_news: list[NewsItem] = []
    seen_article_ids: set[str] = set()
    seen_gnews_urls: set[str] = set()  # de-dupe same link across multiple queries
    seen_body_urls: set[str] = set()  # de-dupe body fetches across queries
    total_articles_scanned = 0
    total_articles_kept = 0
    body_fetches_done = 0
    body_fetches_hit = 0  # resolved a ticker from body that wasn't in title
    dropped_low_confidence = 0

    def _resolve_set(candidates: set[str]) -> set[str]:
        """Intersect + alias-expand against the full tracked universe."""
        resolved: set[str] = set()
        for c in candidates:
            cn = _norm(c)
            if cn in tracked:
                resolved.add(cn)
            alias = alias_map.get(cn)
            if alias and alias.get("new"):
                resolved.add(_norm(alias["new"]))
        return {t for t in resolved if t in tracked}

    def _match_tiers(high_syms: set[str], bare_syms: set[str]) -> tuple[list[str], list[str], list[str]]:
        """Return (explicit_etfs, high_signal_etfs, weak_inferred_etfs)."""
        explicit: set[str] = set()
        high: set[str] = set()
        weak: set[str] = set()
        for s in high_syms | bare_syms:
            if s in etf_universe:
                explicit.add(s)
        for s in high_syms:
            if s in explicit:
                continue
            high.update(underlying_to_etfs.get(s, ()))
        # Alias-anchored bare tokens (BITF/KEEL-like rename chains) are treated
        # as high-signal enough to preserve symbol-change recall.
        alias_anchor = set(alias_map.keys()) | alias_new
        for s in bare_syms:
            if s in explicit or s in high:
                continue
            if s in alias_anchor and len(s) >= 3:
                high.update(underlying_to_etfs.get(s, ()))
        # Weak inferred matches: bare uppercase tokens mapping to underlyings.
        # Keep strict (len>=3) to avoid words like AI/TOP creating spam, and
        # suppress ambiguous English tokens (``TOP`` → oil ETF, etc.).
        for s in bare_syms:
            if s in explicit or s in high:
                continue
            if s in AMBIGUOUS_BARE_FOR_WEAK_INFER:
                continue
            if len(s) >= 3:
                weak.update(underlying_to_etfs.get(s, ()))
        return sorted(explicit), sorted(high), sorted(weak)

    for query, default_category in GOOGLE_NEWS_QUERIES:
        LOGGER.info("google-news query: %r → default_category=%s", query, default_category)
        articles = _fetch_google_news_rss(session, query)
        total_articles_scanned += len(articles)
        for art in articles:
            title = art.get("title") or ""
            url = (art.get("link") or "").strip()
            if url and url in seen_gnews_urls:
                continue

            description = art.get("description") or ""
            desc_plain = _strip_html_to_plain(description) or ""
            # Never extract tickers from raw RSS HTML: sidebars were attaching
            # ``TOPW``-style false positives.  Ticker resolution uses the headline
            # and (if fetched) a tight body window only.
            pub_iso = _parse_rfc822(art.get("pub_date") or "")
            if pub_iso and pub_iso < cutoff_iso:
                continue

            if _GNEWS_OPINION_SYMBOL_CHANGE.search(f"{title}\n{desc_plain}"):
                continue

            text_for_tick = title
            high_syms = _resolve_set(_extract_high_signal_tickers(text_for_tick))
            bare_syms = _resolve_set(_extract_bare_tickers(text_for_tick))
            related_union = high_syms | bare_syms
            prelim_explicit, prelim_high, _prelim_weak = _match_tiers(high_syms, bare_syms)
            full_for_trigger = f"{title}\n{desc_plain}"
            body_window: str | None = None
            # Body fetch when the headline is high-signal but fund tickers live
            # in the article HTML (T-REX XRPK / SOLX pattern).
            if (
                (not related_union or (not prelim_explicit and not prelim_high))
                and url
                and url not in seen_body_urls
                and body_fetches_done < GOOGLE_NEWS_BODY_FETCH_BUDGET
                and _BODY_FETCH_TRIGGER_RE.search(full_for_trigger)
            ):
                seen_body_urls.add(url)
                body = _fetch_article_body(session, url)
                body_fetches_done += 1
                if body:
                    window = _body_window_around_trigger(body)
                    if window:
                        body_window = window
                        text_for_tick = f"{title}\n{window}"
                        high_syms = _resolve_set(_extract_high_signal_tickers(text_for_tick))
                        bare_syms = _resolve_set(_extract_bare_tickers(text_for_tick))
                        related_union = high_syms | bare_syms
                        if related_union:
                            body_fetches_hit += 1

            if not related_union:
                continue

            classify_text_src = f"{title}\n{desc_plain}"
            if body_window:
                classify_text_src = f"{classify_text_src}\n{body_window}"

            # 2) Classify via the shared regex bank so we honour negative patterns
            # (dividend/distribution/earnings drop) but fall back to the query's
            # default category if the classifier doesn't fire.
            cat, conf = classify_text(classify_text_src)
            if not cat:
                cat = default_category
                conf = 0.65  # lower confidence when inferred from query only
            if cat not in {"delisting", "symbol_change", "reverse_split", "forward_split", "merger"}:
                continue

            # Strict tiering to prevent feed spam:
            # explicit ETF ticker in article > high-signal issuer-form ticker
            # > weak inferred (bare token -> underlying -> ETF, only high-conf).
            explicit_syms, high_signal_syms, weak_syms = _match_tiers(high_syms, bare_syms)
            match_tier = None
            if explicit_syms:
                emit_syms = explicit_syms
                match_tier = "explicit"
            elif high_signal_syms:
                emit_syms = high_signal_syms
                match_tier = "high"
            elif weak_syms and conf >= GOOGLE_NEWS_WEAK_INFERRED_MIN_CONF:
                emit_syms = weak_syms
                match_tier = "inferred"
            else:
                dropped_low_confidence += 1
                continue

            # If the headline discloses a specific screener ETF, prefer that over
            # unrelated tickers that crawled in from a syndicator's page chrome.
            t_discl = _extract_title_disclosed_etf_syms(title, etf_universe, alias_map or {})
            if t_discl:
                inter = [x for x in emit_syms if x in t_discl]
                if inter:
                    emit_syms = sorted(inter)
                    if set(emit_syms) <= t_discl:
                        match_tier = "explicit"
                else:
                    emit_syms = sorted(t_discl)
                    match_tier = "explicit"

            # Headline paren / $ disclosure anchors the *subject*; article bodies
            # and syndicated sidebars can mention dozens of unrelated fund tickers
            # (BMAX liquidations picking up random crypto ETF codes from HTML).
            title_line_discl = _raw_ticker_disclosures_in_title(title)
            if title_line_discl:
                emit_syms = sorted(
                    {
                        s
                        for s in emit_syms
                        if s in t_discl
                        or _symbol_appears_in_title_text(title, s)
                    }
                )
                if not emit_syms:
                    continue

            # Stable id; skip near-duplicates from multiple queries.
            slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:60] or "untitled"
            art_id = f"gnews:{slug}:{(pub_iso or '')[:10]}:{emit_syms[0]}"
            if art_id in seen_article_ids:
                continue
            seen_article_ids.add(art_id)
            if url:
                seen_gnews_urls.add(url)

            publisher = art.get("source") or None
            summary_plain = _strip_html_to_plain(description)
            if not summary_plain and body_window:
                bw = re.sub(r"\s+", " ", (body_window or "")).strip()
                if bw:
                    summary_plain = bw[:400] + ("..." if len(bw) > 400 else "")

            # 4) Always emit a NewsItem so the article shows up in the feed.
            all_news.append(
                NewsItem(
                    id=art_id,
                    tickers=emit_syms,
                    category=cat,
                    confidence=round(conf, 3),
                    published_utc=pub_iso,
                    title=title or None,
                    summary=summary_plain,
                    url=art.get("link") or None,
                    publisher=publisher,
                    image_url=None,
                    bucket=bucket_map.get(emit_syms[0]),
                    match_tier=match_tier,
                    source_count=1,
                    source_publishers=[publisher] if publisher else [],
                )
            )

            # 5) Optionally synthesize pending CorporateEvents for the pinned strip.
            #
            # The rolling ``etf_news.json`` feed still includes lower-tier matches
            # for research, but we **only** surface Google News as a structured
            # (pinned) event when the ticker resolution is ``explicit`` or
            # ``high``.  ``inferred`` matches were almost always wrong in
            # production (e.g. underlying ``TOP`` → TOPW on unrelated
            # marketscreener syndication).  Issuer renames like BITF→KEEL and
            # body-resolved T-REX tickers land in ``high``.
            #
            # Delistings already use ``polygon_delisting:{sym}`` ids so multiple
            # press pick-ups merge; symbol_change uses a **ticker-only** id so
            # we do not stack duplicate cards for the same fund.
            if cat in {"delisting", "symbol_change", "reverse_split", "forward_split", "merger"}:
                if match_tier in {"explicit", "high"}:
                    exec_date = _extract_execution_date(classify_text_src)
                    for sym in emit_syms:
                        if cat == "delisting":
                            event_id = f"polygon_delisting:{sym}"
                        elif cat == "symbol_change":
                            event_id = f"gnews_symbol_change:{sym}"
                        else:
                            event_id = (
                                f"gnews_{cat}:{sym}:{exec_date or (pub_iso or '')[:10]}"
                            )
                        all_events.append(
                            CorporateEvent(
                                id=event_id,
                                type=cat,
                                ticker=sym,
                                execution_date=exec_date,
                                announcement_date=(pub_iso or "")[:10] or None,
                                status="pending",
                                source="google_news",
                                source_url=art.get("link") or None,
                                bucket=bucket_map.get(sym),
                                underlying=underlying_map.get(sym),
                                headline=title or None,
                                summary=summary_plain,
                            )
                        )

            total_articles_kept += 1

    LOGGER.info(
        "google-news: queries=%d scanned=%d kept=%d body_fetch=%d/%d (budget %d) dropped_low_confidence=%d → events=%d news=%d",
        len(GOOGLE_NEWS_QUERIES),
        total_articles_scanned,
        total_articles_kept,
        body_fetches_hit,
        body_fetches_done,
        GOOGLE_NEWS_BODY_FETCH_BUDGET,
        dropped_low_confidence,
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
    elif (
        merged.type == "delisting"
        and merged.execution_date
        and other.execution_date
        and other.execution_date < merged.execution_date
    ):
        # For delistings, Polygon sometimes reports ``last_updated_utc`` on
        # consecutive days as it finalizes metadata.  Prefer the EARLIEST
        # execution_date so the card reflects the actual last-traded day
        # rather than a later metadata touch.
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


_MATCH_TIER_RANK = {"explicit": 3, "high": 2, "inferred": 1, None: 0, "": 0}


def dedupe_news(items: list[NewsItem]) -> list[NewsItem]:
    """Collapse near-duplicate headlines (same normalized title + day + category)."""

    def norm_title(t: str | None) -> str:
        s = (t or "").lower()
        s = re.sub(r"\s+", " ", s)
        s = re.sub(r"[^a-z0-9 ]", "", s)
        return s[:120]

    def _better(a: NewsItem, b: NewsItem) -> bool:
        ra = _MATCH_TIER_RANK.get(a.match_tier, 0)
        rb = _MATCH_TIER_RANK.get(b.match_tier, 0)
        if ra != rb:
            return ra > rb
        if float(a.confidence or 0) != float(b.confidence or 0):
            return float(a.confidence or 0) > float(b.confidence or 0)
        return (a.published_utc or "") >= (b.published_utc or "")

    seen: dict[tuple[str, str, str | None], NewsItem] = {}
    for item in items:
        day = (item.published_utc or "")[:10]
        key = (norm_title(item.title), day, item.category)
        prev = seen.get(key)
        if prev is None:
            if not item.source_count:
                item.source_count = 1
            if item.publisher and not item.source_publishers:
                item.source_publishers = [item.publisher]
            seen[key] = item
            continue
        a, b = (item, prev) if _better(item, prev) else (prev, item)
        merged = NewsItem(**asdict(a))
        if _MATCH_TIER_RANK.get(a.match_tier) != _MATCH_TIER_RANK.get(b.match_tier) and a.tickers:
            merged.tickers = sorted({*(a.tickers or [])})
        else:
            merged.tickers = sorted({*(a.tickers or []), *(b.tickers or [])})
        merged.source_count = int(a.source_count or 1) + int(b.source_count or 1)
        merged.source_publishers = sorted(
            {
                *(a.source_publishers or []),
                *(b.source_publishers or []),
                *([a.publisher] if a.publisher else []),
                *([b.publisher] if b.publisher else []),
            }
        )
        seen[key] = merged

    out = list(seen.values())
    out.sort(key=lambda x: (x.published_utc or ""), reverse=True)
    return out


def collapse_news_by_ticker_category(items: list[NewsItem]) -> tuple[list[NewsItem], int]:
    """Collapse delisting/symbol_change feed spam to one row per (category,ticker).

    Keeps the strongest representative (match-tier, confidence, recency), while
    preserving source_count/source_publishers and appending merged-source count
    to the summary text.
    """
    collapse_cats = {"delisting", "symbol_change"}
    grouped: dict[tuple[str, str], list[NewsItem]] = {}
    passthrough: list[NewsItem] = []
    for it in items:
        ticks = sorted({t for t in (it.tickers or []) if t})
        if not ticks or it.category not in collapse_cats:
            passthrough.append(it)
            continue
        for t in ticks:
            key = (it.category, t)
            grouped.setdefault(key, []).append(it)

    collapsed: list[NewsItem] = []
    collapsed_rows = 0
    for (cat, ticker), rows in grouped.items():
        if len(rows) == 1:
            only = NewsItem(**asdict(rows[0]))
            only.tickers = [ticker]
            collapsed.append(only)
            continue
        rows.sort(
            key=lambda it: (
                _MATCH_TIER_RANK.get(it.match_tier, 0),
                float(it.confidence or 0.0),
                (it.published_utc or ""),
            ),
            reverse=True,
        )
        keep = NewsItem(**asdict(rows[0]))
        keep.tickers = [ticker]
        dropped = rows[1:]
        collapsed_rows += len(dropped)
        pubs = sorted(
            {
                *(keep.source_publishers or []),
                *( [keep.publisher] if keep.publisher else [] ),
                *[
                    p
                    for d in dropped
                    for p in (
                        (d.source_publishers or [])
                        + ([d.publisher] if d.publisher else [])
                    )
                ],
            }
        )
        total_sources = int(keep.source_count or 1) + sum(int(d.source_count or 1) for d in dropped)
        keep.source_count = total_sources
        keep.source_publishers = pubs
        if len(rows) > 1:
            extra = len(rows) - 1
            if keep.summary:
                keep.summary = f"{keep.summary} (+{extra} related sources)"
            else:
                keep.summary = f"+{extra} related sources"
        collapsed.append(keep)

    out = passthrough + collapsed
    out.sort(key=lambda x: (x.published_utc or ""), reverse=True)
    return out, collapsed_rows


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
    underlyings_set = _underlying_symbols_as_set(underlying_map.values())
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
                underlyings_set,
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
        # Evict prior delistings whose ticker is back in today's active
        # screener.  Those were almost always ticker-reuse false-positives from
        # Polygon's inactive-tickers feed (e.g. an old "Advance Auto Parts WI"
        # stub sharing AAPW with today's live AAPL weekly-pay ETF).  Without
        # this cleanup they'd persist forever via the prior-record merge.
        current_live_for_prune = _load_current_universe()
        before_prune = len(prior_delistings)
        pruned = {
            eid: ev
            for eid, ev in prior_delistings.items()
            if ev.ticker not in current_live_for_prune
        }
        dropped = before_prune - len(pruned)
        if dropped:
            LOGGER.info(
                "  evicted %d prior delisting records for tickers now active in screener (false-positives)",
                dropped,
            )
        prior_delistings = pruned
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
        # Re-load fresh Google News symbol-change events every run; including
        # them from the prior file caused hundreds of spurious ``TOPW``-style
        # pending rows to persist after we tightened Phase 5.  Polygon-issued
        # renames (source=polygon) stay pinned across runs; Google News renames
        # are only emitted for explicit/high ``match_tier`` and will re-appear
        # while the headline is still in the RSS lookback.
        gnews_dropped = sum(
            1 for e in prior_symbol_changes.values() if (e.source or "") == "google_news"
        )
        prior_only_polygon = {
            eid: ev
            for eid, ev in prior_symbol_changes.items()
            if (ev.source or "") == "polygon"
        }
        if gnews_dropped:
            LOGGER.info(
                "  dropping %d prior symbol_change records from Google News (re-emit in Phase 5 if qualified)",
                gnews_dropped,
            )
        LOGGER.info(
            "  merging %d prior polygon symbol_change records",
            len(prior_only_polygon),
        )
        symbol_change_events = list(symbol_change_events) + list(prior_only_polygon.values())

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
    collapsed_duplicates_by_ticker = 0
    if news:
        news = dedupe_news(news)
        link_news_to_events(news, events)
        news, collapsed_duplicates_by_ticker = collapse_news_by_ticker_category(news)
    LOGGER.info(
        "News items after classify + dedupe: %d (polygon + google-news merged, collapsed_duplicates_by_ticker=%d)",
        len(news),
        collapsed_duplicates_by_ticker,
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

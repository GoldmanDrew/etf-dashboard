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
    phase_1_splits       -- Polygon /v3/reference/splits bulk sweep (date-windowed),
                            filtered to our universe client-side
    phase_2_delistings   -- Polygon /v3/reference/tickers?active=false bulk sweep
    phase_3_news         -- Polygon /v2/reference/news bulk sweep (date-windowed),
                            capped by page budget; each article's tickers[] array is
                            intersected with our universe and regex-classified
    merge / dedupe       -- attach linked_event_id, collapse duplicate headlines
    persist              -- write both JSON files

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
OUT_EVENTS = DATA_DIR / "corporate_actions.json"
OUT_NEWS = DATA_DIR / "etf_news.json"

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
    """Structured corporate-action event.  Surfaced in the pinned strip."""

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
    """Pull inactive ETFs from Polygon and intersect with our universe."""
    raw = _bulk_paginate(
        session,
        "https://api.polygon.io/v3/reference/tickers",
        {
            "type": "ETF",
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
    for r in raw:
        sym = _norm(r.get("ticker") or "")
        if sym not in universe:
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
        "delistings: kept=%d (of %d inactive ETFs returned by Polygon)",
        len(events),
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
# Merge / dedupe
# ---------------------------------------------------------------------------

def dedupe_events(events: list[CorporateEvent]) -> list[CorporateEvent]:
    by_id: dict[str, CorporateEvent] = {}
    for ev in events:
        prev = by_id.get(ev.id)
        if prev is None:
            by_id[ev.id] = ev
            continue
        # Prefer the record with a populated execution_date.
        if not prev.execution_date and ev.execution_date:
            by_id[ev.id] = ev
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
    news: list[NewsItem] = []
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
                session, set(tickers), bucket_map, underlying_map
            )

        if args.skip_news:
            LOGGER.info("Phase 3: news SKIPPED")
        else:
            LOGGER.info("Phase 3: news")
            news = phase_3_news(session, tickers, bucket_map)
    except RateLimitExceeded as e:
        aborted_reason = str(e)
        LOGGER.error("Ingest aborted due to rate-limit wall: %s", e)

    events = dedupe_events(split_events + delisting_events)
    LOGGER.info(
        "Events after dedupe: %d (splits=%d delistings=%d)",
        len(events), len(split_events), len(delisting_events),
    )

    if news:
        news = dedupe_news(news)
        link_news_to_events(news, events)
    LOGGER.info("News items after classify + dedupe: %d", len(news))

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

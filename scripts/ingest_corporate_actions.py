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

Pipeline phases:
    phase_1_splits       -- Polygon v3 splits for every ticker (forward + reverse)
    phase_2_delistings   -- Polygon v3 inactive tickers intersected with our universe
    phase_3_news         -- Polygon v2 news per ticker, regex-classified
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
HTTP_TIMEOUT_SEC = int(os.getenv("ETF_METRICS_HTTP_TIMEOUT_SEC", "20"))
HTTP_RETRY_TOTAL = int(os.getenv("ETF_METRICS_HTTP_RETRY_TOTAL", "2"))
SLEEP_BETWEEN_CALLS = float(os.getenv("CORP_ACTIONS_SLEEP_SEC", "0.12"))
NEWS_WINDOW_DAYS = int(os.getenv("CORP_ACTIONS_NEWS_WINDOW_DAYS", "60"))
NEWS_PAGE_LIMIT = int(os.getenv("CORP_ACTIONS_NEWS_PAGE_LIMIT", "50"))


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


def _polygon_get(session: requests.Session, url: str, params: dict | None = None) -> dict | None:
    """GET with retry/backoff.  Returns parsed JSON or None on unrecoverable failure."""
    if not POLYGON_API_KEY:
        LOGGER.error("POLYGON_API_KEY is not set; corporate-actions ingest cannot run.")
        return None
    p = dict(params or {})
    p.setdefault("apiKey", POLYGON_API_KEY)
    last_err: Exception | None = None
    for attempt in range(HTTP_RETRY_TOTAL + 1):
        try:
            resp = session.get(url, params=p, timeout=HTTP_TIMEOUT_SEC)
            if resp.status_code == 429:
                time.sleep(1.5 * (attempt + 1))
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(0.5 * (attempt + 1))
    LOGGER.warning("polygon GET failed: url=%s err=%s", url, last_err)
    return None


# ---------------------------------------------------------------------------
# Phase 1 - splits (forward + reverse)
# ---------------------------------------------------------------------------

def fetch_splits_for_ticker(
    session: requests.Session,
    ticker: str,
    *,
    lookback_years: int = 5,
) -> list[dict]:
    """Polygon v3 splits for a single ticker, newest first.  Returns raw records."""
    url = "https://api.polygon.io/v3/reference/splits"
    cutoff = (datetime.now(UTC) - timedelta(days=365 * lookback_years)).date().isoformat()
    params = {
        "ticker": ticker,
        "execution_date.gte": cutoff,
        "order": "desc",
        "limit": 1000,
        "sort": "execution_date",
    }
    payload = _polygon_get(session, url, params)
    if not payload:
        return []
    return payload.get("results") or []


def phase_1_splits(
    session: requests.Session,
    tickers: list[str],
    bucket_map: dict[str, str | None],
    underlying_map: dict[str, str | None],
) -> list[CorporateEvent]:
    events: list[CorporateEvent] = []
    for i, sym in enumerate(tickers, start=1):
        raw = fetch_splits_for_ticker(session, sym)
        for r in raw:
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
                # Represent as 1-for-N where N = split_from / split_to.
                ratio_n = split_from / split_to
                ratio_label = f"1-for-{ratio_n:g}"
            else:
                ratio_n = split_to / split_from
                ratio_label = f"{ratio_n:g}-for-1"
            headline = f"{sym} {ratio_label} {'reverse split' if is_reverse else 'stock split'} effective {execution_date}"
            ev = CorporateEvent(
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
            events.append(ev)
        if i % 50 == 0 or i == len(tickers):
            LOGGER.info("  splits progress: %d/%d (events_so_far=%d)", i, len(tickers), len(events))
        if SLEEP_BETWEEN_CALLS > 0 and i < len(tickers):
            time.sleep(SLEEP_BETWEEN_CALLS)
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
    url = "https://api.polygon.io/v3/reference/tickers"
    params = {
        "type": "ETF",
        "market": "stocks",
        "active": "false",
        "order": "desc",
        "sort": "last_updated_utc",
        "limit": 1000,
    }
    events: list[CorporateEvent] = []
    pages_fetched = 0
    while True:
        payload = _polygon_get(session, url, params if pages_fetched == 0 else None)
        if not payload:
            break
        results = payload.get("results") or []
        for r in results:
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
            ev = CorporateEvent(
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
            events.append(ev)

        # pagination
        next_url = payload.get("next_url")
        if not next_url:
            break
        # Polygon's next_url already embeds query params; re-attach apiKey.
        if "apiKey=" not in next_url:
            sep = "&" if "?" in next_url else "?"
            next_url = f"{next_url}{sep}apiKey={POLYGON_API_KEY}"
        url = next_url
        params = None
        pages_fetched += 1
        if pages_fetched > 10:
            LOGGER.warning("delisting pagination exceeded 10 pages; stopping")
            break
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


def fetch_news_for_ticker(session: requests.Session, ticker: str) -> list[dict]:
    url = "https://api.polygon.io/v2/reference/news"
    since_utc = (datetime.now(UTC) - timedelta(days=NEWS_WINDOW_DAYS)).date().isoformat()
    params = {
        "ticker": ticker,
        "published_utc.gte": since_utc,
        "order": "desc",
        "limit": NEWS_PAGE_LIMIT,
        "sort": "published_utc",
    }
    payload = _polygon_get(session, url, params)
    if not payload:
        return []
    return payload.get("results") or []


def phase_3_news(
    session: requests.Session,
    tickers: list[str],
    bucket_map: dict[str, str | None],
) -> list[NewsItem]:
    items: list[NewsItem] = []
    for i, sym in enumerate(tickers, start=1):
        raw = fetch_news_for_ticker(session, sym)
        for r in raw:
            title = (r.get("title") or "").strip()
            description = (r.get("description") or "").strip()
            text = f"{title}\n{description}"
            category, confidence = classify_text(text)
            if not category:
                continue
            art_id = r.get("id") or r.get("article_url") or f"{sym}:{r.get('published_utc')}"
            pub = r.get("publisher") or {}
            # Polygon tags each article with its set of related tickers; keep only
            # those we care about.
            raw_tickers = r.get("tickers") or [sym]
            related = sorted({
                _norm(t) for t in raw_tickers
                if _norm(t) in bucket_map
            })
            if not related:
                related = [sym]
            item = NewsItem(
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
                bucket=bucket_map.get(sym),
            )
            items.append(item)
        if i % 50 == 0 or i == len(tickers):
            LOGGER.info("  news progress: %d/%d (items_so_far=%d)", i, len(tickers), len(items))
        if SLEEP_BETWEEN_CALLS > 0 and i < len(tickers):
            time.sleep(SLEEP_BETWEEN_CALLS)
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
        "Starting corporate-actions ingest: tickers=%d news_window=%dd",
        len(tickers),
        NEWS_WINDOW_DAYS,
    )

    session = build_session()

    LOGGER.info("Phase 1: splits")
    split_events = phase_1_splits(session, tickers, bucket_map, underlying_map)

    if args.skip_delistings:
        LOGGER.info("Phase 2: delistings SKIPPED")
        delisting_events: list[CorporateEvent] = []
    else:
        LOGGER.info("Phase 2: delistings")
        delisting_events = phase_2_delistings(
            session, set(tickers), bucket_map, underlying_map
        )

    events = dedupe_events(split_events + delisting_events)
    LOGGER.info("Events after dedupe: %d (splits=%d delistings=%d)",
                len(events), len(split_events), len(delisting_events))

    if args.skip_news:
        LOGGER.info("Phase 3: news SKIPPED")
        news: list[NewsItem] = []
    else:
        LOGGER.info("Phase 3: news")
        news = phase_3_news(session, tickers, bucket_map)
        news = dedupe_news(news)
        link_news_to_events(news, events)
        LOGGER.info("News items after classify + dedupe: %d", len(news))

    if args.skip_news and OUT_NEWS.exists():
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


if __name__ == "__main__":
    main()

"""
ETF per-position holdings providers.

Mirrors the design of ``etf_providers.py`` (NAV/AUM/Shares) but returns the full
position-level breakdown of each fund: ticker, CUSIP, company, weight, shares,
market value.

Coverage (as of probe run):
  1) TradrAxsHoldingsProvider  -- bulk ``BBH_AXS_ETF_PVAL_WEB.{YYYYMMDD}.csv``
                                  1 HTTP request, all TradrAxs/AXS single-stock ETFs.
  2) DirexionHoldingsProvider  -- per-ticker ``/holdings/{TICKER}.csv``
                                  same CSV already used for NAV/AUM derivation.
  3) YieldMaxHoldingsProvider  -- per-ticker CSV ``?fund_csv_ticker={TICKER}``
                                  (TidalFG_Holdings_{TICKER}.csv).
  4) REXHoldingsProvider       -- per-ticker HTML scrape of the
                                  ``id="top-ten-holdings"`` block on rexshares.com.
  5) DefianceHoldingsProvider  -- per-ticker HTML scrape of the
                                  ``/{ticker}-full-holdings/`` page table.

Roundhill, ProShares, and GraniteShares do not expose a public holdings file we
could scrape cheaply; they're represented by ``UnsupportedHoldingsProvider``
stubs so the orchestrator can report coverage without crashing.

All providers return ``list[HoldingRow]`` -- one row per position -- so the
downstream persistence layer can simply concatenate rows across issuers and
stamp them with the run date.
"""
from __future__ import annotations

import io
import logging
import re
from dataclasses import dataclass, asdict
from datetime import date, datetime
from typing import Iterable, Optional, Protocol

import pandas as pd
import requests

try:
    # Share the same session/headers/retry policy as the NAV stack.
    from etf_providers import _build_session, _get
except ImportError:  # pragma: no cover - allows standalone import during probes
    _build_session = None  # type: ignore[assignment]
    _get = None  # type: ignore[assignment]


LOGGER = logging.getLogger("etf_holdings_providers")


# ---------------------------------------------------------------------------
# Core types
# ---------------------------------------------------------------------------

@dataclass
class HoldingRow:
    as_of_date: date
    etf_ticker: str
    position_ticker: Optional[str]   # e.g. "TSM", "TSLA 260515P00405010"
    security_name: Optional[str]     # e.g. "Taiwan Semiconductor-SP ADR"
    cusip: Optional[str]
    security_type: Optional[str]     # e.g. "COMMON STOCK", "OPTION", "CASH", "SWAP"
    shares: Optional[float]
    price: Optional[float]
    market_value: Optional[float]
    weight_pct: Optional[float]      # e.g. 10.67 (percent, not fraction)
    source: str                      # provider name
    source_url: str


class HoldingsProvider(Protocol):
    name: str

    def supports_ticker(self, ticker: str, as_of: date) -> bool: ...
    def fetch_holdings(self, ticker: str, as_of: date) -> list[HoldingRow]: ...


def _parse_pct(s: object) -> Optional[float]:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    txt = str(s).strip().replace("%", "")
    if not txt:
        return None
    try:
        return float(txt.replace(",", ""))
    except ValueError:
        return None


def _parse_float(s: object) -> Optional[float]:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    txt = str(s).strip().replace(",", "").replace("$", "")
    if not txt or txt == "-":
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _parse_date(s: object) -> Optional[date]:
    if s is None:
        return None
    t = str(s).strip()
    if not t:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y%m%d"):
        try:
            return datetime.strptime(t, fmt).date()
        except ValueError:
            continue
    try:
        return pd.to_datetime(t, errors="coerce").date()
    except Exception:
        return None


def _get_or_default_session(session: requests.Session | None) -> requests.Session:
    if session is not None:
        return session
    if _build_session is not None:
        return _build_session()
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36"
        ),
        "Accept": "text/csv,application/json,text/html,*/*;q=0.8",
    })
    s.timeout_sec = 15  # type: ignore[attr-defined]
    return s


def _session_get(session: requests.Session, url: str, *, extra_headers: dict | None = None):
    if _get is not None:
        return _get(session, url, extra_headers=extra_headers)
    timeout = getattr(session, "timeout_sec", 15)
    return session.get(url, timeout=timeout, headers=extra_headers or None)


# ---------------------------------------------------------------------------
# 1) TradrAxs / AXS — bulk per-day CSV
# ---------------------------------------------------------------------------

class TradrAxsHoldingsProvider:
    """Single bulk CSV covering all AXS/Tradr ETFs for a given date.

    URL: https://axsetf.filepoint.live/assets/data/BBH_AXS_ETF_PVAL_WEB.{YYYYMMDD}.csv

    Columns observed in production (trading day 2026-04-20):
        ETF Ticker, Date, ISIN, CUSIP, SEDOL, Ticker, Description,
        Security Type, Market Value, Maturity Date, Shares, Security Price,
        Asset Currency, Shares Outstanding, Total Net Assets, Market Value Weight
    """

    name = "tradr_axs"

    def __init__(self, session: requests.Session | None = None):
        self.session = _get_or_default_session(session)
        self._df_cache: dict[date, pd.DataFrame | None] = {}
        self._ticker_index: dict[date, set[str]] = {}

    @staticmethod
    def _url(as_of: date) -> str:
        return (
            "https://axsetf.filepoint.live/assets/data/"
            f"BBH_AXS_ETF_PVAL_WEB.{as_of.strftime('%Y%m%d')}.csv"
        )

    def _load(self, as_of: date) -> pd.DataFrame | None:
        if as_of in self._df_cache:
            return self._df_cache[as_of]
        url = self._url(as_of)
        try:
            r = _session_get(self.session, url)
            if r.status_code != 200 or not r.text:
                self._df_cache[as_of] = None
                return None
            df = pd.read_csv(io.StringIO(r.text))
            df.columns = [c.strip() for c in df.columns]
            if "ETF Ticker" not in df.columns:
                self._df_cache[as_of] = None
                return None
            df["ETF Ticker"] = df["ETF Ticker"].astype(str).str.upper().str.strip()
            self._df_cache[as_of] = df
            self._ticker_index[as_of] = set(df["ETF Ticker"].unique().tolist())
            return df
        except Exception as e:
            LOGGER.debug("TradrAxs holdings load failed for %s: %s", as_of, e)
            self._df_cache[as_of] = None
            return None

    def supports_ticker(self, ticker: str, as_of: date) -> bool:
        self._load(as_of)
        return ticker.upper() in self._ticker_index.get(as_of, set())

    def fetch_holdings(self, ticker: str, as_of: date) -> list[HoldingRow]:
        df = self._load(as_of)
        if df is None:
            return []
        t = ticker.upper()
        sub = df[df["ETF Ticker"] == t]
        if sub.empty:
            return []
        url = self._url(as_of)
        out: list[HoldingRow] = []
        for _, row in sub.iterrows():
            row_date = _parse_date(row.get("Date")) or as_of
            out.append(HoldingRow(
                as_of_date=row_date,
                etf_ticker=t,
                position_ticker=(str(row.get("Ticker")).strip() or None) if pd.notna(row.get("Ticker")) else None,
                security_name=(str(row.get("Description")).strip() or None) if pd.notna(row.get("Description")) else None,
                cusip=(str(row.get("CUSIP")).strip() or None) if pd.notna(row.get("CUSIP")) else None,
                security_type=(str(row.get("Security Type")).strip() or None) if pd.notna(row.get("Security Type")) else None,
                shares=_parse_float(row.get("Shares")),
                price=_parse_float(row.get("Security Price")),
                market_value=_parse_float(row.get("Market Value")),
                weight_pct=_parse_pct(row.get("Market Value Weight")),
                source=self.name,
                source_url=url,
            ))
        return out


# ---------------------------------------------------------------------------
# 2) Direxion — per-ticker /holdings/{TICKER}.csv
# ---------------------------------------------------------------------------

class DirexionHoldingsProvider:
    """Direxion publishes a per-fund CSV at https://www.direxion.com/holdings/{T}.csv.

    Format: two header lines (fund name, ticker), a shares-outstanding line, a blank,
    then the standard ``TradeDate,AccountTicker,StockTicker,SecurityDescription,
    Shares,Price,MarketValue,Cusip,HoldingsPercent`` header followed by per-position rows.
    """

    name = "direxion"

    def __init__(self, session: requests.Session | None = None):
        self.session = _get_or_default_session(session)
        self._cache: dict[str, tuple[list[HoldingRow], str]] = {}

    @staticmethod
    def _url(ticker: str) -> str:
        return f"https://www.direxion.com/holdings/{ticker.upper()}.csv"

    def supports_ticker(self, ticker: str, as_of: date) -> bool:
        # Same heuristic as DirexionProvider in etf_providers.py — we still attempt the URL
        # even if the ticker isn't in a hard-coded list, but we avoid that here to keep
        # per-run costs down when the universe is large.
        try:
            from etf_providers import DirexionProvider
            return ticker.upper() in DirexionProvider.KNOWN_TICKERS
        except ImportError:
            return True

    def fetch_holdings(self, ticker: str, as_of: date) -> list[HoldingRow]:
        t = ticker.upper()
        if t in self._cache:
            rows, _ = self._cache[t]
            return rows
        url = self._url(t)
        try:
            r = _session_get(
                self.session, url,
                extra_headers={"Referer": "https://www.direxion.com/"},
            )
            if r.status_code != 200 or not r.text or not r.text.startswith(("Direxion", '"Direxion')):
                self._cache[t] = ([], url)
                return []
            lines = r.text.splitlines()
            hdr_idx = None
            for i, ln in enumerate(lines):
                if "TradeDate" in ln and "MarketValue" in ln:
                    hdr_idx = i
                    break
            if hdr_idx is None:
                self._cache[t] = ([], url)
                return []
            df = pd.read_csv(io.StringIO("\n".join(lines[hdr_idx:])))
            rows: list[HoldingRow] = []
            for _, row in df.iterrows():
                row_date = _parse_date(row.get("TradeDate")) or as_of
                pct = _parse_pct(row.get("HoldingsPercent"))
                rows.append(HoldingRow(
                    as_of_date=row_date,
                    etf_ticker=t,
                    position_ticker=(
                        str(row.get("StockTicker")).strip() or None
                    ) if pd.notna(row.get("StockTicker")) else None,
                    security_name=(
                        str(row.get("SecurityDescription")).strip() or None
                    ) if pd.notna(row.get("SecurityDescription")) else None,
                    cusip=(str(row.get("Cusip")).strip() or None) if pd.notna(row.get("Cusip")) else None,
                    security_type=None,
                    shares=_parse_float(row.get("Shares")),
                    price=_parse_float(row.get("Price")),
                    market_value=_parse_float(row.get("MarketValue")),
                    weight_pct=pct,
                    source=self.name,
                    source_url=url,
                ))
            self._cache[t] = (rows, url)
            return rows
        except Exception as e:
            LOGGER.debug("Direxion holdings failed for %s: %s", t, e)
            self._cache[t] = ([], url)
            return []


# ---------------------------------------------------------------------------
# 3) YieldMax — per-ticker CSV export
# ---------------------------------------------------------------------------

class YieldMaxHoldingsProvider:
    """YieldMax exposes a CSV download per fund at
    ``https://yieldmaxetfs.com/?fund_csv_ticker={TICKER}`` — the
    ``TidalFG_Holdings_{TICKER}.csv`` file with columns:

        Date, Account, StockTicker, CUSIP, SecurityName, Shares, Price,
        MarketValue, Weightings, NetAssets, SharesOutstanding, CreationUnits

    The endpoint responds 200 only when a ``Referer`` header pointing at the
    fund page is attached; the HTTP session re-use keeps the cookie flow happy.
    """

    name = "yieldmax"

    def __init__(self, session: requests.Session | None = None):
        self.session = _get_or_default_session(session)
        self._cache: dict[str, tuple[list[HoldingRow], str]] = {}

    @staticmethod
    def _csv_url(ticker: str) -> str:
        return f"https://yieldmaxetfs.com/?fund_csv_ticker={ticker.upper()}"

    @staticmethod
    def _page_url(ticker: str) -> str:
        return f"https://yieldmaxetfs.com/our-etfs/{ticker.lower()}/"

    def supports_ticker(self, ticker: str, as_of: date) -> bool:
        try:
            from etf_providers import YieldMaxProvider
            return ticker.upper() in YieldMaxProvider.KNOWN_TICKERS
        except ImportError:
            return True

    def fetch_holdings(self, ticker: str, as_of: date) -> list[HoldingRow]:
        t = ticker.upper()
        if t in self._cache:
            rows, _ = self._cache[t]
            return rows
        csv_url = self._csv_url(t)
        try:
            r = _session_get(
                self.session, csv_url,
                extra_headers={"Referer": self._page_url(t), "Accept": "text/csv,*/*;q=0.5"},
            )
            if r.status_code != 200 or not r.text:
                self._cache[t] = ([], csv_url + f"?status={r.status_code}")
                return []
            ct = r.headers.get("content-type", "")
            if "csv" not in ct and "," not in r.text[:200]:
                self._cache[t] = ([], csv_url + "?err=not-csv")
                return []
            # strip BOM if present
            body = r.text.lstrip("\ufeff")
            df = pd.read_csv(io.StringIO(body))
            df.columns = [c.strip() for c in df.columns]
            rows: list[HoldingRow] = []
            for _, row in df.iterrows():
                row_date = _parse_date(row.get("Date")) or as_of
                ticker_col = row.get("StockTicker")
                pos_ticker = None
                if pd.notna(ticker_col):
                    pos_ticker = str(ticker_col).strip().strip('"').strip() or None
                rows.append(HoldingRow(
                    as_of_date=row_date,
                    etf_ticker=t,
                    position_ticker=pos_ticker,
                    security_name=(
                        str(row.get("SecurityName")).strip() or None
                    ) if pd.notna(row.get("SecurityName")) else None,
                    cusip=(
                        str(row.get("CUSIP")).strip() or None
                    ) if pd.notna(row.get("CUSIP")) else None,
                    security_type=None,
                    shares=_parse_float(row.get("Shares")),
                    price=_parse_float(row.get("Price")),
                    market_value=_parse_float(row.get("MarketValue")),
                    weight_pct=_parse_pct(row.get("Weightings")),
                    source=self.name,
                    source_url=csv_url,
                ))
            self._cache[t] = (rows, csv_url)
            return rows
        except Exception as e:
            LOGGER.debug("YieldMax holdings failed for %s: %s", t, e)
            self._cache[t] = ([], csv_url + f"?exc={type(e).__name__}")
            return []


# ---------------------------------------------------------------------------
# 4) REX Shares — per-ticker HTML scrape of top-ten-holdings
# ---------------------------------------------------------------------------

class REXHoldingsProvider:
    """REX Shares embeds a ``<div id="top-ten-holdings">`` block on each fund
    page with six columns:

        Symbol | Name | Security Identifier | Weighting | Net Value | Shares Held

    It's not a ``<table>`` — each cell is a ``<div class="t-col t-data">`` —
    so we reconstruct rows by grouping six data cells at a time after the
    single header row.
    """

    name = "rex_shares"

    _CELL_RE = re.compile(
        r'<div[^>]*class="t-col\s+t-(?P<kind>header|data)"[^>]*>(?P<text>[\s\S]*?)</div>',
        re.IGNORECASE,
    )

    def __init__(self, session: requests.Session | None = None):
        self.session = _get_or_default_session(session)
        self._cache: dict[str, tuple[list[HoldingRow], str]] = {}

    @staticmethod
    def _url(ticker: str) -> str:
        return f"https://www.rexshares.com/{ticker.upper()}/"

    def supports_ticker(self, ticker: str, as_of: date) -> bool:
        try:
            from etf_providers import REXSharesProvider
            return ticker.upper() in REXSharesProvider.KNOWN_TICKERS
        except ImportError:
            return True

    @staticmethod
    def _extract_block(html: str) -> str | None:
        m = re.search(
            r'(?is)<div[^>]*id="top-ten-holdings"[^>]*>(.*?)(?:</div>\s*){3}',
            html,
        )
        return m.group(1) if m else None

    def fetch_holdings(self, ticker: str, as_of: date) -> list[HoldingRow]:
        t = ticker.upper()
        if t in self._cache:
            rows, _ = self._cache[t]
            return rows
        url = self._url(t)
        try:
            r = _session_get(self.session, url)
            if r.status_code != 200 or not r.text or 'id="top-ten-holdings"' not in r.text:
                self._cache[t] = ([], url + f"?status={r.status_code}")
                return []
            block = self._extract_block(r.text)
            if not block:
                self._cache[t] = ([], url + "?err=no-block")
                return []
            as_of_m = re.search(r'class="as_of_date"[^>]*>As of\s*([^<]+)</div>', block, re.I)
            row_date = _parse_date(as_of_m.group(1).strip()) if as_of_m else as_of
            cells = [m.group("text").strip() for m in self._CELL_RE.finditer(block)]
            # First 6 cells are headers ("Symbol", "Name", ...). Discard them; remaining
            # cells are data in groups of 6.
            data_cells = cells[6:] if len(cells) >= 6 else cells
            rows: list[HoldingRow] = []
            for i in range(0, len(data_cells) - 5, 6):
                sym, name, sec_id, weight_s, netval_s, shares_s = data_cells[i:i + 6]
                weight = _parse_pct(weight_s)
                mkt_val = _parse_float(netval_s)
                sh = _parse_float(shares_s)
                # Skip the trailing empty row some pages include.
                if not any([sym, name, sec_id, weight, mkt_val, sh]):
                    continue
                rows.append(HoldingRow(
                    as_of_date=row_date or as_of,
                    etf_ticker=t,
                    position_ticker=sym or None,
                    security_name=name or None,
                    cusip=(sec_id or None),
                    security_type=None,
                    shares=sh,
                    price=None,
                    market_value=mkt_val,
                    weight_pct=weight,
                    source=self.name,
                    source_url=url,
                ))
            self._cache[t] = (rows, url)
            return rows
        except Exception as e:
            LOGGER.debug("REX holdings failed for %s: %s", t, e)
            self._cache[t] = ([], url + f"?exc={type(e).__name__}")
            return []


# ---------------------------------------------------------------------------
# 5) Defiance — per-ticker /{ticker}-full-holdings/ HTML scrape
# ---------------------------------------------------------------------------

class DefianceHoldingsProvider:
    """Defiance publishes a ``<table id="table-full-holdings">`` on each fund's
    ``/{ticker}-full-holdings/`` page with columns:

        Ticker | Name | CUSIP | ETF Weight | Shares

    Market value is not provided; we leave it null for now (can be derived from
    NAV × shares once the position price is known, but that's out of scope here).
    """

    name = "defiance"

    _ROW_RE = re.compile(
        r"(?is)<tr[^>]*>\s*"
        r"<td[^>]*>(?P<ticker>[\s\S]*?)</td>\s*"
        r"<td[^>]*>(?P<name>[\s\S]*?)</td>\s*"
        r"<td[^>]*>(?P<cusip>[\s\S]*?)</td>\s*"
        r"<td[^>]*>(?P<weight>[\s\S]*?)</td>\s*"
        r"<td[^>]*>(?P<shares>[\s\S]*?)</td>\s*"
        r"</tr>"
    )
    _TABLE_RE = re.compile(
        r'(?is)<table[^>]*id="table-full-holdings"[^>]*>(.*?)</table>'
    )

    def __init__(self, session: requests.Session | None = None):
        self.session = _get_or_default_session(session)
        self._cache: dict[str, tuple[list[HoldingRow], str]] = {}

    @staticmethod
    def _url(ticker: str) -> str:
        return f"https://www.defianceetfs.com/{ticker.lower()}-full-holdings/"

    def supports_ticker(self, ticker: str, as_of: date) -> bool:
        try:
            from etf_providers import DefianceProvider
            # DefianceProvider's catalog is loaded lazily; trigger it.
            probe = DefianceProvider(self.session)
            return probe.supports_ticker(ticker, as_of)
        except Exception:
            return True

    @staticmethod
    def _strip_tags(s: str) -> str:
        return re.sub(r"<[^>]+>", "", s).strip()

    def fetch_holdings(self, ticker: str, as_of: date) -> list[HoldingRow]:
        t = ticker.upper()
        if t in self._cache:
            rows, _ = self._cache[t]
            return rows
        url = self._url(t)
        try:
            r = _session_get(
                self.session, url,
                extra_headers={"Referer": f"https://www.defianceetfs.com/{t.lower()}/"},
            )
            if r.status_code != 200 or not r.text:
                self._cache[t] = ([], url + f"?status={r.status_code}")
                return []
            tbl_m = self._TABLE_RE.search(r.text)
            if not tbl_m:
                self._cache[t] = ([], url + "?err=no-table")
                return []
            tbl_html = tbl_m.group(1)
            rows: list[HoldingRow] = []
            for m in self._ROW_RE.finditer(tbl_html):
                ticker_s = self._strip_tags(m.group("ticker"))
                # skip header rows
                if ticker_s.lower() == "ticker":
                    continue
                rows.append(HoldingRow(
                    as_of_date=as_of,
                    etf_ticker=t,
                    position_ticker=ticker_s or None,
                    security_name=self._strip_tags(m.group("name")) or None,
                    cusip=self._strip_tags(m.group("cusip")) or None,
                    security_type=None,
                    shares=_parse_float(self._strip_tags(m.group("shares"))),
                    price=None,
                    market_value=None,
                    weight_pct=_parse_pct(self._strip_tags(m.group("weight"))),
                    source=self.name,
                    source_url=url,
                ))
            self._cache[t] = (rows, url)
            return rows
        except Exception as e:
            LOGGER.debug("Defiance holdings failed for %s: %s", t, e)
            self._cache[t] = ([], url + f"?exc={type(e).__name__}")
            return []


# ---------------------------------------------------------------------------
# Stack / orchestrator
# ---------------------------------------------------------------------------

def build_default_holdings_stack(session: requests.Session | None = None) -> list[HoldingsProvider]:
    """Providers are tried in order; the first one that returns a non-empty row
    list wins for that ticker. TradrAxs sits first because its bulk CSV covers
    50+ tickers with a single HTTP request."""
    s = _get_or_default_session(session)
    return [
        TradrAxsHoldingsProvider(s),
        DirexionHoldingsProvider(s),
        YieldMaxHoldingsProvider(s),
        REXHoldingsProvider(s),
        DefianceHoldingsProvider(s),
    ]


HOLDINGS_COLUMNS = [
    "as_of_date",
    "etf_ticker",
    "position_ticker",
    "security_name",
    "cusip",
    "security_type",
    "shares",
    "price",
    "market_value",
    "weight_pct",
    "source",
    "source_url",
]


def _rows_to_df(rows: Iterable[HoldingRow]) -> pd.DataFrame:
    records = [asdict(r) for r in rows]
    if not records:
        return pd.DataFrame(columns=HOLDINGS_COLUMNS)
    df = pd.DataFrame(records)
    for c in HOLDINGS_COLUMNS:
        if c not in df.columns:
            df[c] = None
    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce").dt.date
    df["etf_ticker"] = df["etf_ticker"].astype(str).str.upper()
    return df[HOLDINGS_COLUMNS].reset_index(drop=True)


def fetch_all_holdings(
    tickers: list[str],
    as_of: date,
    stack: list[HoldingsProvider] | None = None,
    *,
    progress_every: int = 25,
) -> pd.DataFrame:
    """Iterate tickers × providers; return a long-form DataFrame of all positions.

    Provider order matters for coverage reporting: a ticker is considered "covered"
    by the first provider that returns any rows for it.
    """
    stack = stack or build_default_holdings_stack()
    all_rows: list[HoldingRow] = []
    coverage: dict[str, str] = {}

    for i, raw in enumerate(tickers, start=1):
        t = str(raw).strip().upper()
        if not t:
            continue
        picked_provider: str | None = None
        for provider in stack:
            try:
                if not provider.supports_ticker(t, as_of):
                    continue
                rows = provider.fetch_holdings(t, as_of)
            except Exception as e:
                LOGGER.warning("holdings provider=%s ticker=%s err=%s", provider.name, t, e)
                continue
            if rows:
                all_rows.extend(rows)
                picked_provider = provider.name
                break
        coverage[t] = picked_provider or "missing"
        if progress_every and i % progress_every == 0:
            covered = sum(1 for v in coverage.values() if v != "missing")
            LOGGER.info(
                "holdings progress: %d/%d tickers processed (%d covered, %d rows so far)",
                i, len(tickers), covered, len(all_rows),
            )

    df = _rows_to_df(all_rows)
    df.attrs["coverage"] = coverage
    return df


__all__ = [
    "HoldingRow",
    "HoldingsProvider",
    "TradrAxsHoldingsProvider",
    "DirexionHoldingsProvider",
    "YieldMaxHoldingsProvider",
    "REXHoldingsProvider",
    "DefianceHoldingsProvider",
    "build_default_holdings_stack",
    "fetch_all_holdings",
    "HOLDINGS_COLUMNS",
]

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


# ---------------------------------------------------------------------------
# Security-type inference
# ---------------------------------------------------------------------------
#
# Levered / option-income issuers frequently publish holdings CSVs whose rows
# are a mix of common-stock positions, total-return swaps, OCC-format option
# contracts, T-bill/money-market balances, and plain cash lines -- but only
# one issuer (TradrAxs) actually labels the ``Security Type`` column.  For
# the rest we have to classify from free-text fields (``security_name``,
# ``position_ticker``, ``cusip``).  The distinction matters a lot for
# downstream analysis: 500 "shares" of a CALL contract is 500 contracts =
# 50 000 shares of underlying exposure, not 500 shares of stock.
#
# We canonicalize to the labels in ``_CANONICAL_TYPES`` so the column has a
# stable vocabulary no matter which provider produced the row.

_CANONICAL_TYPES = {
    "COMMON_STOCK",     # ordinary equity (e.g. "APPLE INC")
    "ADR",              # ADR / sponsored receipt ("TAIWAN SEMI-SP ADR")
    "PREFERRED",
    "ETF",              # fund-of-fund holding
    "OPTION_CALL",      # e.g. "NVDA 260515C01500000" or "CALL NVDA ..."
    "OPTION_PUT",
    "OPTION",           # unclassified option (call/put not resolvable)
    "SWAP",             # total-return / equity swap
    "FUTURE",
    "TREASURY",         # T-bills, T-notes, T-bonds
    "MONEY_MARKET",     # govt/agency MM fund, Invesco STIT, etc.
    "BOND",             # corporate/agency bonds (rare in our universe)
    "CASH",             # pure cash / cash collateral / "CASH & OTHER"
    "OTHER",
}

# OPRA / OCC option symbol tail: ``YYMMDD[CP]XXXXXXXX`` (8-digit strike x 1000).
# Matches whether attached to an underlying (e.g. ``NVDA260510C01500000``)
# or space-separated (``NVDA 260510C01500000``).  We forbid a preceding digit
# so a random 14-digit number can't be misread as an OPRA tail starting in
# the middle, but we intentionally allow a preceding letter so the common
# "stuck-to-underlying" form matches (``\b`` won't fire between letter/digit).
_OPRA_TAIL_RE = re.compile(r"(?<!\d)\d{6}[CP]\d{7,9}\b")

# Narrative option descriptions seen on YieldMax / Defiance holdings files:
# ``CALL TSLA 05/10/2026 200`` or ``TSLA US 05/10/26 C200 Equity``.
# We trigger on the word CALL/PUT *paired* with a ``MM/DD/YYYY`` date so we
# don't false-positive on random words containing "call".
_OPTION_DATE_RE = re.compile(
    r"\b\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}\b",
    re.I,
)
_OPTION_CALL_WORD_RE = re.compile(r"\bCALLS?\b", re.I)
_OPTION_PUT_WORD_RE = re.compile(r"\bPUTS?\b", re.I)
# Bloomberg-style "C200 Equity" / "P200 Equity" suffix form.
_OPTION_BLP_CALL_RE = re.compile(r"\bC\s*\d+(?:\.\d+)?\s+Equity\b", re.I)
_OPTION_BLP_PUT_RE = re.compile(r"\bP\s*\d+(?:\.\d+)?\s+Equity\b", re.I)

_SWAP_RE = re.compile(
    r"\b(SWAP|TOTAL RETURN SWAP|\bTRS\b|EQUITY SWAP|PORTFOLIO SWAP)\b",
    re.I,
)
_FUTURE_RE = re.compile(r"\b(FUTURE|FUTURES|\bFUT\b|E-MINI|ES FUT|CME FUT)\b", re.I)
_TREASURY_RE = re.compile(
    r"\b(U\.?S\.?\s*TREAS|TREASURY\b|T[-\s]?BILL|TBILL|US\s*T-?BILL|UST\s+\d|"
    r"TREASURY\s*NOTE|TREASURY\s*BOND|GOVT\s+BOND)\b",
    re.I,
)
_MM_RE = re.compile(
    r"\b(MONEY\s*MARKET|MMKT|GOVT\s*&?\s*AGCY|AGCY\s*PORT|STIT-?GOVT|"
    r"INVESCO\s+SHORT\s+TERM|FEDERATED\s+GOVT|GOLDMAN\s+FS\s+GOVT|"
    r"FIRST\s+AMERICAN\s+GOVT\s+OBLIG|DREYFUS\s+GOVT|FIDELITY\s+GOVT)\b",
    re.I,
)
_CASH_RE = re.compile(
    r"\b(CASH|CASH\s*&\s*OTHER|CASH\s*COLLATERAL|USD\s*CASH|NET\s*CASH|"
    r"FOREIGN\s*CURRENCY|CURRENCY\s*POSITION)\b",
    re.I,
)
_ADR_RE = re.compile(r"\b(ADR|SPONSORED\s*ADR|SP\s*ADR)\b", re.I)
_PREF_RE = re.compile(r"\b(PREFERRED|PFD|PREF\s+SHS)\b", re.I)
_BOND_RE = re.compile(
    r"\b\d+(?:\.\d+)?%\s+\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}\b",  # "5.25% 03/15/2029"
    re.I,
)

# Recognize bare cash/money-market tickers some issuers publish as the position
# ticker rather than a CUSIP, e.g. "XXUSD" (USD cash bucket in Bloomberg PORT).
_CASH_TICKERS = {"CASH", "USD", "XXUSD", "USDCASH", "-", "N/A"}


def _infer_security_type(
    *,
    position_ticker: Optional[str],
    security_name: Optional[str],
    cusip: Optional[str] = None,
    raw_type: Optional[str] = None,
) -> str:
    """Return one of :data:`_CANONICAL_TYPES` for a holdings row.

    Inference order matches expected frequency-of-appearance in our universe
    (options + swaps dominate YieldMax/Direxion/REX; cash/T-bills dominate
    their collateral baskets; stocks dominate AXS/Tradr single-stock ETFs).

    ``raw_type`` (if present) wins — it's the issuer's own label, normalized
    to the canonical vocab.  Otherwise we regex over ``security_name`` +
    ``position_ticker`` to infer.
    """
    # 1. Trust the issuer's own label if present.
    if raw_type:
        t = re.sub(r"\s+", "_", raw_type.strip().upper())
        # Common AXS/Tradr labels we see in BBH_AXS_ETF_PVAL_WEB:
        # "COMMON_STOCK", "OPTION", "CASH", "SWAP"
        alias = {
            "COMMON_STOCK": "COMMON_STOCK",
            "COMMONSTOCK": "COMMON_STOCK",
            "EQUITY": "COMMON_STOCK",
            "STOCK": "COMMON_STOCK",
            "OPTIONS": "OPTION",
            "OPTION": "OPTION",
            "CALL_OPTION": "OPTION_CALL",
            "PUT_OPTION": "OPTION_PUT",
            "CASH": "CASH",
            "CASH_EQUIVALENT": "CASH",
            "SWAP": "SWAP",
            "SWAPS": "SWAP",
            "TOTAL_RETURN_SWAP": "SWAP",
            "TOTAL-RETURN_SWAP": "SWAP",
            "FUTURE": "FUTURE",
            "FUTURES": "FUTURE",
            "TREASURY": "TREASURY",
            "US_TREASURY": "TREASURY",
            "U.S._TREASURY": "TREASURY",
            "MONEY_MARKET": "MONEY_MARKET",
            "MONEY-MARKET": "MONEY_MARKET",
            "PREFERRED": "PREFERRED",
            "PREFERRED_STOCK": "PREFERRED",
            "ADR": "ADR",
            "BOND": "BOND",
            "ETF": "ETF",
        }
        if t in alias:
            return alias[t]
        if t in _CANONICAL_TYPES:
            return t

    name = str(security_name or "")
    ticker = str(position_ticker or "")
    haystack = f"{ticker}  {name}".strip()
    t_up = ticker.strip().upper()

    # Classification proceeds from most-specific signal to most-generic so a
    # swap line that happens to carry ``N/A`` as its position_ticker doesn't
    # get misfiled as CASH just because of the placeholder.

    # 1. Options: OPRA / OCC tail is the strongest signal.
    opra = _OPRA_TAIL_RE.search(ticker) or _OPRA_TAIL_RE.search(name)
    if opra:
        tail = opra.group(0)
        cp = tail[6:7].upper()  # 7th char of match: the C/P flag
        if cp == "C":
            return "OPTION_CALL"
        if cp == "P":
            return "OPTION_PUT"
        return "OPTION"
    # 1b. Narrative option descriptions: word CALL/PUT + an explicit date,
    #     or Bloomberg-style "C<strike> Equity" / "P<strike> Equity" suffix.
    if _OPTION_BLP_CALL_RE.search(haystack):
        return "OPTION_CALL"
    if _OPTION_BLP_PUT_RE.search(haystack):
        return "OPTION_PUT"
    if _OPTION_DATE_RE.search(haystack):
        if _OPTION_CALL_WORD_RE.search(haystack):
            return "OPTION_CALL"
        if _OPTION_PUT_WORD_RE.search(haystack):
            return "OPTION_PUT"

    # 2. Swaps (check before anything cash-ticker-like, since swap lines often
    #    carry ``N/A`` / blank position tickers).
    if _SWAP_RE.search(haystack):
        return "SWAP"

    # 3. Futures.
    if _FUTURE_RE.search(haystack):
        return "FUTURE"

    # 4. Treasuries before money-market (both often match "Government").
    if _TREASURY_RE.search(haystack):
        return "TREASURY"
    if _MM_RE.search(haystack):
        return "MONEY_MARKET"

    # 5. Cash collateral / FX currency (from name).
    if _CASH_RE.search(haystack):
        return "CASH"

    # 6. Known-cash tickers (fallback for rows where name is blank).
    if t_up in _CASH_TICKERS and not _SWAP_RE.search(name):
        return "CASH"

    # 7. Fixed-rate corporate / agency bonds ("5.25% 03/15/2029" pattern).
    if _BOND_RE.search(name):
        return "BOND"

    # 8. Preferreds and ADRs.
    if _PREF_RE.search(haystack):
        return "PREFERRED"
    if _ADR_RE.search(haystack):
        return "ADR"

    # 9. Default.  If we have a position ticker that looks like a normal
    #    stock symbol (1–6 chars, letters/dots), call it common stock.
    if ticker and re.fullmatch(r"[A-Z][A-Z0-9.\-]{0,6}", t_up):
        return "COMMON_STOCK"
    return "OTHER"


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
            pos_t = (str(row.get("Ticker")).strip() or None) if pd.notna(row.get("Ticker")) else None
            sec_n = (str(row.get("Description")).strip() or None) if pd.notna(row.get("Description")) else None
            cusip = (str(row.get("CUSIP")).strip() or None) if pd.notna(row.get("CUSIP")) else None
            raw_t = (str(row.get("Security Type")).strip() or None) if pd.notna(row.get("Security Type")) else None
            out.append(HoldingRow(
                as_of_date=row_date,
                etf_ticker=t,
                position_ticker=pos_t,
                security_name=sec_n,
                cusip=cusip,
                # Normalize the issuer's own label so downstream filters can
                # rely on a stable vocabulary across all five providers.
                security_type=_infer_security_type(
                    position_ticker=pos_t, security_name=sec_n, cusip=cusip, raw_type=raw_t,
                ),
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
                pos_t = (
                    str(row.get("StockTicker")).strip() or None
                ) if pd.notna(row.get("StockTicker")) else None
                sec_n = (
                    str(row.get("SecurityDescription")).strip() or None
                ) if pd.notna(row.get("SecurityDescription")) else None
                cusip = (str(row.get("Cusip")).strip() or None) if pd.notna(row.get("Cusip")) else None
                rows.append(HoldingRow(
                    as_of_date=row_date,
                    etf_ticker=t,
                    position_ticker=pos_t,
                    security_name=sec_n,
                    cusip=cusip,
                    # Direxion's CSV doesn't carry an explicit security_type
                    # column, so infer from the description.  Their 2X/3X funds
                    # hold a mix of common stock and total-return swaps; the
                    # swap lines show up as "BASKET SWAP" / "TRS" / "SWAP"
                    # substrings in SecurityDescription.
                    security_type=_infer_security_type(
                        position_ticker=pos_t, security_name=sec_n, cusip=cusip,
                    ),
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
                sec_n = (
                    str(row.get("SecurityName")).strip() or None
                ) if pd.notna(row.get("SecurityName")) else None
                cusip = (
                    str(row.get("CUSIP")).strip() or None
                ) if pd.notna(row.get("CUSIP")) else None
                rows.append(HoldingRow(
                    as_of_date=row_date,
                    etf_ticker=t,
                    position_ticker=pos_ticker,
                    security_name=sec_n,
                    cusip=cusip,
                    # YieldMax option-income funds are overwhelmingly a mix of
                    # synthetic-long option structures (long calls / short puts)
                    # plus a treasury/money-market collateral bucket.  The CSV
                    # doesn't provide an explicit type, so classify from
                    # SecurityName + StockTicker (OPRA tail).
                    security_type=_infer_security_type(
                        position_ticker=pos_ticker, security_name=sec_n, cusip=cusip,
                    ),
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
                    # REX only shows top-10 -- swaps/options are typically top
                    # lines on their 2X leveraged funds, stocks on their
                    # thematic products.  Infer from symbol + name.
                    security_type=_infer_security_type(
                        position_ticker=sym or None,
                        security_name=name or None,
                        cusip=sec_id or None,
                    ),
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
                name_s = self._strip_tags(m.group("name")) or None
                cusip_s = self._strip_tags(m.group("cusip")) or None
                rows.append(HoldingRow(
                    as_of_date=as_of,
                    etf_ticker=t,
                    position_ticker=ticker_s or None,
                    security_name=name_s,
                    cusip=cusip_s,
                    # Defiance's option-income ETFs (e.g. QQQY, WDTE) publish
                    # option contracts in OPRA format in the Ticker column and
                    # T-bills / money-market in the Name column.  Our inference
                    # picks both up.
                    security_type=_infer_security_type(
                        position_ticker=ticker_s or None,
                        security_name=name_s,
                        cusip=cusip_s,
                    ),
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
    "_infer_security_type",
]

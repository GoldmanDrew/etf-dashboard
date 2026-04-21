"""
ETF metrics providers.

Layered fetch strategy (per ticker, tried in order):
  1) AXS / Tradr bulk CSV (authoritative; covers AXS + Tradr single-stock ETFs)
  2) ProShares bulk historical_nav.csv (authoritative; covers all ProShares funds)
  3) Direxion per-ticker holdings CSV (authoritative; covers Direxion 2x/3x)
  4) Roundhill bulk FilepointRoundhill DailyNAV.csv (authoritative; covers all Roundhill ETFs)
  5) YieldMax per-ticker HTML scrape (authoritative; covers YieldMax option-income ETFs)
  6) REX Shares per-ticker HTML scrape (authoritative; covers T-REX / REX lineup)
  7) GraniteSharesProvider -- JSON from graniteshares.com /product/{id}/ (session + XHR; NAV/AUM;
     shares = AUM/NAV when not in payload)
  8) DefianceProvider -- defianceetfs.com/{ticker}/ HTML (Fund Details: Net Assets, NAV, Shares Outstanding)
  9) YFinanceProvider (broad fallback via Yahoo Finance fast_info / info)
  10) PolygonProvider (last-resort close + meta)

Each provider returns a ProviderResult with:
  nav, aum, shares_outstanding, source_provider, source_url, status, stale, stale_age_bdays

Statuses:
  - 'ok'      : all three of (nav, aum, shares) present and positive
  - 'partial' : at least one of nav/aum/shares present but not all (dashboard still gets NAV history)
  - 'missing' : nothing usable
"""
from __future__ import annotations

import io
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOGGER = logging.getLogger("etf_providers")


# ---------------------------------------------------------------------------
# Core types
# ---------------------------------------------------------------------------

@dataclass
class ProviderResult:
    date: date
    ticker: str
    nav: Optional[float]
    aum: Optional[float]
    shares_outstanding: Optional[float]
    source_provider: str
    source_url: str
    status: str  # 'ok' | 'partial' | 'missing'
    stale: bool = False
    stale_age_bdays: Optional[int] = None


def _classify_status(nav, aum, shares) -> str:
    def _good(x):
        return x is not None and not pd.isna(x) and float(x) > 0
    has_nav = _good(nav)
    has_aum = _good(aum)
    has_shares = _good(shares)
    if has_nav and has_aum and has_shares:
        return "ok"
    if has_nav or has_aum or has_shares:
        return "partial"
    return "missing"


# When multiple providers each return partial rows (common for yfinance + polygon), pick each
# field from the most authoritative source that has it, then derive the third from identity.
PROVIDER_MERGE_PRIORITY: tuple[str, ...] = (
    "tradr_axs",
    "proshares",
    "direxion",
    "roundhill",
    "yieldmax",
    "rex_shares",
    "granite_shares",
    "defiance",
    "yfinance",
    "polygon",
)


def merge_provider_attempts(
    attempts: list[ProviderResult],
    ticker: str,
    end_date: date,
) -> ProviderResult:
    """Combine non-null fields from all partial/ok attempts; prefer issuer feeds over Yahoo/Polygon."""
    if not attempts:
        return ProviderResult(
            end_date, ticker, None, None, None, "none", "none://no-providers", "missing",
        )
    pri = {n: i for i, n in enumerate(PROVIDER_MERGE_PRIORITY)}
    attempts_sorted = sorted(attempts, key=lambda r: pri.get(r.source_provider, 99))

    def _pick_first_positive(attr: str) -> float | None:
        for r in attempts_sorted:
            v = getattr(r, attr)
            if v is None or pd.isna(v):
                continue
            try:
                f = float(v)
            except (TypeError, ValueError):
                continue
            if f > 0:
                return f
        return None

    nav = _pick_first_positive("nav")
    aum = _pick_first_positive("aum")
    shares = _pick_first_positive("shares_outstanding")

    if (aum is None or aum <= 0) and nav and shares:
        aum = float(nav * shares)
    if (shares is None or shares <= 0) and aum and nav and float(nav) > 0:
        shares = float(aum / nav)
    if (nav is None or nav <= 0) and aum and shares and float(shares) > 0:
        nav = float(aum / shares)

    status = _classify_status(nav, aum, shares)

    urls: list[str] = []
    for r in attempts:
        u = (r.source_url or "").strip()
        if u and u not in urls:
            urls.append(u)
    source_url = "|".join(urls) if urls else "merged://"
    if len(source_url) > 4000:
        source_url = source_url[:3997] + "..."

    stale = any(r.stale for r in attempts)
    stale_age: int | None = None
    for r in attempts:
        if r.stale_age_bdays is not None:
            stale_age = max(stale_age or 0, int(r.stale_age_bdays))
    if not stale:
        stale_age = None

    return ProviderResult(
        date=end_date,
        ticker=ticker.upper(),
        nav=nav,
        aum=aum,
        shares_outstanding=shares,
        source_provider="merged",
        source_url=source_url,
        status=status,
        stale=stale,
        stale_age_bdays=stale_age,
    )


def _build_session(timeout_sec: int = 15) -> requests.Session:
    """HTTP session with retries tuned for issuer feeds."""
    timeout_sec = int(os.getenv("ETF_METRICS_HTTP_TIMEOUT_SEC", str(timeout_sec)))
    retry_total = int(os.getenv("ETF_METRICS_HTTP_RETRY_TOTAL", "2"))
    retry_backoff = float(os.getenv("ETF_METRICS_HTTP_RETRY_BACKOFF", "0.35"))
    retry = Retry(
        total=max(0, retry_total),
        backoff_factor=max(0.0, retry_backoff),
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=8, pool_maxsize=8)
    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
        "Accept": "text/csv,application/json,text/html,*/*;q=0.8",
    })
    s.timeout_sec = timeout_sec  # type: ignore[attr-defined]
    return s


def _get(session: requests.Session, url: str, *, extra_headers: dict | None = None):
    h = dict(extra_headers or {})
    return session.get(url, timeout=getattr(session, "timeout_sec", 15), headers=h or None)


def _read_csv_url(session: requests.Session, url: str, **read_kw) -> pd.DataFrame | None:
    try:
        r = _get(session, url)
        if r.status_code != 200 or not r.text:
            return None
        return pd.read_csv(io.StringIO(r.text), **read_kw)
    except Exception as e:
        LOGGER.debug("csv fetch failed %s: %s", url, e)
        return None


# ---------------------------------------------------------------------------
# 1) AXS / Tradr (existing logic, kept intact)
# ---------------------------------------------------------------------------

class TradrAxsProvider:
    name = "tradr_axs"

    def __init__(self, session: requests.Session | None = None):
        self.session = session or _build_session()
        self._nav_cache: dict[date, pd.DataFrame | None] = {}
        self._hold_cache: dict[date, pd.DataFrame | None] = {}
        self._ticker_cache: dict[date, set[str]] = {}

    @staticmethod
    def nav_url(as_of: date) -> str:
        return f"https://axsetf.filepoint.live/assets/data/NSDEAXS2.{as_of.strftime('%m%d%Y')}.csv"

    @staticmethod
    def hold_url(as_of: date) -> str:
        return f"https://axsetf.filepoint.live/assets/data/BBH_AXS_ETF_PVAL_WEB.{as_of.strftime('%Y%m%d')}.csv"

    def _nav_df(self, as_of: date) -> pd.DataFrame | None:
        if as_of not in self._nav_cache:
            self._nav_cache[as_of] = _read_csv_url(self.session, self.nav_url(as_of))
        return self._nav_cache[as_of]

    def _hold_df(self, as_of: date) -> pd.DataFrame | None:
        if as_of not in self._hold_cache:
            self._hold_cache[as_of] = _read_csv_url(self.session, self.hold_url(as_of))
        return self._hold_cache[as_of]

    def supports_ticker(self, ticker: str, as_of: date) -> bool:
        key = ticker.upper()
        if as_of not in self._ticker_cache:
            nav_df = self._nav_df(as_of)
            if nav_df is None or "Ticker Symbol" not in nav_df.columns:
                self._ticker_cache[as_of] = set()
            else:
                self._ticker_cache[as_of] = set(
                    nav_df["Ticker Symbol"].astype(str).str.upper().str.strip().tolist()
                )
        return key in self._ticker_cache.get(as_of, set())

    def fetch_for_date(self, ticker: str, as_of: date) -> ProviderResult:
        nav_url = self.nav_url(as_of)
        nav_df = self._nav_df(as_of)
        if nav_df is None or "Ticker Symbol" not in nav_df.columns:
            return ProviderResult(as_of, ticker, None, None, None, self.name, nav_url, "missing")

        row = nav_df[nav_df["Ticker Symbol"].astype(str).str.upper() == ticker.upper()]
        if row.empty:
            return ProviderResult(as_of, ticker, None, None, None, self.name, nav_url, "missing")

        row0 = row.iloc[0]
        nav = pd.to_numeric(row0.get("NAV"), errors="coerce")
        aum = pd.to_numeric(row0.get("Total Net Assets", row0.get("Base TNA (Fund Level)")), errors="coerce")
        shares = pd.to_numeric(row0.get("Shares Outstanding", row0.get("Shrs Out (Fund Level)")), errors="coerce")
        source_url = nav_url

        if pd.isna(aum) or pd.isna(shares) or float(aum) <= 0 or float(shares) <= 0:
            hold_url = self.hold_url(as_of)
            hold_df = self._hold_df(as_of)
            source_url = f"{nav_url}|{hold_url}"
            if hold_df is not None and "ETF Ticker" in hold_df.columns:
                h = hold_df[hold_df["ETF Ticker"].astype(str).str.upper() == ticker.upper()]
                if not h.empty:
                    h0 = h.iloc[0]
                    h_aum = pd.to_numeric(h0.get("Total Net Assets"), errors="coerce")
                    h_shares = pd.to_numeric(h0.get("Shares Outstanding"), errors="coerce")
                    if (pd.isna(aum) or float(aum) <= 0) and pd.notna(h_aum) and float(h_aum) > 0:
                        aum = h_aum
                    if (pd.isna(shares) or float(shares) <= 0) and pd.notna(h_shares) and float(h_shares) > 0:
                        shares = h_shares

        nav_f = float(nav) if pd.notna(nav) else None
        aum_f = float(aum) if pd.notna(aum) else None
        sh_f = float(shares) if pd.notna(shares) else None
        status = _classify_status(nav_f, aum_f, sh_f)
        return ProviderResult(as_of, ticker, nav_f, aum_f, sh_f, self.name, source_url, status)


# ---------------------------------------------------------------------------
# 2) ProShares bulk historical_nav.csv
# ---------------------------------------------------------------------------

class ProSharesProvider:
    """
    Uses the ProShares master `historical_nav.csv` which contains per-day rows for every
    ProShares fund with columns:  Date, Ticker, NAV, Shares Outstanding (000), Assets Under Management

    The CSV is ~50MB but gives us authoritative NAV + shares + AUM for all ProShares in one request.
    """

    name = "proshares"
    URL = "https://accounts.profunds.com/etfdata/historical_nav.csv"

    def __init__(self, session: requests.Session | None = None):
        self.session = session or _build_session()
        self._df: pd.DataFrame | None = None
        self._tickers: set[str] = set()
        self._by_ticker: dict[str, pd.DataFrame] = {}
        self._loaded = False

    def _load(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        try:
            r = _get(self.session, self.URL)
            if r.status_code != 200 or not r.text:
                LOGGER.warning("ProShares master CSV http=%s len=%s", r.status_code, len(r.text or ""))
                return
            df = pd.read_csv(io.StringIO(r.text))
            df["Ticker"] = df["Ticker"].astype(str).str.strip().str.upper()
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
            df = df.dropna(subset=["Date", "Ticker"])
            self._df = df
            self._tickers = set(df["Ticker"].unique().tolist())
            self._by_ticker = {t: g for t, g in df.groupby("Ticker", sort=False)}
            LOGGER.info("ProShares master loaded: %d rows, %d tickers, latest=%s",
                        len(df), len(self._tickers), df["Date"].max())
        except Exception as e:
            LOGGER.warning("ProShares master load failed: %s", e)

    def supports_ticker(self, ticker: str, as_of: date) -> bool:
        self._load()
        return ticker.upper() in self._tickers

    def fetch_for_date(self, ticker: str, as_of: date) -> ProviderResult:
        self._load()
        t = ticker.upper()
        if t not in self._by_ticker:
            return ProviderResult(as_of, ticker, None, None, None, self.name, self.URL, "missing")
        g = self._by_ticker[t]
        # Prefer exact date; else fall back to most recent on-or-before as_of
        exact = g[g["Date"] == as_of]
        stale = False
        age_b = None
        if not exact.empty:
            row = exact.iloc[0]
        else:
            prior = g[g["Date"] < as_of].sort_values("Date")
            if prior.empty:
                return ProviderResult(as_of, ticker, None, None, None, self.name, self.URL, "missing")
            row = prior.iloc[-1]
            try:
                age_b = int(np.busday_count(str(row["Date"]), str(as_of)))
            except Exception:
                age_b = None
            stale = True

        nav = pd.to_numeric(row.get("NAV"), errors="coerce")
        aum = pd.to_numeric(row.get("Assets Under Management"), errors="coerce")
        shares_k = pd.to_numeric(row.get("Shares Outstanding (000)"), errors="coerce")
        shares = float(shares_k) * 1000.0 if pd.notna(shares_k) else None

        nav_f = float(nav) if pd.notna(nav) else None
        aum_f = float(aum) if pd.notna(aum) else None
        sh_f = float(shares) if shares is not None and shares > 0 else None
        status = _classify_status(nav_f, aum_f, sh_f)
        return ProviderResult(
            date=as_of, ticker=t, nav=nav_f, aum=aum_f, shares_outstanding=sh_f,
            source_provider=self.name, source_url=self.URL + f"#ticker={t}&date={row['Date']}",
            status=status, stale=stale, stale_age_bdays=age_b,
        )


# ---------------------------------------------------------------------------
# 3) Direxion per-ticker holdings CSV
# ---------------------------------------------------------------------------

class DirexionProvider:
    """
    Per-ticker holdings CSV at https://www.direxion.com/holdings/{T}.csv

    Format:
        L0: <Fund name>
        L1: <TICKER>
        L2: Shares Outstanding:<N>
        L3: (blank)
        L4: (blank)
        L5: TradeDate,AccountTicker,StockTicker,SecurityDescription,Shares,Price,MarketValue,Cusip,HoldingsPercent
        ...holdings rows...

    We derive:
        shares_outstanding = header line
        trade_date         = TradeDate of any holdings row (all rows share the same date)
        aum                = sum(MarketValue) across all holdings rows
        nav                = aum / shares_outstanding
    """

    name = "direxion"

    # Tickers confirmed to have a live holdings CSV. This is used for fast routing;
    # fetch_for_date still tries the URL even for unlisted tickers in case Direxion
    # adds a new fund.
    KNOWN_TICKERS = {
        # Index 3X
        "SPXL","SPXS","SPXU","TQQQ","SQQQ","QLD","QID","DDM","DXD","UDOW","SDOW","UMDD","MIDU",
        "URTY","TNA","TZA","UWM","TWM","SOXL","SOXS","LABU","LABD","CURE","DRN","DRV","RETL","RETS","NAIL",
        "FAS","FAZ","TECL","TECS","GUSH","ERX","DRIP","DUST","NUGT","JNUG","CWEB","HIBL","HIBS",
        "WEBL","WEBS","PILL","KORU","EDC","EDZ","YINN","YANG","DZK","DPK","INDL","BRZU","EURL","MEXX",
        # 3X Bonds
        "TMF","TMV","UBT","TYD","TTT","UST",
        # Single-Stock 2X / 1X
        "AAPU","AAPB","AAPD","AAPW",
        "TSLR","TSLL","TSLT","TSLI","TSLS","TSLY","TSLO",
        "MSFL","MSFU","MSFO","MSFX",
        "NVDU","NVDL","NVDQ","NVDD","NVDS","NVDG","NVDY","NVDX","NVDW","NVDO","NVDB",
        "AMZU","AMZD","AMZZ","AMZY","AMZW","AMZO",
        "GOOL","GOU","GOOX","GOOY","GOOW",
        "METU","METD","METW",
        "PLTU","PLTD","PLTY","PLTW","PLTG","PLTA","PLUL","PLYY",
        "BRKU","BRKD","BRKC","BRKW",
        "BABO","BABU","BABW","BABX",
        "CONL","CONX","CONY","COIA","COIG","COIO","COIW",
        "CRCD","CRCG","CRCO","CRCA",
        "BITU","BITX","BTCZ","BTCL","BTFL",
        "ETHT","ETHU","ETHD",
        "CRWG","CRWU","CRWL",
        "BOEG","BOEU","UECG","VALG","UNHG","UNHU","UNHW",
    }

    def __init__(self, session: requests.Session | None = None):
        self.session = session or _build_session()
        self._cache: dict[str, tuple[int, pd.DataFrame | None, float | None, date | None, str]] = {}

    def supports_ticker(self, ticker: str, as_of: date) -> bool:
        return ticker.upper() in self.KNOWN_TICKERS

    def _fetch_holdings(self, ticker: str) -> tuple[pd.DataFrame | None, float | None, date | None, str]:
        """Return (holdings_df, shares_outstanding, trade_date, raw_url_or_err)."""
        t = ticker.upper()
        if t in self._cache:
            _, df, sh, dt, src = self._cache[t]
            return df, sh, dt, src
        url = f"https://www.direxion.com/holdings/{t}.csv"
        try:
            r = _get(self.session, url, extra_headers={"Referer": "https://www.direxion.com/"})
            status = r.status_code
            if status != 200 or not r.text or not r.text.startswith(("Direxion", '"Direxion')):
                self._cache[t] = (status, None, None, None, url + f"?status={status}")
                return None, None, None, url + f"?status={status}"
            lines = r.text.splitlines()
            shares_line = next((ln for ln in lines[:6] if ln.lower().startswith("shares outstanding:")), "")
            shares = None
            if shares_line:
                m = re.search(r"[\d,]+", shares_line)
                if m:
                    shares = float(m.group(0).replace(",", ""))
            # Find header row with TradeDate column
            header_idx = None
            for i, ln in enumerate(lines):
                if "TradeDate" in ln and "MarketValue" in ln:
                    header_idx = i
                    break
            if header_idx is None:
                self._cache[t] = (status, None, shares, None, url + "?err=no-header")
                return None, shares, None, url + "?err=no-header"
            csv_body = "\n".join(lines[header_idx:])
            df = pd.read_csv(io.StringIO(csv_body))
            df["MarketValue"] = pd.to_numeric(df["MarketValue"], errors="coerce")
            df["TradeDate"] = pd.to_datetime(df["TradeDate"], errors="coerce", format="mixed").dt.date
            td = df["TradeDate"].dropna().iloc[0] if df["TradeDate"].notna().any() else None
            self._cache[t] = (status, df, shares, td, url)
            return df, shares, td, url
        except Exception as e:
            self._cache[t] = (0, None, None, None, url + f"?exc={type(e).__name__}")
            return None, None, None, url + f"?exc={type(e).__name__}"

    def fetch_for_date(self, ticker: str, as_of: date) -> ProviderResult:
        df, shares, trade_date, src = self._fetch_holdings(ticker)
        if df is None or shares is None or shares <= 0:
            return ProviderResult(as_of, ticker, None, None, None, self.name, src, "missing")
        # AUM is derived via the identity MV_i = (HP_i / 100) * AUM, so AUM = MV_i * 100 / HP_i.
        # Using the median across positions is robust to rounding and to individual zero-percent rows.
        df["HoldingsPercent"] = pd.to_numeric(df.get("HoldingsPercent"), errors="coerce")
        implied = (df["MarketValue"] * 100.0 / df["HoldingsPercent"].replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)
        aum_val = float(implied.median(skipna=True)) if implied.notna().any() else None
        # Bear-fund swaps have negative MV paired with negative HP, so impliedAUM stays positive.
        aum = float(abs(aum_val)) if aum_val is not None and not pd.isna(aum_val) else None
        nav = float(aum / shares) if aum and shares > 0 else None
        stale = False
        age_b = None
        if trade_date and trade_date != as_of:
            stale = True
            try:
                age_b = int(np.busday_count(str(trade_date), str(as_of)))
            except Exception:
                age_b = None
        status = _classify_status(nav, aum, shares)
        return ProviderResult(
            date=as_of, ticker=ticker.upper(),
            nav=nav, aum=aum, shares_outstanding=shares,
            source_provider=self.name, source_url=src,
            status=status, stale=stale, stale_age_bdays=age_b,
        )


# ---------------------------------------------------------------------------
# 4) Roundhill bulk DailyNAV CSV
# ---------------------------------------------------------------------------

class RoundhillProvider:
    """
    Single authoritative bulk CSV at
        https://www.roundhillinvestments.com/assets/data/FilepointRoundhill.40RU.RU_DailyNAV.csv

    Columns: Fund Name, Fund Ticker, CUSIP, Net Assets, Shares Outstanding, NAV,
             NAV Change Dollars, NAV Change Percentage, Market Price, ..., Rate Date

    Covers all Roundhill ETFs (weekly-pay single-stock ETFs ending in 'W', XDTE, YBTC, YETH,
    DEEP, MAGS, etc.). One HTTP request loads them all.
    """

    name = "roundhill"
    URL = "https://www.roundhillinvestments.com/assets/data/FilepointRoundhill.40RU.RU_DailyNAV.csv"

    def __init__(self, session: requests.Session | None = None):
        self.session = session or _build_session()
        self._df: pd.DataFrame | None = None
        self._tickers: set[str] = set()
        self._by_ticker: dict[str, pd.Series] = {}
        self._latest_date: date | None = None
        self._loaded = False

    def _load(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        try:
            r = _get(self.session, self.URL)
            if r.status_code != 200 or not r.text or "Fund Ticker" not in r.text[:200]:
                LOGGER.warning("Roundhill DailyNAV http=%s len=%s", r.status_code, len(r.text or ""))
                return
            df = pd.read_csv(io.StringIO(r.text))
            df.columns = [c.strip() for c in df.columns]
            if "Fund Ticker" not in df.columns:
                LOGGER.warning("Roundhill DailyNAV missing expected columns: %s", df.columns.tolist())
                return
            df["Fund Ticker"] = df["Fund Ticker"].astype(str).str.strip().str.upper()
            df["Rate Date"] = pd.to_datetime(df["Rate Date"], errors="coerce").dt.date
            df = df.dropna(subset=["Fund Ticker"])
            self._df = df
            self._tickers = set(df["Fund Ticker"].tolist())
            # Keep one row per ticker (latest if dupes)
            self._by_ticker = {str(r["Fund Ticker"]).upper(): r for _, r in df.iterrows()}
            self._latest_date = df["Rate Date"].dropna().max() if df["Rate Date"].notna().any() else None
            LOGGER.info(
                "Roundhill DailyNAV loaded: %d funds, latest_rate_date=%s",
                len(self._tickers), self._latest_date,
            )
        except Exception as e:
            LOGGER.warning("Roundhill DailyNAV load failed: %s", e)

    def supports_ticker(self, ticker: str, as_of: date) -> bool:
        self._load()
        return ticker.upper() in self._tickers

    def fetch_for_date(self, ticker: str, as_of: date) -> ProviderResult:
        self._load()
        t = ticker.upper()
        if t not in self._by_ticker:
            return ProviderResult(as_of, ticker, None, None, None, self.name, self.URL, "missing")
        row = self._by_ticker[t]
        nav = pd.to_numeric(row.get("NAV"), errors="coerce")
        aum = pd.to_numeric(row.get("Net Assets"), errors="coerce")
        shares = pd.to_numeric(row.get("Shares Outstanding"), errors="coerce")
        row_date = row.get("Rate Date")

        nav_f = float(nav) if pd.notna(nav) and nav > 0 else None
        aum_f = float(aum) if pd.notna(aum) and aum > 0 else None
        sh_f = float(shares) if pd.notna(shares) and shares > 0 else None

        stale = False
        age_b = None
        if isinstance(row_date, date) and row_date != as_of:
            stale = True
            try:
                age_b = int(np.busday_count(str(row_date), str(as_of)))
            except Exception:
                age_b = None

        status = _classify_status(nav_f, aum_f, sh_f)
        return ProviderResult(
            date=as_of, ticker=t, nav=nav_f, aum=aum_f, shares_outstanding=sh_f,
            source_provider=self.name,
            source_url=self.URL + f"#ticker={t}&date={row_date}",
            status=status, stale=stale, stale_age_bdays=age_b,
        )


# ---------------------------------------------------------------------------
# 5) YieldMax per-ticker HTML scrape
# ---------------------------------------------------------------------------

class YieldMaxProvider:
    """
    YieldMax publishes live fund stats in the SSR HTML of each fund page. Target:
        https://yieldmaxetfs.com/our-etfs/{ticker_lower}/

    The info table embeds four labelled rows we consume directly:
        As of:             04/16/2026
        Net Assets:        $1.15B           (or full $ amount, B/M suffix handled)
        NAV:               $23.41
        Shares Outstanding:49,294,959

    We also keep a KNOWN_TICKERS set so the stack skips URL probes for tickers that
    aren't YieldMax-listed (avoids 404 churn on look-alike symbols like *YY).
    """

    name = "yieldmax"
    URL = "https://yieldmaxetfs.com/our-etfs/{t}/"

    # Confirmed via _probe_coverage: every one of these currently renders a real
    # YieldMax fund page with an 'As of / Net Assets / NAV / Shares' table.
    KNOWN_TICKERS: set[str] = {
        "ABNY", "AIYY", "AMDY", "AMZY", "APLY", "BABO", "CHPY", "CONY", "CVNY",
        "DIPS", "DISO", "DRAY", "FBY", "FBTC", "FIAT", "FIVY", "GDXY", "GLDY",
        "GMEY", "GOOX", "GOOY", "HIYY", "HOOY", "LFGY", "MARO", "MRNY", "MSTY",
        "NFLY", "NVDY", "OARK", "PLTY", "PYPY", "QDTE", "RBLY", "RDTE", "RDTY",
        "RDYY", "SMCY", "SNOY", "TSLY", "TSMY", "ULTY", "XDTE", "XOMO", "XYZY",
        "YMAG", "YMAX", "YBIT",
    }

    def __init__(self, session: requests.Session | None = None):
        self.session = session or _build_session()
        self._cache: dict[str, ProviderResult] = {}

    def supports_ticker(self, ticker: str, as_of: date) -> bool:
        return ticker.upper() in self.KNOWN_TICKERS

    @staticmethod
    def _parse_currency(s: str | None) -> float | None:
        if not s:
            return None
        s = str(s).strip().upper().replace("$", "").replace(",", "").replace(" ", "")
        m = re.match(r"^([\d.]+)([BMK]?)$", s)
        if not m:
            return None
        try:
            val = float(m.group(1))
        except Exception:
            return None
        mult = {"B": 1e9, "M": 1e6, "K": 1e3, "": 1.0}[m.group(2)]
        return val * mult

    @staticmethod
    def _parse_int(s: str | None) -> float | None:
        if not s:
            return None
        try:
            return float(str(s).replace(",", "").strip())
        except Exception:
            return None

    @staticmethod
    def _parse_date(s: str | None) -> date | None:
        if not s:
            return None
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
            try:
                return datetime.strptime(str(s).strip(), fmt).date()
            except Exception:
                continue
        return None

    def fetch_for_date(self, ticker: str, as_of: date) -> ProviderResult:
        t = ticker.upper()
        url = self.URL.format(t=t.lower())
        if t in self._cache:
            cached = self._cache[t]
            return ProviderResult(
                date=as_of, ticker=t, nav=cached.nav, aum=cached.aum,
                shares_outstanding=cached.shares_outstanding,
                source_provider=self.name, source_url=cached.source_url,
                status=cached.status, stale=cached.stale,
                stale_age_bdays=cached.stale_age_bdays,
            )
        try:
            r = _get(self.session, url)
            if r.status_code != 200 or not r.text or "fund-label" not in r.text:
                res = ProviderResult(as_of, ticker, None, None, None, self.name,
                                     url + f"?status={r.status_code}", "missing")
                self._cache[t] = res
                return res
            html = r.text
            row_re = lambda label: re.search(
                rf'<div class="fund-label">\s*{re.escape(label)}[:\s]*</div>\s*'
                rf'<div class="fund-value">\s*([^<]+?)\s*</div>',
                html, re.IGNORECASE,
            )
            as_of_str = (m.group(1) if (m := row_re("As of")) else None)
            aum_str   = (m.group(1) if (m := row_re("Net Assets")) else None)
            nav_str   = (m.group(1) if (m := row_re("NAV")) else None)
            shares_str = (m.group(1) if (m := row_re("Shares Outstanding")) else None)

            nav = self._parse_currency(nav_str)
            aum = self._parse_currency(aum_str)
            shares = self._parse_int(shares_str)
            row_date = self._parse_date(as_of_str)

            stale = False
            age_b = None
            if row_date and row_date != as_of:
                stale = True
                try:
                    age_b = int(np.busday_count(str(row_date), str(as_of)))
                except Exception:
                    age_b = None
            status = _classify_status(nav, aum, shares)
            res = ProviderResult(
                date=as_of, ticker=t, nav=nav, aum=aum, shares_outstanding=shares,
                source_provider=self.name, source_url=url + f"#as_of={row_date}",
                status=status, stale=stale, stale_age_bdays=age_b,
            )
            self._cache[t] = res
            return res
        except Exception as e:
            res = ProviderResult(as_of, ticker, None, None, None, self.name,
                                 url + f"?exc={type(e).__name__}", "missing")
            self._cache[t] = res
            return res


# ---------------------------------------------------------------------------
# 6) REX Shares / T-REX per-ticker HTML scrape
# ---------------------------------------------------------------------------

class REXSharesProvider:
    """
    REX Shares (incl. T-REX) publishes per-fund stats in SSR HTML rows:
        https://www.rexshares.com/{TICKER}/

    Structure (all rows live inside a `fund-details` section):
        <div class="t-col t-label">Fund Assets</div>
        <div class="t-col t-data">$92,510,000.00</div>
        <div class="t-col t-label">Shares Outstanding</div>
        <div class="t-col t-data">11,000,000</div>
        <div class="t-col t-label">Closing Price</div>
        <div class="t-col t-data">$8.41</div>

    NAV is not a distinct field on most REX pages; we compute nav = aum / shares.
    """

    name = "rex_shares"
    URL = "https://www.rexshares.com/{t}/"

    # Confirmed via probe + REX public lineup (T-REX 2x leveraged + income ETFs).
    # NOTE: QDTE / XDTE / RDTE are Roundhill funds, not REX -- they intentionally sit
    # in RoundhillProvider.KNOWN_TICKERS (via DailyNAV.csv).
    KNOWN_TICKERS: set[str] = {
        # T-REX 2x long/inverse single-stock
        "MSTU", "MSTZ", "NVDX", "NVDQ", "AAPX", "TSLT", "TSLZ", "AMZX", "AMZZ",
        "BITX", "BITZ", "BRKX", "BRKZ", "ETH2", "ETHZ", "CONG",
        # Income / thematic
        "FEPI", "AIPI", "BMAX", "SIXJ", "BKCH", "BITC", "BTCL", "BTCZ",
    }

    def __init__(self, session: requests.Session | None = None):
        self.session = session or _build_session()
        self._cache: dict[str, ProviderResult] = {}

    def supports_ticker(self, ticker: str, as_of: date) -> bool:
        return ticker.upper() in self.KNOWN_TICKERS

    @staticmethod
    def _row_match(html: str, label: str) -> str | None:
        m = re.search(
            rf'>{re.escape(label)}</div>\s*<div[^>]*class="[^"]*t-data[^"]*"[^>]*>\s*([^<]+?)\s*</div>',
            html, re.IGNORECASE | re.DOTALL,
        )
        return m.group(1) if m else None

    @staticmethod
    def _to_float(s: str | None) -> float | None:
        if not s:
            return None
        try:
            return float(str(s).replace("$", "").replace(",", "").strip())
        except Exception:
            return None

    def fetch_for_date(self, ticker: str, as_of: date) -> ProviderResult:
        t = ticker.upper()
        url = self.URL.format(t=t)
        if t in self._cache:
            cached = self._cache[t]
            return ProviderResult(
                date=as_of, ticker=t, nav=cached.nav, aum=cached.aum,
                shares_outstanding=cached.shares_outstanding,
                source_provider=self.name, source_url=cached.source_url,
                status=cached.status, stale=cached.stale,
                stale_age_bdays=cached.stale_age_bdays,
            )
        try:
            r = _get(self.session, url)
            if r.status_code != 200 or not r.text or "Fund Assets" not in r.text:
                res = ProviderResult(as_of, ticker, None, None, None, self.name,
                                     url + f"?status={r.status_code}", "missing")
                self._cache[t] = res
                return res
            html = r.text
            aum = self._to_float(self._row_match(html, "Fund Assets"))
            shares_str = self._row_match(html, "Shares Outstanding")
            shares = self._to_float(shares_str)
            close = self._to_float(self._row_match(html, "Closing Price"))
            as_of_str = self._row_match(html, "As of")

            nav = None
            if aum and shares and shares > 0:
                nav = aum / shares
            elif close:
                nav = close

            row_date = None
            if as_of_str:
                for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
                    try:
                        row_date = datetime.strptime(as_of_str.strip(), fmt).date()
                        break
                    except Exception:
                        continue

            stale = False
            age_b = None
            if row_date and row_date != as_of:
                stale = True
                try:
                    age_b = int(np.busday_count(str(row_date), str(as_of)))
                except Exception:
                    age_b = None

            status = _classify_status(nav, aum, shares)
            res = ProviderResult(
                date=as_of, ticker=t, nav=nav, aum=aum, shares_outstanding=shares,
                source_provider=self.name, source_url=url + f"#as_of={row_date}",
                status=status, stale=stale, stale_age_bdays=age_b,
            )
            self._cache[t] = res
            return res
        except Exception as e:
            res = ProviderResult(as_of, ticker, None, None, None, self.name,
                                 url + f"?exc={type(e).__name__}", "missing")
            self._cache[t] = res
            return res


# ---------------------------------------------------------------------------
# 7) GraniteShares (issuer JSON API behind session cookie)
# ---------------------------------------------------------------------------

class GraniteSharesProvider:
    """
    GraniteShares publishes NAV / AUM via JSON:
        GET https://www.graniteshares.com/product/{productId}/en-us/

    The endpoint returns ``application/json`` only after the same session has loaded any
    page on graniteshares.com (sets the cookie the CDN expects). The public site embeds
    product ids in ETF listing HTML; we resolve ``productId`` from ``/etfs/{slug}/``.

    Shares outstanding are not always in the payload; we use ``shares = AUM / NAV`` when
    both are present (same identity as other issuer-derived rows).
    """

    name = "granite_shares"
    BASE = "https://www.graniteshares.com"

    def __init__(self, session: requests.Session | None = None):
        # ``session`` is ignored: GraniteShares' CDN serves full ETF markup only on the
        # first HTTP request in a session to graniteshares.com. The shared ingest
        # session may already have loaded another Granite ticker (slim shell), so each
        # fetch uses a fresh session for the detail page + product JSON pair.
        _ = session
        self._catalog: set[str] | None = None
        self._cache: dict[str, ProviderResult] = {}

    def _load_catalog(self) -> None:
        if self._catalog is not None:
            return
        self._catalog = set()
        # Use a one-off request, not ``self.session``. The GraniteShares CDN returns a
        # full HTML document on the first request in a session, but a slim shell (~360KB)
        # on subsequent navigations—so loading /etfs/ on the same session as /etfs/{slug}/
        # would strip the ETF table and break product-id extraction.
        try:
            r = _get(_build_session(), f"{self.BASE}/etfs/")
            if r.status_code != 200 or not r.text:
                LOGGER.warning("GraniteShares catalog fetch http=%s", r.status_code)
                return
            for m in re.finditer(r'href="/etfs/([a-z0-9.-]+)/"', r.text, re.I):
                self._catalog.add(m.group(1).upper())
            LOGGER.info("GraniteShares ETF catalog: %d symbols", len(self._catalog))
        except Exception as e:
            LOGGER.warning("GraniteShares catalog load failed: %s", e)

    def supports_ticker(self, ticker: str, as_of: date) -> bool:
        self._load_catalog()
        return ticker.upper() in (self._catalog or set())

    @staticmethod
    def _slug_for_ticker(ticker: str) -> str:
        return ticker.strip().lower()

    @staticmethod
    def _product_id_from_page(html: str, slug: str, ticker_upper: str) -> str | None:
        m = re.search(
            rf'data-id="(\d+)"[^>]*>\s*<a[^>]*href="/etfs/{re.escape(slug)}/"',
            html,
            re.I | re.DOTALL,
        )
        if m:
            return m.group(1)
        m2 = re.search(
            rf'class="stocks-scrolling-ticker pId"\s+data-id="(\d+)"[\s\S]{{0,520}}?'
            rf'{re.escape(ticker_upper)}</span>',
            html,
            re.I,
        )
        if m2:
            return m2.group(1)
        return None

    @staticmethod
    def _parse_nav_date(raw: object) -> date | None:
        if raw is None:
            return None
        s = str(raw).strip()
        if len(s) >= 10 and s[4] == "-" and s[7] == "-":
            try:
                return datetime.strptime(s[:10], "%Y-%m-%d").date()
            except Exception:
                return None
        return None

    def fetch_for_date(self, ticker: str, as_of: date) -> ProviderResult:
        t = ticker.upper()
        slug = self._slug_for_ticker(ticker)
        page_url = f"{self.BASE}/etfs/{slug}/"
        api_url = None

        if t in self._cache:
            cached = self._cache[t]
            return ProviderResult(
                date=as_of, ticker=t, nav=cached.nav, aum=cached.aum,
                shares_outstanding=cached.shares_outstanding,
                source_provider=self.name, source_url=cached.source_url,
                status=cached.status, stale=cached.stale,
                stale_age_bdays=cached.stale_age_bdays,
            )

        try:
            sess = _build_session()
            # First request on ``sess`` is always the ETF page, then the product JSON on
            # the same session (cookie gate for application/json on /product/...).
            pr = _get(sess, page_url, extra_headers={"Referer": f"{self.BASE}/etfs/"})
            if pr.status_code != 200 or not pr.text:
                res = ProviderResult(
                    as_of, ticker, None, None, None, self.name,
                    page_url + f"?status={pr.status_code}", "missing",
                )
                self._cache[t] = res
                return res

            product_id = self._product_id_from_page(pr.text, slug, t)
            if not product_id:
                res = ProviderResult(as_of, ticker, None, None, None, self.name, page_url + "?err=no-product-id", "missing")
                self._cache[t] = res
                return res

            api_url = f"{self.BASE}/product/{product_id}/en-us/"
            jr = sess.get(
                api_url,
                timeout=getattr(sess, "timeout_sec", 15),
                headers={
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": page_url,
                },
            )
            if jr.status_code != 200 or not jr.text:
                res = ProviderResult(
                    as_of, ticker, None, None, None, self.name,
                    api_url + f"?status={jr.status_code}", "missing",
                )
                self._cache[t] = res
                return res
            ctype = jr.headers.get("content-type", "")
            if "application/json" not in ctype:
                res = ProviderResult(as_of, ticker, None, None, None, self.name, api_url + "?err=not-json", "missing")
                self._cache[t] = res
                return res

            try:
                payload = jr.json()
            except ValueError:
                res = ProviderResult(as_of, ticker, None, None, None, self.name, api_url + "?err=json", "missing")
                self._cache[t] = res
                return res

            nav = pd.to_numeric(payload.get("Nav"), errors="coerce")
            close_px = pd.to_numeric(payload.get("ClosingPrice"), errors="coerce")
            if pd.isna(nav) or float(nav) <= 0:
                nav = close_px
            aum = pd.to_numeric(payload.get("AUM"), errors="coerce")

            nav_f = float(nav) if pd.notna(nav) and float(nav) > 0 else None
            aum_f = float(aum) if pd.notna(aum) and float(aum) > 0 else None

            shares_f: float | None = None
            if nav_f and aum_f and nav_f > 0:
                shares_f = float(aum_f / nav_f)

            nav_date = self._parse_nav_date(payload.get("NavDate"))
            stale = False
            age_b = None
            if nav_date and nav_date != as_of:
                stale = True
                try:
                    age_b = int(np.busday_count(str(nav_date), str(as_of)))
                except Exception:
                    age_b = None

            status = _classify_status(nav_f, aum_f, shares_f)
            res = ProviderResult(
                date=as_of, ticker=t,
                nav=nav_f, aum=aum_f, shares_outstanding=shares_f,
                source_provider=self.name,
                source_url=api_url + f"#nav_date={nav_date}",
                status=status, stale=stale, stale_age_bdays=age_b,
            )
            self._cache[t] = res
            return res
        except Exception as e:
            res = ProviderResult(
                as_of, ticker, None, None, None, self.name,
                (api_url or page_url) + f"?exc={type(e).__name__}", "missing",
            )
            self._cache[t] = res
            return res


# ---------------------------------------------------------------------------
# 8) Defiance ETFs (issuer HTML — Fund Details block)
# ---------------------------------------------------------------------------

class DefianceProvider:
    """
    Defiance ETFs publish NAV / Net Assets / Shares Outstanding in a **Fund Details** section
    on each product page, e.g. https://defianceetfs.com/qbtz/

    We strip HTML to text and parse the block between ``Fund Details`` and ``Top Holdings``.
    """

    name = "defiance"
    BASE_HOST = "defianceetfs.com"

    # Slugs on listing/marketing pages that are not single-ETF tickers.
    _SLUG_BLOCKLIST = frozenset({
        "etfs", "insights", "in-the-news", "privacy-policy", "prospectuses", "who-we-are",
        "wp-content", "etf-insights", "author", "category", "tag", "page", "feed", "bu", "mpl",
        "mst", "qsu",
    })

    def __init__(self, session: requests.Session | None = None):
        self.session = session or _build_session()
        self._catalog: set[str] | None = None
        self._cache: dict[str, ProviderResult] = {}

    def _load_catalog(self) -> None:
        if self._catalog is not None:
            return
        self._catalog = set()
        try:
            r = _get(self.session, f"https://{self.BASE_HOST}/etfs/")
            if r.status_code != 200 or not r.text:
                LOGGER.warning("Defiance catalog fetch http=%s", r.status_code)
                return
            found = set(
                re.findall(
                    r"https://(?:www\.)?defianceetfs\.com/([a-z0-9]{2,6})/",
                    r.text,
                    re.I,
                )
            )
            found |= set(re.findall(r'href="/([a-z0-9]{2,6})/"', r.text, re.I))
            for s in found:
                sl = s.lower()
                if sl in self._SLUG_BLOCKLIST or "full-holdings" in sl:
                    continue
                if 2 <= len(sl) <= 6:
                    self._catalog.add(sl.upper())
            LOGGER.info("Defiance ETF catalog: %d symbols", len(self._catalog))
        except Exception as e:
            LOGGER.warning("Defiance catalog load failed: %s", e)

    def supports_ticker(self, ticker: str, as_of: date) -> bool:
        self._load_catalog()
        return ticker.upper() in (self._catalog or set())

    @staticmethod
    def _html_to_text(html: str) -> str:
        t = re.sub(r"<script[^>]*>[\s\S]*?</script>", " ", html, flags=re.I)
        t = re.sub(r"<style[^>]*>[\s\S]*?</style>", " ", t, flags=re.I)
        t = re.sub(r"<[^>]+>", " ", t)
        return re.sub(r"\s+", " ", t).strip()

    @staticmethod
    def _parse_aum(num_s: str, suffix: str) -> float | None:
        try:
            val = float(str(num_s).replace(",", "").strip())
        except (TypeError, ValueError):
            return None
        suf = (suffix or "").strip().upper()
        mult = {"K": 1e3, "M": 1e6, "B": 1e9, "": 1.0}.get(suf, 1.0)
        return float(val * mult)

    @classmethod
    def _parse_fund_details(cls, text: str) -> tuple[float | None, float | None, float | None]:
        fd = re.search(r"Fund Details\s*(.*?)(?:Top Holdings|Distributions)", text, re.I | re.DOTALL)
        block = fd.group(1) if fd else text
        m = re.search(
            r"Net Assets\s*\$?\s*([\d,.]+)\s*([KMB]?)\s*.*?NAV\s*\$?\s*([\d,.]+)\s*.*?Shares Outstanding\s*([\d,]+)",
            block,
            re.I | re.DOTALL,
        )
        if not m:
            aum = nav = sh = None
            m1 = re.search(r"Net Assets\s*\$?\s*([\d,.]+)\s*([KMB]?)", block, re.I)
            m2 = re.search(r"NAV\s*\$?\s*([\d,.]+)", block, re.I)
            m3 = re.search(r"Shares Outstanding\s*([\d,]+)", block, re.I)
            if m1:
                aum = cls._parse_aum(m1.group(1), m1.group(2) or "")
            if m2:
                try:
                    nav = float(m2.group(1).replace(",", ""))
                except (TypeError, ValueError):
                    nav = None
            if m3:
                try:
                    sh = float(m3.group(1).replace(",", ""))
                except (TypeError, ValueError):
                    sh = None
            return nav, aum, sh
        aum = cls._parse_aum(m.group(1), m.group(2) or "")
        try:
            nav = float(m.group(3).replace(",", ""))
        except (TypeError, ValueError):
            nav = None
        try:
            sh = float(m.group(4).replace(",", ""))
        except (TypeError, ValueError):
            sh = None
        return nav, aum, sh

    def fetch_for_date(self, ticker: str, as_of: date) -> ProviderResult:
        t = ticker.upper()
        slug = t.lower()
        url = f"https://{self.BASE_HOST}/{slug}/"

        if t in self._cache:
            c = self._cache[t]
            return ProviderResult(
                date=as_of, ticker=t, nav=c.nav, aum=c.aum,
                shares_outstanding=c.shares_outstanding,
                source_provider=self.name, source_url=c.source_url,
                status=c.status, stale=c.stale, stale_age_bdays=c.stale_age_bdays,
            )

        try:
            r = _get(self.session, url, extra_headers={"Referer": f"https://{self.BASE_HOST}/etfs/"})
            if r.status_code != 200 or not r.text:
                res = ProviderResult(
                    as_of, ticker, None, None, None, self.name,
                    url + f"?status={r.status_code}", "missing",
                )
                self._cache[t] = res
                return res
            if "Fund Details" not in r.text and "Net Assets" not in r.text:
                res = ProviderResult(as_of, ticker, None, None, None, self.name, url + "?err=no-fund-details", "missing")
                self._cache[t] = res
                return res

            text = self._html_to_text(r.text)
            nav_f, aum_f, sh_f = self._parse_fund_details(text)
            if (aum_f is None or aum_f <= 0) and nav_f and sh_f and nav_f > 0:
                aum_f = float(nav_f * sh_f)
            if (sh_f is None or sh_f <= 0) and aum_f and nav_f and nav_f > 0:
                sh_f = float(aum_f / nav_f)

            status = _classify_status(nav_f, aum_f, sh_f)
            res = ProviderResult(
                date=as_of, ticker=t,
                nav=nav_f, aum=aum_f, shares_outstanding=sh_f,
                source_provider=self.name, source_url=url,
                status=status, stale=False, stale_age_bdays=None,
            )
            self._cache[t] = res
            return res
        except Exception as e:
            res = ProviderResult(
                as_of, ticker, None, None, None, self.name,
                url + f"?exc={type(e).__name__}", "missing",
            )
            self._cache[t] = res
            return res


# ---------------------------------------------------------------------------
# 9) YFinance broad fallback
# ---------------------------------------------------------------------------

class YFinanceProvider:
    """
    Broad fallback using Yahoo Finance via yfinance.

    Works best on clean IPs (GitHub Actions, cloud). Rate-limited locally.
    Per-call: ~1-3 requests. We keep a tiny cooldown and swallow auth errors quietly.
    """

    name = "yfinance"

    def __init__(self, min_call_interval_ms: int = 50, enable: bool | None = None):
        self._last_call = 0.0
        self._min_int = float(min_call_interval_ms) / 1000.0
        if enable is None:
            enable = os.getenv("ETF_METRICS_DISABLE_YFINANCE", "").lower() not in ("1", "true", "yes")
        self._enabled = enable
        self._yf = None
        if self._enabled:
            try:
                import yfinance as yf  # type: ignore
                self._yf = yf
            except Exception as e:
                LOGGER.warning("yfinance import failed: %s", e)
                self._enabled = False

    def supports_ticker(self, ticker: str, as_of: date) -> bool:
        return self._enabled

    def _throttle(self):
        now = time.monotonic()
        dt_wait = self._last_call + self._min_int - now
        if dt_wait > 0:
            time.sleep(dt_wait)
        self._last_call = time.monotonic()

    def _fetch_close_on(self, ticker, as_of: date) -> float | None:
        try:
            self._throttle()
            start = (as_of - timedelta(days=7)).isoformat()
            end = (as_of + timedelta(days=1)).isoformat()
            hist = self._yf.Ticker(ticker).history(start=start, end=end, auto_adjust=False, raise_errors=False)
            if hist is None or hist.empty:
                return None
            hist.index = pd.to_datetime(hist.index).date
            # Most recent close on-or-before as_of
            elig = [d for d in hist.index if d <= as_of]
            if not elig:
                return None
            return float(hist.loc[max(elig), "Close"])
        except Exception:
            return None

    def fetch_for_date(self, ticker: str, as_of: date) -> ProviderResult:
        src = f"yfinance://{ticker}"
        if not self._enabled:
            return ProviderResult(as_of, ticker, None, None, None, self.name, src + "?disabled=1", "missing")
        nav = aum = shares = None
        try:
            self._throttle()
            tk = self._yf.Ticker(ticker)
            try:
                fi = tk.fast_info
                shares = getattr(fi, "shares", None)
                last = getattr(fi, "last_price", None)
                nav = float(last) if last and last > 0 else None
                shares = float(shares) if shares and shares > 0 else None
            except Exception:
                pass
            if nav is None:
                nav = self._fetch_close_on(ticker, as_of)
            try:
                self._throttle()
                info = tk.info or {}
                aum = info.get("totalAssets")
                if aum is not None:
                    aum = float(aum)
                if shares is None:
                    so = info.get("sharesOutstanding") or info.get("impliedSharesOutstanding")
                    if so and so > 0:
                        shares = float(so)
                if nav is None:
                    p = info.get("navPrice") or info.get("regularMarketPrice") or info.get("previousClose")
                    if p and p > 0:
                        nav = float(p)
            except Exception:
                pass
            # Derive AUM if possible
            if (aum is None or aum <= 0) and nav and shares:
                aum = float(nav * shares)
            status = _classify_status(nav, aum, shares)
            return ProviderResult(as_of, ticker.upper(), nav, aum, shares, self.name, src, status)
        except Exception as e:
            return ProviderResult(as_of, ticker, None, None, None, self.name, src + f"?exc={type(e).__name__}", "missing")


# ---------------------------------------------------------------------------
# 5) Polygon last-resort (price-oriented)
# ---------------------------------------------------------------------------

class PolygonProvider:
    name = "polygon"

    def __init__(self, session: requests.Session | None = None):
        self.session = session or _build_session()
        self.api_key = os.getenv("POLYGON_API_KEY") or os.getenv("POLYGON_IO_API_KEY")
        self._meta_cache: dict[str, tuple[float | None, float | None]] = {}
        self._price_cache: dict[tuple[str, str], float | None] = {}

    def supports_ticker(self, ticker: str, as_of: date) -> bool:
        return bool(self.api_key)

    def _meta(self, ticker: str) -> tuple[float | None, float | None]:
        key = ticker.upper()
        if key in self._meta_cache:
            return self._meta_cache[key]
        if not self.api_key:
            self._meta_cache[key] = (None, None)
            return self._meta_cache[key]
        url = f"https://api.polygon.io/v3/reference/tickers/{key}?apiKey={self.api_key}"
        try:
            r = self.session.get(url, timeout=getattr(self.session, "timeout_sec", 15))
            if r.status_code != 200:
                self._meta_cache[key] = (None, None)
                return self._meta_cache[key]
            data = r.json() if r.text else {}
            res = data.get("results") or {}
            wo = res.get("weighted_shares_outstanding")
            sc = res.get("share_class_shares_outstanding")
            shares_raw = wo if wo not in (None, "") else sc
            shares = pd.to_numeric(shares_raw, errors="coerce")
            mcap = pd.to_numeric(res.get("market_cap"), errors="coerce")
            s = float(shares) if pd.notna(shares) and shares > 0 else None
            m = float(mcap) if pd.notna(mcap) and mcap > 0 else None
            self._meta_cache[key] = (s, m)
            return self._meta_cache[key]
        except Exception:
            self._meta_cache[key] = (None, None)
            return self._meta_cache[key]

    def _close(self, ticker: str, as_of: date) -> float | None:
        key = ticker.upper()
        dkey = (key, as_of.isoformat())
        if dkey in self._price_cache:
            return self._price_cache[dkey]
        if not self.api_key:
            self._price_cache[dkey] = None
            return None
        ds = as_of.isoformat()
        url = (
            f"https://api.polygon.io/v2/aggs/ticker/{key}/range/1/day/{ds}/{ds}"
            f"?adjusted=true&sort=desc&limit=1&apiKey={self.api_key}"
        )
        try:
            r = self.session.get(url, timeout=getattr(self.session, "timeout_sec", 15))
            if r.status_code != 200:
                self._price_cache[dkey] = None
                return None
            rows = (r.json() if r.text else {}).get("results") or []
            if not rows:
                self._price_cache[dkey] = None
                return None
            c = pd.to_numeric(rows[0].get("c"), errors="coerce")
            v = float(c) if pd.notna(c) and c > 0 else None
            self._price_cache[dkey] = v
            return v
        except Exception:
            self._price_cache[dkey] = None
            return None

    def fetch_for_date(self, ticker: str, as_of: date) -> ProviderResult:
        if not self.api_key:
            return ProviderResult(as_of, ticker, None, None, None, self.name,
                                   f"polygon://{ticker}?error=missing_api_key", "missing")
        close = self._close(ticker, as_of)
        shares, mcap = self._meta(ticker)
        aum = mcap
        if (aum is None or aum <= 0) and close and shares:
            aum = close * shares
        status = _classify_status(close, aum, shares)
        return ProviderResult(as_of, ticker.upper(), close, aum, shares, self.name,
                               f"polygon://{ticker}", status)


# ---------------------------------------------------------------------------
# Provider stack / orchestrator
# ---------------------------------------------------------------------------

def build_default_stack(session: requests.Session | None = None) -> list:
    """Build the default provider stack (order matters: first success wins for 'ok').

    Issuer-authoritative providers run before YFinance/Polygon so leveraged/single-stock
    ETF AUM and NAV come from the fund admins (matching the dashboard's exposure math).
    """
    s = session or _build_session()
    return [
        TradrAxsProvider(s),
        ProSharesProvider(s),
        DirexionProvider(s),
        RoundhillProvider(s),
        YieldMaxProvider(s),
        REXSharesProvider(s),
        GraniteSharesProvider(),
        DefianceProvider(s),
        YFinanceProvider(),
        PolygonProvider(s),
    ]

"""
ETF metrics providers.

Layered fetch strategy (per ticker, tried in order):
  1) AXS / Tradr bulk CSV (authoritative; covers AXS + Tradr single-stock ETFs)
  2) ProShares bulk historical_nav.csv (authoritative; covers all ProShares funds)
  3) Direxion per-ticker holdings CSV (authoritative; covers Direxion 2x/3x)
  4) YFinanceProvider (broad fallback via Yahoo Finance fast_info / info)
  5) PolygonProvider (last-resort close + meta)

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
from datetime import date, timedelta
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
# 4) YFinance broad fallback
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
            shares = pd.to_numeric(res.get("weighted_shares_outstanding"), errors="coerce")
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
    """Build the default provider stack (order matters: first success wins for 'ok')."""
    s = session or _build_session()
    return [
        TradrAxsProvider(s),
        ProSharesProvider(s),
        DirexionProvider(s),
        YFinanceProvider(),
        PolygonProvider(s),
    ]

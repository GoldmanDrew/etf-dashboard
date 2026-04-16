#!/usr/bin/env python3
"""Ingest daily ETF NAV/AUM/Shares metrics for dashboard universe."""

from __future__ import annotations

import argparse
import io
import logging
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


LOGGER = logging.getLogger("etf_metrics_ingest")

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
UNIVERSE_CSV = DATA_DIR / "etf_screened_today.csv"
PARQUET_PATH = DATA_DIR / "etf_metrics_daily.parquet"
CSV_PATH = DATA_DIR / "etf_metrics_daily.csv"
JSON_PATH = DATA_DIR / "etf_metrics_daily.json"
LATEST_JSON_PATH = DATA_DIR / "etf_metrics_latest.json"

REQUIRED_COLUMNS = [
    "date",
    "ticker",
    "nav",
    "aum",
    "shares_outstanding",
    "source_provider",
    "source_url",
    "ingested_at_utc",
    "status",
]


def _build_session(timeout_sec: int = 20) -> requests.Session:
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s = requests.Session()
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({"User-Agent": "etf-dashboard-etf-metrics/1.0"})
    s.timeout_sec = timeout_sec  # type: ignore[attr-defined]
    return s


def _read_csv_url(session: requests.Session, url: str) -> pd.DataFrame | None:
    try:
        r = session.get(url, timeout=getattr(session, "timeout_sec", 20))
        if r.status_code != 200:
            return None
        return pd.read_csv(io.StringIO(r.text))
    except Exception:
        return None


@dataclass
class ProviderResult:
    date: date
    ticker: str
    nav: float | None
    aum: float | None
    shares_outstanding: float | None
    source_provider: str
    source_url: str
    status: str


class TradrAxsProvider:
    name = "tradr_axs"

    def __init__(self, session: requests.Session | None = None):
        self.session = session or _build_session()
        self._nav_cache: dict[date, pd.DataFrame | None] = {}
        self._hold_cache: dict[date, pd.DataFrame | None] = {}

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

        status = "ok"
        if pd.isna(nav) or pd.isna(aum) or pd.isna(shares) or float(nav) <= 0 or float(aum) <= 0 or float(shares) <= 0:
            status = "missing"

        return ProviderResult(as_of, ticker, nav, aum, shares, self.name, source_url, status)


class PolygonProvider:
    name = "polygon"

    def __init__(self, session: requests.Session | None = None):
        self.session = session or _build_session()
        self.api_key = os.getenv("POLYGON_API_KEY") or os.getenv("POLYGON_IO_API_KEY")
        self._meta_cache: dict[str, tuple[float | None, float | None]] = {}
        self._price_cache: dict[tuple[str, str], float | None] = {}

    def _get_meta(self, ticker: str) -> tuple[float | None, float | None]:
        key = ticker.upper()
        if key in self._meta_cache:
            return self._meta_cache[key]
        if not self.api_key:
            self._meta_cache[key] = (None, None)
            return self._meta_cache[key]
        url = f"https://api.polygon.io/v3/reference/tickers/{key}?apiKey={self.api_key}"
        try:
            r = self.session.get(url, timeout=getattr(self.session, "timeout_sec", 20))
            if r.status_code != 200:
                self._meta_cache[key] = (None, None)
                return self._meta_cache[key]
            data = r.json() if r.text else {}
            res = data.get("results") or {}
            shares = pd.to_numeric(res.get("weighted_shares_outstanding"), errors="coerce")
            market_cap = pd.to_numeric(res.get("market_cap"), errors="coerce")
            s_val = float(shares) if pd.notna(shares) and float(shares) > 0 else None
            mc_val = float(market_cap) if pd.notna(market_cap) and float(market_cap) > 0 else None
            self._meta_cache[key] = (s_val, mc_val)
            return self._meta_cache[key]
        except Exception:
            self._meta_cache[key] = (None, None)
            return self._meta_cache[key]

    def _get_close(self, ticker: str, as_of: date) -> float | None:
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
            r = self.session.get(url, timeout=getattr(self.session, "timeout_sec", 20))
            if r.status_code != 200:
                self._price_cache[dkey] = None
                return None
            data = r.json() if r.text else {}
            rows = data.get("results") or []
            if not rows:
                self._price_cache[dkey] = None
                return None
            close = pd.to_numeric(rows[0].get("c"), errors="coerce")
            val = float(close) if pd.notna(close) and float(close) > 0 else None
            self._price_cache[dkey] = val
            return val
        except Exception:
            self._price_cache[dkey] = None
            return None

    def fetch_for_date(self, ticker: str, as_of: date) -> ProviderResult:
        close = self._get_close(ticker, as_of)
        shares, market_cap = self._get_meta(ticker)
        aum = market_cap
        if (aum is None or aum <= 0) and close is not None and shares is not None and shares > 0:
            aum = close * shares
        status = "ok"
        if close is None or shares is None or aum is None or close <= 0 or shares <= 0 or aum <= 0:
            status = "missing"
        return ProviderResult(
            date=as_of,
            ticker=ticker,
            nav=close,
            aum=aum,
            shares_outstanding=shares,
            source_provider=self.name,
            source_url=f"polygon://{ticker}",
            status=status,
        )


def _normalize_symbol(v: object) -> str:
    return str(v).strip().upper().replace(".", "-")


def load_universe_tickers(path: Path = UNIVERSE_CSV) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Universe CSV missing: {path}")
    df = pd.read_csv(path)
    if "ETF" not in df.columns:
        raise ValueError(f"Universe CSV missing ETF column: {path}")
    syms = sorted({_normalize_symbol(x) for x in df["ETF"].dropna().tolist()})
    return syms


def _iter_dates(start_date: date, end_date: date) -> Iterable[date]:
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)


def _records_to_df(records: list[ProviderResult], ingested_at: datetime) -> pd.DataFrame:
    rows = []
    for r in records:
        rows.append(
            {
                "date": r.date.isoformat(),
                "ticker": r.ticker.upper(),
                "nav": r.nav,
                "aum": r.aum,
                "shares_outstanding": r.shares_outstanding,
                "source_provider": r.source_provider,
                "source_url": r.source_url,
                "ingested_at_utc": ingested_at.isoformat(),
                "status": r.status,
            }
        )
    out = pd.DataFrame(rows)
    for c in REQUIRED_COLUMNS:
        if c not in out.columns:
            out[c] = None
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    return out[REQUIRED_COLUMNS]


def enforce_status_consistency(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    nav = pd.to_numeric(out["nav"], errors="coerce")
    aum = pd.to_numeric(out["aum"], errors="coerce")
    shares = pd.to_numeric(out["shares_outstanding"], errors="coerce")
    invalid = nav.isna() | aum.isna() | shares.isna() | (nav <= 0) | (aum <= 0) | (shares <= 0)
    out.loc[(out["status"] == "ok") & invalid, "status"] = "missing"
    return out


def validate_df(df: pd.DataFrame) -> None:
    if df["date"].isna().any():
        raise ValueError("null dates found")
    if df.duplicated(subset=["date", "ticker"], keep=False).any():
        raise ValueError("duplicate (date,ticker) rows found")
    ok = df["status"] == "ok"
    nav = pd.to_numeric(df["nav"], errors="coerce")
    aum = pd.to_numeric(df["aum"], errors="coerce")
    shares = pd.to_numeric(df["shares_outstanding"], errors="coerce")
    if (ok & (nav.isna() | (nav <= 0))).any():
        raise ValueError("invalid nav for ok rows")
    if (ok & (aum.isna() | (aum <= 0))).any():
        raise ValueError("invalid aum for ok rows")
    if (ok & (shares.isna() | (shares <= 0))).any():
        raise ValueError("invalid shares for ok rows")


def load_existing(parquet_path: Path = PARQUET_PATH) -> pd.DataFrame:
    if parquet_path.exists():
        return pd.read_parquet(parquet_path)
    if CSV_PATH.exists():
        return pd.read_csv(CSV_PATH)
    return pd.DataFrame(columns=REQUIRED_COLUMNS)


def upsert(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        first = incoming.copy()
        first["ingested_at_utc"] = pd.to_datetime(first["ingested_at_utc"], errors="coerce", utc=True)
        first = first.drop_duplicates(subset=["date", "ticker"], keep="last")
        return enforce_status_consistency(first.sort_values(["date", "ticker"]).reset_index(drop=True))

    combo = pd.concat([existing, incoming], ignore_index=True)
    combo["ingested_at_utc"] = pd.to_datetime(combo["ingested_at_utc"], errors="coerce", utc=True)
    combo = combo.sort_values("ingested_at_utc")
    combo = combo.drop_duplicates(subset=["date", "ticker"], keep="last")
    combo = combo.sort_values(["date", "ticker"]).reset_index(drop=True)
    return enforce_status_consistency(combo)


def save_outputs(df: pd.DataFrame) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(PARQUET_PATH, index=False)
    df.to_csv(CSV_PATH, index=False)
    json_rows = df.copy()
    json_rows["date"] = pd.to_datetime(json_rows["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    json_rows["ingested_at_utc"] = pd.to_datetime(json_rows["ingested_at_utc"], errors="coerce", utc=True).astype(str)

    json_payload = {
        "build_time": datetime.now(UTC).isoformat(),
        "rows": json_rows.to_dict("records"),
    }
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        import json

        json.dump(json_payload, f, separators=(",", ":"))

    work = df.copy()
    work["date"] = pd.to_datetime(work["date"]).dt.date
    ok = work[work["status"] == "ok"]
    latest_date = ok["date"].max() if not ok.empty else (work["date"].max() if not work.empty else None)
    latest_rows = work[work["date"] == latest_date] if latest_date is not None else work.iloc[0:0]
    latest_rows_json = latest_rows.copy()
    latest_rows_json["date"] = pd.to_datetime(latest_rows_json["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    latest_rows_json["ingested_at_utc"] = pd.to_datetime(
        latest_rows_json["ingested_at_utc"], errors="coerce", utc=True
    ).astype(str)
    latest_map_json = {str(r["ticker"]).upper(): r.to_dict() for _, r in latest_rows_json.iterrows()}
    latest_payload = {
        "build_time": datetime.now(UTC).isoformat(),
        "latest_date": latest_date.isoformat() if latest_date is not None else None,
        "rows": latest_rows_json.to_dict("records"),
        "by_symbol": latest_map_json,
    }
    with open(LATEST_JSON_PATH, "w", encoding="utf-8") as f:
        import json

        json.dump(latest_payload, f, separators=(",", ":"))


def ingest(
    tickers: list[str],
    lookback_days: int = 10,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pd.DataFrame:
    tradr_provider = TradrAxsProvider()
    polygon_provider = PolygonProvider()
    if end_date is None:
        end_date = date.today()
    if start_date is None:
        start_date = end_date
    rows: list[ProviderResult] = []

    if start_date == end_date:
        for t in tickers:
            found = False
            for i in range(max(1, lookback_days)):
                d = end_date - timedelta(days=i)
                r = tradr_provider.fetch_for_date(t, d)
                if r.status == "ok":
                    rows.append(r)
                    found = True
                    break
            if not found:
                poly_found = False
                for i in range(max(1, lookback_days)):
                    d = end_date - timedelta(days=i)
                    p = polygon_provider.fetch_for_date(t, d)
                    if p.status == "ok":
                        rows.append(p)
                        poly_found = True
                        break
                if not poly_found:
                    rows.append(tradr_provider.fetch_for_date(t, end_date))
    else:
        for d in _iter_dates(start_date, end_date):
            for t in tickers:
                r = tradr_provider.fetch_for_date(t, d)
                if r.status == "ok":
                    rows.append(r)
                else:
                    rows.append(polygon_provider.fetch_for_date(t, d))

    out = _records_to_df(rows, ingested_at=datetime.now(UTC))
    out = enforce_status_consistency(out)
    validate_df(out)
    return out


def get_summary(df: pd.DataFrame) -> dict:
    return {
        "rows": int(len(df)),
        "ok": int((df["status"] == "ok").sum()),
        "missing": int((df["status"] == "missing").sum()),
        "latest_date": str(pd.to_datetime(df["date"]).max().date()) if not df.empty else None,
    }


def parse_date_arg(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest ETF NAV/AUM/shares metrics for etf-dashboard.")
    parser.add_argument("--lookback-days", type=int, default=10)
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    tickers = load_universe_tickers()
    LOGGER.info("Universe tickers: %d", len(tickers))

    incoming = ingest(
        tickers=tickers,
        lookback_days=args.lookback_days,
        start_date=parse_date_arg(args.start_date),
        end_date=parse_date_arg(args.end_date),
    )
    LOGGER.info("Incoming summary: %s", get_summary(incoming))

    existing = load_existing()
    merged = upsert(existing, incoming)
    validate_df(merged)
    save_outputs(merged)
    LOGGER.info("Saved merged summary: %s", get_summary(merged))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Ingest daily ETF NAV / AUM / Shares Outstanding metrics for the dashboard universe.

Provider stack (tried per ticker, in order):
  1) TradrAxsProvider    -- AXS / Tradr NSDEAXS2 + BBH_AXS_ETF_PVAL_WEB CSVs (authoritative)
  2) ProSharesProvider   -- ProShares historical_nav.csv bulk file (authoritative)
  3) DirexionProvider    -- Direxion per-ticker holdings CSV (authoritative; NAV = AUM/shares)
  4) RoundhillProvider   -- Roundhill FilepointRoundhill.40RU.RU_DailyNAV.csv bulk (authoritative)
  5) YieldMaxProvider    -- per-ticker HTML scrape of yieldmaxetfs.com (authoritative)
  6) REXSharesProvider   -- per-ticker HTML scrape of rexshares.com (authoritative; NAV=AUM/shares)
  7) GraniteSharesProvider -- graniteshares.com /product/{id}/ JSON (authoritative for Granite ETFs)
  8) DefianceProvider     -- defianceetfs.com/{ticker}/ Fund Details scrape (Defiance daily/ thematic ETFs)
  9) YFinanceProvider    -- Yahoo fast_info + info (broad fallback; JPMorgan JEPI/JEPQ, Global X, etc.)
  10) PolygonProvider     -- Polygon v2/aggs + v3/reference (last resort; reliable for close price)

Row statuses:
  'ok'      -> all of (nav, aum, shares) present and positive
  'partial' -> at least one of the three fields present (UI still has a NAV series to plot)
  'missing' -> nothing usable

Before validation, ``repair_shares_vs_aum_nav`` may set ``shares_outstanding`` to
``aum / nav`` when the reported figure disagrees by >80× (decimal-shift glitches).

Every ticker ends up with exactly one row stamped at ``end_date`` in single-day mode; results
sourced from an earlier date are flagged ``stale=True`` with ``stale_age_bdays``.

After each merge, ``collapse_redundant_consecutive_rows`` drops calendar days where
(nav, aum, shares_outstanding) match the prior kept row for that ticker—removing flat
runs caused by repeat jobs before issuers publish new figures. Scheduled GitHub Actions
runs can skip a full re-fetch when ``ETF_METRICS_SKIP_IF_RECENT_HOURS`` is set and the
last ingest for the target date was recent (see workflow env).
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from etf_providers import (
    ProviderResult,
    _build_session,
    build_default_stack,
    merge_provider_attempts,
)
from etf_holdings_providers import (
    HOLDINGS_COLUMNS,
    build_default_holdings_stack,
    fetch_all_holdings,
)


LOGGER = logging.getLogger("etf_metrics_ingest")

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
UNIVERSE_CSV = DATA_DIR / "etf_screened_today.csv"
PARQUET_PATH = DATA_DIR / "etf_metrics_daily.parquet"
CSV_PATH = DATA_DIR / "etf_metrics_daily.csv"
JSON_PATH = DATA_DIR / "etf_metrics_daily.json"
LATEST_JSON_PATH = DATA_DIR / "etf_metrics_latest.json"
HEALTH_JSON_PATH = DATA_DIR / "etf_metrics_health.json"
HOLDINGS_PARQUET_PATH = DATA_DIR / "etf_holdings_daily.parquet"
# Human-readable **latest date only** (positions flat file).  Full history lives
# in ``etf_holdings_daily.parquet`` — committing the multi-day CSV duplicated
# storage and produced noisy diffs on every workflow run.
HOLDINGS_LATEST_CSV_PATH = DATA_DIR / "etf_holdings_latest.csv"
HOLDINGS_LATEST_JSON_PATH = DATA_DIR / "etf_holdings_latest.json"

REQUIRED_COLUMNS = [
    "date",
    "ticker",
    "nav",
    "aum",
    "shares_outstanding",
    "close_price",
    "underlying_adj_close",
    "stale",
    "stale_age_bdays",
    "source_provider",
    "source_url",
    "ingested_at_utc",
    "status",
]


# ---------------------------------------------------------------------------
# Universe / helpers
# ---------------------------------------------------------------------------

def _normalize_symbol(v: object) -> str:
    return str(v).strip().upper().replace(".", "-")


def load_universe_tickers(path: Path = UNIVERSE_CSV) -> list[str]:
    if not path.exists():
        raise FileNotFoundError(f"Universe CSV missing: {path}")
    df = pd.read_csv(path)
    if "ETF" not in df.columns:
        raise ValueError(f"Universe CSV missing ETF column: {path}")
    return sorted({_normalize_symbol(x) for x in df["ETF"].dropna().tolist()})


def load_universe_underlying_map(path: Path = UNIVERSE_CSV) -> dict[str, str]:
    """Map ETF ticker -> underlying symbol from the screener CSV (Granite path)."""
    if not path.exists():
        return {}
    df = pd.read_csv(path)
    if "ETF" not in df.columns or "Underlying" not in df.columns:
        return {}
    out: dict[str, str] = {}
    for _, row in df.iterrows():
        t = _normalize_symbol(row.get("ETF"))
        u = row.get("Underlying")
        if t and pd.notna(u) and str(u).strip():
            out[t] = _normalize_symbol(u)
    return out


def _iter_dates(start_date: date, end_date: date) -> Iterable[date]:
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)


def _records_to_df(records: list[ProviderResult], ingested_at: datetime) -> pd.DataFrame:
    rows = []
    for r in records:
        rows.append({
            "date": r.date.isoformat(),
            "ticker": r.ticker.upper(),
            "nav": r.nav,
            "aum": r.aum,
            "shares_outstanding": r.shares_outstanding,
            "close_price": None,
            "underlying_adj_close": None,
            "stale": bool(r.stale),
            "stale_age_bdays": r.stale_age_bdays,
            "source_provider": r.source_provider,
            "source_url": r.source_url,
            "ingested_at_utc": ingested_at.isoformat(),
            "status": r.status,
        })
    out = pd.DataFrame(rows)
    for c in REQUIRED_COLUMNS:
        if c not in out.columns:
            out[c] = None
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    return out[REQUIRED_COLUMNS]


def repair_shares_vs_aum_nav(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Fix ``shares_outstanding`` when it disagrees with ``aum / nav`` by a huge factor.

    Issuer and Yahoo feeds occasionally emit decimal-shifted share counts. When
    NAV and AUM are mutually consistent with a third share count ``aum/nav`` but
    reported shares differ by more than 80×, replace shares with that implied
    value (same rule as the dashboard Stats panel).
    """
    if df.empty or "shares_outstanding" not in df.columns:
        return df, 0
    out = df.copy()
    nav = pd.to_numeric(out["nav"], errors="coerce")
    aum = pd.to_numeric(out["aum"], errors="coerce")
    shares = pd.to_numeric(out["shares_outstanding"], errors="coerce")
    implied = aum / nav
    ok_triple = (
        nav.notna()
        & (nav > 0)
        & aum.notna()
        & (aum > 0)
        & shares.notna()
        & (shares > 0)
        & implied.notna()
        & (implied > 0)
    )
    ratio = shares / implied
    bad = ok_triple & ((ratio > 80.0) | (ratio < (1.0 / 80.0)))
    n_bad = int(bad.sum())
    if n_bad:
        LOGGER.warning(
            "repair_shares_vs_aum_nav: correcting %d row(s) where shares diverged >80× from aum/nav",
            n_bad,
        )
        out.loc[bad, "shares_outstanding"] = implied[bad]
    # Whole share counts in outputs (providers sometimes emit float noise, which UIs format as 87,333.079.)
    sh_col = pd.to_numeric(out["shares_outstanding"], errors="coerce")
    round_mask = sh_col.notna() & (sh_col > 0)
    if round_mask.any():
        out.loc[round_mask, "shares_outstanding"] = np.round(sh_col[round_mask])
    return out, n_bad


def enforce_status_consistency(df: pd.DataFrame) -> pd.DataFrame:
    """Reconcile stored status with actual field presence. Preserves 'partial' rows."""
    out = df.copy()
    if "stale" not in out.columns:
        out["stale"] = False
    if "stale_age_bdays" not in out.columns:
        out["stale_age_bdays"] = None
    nav = pd.to_numeric(out["nav"], errors="coerce")
    aum = pd.to_numeric(out["aum"], errors="coerce")
    shares = pd.to_numeric(out["shares_outstanding"], errors="coerce")

    has_nav = nav.notna() & (nav > 0)
    has_aum = aum.notna() & (aum > 0)
    has_shares = shares.notna() & (shares > 0)
    full_ok = has_nav & has_aum & has_shares
    any_field = has_nav | has_aum | has_shares

    new_status = out["status"].astype(str).copy()
    new_status = new_status.where(~full_ok, "ok")
    partial_mask = (~full_ok) & any_field
    new_status = new_status.where(~partial_mask, "partial")
    none_mask = ~any_field
    new_status = new_status.where(~none_mask, "missing")
    out["status"] = new_status

    # Stale flags only meaningful for ok/partial rows
    out.loc[out["status"] == "missing", "stale"] = False
    out.loc[out["status"] == "missing", "stale_age_bdays"] = None
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


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def load_existing(parquet_path: Path = PARQUET_PATH) -> pd.DataFrame:
    if parquet_path.exists():
        df = pd.read_parquet(parquet_path)
    elif CSV_PATH.exists():
        df = pd.read_csv(CSV_PATH)
    else:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)
    # Back-fill newly added columns on historical rows so the canonical frame
    # always matches REQUIRED_COLUMNS regardless of the file's generation.
    for c in REQUIRED_COLUMNS:
        if c not in df.columns:
            df[c] = None
    return df


def _metric_triple_equal(a, b) -> bool:
    """Compare nav/aum/shares for redundancy; NaN matches NaN; floats use isclose."""
    if a is None and b is None:
        return True
    try:
        fa, fb = float(a), float(b)
        if math.isnan(fa) and math.isnan(fb):
            return True
        if math.isnan(fa) or math.isnan(fb):
            return False
        return math.isclose(fa, fb, rel_tol=1e-9, abs_tol=1e-6)
    except (TypeError, ValueError):
        return a == b


def collapse_redundant_consecutive_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Drop consecutive calendar rows per ticker when core metrics + overlays are unchanged.

    Multiple scheduler or manual runs can re-ingest the same issuer figures for a new
    stamped date even when public feeds have not updated, producing flat stretches of
    identical values. Removing interior duplicates keeps history compact without losing
    the first date a value appeared or real step-changes.

    ``close_price`` and ``underlying_adj_close`` are part of the fingerprint so we do
    not drop days where only Yahoo prices moved while issuer NAV/AUM/shares repeated.
    """
    if df.empty:
        return df, 0
    work = df.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.date
    work = work.sort_values(["ticker", "date"]).reset_index(drop=True)

    keep_mask = [True] * len(work)
    last_sig_by_ticker: dict[str, tuple] = {}
    for i, row in work.iterrows():
        t = str(row["ticker"]).upper()
        sig = (
            row.get("nav"),
            row.get("aum"),
            row.get("shares_outstanding"),
            row.get("close_price"),
            row.get("underlying_adj_close"),
        )
        prev = last_sig_by_ticker.get(t)
        if prev is not None and all(_metric_triple_equal(a, b) for a, b in zip(prev, sig)):
            keep_mask[i] = False
        else:
            last_sig_by_ticker[t] = sig

    cleaned = work.loc[keep_mask].reset_index(drop=True)
    dropped = int(sum(1 for k in keep_mask if not k))
    return cleaned, dropped


def upsert(existing: pd.DataFrame, incoming: pd.DataFrame) -> pd.DataFrame:
    if existing.empty:
        first = incoming.copy()
        first["ingested_at_utc"] = pd.to_datetime(first["ingested_at_utc"], errors="coerce", utc=True)
        first = first.drop_duplicates(subset=["date", "ticker"], keep="last")
        return enforce_status_consistency(first.sort_values(["date", "ticker"]).reset_index(drop=True))

    # Drop columns that are entirely empty in either frame before concat to silence a future
    # pandas warning about dtype-widening behavior.
    _ex = existing.dropna(axis=1, how="all") if len(existing) else existing
    _in = incoming.dropna(axis=1, how="all") if len(incoming) else incoming
    combo = pd.concat([_ex, _in], ignore_index=True)
    combo["ingested_at_utc"] = pd.to_datetime(combo["ingested_at_utc"], errors="coerce", utc=True)
    combo = combo.sort_values("ingested_at_utc")
    combo = combo.drop_duplicates(subset=["date", "ticker"], keep="last")
    combo = combo.sort_values(["date", "ticker"]).reset_index(drop=True)
    return enforce_status_consistency(combo)


def apply_stale_carry_forward(
    existing: pd.DataFrame,
    incoming: pd.DataFrame,
    as_of_date: date,
    max_stale_business_days: int = 3,
) -> pd.DataFrame:
    """Fill today's rows whose status is not 'ok' from the most recent 'ok' row in history."""
    out = incoming.copy()
    if out.empty or existing.empty or max_stale_business_days <= 0:
        return out
    for c in REQUIRED_COLUMNS:
        if c not in out.columns:
            out[c] = None

    hist = existing.copy()
    hist["date"] = pd.to_datetime(hist["date"], errors="coerce").dt.date
    hist = hist[hist["status"] == "ok"].copy()
    if hist.empty:
        return out

    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    target = as_of_date
    upgrade_idx = out.index[(out["date"] == target) & (out["status"] != "ok")].tolist()
    for idx in upgrade_idx:
        sym = str(out.at[idx, "ticker"]).upper()
        cand = hist[(hist["ticker"].astype(str).str.upper() == sym) & (hist["date"] < target)]
        if cand.empty:
            continue
        cand = cand.sort_values("date")
        last = cand.iloc[-1]
        try:
            age_bdays = int(np.busday_count(str(last["date"]), str(target)))
        except Exception:
            age_bdays = 999999
        if age_bdays < 1 or age_bdays > max_stale_business_days:
            continue
        out.at[idx, "nav"] = float(last["nav"])
        out.at[idx, "aum"] = float(last["aum"])
        out.at[idx, "shares_outstanding"] = float(last["shares_outstanding"])
        out.at[idx, "status"] = "ok"
        out.at[idx, "stale"] = True
        out.at[idx, "stale_age_bdays"] = int(age_bdays)
        out.at[idx, "source_provider"] = "carry_forward"
        out.at[idx, "source_url"] = f"carry_forward://{sym}?from={last['date']}"
    return out


def _sanitize_json_df(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    d["ingested_at_utc"] = pd.to_datetime(d["ingested_at_utc"], errors="coerce", utc=True).astype(str)
    for col in ("nav", "aum", "shares_outstanding", "close_price", "underlying_adj_close", "stale_age_bdays"):
        if col in d.columns:
            d[col] = pd.to_numeric(d[col], errors="coerce").replace([np.inf, -np.inf], np.nan)
    return d.astype(object).where(pd.notna(d), None)


# ---------------------------------------------------------------------------
# Close-price fetch (yfinance bulk) + holdings persistence
# ---------------------------------------------------------------------------

def _yf_download_ohlcv(
    tickers: list[str],
    start: date,
    end: date,
    *,
    auto_adjust: bool,
) -> pd.DataFrame | None:
    """Run yfinance multi-symbol daily download; return raw frame or None on failure."""
    if not tickers:
        return None
    if os.getenv("ETF_METRICS_DISABLE_YFINANCE", "").lower() in ("1", "true", "yes"):
        return None
    try:
        import yfinance as yf
    except Exception as e:  # pragma: no cover
        LOGGER.warning("yfinance unavailable: %s", e)
        return None
    start_s = start.isoformat()
    end_s = (end + timedelta(days=1)).isoformat()
    try:
        return yf.download(
            tickers=list(tickers),
            start=start_s,
            end=end_s,
            interval="1d",
            auto_adjust=auto_adjust,
            actions=False,
            group_by="ticker",
            threads=True,
            progress=False,
        )
    except Exception as e:
        LOGGER.warning("yfinance batch download failed (auto_adjust=%s): %s", auto_adjust, e)
        return None


def _extract_yf_series_to_long(
    raw: pd.DataFrame,
    tickers: list[str],
    price_col: str,
    out_value_key: str,
) -> pd.DataFrame:
    """Pull ``price_col`` per ticker from yfinance download into long-form rows."""
    cols = ["date", "ticker", out_value_key]
    if raw is None or raw.empty:
        return pd.DataFrame(columns=cols)
    records: list[dict] = []
    if isinstance(raw.columns, pd.MultiIndex):
        for t in tickers:
            up = t.upper()
            if t not in raw.columns.get_level_values(0):
                continue
            try:
                sub = raw[t]
            except KeyError:
                continue
            if price_col not in sub.columns:
                continue
            ser = sub[price_col].dropna()
            for idx, v in ser.items():
                try:
                    c = float(v)
                except (TypeError, ValueError):
                    continue
                if c > 0:
                    records.append({
                        "date": idx.date() if hasattr(idx, "date") else idx,
                        "ticker": up,
                        out_value_key: c,
                    })
    else:
        if price_col in raw.columns and len(tickers) == 1:
            up = tickers[0].upper()
            for idx, v in raw[price_col].dropna().items():
                try:
                    c = float(v)
                except (TypeError, ValueError):
                    continue
                if c > 0:
                    records.append({
                        "date": idx.date() if hasattr(idx, "date") else idx,
                        "ticker": up,
                        out_value_key: c,
                    })
    if not records:
        return pd.DataFrame(columns=cols)
    out = pd.DataFrame.from_records(records)
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out = out.dropna(subset=["date", "ticker"])
    return out.drop_duplicates(subset=["date", "ticker"], keep="last")


def fetch_close_prices_batch(
    tickers: list[str],
    start: date,
    end: date,
) -> pd.DataFrame:
    """Return long-form DataFrame (date, ticker, close_price) from yfinance.

    Uses ``yf.download`` with a ticker list; that issues a single multi-symbol
    request rather than one GET per ticker, which is ~20x faster and stays
    well under Yahoo's rate ceiling for cloud IPs.

    An empty DataFrame is returned on any failure so the caller can proceed
    without close-prices rather than blow up the whole ingest.
    """
    cols = ["date", "ticker", "close_price"]
    if os.getenv("ETF_METRICS_DISABLE_YFINANCE", "").lower() in ("1", "true", "yes"):
        LOGGER.info("close-price fetch disabled (ETF_METRICS_DISABLE_YFINANCE)")
        return pd.DataFrame(columns=cols)
    raw = _yf_download_ohlcv(tickers, start, end, auto_adjust=False)
    out = _extract_yf_series_to_long(raw, tickers, "Close", "close_price")
    return out


def fetch_underlying_adj_close_batch(
    und_symbols: list[str],
    start: date,
    end: date,
) -> pd.DataFrame:
    """Long-form (date, ticker, underlying_adj_close) using Yahoo *adjusted* close.

    ``auto_adjust=True`` so the column is dividend/split-adjusted total-return
    level (matches common \"Adj Close\" semantics). One row per (date, und ticker).

    Yahoo's multi-symbol ``download`` becomes unreliable past ~60–80 tickers
    (random symbols silently missing from the returned frame). Chunk the
    universe and concatenate so every underlying gets a fair fetch pass.
    """
    cols = ["date", "ticker", "underlying_adj_close"]
    if os.getenv("ETF_METRICS_DISABLE_YFINANCE", "").lower() in ("1", "true", "yes"):
        return pd.DataFrame(columns=cols)
    uniq = sorted({_normalize_symbol(s) for s in und_symbols if s and str(s).strip()})
    if not uniq:
        return pd.DataFrame(columns=cols)
    chunk_sz = max(1, int(os.getenv("ETF_METRICS_UNDERLYING_YF_CHUNK_SIZE", "50")))
    frames: list[pd.DataFrame] = []
    for i in range(0, len(uniq), chunk_sz):
        chunk = uniq[i : i + chunk_sz]
        raw = _yf_download_ohlcv(chunk, start, end, auto_adjust=True)
        part = _extract_yf_series_to_long(raw, chunk, "Close", "underlying_adj_close")
        if not part.empty:
            frames.append(part)
        else:
            LOGGER.warning(
                "underlying_adj_close chunk empty (%d tickers, offset %d)",
                len(chunk), i,
            )
    if not frames:
        return pd.DataFrame(columns=cols)
    out = pd.concat(frames, ignore_index=True)
    return out.drop_duplicates(subset=["date", "ticker"], keep="last")


def merge_close_prices(df: pd.DataFrame, close_df: pd.DataFrame) -> pd.DataFrame:
    """Left-join close prices onto the metrics frame on (date, ticker)."""
    if close_df.empty:
        if "close_price" not in df.columns:
            df = df.copy()
            df["close_price"] = None
        return df
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out["ticker"] = out["ticker"].astype(str).str.upper()
    merged = out.merge(
        close_df.rename(columns={"close_price": "_close_new"}),
        on=["date", "ticker"],
        how="left",
    )
    if "close_price" not in merged.columns:
        merged["close_price"] = None
    # Prefer the freshly fetched value; keep the existing one when yfinance is silent.
    merged["close_price"] = pd.to_numeric(merged["_close_new"], errors="coerce").combine_first(
        pd.to_numeric(merged["close_price"], errors="coerce")
    )
    merged = merged.drop(columns=["_close_new"])
    return merged


def merge_underlying_adj_close(
    df: pd.DataFrame,
    und_close_df: pd.DataFrame,
    etf_to_underlying: dict[str, str],
) -> pd.DataFrame:
    """Attach Yahoo adjusted underlying close per ETF row on (date, underlying)."""
    if und_close_df.empty or not etf_to_underlying:
        if "underlying_adj_close" not in df.columns:
            df = df.copy()
            df["underlying_adj_close"] = None
        return df
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out["ticker"] = out["ticker"].astype(str).str.upper()

    def _etf_und(sym: object) -> str | None:
        t = _normalize_symbol(sym) if sym is not None and str(sym).strip() else ""
        if not t:
            return None
        u = etf_to_underlying.get(t)
        if u is None or pd.isna(u):
            return None
        us = str(u).strip()
        return _normalize_symbol(us) if us else None

    out["_und_ticker"] = out["ticker"].map(_etf_und)
    und_side = und_close_df.rename(columns={"ticker": "_und_ticker", "underlying_adj_close": "_und_adj_new"})
    und_side["date"] = pd.to_datetime(und_side["date"], errors="coerce").dt.date
    merged = out.merge(
        und_side,
        left_on=["date", "_und_ticker"],
        right_on=["date", "_und_ticker"],
        how="left",
    )
    merged = merged.drop(columns=["_und_ticker"])
    if "underlying_adj_close" not in merged.columns:
        merged["underlying_adj_close"] = None
    merged["underlying_adj_close"] = pd.to_numeric(merged["_und_adj_new"], errors="coerce").combine_first(
        pd.to_numeric(merged["underlying_adj_close"], errors="coerce")
    )
    merged = merged.drop(columns=["_und_adj_new"])
    return merged


def save_holdings_outputs(
    new_rows: pd.DataFrame,
    parquet_path: Path | None = None,
    latest_csv_path: Path | None = None,
    latest_json_path: Path | None = None,
) -> None:
    # Resolve lazily so module-level monkeypatching in tests is honored.
    parquet_path = parquet_path or HOLDINGS_PARQUET_PATH
    latest_csv_path = latest_csv_path or HOLDINGS_LATEST_CSV_PATH
    latest_json_path = latest_json_path or HOLDINGS_LATEST_JSON_PATH
    """Append new holdings rows into the canonical parquet store, refresh the
    latest-date CSV snapshot for humans browsing GitHub, and write the
    latest-per-ticker JSON blob consumed by the dashboard UI.

    Duplicate positions (same etf, same as_of_date, same position_ticker+cusip
    combination, same market value) are dropped so reruns don't bloat history.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    incoming = new_rows.copy()
    if incoming.empty:
        LOGGER.info("holdings: incoming frame empty, nothing to persist")
        # Ensure the latest JSON file stays in sync (empty run still writes meta).
        payload = {"build_time": datetime.now(UTC).isoformat(), "rows": [], "by_symbol": {}}
        with open(latest_json_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, separators=(",", ":"), allow_nan=False)
        pd.DataFrame(columns=HOLDINGS_COLUMNS).to_csv(latest_csv_path, index=False)
        return

    for c in HOLDINGS_COLUMNS:
        if c not in incoming.columns:
            incoming[c] = None
    incoming = incoming[HOLDINGS_COLUMNS]
    incoming["as_of_date"] = pd.to_datetime(incoming["as_of_date"], errors="coerce").dt.date
    incoming["etf_ticker"] = incoming["etf_ticker"].astype(str).str.upper()

    if parquet_path.exists():
        try:
            hist = pd.read_parquet(parquet_path)
            for c in HOLDINGS_COLUMNS:
                if c not in hist.columns:
                    hist[c] = None
            hist = hist[HOLDINGS_COLUMNS]
            hist["as_of_date"] = pd.to_datetime(hist["as_of_date"], errors="coerce").dt.date
            hist["etf_ticker"] = hist["etf_ticker"].astype(str).str.upper()
            combo = pd.concat([hist, incoming], ignore_index=True)
        except Exception as e:
            LOGGER.warning("holdings: failed to read existing parquet, starting fresh: %s", e)
            combo = incoming
    else:
        combo = incoming

    dedup_keys = [
        "as_of_date", "etf_ticker", "position_ticker", "cusip",
        "shares", "market_value", "weight_pct",
    ]
    combo = combo.drop_duplicates(subset=dedup_keys, keep="last")
    combo = combo.sort_values(["as_of_date", "etf_ticker", "weight_pct"], ascending=[True, True, False])
    combo = combo.reset_index(drop=True)

    combo.to_parquet(parquet_path, index=False)
    LOGGER.info(
        "holdings: saved %d total rows (%d new) across %d ETFs, latest date=%s",
        len(combo), len(incoming),
        combo["etf_ticker"].nunique(),
        combo["as_of_date"].max(),
    )

    # Latest snapshot: keep only the newest as_of_date per ETF — that's what the UI renders.
    latest = combo.sort_values(["etf_ticker", "as_of_date"])
    latest = latest.groupby("etf_ticker", group_keys=False).apply(
        lambda g: g[g["as_of_date"] == g["as_of_date"].max()]
    )
    latest.to_csv(latest_csv_path, index=False)
    latest_json = latest.copy()
    latest_json["as_of_date"] = pd.to_datetime(latest_json["as_of_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    for c in ("shares", "price", "market_value", "weight_pct"):
        latest_json[c] = pd.to_numeric(latest_json[c], errors="coerce").replace([np.inf, -np.inf], np.nan)
    latest_json = latest_json.astype(object).where(pd.notna(latest_json), None)
    by_symbol: dict[str, list[dict]] = {}
    for sym, g in latest_json.groupby("etf_ticker"):
        by_symbol[str(sym).upper()] = g.drop(columns=["etf_ticker"]).to_dict("records")
    payload = {
        "build_time": datetime.now(UTC).isoformat(),
        "latest_date": str(combo["as_of_date"].max()) if not combo.empty else None,
        "symbols": sorted(by_symbol.keys()),
        "by_symbol": by_symbol,
    }
    with open(latest_json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), allow_nan=False)


def save_outputs(df: pd.DataFrame) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(PARQUET_PATH, index=False)
    df.to_csv(CSV_PATH, index=False)

    # Full daily JSON
    json_rows = _sanitize_json_df(df)
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump({
            "build_time": datetime.now(UTC).isoformat(),
            "rows": json_rows.to_dict("records"),
        }, f, separators=(",", ":"), allow_nan=False)

    # Latest snapshot JSON.
    # After dedup-style collapsing, a ticker's most-recent row may be older than
    # the overall max date.  We take the newest row per ticker so every ticker
    # is always represented in the latest snapshot.
    work = df.copy()
    work["date"] = pd.to_datetime(work["date"]).dt.date
    latest_date = work["date"].max() if not work.empty else None
    if not work.empty:
        idx = work.groupby("ticker")["date"].idxmax()
        latest_rows = work.loc[idx].sort_values("ticker").reset_index(drop=True)
    else:
        latest_rows = work.iloc[0:0]
    latest_rows_json = _sanitize_json_df(latest_rows)
    latest_map_json = {str(r["ticker"]).upper(): r.to_dict() for _, r in latest_rows_json.iterrows()}
    with open(LATEST_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump({
            "build_time": datetime.now(UTC).isoformat(),
            "latest_date": latest_date.isoformat() if latest_date is not None else None,
            "rows": latest_rows_json.to_dict("records"),
            "by_symbol": latest_map_json,
        }, f, separators=(",", ":"), allow_nan=False)

    # Enriched health / diagnostics
    latest = latest_rows_json.copy()
    latest_status = latest["status"].astype(str) if not latest.empty else pd.Series(dtype=str)
    latest_provider = latest["source_provider"].astype(str) if not latest.empty else pd.Series(dtype=str)
    latest_stale_series = (
        pd.to_numeric(latest.get("stale", 0), errors="coerce").fillna(0).astype(int)
        if not latest.empty else pd.Series(dtype=int)
    )
    provider_status_counts: dict[str, int] = {}
    if not latest.empty:
        grp = latest.groupby(["source_provider", "status"], dropna=False).size().reset_index(name="count")
        provider_status_counts = {
            f"{str(r['source_provider'])}:{str(r['status'])}": int(r["count"])
            for _, r in grp.iterrows()
        }
    missing_tickers = sorted(
        latest.loc[latest_status == "missing", "ticker"].astype(str).tolist()
    ) if not latest.empty else []
    partial_tickers = sorted(
        latest.loc[latest_status == "partial", "ticker"].astype(str).tolist()
    ) if not latest.empty else []

    health_payload = {
        "build_time": datetime.now(UTC).isoformat(),
        "latest_date": latest_date.isoformat() if latest_date is not None else None,
        "latest_total": int(len(latest)),
        "latest_ok": int((latest_status == "ok").sum()) if not latest.empty else 0,
        "latest_partial": int((latest_status == "partial").sum()) if not latest.empty else 0,
        "latest_missing": int((latest_status == "missing").sum()) if not latest.empty else 0,
        "latest_stale_ok": int(latest_stale_series.sum()) if not latest.empty else 0,
        "latest_provider_counts": (
            latest_provider.value_counts(dropna=False).astype(int).to_dict() if not latest.empty else {}
        ),
        "latest_provider_status_counts": provider_status_counts,
        "missing_tickers": missing_tickers,
        "partial_tickers": partial_tickers,
        "overall_rows": int(len(df)),
        "overall_status_counts": df["status"].astype(str).value_counts(dropna=False).astype(int).to_dict(),
    }
    with open(HEALTH_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(health_payload, f, separators=(",", ":"), allow_nan=False)


# ---------------------------------------------------------------------------
# Core ingest loop
# ---------------------------------------------------------------------------

def _anchor(res: ProviderResult, end_date: date) -> ProviderResult:
    """Stamp the result at end_date; mark stale if sourced from a prior day."""
    if res.date == end_date:
        return res
    try:
        age = int(np.busday_count(str(res.date), str(end_date)))
    except Exception:
        age = None
    res.stale = True
    res.stale_age_bdays = age
    res.date = end_date
    return res


def ingest(
    tickers: list[str],
    lookback_days: int = 10,
    polygon_lookback_days: int = 5,
    start_date: date | None = None,
    end_date: date | None = None,
    providers: list | None = None,
) -> pd.DataFrame:
    if end_date is None:
        end_date = date.today()
    if start_date is None:
        start_date = end_date

    if providers is None:
        providers = build_default_stack()

    from etf_providers import (
        TradrAxsProvider, ProSharesProvider, DirexionProvider,
        RoundhillProvider, YieldMaxProvider, REXSharesProvider,
        GraniteSharesProvider,
        DefianceProvider,
        YFinanceProvider, PolygonProvider,
    )

    rows: list[ProviderResult] = []

    if start_date == end_date:
        polygon_probe_days = max(1, min(int(polygon_lookback_days), int(lookback_days)))
        LOGGER.info(
            "Single-day ingest: end=%s tradr_lookback=%d polygon_lookback=%d providers=%s",
            end_date, int(lookback_days), int(polygon_probe_days),
            [type(p).__name__ for p in providers],
        )

        # Providers that simply try end_date directly (bulk feeds or per-ticker HTML scrapes).
        single_shot_types = (
            ProSharesProvider, DirexionProvider,
            RoundhillProvider, YieldMaxProvider, REXSharesProvider,
            GraniteSharesProvider,
            DefianceProvider,
            YFinanceProvider,
        )

        for t in tickers:
            attempts: list[ProviderResult] = []
            best: ProviderResult | None = None

            for provider in providers:
                try:
                    if isinstance(provider, TradrAxsProvider):
                        probe_dates = [end_date - timedelta(days=i) for i in range(max(1, lookback_days))]
                        picked = None
                        for d in probe_dates:
                            if not provider.supports_ticker(t, d):
                                continue
                            r = provider.fetch_for_date(t, d)
                            if r.status in ("ok", "partial"):
                                picked = r
                                break
                        if picked:
                            picked = _anchor(picked, end_date)
                            attempts.append(picked)
                            if picked.status == "ok":
                                best = picked
                                break

                    elif isinstance(provider, PolygonProvider):
                        if provider.supports_ticker(t, end_date):
                            picked = None
                            for i in range(polygon_probe_days):
                                d = end_date - timedelta(days=i)
                                r = provider.fetch_for_date(t, d)
                                if r.status in ("ok", "partial"):
                                    picked = r
                                    break
                            if picked is None:
                                picked = provider.fetch_for_date(t, end_date)
                            picked = _anchor(picked, end_date)
                            attempts.append(picked)
                            if picked.status == "ok":
                                best = picked
                                break

                    elif isinstance(provider, single_shot_types):
                        if provider.supports_ticker(t, end_date):
                            r = provider.fetch_for_date(t, end_date)
                            r = _anchor(r, end_date)
                            attempts.append(r)
                            if r.status == "ok":
                                best = r
                                break

                    else:
                        if provider.supports_ticker(t, end_date):
                            r = provider.fetch_for_date(t, end_date)
                            r = _anchor(r, end_date)
                            attempts.append(r)
                            if r.status == "ok":
                                best = r
                                break
                except Exception as e:
                    LOGGER.warning("provider=%s ticker=%s error=%s", type(provider).__name__, t, e)
                    continue

            rows.append(best or merge_provider_attempts(attempts, t, end_date))

    else:
        # Multi-day range: replay per-date using the same stack. Used rarely; keep simple.
        for d in _iter_dates(start_date, end_date):
            for t in tickers:
                attempts: list[ProviderResult] = []
                best: ProviderResult | None = None
                for provider in providers:
                    try:
                        if provider.supports_ticker(t, d):
                            r = provider.fetch_for_date(t, d)
                            attempts.append(r)
                            if r.status == "ok":
                                best = r
                                break
                    except Exception:
                        continue
                rows.append(best or merge_provider_attempts(attempts, t, d))

    out = _records_to_df(rows, ingested_at=datetime.now(UTC))
    out = enforce_status_consistency(out)
    validate_df(out)
    return out


def get_summary(df: pd.DataFrame) -> dict:
    return {
        "rows": int(len(df)),
        "ok": int((df["status"] == "ok").sum()),
        "partial": int((df["status"] == "partial").sum()),
        "missing": int((df["status"] == "missing").sum()),
        "latest_date": str(pd.to_datetime(df["date"]).max().date()) if not df.empty else None,
    }


def parse_date_arg(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def previous_business_day(ref: date) -> date:
    """Return the most recent US business day strictly earlier than ``ref``.

    Used when the workflow runs at T+1 06:00 ET and we want rows stamped at the actual
    trading day (T), not the run day. Business-day calendar matches np.busday_count's
    default (Mon-Fri, no US holiday adjustments -- good enough for market-close stamping).
    """
    d = ref - timedelta(days=1)
    while d.weekday() >= 5:  # 5=Sat, 6=Sun
        d -= timedelta(days=1)
    return d


def should_skip_scheduled_redundant_ingest(
    existing: pd.DataFrame,
    tickers: list[str],
    end_date: date,
) -> bool:
    """If we just fully ingested ``end_date`` for the whole universe, skip a repeat scheduled run.

    Issuer sites and Yahoo rarely change between duplicate pipeline triggers; re-pulling only
    refreshes ``ingested_at_utc`` and can add redundant consecutive days after merge.  Manual
    ``workflow_dispatch`` runs are never skipped (``GITHUB_EVENT_NAME`` != ``schedule``).

    Set ``ETF_METRICS_SKIP_IF_RECENT_HOURS=0`` (default) to disable.
    """
    hours = int(os.getenv("ETF_METRICS_SKIP_IF_RECENT_HOURS", "0"))
    if hours <= 0:
        return False
    if os.getenv("GITHUB_EVENT_NAME", "") != "schedule":
        return False
    if existing.empty:
        return False
    need = {t.upper() for t in tickers}
    sub = existing.copy()
    sub["date"] = pd.to_datetime(sub["date"], errors="coerce").dt.date
    sub = sub[sub["date"] == end_date]
    have = set(sub["ticker"].astype(str).str.upper())
    if have != need:
        return False
    ing = pd.to_datetime(sub["ingested_at_utc"], errors="coerce", utc=True)
    if ing.isna().all():
        return False
    latest = ing.max()
    if pd.isna(latest):
        return False
    age_sec = (pd.Timestamp.now(tz=UTC) - latest).total_seconds()
    if age_sec > hours * 3600:
        return False
    LOGGER.info(
        "Skipping ingest: scheduled run within %dh of last full snapshot for %s (latest ingested %s)",
        hours,
        end_date,
        latest,
    )
    return True


def resolve_ingest_end_date(ref: date | None = None) -> date:
    """Pick the correct trading-day stamp for an automated run.

    - If today is Mon-Fri (weekday 0..4): we run at T+1 06:00 ET for the PRIOR business day.
    - If today is Sat (weekday 5): the run captures Fri EOD -> use yesterday.
    - Sun (weekday 6) is not on the cron schedule, but if called manually we still return Fri.
    """
    ref = ref or date.today()
    if ref.weekday() < 5 or ref.weekday() == 5:  # Mon-Sat
        return previous_business_day(ref)
    return previous_business_day(ref)  # Sun -> Fri


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest ETF NAV/AUM/Shares metrics for etf-dashboard.")
    parser.add_argument("--lookback-days", type=int, default=10)
    parser.add_argument("--polygon-lookback-days", type=int, default=5)
    parser.add_argument("--start-date", default=None, help="YYYY-MM-DD")
    parser.add_argument("--end-date", default=None, help="YYYY-MM-DD")
    parser.add_argument("--disable-yfinance", action="store_true", help="skip the Yahoo Finance fallback")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    LOGGER.info(
        "HTTP settings: timeout_sec=%s retry_total=%s retry_backoff=%s",
        os.getenv("ETF_METRICS_HTTP_TIMEOUT_SEC", "15"),
        os.getenv("ETF_METRICS_HTTP_RETRY_TOTAL", "2"),
        os.getenv("ETF_METRICS_HTTP_RETRY_BACKOFF", "0.35"),
    )

    tickers = load_universe_tickers()
    LOGGER.info("Universe tickers: %d", len(tickers))

    # Resolve end_date: CLI arg wins, else use previous business day (the data-date
    # for a T+1 run). This keeps rows stamped on their actual trading day and avoids
    # every row looking "stale" on a routine Tue-Sat run.
    resolved_end_date = parse_date_arg(args.end_date) or resolve_ingest_end_date()
    resolved_start_date = parse_date_arg(args.start_date) or resolved_end_date
    LOGGER.info(
        "Run target dates: start=%s end=%s (today=%s, resolved_as_prev_bday=%s)",
        resolved_start_date, resolved_end_date, date.today(), parse_date_arg(args.end_date) is None,
    )

    existing = load_existing()
    if should_skip_scheduled_redundant_ingest(existing, tickers, resolved_end_date):
        LOGGER.info("Exiting without network ingest (redundant scheduled run).")
        return

    session = _build_session()
    # Build provider stack; let YFinance enable-flag honor CLI + env
    from etf_providers import (
        TradrAxsProvider, ProSharesProvider, DirexionProvider,
        RoundhillProvider, YieldMaxProvider, REXSharesProvider,
        GraniteSharesProvider,
        DefianceProvider,
        YFinanceProvider, PolygonProvider,
    )
    providers = [
        TradrAxsProvider(session),
        ProSharesProvider(session),
        DirexionProvider(session),
        RoundhillProvider(session),
        YieldMaxProvider(session),
        REXSharesProvider(session),
        GraniteSharesProvider(),
        DefianceProvider(session),
        YFinanceProvider(enable=not args.disable_yfinance),
        PolygonProvider(session),
    ]

    incoming = ingest(
        tickers=tickers,
        lookback_days=args.lookback_days,
        polygon_lookback_days=args.polygon_lookback_days,
        start_date=resolved_start_date,
        end_date=resolved_end_date,
        providers=providers,
    )
    LOGGER.info("Incoming summary: %s", get_summary(incoming))

    # Close-price overlay: one yfinance batch call covering the run's date window.
    # This powers the NAV-vs-close premium/discount chart in the UI. If the batch
    # fails we still ship NAV data — close_price stays null for those rows.
    close_df = fetch_close_prices_batch(
        tickers, start=resolved_start_date, end=resolved_end_date,
    )
    if not close_df.empty:
        incoming = merge_close_prices(incoming, close_df)
        got = pd.to_numeric(incoming["close_price"], errors="coerce").notna().sum()
        LOGGER.info("close_price attached to %d/%d rows", int(got), len(incoming))
    else:
        if "close_price" not in incoming.columns:
            incoming["close_price"] = None
        LOGGER.info("close_price fetch returned no rows")

    underlying_map = load_universe_underlying_map()
    und_syms = sorted({str(v).strip().upper() for v in underlying_map.values() if v and str(v).strip()})
    und_close_df = fetch_underlying_adj_close_batch(und_syms, resolved_start_date, resolved_end_date)
    if not und_close_df.empty:
        incoming = merge_underlying_adj_close(incoming, und_close_df, underlying_map)
        got_u = pd.to_numeric(incoming["underlying_adj_close"], errors="coerce").notna().sum()
        LOGGER.info("underlying_adj_close attached to %d/%d rows", int(got_u), len(incoming))
    else:
        if "underlying_adj_close" not in incoming.columns:
            incoming["underlying_adj_close"] = None
        LOGGER.info("underlying_adj_close fetch returned no rows")

    max_stale_business_days = int(os.getenv("ETF_METRICS_MAX_STALE_BUSINESS_DAYS", "3"))
    incoming = apply_stale_carry_forward(
        existing=existing,
        incoming=incoming,
        as_of_date=resolved_end_date,
        max_stale_business_days=max_stale_business_days,
    )
    merged = upsert(existing, incoming)
    merged, n_collapse = collapse_redundant_consecutive_rows(merged)
    if n_collapse:
        LOGGER.info("Collapsed %d redundant consecutive (nav,aum,shares) rows", n_collapse)
    merged, n_share_repairs = repair_shares_vs_aum_nav(merged)
    if n_share_repairs:
        merged = enforce_status_consistency(merged)
        merged, n_post = collapse_redundant_consecutive_rows(merged)
        if n_post:
            LOGGER.info(
                "Post-repair collapse removed %d redundant consecutive (nav,aum,shares) rows",
                n_post,
            )
        LOGGER.info("Repaired shares on %d row(s); status re-evaluated", n_share_repairs)
    validate_df(merged)
    save_outputs(merged)
    LOGGER.info("Saved merged summary: %s", get_summary(merged))

    # Holdings phase — runs after NAV/close are persisted so a failure here
    # never takes down the primary metrics output.
    if os.getenv("ETF_METRICS_SKIP_HOLDINGS", "").lower() not in ("1", "true", "yes"):
        try:
            holdings_stack = build_default_holdings_stack(
                session, underlying_by_ticker=underlying_map,
            )
            holdings_df = fetch_all_holdings(
                tickers, as_of=resolved_end_date, stack=holdings_stack,
            )
            cov = holdings_df.attrs.get("coverage", {})
            by_source: dict[str, int] = {}
            for src in cov.values():
                by_source[src] = by_source.get(src, 0) + 1
            LOGGER.info(
                "holdings coverage: %d/%d tickers with holdings, by source=%s",
                sum(1 for v in cov.values() if v != "missing"), len(cov), by_source,
            )
            # Report per-security_type rollup so the workflow log makes it
            # immediately obvious whether the run actually captured the
            # swap/option/cash breakdowns we care about (rather than a flat
            # list of "COMMON_STOCK" rows across the board).
            if not holdings_df.empty and "security_type" in holdings_df.columns:
                type_counts = (
                    holdings_df["security_type"].fillna("UNKNOWN")
                    .value_counts().astype(int).to_dict()
                )
                LOGGER.info("holdings by security_type: %s", type_counts)
            save_holdings_outputs(holdings_df)
        except Exception as e:
            LOGGER.warning("holdings phase failed (continuing): %s", e)
    else:
        LOGGER.info("holdings phase skipped (ETF_METRICS_SKIP_HOLDINGS)")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
build_data.py — Static data builder for GitHub Pages deployment.

Fetches etf_screened_today.csv from GoldmanDrew/ls-algo,
reads real decay + volatility from CSV (computed by etf_analytics.py),
fetches live IBKR borrow rates, assigns buckets, builds summary stats,
then writes data/dashboard_data.json for the frontend to consume.

Run locally:   python scripts/build_data.py
Run in CI:     github actions calls this on a schedule
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlencode, urlparse, parse_qs

import numpy as np
import pandas as pd
import requests

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
UNIVERSE_REPO = os.environ.get("UNIVERSE_REPO", "GoldmanDrew/ls-algo")
UNIVERSE_BRANCH = os.environ.get("UNIVERSE_BRANCH", "main")
UNIVERSE_PATH = os.environ.get("UNIVERSE_PATH", "data/etf_screened_today.csv")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
POLYGON_API_KEY = (
    os.environ.get("POLYGON_API_KEY", "")
    or os.environ.get("POLYGON_IO_API_KEY", "")
).strip()

HIGH_BETA_THRESHOLD = float(os.environ.get("HIGH_BETA_THRESHOLD", "1.5"))
REALIZED_VOL_TIMEOUT_SEC = int(os.environ.get("REALIZED_VOL_TIMEOUT_SEC", "20"))
REALIZED_VOL_RETRIES = int(os.environ.get("REALIZED_VOL_RETRIES", "2"))
REALIZED_VOL_RANGE = os.environ.get("REALIZED_VOL_RANGE", "2y")
REALIZED_VOL_EWMA_LAMBDA = float(os.environ.get("REALIZED_VOL_EWMA_LAMBDA", "0.94"))
BORROW_HISTORY_MAX_COMMITS = int(os.environ.get("BORROW_HISTORY_MAX_COMMITS", "400"))
BORROW_HISTORY_COMMIT_PAGE_SIZE = int(os.environ.get("BORROW_HISTORY_COMMIT_PAGE_SIZE", "100"))

OUTPUT_DIR = Path(__file__).parent.parent / "data"
OUTPUT_FILE = OUTPUT_DIR / "dashboard_data.json"
BORROW_HISTORY_FILE = OUTPUT_DIR / "borrow_history.json"
OPTIONS_CACHE_FILE = OUTPUT_DIR / "options_cache.json"
LS_ALGO_DATA_PATH = Path(
    os.environ.get(
        "LS_ALGO_DATA_PATH",
        str((Path(__file__).resolve().parents[2] / "ls-algo" / "data")),
    )
)

# Curated inverse ETF list (Bucket 3 source of truth)
INVERSE_ETFS = {
    "SQQQ", "SDS", "SPXS", "SPXU", "QID", "SDOW", "DXD", "TWM", "TZA",
    "SOXS", "FAZ", "SKF", "LABD", "TECS", "WEBS", "FNGD", "REW", "TTXD",
    "TSXD", "DUST", "ZSL", "SCO", "DUG", "DRIP", "TSLQ", "MSTZ", "NVDQ",
    "NVDS", "TMV", "TBT", "BTCZ", "ETHD",
}

VOL_WINDOWS = ("1M", "3M", "6M", "YTD", "12M", "ALL")
VOL_WINDOW_LOOKBACK_DAYS = {"1M": 21, "3M": 63, "6M": 126, "12M": 252}


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def norm_sym(s: str) -> str:
    return str(s).strip().upper().replace(".", "-")


def _safe_float(row, key):
    """Read a float from a row, returning None if missing."""
    v = row.get(key)
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    try:
        return round(float(v), 6)
    except (ValueError, TypeError):
        return None


def fetch_csv_from_github() -> pd.DataFrame:
    """Download etf_screened_today.csv from the ls-algo repo."""
    url = f"https://raw.githubusercontent.com/{UNIVERSE_REPO}/{UNIVERSE_BRANCH}/{UNIVERSE_PATH}"
    headers = {"User-Agent": "etf-dashboard-builder/1.0"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"

    print(f"Fetching {url} ...")
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()

    # Write to local file for reference
    csv_path = OUTPUT_DIR / "etf_screened_today.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.write_text(resp.text)
    print(f"  -> {len(resp.text):,} bytes, saved to {csv_path}")

    from io import StringIO
    df = pd.read_csv(StringIO(resp.text))
    return df


def fetch_last_commit_info() -> dict | None:
    """Get the last commit that touched the CSV in ls-algo."""
    url = f"https://api.github.com/repos/{UNIVERSE_REPO}/commits"
    params = {"path": UNIVERSE_PATH, "per_page": 1}
    headers = {"User-Agent": "etf-dashboard-builder/1.0"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=15)
        resp.raise_for_status()
        commits = resp.json()
        if commits:
            c = commits[0]
            return {
                "sha": c["sha"][:12],
                "date": c["commit"]["committer"]["date"],
                "message": c["commit"]["message"].split("\n")[0][:80],
            }
    except Exception as e:
        print(f"  Warning: could not fetch commit info: {e}")
    return None


def _github_headers() -> dict:
    headers = {"User-Agent": "etf-dashboard-builder/1.0"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    return headers


def fetch_universe_commits(max_commits: int = BORROW_HISTORY_MAX_COMMITS) -> list[dict]:
    """Fetch commit metadata for all commits touching etf_screened_today.csv."""
    url = f"https://api.github.com/repos/{UNIVERSE_REPO}/commits"
    commits: list[dict] = []
    page = 1
    per_page = min(max(1, BORROW_HISTORY_COMMIT_PAGE_SIZE), 100)

    while len(commits) < max_commits:
        params = {"path": UNIVERSE_PATH, "per_page": per_page, "page": page}
        resp = requests.get(url, headers=_github_headers(), params=params, timeout=25)
        if not resp.ok:
            raise RuntimeError(f"GitHub commits API failed: HTTP {resp.status_code} on page={page}")
        rows = resp.json() or []
        if not rows:
            break
        for c in rows:
            commits.append(
                {
                    "sha": c.get("sha", ""),
                    "date": (c.get("commit", {}).get("committer", {}) or {}).get("date"),
                }
            )
            if len(commits) >= max_commits:
                break
        page += 1

    return [c for c in commits if c.get("sha") and c.get("date")]


def _fetch_csv_at_sha(sha: str) -> pd.DataFrame | None:
    raw_url = f"https://raw.githubusercontent.com/{UNIVERSE_REPO}/{sha}/{UNIVERSE_PATH}"
    resp = requests.get(raw_url, headers=_github_headers(), timeout=25)
    if not resp.ok:
        return None
    try:
        return pd.read_csv(io.StringIO(resp.text))
    except Exception:
        return None


def _pick_borrow_fee_only(row) -> float | None:
    # Explicitly prefer fee-only borrow (not net of rebate).
    for key in ("borrow_current", "borrow_fee_annual", "borrow_net_annual"):
        v = row.get(key)
        if v is None or (isinstance(v, float) and np.isnan(v)):
            continue
        try:
            return round(float(v), 6)
        except (TypeError, ValueError):
            continue
    return None


def build_borrow_history_from_commits(universe_symbols: set[str]) -> dict:
    """
    Build cleaned borrow/shares time series for each ETF symbol using all available
    historical etf_screened_today.csv snapshots from GitHub commit history.
    """
    print("Building historical borrow/shares database from screener history ...")
    symbols = {norm_sym(s) for s in universe_symbols if str(s).strip()}
    by_symbol_day: dict[str, dict[str, dict]] = {s: {} for s in symbols}

    try:
        commits = fetch_universe_commits()
    except Exception as e:
        print(f"  Warning: could not fetch commit history for borrow DB: {e}")
        return {"symbols": {}, "meta": {"error": str(e)}}

    print(f"  Commit snapshots discovered: {len(commits)}")
    processed = 0
    for idx, c in enumerate(commits, start=1):
        sha = c["sha"]
        commit_date = c["date"]
        day = str(commit_date)[:10]
        snap = _fetch_csv_at_sha(sha)
        if snap is None or snap.empty or "ETF" not in snap.columns:
            continue

        snap["symbol"] = snap["ETF"].apply(norm_sym)
        filtered = snap[snap["symbol"].isin(symbols)]
        if filtered.empty:
            continue

        for _, row in filtered.iterrows():
            sym = row["symbol"]
            borrow = _pick_borrow_fee_only(row)
            shares = None
            if pd.notna(row.get("shares_available")):
                try:
                    shares = int(row.get("shares_available"))
                except (TypeError, ValueError):
                    shares = None
            if borrow is None and shares is None:
                continue

            cur = by_symbol_day[sym].get(day)
            # Keep the latest snapshot in a day.
            if cur is None or str(commit_date) > str(cur.get("_commit_ts", "")):
                by_symbol_day[sym][day] = {
                    "date": day,
                    "borrow_current": borrow,
                    "shares_available": shares,
                    "_commit_ts": commit_date,
                    "_sha": sha[:12],
                }

        processed += 1
        if idx % 25 == 0 or idx == len(commits):
            print(f"  Borrow history progress: {idx}/{len(commits)} commits (processed={processed})")

    # Augment with local ls-algo run snapshots if available.
    local_added = 0
    local_files_scanned = 0
    if LS_ALGO_DATA_PATH.exists():
        run_files = list((LS_ALGO_DATA_PATH / "runs").glob("*/etf_screened_today.csv"))
        current_file = LS_ALGO_DATA_PATH / "etf_screened_today.csv"
        if current_file.exists():
            run_files.append(current_file)

        for path in run_files:
            local_files_scanned += 1
            snap = None
            try:
                snap = pd.read_csv(path)
            except Exception:
                snap = None
            if snap is None or snap.empty or "ETF" not in snap.columns:
                continue
            day = path.parent.name if path.parent != LS_ALGO_DATA_PATH else dt.datetime.fromtimestamp(path.stat().st_mtime, dt.UTC).date().isoformat()
            # Use file mtime as intra-day tie-breaker token.
            ts_token = dt.datetime.fromtimestamp(path.stat().st_mtime, dt.UTC).isoformat().replace("+00:00", "Z")
            snap["symbol"] = snap["ETF"].apply(norm_sym)
            filtered = snap[snap["symbol"].isin(symbols)]
            for _, row in filtered.iterrows():
                sym = row["symbol"]
                borrow = _pick_borrow_fee_only(row)
                shares = None
                if pd.notna(row.get("shares_available")):
                    try:
                        shares = int(row.get("shares_available"))
                    except (TypeError, ValueError):
                        shares = None
                if borrow is None and shares is None:
                    continue
                cur = by_symbol_day[sym].get(day)
                if cur is None:
                    by_symbol_day[sym][day] = {
                        "date": day,
                        "borrow_current": borrow,
                        "shares_available": shares,
                        "_commit_ts": ts_token,
                        "_sha": "local",
                    }
                    local_added += 1
    else:
        print(f"  Note: local ls-algo data path not found: {LS_ALGO_DATA_PATH}")

    cleaned_symbols: dict[str, list[dict]] = {}
    for sym, by_day in by_symbol_day.items():
        rows = sorted(by_day.values(), key=lambda x: x["date"])
        cleaned_symbols[sym] = [
            {
                "date": r["date"],
                "borrow_current": r.get("borrow_current"),
                "shares_available": r.get("shares_available"),
            }
            for r in rows
        ]

    meta = {
        "source_repo": UNIVERSE_REPO,
        "source_path": UNIVERSE_PATH,
        "snapshot_commits_used": processed,
        "local_files_scanned": local_files_scanned,
        "local_points_added": local_added,
        "symbols_with_history": sum(1 for v in cleaned_symbols.values() if v),
        "build_time": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
    }
    return {"symbols": cleaned_symbols, "meta": meta}


def try_fetch_ibkr_ftp() -> dict:
    """
    Try to fetch live IBKR borrow data from FTP.
    Returns {borrow_map, fee_map, rebate_map, available_map, success}.
    Falls back to CSV values if FTP fails.
    """
    import ftplib
    import io

    result = {"borrow_map": {}, "fee_map": {}, "rebate_map": {}, "available_map": {}, "success": False}

    max_retries = 3
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            print(f"Fetching IBKR short stock file from FTP ... (attempt {attempt}/{max_retries})")
            ftp = ftplib.FTP("ftp2.interactivebrokers.com", timeout=30)
            ftp.login(user="shortstock", passwd="")

            buf = io.BytesIO()
            ftp.retrbinary("RETR usa.txt", buf.write)
            ftp.quit()

            buf.seek(0)
            text = buf.getvalue().decode("utf-8", errors="ignore")
            lines = [ln for ln in text.splitlines() if ln.strip()]

            header_idx = None
            for i, ln in enumerate(lines):
                if ln.startswith("#SYM|"):
                    header_idx = i
                    break
            if header_idx is None:
                raise ValueError("No #SYM| header found")

            header_cols = [c.strip().lstrip("#").lower() for c in lines[header_idx].split("|")]
            data_lines = lines[header_idx + 1:]
            data_str = "\n".join(data_lines)

            df = pd.read_csv(io.StringIO(data_str), sep="|", header=None, engine="python")
            n_cols = min(len(header_cols), df.shape[1])
            df = df.iloc[:, :n_cols]
            df.columns = header_cols[:n_cols]

            df["sym"] = df["sym"].astype(str).str.upper().str.strip()
            df["rebate_annual"] = pd.to_numeric(df.get("rebaterate", pd.Series(dtype=float)), errors="coerce") / 100.0
            df["fee_annual"] = pd.to_numeric(df.get("feerate", pd.Series(dtype=float)), errors="coerce") / 100.0
            df["available_int"] = pd.to_numeric(df.get("available", pd.Series(dtype=float)), errors="coerce")
            # Dashboard borrow should reflect fee only (not fee - rebate).
            df["borrow_current"] = df["fee_annual"]

            for _, row in df.iterrows():
                sym = norm_sym(row["sym"])
                if pd.notna(row["borrow_current"]):
                    result["borrow_map"][sym] = round(float(row["borrow_current"]), 6)
                if pd.notna(row["fee_annual"]):
                    result["fee_map"][sym] = round(float(row["fee_annual"]), 6)
                if pd.notna(row["rebate_annual"]):
                    result["rebate_map"][sym] = round(float(row["rebate_annual"]), 6)
                if pd.notna(row["available_int"]):
                    result["available_map"][sym] = int(row["available_int"])

            result["success"] = True
            print(f"  -> IBKR FTP: {len(result['borrow_map'])} symbols fetched")
            break
        except Exception as e:
            last_err = e
            if attempt < max_retries:
                wait_s = 2 ** (attempt - 1)
                print(f"  -> FTP attempt failed: {e}; retrying in {wait_s}s")
                time.sleep(wait_s)
            else:
                print(f"  -> IBKR FTP failed after {max_retries} attempts: {e}. Using CSV borrow values.")
                result["success"] = False

    return result


def _maps_from_universe_csv(df: pd.DataFrame) -> dict:
    out = {"borrow_map": {}, "fee_map": {}, "rebate_map": {}, "available_map": {}}
    if df is None or df.empty or "ETF" not in df.columns:
        return out

    snap = df.copy()
    snap["symbol"] = snap["ETF"].apply(norm_sym)
    for _, row in snap.iterrows():
        sym = row["symbol"]
        borrow_current = _safe_float(row, "borrow_current")
        if borrow_current is None:
            borrow_current = _safe_float(row, "borrow_fee_annual")
        fee = _safe_float(row, "borrow_fee_annual")
        rebate = _safe_float(row, "borrow_rebate_annual")
        shares = None
        if pd.notna(row.get("shares_available")):
            try:
                shares = int(row.get("shares_available"))
            except (TypeError, ValueError):
                shares = None

        if borrow_current is not None:
            out["borrow_map"][sym] = borrow_current
        if fee is not None:
            out["fee_map"][sym] = fee
        if rebate is not None:
            out["rebate_map"][sym] = rebate
        if shares is not None:
            out["available_map"][sym] = shares
    return out


def build_polygon_options_cache(symbols: list[str]) -> dict:
    """
    Build delayed options snapshot cache from Polygon for UI consumption.
    Returns a JSON-serializable dict with per-symbol spot + option rows.
    """
    prior_cache = {}
    if OPTIONS_CACHE_FILE.exists():
        try:
            prior_cache = json.loads(OPTIONS_CACHE_FILE.read_text(encoding="utf-8")) or {}
        except Exception:
            prior_cache = {}

    out = {
        "build_time": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
        "source": "polygon_snapshot",
        "polygon_api_configured": bool(POLYGON_API_KEY),
        "symbols": {},
    }
    if not POLYGON_API_KEY:
        prior_symbols = prior_cache.get("symbols") if isinstance(prior_cache, dict) else {}
        if isinstance(prior_symbols, dict) and prior_symbols:
            out["symbols"] = prior_symbols
            out["symbols_count"] = len(prior_symbols)
            out["warning"] = "POLYGON_API_KEY missing; using previous cached options data."
            return out
        out["error"] = "POLYGON_API_KEY missing"
        return out

    session = requests.Session()
    headers = {"User-Agent": "etf-dashboard-builder/1.0"}
    unique_symbols = sorted({norm_sym(s) for s in symbols if str(s).strip()})

    for sym in unique_symbols:
        try:
            rows = []
            under_px = None
            next_url = f"https://api.polygon.io/v3/snapshot/options/{sym}?{urlencode({'limit': 250, 'apiKey': POLYGON_API_KEY})}"
            pages = 0
            while next_url and pages < 6:
                pages += 1
                resp = session.get(next_url, headers=headers, timeout=25)
                if not resp.ok:
                    break
                payload = resp.json() or {}
                batch = payload.get("results") or []
                if isinstance(batch, list):
                    rows.extend(batch)
                if under_px is None:
                    under_px = (payload.get("underlying_asset") or {}).get("price")
                nurl = payload.get("next_url")
                if nurl:
                    parsed = urlparse(nurl)
                    qs = parse_qs(parsed.query)
                    if "apiKey" not in qs:
                        sep = "&" if parsed.query else ""
                        nurl = f"{nurl}{sep}apiKey={POLYGON_API_KEY}"
                next_url = nurl
            if not rows:
                prior_sym = prior_cache.get("symbols", {}).get(sym) if isinstance(prior_cache, dict) else None
                if prior_sym:
                    out["symbols"][sym] = prior_sym
                continue

            parsed = []
            for r in rows:
                details = r.get("details") or {}
                quote = r.get("last_quote") or {}
                greeks = r.get("greeks") or {}
                iv = r.get("implied_volatility")
                bid = quote.get("bid")
                ask = quote.get("ask")
                mid = None
                if bid is not None and ask is not None:
                    try:
                        mid = 0.5 * (float(bid) + float(ask))
                    except Exception:
                        mid = None
                if mid is None:
                    last_trade = r.get("last_trade") or {}
                    p = last_trade.get("price")
                    mid = float(p) if p is not None else None

                if under_px is None:
                    under_px = r.get("underlying_asset", {}).get("price")
                exp = details.get("expiration_date") or r.get("expiration_date")
                strike = details.get("strike_price") if details.get("strike_price") is not None else r.get("strike_price")
                ctype = details.get("contract_type") or r.get("contract_type")
                parsed.append(
                    {
                        "ticker": details.get("ticker"),
                        "expiration_date": exp,
                        "strike_price": strike,
                        "contract_type": ctype,
                        "mid": mid,
                        "iv": iv,
                        "delta": greeks.get("delta"),
                    }
                )

            parsed = [x for x in parsed if x.get("expiration_date") and x.get("strike_price") is not None]
            if not parsed:
                prior_sym = prior_cache.get("symbols", {}).get(sym) if isinstance(prior_cache, dict) else None
                if prior_sym:
                    out["symbols"][sym] = prior_sym
                continue
            parsed.sort(key=lambda x: (x["expiration_date"], abs(float(x["strike_price"])) if x["strike_price"] is not None else 9e9))
            out["symbols"][sym] = {
                "spot": float(under_px) if under_px is not None else None,
                "options": parsed[:300],
            }
        except Exception:
            prior_sym = prior_cache.get("symbols", {}).get(sym) if isinstance(prior_cache, dict) else None
            if prior_sym:
                out["symbols"][sym] = prior_sym
            continue

    out["symbols_count"] = len(out["symbols"])
    if out["symbols_count"] == 0 and isinstance(prior_cache, dict):
        prior_symbols = prior_cache.get("symbols")
        if isinstance(prior_symbols, dict) and prior_symbols:
            out["symbols"] = prior_symbols
            out["symbols_count"] = len(prior_symbols)
            out["warning"] = "Polygon fetch returned no symbols; using previous cached options data."
    return out


# ──────────────────────────────────────────────
# Realized volatility (server-side canonical)
# ──────────────────────────────────────────────
def _compute_vol_stats_from_closes(closes: list[float]) -> dict:
    """Compute realized and EWMA annualized vol from closes."""
    if len(closes) < 3:
        return {"vol_annual": None, "ewma_vol_annual": None, "n_returns": 0}
    arr = np.asarray(closes, dtype=float)
    if not np.all(np.isfinite(arr)) or np.any(arr <= 0):
        return {"vol_annual": None, "ewma_vol_annual": None, "n_returns": 0}
    rets = np.diff(np.log(arr))
    n_returns = int(rets.size)
    if n_returns < 2:
        return {"vol_annual": None, "ewma_vol_annual": None, "n_returns": n_returns}

    vol = float(np.std(rets, ddof=1) * np.sqrt(252))

    lam = min(max(float(REALIZED_VOL_EWMA_LAMBDA), 0.0), 0.999999)
    ewma_var = float(rets[0] * rets[0])
    for r in rets[1:]:
        ewma_var = lam * ewma_var + (1.0 - lam) * float(r * r)
    ewma_vol = float(np.sqrt(max(ewma_var, 0.0)) * np.sqrt(252))

    return {
        "vol_annual": round(vol, 6),
        "ewma_vol_annual": round(ewma_vol, 6),
        "n_returns": n_returns,
    }


def _build_realized_vol_windows(points: list[tuple[dt.date, float]]) -> dict:
    """
    Build per-window realized vol stats from (date, adj_close) points.
    Returns window -> {vol_annual, n_returns, asof}.
    """
    if len(points) < 3:
        return {}

    points = sorted(points, key=lambda x: x[0])
    asof = points[-1][0].isoformat()
    results = {}

    for window in VOL_WINDOWS:
        if window == "ALL":
            scoped = points
        elif window == "YTD":
            start = dt.date(points[-1][0].year, 1, 1)
            scoped = [p for p in points if p[0] >= start]
        else:
            lookback = VOL_WINDOW_LOOKBACK_DAYS.get(window)
            scoped = points[-(lookback + 1):] if lookback else points

        closes = [c for _, c in scoped]
        stats = _compute_vol_stats_from_closes(closes)
        results[window] = {
            "vol_annual": stats["vol_annual"],
            "ewma_vol_annual": stats["ewma_vol_annual"],
            "n_returns": stats["n_returns"] if stats["n_returns"] > 0 else None,
            "asof": asof,
        }

    return results


def _fetch_yahoo_adjclose_points(
    session: requests.Session,
    symbol: str,
    *,
    range_value: str,
) -> list[tuple[dt.date, float]]:
    """Fetch adjusted-close history from Yahoo chart API for a fixed range."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {"range": range_value, "interval": "1d", "events": "div,splits"}

    last_err = None
    for attempt in range(REALIZED_VOL_RETRIES + 1):
        try:
            resp = session.get(url, params=params, timeout=REALIZED_VOL_TIMEOUT_SEC)
            resp.raise_for_status()
            payload = resp.json()
            result = payload.get("chart", {}).get("result", [None])[0]
            if not result:
                return []

            timestamps = result.get("timestamp") or []
            closes = (
                (result.get("indicators", {}).get("adjclose", [{}])[0].get("adjclose"))
                or (result.get("indicators", {}).get("quote", [{}])[0].get("close"))
                or []
            )
            n = min(len(timestamps), len(closes))
            points = []
            for i in range(n):
                ts = timestamps[i]
                close = closes[i]
                if ts is None or close is None:
                    continue
                try:
                    px = float(close)
                    if not np.isfinite(px) or px <= 0:
                        continue
                    d = dt.datetime.fromtimestamp(int(ts), dt.UTC).date()
                    points.append((d, px))
                except (ValueError, TypeError, OSError):
                    continue
            return points
        except Exception as e:
            last_err = e
            if attempt < REALIZED_VOL_RETRIES:
                continue
    print(f"  Warning: Yahoo history fetch failed for {symbol}: {last_err}")
    return []


def compute_realized_vol_map(symbols: set[str]) -> dict[str, dict]:
    """
    Returns symbol -> realized-vol windows.
    Each window has {vol_annual, n_returns, asof}.
    """
    out: dict[str, dict] = {}
    clean_symbols = sorted({norm_sym(s) for s in symbols if str(s).strip()})
    if not clean_symbols:
        return out

    print(
        f"Fetching Yahoo daily history for realized vol ({len(clean_symbols)} symbols, "
        f"range={REALIZED_VOL_RANGE}) ..."
    )
    session = requests.Session()
    session.headers.update({"User-Agent": "etf-dashboard-builder/1.0"})

    ok = 0
    for i, sym in enumerate(clean_symbols, start=1):
        points = _fetch_yahoo_adjclose_points(session, sym, range_value=REALIZED_VOL_RANGE)
        windows = _build_realized_vol_windows(points) if points else {}
        if windows:
            out[sym] = windows
            ok += 1
        if i % 50 == 0 or i == len(clean_symbols):
            print(f"  Realized vol progress: {i}/{len(clean_symbols)} (ok={ok})")

    return out


# ──────────────────────────────────────────────
# Bucketing
# ──────────────────────────────────────────────
def assign_bucket(sym: str, beta: float) -> str:
    # Primary check: negative beta → inverse (matches ibkr_accounting.py)
    if pd.notna(beta) and beta < 0:
        return "bucket_3_inverse"
    # Fallback: hardcoded list catches inverse ETFs with missing/zero beta
    if sym in INVERSE_ETFS:
        return "bucket_3_inverse"
    if pd.notna(beta) and beta > HIGH_BETA_THRESHOLD:
        return "bucket_1_high_beta"
    return "bucket_2_low_beta"


def _calc_summary(records: list[dict]) -> dict:
    b1 = [r for r in records if r["bucket"] == "bucket_1_high_beta"]
    b2 = [r for r in records if r["bucket"] == "bucket_2_low_beta"]
    b3 = [r for r in records if r["bucket"] == "bucket_3_inverse"]

    with_net = sorted(
        [r for r in records if r.get("net_decay") is not None],
        key=lambda r: r["net_decay"], reverse=True,
    )
    best_net_decay = [
        {"symbol": r["symbol"], "net_decay": r["net_decay"],
         "borrow": r.get("borrow_current"), "decay": r.get("gross_decay_annual")}
        for r in with_net[:5]
    ]

    with_borrow = sorted(
        [r for r in records if r.get("borrow_current") is not None],
        key=lambda r: r["borrow_current"], reverse=True,
    )
    worst_borrows = [
        {"symbol": r["symbol"], "borrow": r.get("borrow_current"), "shares": r.get("shares_available")}
        for r in with_borrow[:5]
    ]

    missing = sum(1 for r in records if r.get("borrow_missing"))
    decay_count = sum(1 for r in records if r.get("gross_decay_annual") is not None)

    return {
        "total_symbols": len(records),
        "bucket_1_count": len(b1),
        "bucket_2_count": len(b2),
        "bucket_3_count": len(b3),
        "best_net_decay": best_net_decay,
        "worst_borrows": worst_borrows,
        "pct_missing": round(missing / len(records) * 100, 1) if records else 0,
        "decay_computed_count": decay_count,
    }


def _normalize_borrow_fields(rec: dict) -> None:
    """Keep borrow fields internally consistent (borrow_current = fee-only)."""
    borrow_current = rec.get("borrow_current")
    if borrow_current is None:
        borrow_current = rec.get("borrow_fee_annual")
    if borrow_current is None:
        borrow_current = rec.get("borrow_net_annual")

    rec["borrow_current"] = round(float(borrow_current), 6) if borrow_current is not None else None
    # Keep net field as backward-compatible alias of borrow_current.
    rec["borrow_net_annual"] = rec["borrow_current"]

    gross = rec.get("gross_decay_annual")
    if gross is not None and rec["borrow_current"] is not None:
        rec["net_decay"] = round(float(gross) - float(rec["borrow_current"]), 6)


def refresh_borrow_only() -> None:
    """Update borrow + shares in existing dashboard JSON without re-pulling universe."""
    if not OUTPUT_FILE.exists():
        raise FileNotFoundError(
            f"Cannot run --borrow-only because {OUTPUT_FILE} does not exist. Run full build first."
        )

    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        output = json.load(f)

    records = output.get("records", [])
    if not isinstance(records, list):
        raise ValueError("dashboard_data.json is malformed: missing records list")

    ibkr = try_fetch_ibkr_ftp()
    csv_maps = {"borrow_map": {}, "fee_map": {}, "rebate_map": {}, "available_map": {}}
    try:
        csv_df = fetch_csv_from_github()
        csv_maps = _maps_from_universe_csv(csv_df)
    except Exception as e:
        print(f"  Warning: could not fetch latest CSV fallback during borrow refresh: {e}")

    updated = 0
    updated_csv = 0

    for rec in records:
        sym = norm_sym(rec.get("symbol", ""))
        if not sym:
            continue

        if ibkr["success"] and sym in ibkr["borrow_map"]:
            borrow_current = ibkr["borrow_map"][sym]
            rec["borrow_fee_annual"] = ibkr["fee_map"].get(sym)
            rec["borrow_rebate_annual"] = ibkr["rebate_map"].get(sym)
            rec["shares_available"] = ibkr["available_map"].get(sym)
            rec["borrow_current"] = round(float(borrow_current), 6)
            rec["borrow_source"] = "ibkr_ftp"
            rec["borrow_missing"] = False
            updated += 1
        elif sym in csv_maps["borrow_map"] or sym in csv_maps["available_map"]:
            borrow_current = csv_maps["borrow_map"].get(sym)
            rec["borrow_fee_annual"] = csv_maps["fee_map"].get(sym)
            rec["borrow_rebate_annual"] = csv_maps["rebate_map"].get(sym)
            rec["shares_available"] = csv_maps["available_map"].get(sym)
            rec["borrow_current"] = round(float(borrow_current), 6) if borrow_current is not None else rec.get("borrow_current")
            rec["borrow_source"] = "csv"
            rec["borrow_missing"] = borrow_current is None
            updated_csv += 1
        else:
            # Keep existing record values, but normalize borrowed field aliases.
            rec["borrow_source"] = rec.get("borrow_source", "csv")
            rec["borrow_missing"] = bool(rec.get("borrow_missing", False))

        _normalize_borrow_fields(rec)

    output["summary"] = _calc_summary(records)
    output["build_time"] = dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")
    output["ibkr_ftp_success"] = ibkr["success"]
    output["ibkr_symbols_fetched"] = len(ibkr["borrow_map"]) if ibkr["success"] else 0
    output["refresh_type"] = "borrow_only"
    output["borrow_refresh_interval_minutes"] = 30
    output["freshness"] = {
        "updated_from_ibkr": int(updated),
        "updated_from_csv": int(updated_csv),
        "ibkr_attempt_success": bool(ibkr["success"]),
    }
    output["polygon_api_configured"] = bool(POLYGON_API_KEY)
    output["options_cache_file"] = "data/options_cache.json"

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=None, separators=(",", ":"))

    print(f"[OK] Borrow-only refresh wrote {OUTPUT_FILE}")
    print(f"  Updated from IBKR FTP: {updated}/{len(records)} symbols")
    print(f"  Updated from latest CSV fallback: {updated_csv}/{len(records)} symbols")


# ──────────────────────────────────────────────
# Main build
# ──────────────────────────────────────────────
def build():
    print("=" * 60)
    print("ETF Dashboard — Static Data Builder")
    print("=" * 60)

    # 1. Fetch universe CSV from GitHub
    df = fetch_csv_from_github()
    df["symbol"] = df["ETF"].apply(norm_sym)
    df["underlying_sym"] = df["Underlying"].apply(norm_sym)
    print(f"Universe: {len(df)} ETFs loaded")

    # Check which analytics columns are present
    for col in ["gross_decay_annual", "net_decay_annual",
                 "vol_underlying_annual", "vol_etf_annual"]:
        n = df[col].notna().sum() if col in df.columns else 0
        print(f"  {col}: {n}/{len(df)}" if col in df.columns else f"  {col}: MISSING")

    # 2. Fetch last commit info
    commit_info = fetch_last_commit_info()
    if commit_info:
        print(f"Last screener run: {commit_info['date']} ({commit_info['sha']})")

    # 3. Try IBKR FTP for live borrow data
    ibkr = try_fetch_ibkr_ftp()

    # 4. Build canonical realized-vol map (server-side, not browser-side)
    vol_symbols = set(df["symbol"].dropna().tolist()) | set(df["underlying_sym"].dropna().tolist())
    realized_vol_map = compute_realized_vol_map(vol_symbols)

    # 4b. Build historical borrow/shares database (fee-only borrow, no rebate).
    borrow_history = build_borrow_history_from_commits(set(df["symbol"].dropna().tolist()))
    borrow_history_symbols = borrow_history.get("symbols", {})

    today_utc = dt.datetime.now(dt.UTC).date().isoformat()

    # 5. Build records
    records = []
    decay_count = 0

    for _, row in df.iterrows():
        sym = row["symbol"]
        beta = float(row["Beta"]) if pd.notna(row.get("Beta")) else None

        bucket = assign_bucket(sym, beta or 0)

        # Borrow data: prefer IBKR FTP, fall back to CSV
        if ibkr["success"] and sym in ibkr["borrow_map"]:
            borrow_current = ibkr["borrow_map"][sym]
            borrow_fee = ibkr["fee_map"].get(sym)
            borrow_rebate = ibkr["rebate_map"].get(sym)
            shares_avail = ibkr["available_map"].get(sym)
            borrow_source = "ibkr_ftp"
        else:
            borrow_fee = _safe_float(row, "borrow_fee_annual")
            borrow_rebate = _safe_float(row, "borrow_rebate_annual")
            borrow_current = _safe_float(row, "borrow_current")
            if borrow_current is None:
                borrow_current = borrow_fee
            if borrow_current is None:
                borrow_current = _safe_float(row, "borrow_net_annual")
            shares_avail = int(row["shares_available"]) if pd.notna(row.get("shares_available")) else None
            borrow_source = "csv"

        # Analytics from CSV (computed by etf_analytics.py in ls-algo)
        gross_decay = _safe_float(row, "gross_decay_annual")
        net_decay = _safe_float(row, "net_decay_annual")
        if gross_decay is not None and borrow_current is not None:
            net_decay = round(gross_decay - borrow_current, 6)
        etf_realized = realized_vol_map.get(sym, {})
        und_realized = realized_vol_map.get(row["underlying_sym"], {})
        realized_vol = {}
        for window in VOL_WINDOWS:
            etf_w = etf_realized.get(window, {})
            und_w = und_realized.get(window, {})
            if not etf_w and not und_w:
                continue
            realized_vol[window] = {
                "etf": etf_w.get("vol_annual"),
                "etf_ewma": etf_w.get("ewma_vol_annual"),
                "underlying": und_w.get("vol_annual"),
                "underlying_ewma": und_w.get("ewma_vol_annual"),
                "n_returns_etf": etf_w.get("n_returns"),
                "n_returns_underlying": und_w.get("n_returns"),
                "asof_etf": etf_w.get("asof"),
                "asof_underlying": und_w.get("asof"),
            }

        # Keep legacy top-level fields as 6M default fallback.
        vol_und_csv = _safe_float(row, "vol_underlying_annual")
        vol_etf_csv = _safe_float(row, "vol_etf_annual")
        vol_und = realized_vol.get("6M", {}).get("underlying")
        vol_etf = realized_vol.get("6M", {}).get("etf")
        if vol_und is None:
            vol_und = vol_und_csv
        if vol_etf is None:
            vol_etf = vol_etf_csv

        # Append/overwrite today's most recent borrow snapshot in history.
        hist_rows = borrow_history_symbols.get(sym, [])
        by_day = {str(x.get("date")): x for x in hist_rows if x.get("date")}
        if borrow_current is not None or shares_avail is not None:
            by_day[today_utc] = {
                "date": today_utc,
                "borrow_current": round(float(borrow_current), 6) if borrow_current is not None else None,
                "shares_available": shares_avail,
            }
        hist_rows = sorted(by_day.values(), key=lambda x: x["date"])
        borrow_history_symbols[sym] = hist_rows
        hist_borrows = [float(x["borrow_current"]) for x in hist_rows if x.get("borrow_current") is not None]
        borrow_avg_annual = round(float(np.mean(hist_borrows)), 6) if hist_borrows else None

        if gross_decay is not None:
            decay_count += 1

        rec = {
            "symbol": sym,
            "underlying": row["underlying_sym"],
            "beta": round(beta, 4) if beta else None,
            "beta_n_obs": int(row["Beta_n_obs"]) if pd.notna(row.get("Beta_n_obs")) else None,
            "bucket": bucket,
            "borrow_fee_annual": round(borrow_fee, 6) if borrow_fee is not None else None,
            "borrow_rebate_annual": round(borrow_rebate, 6) if borrow_rebate is not None else None,
            "borrow_current": round(borrow_current, 6) if borrow_current is not None else None,
            "borrow_avg_annual": borrow_avg_annual,
            "borrow_net_annual": round(borrow_current, 6) if borrow_current is not None else None,
            "shares_available": shares_avail,
            "borrow_spiking": bool(row.get("borrow_spiking", False)),
            "borrow_missing": bool(row.get("borrow_missing_from_ftp", False)),
            "gross_decay_annual": gross_decay,
            "net_decay": net_decay,
            "vol_underlying_annual": vol_und,
            "vol_etf_annual": vol_etf,
            "realized_vol": realized_vol,
            "include_for_algo": bool(row.get("include_for_algo", False)),
            "protected": bool(row.get("protected", False)),
            "cagr_positive": bool(row.get("cagr_positive")) if pd.notna(row.get("cagr_positive")) else None,
            "borrow_source": borrow_source,
        }
        records.append(rec)

    # 6. Compute summary
    summary = _calc_summary(records)

    build_time = dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")

    output = {
        "build_time": build_time,
        "source_repo": UNIVERSE_REPO,
        "source_branch": UNIVERSE_BRANCH,
        "last_commit": commit_info,
        "ibkr_ftp_success": ibkr["success"],
        "ibkr_symbols_fetched": len(ibkr["borrow_map"]) if ibkr["success"] else 0,
        "refresh_type": "full",
        "decay_method": "linear_daily_pnl_1_over_beta_hedge",
        "borrow_history_file": "data/borrow_history.json",
        "polygon_api_configured": bool(POLYGON_API_KEY),
        "options_cache_file": "data/options_cache.json",
        "summary": summary,
        "records": records,
    }

    # 7. Write JSON
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=None, separators=(",", ":"))
    with open(BORROW_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(borrow_history, f, indent=None, separators=(",", ":"))
    symbols_for_options = sorted({rec["symbol"] for rec in records} | {rec["underlying"] for rec in records})
    options_cache = build_polygon_options_cache(symbols_for_options)
    with open(OPTIONS_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(options_cache, f, indent=None, separators=(",", ":"))

    file_size = OUTPUT_FILE.stat().st_size
    print(f"\n[OK] Wrote {OUTPUT_FILE} ({file_size:,} bytes)")
    if BORROW_HISTORY_FILE.exists():
        print(f"  [OK] Wrote {BORROW_HISTORY_FILE} ({BORROW_HISTORY_FILE.stat().st_size:,} bytes)")
    if OPTIONS_CACHE_FILE.exists():
        print(f"  [OK] Wrote {OPTIONS_CACHE_FILE} ({OPTIONS_CACHE_FILE.stat().st_size:,} bytes)")
    print(f"  {len(records)} records | B1={summary['bucket_1_count']} B2={summary['bucket_2_count']} B3={summary['bucket_3_count']}")
    print(f"  Decay: {decay_count}/{len(records)} ({100*decay_count/len(records):.0f}%)")
    print(f"  IBKR FTP: {'OK' if ibkr['success'] else 'FAIL'}")
    print(f"  Build time: {build_time}")
    print("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build ETF dashboard static JSON")
    parser.add_argument(
        "--borrow-only",
        action="store_true",
        help="Only refresh borrow rate + shares available in existing dashboard_data.json",
    )
    args = parser.parse_args()

    if args.borrow_only:
        refresh_borrow_only()
    else:
        build()

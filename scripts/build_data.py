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
import collections
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
TRADIER_TOKEN = os.environ.get("TRADIER_TOKEN", "").strip()
TRADIER_BASE_URL = os.environ.get("TRADIER_BASE_URL", "https://api.tradier.com/v1").strip().rstrip("/")

HIGH_BETA_THRESHOLD = float(os.environ.get("HIGH_BETA_THRESHOLD", "1.5"))
REALIZED_VOL_TIMEOUT_SEC = int(os.environ.get("REALIZED_VOL_TIMEOUT_SEC", "20"))
REALIZED_VOL_RETRIES = int(os.environ.get("REALIZED_VOL_RETRIES", "2"))
REALIZED_VOL_RANGE = os.environ.get("REALIZED_VOL_RANGE", "2y")
REALIZED_VOL_EWMA_LAMBDA = float(os.environ.get("REALIZED_VOL_EWMA_LAMBDA", "0.94"))
BORROW_HISTORY_MAX_COMMITS = int(os.environ.get("BORROW_HISTORY_MAX_COMMITS", "400"))
BORROW_HISTORY_COMMIT_PAGE_SIZE = int(os.environ.get("BORROW_HISTORY_COMMIT_PAGE_SIZE", "100"))
POLYGON_OPTIONS_MAX_SYMBOLS = int(os.environ.get("POLYGON_OPTIONS_MAX_SYMBOLS", "100"))
TRADIER_SPOT_MAX_SYMBOLS_PER_BATCH = int(os.environ.get("TRADIER_SPOT_MAX_SYMBOLS_PER_BATCH", "200"))
TRADIER_SPOT_MAX_REQUESTS = int(os.environ.get("TRADIER_SPOT_MAX_REQUESTS", "30"))
OPTIONS_REFRESH_SLEEP_MS = int(os.environ.get("OPTIONS_REFRESH_SLEEP_MS", "0"))
POLYGON_MAX_REQUESTS_PER_MINUTE = int(os.environ.get("POLYGON_MAX_REQUESTS_PER_MINUTE", "25"))
POLYGON_MAX_TOTAL_REQUESTS = int(os.environ.get("POLYGON_MAX_TOTAL_REQUESTS", "90"))
POLYGON_MAX_SNAPSHOT_PAGES_PER_SYMBOL = int(os.environ.get("POLYGON_MAX_SNAPSHOT_PAGES_PER_SYMBOL", "1"))
POLYGON_MAX_CONTRACT_PAGES_PER_SYMBOL = int(os.environ.get("POLYGON_MAX_CONTRACT_PAGES_PER_SYMBOL", "0"))
POLYGON_RETRY_MAX_429 = int(os.environ.get("POLYGON_RETRY_MAX_429", "1"))
POLYGON_CHAIN_MAX_EXPIRIES = int(os.environ.get("POLYGON_CHAIN_MAX_EXPIRIES", "16"))
POLYGON_CHAIN_STRIKE_BAND_PCT = float(os.environ.get("POLYGON_CHAIN_STRIKE_BAND_PCT", "0.50"))
POLYGON_CHAIN_STRIKE_BAND_DOWN_PCT = float(os.environ.get("POLYGON_CHAIN_STRIKE_BAND_DOWN_PCT", str(POLYGON_CHAIN_STRIKE_BAND_PCT)))
POLYGON_CHAIN_STRIKE_BAND_UP_PCT = float(os.environ.get("POLYGON_CHAIN_STRIKE_BAND_UP_PCT", str(POLYGON_CHAIN_STRIKE_BAND_PCT)))
APLZ_CHAIN_STRIKE_BAND_UP_PCT = float(os.environ.get("APLZ_CHAIN_STRIKE_BAND_UP_PCT", "2.00"))
POLYGON_CHAIN_MONEYNESS_MODE = os.environ.get("POLYGON_CHAIN_MONEYNESS_MODE", "atm_otm").strip().lower()
POLYGON_DROP_NULL_QUOTES = os.environ.get("POLYGON_DROP_NULL_QUOTES", "0").strip().lower() not in {"0", "false", "no"}
TRADIER_CHAIN_SYMBOLS_RAW = [
    s.strip()
    for s in os.environ.get("TRADIER_CHAIN_SYMBOLS", "").split(",")
    if s.strip()
]
TRADIER_MAX_REQUESTS_PER_MINUTE = int(os.environ.get("TRADIER_MAX_REQUESTS_PER_MINUTE", "25"))
TRADIER_MAX_TOTAL_REQUESTS = int(os.environ.get("TRADIER_MAX_TOTAL_REQUESTS", "70"))
TRADIER_OPTION_QUOTES_MAX_REQUESTS = int(os.environ.get("TRADIER_OPTION_QUOTES_MAX_REQUESTS", "12"))
TRADIER_OPTION_QUOTES_BATCH_SIZE = int(os.environ.get("TRADIER_OPTION_QUOTES_BATCH_SIZE", "75"))
TRADIER_CHAIN_MAX_EXPIRIES = int(os.environ.get("TRADIER_CHAIN_MAX_EXPIRIES", "16"))
TRADIER_CHAIN_MAX_CONTRACTS_PER_SYMBOL = int(os.environ.get("TRADIER_CHAIN_MAX_CONTRACTS_PER_SYMBOL", "320"))
TRADIER_CHAIN_STRIKE_BAND_PCT = float(os.environ.get("TRADIER_CHAIN_STRIKE_BAND_PCT", "0.50"))
TRADIER_CHAIN_STRIKE_BAND_DOWN_PCT = float(os.environ.get("TRADIER_CHAIN_STRIKE_BAND_DOWN_PCT", str(TRADIER_CHAIN_STRIKE_BAND_PCT)))
TRADIER_CHAIN_STRIKE_BAND_UP_PCT = float(os.environ.get("TRADIER_CHAIN_STRIKE_BAND_UP_PCT", str(TRADIER_CHAIN_STRIKE_BAND_PCT)))
TRADIER_APLZ_CHAIN_STRIKE_BAND_UP_PCT = float(os.environ.get("TRADIER_APLZ_CHAIN_STRIKE_BAND_UP_PCT", "2.00"))
TRADIER_CHAIN_MONEYNESS_MODE = os.environ.get("TRADIER_CHAIN_MONEYNESS_MODE", "atm_otm").strip().lower()
OPTIONS_SYMBOLS_PER_RUN = int(os.environ.get("OPTIONS_SYMBOLS_PER_RUN", "12"))
OPTIONS_SHARD_COUNT = int(os.environ.get("OPTIONS_SHARD_COUNT", "20"))
OPTIONS_SHARD_INTERVAL_MINUTES = int(os.environ.get("OPTIONS_SHARD_INTERVAL_MINUTES", "3"))
OPTIONS_STALE_AFTER_MINUTES = int(os.environ.get("OPTIONS_STALE_AFTER_MINUTES", "180"))
OPTIONS_ONLY_BUCKET3 = os.environ.get("OPTIONS_ONLY_BUCKET3", "1").strip().lower() not in {"0", "false", "no"}
OPTIONS_INCLUDE_BUCKET3_UNDERLYING = os.environ.get("OPTIONS_INCLUDE_BUCKET3_UNDERLYING", "1").strip().lower() not in {"0", "false", "no"}
OPTIONS_ACCUMULATE_CACHE = os.environ.get("OPTIONS_ACCUMULATE_CACHE", "1").strip().lower() not in {"0", "false", "no"}
OPTIONS_MAX_ROWS_PER_SYMBOL = int(os.environ.get("OPTIONS_MAX_ROWS_PER_SYMBOL", "1200"))
POLYGON_FORCE_SYMBOLS_RAW = [
    s.strip()
    for s in os.environ.get("POLYGON_FORCE_SYMBOLS", "").split(",")
    if s.strip()
]

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


def _validate_universe_schema(df: pd.DataFrame) -> None:
    required = ("ETF", "Underlying", "Beta")
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            "Universe CSV missing required columns: "
            + ", ".join(missing)
            + ". Expected at least: ETF, Underlying, Beta."
        )


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

    tradier_chain_symbols = {norm_sym(s) for s in TRADIER_CHAIN_SYMBOLS_RAW if str(s).strip()}
    all_symbols = sorted({norm_sym(s) for s in symbols if str(s).strip()})
    prior_symbols = prior_cache.get("symbols") if isinstance(prior_cache, dict) else {}
    if not isinstance(prior_symbols, dict):
        prior_symbols = {}

    def _coverage_score_for_symbol_payload(payload: dict) -> float:
        def _num(v):
            try:
                x = float(v)
                return x if np.isfinite(x) else None
            except Exception:
                return None

        def _iv(v):
            x = _num(v)
            if x is None or x <= 0:
                return None
            if x > 5:
                x = x / 100.0
            return x if x <= 5 else None

        rows = (payload or {}).get("options") or []
        if not isinstance(rows, list) or not rows:
            return 0.0
        mid_ok = 0
        iv_ok = 0
        for r in rows:
            if _num((r or {}).get("mid")) is not None:
                mid_ok += 1
            if _iv((r or {}).get("iv")) is not None:
                iv_ok += 1
        ratio_mid = mid_ok / len(rows)
        ratio_iv = iv_ok / len(rows)
        return float((ratio_mid + ratio_iv) / 2.0)

    def _cache_age_seconds(payload: dict) -> int:
        ts = (payload or {}).get("updated_at")
        if not isinstance(ts, str) or not ts:
            return 10**9
        try:
            parsed = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return max(0, int((dt.datetime.now(dt.UTC) - parsed).total_seconds()))
        except Exception:
            return 10**9

    # Time-sharded refresh keeps frequent updates while controlling API pressure.
    forced_symbols = [norm_sym(s) for s in POLYGON_FORCE_SYMBOLS_RAW if str(s).strip()]
    forced_set = set(forced_symbols)
    shard_count = max(1, OPTIONS_SHARD_COUNT)
    symbols_per_run = max(1, OPTIONS_SYMBOLS_PER_RUN)
    shard_interval_minutes = max(1, OPTIONS_SHARD_INTERVAL_MINUTES)
    slot = int(time.time() // (shard_interval_minutes * 60)) % shard_count
    shard_candidates = [s for s in all_symbols if (hash(s) % shard_count) == slot and s not in forced_set]
    # Prioritize low-quality/stale symbols first inside the active shard.
    shard_candidates.sort(
        key=lambda s: (
            _coverage_score_for_symbol_payload(prior_symbols.get(s) if isinstance(prior_symbols, dict) else {}),
            -_cache_age_seconds(prior_symbols.get(s) if isinstance(prior_symbols, dict) else {}),
            s,
        )
    )
    refresh_candidates = forced_symbols + shard_candidates
    seen_refresh = set()
    refresh_symbols: list[str] = []
    for s in refresh_candidates:
        if s and s not in seen_refresh:
            seen_refresh.add(s)
            refresh_symbols.append(s)
        if len(refresh_symbols) >= symbols_per_run:
            break
    # Guarantee tradier-chain symbols are refreshed each run, then trim.
    for s in sorted(tradier_chain_symbols):
        if s in all_symbols and s not in seen_refresh:
            refresh_symbols.insert(0, s)
            seen_refresh.add(s)
    refresh_symbols = refresh_symbols[: max(symbols_per_run, len([s for s in tradier_chain_symbols if s in all_symbols]))]

    build_time = dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")
    out = {
        "build_time": build_time,
        "source": "polygon_snapshot",
        "polygon_api_configured": bool(POLYGON_API_KEY),
        "tradier_api_configured": bool(TRADIER_TOKEN),
        "requested_symbols": int(len(symbols)),
        "max_symbols": int(POLYGON_OPTIONS_MAX_SYMBOLS),
        "forced_symbols": [norm_sym(s) for s in POLYGON_FORCE_SYMBOLS_RAW],
        "tradier_chain_symbols": sorted(tradier_chain_symbols),
        "refresh_symbols_count": int(len(refresh_symbols)),
        "shard_interval_minutes": int(shard_interval_minutes),
        "polygon_request_limits": {
            "max_requests_per_minute": int(POLYGON_MAX_REQUESTS_PER_MINUTE),
            "max_total_requests": int(POLYGON_MAX_TOTAL_REQUESTS),
            "max_snapshot_pages_per_symbol": int(POLYGON_MAX_SNAPSHOT_PAGES_PER_SYMBOL),
            "max_contract_pages_per_symbol": int(POLYGON_MAX_CONTRACT_PAGES_PER_SYMBOL),
        },
        "symbols": dict(prior_symbols),
        "errors": [],
        "errors_by_symbol": {},
    }
    for _, payload in out["symbols"].items():
        if not isinstance(payload, dict):
            continue
        if "updated_at" not in payload and prior_cache.get("build_time"):
            payload["updated_at"] = prior_cache.get("build_time")
        if "source" not in payload:
            payload["source"] = "cache"
        payload["quote_coverage_score"] = round(_coverage_score_for_symbol_payload(payload), 4)
    if not POLYGON_API_KEY:
        if prior_symbols:
            out["symbols_count"] = len(prior_symbols)
            out["warning"] = "POLYGON_API_KEY missing; using previous cached options data."
            return out
        out["error"] = "POLYGON_API_KEY missing"
        return out

    session = requests.Session()
    headers = {"User-Agent": "etf-dashboard-builder/1.0"}

    request_timestamps: collections.deque[float] = collections.deque()
    total_polygon_requests = 0
    tradier_request_timestamps: collections.deque[float] = collections.deque()
    total_tradier_requests = 0

    def _safe_float(v):
        if v is None:
            return None
        if isinstance(v, str):
            s = v.strip().replace(",", "")
            if not s:
                return None
            if s.endswith("%"):
                s = s[:-1].strip()
            try:
                return float(s)
            except (TypeError, ValueError):
                return None
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    def _normalize_iv(v) -> float | None:
        iv = _safe_float(v)
        if iv is None or iv <= 0:
            return None
        # Providers can return IV in percent (e.g., 63.5) or decimal (0.635).
        if iv > 5:
            iv = iv / 100.0
        if iv > 5:
            return None
        return iv

    def _pick_iv(values: list[object]) -> float | None:
        for v in values:
            iv = _normalize_iv(v)
            if iv is not None:
                return iv
        return None

    def _normalize_mid(
        bid_val: object,
        ask_val: object,
        *fallback_values: object,
    ) -> float | None:
        bid = _safe_float(bid_val)
        ask = _safe_float(ask_val)
        if bid is not None and ask is not None and bid >= 0 and ask >= bid:
            return 0.5 * (bid + ask)
        for v in fallback_values:
            px = _safe_float(v)
            if px is not None and px > 0:
                return px
        if bid is not None and bid > 0:
            return bid
        if ask is not None and ask > 0:
            return ask
        return None

    def _canon_option_ticker(v: object) -> str:
        s = str(v or "").strip().upper()
        if not s:
            return ""
        if s.startswith("O:"):
            s = s[2:]
        return s

    def _chunked(seq: list[str], n: int):
        step = max(1, int(n))
        for i in range(0, len(seq), step):
            yield seq[i : i + step]

    def _enrich_rows_with_tradier_quotes(rows: list[dict]) -> tuple[list[dict], str | None]:
        if not TRADIER_TOKEN or not rows:
            return rows, None

        targets: list[str] = []
        seen: set[str] = set()
        for r in rows:
            need = r.get("mid") is None or r.get("iv") is None or r.get("delta") is None
            if not need:
                continue
            t = _canon_option_ticker(r.get("ticker"))
            if t and t not in seen:
                seen.add(t)
                targets.append(t)

        if not targets:
            return rows, None

        quote_by_ticker: dict[str, dict] = {}
        errs: list[str] = []
        used_requests = 0
        for batch in _chunked(targets, TRADIER_OPTION_QUOTES_BATCH_SIZE):
            if used_requests >= max(1, TRADIER_OPTION_QUOTES_MAX_REQUESTS):
                errs.append(
                    f"tradier quotes capped at {TRADIER_OPTION_QUOTES_MAX_REQUESTS} requests"
                )
                break
            used_requests += 1
            resp, req_err = _tradier_get(
                "/markets/quotes",
                {"symbols": ",".join(batch), "greeks": "true"},
            )
            if req_err and resp is None:
                errs.append(req_err)
                continue
            if resp is None or not resp.ok:
                continue
            payload = resp.json() or {}
            quotes = ((payload.get("quotes") or {}).get("quote")) or []
            if isinstance(quotes, dict):
                quotes = [quotes]
            for q in quotes:
                t = _canon_option_ticker(q.get("symbol") or q.get("option_symbol"))
                if not t:
                    continue
                greeks = q.get("greeks") or {}
                quote_by_ticker[t] = {
                    "mid": _normalize_mid(
                        q.get("bid"),
                        q.get("ask"),
                        q.get("mark"),
                        q.get("last"),
                        q.get("close"),
                    ),
                    "iv": _pick_iv(
                        [
                            greeks.get("mid_iv"),
                            greeks.get("smv_vol"),
                            greeks.get("bid_iv"),
                            greeks.get("ask_iv"),
                            greeks.get("iv"),
                            q.get("implied_volatility"),
                            q.get("iv"),
                        ]
                    ),
                    "delta": _safe_float(greeks.get("delta") or q.get("delta")),
                }

        if not quote_by_ticker:
            return rows, "; ".join(errs[:2]) if errs else None

        for r in rows:
            t = _canon_option_ticker(r.get("ticker"))
            if not t:
                continue
            q = quote_by_ticker.get(t)
            if not q:
                continue
            if r.get("mid") is None and q.get("mid") is not None:
                r["mid"] = q["mid"]
            if r.get("iv") is None and q.get("iv") is not None:
                r["iv"] = q["iv"]
            if r.get("delta") is None and q.get("delta") is not None:
                r["delta"] = q["delta"]

        return rows, "; ".join(errs[:2]) if errs else None

    def _strike_bands_for_symbol(sym: str, spot_value: float | None, *, provider: str) -> tuple[float | None, float | None]:
        if spot_value is None or spot_value <= 0:
            return None, None
        symbol = norm_sym(sym)
        if provider == "tradier":
            down = max(0.01, float(TRADIER_CHAIN_STRIKE_BAND_DOWN_PCT))
            up = max(0.01, float(TRADIER_CHAIN_STRIKE_BAND_UP_PCT))
            if symbol == "APLZ":
                up = max(up, float(TRADIER_APLZ_CHAIN_STRIKE_BAND_UP_PCT))
        else:
            down = max(0.01, float(POLYGON_CHAIN_STRIKE_BAND_DOWN_PCT))
            up = max(0.01, float(POLYGON_CHAIN_STRIKE_BAND_UP_PCT))
            if symbol == "APLZ":
                up = max(up, float(APLZ_CHAIN_STRIKE_BAND_UP_PCT))
        return spot_value * (1.0 - down), spot_value * (1.0 + up)

    def _merge_option_rows(prior_rows: list[dict], fresh_rows: list[dict]) -> list[dict]:
        merged: dict[tuple[str, str, str], dict] = {}

        def key_of(r: dict) -> tuple[str, str, str]:
            exp = str(r.get("expiration_date") or "")
            ctype = str(r.get("contract_type") or "").lower()
            strike = _safe_float(r.get("strike_price"))
            strike_key = f"{strike:.6f}" if strike is not None else str(r.get("strike_price") or "")
            return (exp, ctype, strike_key)

        def merge_into(base: dict, incoming: dict) -> dict:
            out_row = dict(base)
            for k, v in incoming.items():
                if k in {"mid", "iv", "delta"}:
                    if out_row.get(k) is None and v is not None:
                        out_row[k] = v
                elif k in {"ticker"}:
                    if (not out_row.get(k)) and v:
                        out_row[k] = v
                else:
                    if v is not None:
                        out_row[k] = v
            return out_row

        for r in prior_rows or []:
            if not isinstance(r, dict):
                continue
            merged[key_of(r)] = dict(r)
        for r in fresh_rows or []:
            if not isinstance(r, dict):
                continue
            k = key_of(r)
            if k in merged:
                merged[k] = merge_into(merged[k], r)
            else:
                merged[k] = dict(r)
        return list(merged.values())

    def _filter_option_rows(rows: list[dict], spot_value: float | None, sym: str) -> list[dict]:
        if not rows:
            return rows
        expiries = sorted(
            {str(r.get("expiration_date")) for r in rows if r.get("expiration_date")}
        )
        if POLYGON_CHAIN_MAX_EXPIRIES > 0 and expiries:
            allowed_exp = set(expiries[:POLYGON_CHAIN_MAX_EXPIRIES])
            rows = [r for r in rows if str(r.get("expiration_date")) in allowed_exp]

        if spot_value is not None and spot_value > 0:
            strike_min, strike_max = _strike_bands_for_symbol(sym, spot_value, provider="polygon")
            scoped: list[dict] = []
            for r in rows:
                strike = _safe_float(r.get("strike_price"))
                if strike is None:
                    continue
                ctype = str(r.get("contract_type") or "").lower()
                if POLYGON_CHAIN_MONEYNESS_MODE == "atm_otm":
                    if ctype.startswith("call") and strike < spot_value:
                        continue
                    if ctype.startswith("put") and strike > spot_value:
                        continue
                if strike_min is not None and strike < strike_min:
                    continue
                if strike_max is not None and strike > strike_max:
                    continue
                scoped.append(r)
            rows = scoped

        if POLYGON_DROP_NULL_QUOTES:
            rows = [
                r
                for r in rows
                if _safe_float(r.get("mid")) is not None
                or _normalize_iv(r.get("iv")) is not None
                or _safe_float(r.get("delta")) is not None
            ]

        rows.sort(
            key=lambda x: (
                str(x.get("expiration_date") or ""),
                abs(float(x.get("strike_price"))) if x.get("strike_price") is not None else 9e9,
            )
        )
        return rows

    def _fetch_tradier_spot_map(target_symbols: list[str]) -> tuple[dict[str, float], str | None]:
        if not TRADIER_TOKEN:
            return {}, None
        tradier_headers = {
            "Authorization": f"Bearer {TRADIER_TOKEN}",
            "Accept": "application/json",
            "User-Agent": "etf-dashboard-builder/1.0",
        }
        tradier_session = requests.Session()
        out_map: dict[str, float] = {}
        errs: list[str] = []
        request_count = 0

        for batch in _chunked(target_symbols, TRADIER_SPOT_MAX_SYMBOLS_PER_BATCH):
            if request_count >= max(1, TRADIER_SPOT_MAX_REQUESTS):
                errs.append(
                    f"tradier capped at {TRADIER_SPOT_MAX_REQUESTS} requests; "
                    "remaining symbols may use polygon fallback"
                )
                break
            request_count += 1
            try:
                resp = tradier_session.post(
                    f"{TRADIER_BASE_URL}/markets/quotes",
                    headers=tradier_headers,
                    data={"symbols": ",".join(batch)},
                    timeout=20,
                )
                if not resp.ok:
                    msg = ""
                    try:
                        payload = resp.json() or {}
                        msg = payload.get("error") or payload.get("message") or ""
                    except Exception:
                        msg = ""
                    errs.append(f"tradier HTTP {resp.status_code}{f' {msg}' if msg else ''}")
                    continue
                payload = resp.json() or {}
                quotes = ((payload.get("quotes") or {}).get("quote")) or []
                if isinstance(quotes, dict):
                    quotes = [quotes]
                for q in quotes:
                    sym = norm_sym(q.get("symbol"))
                    if not sym:
                        continue
                    px = (
                        _safe_float(q.get("last"))
                        or _safe_float(q.get("close"))
                        or _safe_float(q.get("bid"))
                        or _safe_float(q.get("ask"))
                    )
                    if px is not None:
                        out_map[sym] = px
            except Exception:
                errs.append("tradier request exception")

            if OPTIONS_REFRESH_SLEEP_MS > 0:
                time.sleep(OPTIONS_REFRESH_SLEEP_MS / 1000.0)
        return out_map, ("; ".join(errs[:4]) if errs else None)

    def _rate_limit_polygon() -> None:
        if POLYGON_MAX_REQUESTS_PER_MINUTE <= 0:
            return
        now = time.monotonic()
        while request_timestamps and (now - request_timestamps[0]) >= 60.0:
            request_timestamps.popleft()
        if len(request_timestamps) < POLYGON_MAX_REQUESTS_PER_MINUTE:
            return
        wait_s = max(0.01, 60.0 - (now - request_timestamps[0]))
        time.sleep(wait_s)

    def _polygon_get(url: str, timeout: int = 25) -> tuple[requests.Response | None, str | None]:
        nonlocal total_polygon_requests
        retries_left = max(0, POLYGON_RETRY_MAX_429)
        while True:
            if POLYGON_MAX_TOTAL_REQUESTS > 0 and total_polygon_requests >= POLYGON_MAX_TOTAL_REQUESTS:
                return None, (
                    f"polygon request budget exceeded ({POLYGON_MAX_TOTAL_REQUESTS}); "
                    "using cached/stale symbols for remainder"
                )
            _rate_limit_polygon()
            try:
                resp = session.get(url, headers=headers, timeout=timeout)
            except Exception:
                return None, "polygon request exception"

            total_polygon_requests += 1
            request_timestamps.append(time.monotonic())

            if resp.status_code != 429:
                return resp, None

            if retries_left <= 0:
                return resp, "HTTP 429 rate limited"

            retry_after = resp.headers.get("Retry-After", "").strip()
            try:
                wait_s = float(retry_after) if retry_after else 2.0
            except ValueError:
                wait_s = 2.0
            wait_s = min(max(wait_s, 0.5), 15.0)
            time.sleep(wait_s)
            retries_left -= 1

    def _rate_limit_tradier() -> None:
        if TRADIER_MAX_REQUESTS_PER_MINUTE <= 0:
            return
        now = time.monotonic()
        while tradier_request_timestamps and (now - tradier_request_timestamps[0]) >= 60.0:
            tradier_request_timestamps.popleft()
        if len(tradier_request_timestamps) < TRADIER_MAX_REQUESTS_PER_MINUTE:
            return
        wait_s = max(0.01, 60.0 - (now - tradier_request_timestamps[0]))
        time.sleep(wait_s)

    def _tradier_get(path: str, params: dict[str, str]) -> tuple[requests.Response | None, str | None]:
        nonlocal total_tradier_requests
        if not TRADIER_TOKEN:
            return None, "tradier token missing"
        if TRADIER_MAX_TOTAL_REQUESTS > 0 and total_tradier_requests >= TRADIER_MAX_TOTAL_REQUESTS:
            return None, f"tradier request budget exceeded ({TRADIER_MAX_TOTAL_REQUESTS})"
        _rate_limit_tradier()
        headers = {
            "Authorization": f"Bearer {TRADIER_TOKEN}",
            "Accept": "application/json",
            "User-Agent": "etf-dashboard-builder/1.0",
        }
        try:
            resp = requests.get(
                f"{TRADIER_BASE_URL}{path}",
                headers=headers,
                params=params,
                timeout=20,
            )
        except Exception:
            return None, "tradier request exception"
        total_tradier_requests += 1
        tradier_request_timestamps.append(time.monotonic())
        if not resp.ok:
            msg = ""
            try:
                payload = resp.json() or {}
                msg = payload.get("error") or payload.get("message") or ""
            except Exception:
                msg = ""
            return resp, f"tradier HTTP {resp.status_code}{f' {msg}' if msg else ''}"
        return resp, None

    def _append_api_key(next_url: str) -> str:
        parsed = urlparse(next_url)
        qs = parse_qs(parsed.query)
        if "apiKey" in qs:
            return next_url
        sep = "&" if parsed.query else ""
        return f"{next_url}{sep}apiKey={POLYGON_API_KEY}"

    def _fetch_json_pages(start_url: str, max_pages: int = 6) -> tuple[list[dict], str | None]:
        rows = []
        next_url = start_url
        pages = 0
        last_error = None
        while next_url and pages < max_pages:
            pages += 1
            resp, req_err = _polygon_get(next_url, timeout=25)
            if req_err and resp is None:
                return rows, req_err
            if resp is None:
                return rows, "polygon request failed"
            if not resp.ok:
                msg = None
                try:
                    payload = resp.json() or {}
                    msg = payload.get("error") or payload.get("message")
                except Exception:
                    msg = None
                last_error = f"HTTP {resp.status_code}{f' {msg}' if msg else ''}"
                return rows, last_error
            payload = resp.json() or {}
            batch = payload.get("results") or []
            if isinstance(batch, list):
                rows.extend(batch)
            nurl = payload.get("next_url")
            next_url = _append_api_key(nurl) if nurl else None
        return rows, last_error

    def _fetch_last_spot(sym: str) -> tuple[float | None, str | None]:
        errs = []
        try:
            # Stocks last trade endpoint (widely available on stock plans).
            url = f"https://api.polygon.io/v2/last/trade/{sym}?{urlencode({'apiKey': POLYGON_API_KEY})}"
            resp, req_err = _polygon_get(url, timeout=15)
            if req_err and resp is None:
                errs.append(req_err)
                resp = None
            if resp is not None and resp.ok:
                payload = resp.json() or {}
                p = (payload.get("results") or {}).get("p")
                if p is not None:
                    return float(p), None
            elif resp is not None:
                try:
                    payload = resp.json() or {}
                    errs.append(f"last_trade HTTP {resp.status_code} {payload.get('error') or payload.get('message') or ''}".strip())
                except Exception:
                    errs.append(f"last_trade HTTP {resp.status_code}")
        except Exception:
            errs.append("last_trade exception")

        try:
            # Previous daily aggregate fallback.
            url = f"https://api.polygon.io/v2/aggs/ticker/{sym}/prev?{urlencode({'adjusted': 'true', 'apiKey': POLYGON_API_KEY})}"
            resp, req_err = _polygon_get(url, timeout=15)
            if req_err and resp is None:
                errs.append(req_err)
                resp = None
            if resp is not None and resp.ok:
                payload = resp.json() or {}
                rows = payload.get("results") or []
                if rows and rows[0].get("c") is not None:
                    return float(rows[0]["c"]), None
            elif resp is not None:
                try:
                    payload = resp.json() or {}
                    errs.append(f"prev_agg HTTP {resp.status_code} {payload.get('error') or payload.get('message') or ''}".strip())
                except Exception:
                    errs.append(f"prev_agg HTTP {resp.status_code}")
        except Exception:
            errs.append("prev_agg exception")

        return None, "; ".join(errs[:3]) if errs else "spot unavailable"

    def _fetch_tradier_chain(sym: str, spot_hint: float | None) -> tuple[list[dict], float | None, str | None]:
        # Tradier chain fetch is intentionally narrow: few expiries + near-ATM strikes.
        exp_resp, exp_err = _tradier_get(
            "/markets/options/expirations",
            {"symbol": sym, "includeAllRoots": "true"},
        )
        if exp_err and exp_resp is None:
            return [], spot_hint, exp_err

        expirations = []
        if exp_resp is not None and exp_resp.ok:
            payload = exp_resp.json() or {}
            dates = ((payload.get("expirations") or {}).get("date")) or []
            if isinstance(dates, str):
                expirations = [dates]
            elif isinstance(dates, list):
                expirations = [str(x) for x in dates if x]
        expirations = expirations[: max(0, TRADIER_CHAIN_MAX_EXPIRIES)]
        if not expirations:
            return [], spot_hint, exp_err or "tradier expirations unavailable"

        out_rows: list[dict] = []
        errs: list[str] = []
        spot_value = spot_hint
        strike_min, strike_max = _strike_bands_for_symbol(sym, spot_value, provider="tradier")

        for exp in expirations:
            chain_resp, chain_err = _tradier_get(
                "/markets/options/chains",
                {"symbol": sym, "expiration": exp, "greeks": "true"},
            )
            if chain_err:
                errs.append(chain_err)
            if chain_resp is None or not chain_resp.ok:
                continue

            payload = chain_resp.json() or {}
            options = ((payload.get("options") or {}).get("option")) or []
            if isinstance(options, dict):
                options = [options]

            for opt in options:
                strike = _safe_float(opt.get("strike"))
                if strike is None:
                    continue
                option_type = str(opt.get("option_type") or opt.get("type") or "").lower()
                if spot_value is not None and spot_value > 0 and TRADIER_CHAIN_MONEYNESS_MODE == "atm_otm":
                    # Keep only ATM/OTM contracts:
                    # - Calls: strike >= spot (ATM or OTM)
                    # - Puts:  strike <= spot (ATM or OTM)
                    if option_type.startswith("call") and strike < spot_value:
                        continue
                    if option_type.startswith("put") and strike > spot_value:
                        continue
                if strike_min is not None and strike < strike_min:
                    continue
                if strike_max is not None and strike > strike_max:
                    continue

                quote = opt.get("quote") or {}
                greeks = opt.get("greeks") or {}
                iv = _pick_iv(
                    [
                        greeks.get("mid_iv"),
                        greeks.get("smv_vol"),
                        greeks.get("bid_iv"),
                        greeks.get("ask_iv"),
                        greeks.get("iv"),
                        opt.get("implied_volatility"),
                        opt.get("iv"),
                    ]
                )
                out_rows.append(
                    {
                        "ticker": opt.get("symbol") or opt.get("option_symbol"),
                        "expiration_date": opt.get("expiration_date") or opt.get("expiration") or exp,
                        "strike_price": strike,
                        "contract_type": "put" if option_type.startswith("put") else "call",
                        "mid": _normalize_mid(
                            opt.get("bid") if opt.get("bid") is not None else quote.get("bid"),
                            opt.get("ask") if opt.get("ask") is not None else quote.get("ask"),
                            quote.get("mid"),
                            quote.get("mark"),
                            opt.get("mark"),
                            opt.get("last"),
                            opt.get("close"),
                        ),
                        "iv": iv,
                        "delta": _safe_float(greeks.get("delta")),
                    }
                )
                if len(out_rows) >= max(1, TRADIER_CHAIN_MAX_CONTRACTS_PER_SYMBOL):
                    break
            if len(out_rows) >= max(1, TRADIER_CHAIN_MAX_CONTRACTS_PER_SYMBOL):
                break

        err_msg = "; ".join(errs[:3]) if errs else None
        out_rows, enrich_err = _enrich_rows_with_tradier_quotes(out_rows)
        if enrich_err:
            err_msg = f"{err_msg}; tradier_quote_enrich: {enrich_err}" if err_msg else f"tradier_quote_enrich: {enrich_err}"
        return out_rows, spot_value, err_msg

    def _set_symbol_entry(sym: str, spot: float | None, options_rows: list[dict], source: str) -> None:
        out["symbols"][sym] = {
            "spot": float(spot) if spot is not None else None,
            "options": options_rows,
            "updated_at": build_time,
            "source": source,
        }

    tradier_spot_map, tradier_err = _fetch_tradier_spot_map(all_symbols)
    if tradier_err:
        out["errors"].append(tradier_err)

    for sym in refresh_symbols:
        try:
            under_px = None
            tradier_spot = tradier_spot_map.get(sym)
            sym_errors = []
            snapshot_rate_limited = False
            rows = []
            symbol_source = "polygon_snapshot"
            payload = {}
            resp = None

            # For configured symbols, prefer Tradier chain directly to avoid Polygon 403/429 churn.
            if sym in tradier_chain_symbols:
                tradier_rows, _, tradier_chain_err = _fetch_tradier_chain(sym, tradier_spot)
                if tradier_rows:
                    rows = tradier_rows
                    symbol_source = "tradier_chain"
                elif tradier_chain_err:
                    sym_errors.append(f"tradier_chain: {tradier_chain_err}")

            if not rows:
                snap_start = f"https://api.polygon.io/v3/snapshot/options/{sym}?{urlencode({'limit': 250, 'apiKey': POLYGON_API_KEY})}"
                resp, req_err = _polygon_get(snap_start, timeout=25)
                if req_err and resp is None:
                    sym_errors.append(f"snapshot: {req_err}")
                    payload = {}
                else:
                    payload = resp.json() if (resp is not None and resp.ok) else {}
                if resp is not None and resp.ok:
                    batch = payload.get("results") or []
                    if isinstance(batch, list):
                        rows.extend(batch)
                    under_px = (payload.get("underlying_asset") or {}).get("price")
                    nurl = payload.get("next_url")
                    if nurl:
                        more_rows, page_err = _fetch_json_pages(
                            _append_api_key(nurl),
                            max_pages=max(0, POLYGON_MAX_SNAPSHOT_PAGES_PER_SYMBOL),
                        )
                        rows.extend(more_rows)
                        if page_err:
                            sym_errors.append(f"snapshot next_url: {page_err}")
                            if "HTTP 429" in str(page_err):
                                snapshot_rate_limited = True
                elif resp is not None:
                    msg = payload.get("error") or payload.get("message") or ""
                    sym_errors.append(f"snapshot HTTP {resp.status_code}{f' {msg}' if msg else ''}")
                    if resp.status_code == 429:
                        snapshot_rate_limited = True

            # Fallback: contracts reference endpoint to at least populate expiries/strikes.
            if not rows and not snapshot_rate_limited and sym not in tradier_chain_symbols:
                ref_start = (
                    "https://api.polygon.io/v3/reference/options/contracts?"
                    + urlencode({
                        "underlying_ticker": sym,
                        "expired": "false",
                        "limit": 1000,
                        "order": "asc",
                        "sort": "expiration_date",
                        "apiKey": POLYGON_API_KEY,
                    })
                )
                ref_rows, ref_err = _fetch_json_pages(
                    ref_start,
                    max_pages=max(0, POLYGON_MAX_CONTRACT_PAGES_PER_SYMBOL),
                )
                if ref_rows:
                    rows = ref_rows
                if ref_err:
                    sym_errors.append(f"contracts: {ref_err}")
            elif not rows and snapshot_rate_limited and sym not in tradier_chain_symbols:
                sym_errors.append("contracts fallback skipped after snapshot 429")

            # Secondary fallback to Tradier chains for non-primary symbols that failed Polygon.
            if not rows and sym not in tradier_chain_symbols:
                tradier_rows, _, tradier_chain_err = _fetch_tradier_chain(sym, tradier_spot)
                if tradier_rows:
                    rows = tradier_rows
                    symbol_source = "tradier_chain_fallback"
                elif tradier_chain_err:
                    sym_errors.append(f"tradier_chain: {tradier_chain_err}")
            if not rows:
                spot_only = tradier_spot if tradier_spot is not None else None
                spot_err = None
                if spot_only is None:
                    spot_only, spot_err = _fetch_last_spot(sym)
                if spot_only is not None:
                    _set_symbol_entry(sym, spot_only, [], "spot_only")
                    if sym_errors:
                        out["errors_by_symbol"][sym] = "; ".join(sym_errors[:3])
                    continue
                prior_sym = prior_cache.get("symbols", {}).get(sym) if isinstance(prior_cache, dict) else None
                if prior_sym:
                    out["symbols"][sym] = prior_sym
                    if sym_errors:
                        out["errors_by_symbol"][sym] = "; ".join(sym_errors[:3])
                else:
                    if spot_err:
                        sym_errors.append(spot_err)
                    if sym_errors:
                        out["errors_by_symbol"][sym] = "; ".join(sym_errors[:4])
                continue

            parsed = []
            for r in rows:
                details = r.get("details") or {}
                quote = r.get("last_quote") or {}
                greeks = r.get("greeks") or {}
                iv = _pick_iv(
                    [
                        r.get("implied_volatility"),
                        r.get("impliedVolatility"),
                        r.get("iv"),
                        greeks.get("implied_volatility"),
                        greeks.get("impliedVolatility"),
                        greeks.get("mid_iv"),
                        greeks.get("smv_vol"),
                        greeks.get("iv"),
                    ]
                )
                last_trade = r.get("last_trade") or {}
                day = r.get("day") or {}
                mid = _normalize_mid(
                    quote.get("bid") if quote.get("bid") is not None else quote.get("bid_price"),
                    quote.get("ask") if quote.get("ask") is not None else quote.get("ask_price"),
                    quote.get("midpoint"),
                    last_trade.get("price") if last_trade.get("price") is not None else last_trade.get("p"),
                    day.get("close") if day.get("close") is not None else day.get("c"),
                )

                if under_px is None:
                    under_px = r.get("underlying_asset", {}).get("price")
                exp = (
                    details.get("expiration_date")
                    or r.get("expiration_date")
                    or r.get("expiration_date_from")
                )
                strike = (
                    details.get("strike_price")
                    if details.get("strike_price") is not None
                    else r.get("strike_price")
                )
                strike = _safe_float(strike)
                ctype = (details.get("contract_type") or r.get("contract_type") or "").strip().lower()
                if not ctype:
                    opt_ticker = details.get("ticker") or r.get("ticker") or ""
                    if "P" in str(opt_ticker):
                        ctype = "put"
                    elif "C" in str(opt_ticker):
                        ctype = "call"
                parsed.append(
                    {
                        "ticker": details.get("ticker") or r.get("ticker"),
                        "expiration_date": exp,
                        "strike_price": strike,
                        "contract_type": ctype,
                        "mid": mid,
                        "iv": iv,
                        "delta": _safe_float(greeks.get("delta")),
                    }
                )

            parsed = [x for x in parsed if x.get("expiration_date") and x.get("strike_price") is not None]
            parsed, enrich_err = _enrich_rows_with_tradier_quotes(parsed)
            if enrich_err:
                sym_errors.append(f"tradier_quote_enrich: {enrich_err}")
            prior_rows = []
            prior_payload = prior_symbols.get(sym) if isinstance(prior_symbols, dict) else None
            if isinstance(prior_payload, dict):
                pr = prior_payload.get("options")
                if isinstance(pr, list):
                    prior_rows = pr
            if OPTIONS_ACCUMULATE_CACHE and prior_rows:
                parsed = _merge_option_rows(prior_rows, parsed)
            parsed = _filter_option_rows(parsed, tradier_spot if tradier_spot is not None else under_px, sym)
            if not parsed:
                spot_only = tradier_spot if tradier_spot is not None else under_px
                if spot_only is None:
                    spot_only, spot_err = _fetch_last_spot(sym)
                    if spot_err:
                        sym_errors.append(spot_err)
                if spot_only is not None:
                    _set_symbol_entry(sym, spot_only, [], "spot_only")
                    if sym_errors:
                        out["errors_by_symbol"][sym] = "; ".join(sym_errors[:4])
                    continue
                prior_sym = prior_cache.get("symbols", {}).get(sym) if isinstance(prior_cache, dict) else None
                if prior_sym:
                    out["symbols"][sym] = prior_sym
                    if sym_errors:
                        out["errors_by_symbol"][sym] = "; ".join(sym_errors[:4])
                elif sym_errors:
                    out["errors_by_symbol"][sym] = "; ".join(sym_errors[:4])
                continue
            final_spot = tradier_spot if tradier_spot is not None else under_px
            if final_spot is None:
                final_spot, spot_err = _fetch_last_spot(sym)
                if spot_err:
                    sym_errors.append(spot_err)
            _set_symbol_entry(sym, final_spot, parsed[: max(100, OPTIONS_MAX_ROWS_PER_SYMBOL)], symbol_source)
            if sym_errors:
                out["errors_by_symbol"][sym] = "; ".join(sym_errors[:4])
            if OPTIONS_REFRESH_SLEEP_MS > 0:
                time.sleep(OPTIONS_REFRESH_SLEEP_MS / 1000.0)
        except Exception:
            prior_sym = prior_cache.get("symbols", {}).get(sym) if isinstance(prior_cache, dict) else None
            if prior_sym:
                out["symbols"][sym] = prior_sym
            continue

    out["symbols_count"] = len(out["symbols"])
    out["polygon_requests_used"] = int(total_polygon_requests)
    out["tradier_requests_used"] = int(total_tradier_requests)
    # Normalize/prune carried cache entries so stale null-only chains do not dominate.
    for sym, payload in list(out["symbols"].items()):
        if not isinstance(payload, dict):
            continue
        rows = payload.get("options")
        if not isinstance(rows, list):
            continue
        spot = _safe_float(payload.get("spot"))
        payload["options"] = _filter_option_rows(rows, spot, sym)
    option_rows_total = 0
    option_mid_nonnull = 0
    option_iv_nonnull = 0
    for payload in out["symbols"].values():
        if not isinstance(payload, dict):
            continue
        for row in (payload.get("options") or []):
            option_rows_total += 1
            if _safe_float(row.get("mid")) is not None:
                option_mid_nonnull += 1
            if _normalize_iv(row.get("iv")) is not None:
                option_iv_nonnull += 1
    out["option_field_coverage"] = {
        "rows_total": int(option_rows_total),
        "mid_nonnull": int(option_mid_nonnull),
        "iv_nonnull": int(option_iv_nonnull),
    }
    now_utc = dt.datetime.now(dt.UTC)
    ages: list[int] = []
    refreshed = 0
    stale_after_s = max(60, int(OPTIONS_STALE_AFTER_MINUTES) * 60)
    stale_symbols = 0
    for _, payload in out["symbols"].items():
        if not isinstance(payload, dict):
            continue
        ts = payload.get("updated_at")
        age = None
        if isinstance(ts, str) and ts:
            try:
                parsed = dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
                age = max(0, int((now_utc - parsed).total_seconds()))
            except Exception:
                age = None
        if age is not None:
            payload["cache_age_seconds"] = age
            payload["stale"] = bool(age > stale_after_s)
            if payload["stale"]:
                stale_symbols += 1
            ages.append(age)
            if age <= 5:
                refreshed += 1
            payload["quote_coverage_score"] = round(_coverage_score_for_symbol_payload(payload), 4)
    if ages:
        out["cache_age_summary"] = {
            "min_seconds": int(min(ages)),
            "median_seconds": int(np.median(ages)),
            "max_seconds": int(max(ages)),
            "refreshed_symbols": int(refreshed),
            "stale_symbols": int(stale_symbols),
            "stale_after_minutes": int(OPTIONS_STALE_AFTER_MINUTES),
        }
    if out["errors"]:
        out["errors"] = sorted(set(out["errors"]))[:50]
    else:
        out.pop("errors", None)
    if not out["errors_by_symbol"]:
        out.pop("errors_by_symbol", None)
    if out["symbols_count"] == 0 and isinstance(prior_cache, dict):
        prior_symbols = prior_cache.get("symbols")
        if isinstance(prior_symbols, dict) and prior_symbols:
            out["symbols"] = prior_symbols
            out["symbols_count"] = len(prior_symbols)
            out["warning"] = "Polygon fetch returned no symbols; using previous cached options data."
    return out


def select_symbols_for_polygon_cache(records: list[dict]) -> list[str]:
    """
    Keep Polygon requests bounded to avoid rate-limit starvation.
    Priority:
      1) forced symbols from env
      2) symbol + underlying pairs in record order
    """
    seen = set()
    ordered: list[str] = []

    def add(sym):
        s = norm_sym(sym)
        if not s or s in seen:
            return
        seen.add(s)
        ordered.append(s)

    # Bucket-3-only universe by default (inverse ETF options only).
    base_set = set()
    for rec in records:
        if OPTIONS_ONLY_BUCKET3 and str(rec.get("bucket")) != "bucket_3_inverse":
            continue
        base_set.add(norm_sym(rec.get("symbol")))
        if OPTIONS_INCLUDE_BUCKET3_UNDERLYING:
            base_set.add(norm_sym(rec.get("underlying")))

    allowed_forced = {norm_sym(s) for s in base_set}
    for s in POLYGON_FORCE_SYMBOLS_RAW:
        ss = norm_sym(s)
        if ss in allowed_forced:
            add(ss)

    # Deterministic ordering inside selected universe.
    base_symbols = sorted(s for s in base_set if s)
    for s in base_symbols:
        add(s)
        if len(ordered) >= max(1, POLYGON_OPTIONS_MAX_SYMBOLS):
            break

    return ordered


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


def _scope_series_window(
    points: list[tuple[dt.date, float, float]],
    window: str,
) -> list[tuple[dt.date, float, float]]:
    if not points:
        return []
    if window == "ALL":
        return points
    if window == "YTD":
        start = dt.date(points[-1][0].year, 1, 1)
        return [p for p in points if p[0] >= start]
    months_by_window = {"1M": 1, "3M": 3, "6M": 6, "12M": 12}
    m = months_by_window.get(window)
    if not m:
        return points
    asof = points[-1][0]
    y = asof.year
    mo = asof.month - m
    while mo <= 0:
        y -= 1
        mo += 12
    # Clamp day to end-of-month of target month.
    day = min(asof.day, [31, 29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][mo - 1])
    start = dt.date(y, mo, day)
    scoped = [p for p in points if p[0] >= start]
    return scoped if scoped else points[-2:]


def _build_market_windows(
    points: list[tuple[dt.date, float, float]],
    dividends: list[tuple[dt.date, float]],
) -> dict:
    """
    Build per-window stats from (date, close, adj_close) plus dividends.
    Returns window -> realized vol stats + dividend/return diagnostics.
    """
    if len(points) < 2:
        return {}

    points = sorted(points, key=lambda x: x[0])
    dividends = sorted(dividends, key=lambda x: x[0])
    results: dict[str, dict] = {}

    for window in VOL_WINDOWS:
        scoped = _scope_series_window(points, window)
        if len(scoped) < 2:
            continue
        start_date = scoped[0][0]
        end_date = scoped[-1][0]
        start_close = scoped[0][1]
        end_close = scoped[-1][1]
        start_adj = scoped[0][2]
        end_adj = scoped[-1][2]

        adj_closes = [adj for _, _, adj in scoped]
        stats = _compute_vol_stats_from_closes(adj_closes)

        total_dividends = 0.0
        for d, amount in dividends:
            if d < start_date or d > end_date:
                continue
            total_dividends += float(amount)

        price_return = None
        adj_return = None
        dividend_yield = None
        if start_close > 0:
            price_return = (end_close / start_close) - 1.0
            dividend_yield = total_dividends / start_close
        if start_adj > 0:
            adj_return = (end_adj / start_adj) - 1.0

        results[window] = {
            "vol_annual": stats["vol_annual"],
            "ewma_vol_annual": stats["ewma_vol_annual"],
            "n_returns": stats["n_returns"] if stats["n_returns"] > 0 else None,
            "asof": end_date.isoformat(),
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "start_close": round(float(start_close), 6),
            "end_close": round(float(end_close), 6),
            "start_adj_close": round(float(start_adj), 6),
            "end_adj_close": round(float(end_adj), 6),
            "total_dividends": round(float(total_dividends), 6),
            "dividend_yield": round(float(dividend_yield), 6) if dividend_yield is not None else None,
            "price_return": round(float(price_return), 6) if price_return is not None else None,
            "adj_return": round(float(adj_return), 6) if adj_return is not None else None,
        }

    return results


def _fetch_yahoo_symbol_series(
    session: requests.Session,
    symbol: str,
    *,
    range_value: str,
) -> dict:
    """Fetch close/adjclose plus dividend events from Yahoo chart API."""
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
                return {"points": [], "dividends": []}

            timestamps = result.get("timestamp") or []
            close_values = (result.get("indicators", {}).get("quote", [{}])[0].get("close")) or []
            adj_values = (
                (result.get("indicators", {}).get("adjclose", [{}])[0].get("adjclose"))
                or close_values
            )
            n = min(len(timestamps), len(close_values), len(adj_values))
            points = []
            for i in range(n):
                ts = timestamps[i]
                close = close_values[i]
                adj_close = adj_values[i]
                if ts is None or close is None or adj_close is None:
                    continue
                try:
                    px_close = float(close)
                    px_adj = float(adj_close)
                    if (
                        not np.isfinite(px_close)
                        or not np.isfinite(px_adj)
                        or px_close <= 0
                        or px_adj <= 0
                    ):
                        continue
                    d = dt.datetime.fromtimestamp(int(ts), dt.UTC).date()
                    points.append((d, px_close, px_adj))
                except (ValueError, TypeError, OSError):
                    continue

            dividends = []
            div_map = (result.get("events") or {}).get("dividends") or {}
            if isinstance(div_map, dict):
                for ts_key, payload in div_map.items():
                    amount = payload.get("amount") if isinstance(payload, dict) else None
                    try:
                        amt = float(amount)
                        if not np.isfinite(amt):
                            continue
                    except (TypeError, ValueError):
                        continue
                    try:
                        ev_ts = int(payload.get("date") if isinstance(payload, dict) and payload.get("date") is not None else ts_key)
                        ev_date = dt.datetime.fromtimestamp(ev_ts, dt.UTC).date()
                    except (ValueError, TypeError, OSError):
                        continue
                    dividends.append((ev_date, float(amt)))

            return {"points": points, "dividends": dividends}
        except Exception as e:
            last_err = e
            if attempt < REALIZED_VOL_RETRIES:
                continue
    print(f"  Warning: Yahoo history fetch failed for {symbol}: {last_err}")
    return {"points": [], "dividends": []}


def compute_realized_vol_map(symbols: set[str]) -> dict[str, dict]:
    """
    Returns symbol -> market windows.
    Each window has realized-vol and dividend/return diagnostics.
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
        series = _fetch_yahoo_symbol_series(session, sym, range_value=REALIZED_VOL_RANGE)
        points = series.get("points") or []
        dividends = series.get("dividends") or []
        windows = _build_market_windows(points, dividends) if points else {}
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
    output["tradier_api_configured"] = bool(TRADIER_TOKEN)
    output["options_cache_file"] = "data/options_cache.json"

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=None, separators=(",", ":"))

    print(f"[OK] Borrow-only refresh wrote {OUTPUT_FILE}")
    print(f"  Updated from IBKR FTP: {updated}/{len(records)} symbols")
    print(f"  Updated from latest CSV fallback: {updated_csv}/{len(records)} symbols")


def refresh_options_only() -> None:
    """Refresh options/spot cache used by Trade Lab."""
    if not OUTPUT_FILE.exists():
        raise FileNotFoundError(
            f"Cannot run --options-only because {OUTPUT_FILE} does not exist. Run full build first."
        )

    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        output = json.load(f)
    records = output.get("records", [])
    if not isinstance(records, list) or not records:
        raise ValueError("dashboard_data.json is malformed: missing records list")

    symbols_for_options = select_symbols_for_polygon_cache(records)
    options_cache = build_polygon_options_cache(symbols_for_options)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OPTIONS_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(options_cache, f, indent=None, separators=(",", ":"))

    print(f"[OK] Options-only refresh wrote {OPTIONS_CACHE_FILE}")
    print(f"  requested symbols: {options_cache.get('requested_symbols')}")
    print(f"  cached symbols: {options_cache.get('symbols_count')}")
    if options_cache.get("errors"):
        print(f"  sample error: {options_cache['errors'][0]}")


# ──────────────────────────────────────────────
# Main build
# ──────────────────────────────────────────────
def build():
    print("=" * 60)
    print("ETF Dashboard — Static Data Builder")
    print("=" * 60)

    # 1. Fetch universe CSV from GitHub
    df = fetch_csv_from_github()
    _validate_universe_schema(df)
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
        dividend_adjustment = {}
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
            dividend_adjustment[window] = {
                "etf_total_dividends": etf_w.get("total_dividends"),
                "underlying_total_dividends": und_w.get("total_dividends"),
                "etf_dividend_yield": etf_w.get("dividend_yield"),
                "underlying_dividend_yield": und_w.get("dividend_yield"),
                "etf_price_return": etf_w.get("price_return"),
                "underlying_price_return": und_w.get("price_return"),
                "etf_adj_return": etf_w.get("adj_return"),
                "underlying_adj_return": und_w.get("adj_return"),
                "etf_start_close": etf_w.get("start_close"),
                "underlying_start_close": und_w.get("start_close"),
                "etf_end_close": etf_w.get("end_close"),
                "underlying_end_close": und_w.get("end_close"),
                "etf_start_date": etf_w.get("start_date"),
                "etf_end_date": etf_w.get("end_date"),
                "underlying_start_date": und_w.get("start_date"),
                "underlying_end_date": und_w.get("end_date"),
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
            "dividend_adjustment": dividend_adjustment,
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
        "tradier_api_configured": bool(TRADIER_TOKEN),
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
    symbols_for_options = select_symbols_for_polygon_cache(records)
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
    parser.add_argument(
        "--options-only",
        action="store_true",
        help="Only refresh options/spot cache in data/options_cache.json",
    )
    args = parser.parse_args()

    if args.borrow_only and args.options_only:
        raise SystemExit("Use either --borrow-only or --options-only, not both.")
    if args.borrow_only:
        refresh_borrow_only()
    elif args.options_only:
        refresh_options_only()
    else:
        build()

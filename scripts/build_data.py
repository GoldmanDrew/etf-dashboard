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

import datetime as dt
import json
import os
import sys
from pathlib import Path

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

HIGH_BETA_THRESHOLD = float(os.environ.get("HIGH_BETA_THRESHOLD", "1.5"))

OUTPUT_DIR = Path(__file__).parent.parent / "data"
OUTPUT_FILE = OUTPUT_DIR / "dashboard_data.json"

# Curated inverse ETF list (Bucket 3 source of truth)
INVERSE_ETFS = {
    "SQQQ", "SDS", "SPXS", "SPXU", "QID", "SDOW", "DXD", "TWM", "TZA",
    "SOXS", "FAZ", "SKF", "LABD", "TECS", "WEBS", "FNGD", "REW", "TTXD",
    "TSXD", "DUST", "ZSL", "SCO", "DUG", "DRIP", "TSLQ", "MSTZ", "NVDQ",
    "NVDS", "TMV", "TBT", "BTCZ", "ETHD",
}


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
    print(f"  → {len(resp.text):,} bytes, saved to {csv_path}")

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


def try_fetch_ibkr_ftp() -> dict:
    """
    Try to fetch live IBKR borrow data from FTP.
    Returns {borrow_map, fee_map, rebate_map, available_map, success}.
    Falls back to CSV values if FTP fails.
    """
    import ftplib
    import io

    result = {"borrow_map": {}, "fee_map": {}, "rebate_map": {}, "available_map": {}, "success": False}

    try:
        print("Fetching IBKR short stock file from FTP ...")
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
        df["net_borrow"] = (df["fee_annual"] - df["rebate_annual"]).clip(lower=0)

        for _, row in df.iterrows():
            sym = norm_sym(row["sym"])
            if pd.notna(row["net_borrow"]):
                result["borrow_map"][sym] = round(float(row["net_borrow"]), 6)
            if pd.notna(row["fee_annual"]):
                result["fee_map"][sym] = round(float(row["fee_annual"]), 6)
            if pd.notna(row["rebate_annual"]):
                result["rebate_map"][sym] = round(float(row["rebate_annual"]), 6)
            if pd.notna(row["available_int"]):
                result["available_map"][sym] = int(row["available_int"])

        result["success"] = True
        print(f"  → IBKR FTP: {len(result['borrow_map'])} symbols fetched")

    except Exception as e:
        print(f"  → IBKR FTP failed: {e}. Using CSV borrow values.")
        result["success"] = False

    return result


# ──────────────────────────────────────────────
# Bucketing
# ──────────────────────────────────────────────
def assign_bucket(sym: str, beta: float) -> str:
    if sym in INVERSE_ETFS:
        return "bucket_3_inverse"
    if pd.notna(beta) and beta > HIGH_BETA_THRESHOLD:
        return "bucket_1_high_beta"
    return "bucket_2_low_beta"


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
    for col in ["gross_decay_annual", "net_decay_annual", "borrow_drag_annual",
                 "vol_underlying_annual", "vol_etf_annual"]:
        n = df[col].notna().sum() if col in df.columns else 0
        print(f"  {col}: {n}/{len(df)}" if col in df.columns else f"  {col}: MISSING")

    # 2. Fetch last commit info
    commit_info = fetch_last_commit_info()
    if commit_info:
        print(f"Last screener run: {commit_info['date']} ({commit_info['sha']})")

    # 3. Try IBKR FTP for live borrow data
    ibkr = try_fetch_ibkr_ftp()

    # 4. Build records
    records = []
    decay_count = 0

    for _, row in df.iterrows():
        sym = row["symbol"]
        beta = float(row["Beta"]) if pd.notna(row.get("Beta")) else None

        bucket = assign_bucket(sym, beta or 0)

        # Borrow data: prefer IBKR FTP, fall back to CSV
        if ibkr["success"] and sym in ibkr["borrow_map"]:
            borrow_net = ibkr["borrow_map"][sym]
            borrow_fee = ibkr["fee_map"].get(sym)
            borrow_rebate = ibkr["rebate_map"].get(sym)
            shares_avail = ibkr["available_map"].get(sym)
            borrow_source = "ibkr_ftp"
        else:
            borrow_net = _safe_float(row, "borrow_net_annual")
            borrow_fee = _safe_float(row, "borrow_fee_annual")
            borrow_rebate = _safe_float(row, "borrow_rebate_annual")
            shares_avail = int(row["shares_available"]) if pd.notna(row.get("shares_available")) else None
            borrow_source = "csv"

        # Analytics from CSV (computed by etf_analytics.py in ls-algo)
        gross_decay = _safe_float(row, "gross_decay_annual")
        net_decay = _safe_float(row, "net_decay_annual")
        borrow_drag = _safe_float(row, "borrow_drag_annual")
        vol_und = _safe_float(row, "vol_underlying_annual")
        vol_etf = _safe_float(row, "vol_etf_annual")

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
            "borrow_net_annual": round(borrow_net, 6) if borrow_net is not None else None,
            "shares_available": shares_avail,
            "borrow_spiking": bool(row.get("borrow_spiking", False)),
            "borrow_missing": bool(row.get("borrow_missing_from_ftp", False)),
            "gross_decay_annual": gross_decay,
            "net_decay": net_decay,
            "borrow_drag_annual": borrow_drag,
            "vol_underlying_annual": vol_und,
            "vol_etf_annual": vol_etf,
            "include_for_algo": bool(row.get("include_for_algo", False)),
            "protected": bool(row.get("protected", False)),
            "cagr_positive": bool(row.get("cagr_positive")) if pd.notna(row.get("cagr_positive")) else None,
            "borrow_source": borrow_source,
        }
        records.append(rec)

    # 5. Compute summary
    b1 = [r for r in records if r["bucket"] == "bucket_1_high_beta"]
    b2 = [r for r in records if r["bucket"] == "bucket_2_low_beta"]
    b3 = [r for r in records if r["bucket"] == "bucket_3_inverse"]

    with_net = sorted(
        [r for r in records if r["net_decay"] is not None],
        key=lambda r: r["net_decay"], reverse=True,
    )
    best_net_decay = [
        {"symbol": r["symbol"], "net_decay": r["net_decay"],
         "borrow": r["borrow_net_annual"], "decay": r["gross_decay_annual"]}
        for r in with_net[:5]
    ]

    with_borrow = sorted(
        [r for r in records if r["borrow_net_annual"] is not None],
        key=lambda r: r["borrow_net_annual"], reverse=True,
    )
    worst_borrows = [
        {"symbol": r["symbol"], "borrow": r["borrow_net_annual"], "shares": r["shares_available"]}
        for r in with_borrow[:5]
    ]

    missing = sum(1 for r in records if r["borrow_missing"])

    build_time = dt.datetime.utcnow().isoformat() + "Z"

    output = {
        "build_time": build_time,
        "source_repo": UNIVERSE_REPO,
        "source_branch": UNIVERSE_BRANCH,
        "last_commit": commit_info,
        "ibkr_ftp_success": ibkr["success"],
        "ibkr_symbols_fetched": len(ibkr["borrow_map"]) if ibkr["success"] else 0,
        "decay_method": "linear_daily_pnl_1_over_beta_hedge",
        "summary": {
            "total_symbols": len(records),
            "bucket_1_count": len(b1),
            "bucket_2_count": len(b2),
            "bucket_3_count": len(b3),
            "best_net_decay": best_net_decay,
            "worst_borrows": worst_borrows,
            "pct_missing": round(missing / len(records) * 100, 1) if records else 0,
            "decay_computed_count": decay_count,
        },
        "records": records,
    }

    # 6. Write JSON
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=None, separators=(",", ":"))

    file_size = OUTPUT_FILE.stat().st_size
    print(f"\n✓ Wrote {OUTPUT_FILE} ({file_size:,} bytes)")
    print(f"  {len(records)} records | B1={len(b1)} B2={len(b2)} B3={len(b3)}")
    print(f"  Decay: {decay_count}/{len(records)} ({100*decay_count/len(records):.0f}%)")
    print(f"  IBKR FTP: {'✓' if ibkr['success'] else '✗'}")
    print(f"  Build time: {build_time}")
    print("=" * 60)


if __name__ == "__main__":
    build()

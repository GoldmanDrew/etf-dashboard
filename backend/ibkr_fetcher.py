"""
IBKR Short Stock Borrow Rate Fetcher.

Supports two modes:
  - Live:  Fetches from ftp2.interactivebrokers.com (public shortstock FTP)
  - Mock:  Generates realistic mock data for development/testing
"""
from __future__ import annotations

import datetime as dt
import ftplib
import io
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class BorrowSnapshot:
    """Result of a single borrow-rate fetch cycle."""
    borrow_map: dict[str, float] = field(default_factory=dict)      # sym → net annual rate
    rebate_map: dict[str, float] = field(default_factory=dict)      # sym → rebate annual
    fee_map: dict[str, float] = field(default_factory=dict)         # sym → fee annual
    available_map: dict[str, int] = field(default_factory=dict)     # sym → shares available
    timestamp: dt.datetime = field(default_factory=dt.datetime.utcnow)
    duration_ms: float = 0.0
    success: bool = True
    error: Optional[str] = None


def _norm(s: str) -> str:
    return str(s).strip().upper().replace(".", "-")


# ──────────────────────────────────────────────
# LIVE IBKR FTP Fetcher
# ──────────────────────────────────────────────
def fetch_ibkr_ftp(
    host: str = "ftp2.interactivebrokers.com",
    user: str = "shortstock",
    passwd: str = "",
    filename: str = "usa.txt",
    timeout: int = 30,
) -> BorrowSnapshot:
    """Fetch borrow rates from IBKR's public shortstock FTP file."""
    t0 = time.monotonic()
    snap = BorrowSnapshot()
    try:
        ftp = ftplib.FTP(host, timeout=timeout)
        ftp.login(user=user, passwd=passwd)

        buf = io.BytesIO()
        ftp.retrbinary(f"RETR {filename}", buf.write)
        ftp.quit()

        buf.seek(0)
        text = buf.getvalue().decode("utf-8", errors="ignore")
        lines = [ln for ln in text.splitlines() if ln.strip()]

        # Find header line #SYM|...
        header_idx = None
        for i, ln in enumerate(lines):
            if ln.startswith("#SYM|"):
                header_idx = i
                break
        if header_idx is None:
            raise ValueError("No header line (#SYM|...) found in FTP file")

        header_cols = [c.strip().lstrip("#").lower() for c in lines[header_idx].split("|")]
        data_lines = lines[header_idx + 1:]
        data_str = "\n".join(data_lines)
        df = pd.read_csv(io.StringIO(data_str), sep="|", header=None, engine="python")

        n_cols = min(len(header_cols), df.shape[1])
        df = df.iloc[:, :n_cols]
        df.columns = header_cols[:n_cols]
        df = df.drop(columns=[c for c in df.columns if not c or str(c).startswith("unnamed")], errors="ignore")

        # Normalize
        df["sym"] = df["sym"].astype(str).str.upper().str.strip()

        # Rates: file values are in percent (e.g., 3.63 means 3.63%)
        df["rebate_annual"] = pd.to_numeric(df.get("rebaterate", pd.Series(dtype=float)), errors="coerce") / 100.0
        df["fee_annual"] = pd.to_numeric(df.get("feerate", pd.Series(dtype=float)), errors="coerce") / 100.0
        df["available_int"] = pd.to_numeric(df.get("available", pd.Series(dtype=float)), errors="coerce")

        df["net_borrow"] = (df["fee_annual"] - df["rebate_annual"]).clip(lower=0)

        for _, row in df.iterrows():
            sym = _norm(row["sym"])
            if pd.notna(row["net_borrow"]):
                snap.borrow_map[sym] = float(row["net_borrow"])
            if pd.notna(row["fee_annual"]):
                snap.fee_map[sym] = float(row["fee_annual"])
            if pd.notna(row["rebate_annual"]):
                snap.rebate_map[sym] = float(row["rebate_annual"])
            if pd.notna(row["available_int"]):
                snap.available_map[sym] = int(row["available_int"])

        snap.success = True
        logger.info(f"IBKR FTP: fetched {len(snap.borrow_map)} symbols")

    except Exception as e:
        snap.success = False
        snap.error = str(e)
        logger.error(f"IBKR FTP fetch failed: {e}")

    snap.duration_ms = (time.monotonic() - t0) * 1000
    snap.timestamp = dt.datetime.utcnow()
    return snap


# ──────────────────────────────────────────────
# MOCK Fetcher (for development / testing)
# ──────────────────────────────────────────────
def fetch_mock(
    universe_df: pd.DataFrame,
    seed_from_csv: bool = True,
) -> BorrowSnapshot:
    """
    Generate mock borrow data.
    If seed_from_csv=True, uses the CSV's borrow columns as base values
    with small random perturbations to simulate live updates.
    """
    t0 = time.monotonic()
    snap = BorrowSnapshot()

    for _, row in universe_df.iterrows():
        sym = _norm(str(row.get("symbol", row.get("ETF", ""))))
        if not sym:
            continue

        if seed_from_csv and pd.notna(row.get("borrow_fee_annual")):
            # Use CSV values with ±5% jitter
            jitter = 1.0 + random.uniform(-0.05, 0.05)
            fee = float(row["borrow_fee_annual"]) * jitter
            rebate = float(row.get("borrow_rebate_annual", 0.0) or 0.0) * jitter
            net = max(fee - rebate, 0.0)
            avail = int(row.get("shares_available", 0) or 0)
            # Jitter availability ±10%
            avail = max(0, int(avail * (1 + random.uniform(-0.1, 0.1))))
        else:
            # Fully random
            fee = random.uniform(0.005, 0.15)
            rebate = random.uniform(-0.02, fee * 0.3)
            net = max(fee - rebate, 0.0)
            avail = random.choice([0, 5000, 10000, 50000, 100000, 500000, 1000000])

        snap.fee_map[sym] = round(fee, 6)
        snap.rebate_map[sym] = round(rebate, 6)
        snap.borrow_map[sym] = round(net, 6)
        snap.available_map[sym] = avail

    snap.success = True
    snap.duration_ms = (time.monotonic() - t0) * 1000
    snap.timestamp = dt.datetime.utcnow()
    logger.info(f"Mock fetcher: generated {len(snap.borrow_map)} symbols")
    return snap

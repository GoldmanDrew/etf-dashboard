"""Universe loading, symbol normalization, and bucket assignment."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from backend.models import Bucket

logger = logging.getLogger(__name__)


def norm_sym(s: str) -> str:
    """Normalize symbol: uppercase, replace . with -."""
    return str(s).strip().upper().replace(".", "-")


def load_universe(csv_path: str | Path) -> pd.DataFrame:
    """Load the ETF screened universe CSV and normalize."""
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"Universe file not found: {path}")

    df = pd.read_csv(path)

    # Normalize column names
    col_map = {}
    for c in df.columns:
        cl = c.strip()
        col_map[c] = cl
    df = df.rename(columns=col_map)

    # Normalize symbols
    if "ETF" in df.columns:
        df["symbol"] = df["ETF"].apply(norm_sym)
    if "Underlying" in df.columns:
        df["underlying"] = df["Underlying"].apply(norm_sym)

    # Ensure numeric types
    for col in [
        "Beta", "Leverage", "ExpectedLeverage", "Beta_n_obs",
        "borrow_fee_annual", "borrow_rebate_annual", "borrow_net_annual",
        "shares_available", "borrow_current",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Boolean columns
    for col in [
        "borrow_spiking", "borrow_missing_from_ftp", "protected",
        "cagr_positive", "include_for_algo", "purgatory",
    ]:
        if col in df.columns:
            df[col] = df[col].astype(bool)

    logger.info(f"Loaded universe: {len(df)} ETFs from {path}")
    return df


def load_inverse_etfs(csv_path: str | Path) -> set[str]:
    """Load the curated inverse ETF list (Bucket 3 source of truth)."""
    path = Path(csv_path)
    if not path.exists():
        logger.warning(f"Inverse ETF file not found: {path}")
        return set()

    df = pd.read_csv(path)
    symbols = set()
    for col in ["ETF", "symbol", "Symbol", "Ticker"]:
        if col in df.columns:
            symbols.update(df[col].dropna().apply(norm_sym).tolist())
            break

    logger.info(f"Loaded {len(symbols)} inverse ETFs from {path}")
    return symbols


def assign_buckets(
    universe: pd.DataFrame,
    inverse_set: set[str],
    high_beta_threshold: float = 1.5,
    blacklist: Optional[list[str]] = None,
) -> pd.DataFrame:
    """
    Assign each ETF to a bucket:
      Bucket 3: Symbol in curated inverse ETF list
      Bucket 1: Beta > high_beta_threshold (and not in Bucket 3)
      Bucket 2: Beta <= high_beta_threshold (and not in Bucket 3)
    """
    blacklist_set = set(norm_sym(s) for s in (blacklist or []))
    df = universe.copy()

    # Filter blacklist
    if blacklist_set:
        df = df[~df["symbol"].isin(blacklist_set)].copy()

    def _assign(row):
        sym = row.get("symbol", "")
        if sym in inverse_set:
            return Bucket.INVERSE.value
        beta = row.get("Beta", np.nan)
        if pd.notna(beta) and beta > high_beta_threshold:
            return Bucket.HIGH_BETA.value
        return Bucket.LOW_BETA.value

    df["bucket"] = df.apply(_assign, axis=1)
    counts = df["bucket"].value_counts().to_dict()
    logger.info(f"Bucket assignment: {counts}")
    return df

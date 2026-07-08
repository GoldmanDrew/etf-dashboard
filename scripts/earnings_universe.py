#!/usr/bin/env python3
"""Universe helpers for earnings calendar ingest (B2 + B4 underlyings)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_UNIVERSE_CSV = REPO_ROOT / "data" / "etf_screened_today.csv"
DEFAULT_EARNINGS_BUCKETS = ("bucket_2", "bucket_4")


def _norm_underlying(raw: object) -> str | None:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    s = str(raw).strip().upper()
    if not s or s == "NAN":
        return None
    return s


def load_bucket_underlyings(
    buckets: tuple[str, ...] = DEFAULT_EARNINGS_BUCKETS,
    *,
    csv_path: Path | str | None = None,
) -> list[str]:
    """Return sorted unique underlyings for ETFs in the requested screener buckets."""
    path = Path(csv_path) if csv_path else DEFAULT_UNIVERSE_CSV
    if not path.exists():
        return []
    try:
        df = pd.read_csv(path)
    except Exception:
        return []
    if "bucket" not in df.columns or "Underlying" not in df.columns:
        return []
    sub = df[df["bucket"].astype(str).str.lower().isin({b.lower() for b in buckets})]
    out: set[str] = set()
    for raw in sub["Underlying"].tolist():
        und = _norm_underlying(raw)
        if und:
            out.add(und)
    return sorted(out)


def load_bucket_underlying_etfs(
    buckets: tuple[str, ...] = DEFAULT_EARNINGS_BUCKETS,
    *,
    csv_path: Path | str | None = None,
) -> dict[str, list[str]]:
    """Map underlying -> sorted ETF tickers in the requested buckets."""
    path = Path(csv_path) if csv_path else DEFAULT_UNIVERSE_CSV
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path)
    except Exception:
        return {}
    if "bucket" not in df.columns or "Underlying" not in df.columns or "ETF" not in df.columns:
        return {}
    sub = df[df["bucket"].astype(str).str.lower().isin({b.lower() for b in buckets})]
    out: dict[str, set[str]] = {}
    for _, row in sub.iterrows():
        und = _norm_underlying(row.get("Underlying"))
        etf = _norm_underlying(row.get("ETF"))
        if und and etf:
            out.setdefault(und, set()).add(etf)
    return {k: sorted(v) for k, v in sorted(out.items())}

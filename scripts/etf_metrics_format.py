"""Shared ETF metrics serialization helpers (no provider / ingest dependencies)."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from market_calendar import is_nyse_session

LOGGER = logging.getLogger(__name__)
_ISSUER_EARLY = "issuer_early"


def sanitize_metrics_json_df(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    d["ingested_at_utc"] = pd.to_datetime(d["ingested_at_utc"], errors="coerce", utc=True).astype(str)
    for col in (
        "nav",
        "aum",
        "shares_outstanding",
        "shares_traded",
        "close_price",
        "etf_adj_close",
        "underlying_adj_close",
        "stale_age_bdays",
    ):
        if col in d.columns:
            d[col] = pd.to_numeric(d[col], errors="coerce").replace([np.inf, -np.inf], np.nan)
    return d.astype(object).where(pd.notna(d), None)


def browser_metrics_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Rows safe to expose as completed daily ETF metrics in the browser."""
    if df.empty or "date" not in df.columns:
        return df.copy()
    d = df.copy()
    dates = pd.to_datetime(d["date"], errors="coerce").dt.date
    keep = dates.map(lambda x: bool(x and is_nyse_session(x)))
    if "stale_kind" in d.columns:
        keep &= d["stale_kind"].astype(str).str.lower().ne(_ISSUER_EARLY)
    return d.loc[keep].reset_index(drop=True)


def write_metrics_daily_json(
    df: pd.DataFrame,
    json_path: Path,
    *,
    build_time: datetime | None = None,
) -> Path:
    """Write the browser-facing daily metrics JSON."""
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_rows = sanitize_metrics_json_df(browser_metrics_frame(df))
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "build_time": (build_time or datetime.now(UTC)).isoformat(),
                "rows": json_rows.to_dict("records"),
            },
            f,
            separators=(",", ":"),
            allow_nan=False,
        )
    size_mb = json_path.stat().st_size / (1024 * 1024)
    if size_mb >= 90:
        LOGGER.warning(
            "etf_metrics_daily.json is %.1f MB (GitHub blob limit 100 MB); "
            "keep this file out of git and materialize at Pages deploy",
            size_mb,
        )
    return json_path

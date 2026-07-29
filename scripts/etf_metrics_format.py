"""Shared ETF metrics serialization helpers (no provider / ingest dependencies)."""

from __future__ import annotations

import gzip
import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from market_calendar import is_nyse_session

LOGGER = logging.getLogger(__name__)
_ISSUER_EARLY = "issuer_early"

# Dropped from the SPA panel — audit-only and dominate JSON size.
BROWSER_DROP_COLS = ("source_url", "ingested_at_utc")


def sanitize_metrics_json_df(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["date"] = pd.to_datetime(d["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    if "ingested_at_utc" in d.columns:
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


def prune_browser_audit_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop audit-only columns from the SPA metrics payload."""
    drop = [c for c in BROWSER_DROP_COLS if c in df.columns]
    return df.drop(columns=drop) if drop else df


def metrics_daily_payload(
    df: pd.DataFrame,
    *,
    build_time: datetime | None = None,
) -> dict:
    """Build the browser-facing daily metrics object (pruned columns)."""
    json_rows = prune_browser_audit_columns(sanitize_metrics_json_df(browser_metrics_frame(df)))
    return {
        "build_time": (build_time or datetime.now(UTC)).isoformat(),
        "rows": json_rows.to_dict("records"),
    }


def write_gzip_json(payload: object, gz_path: Path) -> Path:
    """Write compact JSON gzipped to ``gz_path`` (Cloudflare Pages ≤20 MB gate)."""
    gz_path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(payload, separators=(",", ":"), allow_nan=False).encode("utf-8")
    with gzip.open(gz_path, "wb", compresslevel=9) as f:
        f.write(raw)
    return gz_path


def write_metrics_daily_json(
    df: pd.DataFrame,
    json_path: Path,
    *,
    build_time: datetime | None = None,
    also_gzip: bool = True,
) -> Path:
    """Write the browser-facing daily metrics JSON (and optional ``.json.gz``).

    Plain JSON can exceed Cloudflare Pages' ~25 MiB upload limit; deploy ships
    the gzip sibling. Uncompressed remains useful for local ``http.server``.
    """
    json_path.parent.mkdir(parents=True, exist_ok=True)
    payload = metrics_daily_payload(df, build_time=build_time)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), allow_nan=False)
    size_mb = json_path.stat().st_size / (1024 * 1024)
    if size_mb >= 90:
        LOGGER.warning(
            "etf_metrics_daily.json is %.1f MB (GitHub blob limit 100 MB); "
            "keep this file out of git and materialize at Pages deploy",
            size_mb,
        )
    if also_gzip:
        gz_path = Path(str(json_path) + ".gz")
        write_gzip_json(payload, gz_path)
        gz_mb = gz_path.stat().st_size / (1024 * 1024)
        LOGGER.info("Wrote %s (%.1f MB gzip)", gz_path.name, gz_mb)
        if gz_mb >= 20:
            LOGGER.warning(
                "%s is %.1f MB — Cloudflare Pages rejects files ≥25 MiB "
                "(deploy budget 20 MB); prune further or shard",
                gz_path.name,
                gz_mb,
            )
    return json_path


def gzip_json_file(src: Path, dest: Path | None = None) -> Path:
    """Gzip an existing JSON file to ``dest`` (default ``src`` + ``.gz``)."""
    if not src.is_file():
        raise FileNotFoundError(src)
    dest = dest or Path(str(src) + ".gz")
    payload = json.loads(src.read_text(encoding="utf-8"))
    return write_gzip_json(payload, dest)

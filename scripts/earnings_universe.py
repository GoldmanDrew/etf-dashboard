#!/usr/bin/env python3
"""Universe helpers for earnings calendar ingest (B2 YieldBOOST + B4 underlyings)."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

SCRIPTS = Path(__file__).resolve().parent
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from product_taxonomy import yieldboost_fof_symbols, yieldboost_income_pairs  # noqa: E402

REPO_ROOT = SCRIPTS.parent
DEFAULT_UNIVERSE_CSV = REPO_ROOT / "data" / "etf_screened_today.csv"
DEFAULT_EARNINGS_BUCKETS = ("bucket_2", "bucket_4")
_BUCKET_2 = "bucket_2"

# Index / fund / commodity ETF tickers used as YieldBOOST "underlyings". These
# products do not report corporate earnings — seed_quarterly dates for them are
# legacy vol-shock placeholders and must not appear in ops emails or calendars.
EARNINGS_INELIGIBLE_UNDERLYINGS = frozenset({
    "DIA", "ETHA", "EWJ", "GDX", "GLD", "IBIT", "IWM", "QQQ", "SLV", "SOXX",
    "SPY", "SPHB", "TLT", "USO", "XBI", "XLE", "XLF", "XLK", "XOP",
})

_YB_PAIRS = yieldboost_income_pairs()
_YB_FOF = yieldboost_fof_symbols()


def is_earnings_eligible_underlying(symbol: str) -> bool:
    """True when the ticker is a single-name equity with a real earnings cycle."""
    return str(symbol or "").strip().upper() not in EARNINGS_INELIGIBLE_UNDERLYINGS


def _norm_sym(raw: object) -> str | None:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    s = str(raw).strip().upper()
    if not s or s == "NAN":
        return None
    return s


def _norm_underlying(raw: object) -> str | None:
    return _norm_sym(raw)


def _truthy(raw: object) -> bool:
    if raw is True:
        return True
    if isinstance(raw, (int, float)) and raw == 1:
        return True
    return str(raw or "").strip().lower() in {"1", "true", "yes"}


def is_yieldboost_bucket2_row(row: pd.Series) -> bool:
    """Bucket 2 includes only YieldBOOST income (+ FoF), not passive low-β names."""
    bucket = str(row.get("bucket") or "").strip().lower()
    if bucket != _BUCKET_2:
        return True

    if _truthy(row.get("is_yieldboost")):
        return True

    product_class = str(row.get("product_class") or "").strip().lower()
    if product_class in {"income_yieldboost", "income_yieldboost_fof"}:
        return True

    etf = _norm_sym(row.get("ETF"))
    und = _norm_sym(row.get("Underlying"))
    if etf and und and (etf, und) in _YB_PAIRS:
        return True
    return bool(etf and etf in _YB_FOF)


def _earnings_scope_rows(df: pd.DataFrame, buckets: tuple[str, ...]) -> pd.DataFrame:
    want = {b.lower() for b in buckets}
    sub = df[df["bucket"].astype(str).str.lower().isin(want)].copy()
    if _BUCKET_2 in want or "bucket_2" in want:
        mask = sub.apply(is_yieldboost_bucket2_row, axis=1)
        sub = sub[mask]
    return sub


def load_bucket_underlyings(
    buckets: tuple[str, ...] = DEFAULT_EARNINGS_BUCKETS,
    *,
    csv_path: Path | str | None = None,
) -> list[str]:
    """Return sorted unique underlyings for B2 YieldBOOST + B4 ETFs in the screener."""
    path = Path(csv_path) if csv_path else DEFAULT_UNIVERSE_CSV
    if not path.exists():
        return []
    try:
        df = pd.read_csv(path)
    except Exception:
        return []
    if "bucket" not in df.columns or "Underlying" not in df.columns:
        return []
    sub = _earnings_scope_rows(df, buckets)
    out: set[str] = set()
    for raw in sub["Underlying"].tolist():
        und = _norm_underlying(raw)
        if und:
            out.add(und)
    return sorted(u for u in out if is_earnings_eligible_underlying(u))


def load_bucket_underlying_etfs(
    buckets: tuple[str, ...] = DEFAULT_EARNINGS_BUCKETS,
    *,
    csv_path: Path | str | None = None,
) -> dict[str, list[str]]:
    """Map underlying -> sorted ETF tickers (B2 YieldBOOST-only, all B4)."""
    path = Path(csv_path) if csv_path else DEFAULT_UNIVERSE_CSV
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path)
    except Exception:
        return {}
    if "bucket" not in df.columns or "Underlying" not in df.columns or "ETF" not in df.columns:
        return {}
    sub = _earnings_scope_rows(df, buckets)
    out: dict[str, set[str]] = {}
    for _, row in sub.iterrows():
        und = _norm_underlying(row.get("Underlying"))
        etf = _norm_sym(row.get("ETF"))
        if und and etf:
            out.setdefault(und, set()).add(etf)
    return {
        k: sorted(v)
        for k, v in sorted(out.items())
        if is_earnings_eligible_underlying(k)
    }

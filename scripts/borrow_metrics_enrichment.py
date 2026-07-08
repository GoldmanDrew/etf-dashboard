#!/usr/bin/env python3
"""Join ETF metrics supply/scale features onto borrow history panels."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"


def load_etf_metrics_daily(data_dir: Path | None = None) -> pd.DataFrame:
    data_dir = data_dir or DATA_DIR
    pq = data_dir / "etf_metrics_daily.parquet"
    csv = data_dir / "etf_metrics_daily.csv"
    if pq.exists():
        df = pd.read_parquet(pq)
    elif csv.exists():
        df = pd.read_csv(csv)
    else:
        return pd.DataFrame()
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    sym_col = "ticker" if "ticker" in df.columns else "symbol"
    df["symbol"] = df[sym_col].astype(str).str.upper()
    return df.dropna(subset=["date", "symbol"])


def _median_shares_traded(metrics: pd.DataFrame, window: int = 20) -> pd.Series:
    st = pd.to_numeric(metrics.get("shares_traded"), errors="coerce")
    if st is None:
        return pd.Series(dtype=float)
    g = metrics.groupby("symbol", sort=False)
    return g["shares_traded"].transform(
        lambda s: pd.to_numeric(s, errors="coerce").rolling(window, min_periods=3).median()
    )


def enrich_metrics_features(metrics: pd.DataFrame) -> pd.DataFrame:
    if metrics.empty:
        return metrics
    out = metrics.copy()
    so = pd.to_numeric(out.get("shares_outstanding"), errors="coerce")
    aum = pd.to_numeric(out.get("aum"), errors="coerce")
    nav = pd.to_numeric(out.get("nav"), errors="coerce")
    close = pd.to_numeric(out.get("close_price"), errors="coerce")
    implied_aum = nav * so
    use_implied = (~aum.notna() | (aum <= 0)) & implied_aum.notna() & (implied_aum > 0)
    out["aum_filled"] = aum.where(~use_implied, implied_aum)
    out["log_aum"] = np.log1p(out["aum_filled"].clip(lower=0))
    med_st = _median_shares_traded(out, 20)
    out["turnover_20d"] = med_st / so.replace(0, np.nan)
    out["prem_disc_bps"] = (close - nav) / nav.replace(0, np.nan) * 10000.0
    return out


def join_supply_to_panel(panel: pd.DataFrame, metrics: pd.DataFrame | None = None) -> pd.DataFrame:
    """Add utilization_proxy, avail_to_adv, log_aum, turnover_20d to a borrow panel."""
    if panel.empty:
        return panel
    work = panel.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work["symbol"] = work["symbol"].astype(str).str.upper()
    if metrics is None:
        metrics = load_etf_metrics_daily()
    if metrics.empty:
        for c in ("utilization_proxy", "avail_to_adv", "log_aum", "turnover_20d", "prem_disc_bps"):
            work[c] = np.nan
        work["supply_data_grade"] = "missing_metrics"
        return work

    m = enrich_metrics_features(metrics)
    keep = ["date", "symbol", "shares_outstanding", "log_aum", "turnover_20d", "prem_disc_bps"]
    m = m[keep].drop_duplicates(subset=["date", "symbol"], keep="last")
    work = work.merge(m, on=["date", "symbol"], how="left")
    sa = pd.to_numeric(work.get("shares_available"), errors="coerce")
    so = pd.to_numeric(work.get("shares_outstanding"), errors="coerce")
    work["utilization_proxy"] = (1.0 - (sa / so.replace(0, np.nan))).clip(0, 1)
    med_st = m.groupby("symbol")["turnover_20d"].transform(lambda x: x)
    # avail_to_adv: shares_available / median daily volume
    if "shares_traded" in metrics.columns:
        st_med = metrics.copy()
        st_med["date"] = pd.to_datetime(st_med["date"], errors="coerce")
        st_med["symbol"] = st_med["ticker" if "ticker" in st_med.columns else "symbol"].astype(str).str.upper()
        st_med["st_med20"] = st_med.groupby("symbol")["shares_traded"].transform(
            lambda s: pd.to_numeric(s, errors="coerce").rolling(20, min_periods=3).median()
        )
        work = work.merge(
            st_med[["date", "symbol", "st_med20"]].drop_duplicates(["date", "symbol"]),
            on=["date", "symbol"],
            how="left",
        )
        work["avail_to_adv"] = sa / work["st_med20"].replace(0, np.nan)
        work = work.drop(columns=["st_med20"], errors="ignore")
    else:
        work["avail_to_adv"] = np.nan

    has_so = so.notna() & (so > 0)
    work["supply_data_grade"] = np.where(
        has_so,
        "full",
        np.where(sa.notna(), "shares_only", "missing"),
    )
    return work


def latest_supply_features_for_symbol(
    sym: str,
    as_of_date: str,
    *,
    hist_shares_available: float | None,
    metrics: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """Point-in-time supply features for production scoring."""
    sym = str(sym).upper()
    out: dict[str, Any] = {
        "utilization_proxy": 0.0,
        "avail_to_adv": 0.0,
        "log_aum": 0.0,
        "turnover_20d": 0.0,
        "prem_disc_bps": 0.0,
        "supply_data_grade": "missing",
    }
    if metrics is None:
        metrics = load_etf_metrics_daily()
    if metrics.empty:
        return out
    m = enrich_metrics_features(metrics)
    m = m[m["symbol"] == sym].sort_values("date")
    if m.empty:
        return out
    as_of = pd.Timestamp(as_of_date)
    row = m[m["date"] <= as_of].tail(1)
    if row.empty:
        row = m.tail(1)
    r = row.iloc[-1]
    so = float(r.get("shares_outstanding") or 0)
    sa = float(hist_shares_available or 0)
    if so > 0 and sa >= 0:
        out["utilization_proxy"] = float(np.clip(1.0 - sa / so, 0, 1))
        out["supply_data_grade"] = "full"
    elif sa > 0:
        out["supply_data_grade"] = "shares_only"
    out["log_aum"] = float(r.get("log_aum") or 0)
    out["turnover_20d"] = float(r.get("turnover_20d") or 0) if pd.notna(r.get("turnover_20d")) else 0.0
    out["prem_disc_bps"] = float(r.get("prem_disc_bps") or 0) if pd.notna(r.get("prem_disc_bps")) else 0.0
    return out

#!/usr/bin/env python3
"""Join ETF metrics supply/scale features onto borrow history panels."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"

SUPPLY_ASOF_TOLERANCE_DAYS = 45
SUPPLY_FFILL_LIMIT = 90


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
    df["date"] = pd.to_datetime(df["date"], errors="coerce").astype("datetime64[ns]")
    sym_col = "ticker" if "ticker" in df.columns else "symbol"
    df["symbol"] = df[sym_col].astype(str).str.upper()
    return df.dropna(subset=["date", "symbol"])


def load_latest_shares_outstanding_map(data_dir: Path | None = None) -> dict[str, float]:
    """Per-symbol latest known shares_outstanding from metrics snapshots."""
    data_dir = data_dir or DATA_DIR
    out: dict[str, float] = {}
    metrics = load_etf_metrics_daily(data_dir)
    if not metrics.empty and "shares_outstanding" in metrics.columns:
        m = metrics.copy()
        m["_so"] = pd.to_numeric(m["shares_outstanding"], errors="coerce")
        m = m[m["_so"] > 0].sort_values(["symbol", "date"])
        for sym, grp in m.groupby("symbol", sort=False):
            out[str(sym).upper()] = float(grp["_so"].iloc[-1])

    latest_path = data_dir / "etf_metrics_latest.json"
    if latest_path.exists():
        try:
            payload = json.loads(latest_path.read_text(encoding="utf-8"))
            rows = payload.get("rows") or payload.get("records") or []
            if isinstance(rows, list):
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    sym = str(row.get("ticker") or row.get("symbol") or "").upper()
                    so = pd.to_numeric(row.get("shares_outstanding"), errors="coerce")
                    if sym and pd.notna(so) and float(so) > 0:
                        out[sym] = float(so)
        except Exception:
            pass

    dash_path = data_dir / "dashboard_data.json"
    if dash_path.exists():
        try:
            payload = json.loads(dash_path.read_text(encoding="utf-8"))
            for row in payload.get("rows") or payload.get("records") or []:
                if not isinstance(row, dict):
                    continue
                sym = str(row.get("symbol") or "").upper()
                so = pd.to_numeric(row.get("shares_outstanding"), errors="coerce")
                if sym and pd.notna(so) and float(so) > 0:
                    out.setdefault(sym, float(so))
        except Exception:
            pass
    return out


def _median_shares_traded(metrics: pd.DataFrame, window: int = 20) -> pd.Series:
    st = pd.to_numeric(metrics.get("shares_traded"), errors="coerce")
    if st is None:
        return pd.Series(dtype=float)
    return metrics.groupby("symbol", sort=False)["shares_traded"].transform(
        lambda s: pd.to_numeric(s, errors="coerce").rolling(window, min_periods=3).median()
    )


def enrich_metrics_features(metrics: pd.DataFrame) -> pd.DataFrame:
    if metrics.empty:
        return metrics
    out = metrics.copy()
    if "symbol" not in out.columns:
        sym_col = "ticker" if "ticker" in out.columns else None
        if sym_col:
            out["symbol"] = out[sym_col].astype(str).str.upper()
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
    out["st_med20"] = med_st
    return out


def prepare_metrics_supply_frame(
    metrics: pd.DataFrame,
    *,
    latest_so_map: dict[str, float] | None = None,
) -> pd.DataFrame:
    if metrics.empty:
        return metrics
    m = enrich_metrics_features(metrics).sort_values(["symbol", "date"]).copy()
    m["shares_outstanding"] = (
        m.groupby("symbol", sort=False)["shares_outstanding"]
        .transform(lambda s: pd.to_numeric(s, errors="coerce").ffill(limit=SUPPLY_FFILL_LIMIT))
    )
    m["shares_outstanding"] = (
        m.groupby("symbol", sort=False)["shares_outstanding"]
        .transform(lambda s: pd.to_numeric(s, errors="coerce").bfill(limit=30))
    )
    if latest_so_map:
        for sym, so_val in latest_so_map.items():
            mask = (m["symbol"] == sym) & (m["shares_outstanding"].isna() | (m["shares_outstanding"] <= 0))
            if mask.any():
                m.loc[mask, "shares_outstanding"] = float(so_val)
    return m


def _merge_supply_asof(panel: pd.DataFrame, metrics: pd.DataFrame) -> pd.DataFrame:
    if panel.empty:
        return panel
    keep_cols = ["date", "symbol", "shares_outstanding", "log_aum", "turnover_20d", "prem_disc_bps", "st_med20"]
    mcols = [c for c in keep_cols if c in metrics.columns]
    if not mcols or "date" not in mcols:
        return panel

    tol = pd.Timedelta(days=SUPPLY_ASOF_TOLERANCE_DAYS)
    parts: list[pd.DataFrame] = []
    for sym, grp in panel.groupby("symbol", sort=False):
        grp = grp.sort_values("date").copy()
        grp["date"] = pd.to_datetime(grp["date"]).astype("datetime64[ns]")
        mm = metrics[metrics["symbol"] == sym].sort_values("date")
        if mm.empty:
            parts.append(grp)
            continue
        right_cols = [c for c in mcols if c != "symbol"]
        mm_right = mm[right_cols].drop_duplicates("date", keep="last").copy()
        mm_right["date"] = pd.to_datetime(mm_right["date"]).astype("datetime64[ns]")
        merged = pd.merge_asof(
            grp,
            mm_right,
            on="date",
            direction="backward",
            tolerance=tol,
        )
        parts.append(merged)
    if not parts:
        return panel
    return pd.concat(parts, ignore_index=True).sort_values(["date", "symbol"]).reset_index(drop=True)


def join_supply_to_panel(
    panel: pd.DataFrame,
    metrics: pd.DataFrame | None = None,
    *,
    latest_so_map: dict[str, float] | None = None,
) -> pd.DataFrame:
    if panel.empty:
        return panel
    work = panel.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").astype("datetime64[ns]")
    work["symbol"] = work["symbol"].astype(str).str.upper()
    if metrics is None:
        metrics = load_etf_metrics_daily()
    if metrics.empty:
        for c in ("utilization_proxy", "avail_to_adv", "log_aum", "turnover_20d", "prem_disc_bps"):
            work[c] = np.nan
        work["supply_data_grade"] = "missing_metrics"
        return work

    if latest_so_map is None:
        latest_so_map = load_latest_shares_outstanding_map()
    m = prepare_metrics_supply_frame(metrics, latest_so_map=latest_so_map)
    work = _merge_supply_asof(work, m)

    sa = pd.to_numeric(work.get("shares_available"), errors="coerce")
    so = pd.to_numeric(work.get("shares_outstanding"), errors="coerce")
    if latest_so_map:
        for sym, fallback in latest_so_map.items():
            if not fallback or fallback <= 0:
                continue
            mask = (work["symbol"] == sym) & (so.isna() | (so <= 0))
            if mask.any():
                work.loc[mask, "shares_outstanding"] = float(fallback)

    so = pd.to_numeric(work.get("shares_outstanding"), errors="coerce")
    work["utilization_proxy"] = (1.0 - (sa / so.replace(0, np.nan))).clip(0, 1)
    st_med = pd.to_numeric(work.get("st_med20"), errors="coerce")
    work["avail_to_adv"] = sa / st_med.replace(0, np.nan) if st_med.notna().any() else np.nan
    work = work.drop(columns=["st_med20"], errors="ignore")

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
    latest_so_map: dict[str, float] | None = None,
) -> dict[str, Any]:
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
    if latest_so_map is None:
        latest_so_map = load_latest_shares_outstanding_map()
    m = prepare_metrics_supply_frame(metrics, latest_so_map=latest_so_map)
    m = m[m["symbol"] == sym].sort_values("date")
    if m.empty:
        return out
    as_of = pd.Timestamp(as_of_date)
    row = m[m["date"] <= as_of].tail(1)
    if row.empty:
        row = m.tail(1)
    r = row.iloc[-1]
    so = float(r.get("shares_outstanding") or 0)
    if so <= 0:
        so = float(latest_so_map.get(sym) or 0)
    sa = float(hist_shares_available or 0)
    if so > 0 and sa >= 0:
        out["utilization_proxy"] = float(np.clip(1.0 - sa / so, 0, 1))
        out["supply_data_grade"] = "full"
    elif sa > 0:
        out["supply_data_grade"] = "shares_only"
    out["log_aum"] = float(r.get("log_aum") or 0)
    out["turnover_20d"] = float(r.get("turnover_20d") or 0) if pd.notna(r.get("turnover_20d")) else 0.0
    out["prem_disc_bps"] = float(r.get("prem_disc_bps") or 0) if pd.notna(r.get("prem_disc_bps")) else 0.0
    st_med = float(r.get("st_med20") or 0) if pd.notna(r.get("st_med20")) else 0.0
    if st_med > 0 and sa > 0:
        out["avail_to_adv"] = float(sa / st_med)
    return out


def utilization_coverage_fraction(panel: pd.DataFrame) -> float:
    if panel.empty or "utilization_proxy" not in panel.columns:
        return 0.0
    so = pd.to_numeric(panel.get("shares_outstanding"), errors="coerce")
    util = pd.to_numeric(panel["utilization_proxy"], errors="coerce")
    ok = so.notna() & (so > 0) & util.notna()
    return round(float(ok.mean()), 4)

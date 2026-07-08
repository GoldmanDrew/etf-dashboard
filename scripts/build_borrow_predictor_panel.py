#!/usr/bin/env python3
"""Build (date, symbol) borrow predictor study panel with supply/float/peer features."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from borrow_metrics_enrichment import join_supply_to_panel, utilization_coverage_fraction  # noqa: E402
from borrow_spike_model import (  # noqa: E402
    FEATURE_COLS,
    _symbol_history_frame,
    apply_spike_labels,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
BORROW_HISTORY_FILE = DATA_DIR / "borrow_history.json"
ETF_METRICS_PARQUET = DATA_DIR / "etf_metrics_daily.parquet"
ETF_METRICS_CSV = DATA_DIR / "etf_metrics_daily.csv"
SCREENER_CSV = DATA_DIR / "etf_screened_today.csv"
REBALANCE_FLOWS_PARQUET = DATA_DIR / "letf_rebalance_flows_daily.parquet"
DASHBOARD_JSON = DATA_DIR / "dashboard_data.json"
PANEL_PARQUET = DATA_DIR / "borrow_predictor_panel.parquet"
PANEL_META_JSON = DATA_DIR / "borrow_predictor_panel_meta.json"

TARGET_HORIZONS = (1, 3, 5, 10)

BORROW_DYNAMICS_COLS = [
    "borrow_current",
    "borrow_z60",
    "borrow_slope5",
    "borrow_vol10",
    "borrow_pctile_60",
]

SUPPLY_COLS = [
    "shares_available",
    "shares_drop1",
    "shares_drop3",
    "shares_drop5",
    "utilization_proxy",
    "avail_to_adv",
]

SCALE_COLS = [
    "log_aum",
    "turnover_20d",
    "prem_disc_bps",
]

FLOAT_COLS = [
    "tradable_float_shares",
    "etf_aum_over_float",
    "rebalance_pct_adv",
]

PEER_COLS = [
    "peer_borrow_z_mean",
    "peer_shares_drop3_mean",
    "peer_shares_avail_sum",
]

SCREENER_COLS = [
    "delta",
    "leverage",
    "net_edge_p50",
    "gross_decay_annual",
    "forecast_vol_underlying_annual",
    "product_class",
    "bucket",
    "underlying",
]

ALL_FEATURE_COLS = (
    BORROW_DYNAMICS_COLS
    + SUPPLY_COLS
    + SCALE_COLS
    + FLOAT_COLS
    + PEER_COLS
    + [c for c in SCREENER_COLS if c not in ("product_class", "underlying")]
)


def _load_borrow_history(path: Path) -> dict[str, list[dict]]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    syms = payload.get("symbols") or {}
    return syms if isinstance(syms, dict) else {}


def _load_etf_metrics(path_parquet: Path, path_csv: Path) -> pd.DataFrame:
    if path_parquet.exists():
        df = pd.read_parquet(path_parquet)
    elif path_csv.exists():
        df = pd.read_csv(path_csv)
    else:
        return pd.DataFrame()
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    sym_col = "ticker" if "ticker" in df.columns else "symbol"
    df["symbol"] = df[sym_col].astype(str).str.upper()
    return df.dropna(subset=["date", "symbol"])


def _load_screener(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    sym_col = "ETF" if "ETF" in df.columns else ("symbol" if "symbol" in df.columns else None)
    if sym_col is None:
        return pd.DataFrame()
    out = df.copy()
    out["symbol"] = out[sym_col].astype(str).str.upper()
    und_col = "Underlying" if "Underlying" in out.columns else "underlying"
    if und_col in out.columns:
        out["underlying"] = out[und_col].astype(str).str.upper()
    rename = {
        "Delta": "delta",
        "Leverage": "leverage",
        "net_edge_p50_annual": "net_edge_p50",
    }
    for src, dst in rename.items():
        if src in out.columns and dst not in out.columns:
            out[dst] = out[src]
    if "leverage" not in out.columns and "expected_leverage" in out.columns:
        out["leverage"] = out["expected_leverage"]
    if "forecast_vol_underlying_annual" not in out.columns and "vol_underlying_annual" in out.columns:
        out["forecast_vol_underlying_annual"] = out["vol_underlying_annual"]
    return out.drop_duplicates(subset=["symbol"], keep="last")


def _load_dashboard_forecast_vol(path: Path) -> dict[str, float]:
    if not path.exists():
        return {}
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}
    out: dict[str, float] = {}
    for row in payload.get("rows") or []:
        sym = str(row.get("symbol") or "").upper()
        fv = row.get("forecast_vol_underlying_annual")
        if sym and fv is not None and np.isfinite(fv):
            out[sym] = float(fv)
    return out


def _load_rebalance_flows(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_parquet(path)
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    sym_col = "ticker" if "ticker" in df.columns else "symbol"
    df["symbol"] = df[sym_col].astype(str).str.upper()
    return df.dropna(subset=["date", "symbol"])


def _add_borrow_pctile_60(s: pd.DataFrame) -> pd.DataFrame:
    out = s.copy()
    borrow = out["borrow_current"].astype(float)

    def _pctile(win: pd.Series) -> float:
        if win.empty:
            return np.nan
        cur = win.iloc[-1]
        if pd.isna(cur):
            return np.nan
        return float((win <= float(cur)).mean())

    out["borrow_pctile_60"] = borrow.rolling(60, min_periods=10).apply(_pctile, raw=False)
    return out


def _compute_targets(s: pd.DataFrame, horizons: tuple[int, ...]) -> pd.DataFrame:
    out = s.copy()
    borrow = out["borrow_current"].astype(float)
    for h in horizons:
        fut_borrow = borrow.shift(-h)
        out[f"delta_borrow_{h}"] = fut_borrow - borrow
        fut_max = borrow.shift(-1).rolling(h, min_periods=1).max().shift(-(h - 1))
        out[f"max_borrow_jump_{h}"] = fut_max - borrow
        labeled = apply_spike_labels(out, horizon_days=h, label_variant="L0")
        out[f"y_spike_{h}"] = labeled["spike_event"]
    return out


def _symbol_base_rows(sym: str, hist: list[dict]) -> pd.DataFrame:
    s = _symbol_history_frame(hist)
    if len(s) < 12:
        return pd.DataFrame()
    s = _add_borrow_pctile_60(s)
    s = _compute_targets(s, TARGET_HORIZONS)
    s["symbol"] = str(sym).upper()
    s["date"] = s["date"].dt.normalize()
    keep = ["symbol", "date"] + list(FEATURE_COLS) + BORROW_DYNAMICS_COLS
    for h in TARGET_HORIZONS:
        keep.extend([f"delta_borrow_{h}", f"max_borrow_jump_{h}", f"y_spike_{h}"])
    keep = list(dict.fromkeys(keep))
    for c in keep:
        if c not in s.columns:
            s[c] = np.nan
    return s[keep].copy()


def _enrich_metrics(panel: pd.DataFrame, metrics: pd.DataFrame) -> pd.DataFrame:
    if panel.empty:
        return panel
    return join_supply_to_panel(panel, metrics)


def _enrich_screener(panel: pd.DataFrame, screener: pd.DataFrame, forecast_vol: dict[str, float]) -> pd.DataFrame:
    if panel.empty:
        return panel
    if screener.empty:
        out = panel.copy()
        for c in SCREENER_COLS:
            if c not in out.columns:
                out[c] = np.nan if c not in ("product_class", "underlying") else None
        return out
    pick = ["symbol"] + [c for c in SCREENER_COLS if c in screener.columns]
    s = screener[pick].drop_duplicates(subset=["symbol"], keep="last")
    out = panel.merge(s, on="symbol", how="left")
    if forecast_vol:
        fv = out["symbol"].map(forecast_vol)
        base = out.get("forecast_vol_underlying_annual")
        if base is None:
            out["forecast_vol_underlying_annual"] = fv
        else:
            out["forecast_vol_underlying_annual"] = base.where(base.notna(), fv)
    return out


def _enrich_float(panel: pd.DataFrame, flows: pd.DataFrame) -> pd.DataFrame:
    if panel.empty or flows.empty:
        for c in FLOAT_COLS:
            if c not in panel.columns:
                panel[c] = np.nan
        return panel
    f = flows.copy()
    rename = {
        "rebalance_pct_adv_20d": "rebalance_pct_adv",
    }
    for src, dst in rename.items():
        if src in f.columns:
            f[dst] = f[src]
    keep = ["date", "symbol", "tradable_float_shares", "rebalance_pct_adv"]
    f = f[[c for c in keep if c in f.columns]].drop_duplicates(subset=["date", "symbol"], keep="last")
    out = panel.merge(f, on=["date", "symbol"], how="left", suffixes=("", "_flow"))
    if "tradable_float_shares_flow" in out.columns:
        out["tradable_float_shares"] = out["tradable_float_shares"].where(
            out["tradable_float_shares"].notna(),
            out["tradable_float_shares_flow"],
        )
        out = out.drop(columns=["tradable_float_shares_flow"], errors="ignore")
    if "rebalance_pct_adv_flow" in out.columns:
        out["rebalance_pct_adv"] = out["rebalance_pct_adv"].where(
            out["rebalance_pct_adv"].notna(),
            out["rebalance_pct_adv_flow"],
        )
        out = out.drop(columns=["rebalance_pct_adv_flow"], errors="ignore")
    aum = out.get("aum")
    tfs = out.get("tradable_float_shares")
    if aum is not None and tfs is not None:
        denom = tfs.astype(float).clip(lower=1.0)
        out["etf_aum_over_float"] = aum.astype(float) / denom
    else:
        out["etf_aum_over_float"] = np.nan
    return out


def _compute_peer_features(panel: pd.DataFrame) -> pd.DataFrame:
    if panel.empty or "underlying" not in panel.columns:
        for c in PEER_COLS:
            panel[c] = np.nan
        return panel
    out = panel.copy()
    out["underlying"] = out["underlying"].astype(str).str.upper()
    peer_frames: list[pd.DataFrame] = []
    for dt, grp in out.groupby("date"):
        if len(grp) < 2:
            continue
        for und, sub in grp.groupby("underlying"):
            if pd.isna(und) or str(und).upper() in ("", "NAN", "NONE"):
                continue
            if len(sub) < 2:
                continue
            for _, row in sub.iterrows():
                peers = sub[sub["symbol"] != row["symbol"]]
                if peers.empty:
                    continue
                peer_frames.append(
                    {
                        "date": dt,
                        "symbol": row["symbol"],
                        "peer_borrow_z_mean": float(peers["borrow_z60"].mean()),
                        "peer_shares_drop3_mean": float(peers["shares_drop3"].mean()),
                        "peer_shares_avail_sum": float(peers["shares_available"].sum()),
                    }
                )
    if not peer_frames:
        for c in PEER_COLS:
            out[c] = np.nan
        return out
    peers_df = pd.DataFrame(peer_frames)
    out = out.merge(peers_df, on=["date", "symbol"], how="left", suffixes=("", "_peer"))
    for c in PEER_COLS:
        peer_c = f"{c}_peer"
        if peer_c in out.columns:
            out[c] = out[c].where(out[c].notna(), out[peer_c])
            out = out.drop(columns=[peer_c], errors="ignore")
    return out


def build_borrow_predictor_panel(
    *,
    repo_root: Path = REPO_ROOT,
    as_of_max: str | None = None,
) -> pd.DataFrame:
    data = repo_root / "data"
    borrow_symbols = _load_borrow_history(data / "borrow_history.json")
    metrics = _load_etf_metrics(data / "etf_metrics_daily.parquet", data / "etf_metrics_daily.csv")
    screener = _load_screener(data / "etf_screened_today.csv")
    flows = _load_rebalance_flows(data / "letf_rebalance_flows_daily.parquet")
    forecast_vol = _load_dashboard_forecast_vol(data / "dashboard_data.json")

    frames: list[pd.DataFrame] = []
    for sym, hist in (borrow_symbols or {}).items():
        if not hist:
            continue
        chunk = _symbol_base_rows(str(sym).upper(), hist)
        if not chunk.empty:
            frames.append(chunk)
    if not frames:
        return pd.DataFrame()

    panel = pd.concat(frames, ignore_index=True)
    panel["date"] = pd.to_datetime(panel["date"], errors="coerce")
    panel = panel.dropna(subset=["date"]).sort_values(["date", "symbol"]).reset_index(drop=True)
    if as_of_max:
        panel = panel[panel["date"] <= pd.Timestamp(as_of_max)].reset_index(drop=True)

    panel = _enrich_metrics(panel, metrics)
    panel = _enrich_screener(panel, screener, forecast_vol)
    panel = _enrich_float(panel, flows)
    panel = _compute_peer_features(panel)
    return panel


def panel_meta(panel: pd.DataFrame) -> dict[str, Any]:
    target_cols = []
    for h in TARGET_HORIZONS:
        target_cols.extend([f"delta_borrow_{h}", f"max_borrow_jump_{h}", f"y_spike_{h}"])
    feature_cov: dict[str, float] = {}
    for c in ALL_FEATURE_COLS:
        if c in panel.columns:
            feature_cov[c] = round(float(panel[c].notna().mean()), 4)
    feature_cov["utilization_proxy"] = utilization_coverage_fraction(panel)
    return {
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "grain": ["date", "symbol"],
        "rows": int(len(panel)),
        "symbols": int(panel["symbol"].nunique()) if not panel.empty else 0,
        "date_min": str(panel["date"].min().date()) if not panel.empty else None,
        "date_max": str(panel["date"].max().date()) if not panel.empty else None,
        "target_horizons": list(TARGET_HORIZONS),
        "target_columns": target_cols,
        "feature_blocks": {
            "borrow_dynamics": BORROW_DYNAMICS_COLS,
            "supply": SUPPLY_COLS,
            "scale": SCALE_COLS,
            "float": FLOAT_COLS,
            "peer": PEER_COLS,
            "screener": [c for c in SCREENER_COLS if c not in ("product_class", "underlying")],
        },
        "feature_coverage": feature_cov,
        "label_variant": "L0",
        "inputs": {
            "borrow_history": str(BORROW_HISTORY_FILE.relative_to(REPO_ROOT)),
            "etf_metrics": "etf_metrics_daily.parquet|csv",
            "screener": str(SCREENER_CSV.relative_to(REPO_ROOT)),
            "rebalance_flows": str(REBALANCE_FLOWS_PARQUET.relative_to(REPO_ROOT)),
        },
    }


def write_panel_outputs(
    panel: pd.DataFrame,
    *,
    parquet_path: Path = PANEL_PARQUET,
    meta_path: Path = PANEL_META_JSON,
) -> None:
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    panel.to_parquet(parquet_path, index=False)
    meta_path.write_text(json.dumps(panel_meta(panel), indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build borrow predictor study panel.")
    parser.add_argument("--repo-root", type=str, default=str(REPO_ROOT))
    parser.add_argument("--as-of-max", type=str, default=None)
    parser.add_argument("--fail-if-empty", action="store_true")
    args = parser.parse_args()

    repo_root = Path(args.repo_root)
    panel = build_borrow_predictor_panel(repo_root=repo_root, as_of_max=args.as_of_max)
    if panel.empty:
        if args.fail_if_empty:
            raise SystemExit("borrow predictor panel is empty")
        print("borrow predictor panel is empty — no output written")
        return

    out_parquet = repo_root / "data" / "borrow_predictor_panel.parquet"
    out_meta = repo_root / "data" / "borrow_predictor_panel_meta.json"
    write_panel_outputs(panel, parquet_path=out_parquet, meta_path=out_meta)
    print(f"wrote {len(panel)} rows -> {out_parquet}")


if __name__ == "__main__":
    main()

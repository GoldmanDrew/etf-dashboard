"""Price loading for Bucket 4 backtests from etf-dashboard metrics."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
MIN_PRICE_PANEL_DAYS = 40


def _norm_sym(x: str) -> str:
    return str(x).strip().upper().replace(".", "-")


def _metrics_paths() -> list[Path]:
    data = REPO / "data"
    return [
        data / "etf_metrics_daily.parquet",
        data / "etf_metrics_daily.csv",
    ]


def load_metrics_frame() -> pd.DataFrame:
    """Load ETF metrics daily table (parquet preferred, CSV fallback)."""
    for path in _metrics_paths():
        if not path.is_file():
            continue
        if path.suffix == ".parquet":
            md = pd.read_parquet(path)
        else:
            md = pd.read_csv(path)
        if md.empty:
            continue
        return md
    raise FileNotFoundError(
        "No etf_metrics_daily file found under data/ (expected parquet or csv)"
    )


def load_price_panel(
    *,
    min_days: int = MIN_PRICE_PANEL_DAYS,
    metrics: pd.DataFrame | None = None,
) -> dict[str, pd.DataFrame]:
    """Per-ETF aligned (etf_adj_close, underlying_adj_close) price panels.

    Applies ls-algo ``data/splits_from_flex.csv`` when available so reverse
    splits do not invent +400% daily returns in the dashboard B4 backtest.
    """
    md = metrics if metrics is not None else load_metrics_frame()
    cols = ["date", "ticker", "etf_adj_close", "underlying_adj_close"]
    missing = [c for c in cols if c not in md.columns]
    if missing:
        raise ValueError(f"etf_metrics_daily missing columns: {missing}")

    # Prefer shared ls-algo split logic when the sibling repo is importable.
    try:
        import sys

        ls_algo = Path(r"C:\Users\drewg\Projects\quant\ls-algo")
        if ls_algo.is_dir() and str(ls_algo) not in sys.path:
            sys.path.insert(0, str(ls_algo))
        from scripts.pair_price_panel import frames_from_metrics, split_events_by_symbol

        flex = ls_algo / "data" / "splits_from_flex.csv"
        split_map = split_events_by_symbol(flex_csv=flex if flex.is_file() else None, repo=ls_algo)
        return frames_from_metrics(
            md[cols],
            min_days=min_days,
            apply_splits=True,
            split_map=split_map,
        )
    except Exception:
        pass

    md = md[cols].copy()
    md["ticker"] = md["ticker"].map(_norm_sym)
    md["date"] = pd.to_datetime(md["date"], errors="coerce").dt.normalize()
    md = md.dropna(subset=["date"]).sort_values(["ticker", "date"])

    out: dict[str, pd.DataFrame] = {}
    for etf, g in md.groupby("ticker"):
        g = g.dropna(subset=["etf_adj_close", "underlying_adj_close"])
        if len(g) < min_days:
            continue
        df = pd.DataFrame(
            {
                "a_px": g["etf_adj_close"].to_numpy(dtype=float),
                "b_px": g["underlying_adj_close"].to_numpy(dtype=float),
            },
            index=pd.DatetimeIndex(g["date"]),
        )
        df = df[~df.index.duplicated(keep="last")].sort_index()
        if len(df) >= min_days:
            out[etf] = df
    return out


def load_pair_prices(
    etf: str,
    underlying: str,
    start: str | None = None,
    *,
    panel: dict[str, pd.DataFrame] | None = None,
) -> pd.DataFrame:
    """Return aligned pair prices for one ETF (underlying from same row)."""
    etf_n = _norm_sym(etf)
    px = (panel or load_price_panel()).get(etf_n)
    if px is None or px.empty:
        raise KeyError(f"No price panel for {etf_n}/{_norm_sym(underlying)}")
    if start:
        px = px.loc[px.index >= pd.Timestamp(start)]
    if px.empty:
        raise ValueError(f"No prices for {etf_n} after start={start}")
    return px.copy()


def perf_stats(bt: pd.DataFrame) -> pd.Series:
    n = len(bt)
    if n < 2:
        return pd.Series(dtype=float)
    total_return = bt["equity"].iloc[-1] / bt["equity"].iloc[0] - 1.0
    ann_factor = 252 / max(1, n - 1)
    cagr = (1 + total_return) ** ann_factor - 1 if total_return > -1 else np.nan
    vol = bt["ret"].std() * np.sqrt(252)
    sharpe = (bt["ret"].mean() * 252 / vol) if vol > 0 else np.nan
    max_dd = bt["drawdown"].min()
    return pd.Series(
        {
            "total_return": total_return,
            "cagr": cagr,
            "annual_vol": vol,
            "sharpe": sharpe,
            "max_drawdown": max_dd,
            "rebalance_count": int(bt["rebalance"].sum()) if "rebalance" in bt.columns else 0,
        }
    )

"""Price loading for Bucket 4 backtests from etf-dashboard metrics."""

from __future__ import annotations

import os
import sys
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


# Single-day |return| above this on the ETF leg means the split pipeline left a
# fabricated cliff (e.g. QBTZ 2026-02-02: history ÷3, later days unscaled → +207%).
_PANEL_CLIFF_ABS_RET = 1.0


def sanitize_panel_vs_session_close(
    panel: dict[str, pd.DataFrame],
    metrics: pd.DataFrame,
    *,
    cliff_abs_ret: float = _PANEL_CLIFF_ABS_RET,
) -> dict[str, pd.DataFrame]:
    """Replace ETF panel prices with session close when split-apply invents cliffs.

    ls-algo ``frames_from_metrics(apply_splits=True)`` can scale only a prefix of
    history (QBTZ Jan≤30 at close/3, Feb≥2 at raw close). Holding shorts through
    that cliff wipes research equity and leaves ghost rebalances.
    """
    if not panel or metrics is None or metrics.empty:
        return panel
    if "close_price" not in metrics.columns:
        return panel
    md = metrics.copy()
    md["ticker"] = md["ticker"].map(_norm_sym)
    md["date"] = pd.to_datetime(md["date"], errors="coerce").dt.normalize()
    out: dict[str, pd.DataFrame] = {}
    for etf, px in panel.items():
        if px is None or px.empty or "a_px" not in px.columns:
            out[etf] = px
            continue
        fixed = px.copy()
        ret = fixed["a_px"].astype(float).pct_change()
        if not bool((ret.abs() > float(cliff_abs_ret)).fillna(False).any()):
            out[etf] = fixed
            continue
        g = md.loc[md["ticker"] == _norm_sym(etf), ["date", "close_price"]].dropna()
        if g.empty:
            out[etf] = fixed
            continue
        close = pd.Series(
            g["close_price"].astype(float).to_numpy(),
            index=pd.DatetimeIndex(g["date"]),
            dtype=float,
        )
        close = close[~close.index.duplicated(keep="last")].sort_index()
        aligned = close.reindex(fixed.index)
        # Only swap days where we have a positive session close.
        use = aligned.notna() & (aligned > 0)
        min_overlap = min(len(fixed), max(3, len(fixed) // 4))
        if int(use.sum()) < min_overlap:
            out[etf] = fixed
            continue
        fixed.loc[use, "a_px"] = aligned.loc[use].to_numpy()
        out[etf] = fixed
    return out


def load_price_panel(
    *,
    min_days: int = MIN_PRICE_PANEL_DAYS,
    metrics: pd.DataFrame | None = None,
    corporate_actions_path: Path | None = None,
) -> dict[str, pd.DataFrame]:
    """Per-ETF aligned (etf_adj_close, underlying_adj_close) price panels.

    Applies ls-algo ``data/splits_from_flex.csv`` when available so reverse
    splits do not invent +400% daily returns in the dashboard B4 backtest.
    """
    md = (metrics if metrics is not None else load_metrics_frame()).copy()
    cols = ["date", "ticker", "etf_adj_close", "underlying_adj_close"]
    missing = [c for c in cols if c not in md.columns]
    if missing:
        raise ValueError(f"etf_metrics_daily missing columns: {missing}")

    # Repair the dashboard's canonical adjusted-close basis first. This is
    # required even when ls-algo is available: Flex can report the formal
    # action date after the market close has already switched basis (NBIZ).
    from ingest_etf_metrics import backfill_split_adjusted_etf_adj_close

    md = backfill_split_adjusted_etf_adj_close(
        md,
        corporate_actions_path=corporate_actions_path,
    )
    md_for_close = md

    panel: dict[str, pd.DataFrame] | None = None
    # Then prefer shared ls-algo crater/override logic when a sibling checkout
    # is available. Keep path discovery aligned with the sizing bridge and CI.
    try:
        env_root = os.environ.get("LS_ALGO_ROOT", "").strip()
        candidates = [
            Path(env_root) if env_root else None,
            REPO / "ls-algo",
            REPO.parent / "ls-algo",
            Path.home() / "Projects" / "quant" / "ls-algo",
        ]
        ls_algo = next(
            p.resolve()
            for p in candidates
            if p is not None and (p / "scripts" / "pair_price_panel.py").is_file()
        )
        if str(ls_algo) not in sys.path:
            sys.path.insert(0, str(ls_algo))
        from scripts.pair_price_panel import frames_from_metrics, split_events_by_symbol

        flex = ls_algo / "data" / "splits_from_flex.csv"
        split_map = split_events_by_symbol(flex_csv=flex if flex.is_file() else None, repo=ls_algo)
        panel = frames_from_metrics(
            md[cols],
            min_days=min_days,
            apply_splits=True,
            split_map=split_map,
        )
    except Exception:
        panel = None

    if panel is None:
        md = md[cols].copy()
        md["ticker"] = md["ticker"].map(_norm_sym)
        md["date"] = pd.to_datetime(md["date"], errors="coerce").dt.normalize()
        md = md.dropna(subset=["date"]).sort_values(["ticker", "date"])

        panel = {}
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
                panel[etf] = df

    return sanitize_panel_vs_session_close(panel, md_for_close)


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

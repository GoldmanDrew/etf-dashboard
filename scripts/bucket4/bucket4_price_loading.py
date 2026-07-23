"""Price loading for Bucket 4 backtests from etf-dashboard metrics."""

from __future__ import annotations

import math
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
_SCRIPTS = REPO / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
MIN_PRICE_PANEL_DAYS = 40

# Whitelist reverse-split multiples used when provider restates a segment onto a
# split basis while neighbors stay on the old print (TECS/TZA/SOXS May-2026).
_PANEL_SPLIT_MULTS = (2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0, 15.0, 20.0, 25.0, 50.0)


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
# Relative gap between early vs late median(panel/close) that implies a prefix
# scale (APLZ/BEZ/NBIZ) even when no single day exceeds 100%.
_PANEL_RATIO_STEP_REL = 0.25


def _panel_needs_close_sanitize(
    a_px: pd.Series,
    close_aligned: pd.Series,
    *,
    cliff_abs_ret: float = _PANEL_CLIFF_ABS_RET,
    ratio_step_rel: float = _PANEL_RATIO_STEP_REL,
) -> bool:
    """True when ETF panel has a day cliff or an early/late panel÷close step."""
    a = a_px.astype(float)
    ret = a.pct_change()
    if bool((ret.abs() > float(cliff_abs_ret)).fillna(False).any()):
        return True
    c = close_aligned.astype(float)
    use = a.notna() & c.notna() & (c > 0) & (a > 0)
    if int(use.sum()) < 10:
        return False
    ratio = (a.loc[use] / c.loc[use]).replace([np.inf, -np.inf], np.nan).dropna()
    if len(ratio) < 10:
        return False
    n = max(5, len(ratio) // 5)
    early = float(ratio.iloc[:n].median())
    late = float(ratio.iloc[-n:].median())
    if not (np.isfinite(early) and np.isfinite(late) and early > 0 and late > 0):
        return False
    rel = abs(early - late) / max(early, late)
    return rel > float(ratio_step_rel)


def _max_abs_day_ret(series: pd.Series) -> float:
    ret = series.astype(float).pct_change().abs()
    if ret.empty or not bool(ret.notna().any()):
        return float("inf")
    return float(ret.max())


def _metrics_price_series(
    md: pd.DataFrame,
    etf: str,
    column: str,
    index: pd.DatetimeIndex,
) -> pd.Series | None:
    if column not in md.columns:
        return None
    g = md.loc[md["ticker"] == _norm_sym(etf), ["date", column]].dropna()
    if g.empty:
        return None
    s = pd.Series(
        g[column].astype(float).to_numpy(),
        index=pd.DatetimeIndex(g["date"]),
        dtype=float,
    )
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return s.reindex(index)


def sanitize_panel_vs_session_close(
    panel: dict[str, pd.DataFrame],
    metrics: pd.DataFrame,
    *,
    cliff_abs_ret: float = _PANEL_CLIFF_ABS_RET,
    ratio_step_rel: float = _PANEL_RATIO_STEP_REL,
) -> dict[str, pd.DataFrame]:
    """Replace ETF panel prices when split-apply invents cliffs or prefix scales.

    ls-algo ``frames_from_metrics(apply_splits=True)`` can scale only a prefix of
    history (QBTZ Jan≤30 at close/3, Feb≥2 at raw close; APLZ early×5 vs late×1).
    Holding shorts through that cliff wipes research equity and leaves ghost rebalances.

    Preference when sanitizing:
    1. ``etf_adj_close`` when it removes the cliff (APLZ/BEZ/NBIZ reverse-split
       days where raw ``close_price`` jumps but adj is continuous)
    2. else ``close_price`` (QBTZ fabricated adj / partial scale vs session close)
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
        aligned_close = _metrics_price_series(md, etf, "close_price", fixed.index)
        if aligned_close is None:
            out[etf] = fixed
            continue
        if not _panel_needs_close_sanitize(
            fixed["a_px"],
            aligned_close,
            cliff_abs_ret=cliff_abs_ret,
            ratio_step_rel=ratio_step_rel,
        ):
            out[etf] = fixed
            continue
        min_overlap = min(len(fixed), max(3, len(fixed) // 4))
        baseline = _max_abs_day_ret(fixed["a_px"])
        best_score = baseline
        best_series: pd.Series | None = None
        best_use: pd.Series | None = None
        # Prefer adj when it is the smoother continuous basis; fall back to close.
        for column in ("etf_adj_close", "close_price"):
            candidate = (
                aligned_close
                if column == "close_price"
                else _metrics_price_series(md, etf, column, fixed.index)
            )
            if candidate is None:
                continue
            use = candidate.notna() & (candidate > 0)
            if int(use.sum()) < min_overlap:
                continue
            trial = fixed["a_px"].astype(float).copy()
            trial.loc[use] = candidate.loc[use].to_numpy()
            score = _max_abs_day_ret(trial)
            if score < best_score - 1e-12:
                best_score = score
                best_series = candidate
                best_use = use
        if best_series is not None and best_use is not None:
            fixed.loc[best_use, "a_px"] = best_series.loc[best_use].to_numpy()
        out[etf] = fixed
    return out


def _match_panel_split_jump(jump_abs: float, *, rel_tol: float = 0.18) -> float | None:
    if not (math.isfinite(jump_abs) and jump_abs > 1.0):
        return None
    best: tuple[float, float] | None = None
    for mult in _PANEL_SPLIT_MULTS:
        err = abs(jump_abs / mult - 1.0)
        if err <= rel_tol and (best is None or err < best[0]):
            best = (err, mult)
    return best[1] if best else None


def sanitize_panel_split_sized_basis_jumps(
    panel: dict[str, pd.DataFrame],
    *,
    max_underlying_log_move: float = 0.25,
    min_etf_log_jump: float = 0.55,
) -> dict[str, pd.DataFrame]:
    """Scale ETF segments when an ETF-only jump matches a split multiple.

    Mirrors ``price_basis._normalize_split_sized_basis_jumps`` (Decay / Backtest
    TR already apply this). Without it, B4 Optimized loads raw ``etf_adj_close``
    spikes (TECS/TZA/SOXS ~10× May-2026) and wipes the unit-equity book.
    """
    if not panel:
        return panel
    out: dict[str, pd.DataFrame] = {}
    for etf, px in panel.items():
        if px is None or px.empty or "a_px" not in px.columns or "b_px" not in px.columns:
            out[etf] = px
            continue
        fixed = px.sort_index().copy()
        a = fixed["a_px"].astype(float).to_numpy()
        b = fixed["b_px"].astype(float).to_numpy()
        n = len(a)
        if n < 2:
            out[etf] = fixed
            continue
        scale = 1.0
        adjusted = np.full(n, np.nan, dtype=float)
        adjusted[-1] = a[-1]
        for i in range(n - 2, -1, -1):
            prev_raw = float(a[i])
            cur_adj = float(adjusted[i + 1])
            u0 = float(b[i])
            u1 = float(b[i + 1])
            if not (prev_raw > 0 and cur_adj > 0 and u0 > 0 and u1 > 0):
                adjusted[i] = prev_raw * scale if prev_raw > 0 else np.nan
                continue
            prev_adj = prev_raw * scale
            lr_e = math.log(prev_adj / cur_adj)
            lr_u = abs(math.log(u1 / u0))
            if abs(lr_e) >= min_etf_log_jump and lr_u < max_underlying_log_move:
                matched = _match_panel_split_jump(math.exp(abs(lr_e)))
                if matched is not None:
                    scale = scale / matched if lr_e > 0 else scale * matched
                    prev_adj = prev_raw * scale
            adjusted[i] = prev_adj
        fixed["a_px"] = adjusted
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

    sanitized = sanitize_panel_vs_session_close(panel, md_for_close)
    return sanitize_panel_split_sized_basis_jumps(sanitized)


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

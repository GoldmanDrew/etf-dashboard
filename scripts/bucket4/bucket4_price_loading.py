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


def load_etf_underlying_map(
    screener_path: Path | None = None,
) -> dict[str, str]:
    """ETF → underlying map from screener CSV (fallback: empty)."""
    path = Path(screener_path) if screener_path is not None else (REPO / "data" / "etf_screened_today.csv")
    if not path.is_file():
        return {}
    try:
        scr = pd.read_csv(path, usecols=lambda c: str(c) in {"ETF", "Underlying", "etf", "underlying", "symbol", "Symbol"})
    except Exception:
        return {}
    etf_col = next((c for c in ("ETF", "etf", "symbol", "Symbol") if c in scr.columns), None)
    und_col = next((c for c in ("Underlying", "underlying") if c in scr.columns), None)
    if not etf_col or not und_col:
        return {}
    out: dict[str, str] = {}
    for etf, und in zip(scr[etf_col].tolist(), scr[und_col].tolist()):
        e, u = _norm_sym(etf), _norm_sym(und)
        if e and u and u not in {"", "NAN", "NONE"}:
            out[e] = u
    return out


def load_underlying_adj_close_series(
    underlying: str,
    *,
    metrics: pd.DataFrame | None = None,
    etf_to_und: dict[str, str] | None = None,
    panel_fallback: pd.Series | None = None,
    asof: str | pd.Timestamp | None = None,
    entry_date: str | pd.Timestamp | None = None,
    min_obs_before_entry: int = 252,
    yahoo_fallback: bool = True,
) -> pd.Series:
    """Full underlying adj-close history for crash / vol-shape lookbacks.

    Joint ETF panels only start at listing, which blinds cash-residual crash
    stats for ~``tail_min_obs`` sessions. This merges ``underlying_adj_close``
    across every metrics ticker mapped to the same underlying, then optionally
    extends with Yahoo when history *before entry* is still thin.
    """
    und = _norm_sym(underlying)
    if not und:
        return pd.Series(dtype=float)

    md = metrics if metrics is not None else load_metrics_frame()
    mapping = etf_to_und if etf_to_und is not None else load_etf_underlying_map()
    tickers = [t for t, u in mapping.items() if u == und]
    if "ticker" not in md.columns or "underlying_adj_close" not in md.columns:
        series = pd.Series(dtype=float)
    else:
        work = md.loc[:, ["date", "ticker", "underlying_adj_close"]].copy()
        work["ticker"] = work["ticker"].map(_norm_sym)
        work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.normalize()
        work["underlying_adj_close"] = pd.to_numeric(work["underlying_adj_close"], errors="coerce")
        if tickers:
            work = work[work["ticker"].isin(tickers)]
        work = work.dropna(subset=["date", "underlying_adj_close"])
        if work.empty:
            series = pd.Series(dtype=float)
        else:
            # Median across sleeves on a date (same und print, robust to glitches).
            grp = work.groupby("date", sort=True)["underlying_adj_close"].median()
            series = pd.Series(grp.to_numpy(dtype=float), index=pd.DatetimeIndex(grp.index), name=und)

    if panel_fallback is not None and not panel_fallback.empty:
        fb = pd.to_numeric(panel_fallback, errors="coerce").dropna().astype(float)
        fb.index = pd.DatetimeIndex(pd.to_datetime(fb.index, errors="coerce")).normalize()
        fb = fb[fb.index.notna()]
        fb = fb[~fb.index.duplicated(keep="last")].sort_index()
        if series.empty:
            series = fb.rename(und)
        else:
            series = series.combine_first(fb).sort_index()
            series = series[~series.index.duplicated(keep="last")]

    asof_ts = pd.Timestamp(asof).normalize() if asof is not None else None
    entry_ts = pd.Timestamp(entry_date).normalize() if entry_date is not None else None
    if entry_ts is None and panel_fallback is not None and len(panel_fallback):
        try:
            entry_ts = pd.Timestamp(pd.DatetimeIndex(panel_fallback.index).min()).normalize()
        except Exception:
            entry_ts = None

    def _n_before_entry(s: pd.Series) -> int:
        if s is None or s.empty:
            return 0
        if entry_ts is None:
            return int(s.shape[0])
        # Observations strictly before the first trade/listing day.
        return int(s.loc[s.index < entry_ts].shape[0])

    if yahoo_fallback and _n_before_entry(series) < int(min_obs_before_entry):
        y = _yahoo_adj_close(und, end=asof_ts or entry_ts, entry_date=entry_ts)
        if y is not None and not y.empty:
            if series.empty:
                series = y.rename(und)
            else:
                # Yahoo fills the left (pre-listing) tail; metrics wins on overlap.
                series = series.combine_first(y).sort_index()
                series = series[~series.index.duplicated(keep="last")]

    # Brand-new IPO underlyings (CBRS/SPCX) still have no left tail — use SPY
    # *returns* as a last-resort crash/vol lookback so day-1 sizing is not blind.
    # Never combine_first SPY price levels onto the und (that fabricates a cliff
    # at IPO, e.g. SPY ~$740 → CBRS ~$311).
    if yahoo_fallback and _n_before_entry(series) < int(min_obs_before_entry):
        spy = _yahoo_adj_close("SPY", end=asof_ts or entry_ts, entry_date=entry_ts, try_aliases=False)
        if spy is not None and not spy.empty:
            if series.empty:
                # Returns-only proxy scaled to a neutral level.
                last = float(spy.iloc[-1])
                series = (spy / last * 100.0).rename(und) if last > 0 else spy.rename(und)
            else:
                series = _prepend_proxy_returns(series, spy).rename(und)

    if asof_ts is not None and not series.empty:
        series = series.loc[:asof_ts]
    return series.astype(float)


def _prepend_proxy_returns(real: pd.Series, proxy: pd.Series) -> pd.Series:
    """Prepend ``proxy`` returns onto ``real`` levels without a price-level cliff.

    Scales the proxy path so its last pre-``real`` print equals the first real
    print (flat splice). Preserves proxy daily returns in the left tail; real
    levels win on overlap.
    """
    real_s = pd.to_numeric(real, errors="coerce").dropna().astype(float)
    proxy_s = pd.to_numeric(proxy, errors="coerce").dropna().astype(float)
    real_s.index = pd.DatetimeIndex(pd.to_datetime(real_s.index, errors="coerce")).normalize()
    proxy_s.index = pd.DatetimeIndex(pd.to_datetime(proxy_s.index, errors="coerce")).normalize()
    real_s = real_s[real_s.index.notna()]
    proxy_s = proxy_s[proxy_s.index.notna()]
    real_s = real_s[~real_s.index.duplicated(keep="last")].sort_index()
    proxy_s = proxy_s[~proxy_s.index.duplicated(keep="last")].sort_index()
    if real_s.empty or proxy_s.empty:
        return real_s
    t0 = real_s.index.min()
    p0 = float(real_s.iloc[0])
    if not (p0 > 0):
        return real_s
    pre = proxy_s.loc[proxy_s.index < t0]
    if len(pre) < 2:
        return real_s
    anchor = float(pre.iloc[-1])
    if not (anchor > 0):
        return real_s
    synth = (pre / anchor) * p0
    out = pd.concat([synth, real_s]).sort_index()
    return out[~out.index.duplicated(keep="last")]


# When screener "underlying" is a co-listed ETP with no pre-history, Yahoo-fetch
# a longer proxy for crash / vol-shape lookbacks (levels are only used for
# returns in conditional_crash_stats / TR-VCR).
_YAHOO_UND_ALIASES: dict[str, tuple[str, ...]] = {
    "ETHA": ("ETH-USD",),
    "XRPZ": ("XRP-USD",),
    "SVIX": ("VIXY", "^VIX"),
    "UVIX": ("VIXY", "^VIX"),
    "BTCZ": ("BTC-USD",),
    "BITO": ("BTC-USD",),
}


def _yahoo_adj_close(
    symbol: str,
    *,
    end: pd.Timestamp | None = None,
    entry_date: str | pd.Timestamp | None = None,
    lookback_years: int = 15,
    try_aliases: bool = True,
) -> pd.Series | None:
    try:
        import yfinance as yf  # type: ignore
    except Exception:
        return None

    def _one(sym: str) -> pd.Series | None:
        if not sym:
            return None
        end_ts = pd.Timestamp(end or pd.Timestamp.utcnow()).normalize()
        # Reach back far enough that entry_date has a full crash lookback.
        start_ts = end_ts - pd.DateOffset(years=int(lookback_years))
        if entry_date is not None:
            entry_ts = pd.Timestamp(entry_date).normalize() - pd.DateOffset(years=6)
            if entry_ts < start_ts:
                start_ts = entry_ts
        try:
            hist = yf.download(
                sym,
                start=start_ts.strftime("%Y-%m-%d"),
                end=(end_ts + pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
                auto_adjust=True,
                progress=False,
                threads=False,
            )
        except Exception:
            return None
        if hist is None or getattr(hist, "empty", True):
            return None
        close = hist["Close"] if "Close" in hist.columns else hist.iloc[:, 0]
        if isinstance(close, pd.DataFrame):
            close = close.iloc[:, 0]
        s = pd.to_numeric(close, errors="coerce").dropna().astype(float)
        s.index = pd.DatetimeIndex(pd.to_datetime(s.index, errors="coerce")).normalize()
        s = s[s.index.notna()]
        s = s[~s.index.duplicated(keep="last")].sort_index()
        s.name = sym
        return s if len(s) >= 5 else None

    primary = _norm_sym(symbol)
    # Keep ^VIX-style Yahoo tickers intact.
    raw = str(symbol or "").strip()
    candidates = [raw if raw.startswith("^") else primary]
    if try_aliases:
        for alt in _YAHOO_UND_ALIASES.get(primary, ()):
            if alt not in candidates:
                candidates.append(alt)

    best: pd.Series | None = None
    for cand in candidates:
        s = _one(cand)
        if s is None or s.empty:
            continue
        if best is None or s.index.min() < best.index.min() or (
            s.index.min() == best.index.min() and len(s) > len(best)
        ):
            best = s
    return best


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

"""Underlying vol-shape from joint ETF metrics (matches index.html charts).

Keep in sync with ls-algo/vol_shape.py. Parity: tests/test_vol_shape_ls_algo_parity.py
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

TRADING_DAYS = 252
VOL_SHAPE_WINDOWS: tuple[int, ...] = (20, 60)
VOL_SHAPE_PRIMARY_WINDOW = 60
VOL_SHAPE_HISTORY_MAX_POINTS = 252
PRICE_BASIS_JOINT_METRICS = "joint_etf_metrics"
PRICE_BASIS_UNDERLYING_TR = "underlying_total_return"
TREND_ESTIMATOR_SOURCE = "heuristic_v2_persistence_cadence"


def _vol_shape_columns_for_window(window: int) -> tuple[str, ...]:
    return (
        f"und_rv_{window}d_daily_annual",
        f"und_rv_{window}d_weekly_annual",
        f"und_trend_ratio_{window}d",
        f"und_trend_ratio_fwd_{window}d",
        f"und_trend_regime_prob_trend_{window}d",
        f"und_trend_regime_prob_chop_{window}d",
        f"und_trend_estimator_confidence_{window}d",
        f"und_trend_efficiency_{window}d",
        f"und_trend_consistency_{window}d",
        f"und_trend_r2_{window}d",
        f"und_trend_persistence_{window}d",
        f"und_rebalance_cadence_score_{window}d",
        f"und_vcr_{window}d",
        f"und_return_{window}d",
        f"und_abs_return_{window}d_pctile",
        f"und_rv_{window}d_pctile",
        f"und_trend_ratio_{window}d_pctile",
        f"und_vcr_{window}d_pctile",
        f"und_vcr_{window}d_median",
        f"und_vol_shape_{window}d",
    )


def _all_vol_shape_columns() -> tuple[str, ...]:
    cols: list[str] = []
    for w in VOL_SHAPE_WINDOWS:
        cols.extend(_vol_shape_columns_for_window(w))
    return tuple(cols)


def _percentile_of_latest(values: list[float]) -> float | None:
    a = np.asarray([v for v in values if np.isfinite(v)], dtype=float)
    if a.size < 2:
        return None
    latest = float(a[-1])
    return float(np.mean(a <= latest))


def _clip(v: float, lo: float, hi: float) -> float:
    return float(min(max(v, lo), hi))


def _clip01(v: float) -> float:
    return _clip(v, 0.0, 1.0)


def _sigmoid(v: float) -> float:
    if v >= 0:
        z = np.exp(-v)
        return float(1.0 / (1.0 + z))
    z = np.exp(v)
    return float(z / (1.0 + z))


def _trend_r2(tail: np.ndarray) -> float:
    y = np.cumsum(tail)
    if y.size < 3:
        return 0.0
    x = np.arange(y.size, dtype=float)
    x = x - x.mean()
    y_centered = y - y.mean()
    denom = float(np.dot(x, x))
    ss_tot = float(np.dot(y_centered, y_centered))
    if denom <= 0 or ss_tot <= 0:
        return 0.0
    beta = float(np.dot(x, y_centered) / denom)
    yhat = beta * x
    ss_res = float(np.sum((y_centered - yhat) ** 2))
    return _clip01(1.0 - ss_res / ss_tot)


def _regime_persistence(tail: np.ndarray) -> float:
    """In-window regime persistence from consecutive sub-window behavior.

    This intentionally uses only the current trailing window. It rewards paths
    whose sub-window cumulative returns point the same way with similar
    magnitudes, and penalizes alternating/reversing sub-windows.
    """
    n = int(tail.size)
    if n < 10:
        return 0.0
    n_blocks = min(6, max(3, n // 10))
    blocks = [b for b in np.array_split(tail, n_blocks) if b.size]
    block_rets = np.asarray([float(np.sum(b)) for b in blocks], dtype=float)
    block_abs = np.abs(block_rets)
    if not np.any(np.isfinite(block_abs)) or float(np.sum(block_abs)) <= 0:
        return 0.0
    signs = np.sign(block_rets[np.abs(block_rets) > 1e-12])
    sign_stability = _clip01(abs(float(np.mean(signs)))) if signs.size else 0.0
    same_direction = (
        float(np.mean(np.sign(block_rets[1:]) == np.sign(block_rets[:-1])))
        if block_rets.size > 1 else 0.0
    )
    mag_mean = float(np.mean(block_abs))
    mag_cv = float(np.std(block_abs) / mag_mean) if mag_mean > 0 else 2.0
    magnitude_stability = _clip01(1.0 - mag_cv / 1.5)
    return _clip01(0.45 * sign_stability + 0.35 * same_direction + 0.20 * magnitude_stability)


def trend_regime_estimator_from_returns(
    tail: np.ndarray,
    *,
    trend_ratio: float | None = None,
    vcr: float | None = None,
) -> dict[str, float | None]:
    """Forward-looking path-regime proxy from point-in-time trailing returns.

    This is intentionally a conservative heuristic: it shrinks the realized TR
    toward 1.0 unless the path is smooth, sign-consistent, and not jump-driven.
    It is an estimator to test, not a replacement for the realized TR field.
    """
    a = np.asarray(tail, dtype=float)
    a = a[np.isfinite(a)]
    n = int(a.size)
    if n < 5:
        return {
            "trend_ratio_fwd": None,
            "prob_trend": None,
            "prob_chop": None,
            "confidence": None,
            "efficiency": None,
            "consistency": None,
            "r2": None,
            "persistence": None,
            "cadence_score": None,
        }
    sq = a * a
    sum_sq = float(np.sum(sq))
    if not np.isfinite(sum_sq) or sum_sq <= 0:
        return {
            "trend_ratio_fwd": None,
            "prob_trend": None,
            "prob_chop": None,
            "confidence": None,
            "efficiency": None,
            "consistency": None,
            "r2": None,
            "persistence": None,
            "cadence_score": None,
        }

    tr = float(trend_ratio) if trend_ratio is not None and np.isfinite(trend_ratio) else 1.0
    vc = float(vcr) if vcr is not None and np.isfinite(vcr) else float(np.max(sq) / sum_sq)

    efficiency = _clip01(abs(float(np.sum(a))) / np.sqrt(sum_sq * n))
    signs = np.sign(a[np.abs(a) > 1e-12])
    consistency = _clip01(abs(float(np.mean(signs)))) if signs.size else 0.0
    r2 = _trend_r2(a)
    persistence = _regime_persistence(a)

    random_eff = float(np.sqrt(2.0 / np.pi) / np.sqrt(n))
    random_consistency = random_eff
    sample_conf = _clip01(np.sqrt(n / 60.0))
    jump_floor = max(0.15, 3.0 / n)
    jump_penalty = _clip01((vc - jump_floor) / 0.35)

    tr_shrunk = 1.0 + (tr - 1.0) * sample_conf * (1.0 - 0.55 * jump_penalty)
    tr_z = _clip((tr_shrunk - 1.0) / 0.18, -3.0, 3.0)
    eff_z = _clip((efficiency - 1.5 * random_eff) / 0.25, -3.0, 3.0)
    cons_z = _clip((consistency - 1.5 * random_consistency) / 0.25, -3.0, 3.0)
    r2_z = _clip((r2 - 0.35) / 0.25, -3.0, 3.0)
    persistence_z = _clip((persistence - 0.50) / 0.25, -3.0, 3.0)

    evidence = (
        0.40 * tr_z
        + 0.22 * eff_z
        + 0.20 * cons_z
        + 0.20 * r2_z
        + 0.22 * persistence_z
        - 0.35 * jump_penalty
    )
    confidence = _clip01(sample_conf * (1.0 - 0.45 * jump_penalty))
    scaled = evidence * (0.50 + 0.50 * confidence)
    ratio_fwd = _clip(1.0 + 0.22 * float(np.tanh(scaled / 2.0)), 0.72, 1.28)
    prob_trend = _sigmoid(1.35 * scaled)
    prob_chop = _sigmoid(-1.35 * scaled)
    cadence_evidence = (
        0.45 * persistence_z
        + 0.30 * r2_z
        + 0.25 * eff_z
        + 0.15 * tr_z
        - 0.55 * jump_penalty
        - 0.20 * max(0.0, -tr_z)
    )
    cadence_scaled = cadence_evidence * (0.50 + 0.50 * confidence)
    cadence_score = _clip(1.0 + 0.30 * float(np.tanh(cadence_scaled / 2.0)), 0.70, 1.30)

    return {
        "trend_ratio_fwd": ratio_fwd,
        "prob_trend": prob_trend,
        "prob_chop": prob_chop,
        "confidence": confidence,
        "efficiency": efficiency,
        "consistency": consistency,
        "r2": r2,
        "persistence": persistence,
        "cadence_score": cadence_score,
    }


def vol_shape_label(
    *,
    trend_ratio: float | None,
    vcr: float | None,
    abs_return_pctile: float | None,
    rv_pctile: float | None,
    vcr_pctile: float | None,
) -> str | None:
    if trend_ratio is None or vcr is None:
        return None
    notable = (
        (abs_return_pctile is not None and abs_return_pctile >= 0.80)
        or (rv_pctile is not None and rv_pctile >= 0.80)
    )
    trending = trend_ratio >= 1.05
    mean_reverting = trend_ratio <= 0.95
    jumpy = vcr >= 0.40 or (vcr_pctile is not None and vcr_pctile >= 0.80)

    if notable and trending and jumpy:
        return "jumpy_trend"
    if notable and trending:
        return "boiling_trend"
    if notable and mean_reverting:
        return "choppy_volatile"
    if notable:
        return "volatile_mixed"
    if trending:
        return "quiet_trend"
    if mean_reverting:
        return "quiet_chop"
    return "quiet_mixed"


def underlying_vol_shape_from_prices(prices: pd.Series, window: int) -> dict[str, Any]:
    """Rolling vol-shape on underlying prices (log returns), ls-algo semantics."""
    cols = _vol_shape_columns_for_window(window)
    label_col = f"und_vol_shape_{window}d"
    empty: dict[str, Any] = {col: None for col in cols if col != label_col}
    empty[label_col] = None
    if window <= 0 or window % 5 != 0:
        return empty
    if prices is None:
        return empty

    s = pd.to_numeric(prices, errors="coerce").dropna()
    s = s[~s.index.duplicated(keep="last")].sort_index()
    if len(s) < window + 1:
        return empty

    r = np.log(s / s.shift(1)).replace([np.inf, -np.inf], np.nan).dropna()
    r = r[np.isfinite(r)]
    if len(r) < window:
        return empty

    rv_daily_hist: list[float] = []
    rv_weekly_hist: list[float] = []
    tr_hist: list[float] = []
    tr_fwd_hist: list[float] = []
    trend_prob_hist: list[float] = []
    chop_prob_hist: list[float] = []
    estimator_conf_hist: list[float] = []
    trend_eff_hist: list[float] = []
    trend_consistency_hist: list[float] = []
    trend_r2_hist: list[float] = []
    trend_persistence_hist: list[float] = []
    cadence_score_hist: list[float] = []
    vcr_hist: list[float] = []
    ret_hist: list[float] = []

    n_weeks = window // 5
    vals = r.to_numpy(dtype=float)
    for end in range(window, vals.size + 1):
        tail = vals[end - window : end]
        sq = tail**2
        sum_sq = float(np.sum(sq))
        if not np.isfinite(sum_sq) or sum_sq <= 0:
            continue
        rv_daily = float(np.sqrt(np.mean(sq) * TRADING_DAYS))
        weekly = tail.reshape(n_weeks, 5).sum(axis=1)
        rv_weekly = float(np.sqrt(np.mean(weekly**2) * (TRADING_DAYS / 5.0)))
        trend_ratio = rv_weekly / rv_daily if rv_daily > 0 else np.nan
        vcr = float(np.max(sq) / sum_sq)
        est = trend_regime_estimator_from_returns(tail, trend_ratio=trend_ratio, vcr=vcr)
        rv_daily_hist.append(rv_daily)
        rv_weekly_hist.append(rv_weekly)
        tr_hist.append(float(trend_ratio))
        tr_fwd_hist.append(float(est["trend_ratio_fwd"]) if est["trend_ratio_fwd"] is not None else np.nan)
        trend_prob_hist.append(float(est["prob_trend"]) if est["prob_trend"] is not None else np.nan)
        chop_prob_hist.append(float(est["prob_chop"]) if est["prob_chop"] is not None else np.nan)
        estimator_conf_hist.append(float(est["confidence"]) if est["confidence"] is not None else np.nan)
        trend_eff_hist.append(float(est["efficiency"]) if est["efficiency"] is not None else np.nan)
        trend_consistency_hist.append(float(est["consistency"]) if est["consistency"] is not None else np.nan)
        trend_r2_hist.append(float(est["r2"]) if est["r2"] is not None else np.nan)
        trend_persistence_hist.append(float(est["persistence"]) if est["persistence"] is not None else np.nan)
        cadence_score_hist.append(float(est["cadence_score"]) if est["cadence_score"] is not None else np.nan)
        vcr_hist.append(vcr)
        ret_hist.append(float(np.sum(tail)))

    if not rv_daily_hist:
        return empty

    rv_pctile = _percentile_of_latest(rv_daily_hist)
    abs_ret_pctile = _percentile_of_latest([abs(x) for x in ret_hist])
    trend_pctile = _percentile_of_latest(tr_hist)
    vcr_pctile = _percentile_of_latest(vcr_hist)
    vcr_median_hist = float(np.median(vcr_hist)) if vcr_hist else np.nan
    label = vol_shape_label(
        trend_ratio=tr_hist[-1],
        vcr=vcr_hist[-1],
        abs_return_pctile=abs_ret_pctile,
        rv_pctile=rv_pctile,
        vcr_pctile=vcr_pctile,
    )

    def _f(v: float | None) -> float | None:
        if v is None or not np.isfinite(v):
            return None
        return round(float(v), 6)

    return {
        f"und_rv_{window}d_daily_annual": _f(rv_daily_hist[-1]),
        f"und_rv_{window}d_weekly_annual": _f(rv_weekly_hist[-1]),
        f"und_trend_ratio_{window}d": _f(tr_hist[-1]),
        f"und_trend_ratio_fwd_{window}d": _f(tr_fwd_hist[-1]),
        f"und_trend_regime_prob_trend_{window}d": _f(trend_prob_hist[-1]),
        f"und_trend_regime_prob_chop_{window}d": _f(chop_prob_hist[-1]),
        f"und_trend_estimator_confidence_{window}d": _f(estimator_conf_hist[-1]),
        f"und_trend_efficiency_{window}d": _f(trend_eff_hist[-1]),
        f"und_trend_consistency_{window}d": _f(trend_consistency_hist[-1]),
        f"und_trend_r2_{window}d": _f(trend_r2_hist[-1]),
        f"und_trend_persistence_{window}d": _f(trend_persistence_hist[-1]),
        f"und_rebalance_cadence_score_{window}d": _f(cadence_score_hist[-1]),
        f"und_vcr_{window}d": _f(vcr_hist[-1]),
        f"und_return_{window}d": _f(ret_hist[-1]),
        f"und_abs_return_{window}d_pctile": _f(abs_ret_pctile),
        f"und_rv_{window}d_pctile": _f(rv_pctile),
        f"und_trend_ratio_{window}d_pctile": _f(trend_pctile),
        f"und_vcr_{window}d_pctile": _f(vcr_pctile),
        f"und_vcr_{window}d_median": _f(vcr_median_hist),
        f"und_vol_shape_{window}d": label,
        "und_trend_estimator_source": TREND_ESTIMATOR_SOURCE,
    }


def underlying_vol_shape_panel_from_prices(prices: pd.Series) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for w in VOL_SHAPE_WINDOWS:
        out.update(underlying_vol_shape_from_prices(prices, w))
    return out


def _round_hist(v: float | None) -> float | None:
    if v is None or not np.isfinite(v):
        return None
    return round(float(v), 6)


def build_underlying_vol_shape_history(
    prices: pd.Series,
    window: int = VOL_SHAPE_PRIMARY_WINDOW,
    max_points: int = VOL_SHAPE_HISTORY_MAX_POINTS,
) -> dict[str, Any]:
    """Rolling TR/VCR/RV series (matches index.html buildUnderlyingVolShapeHistory)."""
    empty: dict[str, Any] = {"series": [], "vcrMedian": None, "window": window}
    if window <= 0 or window % 5 != 0:
        return empty
    if prices is None:
        return empty

    s = pd.to_numeric(prices, errors="coerce").dropna()
    s = s[~s.index.duplicated(keep="last")].sort_index()
    if len(s) < window + 1:
        return empty

    log_r = np.log(s / s.shift(1)).replace([np.inf, -np.inf], np.nan).dropna()
    log_r = log_r[np.isfinite(log_r)]
    if len(log_r) < window:
        return empty

    rets: list[tuple[str, float]] = [
        (str(log_r.index[i]), float(log_r.iloc[i])) for i in range(len(log_r))
    ]
    n_weeks = window // 5
    out: list[dict[str, Any]] = []
    for end in range(window, len(rets) + 1):
        tail = np.asarray([r for _, r in rets[end - window : end]], dtype=float)
        sum_sq = float(np.sum(tail**2))
        if not np.isfinite(sum_sq) or sum_sq <= 0:
            continue
        rv_daily = float(np.sqrt((sum_sq / window) * TRADING_DAYS))
        weekly = tail.reshape(n_weeks, 5).sum(axis=1)
        rv_weekly = float(np.sqrt(np.mean(weekly**2) * (TRADING_DAYS / 5.0)))
        trend_ratio = rv_weekly / rv_daily if rv_daily > 0 else None
        vcr = float(np.max(tail**2) / sum_sq)
        est = trend_regime_estimator_from_returns(tail, trend_ratio=trend_ratio, vcr=vcr)
        out.append(
            {
                "date": rets[end - 1][0],
                "rv_daily": _round_hist(rv_daily),
                "rv_weekly": _round_hist(rv_weekly),
                "trend_ratio": _round_hist(trend_ratio),
                "trend_ratio_fwd": _round_hist(est["trend_ratio_fwd"]),
                "trend_prob": _round_hist(est["prob_trend"]),
                "chop_prob": _round_hist(est["prob_chop"]),
                "trend_estimator_confidence": _round_hist(est["confidence"]),
                "trend_persistence": _round_hist(est["persistence"]),
                "rebalance_cadence_score": _round_hist(est["cadence_score"]),
                "vcr": _round_hist(vcr),
            }
        )

    if max_points > 0 and len(out) > max_points:
        out = out[-max_points:]

    vcr_vals = sorted(
        float(x["vcr"]) for x in out if x.get("vcr") is not None and np.isfinite(float(x["vcr"]))
    )
    if vcr_vals:
        mid = len(vcr_vals) // 2
        vcr_median = (
            vcr_vals[mid]
            if len(vcr_vals) % 2
            else 0.5 * (vcr_vals[mid - 1] + vcr_vals[mid])
        )
    else:
        vcr_median = None
    vcr_median_r = _round_hist(vcr_median)
    series = [{**row, "vcr_median": vcr_median_r} for row in out]
    return {"series": series, "vcrMedian": vcr_median_r, "window": window}


def _joint_metrics_price_series(rows: pd.DataFrame) -> pd.Series | None:
    if rows is None or rows.empty:
        return None
    prices: list[tuple[str, float]] = []
    for _, row in rows.iterrows():
        ds = str(row.get("date") or "").strip()
        pl = row.get("close_price")
        if pl is None or (isinstance(pl, float) and not np.isfinite(pl)):
            pl = row.get("nav")
        ps = row.get("underlying_adj_close")
        try:
            pl_f = float(pl)
            ps_f = float(ps)
        except (TypeError, ValueError):
            continue
        if not ds or not (np.isfinite(pl_f) and pl_f > 0 and np.isfinite(ps_f) and ps_f > 0):
            continue
        prices.append((ds, ps_f))
    if not prices:
        return None
    prices.sort(key=lambda x: x[0])
    idx = pd.Index([p[0] for p in prices], name="date")
    return pd.Series([p[1] for p in prices], index=idx, dtype=float)


def load_vol_shape_from_metrics(
    metrics_path: Path,
    universe_symbols: set[str] | None = None,
    *,
    history_max_points: int = VOL_SHAPE_HISTORY_MAX_POINTS,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Headline vol-shape panels and rolling history keyed by ETF symbol."""
    empty_history: dict[str, Any] = {
        "window": VOL_SHAPE_PRIMARY_WINDOW,
        "history_max_points": history_max_points,
        "symbols": {},
        "symbols_count": 0,
    }
    if not metrics_path.exists():
        return {}, empty_history
    try:
        df = pd.read_csv(metrics_path)
    except Exception as e:
        print(f"  Warning: could not read ETF metrics for vol-shape: {e}")
        return {}, empty_history

    if "ticker" not in df.columns:
        return {}, empty_history

    if "date" in df.columns:
        df = df.sort_values(["ticker", "date"], kind="stable")

    out: dict[str, dict[str, Any]] = {}
    history_symbols: dict[str, Any] = {}
    for ticker, grp in df.groupby("ticker", sort=False):
        sym = str(ticker or "").strip().upper()
        if not sym:
            continue
        if universe_symbols is not None and sym not in universe_symbols:
            continue
        px = _joint_metrics_price_series(grp)
        if px is None:
            continue
        panel = underlying_vol_shape_panel_from_prices(px)
        primary_tr = panel.get(f"und_trend_ratio_{VOL_SHAPE_PRIMARY_WINDOW}d")
        if primary_tr is None:
            continue
        panel["und_vol_shape_source"] = "etf_metrics_daily"
        panel["und_trend_estimator_source"] = TREND_ESTIMATOR_SOURCE
        panel["und_vol_shape_price_basis"] = PRICE_BASIS_JOINT_METRICS
        panel["und_vol_shape_metrics_asof"] = str(px.index[-1]) if len(px.index) else None
        panel["und_vol_shape_joint_days"] = int(len(px))
        out[sym] = panel
        hist = build_underlying_vol_shape_history(
            px,
            window=VOL_SHAPE_PRIMARY_WINDOW,
            max_points=history_max_points,
        )
        if hist.get("series"):
            history_symbols[sym] = hist

    empty_history["symbols"] = history_symbols
    empty_history["symbols_count"] = len(history_symbols)
    return out, empty_history


def load_vol_shape_by_symbol(
    metrics_path: Path,
    universe_symbols: set[str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Per-ETF headline vol-shape only (backward-compatible helper)."""
    panels, _ = load_vol_shape_from_metrics(metrics_path, universe_symbols)
    return panels


def apply_vol_shape_to_record(rec: dict[str, Any], panel: dict[str, Any] | None) -> None:
    """Overwrite vol-shape fields when metrics panel is available."""
    if not panel:
        rec.setdefault("und_vol_shape_source", "screener")
        return
    for key in _all_vol_shape_columns():
        if key in panel and panel[key] is not None:
            rec[key] = panel[key]
    rec["und_vol_shape_source"] = panel.get("und_vol_shape_source", "etf_metrics_daily")
    rec["und_trend_estimator_source"] = panel.get("und_trend_estimator_source", TREND_ESTIMATOR_SOURCE)
    if panel.get("und_vol_shape_price_basis"):
        rec["und_vol_shape_price_basis"] = panel["und_vol_shape_price_basis"]
    if panel.get("und_vol_shape_metrics_asof"):
        rec["und_vol_shape_metrics_asof"] = panel["und_vol_shape_metrics_asof"]
    if panel.get("und_vol_shape_joint_days") is not None:
        rec["und_vol_shape_joint_days"] = panel["und_vol_shape_joint_days"]

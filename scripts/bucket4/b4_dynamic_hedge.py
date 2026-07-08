"""Bucket 4 dynamic hedge ratio (cross-sectional z-composite)."""

from __future__ import annotations

import numpy as np
import pandas as pd


def robust_z_cross_sectional(s: pd.Series) -> pd.Series:
    v = pd.to_numeric(s, errors="coerce")
    m = v.median(skipna=True)
    mad = (v - m).abs().median(skipna=True)
    scale = 1.4826 * float(mad) if pd.notna(mad) and mad > 0 else float(v.std(skipna=True) or 1.0)
    z = (v - m) / scale if scale > 0 else v * 0.0
    return z.clip(lower=-3.0, upper=3.0)


def price_features_r10d_range_expansion(close: pd.Series) -> pd.DataFrame:
    c = close.astype(float)
    logret = np.log(c / c.shift(1))
    sigma5 = logret.rolling(5, min_periods=5).std(ddof=1)
    sigma63 = logret.rolling(63, min_periods=63).std(ddof=1)
    out = pd.DataFrame(index=c.index)
    out["r_10d"] = np.log(c / c.shift(10))
    out["range_expansion"] = sigma5 / sigma63.replace(0, np.nan)
    return out


def h_star_from_z_composite(
    z_composite: float,
    *,
    h_base: float = 0.75,
    k: float = 0.05,
    h_min: float = 0.10,
    h_max: float = 1.10,
) -> float:
    if not np.isfinite(z_composite):
        return float(h_base)
    return float(np.clip(h_base - k * float(z_composite), h_min, h_max))


def ema_update_h(h_prev: float, h_star: float, alpha: float) -> float:
    a = float(np.clip(alpha, 0.0, 1.0))
    return float((1.0 - a) * h_prev + a * h_star)


def cross_sectional_b4_signals(
    underlying_to_close: dict[str, pd.Series],
    asof: pd.Timestamp,
    *,
    min_names: int = 5,
) -> tuple[dict[str, float], dict[str, float], dict[str, float]]:
    r10: dict[str, float] = {}
    rexp: dict[str, float] = {}
    for u, ser in underlying_to_close.items():
        if ser is None or len(ser) < 70:
            continue
        s = ser.loc[:asof].dropna()
        if len(s) < 70:
            continue
        feats = price_features_r10d_range_expansion(s)
        try:
            row = feats.iloc[-1]
            v10 = float(row["r_10d"])
            vr = float(row["range_expansion"])
        except (IndexError, KeyError, TypeError, ValueError):
            continue
        if np.isfinite(v10):
            r10[u] = v10
        if np.isfinite(vr):
            rexp[u] = vr
    common = sorted(set(r10) & set(rexp))
    if len(common) < int(min_names):
        return {}, r10, rexp
    s10 = pd.Series({u: r10[u] for u in common})
    sr = pd.Series({u: rexp[u] for u in common})
    z10 = robust_z_cross_sectional(s10)
    zr = robust_z_cross_sectional(sr)
    aligned_r10d = -1.0 * z10
    aligned_rexp = 1.0 * zr
    zc = (aligned_r10d + aligned_rexp) / 2.0
    return zc.to_dict(), r10, rexp


def update_b4_h_state(
    h_prev_by_u: dict[str, float],
    b4_underlyings: set[str],
    prices_by_sym: dict[str, pd.Series],
    asof: pd.Timestamp,
    *,
    h_base: float = 0.75,
    k: float = 0.05,
    alpha: float = 0.25,
    h_min: float = 0.10,
    h_max: float = 1.10,
    min_names: int = 5,
) -> tuple[dict[str, float], dict[str, float | None], dict[str, float | None]]:
    und_close = {u: prices_by_sym.get(u) for u in b4_underlyings}
    zc_map, _r10, _rexp = cross_sectional_b4_signals(und_close, asof, min_names=min_names)
    z_out: dict[str, float | None] = {}
    h_star_out: dict[str, float | None] = {}
    h_new = dict(h_prev_by_u)
    if not zc_map:
        return h_new, {u: None for u in b4_underlyings}, {u: None for u in b4_underlyings}
    for u in b4_underlyings:
        zv = zc_map.get(u)
        z_out[u] = float(zv) if zv is not None and np.isfinite(zv) else None
        hp = float(h_prev_by_u.get(u, h_base))
        if z_out[u] is None:
            h_star_out[u] = None
            h_new[u] = hp
        else:
            hs = h_star_from_z_composite(z_out[u], h_base=h_base, k=k, h_min=h_min, h_max=h_max)
            h_star_out[u] = hs
            h_new[u] = ema_update_h(hp, hs, alpha)
    return h_new, z_out, h_star_out


def effective_h_for_pair(
    und: str,
    h_by_u: dict[str, float],
    *,
    yaml_partial: float = 1.0,
    h_base: float = 0.75,
) -> float:
    h0 = float(h_by_u.get(und, h_base))
    p = float(np.clip(yaml_partial, 0.0, 1.0))
    return float(np.clip(h0 * p, 0.0, 1.0))

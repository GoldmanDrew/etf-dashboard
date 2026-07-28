"""Per-pair cash-residual sizing path for Optimized research.

``scale_to_budget=false``: crash caps trim gross; freed dollars stay cash.
Optional h-first: bump hedge before cutting gross when the cap binds.
Cadence: only update applied gross on rebalance (DUE) days unless an
emergency relative cut fires.

Neutral research overlay — not production book PnL.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

import numpy as np
import pandas as pd

TRADING_DAYS = 252.0


@dataclass
class CashResidualParams:
    rho: float = 0.0075
    theta: float = 0.5
    phi: float = 0.5
    l_floor: float = 0.02
    anchor_window: int = 252
    anchor_min_obs: int = 126
    tail_horizon: int = 20
    tail_lookback: int = 756
    tail_min_obs: int = 40
    downside_vol_lookback: int = 126
    downside_vol_blend: float = 0.45
    l_ema_alpha: float = 0.4
    emergency_cut_rel: float = 0.25
    # h_first
    h_first_enabled: bool = True
    kappa: float = 0.5
    h_max: float = 1.0
    runup_min: float = 0.25
    edge_floor: float = 0.0  # Optimized research: don't gate on edge by default
    # Residual crash vs already-realized peak drawdown (see docs/cash_residual_drawdown_shrink_C_plan.md)
    drawdown_shrink_enabled: bool = True
    peak_window: int = 756
    peak_min_obs: int = 40
    dd_floor_abs: float = 0.10
    dd_floor_frac: float = 0.30
    # v2 h-first gate: also allow when deep in drawdown or residual crash has shrunk
    # (avoids emergency_cut into selloffs when runup < runup_min).
    h_first_on_drawdown: bool = True
    dd_h_first_min: float = 0.25
    shrink_h_first_frac: float = 0.70
    # Do not unlock via DD/shrink when crash size is outside a sensible envelope
    # (e.g. ETHD D≈1 with T>1 → C≫1; hedging to h=1 would fake L≈0).
    c_h_first_max: float = 1.0
    dd_h_first_max: float = 0.95


def residual_crash_from_drawdown(tail: float, drawdown: float) -> float:
    """Remaining fractional crash to the same peak-envelope floor: (T-D)/(1-D)."""
    T = max(0.0, float(tail))
    D = float(np.clip(drawdown, 0.0, 1.0 - 1e-12))
    return max(0.0, (T - D) / (1.0 - D))


def conditional_crash_stats(close: pd.Series, p: CashResidualParams) -> dict[str, float] | None:
    c = pd.to_numeric(close, errors="coerce").dropna().astype(float)
    runup = np.nan
    if len(c) >= p.anchor_min_obs:
        anchor = float(c.iloc[-int(p.anchor_window) :].median())
        if anchor > 0:
            runup = max(0.0, float(c.iloc[-1]) / anchor - 1.0)
    tail = np.nan
    if len(c) >= max(p.tail_min_obs, p.tail_horizon + 5):
        hret = c.pct_change(p.tail_horizon).dropna().iloc[-int(p.tail_lookback) :]
        worst = max(0.0, -float(hret.min())) if len(hret) else 0.0
        dret = c.pct_change().dropna().iloc[-int(p.downside_vol_lookback) :]
        down = dret[dret < 0.0]
        dvol = float(down.std(ddof=1) * np.sqrt(TRADING_DAYS)) if len(down) >= 5 else 0.0
        tail = worst + p.downside_vol_blend * dvol
    if not (np.isfinite(runup) or np.isfinite(tail)):
        return None
    ru = runup if np.isfinite(runup) else 0.0
    retrace = p.theta * ru / (1.0 + ru)
    T_raw = float(tail) if np.isfinite(tail) else 0.0

    peak = float("nan")
    D = 0.0
    T_shrunk = T_raw
    T_floor = T_raw
    dd_reason = "disabled"
    if p.drawdown_shrink_enabled and np.isfinite(T_raw):
        wp = max(2, int(p.peak_window))
        peak_slice = c.iloc[-wp:]
        if len(peak_slice) >= int(p.peak_min_obs):
            peak = float(peak_slice.max())
            pt = float(c.iloc[-1])
            if peak > 0 and pt > 0:
                D = max(0.0, 1.0 - pt / peak)
                T_shrunk = residual_crash_from_drawdown(T_raw, D)
                T_floor = max(float(p.dd_floor_abs), float(p.dd_floor_frac) * T_raw)
                # Floor only after a realized drawdown — at peak (D≈0) match legacy C.
                if D > 1e-12:
                    T_eff = max(T_shrunk, T_floor)
                    dd_reason = "applied"
                else:
                    T_eff = T_raw
                    T_shrunk = T_raw
                    dd_reason = "at_peak"
            else:
                T_eff = T_raw
                dd_reason = "bad_peak_or_spot"
        else:
            T_eff = T_raw
            dd_reason = "insufficient_peak_history"
    else:
        T_eff = T_raw

    crash = max(T_eff, retrace)
    return {
        "runup": float(ru),
        "tail": float(T_raw),
        "tail_shrunk": float(T_shrunk) if np.isfinite(T_shrunk) else float(T_raw),
        "tail_floor": float(T_floor) if np.isfinite(T_floor) else float(T_raw),
        "tail_effective": float(T_eff),
        "retrace": float(retrace),
        "peak": float(peak) if np.isfinite(peak) else None,
        "drawdown": float(D),
        "dd_shrink_reason": dd_reason,
        "C": float(crash),
        "C_raw": float(max(T_raw, retrace)),
    }


def pair_loss(crash: float, h: float, beta: float, phi: float) -> float:
    c = max(0.0, float(crash))
    hh = float(np.clip(h, 0.0, 1.0))
    b = max(0.1, abs(float(beta)))
    return (1.0 - hh) * b / (1.0 + hh * b) * c * (1.0 + float(phi) * c)


def min_h_for_crash_budget(
    *,
    gross_usd: float,
    budget_usd: float,
    rho: float,
    C: float,
    beta: float,
    phi: float,
    h0: float,
    h_max: float,
    l_floor: float,
) -> float | None:
    """Smallest h ≥ h0 that makes cap ≥ gross (cash residual). None if impossible."""
    if gross_usd <= 0 or budget_usd <= 0 or C <= 0:
        return float(h0)
    # Need L <= rho * budget / gross
    l_need = float(rho) * float(budget_usd) / float(gross_usd)
    if l_need <= l_floor:
        return float(h0)
    # Solve pair_loss(C,h,beta,phi) = l_need for h (monotone decreasing in h)
    lo, hi = float(h0), float(h_max)
    if pair_loss(C, hi, beta, phi) > l_need + 1e-12:
        return None
    if pair_loss(C, lo, beta, phi) <= l_need + 1e-12:
        return lo
    for _ in range(40):
        mid = 0.5 * (lo + hi)
        if pair_loss(C, mid, beta, phi) > l_need:
            lo = mid
        else:
            hi = mid
    return float(hi)


def size_day(
    *,
    und_close: pd.Series,
    h0: float,
    beta: float,
    sleeve_budget_usd: float,
    pair_target_usd: float,
    edge_annual: float | None,
    params: CashResidualParams,
    l_ema_prev: float | None,
) -> dict[str, Any]:
    """Cash-residual size for one as-of day (no cadence freeze)."""
    stats = conditional_crash_stats(und_close, params)
    if stats is None:
        return {
            "gross_target_usd": float(pair_target_usd),
            "gross_applied_usd": float(pair_target_usd),
            "crash_mult": 1.0,
            "L": None,
            "L_raw": None,
            "runup": None,
            "C": None,
            "C_raw": None,
            "tail": None,
            "tail_shrunk": None,
            "drawdown": None,
            "peak": None,
            "dd_shrink_reason": "no_crash_stats",
            "h0": float(h0),
            "h1": float(h0),
            "h_first_reason": "no_crash_stats",
            "cap_usd": None,
        }
    C = float(stats["C"])
    runup = float(stats["runup"])
    L_raw = pair_loss(C, h0, beta, params.phi)
    if l_ema_prev is not None and np.isfinite(l_ema_prev) and params.l_ema_alpha < 1.0 - 1e-12:
        a = float(params.l_ema_alpha)
        if L_raw > float(l_ema_prev):
            L = L_raw  # risk-up immediate
        else:
            L = a * L_raw + (1.0 - a) * float(l_ema_prev)
    else:
        L = L_raw
    cap = float(params.rho) * float(sleeve_budget_usd) / max(L, float(params.l_floor))
    target = float(pair_target_usd)
    h1 = float(h0)
    reason = "no_bind"
    binds = target > cap * (1.0 + 1e-9)
    if binds and params.h_first_enabled:
        edge_ok = edge_annual is None or (
            np.isfinite(float(edge_annual)) and float(edge_annual) >= float(params.edge_floor)
        )
        runup_ok = runup >= float(params.runup_min)
        D = float(stats.get("drawdown") or 0.0)
        T_raw = float(stats.get("tail") or 0.0)
        T_shrunk = float(
            stats.get("tail_shrunk") if stats.get("tail_shrunk") is not None else T_raw
        )
        patho = C > float(params.c_h_first_max) or D > float(params.dd_h_first_max)
        dd_ok = (
            bool(params.h_first_on_drawdown)
            and (not patho)
            and D >= float(params.dd_h_first_min)
        )
        shrink_ok = (
            bool(params.h_first_on_drawdown)
            and (not patho)
            and D > 1e-12
            and T_raw > 1e-12
            and T_shrunk <= float(params.shrink_h_first_frac) * T_raw
        )
        gate_ok = runup_ok or dd_ok or shrink_ok
        if not gate_ok:
            reason = "patho_gate" if patho and not runup_ok else "runup_gate"
        elif not edge_ok:
            reason = "edge_gate"
        else:
            h_solve = min_h_for_crash_budget(
                gross_usd=target,
                budget_usd=sleeve_budget_usd,
                rho=params.rho,
                C=C,
                beta=beta,
                phi=params.phi,
                h0=h0,
                h_max=params.h_max,
                l_floor=params.l_floor,
            )
            retrace = float(stats["retrace"])
            h_bump = float(np.clip(h0 + params.kappa * retrace, 0.0, params.h_max))
            if h_solve is not None:
                h1 = max(h_solve, h_bump) if h_bump > h0 else h_solve
                h1 = float(np.clip(h1, h0, params.h_max))
                if runup_ok:
                    reason = "h_first_solve"
                elif dd_ok:
                    reason = "h_first_solve_dd"
                else:
                    reason = "h_first_solve_shrink"
            else:
                h1 = float(params.h_max)
                reason = "h_max_then_cut"
            L = pair_loss(C, h1, beta, params.phi)
            if l_ema_prev is not None and np.isfinite(l_ema_prev) and params.l_ema_alpha < 1.0 - 1e-12:
                a = float(params.l_ema_alpha)
                if L > float(l_ema_prev):
                    pass
                else:
                    L = a * L + (1.0 - a) * float(l_ema_prev)
            cap = float(params.rho) * float(sleeve_budget_usd) / max(L, float(params.l_floor))
    elif binds:
        reason = "crash_cap"
    applied = min(target, cap)
    mult = applied / target if target > 1e-9 else 1.0
    return {
        "gross_target_usd": target,
        "gross_applied_usd": float(applied),
        "crash_mult": float(mult),
        "L": float(L),
        "L_raw": float(L_raw),
        "runup": runup,
        "C": C,
        "C_raw": float(stats.get("C_raw") if stats.get("C_raw") is not None else C),
        "tail": stats.get("tail"),
        "tail_shrunk": stats.get("tail_shrunk"),
        "tail_floor": stats.get("tail_floor"),
        "drawdown": stats.get("drawdown"),
        "peak": stats.get("peak"),
        "dd_shrink_reason": stats.get("dd_shrink_reason"),
        "h0": float(h0),
        "h1": float(h1),
        "h_first_reason": reason,
        "cap_usd": float(cap),
    }


def build_cash_residual_pins(
    *,
    dates: list[str] | pd.DatetimeIndex,
    rebalance: list[int] | pd.Series,
    h_series: Mapping[str, float] | pd.Series,
    und_close: pd.Series,
    beta: float,
    sleeve_budget_usd: float,
    pair_weight: float,
    edge_annual: float | None = None,
    params: CashResidualParams | None = None,
) -> dict[str, Any]:
    """Walk the calendar; emit pin maps + parallel telemetry (ffilled)."""
    p = params or CashResidualParams()
    idx = pd.DatetimeIndex([pd.Timestamp(d) for d in dates])
    if isinstance(h_series, pd.Series):
        h_map = {pd.Timestamp(k).strftime("%Y-%m-%d"): float(v) for k, v in h_series.dropna().items()}
    else:
        h_map = {str(k)[:10]: float(v) for k, v in dict(h_series).items()}
    rb = list(rebalance) if not isinstance(rebalance, pd.Series) else [int(x) for x in rebalance.tolist()]
    while len(rb) < len(idx):
        rb.append(0)
    und = pd.to_numeric(und_close, errors="coerce").astype(float)
    und.index = pd.DatetimeIndex(und.index)

    pair_target = float(sleeve_budget_usd) * max(0.0, float(pair_weight))
    target_gross: dict[str, float] = {}
    h_target: dict[str, float] = {}

    n = len(idx)
    gross_target = [None] * n
    gross_applied = [None] * n
    delta_gross = [None] * n
    residual_usd = [None] * n
    residual_pct = [None] * n
    crash_mult = [None] * n
    L_arr = [None] * n
    runup_arr = [None] * n
    C_arr = [None] * n
    C_raw_arr = [None] * n
    tail_arr = [None] * n
    tail_shrunk_arr = [None] * n
    drawdown_arr = [None] * n
    h0_arr = [None] * n
    h1_arr = [None] * n
    cadence_due = [0] * n
    reason_arr = [""] * n

    applied_prev: float | None = None
    l_ema: float | None = None
    frozen_target = pair_target
    frozen_applied = pair_target
    frozen_h = 0.45
    frozen_meta: dict[str, Any] = {}

    for i, dt in enumerate(idx):
        ds = dt.strftime("%Y-%m-%d")
        due = bool(rb[i]) or i == 0
        h0 = float(h_map.get(ds, frozen_h if frozen_h is not None else 0.45))
        close_slice = und.loc[:dt].dropna()
        sized = size_day(
            und_close=close_slice,
            h0=h0,
            beta=beta,
            sleeve_budget_usd=sleeve_budget_usd,
            pair_target_usd=pair_target,
            edge_annual=edge_annual,
            params=p,
            l_ema_prev=l_ema,
        )
        if sized.get("L") is not None:
            l_ema = float(sized["L"])

        proposed = float(sized["gross_applied_usd"])
        emergency = False
        if applied_prev is not None and applied_prev > 1e-9:
            rel_cut = (applied_prev - proposed) / applied_prev
            emergency = rel_cut >= float(p.emergency_cut_rel)

        apply_today = due or emergency or applied_prev is None
        if apply_today:
            frozen_target = float(sized["gross_target_usd"])
            frozen_applied = proposed
            frozen_h = float(sized["h1"])
            frozen_meta = sized
            reason = str(sized.get("h_first_reason") or "")
            if emergency and not due:
                reason = (reason + "+emergency_cut") if reason else "emergency_cut"
            elif due and not emergency:
                reason = reason or "cadence_due"
        else:
            reason = "cadence_freeze"

        delta = None if applied_prev is None else frozen_applied - applied_prev
        applied_prev = frozen_applied
        resid = max(0.0, frozen_target - frozen_applied)
        resid_pct = 100.0 * resid / frozen_target if frozen_target > 1e-9 else 0.0

        target_gross[ds] = frozen_applied  # engine pins *applied* gross
        h_target[ds] = frozen_h

        gross_target[i] = frozen_target
        gross_applied[i] = frozen_applied
        delta_gross[i] = delta
        residual_usd[i] = resid
        residual_pct[i] = resid_pct
        crash_mult[i] = float(frozen_meta.get("crash_mult") or 1.0)
        L_arr[i] = frozen_meta.get("L")
        runup_arr[i] = frozen_meta.get("runup")
        C_arr[i] = frozen_meta.get("C")
        C_raw_arr[i] = frozen_meta.get("C_raw")
        tail_arr[i] = frozen_meta.get("tail")
        tail_shrunk_arr[i] = frozen_meta.get("tail_shrunk")
        drawdown_arr[i] = frozen_meta.get("drawdown")
        h0_arr[i] = float(frozen_meta.get("h0") or h0)
        h1_arr[i] = frozen_h
        cadence_due[i] = 1 if due else 0
        reason_arr[i] = reason

    # Ffill telemetry for non-event days already held frozen values above.
    n_add = sum(1 for d in delta_gross if d is not None and d > 1e-6)
    n_cut = sum(1 for d in delta_gross if d is not None and d < -1e-6)
    end_applied = next((g for g in reversed(gross_applied) if g is not None), None)
    end_resid = next((g for g in reversed(residual_pct) if g is not None), 0.0)

    return {
        "target_gross_by_date": target_gross,
        "h_target_by_date": h_target,
        "telemetry": {
            "gross_target_usd": gross_target,
            "gross_applied_usd": gross_applied,
            "delta_gross_usd": delta_gross,
            "cash_residual_usd": residual_usd,
            "cash_residual_pct": residual_pct,
            "crash_mult": crash_mult,
            "L": L_arr,
            "runup": runup_arr,
            "C": C_arr,
            "C_raw": C_raw_arr,
            "tail": tail_arr,
            "tail_shrunk": tail_shrunk_arr,
            "drawdown": drawdown_arr,
            "h0": h0_arr,
            "h1": h1_arr,
            "cadence_due": cadence_due,
            "reason": reason_arr,
        },
        "summary": {
            "pair_target_usd": pair_target,
            "sleeve_budget_usd": float(sleeve_budget_usd),
            "pair_weight": float(pair_weight),
            "end_gross_applied_usd": end_applied,
            "end_cash_residual_pct": end_resid,
            "n_cadence_adds": n_add,
            "n_cadence_cuts": n_cut,
            "n_days": n,
            "scale_to_budget": False,
            "h_first_enabled": bool(p.h_first_enabled),
            "drawdown_shrink_enabled": bool(p.drawdown_shrink_enabled),
            "rho": float(p.rho),
        },
    }

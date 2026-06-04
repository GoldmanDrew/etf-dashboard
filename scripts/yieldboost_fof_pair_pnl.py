"""Realized and forward pair metrics for YieldBOOST fund-of-funds (YBTY/YBST)."""
from __future__ import annotations

import math
from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from yieldboost_fof_constants import YIELDBOOST_FOF_SYMBOLS
from yieldboost_fof_holdings import build_fof_holdings_history, stale_days

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_TRADING_DAYS = 252
_HORIZON_DAYS = {
    "5D": 5,
    "20D": 20,
    "60D": 60,
    "120D": 120,
    "251D": 251,
}


def _norm_sym(s: object) -> str:
    return str(s or "").strip().upper().replace(".", "-")


def _load_metrics_daily(path: Path | None = None) -> pd.DataFrame:
    path = path or (_DATA_DIR / "etf_metrics_daily.csv")
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        if "date" in df.columns:
            df["date"] = df["date"].astype(str).str.slice(0, 10)
        return df
    except Exception:
        return pd.DataFrame()


def _fof_nav_series(metrics: pd.DataFrame, fof_symbol: str) -> pd.Series:
    sym = _norm_sym(fof_symbol)
    sub = metrics[metrics["ticker"].astype(str).str.upper() == sym].copy()
    if sub.empty:
        return pd.Series(dtype=float)
    sub = sub.sort_values("date")
    if "etf_adj_close" in sub.columns:
        px = pd.to_numeric(sub["etf_adj_close"], errors="coerce")
    else:
        px = pd.Series(index=sub.index, dtype=float)
    if px.notna().sum() < 3:
        px = pd.to_numeric(sub["nav"], errors="coerce")
    if px.notna().sum() < 3:
        px = pd.to_numeric(sub.get("close_price"), errors="coerce")
    out = pd.Series(px.values, index=sub["date"].astype(str))
    return out.dropna()


def _underlying_tr_series(metrics: pd.DataFrame, underlying: str) -> pd.Series:
    und = _norm_sym(underlying)
    if metrics.empty:
        return pd.Series(dtype=float)
    if "underlying" in metrics.columns:
        sub = metrics[metrics["underlying"].astype(str).str.upper() == und].copy()
    else:
        sub = metrics.copy()
    if sub.empty:
        return pd.Series(dtype=float)
    sub = sub.sort_values("date")
    by_date = sub.groupby("date", sort=True)["underlying_adj_close"].first()
    px = pd.to_numeric(by_date, errors="coerce").dropna()
    return px


def _weights_for_date(
    history_snaps: list[dict[str, Any]],
    d: str,
) -> dict[str, float]:
    """Piecewise-constant weights: latest basket as-of on or before ``d``."""
    applicable = [s for s in history_snaps if str(s.get("as_of") or "") <= d]
    snap = applicable[-1] if applicable else (history_snaps[0] if history_snaps else None)
    if not snap:
        return {}
    return dict(snap.get("underlying_weights") or {})


def compute_fof_realized_pair_metrics(
    fof_symbol: str,
    history_snaps: list[dict[str, Any]],
    metrics: pd.DataFrame,
    *,
    borrow_annual: float | None = None,
) -> dict[str, Any]:
    """Short FoF vs weighted underlying long basket — log-drag series."""
    fof_px = _fof_nav_series(metrics, fof_symbol)
    if fof_px.empty or len(fof_px) < 5:
        return {"ok": False, "error": "insufficient FoF price history"}

    all_dates = sorted(fof_px.index.tolist())
    und_series: dict[str, pd.Series] = {}
    weights0 = _weights_for_date(history_snaps, all_dates[-1]) if history_snaps else {}
    for und in weights0:
        s = _underlying_tr_series(metrics, und)
        if not s.empty:
            und_series[und] = s

    if not und_series:
        return {"ok": False, "error": "no underlying price series for basket"}

    daily_drags: list[tuple[str, float]] = []
    for i in range(1, len(all_dates)):
        d0, d1 = all_dates[i - 1], all_dates[i]
        wmap = _weights_for_date(history_snaps, d1)
        if not wmap:
            continue
        try:
            r_fof = float(fof_px.loc[d1]) / float(fof_px.loc[d0])
        except (KeyError, ZeroDivisionError, TypeError):
            continue
        if not math.isfinite(r_fof) or r_fof <= 0:
            continue
        basket_log = 0.0
        wsum = 0.0
        for und, w in wmap.items():
            s = und_series.get(und)
            if s is None or d0 not in s.index or d1 not in s.index:
                continue
            try:
                r_u = float(s.loc[d1]) / float(s.loc[d0])
            except (KeyError, ZeroDivisionError, TypeError):
                continue
            if not math.isfinite(r_u) or r_u <= 0:
                continue
            basket_log += w * math.log(r_u)
            wsum += w
        if wsum <= 0:
            continue
        basket_log /= wsum
        # Short-favorable: long basket minus short FoF (same sign as gross_decay).
        drag = basket_log - math.log(r_fof)
        daily_drags.append((d1, drag))

    if len(daily_drags) < 3:
        return {"ok": False, "error": "insufficient aligned pair days"}

    drags = np.array([x[1] for x in daily_drags], dtype=float)
    gross_annual = float(np.mean(drags) * _TRADING_DAYS)

    horizons: list[dict[str, Any]] = []
    for label, n in _HORIZON_DAYS.items():
        if len(drags) < n + 1:
            continue
        window = drags[-n:]
        gross_h = float(np.sum(window))
        borrow_h = 0.0
        if borrow_annual is not None and math.isfinite(borrow_annual):
            borrow_h = float(borrow_annual) * (n / _TRADING_DAYS)
        horizons.append({
            "label": label,
            "days": n,
            "horizonDays": n,
            "gross": round(gross_h, 6),
            "net": round(gross_h - borrow_h, 6),
            "grossSimple": round(math.expm1(gross_h), 6),
            "netSimple": round(math.expm1(gross_h - borrow_h), 6),
            "borrow_drag": round(borrow_h, 6),
            "borrowLog": round(-borrow_h, 6),
            "sufficient": True,
        })

    return {
        "ok": True,
        "gross_decay_annual": round(gross_annual, 6),
        "n_days": len(drags),
        "start_date": daily_drags[0][0],
        "end_date": daily_drags[-1][0],
        "horizons": horizons,
        "daily_drag_mean": round(float(np.mean(drags)), 8),
    }


def weighted_child_forward_metrics(
    basket: dict[str, Any],
    child_records: dict[str, dict[str, Any]],
) -> dict[str, float | None]:
    """Blend child screener forward pair / net edge by FoF child weights."""
    children = basket.get("children") or []
    if not children:
        return {
            "expected_pair_pnl_p50_annual": None,
            "expected_pair_pnl_p10_annual": None,
            "expected_pair_pnl_p90_annual": None,
            "net_edge_p50_annual": None,
            "effective_beta": None,
        }
    wsum = sum(float(c.get("weight_pct") or 0) for c in children)
    if wsum <= 0:
        wsum = 100.0
    acc = {
        "expected_pair_pnl_p50_annual": 0.0,
        "expected_pair_pnl_p10_annual": 0.0,
        "expected_pair_pnl_p90_annual": 0.0,
        "net_edge_p50_annual": 0.0,
        "effective_beta": 0.0,
    }
    got = {k: False for k in acc}
    for c in children:
        yb = _norm_sym(c.get("yb_etf"))
        w = float(c.get("weight_pct") or 0) / wsum
        if w <= 0:
            continue
        rec = child_records.get(yb) or {}
        for key in (
            "expected_pair_pnl_p50_annual",
            "expected_pair_pnl_p10_annual",
            "expected_pair_pnl_p90_annual",
            "net_edge_p50_annual",
        ):
            v = rec.get(key)
            if v is None:
                continue
            try:
                fv = float(v)
                if math.isfinite(fv):
                    acc[key] += w * fv
                    got[key] = True
            except (TypeError, ValueError):
                pass
        d = rec.get("delta")
        if d is not None:
            try:
                fd = float(d)
                if math.isfinite(fd):
                    acc["effective_beta"] += w * fd
                    got["effective_beta"] = True
            except (TypeError, ValueError):
                pass

    return {
        k: (round(acc[k], 6) if got[k] else None)
        for k in acc
    }


def build_fof_dashboard_record(
    fof_symbol: str,
    *,
    basket: dict[str, Any],
    history_snaps: list[dict[str, Any]],
    child_records: dict[str, dict[str, Any]],
    metrics: pd.DataFrame,
    borrow_current: float | None,
    shares_available: int | None,
    borrow_source: str = "ibkr_ftp",
) -> dict[str, Any] | None:
    sym = _norm_sym(fof_symbol)
    if not basket:
        return None

    fwd = weighted_child_forward_metrics(basket, child_records)
    realized = compute_fof_realized_pair_metrics(
        sym,
        history_snaps,
        metrics,
        borrow_annual=borrow_current,
    )

    und_weights = basket.get("underlying_weights") or {}
    und_list = sorted(und_weights.keys())
    underlying_display = "BASKET" if len(und_list) != 1 else und_list[0]

    gross = realized.get("gross_decay_annual") if realized.get("ok") else None
    exp_p50 = fwd.get("expected_pair_pnl_p50_annual")
    child_ne = fwd.get("net_edge_p50_annual")
    net_edge = None
    if child_ne is not None and borrow_current is not None:
        try:
            net_edge = round(float(child_ne) - float(borrow_current), 6)
        except (TypeError, ValueError):
            net_edge = child_ne
    elif child_ne is not None:
        net_edge = child_ne

    stale = stale_days(basket.get("as_of"))

    fof_basket = {
        **basket,
        "effective_beta": fwd.get("effective_beta"),
        "weights_stale_days": stale,
        "history_points": len(history_snaps),
    }

    rec: dict[str, Any] = {
        "symbol": sym,
        "underlying": underlying_display,
        "delta": fwd.get("effective_beta"),
        "delta_n_obs": realized.get("n_days") if realized.get("ok") else None,
        "bucket": "bucket_2_low_beta",
        "is_yieldboost": True,
        "is_dashboard_synthetic": True,
        "scenario_style": "income_style_fof",
        "product_class": "income_yieldboost_fof",
        "expected_decay_available": True,
        "gross_decay_annual": gross,
        "expected_gross_decay_annual": exp_p50,
        "expected_gross_decay_p50_annual": exp_p50,
        "expected_gross_decay_p10_annual": fwd.get("expected_pair_pnl_p10_annual"),
        "expected_gross_decay_p90_annual": fwd.get("expected_pair_pnl_p90_annual"),
        "expected_pair_pnl_p50_annual": exp_p50,
        "expected_pair_pnl_p10_annual": fwd.get("expected_pair_pnl_p10_annual"),
        "expected_pair_pnl_p90_annual": fwd.get("expected_pair_pnl_p90_annual"),
        "expected_pair_pnl_basis": "fof_weighted_child_forward",
        "expected_pair_pnl_units": "log_continuous_annual",
        "net_edge_p50_annual": net_edge,
        "borrow_current": borrow_current,
        "borrow_fee_annual": borrow_current,
        "borrow_net_annual": borrow_current,
        "shares_available": shares_available,
        "borrow_source": borrow_source,
        "decomposition_note": "fof_synthetic_dashboard_v1",
        "gross_edge_definition": "fof_weighted_basket_realized",
        "schema_v": 4,
        "edge_sign_convention": "short_favorable_positive",
        "fof_basket": fof_basket,
        "fof_realized_pair": realized if realized.get("ok") else {"ok": False, "error": realized.get("error")},
    }
    if und_list:
        rec["fof_underlyings"] = [
            {"underlying": u, "weight": und_weights[u]} for u in und_list
        ]
    return rec


def build_all_fof_dashboard_records(
    *,
    holdings_df: pd.DataFrame,
    child_records_by_sym: dict[str, dict[str, Any]],
    ibkr_borrow: dict[str, Any] | None = None,
    metrics_path: Path | None = None,
) -> list[dict[str, Any]]:
    history_by_sym = build_fof_holdings_history(holdings_df)
    metrics = _load_metrics_daily(metrics_path)
    ibkr_borrow = ibkr_borrow or {}
    borrow_map = ibkr_borrow.get("borrow_map") or {}
    avail_map = ibkr_borrow.get("available_map") or {}
    out: list[dict[str, Any]] = []
    for sym in YIELDBOOST_FOF_SYMBOLS:
        snaps = history_by_sym.get(_norm_sym(sym)) or []
        basket = snaps[-1] if snaps else None
        if not basket:
            continue
        bc = borrow_map.get(sym)
        sa = avail_map.get(sym)
        rec = build_fof_dashboard_record(
            sym,
            basket=basket,
            history_snaps=snaps,
            child_records=child_records_by_sym,
            metrics=metrics,
            borrow_current=float(bc) if bc is not None else None,
            shares_available=int(sa) if sa is not None else None,
            borrow_source="ibkr_ftp" if sym in borrow_map else "missing",
        )
        if rec:
            out.append(rec)
    return out

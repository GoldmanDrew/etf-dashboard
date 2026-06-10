"""Realized and forward pair metrics for YieldBOOST fund-of-funds (YBTY/YBST)."""
from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from yieldboost_fof_constants import (
    FOF_DEFAULT_EXPENSE_RATIO_ANNUAL,
    YIELDBOOST_CHILD_TO_UNDERLYING,
    YIELDBOOST_FOF_SYMBOLS,
)
from yieldboost_fof_forward import (
    bootstrap_fof_net_edge,
    build_fof_income_distribution_calibration,
    build_fof_income_scenario_grid,
    build_fof_pair_scenario_grid,
    weighted_child_forecast_vol,
    weighted_child_nav_decay_forward,
    weighted_child_pair_pnl_blend,
)
from yieldboost_fof_basket_series import (
    enrich_fof_dashboard_extras,
    resolve_fof_price_series,
)

try:
    from realized_gross_decay import realized_pair_gross_60d_fields
except ImportError:
    realized_pair_gross_60d_fields = None  # type: ignore[misc, assignment]

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
    """FoF price path: issuer NAV first, then Yahoo adj close, then raw close."""
    sym = _norm_sym(fof_symbol)
    sub = metrics[metrics["ticker"].astype(str).str.upper() == sym].copy()
    if sub.empty:
        return pd.Series(dtype=float)
    sub = sub.sort_values("date")
    px = pd.to_numeric(sub.get("nav"), errors="coerce")
    if px.notna().sum() < 3 and "etf_adj_close" in sub.columns:
        px = pd.to_numeric(sub["etf_adj_close"], errors="coerce")
    if px.notna().sum() < 3:
        px = pd.to_numeric(sub.get("close_price"), errors="coerce")
    out = pd.Series(px.values, index=sub["date"].astype(str))
    return out.dropna()


def _tickers_for_underlying(underlying: str) -> list[str]:
    und = _norm_sym(underlying)
    tickers = [t for t, u in YIELDBOOST_CHILD_TO_UNDERLYING.items() if _norm_sym(u) == und]
    if und not in tickers:
        tickers.append(und)
    return [_norm_sym(t) for t in tickers]


def _underlying_tr_series(metrics: pd.DataFrame, underlying: str) -> pd.Series:
    """Global underlying panel: first ``underlying_adj_close`` per date across child ETF rows."""
    if metrics.empty or "underlying_adj_close" not in metrics.columns:
        return pd.Series(dtype=float)
    tickers = _tickers_for_underlying(underlying)
    sub = metrics[metrics["ticker"].astype(str).str.upper().isin(tickers)].copy()
    if sub.empty:
        return pd.Series(dtype=float)
    sub = sub.sort_values("date")
    by_date = sub.groupby("date", sort=True)["underlying_adj_close"].first()
    px = pd.to_numeric(by_date, errors="coerce").dropna()
    return px


def _cash_fraction_from_snap(snap: dict[str, Any] | None) -> float:
    if not snap:
        return 0.0
    raw = snap.get("cash_pct")
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(v):
        return 0.0
    if v > 1.0:
        return min(1.0, max(0.0, v / 100.0))
    return min(1.0, max(0.0, v))


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


def _snap_for_date(history_snaps: list[dict[str, Any]], d: str) -> dict[str, Any] | None:
    applicable = [s for s in history_snaps if str(s.get("as_of") or "") <= d]
    return applicable[-1] if applicable else (history_snaps[0] if history_snaps else None)


def compute_fof_realized_pair_metrics(
    fof_symbol: str,
    history_snaps: list[dict[str, Any]],
    metrics: pd.DataFrame,
    *,
    borrow_annual: float | None = None,
    cash_return_annual: float = 0.0,
) -> dict[str, Any]:
    """Short FoF vs weighted underlying long basket — log-drag series."""
    fof_px = resolve_fof_price_series(fof_symbol, history_snaps, metrics)
    if fof_px.empty or len(fof_px) < 5:
        return {"ok": False, "error": "insufficient FoF price history"}

    all_dates = sorted(fof_px.index.tolist())
    weights0 = _weights_for_date(history_snaps, all_dates[-1]) if history_snaps else {}
    und_series: dict[str, pd.Series] = {}
    for und in weights0:
        s = _underlying_tr_series(metrics, und)
        if not s.empty:
            und_series[und] = s

    if not und_series:
        return {"ok": False, "error": "no underlying price series for basket"}

    latest_snap = _snap_for_date(history_snaps, all_dates[-1])
    cash_frac = _cash_fraction_from_snap(latest_snap)
    invested_frac = max(0.0, 1.0 - cash_frac)

    daily_drags: list[tuple[str, float]] = []
    for i in range(1, len(all_dates)):
        d0, d1 = all_dates[i - 1], all_dates[i]
        snap = _snap_for_date(history_snaps, d1)
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
        # Cash sleeve earns cash_return; invested sleeve tracks basket.
        if cash_frac > 0 and cash_return_annual:
            cash_log = math.log1p(cash_return_annual / _TRADING_DAYS)
            blended_basket = invested_frac * basket_log + cash_frac * cash_log
        else:
            blended_basket = basket_log
        drag = blended_basket - math.log(r_fof)
        daily_drags.append((d1, drag))

    if len(daily_drags) < 3:
        return {"ok": False, "error": "insufficient aligned pair days"}

    drags = np.array([x[1] for x in daily_drags], dtype=float)
    gross_annual = float(np.mean(drags) * _TRADING_DAYS)

    drag_dates = [x[0] for x in daily_drags]
    horizons: list[dict[str, Any]] = []
    for label, n in _HORIZON_DAYS.items():
        if len(drags) < n:
            continue
        window = drags[-n:]
        gross_h = float(np.sum(window))
        borrow_h = 0.0
        if borrow_annual is not None and math.isfinite(borrow_annual):
            borrow_h = float(borrow_annual) * (n / _TRADING_DAYS)
        net_h = gross_h - borrow_h
        horizons.append({
            "label": label,
            "days": n,
            "horizonDays": n,
            "obs": n,
            "gross": round(gross_h, 6),
            "net": round(net_h, 6),
            "grossLog": round(gross_h, 6),
            "netLog": round(net_h, 6),
            "grossSimple": round(math.expm1(gross_h), 6),
            "netSimple": round(math.expm1(net_h), 6),
            "borrow_drag": round(borrow_h, 6),
            "borrowLog": round(borrow_h, 6),
            "start_date": drag_dates[-n],
            "end_date": drag_dates[-1],
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
        "daily_drags": [round(float(x), 8) for x in drags.tolist()],
        "cash_pct": round(cash_frac * 100.0, 4),
        "effective_invested_pct": round(invested_frac * 100.0, 4),
        "weights_as_of": latest_snap.get("as_of") if latest_snap else None,
    }


def weighted_child_forward_metrics(
    basket: dict[str, Any],
    child_records: dict[str, dict[str, Any]],
) -> dict[str, float | None]:
    """Legacy v1 wrapper — prefer ``weighted_child_nav_decay_forward``."""
    return weighted_child_nav_decay_forward(basket, child_records)


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

    fwd = weighted_child_nav_decay_forward(basket, child_records)
    child_diag = weighted_child_pair_pnl_blend(basket, child_records)
    fof_px = resolve_fof_price_series(sym, history_snaps, metrics)
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
    exp_p50 = fwd.get("expected_gross_decay_p50_annual")
    exp_p10 = fwd.get("expected_gross_decay_p10_annual")
    exp_p90 = fwd.get("expected_gross_decay_p90_annual")

    net_boot = {}
    if realized.get("ok"):
        net_boot = bootstrap_fof_net_edge(
            realized.get("daily_drags") or [],
            borrow_annual=borrow_current,
            forward_p50=exp_p50,
            forward_p10=exp_p10,
            forward_p90=exp_p90,
        )

    stale = None
    try:
        from yieldboost_fof_holdings import stale_days

        stale = stale_days(basket.get("as_of"))
    except Exception:
        pass

    fof_basket = {
        **basket,
        "effective_beta": fwd.get("effective_beta"),
        "weights_stale_days": stale,
        "history_points": len(history_snaps),
        "cash_pct": fwd.get("cash_pct") if fwd.get("cash_pct") is not None else basket.get("cash_pct"),
        "effective_invested_pct": fwd.get("effective_invested_pct"),
        "expense_ratio_annual": fwd.get("expense_ratio_annual"),
    }

    rec: dict[str, Any] = {
        "symbol": sym,
        "underlying": underlying_display,
        "delta": fwd.get("effective_beta"),
        "delta_n_obs": realized.get("n_days") if realized.get("ok") else None,
        "bucket": "bucket_2_low_beta",
        "bucket_2_ui_visible": True,
        "is_yieldboost": True,
        "is_dashboard_synthetic": True,
        "scenario_style": "income_style_fof",
        "product_class": "income_yieldboost_fof",
        "expected_decay_available": True,
        "gross_decay_annual": gross,
        "expected_gross_decay_annual": exp_p50,
        "expected_gross_decay_p50_annual": exp_p50,
        "expected_gross_decay_p10_annual": exp_p10,
        "expected_gross_decay_p90_annual": exp_p90,
        "expected_pair_pnl_p50_annual": exp_p50,
        "expected_pair_pnl_p10_annual": exp_p10,
        "expected_pair_pnl_p90_annual": exp_p90,
        "expected_pair_pnl_basis": "fof_weighted_child_nav_decay",
        "expected_pair_pnl_units": "log_continuous_annual",
        "net_edge_p50_annual": net_boot.get("net_edge_p50_annual"),
        "net_edge_p05_annual": net_boot.get("net_edge_p05_annual"),
        "net_edge_p25_annual": net_boot.get("net_edge_p25_annual"),
        "net_edge_p75_annual": net_boot.get("net_edge_p75_annual"),
        "net_edge_p95_annual": net_boot.get("net_edge_p95_annual"),
        "net_edge_hist_json": net_boot.get("net_edge_hist_json"),
        "gross_realized_mean_annual": net_boot.get("gross_realized_mean_annual"),
        "gross_anchor_target_annual": net_boot.get("gross_anchor_target_annual"),
        "gross_blend_weight_forward": net_boot.get("gross_blend_weight_forward"),
        "gross_sigma_realized_annual": net_boot.get("gross_sigma_realized_annual"),
        "gross_sigma_forward_annual": net_boot.get("gross_sigma_forward_annual"),
        "gross_blend_method": net_boot.get("gross_blend_method"),
        "gross_anchor_source": net_boot.get("gross_anchor_source"),
        "borrow_current": borrow_current,
        "borrow_fee_annual": borrow_current,
        "borrow_net_annual": borrow_current,
        "borrow_for_net_annual": borrow_current,
        "shares_available": shares_available,
        "borrow_source": borrow_source,
        "decomposition_note": "fof_synthetic_dashboard_v2",
        "gross_edge_definition": "fof_weighted_basket_realized",
        "schema_v": 4,
        "edge_sign_convention": "short_favorable_positive",
        "fof_basket": fof_basket,
        "fof_forward_meta": {
            "child_pair_p50_blend": child_diag.get("child_pair_p50_annual"),
            "child_pair_p10_blend": child_diag.get("child_pair_p10_annual"),
            "child_pair_p90_blend": child_diag.get("child_pair_p90_annual"),
            "expense_ratio_annual": fwd.get("expense_ratio_annual"),
            "cash_drag_annual": fwd.get("cash_drag_annual"),
            "forward_band_collapsed": fwd.get("forward_band_collapsed"),
        },
        "fof_realized_pair": realized if realized.get("ok") else {"ok": False, "error": realized.get("error")},
    }
    if realized.get("ok") and realized_pair_gross_60d_fields is not None:
        h60 = next(
            (
                h
                for h in (realized.get("horizons") or [])
                if int(h.get("days") or h.get("horizonDays") or 0) == 60
            ),
            None,
        )
        if h60:
            rec.update(realized_pair_gross_60d_fields(h60, source="fof_weighted_basket"))
    extras = enrich_fof_dashboard_extras(
        sym,
        history_snaps=history_snaps,
        metrics=metrics,
        fof_px=fof_px,
        forward_p50=exp_p50,
        effective_beta=fwd.get("effective_beta"),
        borrow_annual=borrow_current,
    )
    rec.update(extras)
    basket_vol = None
    if extras.get("fof_basket_vol", {}).get("vol_annual") is not None:
        basket_vol = extras["fof_basket_vol"]["vol_annual"]
        rec["fof_basket_vol_annual"] = basket_vol

    vol_fields = weighted_child_forecast_vol(
        basket,
        child_records,
        basket_vol_fallback=basket_vol,
    )
    rec.update({k: v for k, v in vol_fields.items() if v is not None})

    fof_calib = build_fof_income_distribution_calibration(basket, child_records)
    if fof_calib:
        rec["income_distribution_calibration"] = fof_calib

    pair_grid = build_fof_pair_scenario_grid(
        basket,
        child_records,
        anchor_p50=exp_p50,
        expense_ratio_annual=fwd.get("expense_ratio_annual") or FOF_DEFAULT_EXPENSE_RATIO_ANNUAL,
        cash_drag_annual=fwd.get("cash_drag_annual") or 0.0,
    )
    if pair_grid:
        rec["pair_scenario_grid"] = pair_grid

    fof_sigma = vol_fields.get("forecast_vol_underlying_annual")
    if fof_sigma is not None:
        try:
            fof_sigma_f = float(fof_sigma)
        except (TypeError, ValueError):
            fof_sigma_f = None
    else:
        fof_sigma_f = None
    if fof_sigma_f is not None and math.isfinite(fof_sigma_f) and fof_sigma_f > 0:
        income_grid = build_fof_income_scenario_grid(
            basket,
            child_records,
            scenario_sigma=fof_sigma_f,
            horizon_years=0.25,
            fof_borrow_annual=borrow_current,
            expense_ratio_annual=fwd.get("expense_ratio_annual") or FOF_DEFAULT_EXPENSE_RATIO_ANNUAL,
        )
        if income_grid:
            rec["income_scenario_grid"] = income_grid

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
    from yieldboost_fof_holdings import build_fof_holdings_history

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

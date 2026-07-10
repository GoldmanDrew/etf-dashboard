"""ls-algo production B4 sizing bridge for the dashboard backtest builder."""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]


def find_ls_algo() -> Path | None:
    env = os.environ.get("LS_ALGO_ROOT", "").strip()
    candidates = [
        Path(env) if env else None,
        REPO / "ls-algo",
        REPO.parent / "ls-algo",
        Path.home() / "Projects" / "quant" / "ls-algo",
    ]
    for p in candidates:
        if p and (p / "scripts" / "bucket4_backtest_api.py").is_file():
            return p.resolve()
    return None


def import_sizing_api():
    ls = find_ls_algo()
    if ls is None:
        raise ImportError(
            "ls-algo not found (need scripts/bucket4_backtest_api.py). "
            "Clone sibling repo, set LS_ALGO_ROOT, or checkout path: ls-algo in CI."
        )
    root = str(ls)
    if root not in sys.path:
        sys.path.insert(0, root)
    from scripts.bucket4_backtest_api import (  # noqa: E402
        build_closes_broad_from_panel,
        build_pair_cache_from_panel,
        size_b4_book_asof,
        weekly_rebalance_dates,
    )

    return {
        "ls_algo": ls,
        "size_b4_book_asof": size_b4_book_asof,
        "build_pair_cache_from_panel": build_pair_cache_from_panel,
        "build_closes_broad_from_panel": build_closes_broad_from_panel,
        "weekly_rebalance_dates": weekly_rebalance_dates,
    }


def opt2_cfg_from_policy(policy: Mapping[str, Any]) -> dict[str, Any]:
    rules = (policy.get("inverse_decay_bucket4") or {}).get("rules") or {}
    opt2 = dict(rules.get("bucket4_weekly_opt2") or {})
    # Production defaults if policy snapshot is stale.
    opt2.setdefault("decay_borrow_quad", 0)
    opt2.setdefault("borrow_linear_aversion", 1.5)
    opt2.setdefault("borrow_uncertainty_penalty", 3.0)
    opt2.setdefault("borrow_aversion_source", "posterior")
    opt2.setdefault("pf_min_pairs", 5)
    opt2.setdefault("min_weight", 0.005)
    opt2.setdefault("max_weight", 0.35)
    opt2.setdefault("cov_penalty", 0.85)
    opt2.setdefault("cov_shrink", 0.35)
    cb = dict(opt2.get("crash_budget") or {})
    cb.setdefault("enabled", True)
    cb.setdefault("rho", 0.0075)
    cb.setdefault("theta", 0.5)
    cb.setdefault("phi", 0.5)
    cb.setdefault("l_floor", 0.02)
    cb.setdefault("missing_policy", "book_quantile")
    cb.setdefault("missing_l_quantile", 0.75)
    opt2["crash_budget"] = cb
    return opt2


def size_production_book(
    uni: pd.DataFrame,
    panel: Mapping[str, pd.DataFrame],
    hedge_by_underlying: Mapping[str, pd.Series],
    *,
    screened_csv: str | Path,
    policy: Mapping[str, Any],
    run_date: str | pd.Timestamp,
    sleeve_budget_usd: float = 100_000.0,
):
    api = import_sizing_api()
    cache = api["build_pair_cache_from_panel"](uni, panel)
    closes = api["build_closes_broad_from_panel"](panel, uni)
    return api["size_b4_book_asof"](
        run_date=run_date,
        pair_cache=cache,
        hedge_by_underlying=hedge_by_underlying,
        closes_broad=closes,
        screened_csv=screened_csv,
        sleeve_budget_usd=float(sleeve_budget_usd),
        opt2_cfg=opt2_cfg_from_policy(policy),
        use_ibkr_uvix_borrow=False,
    )


def build_walk_forward_weights(
    uni: pd.DataFrame,
    panel: Mapping[str, pd.DataFrame],
    hedge_by_underlying: Mapping[str, pd.Series],
    *,
    screened_csv: str | Path,
    policy: Mapping[str, Any],
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    sleeve_budget_usd: float = 100_000.0,
    warmup_bdays: int = 60,
) -> tuple[pd.DataFrame, list[dict], dict]:
    """Weekly (W-FRI) opt2+crash weights; rows = rebalance dates, cols = ETF.

    Weights are **deployed** fractions (sum <= 1). Between Fridays the caller
    should forward-fill. Returns (weight_df, telemetry_by_date, latest_meta).
    """
    api = import_sizing_api()
    opt2 = opt2_cfg_from_policy(policy)
    dates = api["weekly_rebalance_dates"](start, end, freq="W-FRI")
    # Skip early Fridays until warmup has elapsed from start.
    start_ts = pd.Timestamp(start)
    warmup_end = start_ts + pd.offsets.BDay(int(warmup_bdays))
    dates = [d for d in dates if d >= warmup_end]
    if not dates:
        dates = [pd.Timestamp(end)]

    etfs = [str(x).strip().upper() for x in uni["ETF"].tolist()]
    rows: list[dict[str, float]] = []
    index: list[pd.Timestamp] = []
    tele_hist: list[dict] = []
    latest_meta: dict = {}

    cache_builder = api["build_pair_cache_from_panel"]
    closes_builder = api["build_closes_broad_from_panel"]
    size_fn = api["size_b4_book_asof"]

    for d in dates:
        # Need enough pairs with price history as-of d.
        live = []
        for _, row in uni.iterrows():
            etf = str(row["ETF"]).strip().upper()
            px = panel.get(etf)
            if px is None or px.empty:
                continue
            if px.index.min() > d:
                continue
            if int((px.index <= d).sum()) < 40:
                continue
            live.append(row)
        if len(live) < int(opt2.get("pf_min_pairs", 5)):
            continue
        live_df = pd.DataFrame(live).reset_index(drop=True)
        try:
            sized = size_fn(
                run_date=d,
                pair_cache=cache_builder(live_df, panel),
                hedge_by_underlying=hedge_by_underlying,
                closes_broad=closes_builder(panel, live_df),
                screened_csv=screened_csv,
                sleeve_budget_usd=float(sleeve_budget_usd),
                opt2_cfg=opt2,
                use_ibkr_uvix_borrow=False,
            )
        except Exception as exc:  # noqa: BLE001 — skip thin dates
            tele_hist.append({"date": d.strftime("%Y-%m-%d"), "error": str(exc)})
            continue
        w = sized.weights_by_etf()
        row = {e: float(w.get(e, 0.0)) for e in etfs}
        rows.append(row)
        index.append(pd.Timestamp(d))
        tele_hist.append(
            {
                "date": d.strftime("%Y-%m-%d"),
                "deployed_fraction": sized.deployed_fraction,
                "cash_residual": sized.cash_residual,
                "budget_eff": sized.budget_eff,
                "n_pairs": len(w),
                "telemetry": sized.telemetry,
            }
        )
        latest_meta = {
            "run_date": sized.run_date,
            "sizing_method": sized.sizing_method,
            "deployed_fraction": sized.deployed_fraction,
            "cash_residual": sized.cash_residual,
            "budget_usd": sized.budget_usd,
            "budget_eff": sized.budget_eff,
            "weights_by_etf": w,
            "weights_opt2_by_etf": {
                str(kk[0]): float(vv) for kk, vv in sized.weights_opt2.items()
            },
            "telemetry": sized.telemetry,
            "opt2_meta": sized.opt2_meta,
        }

    if not rows:
        raise RuntimeError("walk-forward sizing produced no rebalance dates")
    wdf = pd.DataFrame(rows, index=pd.DatetimeIndex(index)).reindex(columns=etfs).fillna(0.0)
    return wdf, tele_hist, latest_meta


def expand_weights_to_calendar(weight_df: pd.DataFrame, calendar: pd.DatetimeIndex) -> pd.DataFrame:
    """Forward-fill weekly weights onto a daily trading calendar (cash = 1 - row sum)."""
    if weight_df.empty:
        return pd.DataFrame(0.0, index=calendar, columns=weight_df.columns)
    aligned = weight_df.reindex(calendar.union(weight_df.index)).sort_index().ffill()
    return aligned.reindex(calendar).fillna(0.0)


def port_returns_with_cash(
    ret_df: pd.DataFrame,
    weight_matrix: pd.DataFrame,
    *,
    ret_floor: float = -0.95,
    ret_cap: float = 0.95,
) -> pd.Series:
    """Portfolio returns with cash residual (weights need not sum to 1)."""
    cols = [c for c in ret_df.columns if c in weight_matrix.columns]
    if not cols:
        return pd.Series(dtype=float)
    r = ret_df[cols]
    w = weight_matrix.reindex(index=r.index, columns=cols).fillna(0.0)
    pr = r.mul(w, axis=1).sum(axis=1)
    return pr.clip(lower=ret_floor, upper=ret_cap)

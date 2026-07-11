"""ls-algo production B4 sizing bridge for the dashboard backtest builder.

Walk-forward path mirrors live GTP layers:
  v6 opt2 → trim-only weight_smoothing → crash_budget → sim ratchet
with optional point-in-time borrow overrides from borrow_history.json.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]

from bucket4.b4_ratchet_overlay import RatchetConfig, SimRatchetState  # noqa: E402
from bucket4.pit_inputs import (  # noqa: E402
    apply_pit_borrow_to_universe,
    load_borrow_history,
)


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
    opt2.setdefault("borrow_ramp_lo", 0.80)
    opt2.setdefault("borrow_ramp_hi", 1.20)
    opt2.setdefault("pf_min_pairs", 5)
    opt2.setdefault("min_weight", 0.005)
    opt2.setdefault("max_weight", 0.35)
    opt2.setdefault("cov_penalty", 0.85)
    opt2.setdefault("cov_shrink", 0.35)
    opt2.setdefault("drift_threshold_share_of_gross", 0.02)
    ws = dict(opt2.get("weight_smoothing") or {})
    ws.setdefault("enabled", True)
    ws.setdefault("alpha", 0.5)
    opt2["weight_smoothing"] = ws
    cb = dict(opt2.get("crash_budget") or {})
    cb.setdefault("enabled", True)
    # Match live GTP (2026-07-10); never silently fall back to the old 0.0075.
    cb.setdefault("rho", 0.087)
    cb.setdefault("theta", 0.5)
    cb.setdefault("phi", 0.5)
    cb.setdefault("l_floor", 0.02)
    cb.setdefault("missing_policy", "book_quantile")
    cb.setdefault("missing_l_quantile", 0.75)
    opt2["crash_budget"] = cb
    return opt2


def smooth_pair_weights_trim_only(
    pair_weights: Mapping[tuple[str, str], float],
    prev_weights: Mapping[tuple[str, str], float],
    *,
    alpha: float,
) -> dict[tuple[str, str], float]:
    """Trim-only EMA (mirrors ls-algo bucket4_weekly_opt2.smooth_pair_weights_trim_only)."""
    a = float(np.clip(alpha, 0.0, 1.0))
    out: dict[tuple[str, str], float] = {}
    for k, w in pair_weights.items():
        w = max(0.0, float(w))
        wp = prev_weights.get(k)
        if wp is None or not np.isfinite(float(wp)):
            out[k] = w
        elif w < float(wp):
            out[k] = w
        else:
            out[k] = float(wp) + a * (w - float(wp))
    return out


def _pair_key_tuple(etf: str, und: str) -> tuple[str, str]:
    return (str(etf).strip().upper(), str(und).strip().upper())


def apply_weight_smoothing(
    weights_opt2: Mapping[tuple[str, str], float],
    prev: dict[tuple[str, str], float],
    opt2_cfg: Mapping[str, Any],
) -> tuple[dict[tuple[str, str], float], dict[tuple[str, str], float]]:
    """Apply trim-only smoothing; return (smoothed, new_prev_state)."""
    ws = opt2_cfg.get("weight_smoothing") or {}
    if not bool(ws.get("enabled", True)):
        new_prev = {k: float(v) for k, v in weights_opt2.items()}
        return dict(weights_opt2), new_prev
    alpha = float(ws.get("alpha", 0.5))
    smoothed = smooth_pair_weights_trim_only(weights_opt2, prev, alpha=alpha)
    return smoothed, {k: float(v) for k, v in smoothed.items()}


def apply_sim_ratchet_to_weights(
    weights_by_etf: Mapping[str, float],
    uni: pd.DataFrame,
    ratchet_sim: SimRatchetState,
    *,
    sleeve_cap: float = 1.0,
) -> dict[str, float]:
    """Apply grow-only / continuous-trim ratchet to ETF weight map; update sim state.

    After ratchet, if the book exceeds ``sleeve_cap`` (default 1.0), scale down
    proportionally so the sleeve budget hard-caps deployment. Live GTP can
    temporarily overshoot via USD floors; the dashboard sleeve is unit-normalized.
    """
    und_by_etf = {
        str(r.get("ETF")).strip().upper(): str(r.get("Underlying")).strip().upper()
        for _, r in uni.iterrows()
    }
    edge_by_etf = {
        str(r.get("ETF")).strip().upper(): float(
            pd.to_numeric(r.get("bucket4_net_edge_annual"), errors="coerce") or 0.0
        )
        for _, r in uni.iterrows()
    }
    borrow_by_etf = {
        str(r.get("ETF")).strip().upper(): float(
            pd.to_numeric(r.get("borrow_current"), errors="coerce") or 0.0
        )
        for _, r in uni.iterrows()
    }
    eff: dict[str, float] = {}
    for etf, w in weights_by_etf.items():
        etf_n = str(etf).strip().upper()
        w = float(w) if np.isfinite(float(w)) else 0.0
        if w <= 1e-12:
            continue
        und = und_by_etf.get(etf_n, etf_n)
        mult, _res = ratchet_sim.apply_gross_multiplier(
            etf_n,
            und,
            w,
            fwd_edge=edge_by_etf.get(etf_n, 0.0),
            borrow=borrow_by_etf.get(etf_n, 0.0),
        )
        eff_w = float(w) * float(mult)
        if eff_w > 1e-12:
            eff[etf_n] = eff_w
    total = float(sum(eff.values()))
    cap = float(sleeve_cap) if sleeve_cap and sleeve_cap > 0 else 1.0
    if total > cap + 1e-12:
        scale = cap / total
        eff = {k: float(v) * scale for k, v in eff.items()}
    for etf_n, eff_w in eff.items():
        und = und_by_etf.get(etf_n, etf_n)
        ratchet_sim.record_rebalance(etf_n, und, eff_w)
    return eff


def size_production_book(
    uni: pd.DataFrame,
    panel: Mapping[str, pd.DataFrame],
    hedge_by_underlying: Mapping[str, pd.Series],
    *,
    screened_csv: str | Path,
    policy: Mapping[str, Any],
    run_date: str | pd.Timestamp,
    sleeve_budget_usd: float = 100_000.0,
    prev_smooth: dict[tuple[str, str], float] | None = None,
    ratchet_sim: SimRatchetState | None = None,
    borrow_history: Mapping[str, pd.Series] | None = None,
):
    api = import_sizing_api()
    opt2 = opt2_cfg_from_policy(policy)
    bt_cfg = policy.get("backtest") or {}
    rules = (policy.get("inverse_decay_bucket4") or {}).get("rules") or {}
    b4_rules = (policy.get("bucket_4") or {}).get("rules") or {}
    live = uni
    hist = borrow_history
    if hist is None and bool(bt_cfg.get("pit_borrow", True)):
        hist = load_borrow_history()
    if hist and bool(bt_cfg.get("pit_borrow", True)):
        max_b = b4_rules.get("max_borrow_current")
        live = apply_pit_borrow_to_universe(
            uni,
            hist,
            run_date,
            max_borrow=float(max_b) if max_b is not None else None,
        )
    cache = api["build_pair_cache_from_panel"](live, panel)
    closes = api["build_closes_broad_from_panel"](panel, live)
    size_kwargs: dict[str, Any] = dict(
        run_date=run_date,
        pair_cache=cache,
        hedge_by_underlying=hedge_by_underlying,
        closes_broad=closes,
        screened_csv=screened_csv,
        sleeve_budget_usd=float(sleeve_budget_usd),
        opt2_cfg=opt2,
        use_ibkr_uvix_borrow=False,
    )
    # Prefer API-native smoothing (opt2 → smooth → crash) when supported.
    import inspect

    size_fn = api["size_b4_book_asof"]
    if "prev_smooth_weights" in inspect.signature(size_fn).parameters:
        size_kwargs["prev_smooth_weights"] = prev_smooth or {}
        sized = size_fn(**size_kwargs)
        prev = dict(getattr(sized, "smooth_prev", None) or prev_smooth or {})
    else:
        sized = size_fn(**size_kwargs)
        # Legacy API: smooth opt2 then re-cap locally.
        ws = opt2.get("weight_smoothing") or {}
        prev = dict(prev_smooth or {})
        if bool(ws.get("enabled", True)) and sized.weights_opt2:
            smoothed, prev = apply_weight_smoothing(sized.weights_opt2, prev, opt2)
            try:
                from scripts.b4_crash_budget import (  # noqa: E402
                    CrashBudgetParams,
                    cap_pair_weights,
                    compute_crash_caps,
                )

                h_map = {}
                as_of = pd.Timestamp(run_date)
                for und, ser in (hedge_by_underlying or {}).items():
                    s = pd.Series(ser).copy()
                    if not isinstance(s.index, pd.DatetimeIndex):
                        s.index = pd.to_datetime(s.index)
                    s = s.loc[s.index <= as_of].dropna()
                    if len(s):
                        h_map[und] = s
                cb_cfg = opt2.get("crash_budget") or {}
                h_policy = opt2.get("hedge_cadence_policy") or {}
                h_base = float(h_policy.get("h_mid", 0.45))
                if cb_cfg.get("enabled", True):
                    caps = compute_crash_caps(
                        pair_cache=cache,
                        hedge_by_underlying=h_map,
                        closes_broad=closes,
                        hedge_base=h_base,
                        run_date=as_of.strftime("%Y-%m-%d"),
                        budget_usd=float(sleeve_budget_usd),
                        params=CrashBudgetParams.from_config(cb_cfg),
                    )
                    capped, budget_eff, tel = cap_pair_weights(
                        smoothed, caps, float(sleeve_budget_usd)
                    )
                    sized.weights_opt2 = {k: float(v) for k, v in smoothed.items()}
                    sized.weights_capped = {k: float(v) for k, v in capped.items()}
                    sized.budget_eff = float(budget_eff)
                    sized.deployed_fraction = float(sum(capped.values()))
                    sized.cash_residual = max(0.0, 1.0 - sized.deployed_fraction)
                    if tel is not None and not getattr(tel, "empty", True):
                        sized.telemetry = tel.to_dict(orient="records")
                else:
                    sized.weights_opt2 = {k: float(v) for k, v in smoothed.items()}
                    sized.weights_capped = {k: float(v) for k, v in smoothed.items()}
                    sized.deployed_fraction = float(sum(smoothed.values()))
                    sized.cash_residual = max(0.0, 1.0 - sized.deployed_fraction)
            except Exception:
                scaled: dict[tuple[str, str], float] = {}
                for k, w_cap in sized.weights_capped.items():
                    w_opt = float(sized.weights_opt2.get(k, 0.0))
                    w_sm = float(smoothed.get(k, 0.0))
                    if w_opt > 1e-12:
                        scaled[k] = float(w_cap) * (w_sm / w_opt)
                    else:
                        scaled[k] = float(w_sm)
                sized.weights_opt2 = {k: float(v) for k, v in smoothed.items()}
                sized.weights_capped = scaled
                sized.deployed_fraction = float(sum(scaled.values()))
                sized.cash_residual = max(0.0, 1.0 - sized.deployed_fraction)

    w_etf = sized.weights_by_etf()
    if ratchet_sim is not None and ratchet_sim.cfg.enabled:
        w_etf = apply_sim_ratchet_to_weights(w_etf, live, ratchet_sim)
        deployed = float(sum(w_etf.values()))
        sized.deployed_fraction = deployed
        sized.cash_residual = max(0.0, 1.0 - deployed)
        und_map = {
            str(r.get("ETF")).strip().upper(): str(r.get("Underlying")).strip().upper()
            for _, r in live.iterrows()
        }
        sized.weights_capped = {
            _pair_key_tuple(e, und_map.get(e, e)): float(v) for e, v in w_etf.items()
        }

    ws = opt2.get("weight_smoothing") or {}
    sized.opt2_meta = dict(sized.opt2_meta or {})
    sized.opt2_meta["weight_smoothing_enabled"] = bool(ws.get("enabled", True))
    sized.opt2_meta["ratchet_applied"] = bool(
        ratchet_sim is not None and ratchet_sim.cfg.enabled
    )
    sized._smooth_prev = prev  # type: ignore[attr-defined]
    sized._live_uni = live  # type: ignore[attr-defined]
    return sized


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
    """Weekly (W-FRI) opt2 → smooth → crash → ratchet weights.

    Weights are **deployed** fractions (sum <= 1). Between Fridays the caller
    should forward-fill. Returns (weight_df, telemetry_by_date, latest_meta).
    """
    api = import_sizing_api()
    opt2 = opt2_cfg_from_policy(policy)
    rules = (policy.get("inverse_decay_bucket4") or {}).get("rules") or {}
    bt_cfg = policy.get("backtest") or {}
    dates = api["weekly_rebalance_dates"](start, end, freq="W-FRI")
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

    ratchet_cfg = RatchetConfig.from_cfg(rules.get("ratchet") or policy.get("bucket_4_ratchet"))
    ratchet_sim = SimRatchetState(cfg=ratchet_cfg)
    prev_smooth: dict[tuple[str, str], float] = {}
    borrow_history = load_borrow_history() if bool(bt_cfg.get("pit_borrow", True)) else {}

    for d in dates:
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
            sized = size_production_book(
                live_df,
                panel,
                hedge_by_underlying,
                screened_csv=screened_csv,
                policy=policy,
                run_date=d,
                sleeve_budget_usd=float(sleeve_budget_usd),
                prev_smooth=prev_smooth,
                ratchet_sim=ratchet_sim,
                borrow_history=borrow_history,
            )
            prev_smooth = getattr(sized, "_smooth_prev", prev_smooth)
        except Exception as exc:  # noqa: BLE001 — skip thin dates
            tele_hist.append({"date": d.strftime("%Y-%m-%d"), "error": str(exc)})
            continue
        w = sized.weights_by_etf()
        # After ratchet, weights_by_etf may be stale — prefer capped map.
        if sized.weights_capped:
            w = {k[0]: float(v) for k, v in sized.weights_capped.items()}
        row = {e: float(w.get(e, 0.0)) for e in etfs}
        rows.append(row)
        index.append(pd.Timestamp(d))
        tele_hist.append(
            {
                "date": d.strftime("%Y-%m-%d"),
                "deployed_fraction": sized.deployed_fraction,
                "cash_residual": sized.cash_residual,
                "budget_eff": sized.budget_eff,
                "n_pairs": len([x for x in w.values() if x > 1e-12]),
                "telemetry": sized.telemetry,
                "weight_smoothing": bool((opt2.get("weight_smoothing") or {}).get("enabled", True)),
                "ratchet": bool(ratchet_cfg.enabled),
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
            "ratchet_state": ratchet_sim.as_dict(),
            "parity_layers": {
                "opt2": True,
                "weight_smoothing": bool((opt2.get("weight_smoothing") or {}).get("enabled", True)),
                "crash_budget": bool((opt2.get("crash_budget") or {}).get("enabled", True)),
                "ratchet_walk_forward": bool(ratchet_cfg.enabled),
                "pit_borrow": bool(bt_cfg.get("pit_borrow", True)),
                "crash_rho": float((opt2.get("crash_budget") or {}).get("rho", 0.087)),
            },
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

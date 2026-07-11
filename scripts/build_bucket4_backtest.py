#!/usr/bin/env python3
"""Build static Bucket 4 backtest artifacts for the ETF dashboard.

Writes:
  - data/bucket4_backtest.json
  - data/bucket4_backtest_state.json
  - data/bucket4_backtest_policy_hash.txt

Run:
    python scripts/build_bucket4_backtest.py
    python scripts/build_bucket4_backtest.py --start 2024-06-01 --screener data/etf_screened_today.csv
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

REPO = Path(__file__).resolve().parents[1]
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from bucket4.b4_ratchet_overlay import RatchetConfig, SimRatchetState  # noqa: E402
from bucket4.bucket4_dynamic_bt import run_bucket4_backtest_dynamic_h  # noqa: E402
from bucket4.bucket4_hedge_cadence import build_h_series, build_rebal_dates, load_policy_from_config  # noqa: E402
from bucket4.bucket4_price_loading import load_price_panel, perf_stats  # noqa: E402
from bucket4.bucket4_sizing import (  # noqa: E402
    apply_cluster_caps_to_b4,
    apply_concentration_to_b4,
    concentration_scores,
)
from bucket4.bucket4_vol_shape_signals import get_pair_signal, load_vol_shape_history  # noqa: E402
from bucket4.ls_algo_sizing import (  # noqa: E402
    build_walk_forward_weights,
    expand_weights_to_calendar,
    find_ls_algo,
    port_returns_with_cash,
    size_production_book,
)
from bucket4.pit_inputs import (  # noqa: E402
    borrow_series_for_calendar,
    load_borrow_history,
    pit_meta,
)
from bucket4.policy_helpers import knobs_from_policy, load_policy, make_knobs  # noqa: E402

DEFAULT_SCREENER = REPO / "data" / "etf_screened_today.csv"
DEFAULT_POLICY = REPO / "config" / "bucket4_backtest_policy.yml"
OUT_JSON = REPO / "data" / "bucket4_backtest.json"
OUT_STATE = REPO / "data" / "bucket4_backtest_state.json"
OUT_HASH = REPO / "data" / "bucket4_backtest_policy_hash.txt"
OUT_PAIR_DIR = REPO / "data" / "bucket4_pairs"
VOL_SHAPE_HISTORY = REPO / "data" / "vol_shape_history.json"
RET_FLOOR = -0.95
SCHEMA = "bucket4_backtest.v3"
PAIR_SCHEMA = "bucket4_pair.v2"
PAIR_SHARD_MIN_DAYS = 20
SIZING_METHOD = "v6_opt2_crash_budget"


def _norm(x: object) -> str:
    return str(x).strip().upper().replace(".", "-")


def _bool_series(s: pd.Series) -> pd.Series:
    if s.dtype == bool:
        return s.fillna(False)
    return s.fillna(False).astype(str).str.lower().isin({"1", "true", "t", "yes", "y"})


def _finite_float(val, default: float = 0.0) -> float:
    try:
        v = float(pd.to_numeric(val, errors="coerce"))
    except (TypeError, ValueError):
        return default
    return default if not np.isfinite(v) else v


def _round_or_none(val, ndigits: int = 6):
    try:
        v = float(pd.to_numeric(val, errors="coerce"))
    except (TypeError, ValueError):
        return None
    if not np.isfinite(v):
        return None
    return round(v, ndigits)


def _json_safe(obj):
    """Replace NaN/Inf with None and coerce pandas/numpy scalars for JSON."""
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    if isinstance(obj, pd.Timestamp):
        return obj.strftime("%Y-%m-%d")
    if isinstance(obj, (np.datetime64,)):
        return pd.Timestamp(obj).strftime("%Y-%m-%d")
    if isinstance(obj, float):
        return obj if np.isfinite(obj) else None
    if isinstance(obj, (np.floating,)):
        v = float(obj)
        return v if np.isfinite(v) else None
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, Path):
        return str(obj)
    return obj


def _compact_series(s: pd.Series, ndigits: int = 6) -> list:
    return [_round_or_none(x, ndigits) for x in s.to_numpy()]


def _rebalance_log(bt: pd.DataFrame, max_rows: int = 160) -> list[dict]:
    if bt.empty or "rebalance_scheduled" not in bt.columns:
        return []
    rows = bt.loc[bt["rebalance_scheduled"].fillna(False).astype(bool)].tail(max_rows)
    out: list[dict] = []
    for dt, row in rows.iterrows():
        out.append(
            {
                "date": pd.Timestamp(dt).strftime("%Y-%m-%d"),
                "h": _round_or_none(row.get("h_used"), 4),
                "drift_share": _round_or_none(row.get("drift_share_of_gross"), 6),
                "skipped_below_drift": bool(row.get("rebalance_skipped_below_drift", False)),
                "executed": bool(row.get("rebalance", False)),
                "rebalance_fee": _round_or_none(row.get("rebalance_fee"), 6),
            }
        )
    return out


def _pair_daily_payload(bt: pd.DataFrame) -> dict:
    a_mv = bt["a_shares"].mul(bt["a_px"]) if {"a_shares", "a_px"}.issubset(bt.columns) else pd.Series(0.0, index=bt.index)
    b_mv = bt["b_shares"].mul(bt["b_px"]) if {"b_shares", "b_px"}.issubset(bt.columns) else pd.Series(0.0, index=bt.index)
    a_pnl_daily = a_mv.shift(1).fillna(a_mv).div(bt["a_px"].shift(1).fillna(bt["a_px"])).mul(bt["a_px"].diff().fillna(0.0))
    b_pnl_daily = b_mv.shift(1).fillna(b_mv).div(bt["b_px"].shift(1).fillna(bt["b_px"])).mul(bt["b_px"].diff().fillna(0.0))
    etf_leg_cum = a_pnl_daily.cumsum()
    und_leg_cum = b_pnl_daily.cumsum()
    borrow_cum = bt["borrow_cost"].cumsum() if "borrow_cost" in bt.columns else pd.Series(0.0, index=bt.index)
    fee_cum = bt["rebalance_fee"].cumsum() if "rebalance_fee" in bt.columns else pd.Series(0.0, index=bt.index)
    slip_cum = bt["slippage_cost"].cumsum() if "slippage_cost" in bt.columns else pd.Series(0.0, index=bt.index)
    tcost_cum = fee_cum + slip_cum
    equity0 = float(bt["equity"].iloc[0]) if len(bt) else 0.0
    return {
        "dates": [pd.Timestamp(d).strftime("%Y-%m-%d") for d in bt.index],
        "ret": _compact_series(bt["ret"]),
        "equity": _compact_series(bt["equity"]),
        "drawdown": _compact_series(bt["drawdown"]),
        "h_used": _compact_series(bt["h_used"], 4),
        "rebalance": [1 if bool(x) else 0 for x in bt.get("rebalance", pd.Series(False, index=bt.index)).to_numpy()],
        "rebalance_scheduled": [
            1 if bool(x) else 0
            for x in bt.get("rebalance_scheduled", pd.Series(False, index=bt.index)).to_numpy()
        ],
        "borrow_cost": _compact_series(bt["borrow_cost"]),
        "financing_pnl": _compact_series(bt["financing_pnl"]),
        "rebalance_fee": _compact_series(bt["rebalance_fee"]),
        "slippage_cost": _compact_series(bt["slippage_cost"]),
        "net_pnl": _compact_series(bt["equity"] - equity0),
        "etf_leg_pnl_cum": _compact_series(etf_leg_cum),
        "underlying_leg_pnl_cum": _compact_series(und_leg_cum),
        "total_gross": _compact_series(etf_leg_cum + und_leg_cum),
        "borrow_cost_cum": _compact_series(borrow_cum),
        "tcost_cum": _compact_series(tcost_cum),
        "etf_gross": _compact_series(a_mv.abs()),
        "underlying_gross": _compact_series(b_mv.abs()),
        "gross_exposure": _compact_series(a_mv.abs() + b_mv.abs()),
    }


def policy_hash(policy_path: Path) -> str:
    raw = policy_path.read_bytes()
    return hashlib.sha256(raw).hexdigest()


def _bucket4_rules(policy: dict) -> tuple[dict, dict, list[str]]:
    rules = (policy.get("inverse_decay_bucket4") or {}).get("rules") or {}
    b4_rules = (policy.get("bucket_4") or {}).get("rules") or {}
    buckets = (policy.get("bucket_4") or {}).get("screener_buckets") or ["bucket_4"]
    return rules, b4_rules, [str(b).lower() for b in buckets]


def _annotate_bucket4_gates(sc: pd.DataFrame, policy: dict) -> pd.DataFrame:
    out = sc.copy()
    if out.empty:
        out["gate_reason"] = []
        out["production_candidate"] = []
        return out

    rules, b4_rules, buckets = _bucket4_rules(policy)
    bucket = out.get("bucket", pd.Series("", index=out.index)).astype(str).str.lower()
    beta = pd.to_numeric(out.get("Delta"), errors="coerce")
    inv_ok = out.get("inverse_shortable", pd.Series(True, index=out.index))
    inv_ok = _bool_series(inv_ok) if inv_ok is not None else pd.Series(True, index=out.index)
    excluded = {_norm(x) for x in (rules.get("excluded_etfs") or [])}
    min_edge = float(rules.get("min_net_edge_annual", 0.30))
    edge = pd.to_numeric(out.get("bucket4_net_edge_annual"), errors="coerce")
    min_vol = float(rules.get("min_underlying_vol", b4_rules.get("min_underlying_vol", 0.40)))
    vol = pd.to_numeric(out.get("vol_underlying_annual"), errors="coerce")
    if b4_rules.get("exclude_purgatory", True) and "purgatory" in out.columns:
        purg = _bool_series(out["purgatory"])
    else:
        purg = pd.Series(False, index=out.index)
    blk = _bool_series(out.get("strategy_blacklisted", pd.Series(False, index=out.index)))

    reasons: list[str] = []
    for idx, row in out.iterrows():
        sym = _norm(row.get("ETF", ""))
        reason = ""
        if str(bucket.loc[idx]) not in buckets:
            reason = "not_bucket_4"
        elif not (pd.notna(beta.loc[idx]) and float(beta.loc[idx]) < 0):
            reason = "non_inverse_beta"
        elif not bool(inv_ok.loc[idx]):
            reason = "not_inverse_shortable"
        elif sym in excluded:
            reason = "excluded_etf"
        elif not (pd.notna(edge.loc[idx]) and float(edge.loc[idx]) >= min_edge):
            reason = "edge_below_min"
        elif not (pd.notna(vol.loc[idx]) and float(vol.loc[idx]) >= min_vol):
            reason = "underlying_vol_below_min"
        elif bool(purg.loc[idx]):
            reason = "purgatory"
        elif bool(blk.loc[idx]):
            reason = "strategy_blacklisted"
        reasons.append(reason)
    out["gate_reason"] = reasons
    out["production_candidate"] = out["gate_reason"].eq("")
    return out


def load_bucket4_rows(screener_path: Path, policy: dict) -> pd.DataFrame:
    sc = pd.read_csv(screener_path)
    if sc.empty:
        return sc
    sc = sc.copy()
    sc["ETF"] = sc["ETF"].map(_norm)
    sc["Underlying"] = sc["Underlying"].map(_norm)
    _rules, _b4_rules, buckets = _bucket4_rules(policy)
    bucket = sc.get("bucket", pd.Series("", index=sc.index)).astype(str).str.lower()
    out = sc.loc[bucket.isin(buckets)].copy()
    out = _annotate_bucket4_gates(out, policy)
    out = out.drop_duplicates(subset=["ETF"], keep="first").reset_index(drop=True)
    return out


def load_universe(screener_path: Path, policy: dict) -> pd.DataFrame:
    b4 = load_bucket4_rows(screener_path, policy)
    if b4.empty:
        return b4
    return b4.loc[b4["production_candidate"].fillna(False)].copy().reset_index(drop=True)


def score_weights_legacy_concentration(df: pd.DataFrame, policy: dict) -> tuple[pd.DataFrame, np.ndarray]:
    """Legacy (edge-borrow)/vol weights — only used with --legacy-concentration."""
    if df.empty:
        return df, np.array([])
    scores = concentration_scores(df)
    w = scores.to_numpy(dtype=float)
    w = np.where(np.isfinite(w) & (w > -np.inf), w, 0.0)
    w = np.maximum(w, 0.0)
    if w.sum() <= 1e-12:
        w = np.ones(len(df), dtype=float)
    w = w / w.sum()

    rules = (policy.get("inverse_decay_bucket4") or {}).get("rules") or {}
    conc = rules.get("concentration") or {}
    if conc.get("enabled"):
        top_n = int(conc.get("top_n_pairs", 15))
        df, w, _info = apply_concentration_to_b4(df, w, top_n=top_n)

    cluster_caps = rules.get("cluster_caps") or {}
    w, _ = apply_cluster_caps_to_b4(df, w, cluster_caps)
    tot = float(w.sum())
    if tot > 1e-12:
        w = w / tot
    return df.reset_index(drop=True), w


# Back-compat alias for tests that still import score_weights.
score_weights = score_weights_legacy_concentration


def _telemetry_by_etf(telemetry: list[dict] | None) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for row in telemetry or []:
        etf = _norm(row.get("ETF") or row.get("etf"))
        if etf:
            out[etf] = row
    return out


def _pair_summary(
    row: pd.Series,
    bt: pd.DataFrame,
    h_daily: pd.Series,
    rb_diag: pd.DataFrame,
    *,
    production_weight: float | None = None,
    effective_weight: float | None = None,
    in_production_book: bool = False,
    gate_reason: str = "",
) -> dict:
    etf = _norm(row.get("ETF"))
    und = _norm(row.get("Underlying"))
    stats = perf_stats(bt)
    mean_h = float(h_daily.dropna().mean()) if len(h_daily.dropna()) else np.nan
    last_h = float(h_daily.dropna().iloc[-1]) if len(h_daily.dropna()) else np.nan
    n_rebal = int(bt["rebalance"].sum()) if "rebalance" in bt.columns else 0
    n_skip = int(bt["rebalance_skipped_below_drift"].sum()) if "rebalance_skipped_below_drift" in bt.columns else 0
    total_borrow = float(bt["borrow_cost"].sum()) if "borrow_cost" in bt.columns else 0.0
    total_fees = float(bt["rebalance_fee"].sum()) if "rebalance_fee" in bt.columns else 0.0
    rets = bt["ret"].dropna()
    daily_hit = float((rets > 0).mean()) if len(rets) else np.nan
    first_date = pd.Timestamp(bt.index[0]).strftime("%Y-%m-%d") if len(bt) else None
    latest_date = pd.Timestamp(bt.index[-1]).strftime("%Y-%m-%d") if len(bt) else None
    out = {
        "etf": etf,
        "underlying": und,
        "in_production_book": bool(in_production_book),
        "production_status": "production" if in_production_book else "gated_research",
        "gate_reason": gate_reason or ("production_book" if in_production_book else "not_in_production_book"),
        "model_status": "ok",
        "first_metrics_date": first_date,
        "entry_date": first_date,
        "latest_date": latest_date,
        "price_rows": int(len(bt)),
        "n_days": int(len(bt)),
        "cagr": _round_or_none(stats.get("cagr"), 4),
        "ann_vol": _round_or_none(stats.get("annual_vol"), 4),
        "vol_annual": _round_or_none(stats.get("annual_vol"), 4),
        "sharpe": _round_or_none(stats.get("sharpe"), 3),
        "max_drawdown": _round_or_none(stats.get("max_drawdown"), 4),
        "daily_hit_rate": _round_or_none(daily_hit, 4),
        "best_day": _round_or_none(rets.max() if len(rets) else np.nan, 4),
        "worst_day": _round_or_none(rets.min() if len(rets) else np.nan, 4),
        "mean_h": _round_or_none(mean_h, 4),
        "h_last": _round_or_none(last_h, 4),
        "n_rebalances": n_rebal,
        "n_rebalances_skipped": n_skip,
        "total_borrow": _round_or_none(total_borrow, 6),
        "total_fees": _round_or_none(total_fees, 6),
        "final_equity": _round_or_none(bt["equity"].iloc[-1] if len(bt) else np.nan, 6),
        "portfolio_weight": _round_or_none(production_weight, 4),
        "effective_weight": _round_or_none(effective_weight, 4),
        "bucket4_net_edge_annual": _round_or_none(row.get("bucket4_net_edge_annual"), 4),
        "borrow": _round_or_none(row.get("borrow_current"), 4),
        "beta": _round_or_none(row.get("Delta"), 4),
        "vol_underlying_annual": _round_or_none(row.get("vol_underlying_annual"), 4),
    }
    if len(rb_diag):
        out["last_interval_days"] = int(rb_diag["interval_days"].iloc[-1])
    return out


def _empty_pair_summary(row: pd.Series, *, status: str, gate_reason: str, price_rows: int = 0) -> dict:
    return {
        "etf": _norm(row.get("ETF")),
        "underlying": _norm(row.get("Underlying")),
        "in_production_book": False,
        "production_status": "gated_research",
        "gate_reason": gate_reason or "not_in_production_book",
        "model_status": status,
        "first_metrics_date": None,
        "entry_date": None,
        "latest_date": None,
        "price_rows": int(price_rows),
        "n_days": int(price_rows),
        "cagr": None,
        "ann_vol": None,
        "vol_annual": None,
        "sharpe": None,
        "max_drawdown": None,
        "daily_hit_rate": None,
        "best_day": None,
        "worst_day": None,
        "mean_h": None,
        "h_last": None,
        "n_rebalances": 0,
        "n_rebalances_skipped": 0,
        "total_borrow": None,
        "total_fees": None,
        "final_equity": None,
        "portfolio_weight": None,
        "effective_weight": None,
        "bucket4_net_edge_annual": _round_or_none(row.get("bucket4_net_edge_annual"), 4),
        "borrow": _round_or_none(row.get("borrow_current"), 4),
        "beta": _round_or_none(row.get("Delta"), 4),
        "vol_underlying_annual": _round_or_none(row.get("vol_underlying_annual"), 4),
    }


def _screener_payload(row: pd.Series) -> dict:
    return {
        "bucket4_net_edge_annual": _round_or_none(row.get("bucket4_net_edge_annual"), 4),
        "borrow": _round_or_none(row.get("borrow_current"), 4),
        "beta": _round_or_none(row.get("Delta"), 4),
        "vol_underlying_annual": _round_or_none(row.get("vol_underlying_annual"), 4),
        "init_pct_short": _round_or_none(row.get("init_pct_short"), 4),
        "maint_pct_short": _round_or_none(row.get("maint_pct_short"), 4),
        "purgatory": str(row.get("purgatory", "")).strip().lower() in {"1", "true", "t", "yes", "y"},
        "screener_bucket": str(row.get("bucket", "")),
    }


def _pair_engine_kwargs(policy: dict, row: pd.Series, cal: pd.DatetimeIndex, borrow_history: dict) -> dict:
    """Shared pair-sim knobs: drift skip, force clock, PIT borrow series."""
    rules = (policy.get("inverse_decay_bucket4") or {}).get("rules") or {}
    bt_cfg = policy.get("backtest") or {}
    opt2 = rules.get("bucket4_weekly_opt2") or {}
    hcp = opt2.get("hedge_cadence_policy") or {}
    spot_borrow = _finite_float(row.get("borrow_current"), 0.0)
    etf = _norm(row.get("ETF"))
    kwargs: dict = {
        "borrow_a_annual": spot_borrow,
        "slippage_bps": float(opt2.get("slippage_bps", 20.0)),
        "fee_bps": float(opt2.get("fee_bps", 1.0)),
        "opt2_h_base": float(hcp.get("h_mid", 0.45)),
    }
    drift = opt2.get("drift_threshold_share_of_gross")
    if drift is not None and np.isfinite(float(drift)):
        kwargs["drift_threshold_share_of_gross"] = float(drift)
    if bool(hcp.get("force_on_max_interval", True)):
        kwargs["force_rebalance_after_days"] = int(hcp.get("max_interval", 21))
    if bool(bt_cfg.get("pit_borrow", True)) and borrow_history:
        kwargs["borrow_a_series"] = borrow_series_for_calendar(
            borrow_history, etf, cal, fallback=spot_borrow
        )
    return kwargs


def run_pair_backtest_for_row(
    row: pd.Series,
    panel: dict[str, pd.DataFrame],
    policy: dict,
    *,
    start: str,
    min_days: int,
    warmup_bdays: int,
    signal_window: int,
    vol_history: dict[str, pd.DataFrame],
    borrow_history: dict | None = None,
) -> tuple[pd.DataFrame | None, pd.Series | None, pd.DataFrame | None, str]:
    etf = _norm(row.get("ETF"))
    und = _norm(row.get("Underlying"))
    px = panel.get(etf)
    if px is None or px.empty:
        return None, None, None, "missing_price"
    cal = pd.DatetimeIndex([d for d in px.index if d >= pd.Timestamp(start)])
    if len(cal) < min_days:
        return None, None, None, "short_history"
    rules = (policy.get("inverse_decay_bucket4") or {}).get("rules") or {}
    bt_cfg = policy.get("backtest") or {}
    blk = knobs_from_policy(policy)
    _knobs0, tilts, _source = load_policy_from_config(policy)
    knobs = make_knobs(blk)
    hist = borrow_history if borrow_history is not None else (
        load_borrow_history() if bool(bt_cfg.get("pit_borrow", True)) else {}
    )

    sig = get_pair_signal(
        etf,
        und,
        cal,
        history=vol_history,
        underlying_prices=px["b_px"],
        window=signal_window,
        lookahead_shift=1,
    )
    tilt = tilts.get(etf) or tilts.get(und)
    h_daily = build_h_series(sig, cal, knobs=knobs, name_tilt=tilt)
    rb, rb_diag = build_rebal_dates(sig, cal, knobs=knobs, name_tilt=tilt, warmup_bdays=warmup_bdays)
    eng = _pair_engine_kwargs(policy, row, cal, hist)
    bt = run_bucket4_backtest_dynamic_h(
        px.reindex(cal),
        h_daily,
        rb,
        initial_capital=float(bt_cfg.get("initial_capital", 1.0)),
        gross_multiplier=1.0,
        beta_a=-abs(_finite_float(row.get("Delta"), -2.0)),
        beta_b=1.0,
        **eng,
    )
    return bt, h_daily, rb_diag, "ok"


def pair_shard_from_result(
    row: pd.Series,
    bt: pd.DataFrame | None,
    h_daily: pd.Series | None,
    rb_diag: pd.DataFrame | None,
    *,
    status: str,
    gate_reason: str,
    in_production_book: bool = False,
    production_weight: float | None = None,
    effective_weight: float | None = None,
    generated_at_utc: str | None = None,
    policy_version: str | None = None,
    price_rows: int = 0,
) -> dict:
    if bt is None or h_daily is None or rb_diag is None or status != "ok":
        summary = _empty_pair_summary(row, status=status, gate_reason=gate_reason, price_rows=price_rows)
        return {
            "schema": PAIR_SCHEMA,
            "generated_at_utc": generated_at_utc,
            "policy_version": policy_version,
            "etf": summary["etf"],
            "underlying": summary["underlying"],
            "in_production_book": False,
            "production_status": "gated_research",
            "gate_reason": summary["gate_reason"],
            "model_status": status,
            "summary": summary,
            "screener": _screener_payload(row),
            "daily": {"dates": []},
            "rebalance_log": [],
        }
    summary = _pair_summary(
        row,
        bt,
        h_daily,
        rb_diag,
        production_weight=production_weight,
        effective_weight=effective_weight,
        in_production_book=in_production_book,
        gate_reason=gate_reason,
    )
    return {
        "schema": PAIR_SCHEMA,
        "generated_at_utc": generated_at_utc,
        "policy_version": policy_version,
        "etf": summary["etf"],
        "underlying": summary["underlying"],
        "in_production_book": bool(in_production_book),
        "production_status": summary["production_status"],
        "gate_reason": summary["gate_reason"],
        "model_status": "ok",
        "first_metrics_date": summary["first_metrics_date"],
        "entry_date": summary["entry_date"],
        "latest_date": summary["latest_date"],
        "summary": summary,
        "screener": _screener_payload(row),
        "daily": _pair_daily_payload(bt),
        "rebalance_log": _rebalance_log(bt),
    }


def port_returns(ret_df: pd.DataFrame, weights: pd.Series, *, renormalize: bool = False) -> pd.Series:
    """Blend pair returns. Production path keeps cash residual (renormalize=False)."""
    w = weights.reindex(ret_df.columns).fillna(0.0)
    if w.sum() <= 1e-12:
        return pd.Series(dtype=float)
    if renormalize:
        w = w / w.sum()
    pr = ret_df.mul(w, axis=1).sum(axis=1)
    return pr.clip(lower=RET_FLOOR, upper=0.95)


def build_pair_shards(
    rows: pd.DataFrame,
    panel: dict[str, pd.DataFrame],
    policy: dict,
    *,
    start: str,
    min_days: int,
    warmup_bdays: int,
    signal_window: int,
    vol_history: dict[str, pd.DataFrame],
    production_pairs: list[dict],
    generated_at_utc: str,
    policy_version: str,
) -> tuple[dict[str, dict], list[dict]]:
    prod_by_sym = {str(p.get("etf", "")).upper(): p for p in (production_pairs or [])}
    shards: dict[str, dict] = {}
    manifest: list[dict] = []
    for _, row in rows.iterrows():
        etf = _norm(row.get("ETF"))
        prod = prod_by_sym.get(etf)
        gate_reason = str(row.get("gate_reason") or "").strip()
        in_prod = prod is not None
        px = panel.get(etf)
        price_rows = int(len(px.loc[px.index >= pd.Timestamp(start)])) if px is not None and not px.empty else 0
        bt, h_daily, rb_diag, status = run_pair_backtest_for_row(
            row,
            panel,
            policy,
            start=start,
            min_days=min_days,
            warmup_bdays=warmup_bdays,
            signal_window=signal_window,
            vol_history=vol_history,
            borrow_history=None,  # loaded inside when pit_borrow enabled
        )
        shard = pair_shard_from_result(
            row,
            bt,
            h_daily,
            rb_diag,
            status=status,
            gate_reason=gate_reason or ("production_book" if in_prod else "not_in_production_book"),
            in_production_book=in_prod,
            production_weight=prod.get("portfolio_weight") if prod else None,
            effective_weight=prod.get("effective_weight") if prod else None,
            generated_at_utc=generated_at_utc,
            policy_version=policy_version,
            price_rows=price_rows,
        )
        shards[etf] = shard
        summ = shard.get("summary") or {}
        manifest.append(
            {
                "etf": etf,
                "underlying": _norm(row.get("Underlying")),
                "in_production_book": in_prod,
                "production_status": "production" if in_prod else "gated_research",
                "gate_reason": shard.get("gate_reason") or summ.get("gate_reason"),
                "model_status": shard.get("model_status"),
                "first_metrics_date": summ.get("first_metrics_date"),
                "entry_date": summ.get("entry_date"),
                "latest_date": summ.get("latest_date"),
                "price_rows": summ.get("price_rows"),
                "shard_url": f"data/bucket4_pairs/{etf}.json",
                "production_weight": prod.get("portfolio_weight") if prod else None,
                "effective_weight": prod.get("effective_weight") if prod else None,
                "bucket4_net_edge_annual": summ.get("bucket4_net_edge_annual"),
                "borrow": summ.get("borrow"),
                "beta": summ.get("beta"),
                "vol_underlying_annual": summ.get("vol_underlying_annual"),
                "cagr": summ.get("cagr"),
                "ann_vol": summ.get("ann_vol"),
                "vol_annual": summ.get("vol_annual"),
                "sharpe": summ.get("sharpe"),
                "max_drawdown": summ.get("max_drawdown"),
                "n_rebalances": summ.get("n_rebalances"),
                "h_last": summ.get("h_last"),
            }
        )
    manifest.sort(key=lambda x: (not x.get("in_production_book"), str(x.get("etf", ""))))
    return shards, manifest


def write_pair_shards(shards: dict[str, dict], out_dir: Path = OUT_PAIR_DIR) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for old in out_dir.glob("*.json"):
        old.unlink()
    for sym, payload in sorted(shards.items()):
        (out_dir / f"{sym}.json").write_text(
            json.dumps(_json_safe(payload), indent=2, allow_nan=False),
            encoding="utf-8",
        )


def build_backtest(
    uni: pd.DataFrame,
    panel: dict[str, pd.DataFrame],
    policy: dict,
    *,
    start: str,
    min_days: int,
    warmup_bdays: int,
    signal_window: int,
    vol_history: dict[str, pd.DataFrame],
    screened_csv: str | Path | None = None,
    legacy_concentration: bool = False,
) -> dict | None:
    if uni.empty:
        return None

    rules = (policy.get("inverse_decay_bucket4") or {}).get("rules") or {}
    bt_cfg = policy.get("backtest") or {}
    opt2 = rules.get("bucket4_weekly_opt2") or {}
    blk = knobs_from_policy(policy)
    knobs, tilts, _source = load_policy_from_config(policy)
    knobs = make_knobs(blk)

    slippage_bps = float(opt2.get("slippage_bps", 20.0))
    fee_bps = float(opt2.get("fee_bps", 1.0))
    initial_capital = float(bt_cfg.get("initial_capital", 1.0))
    sleeve_budget_usd = float(bt_cfg.get("sleeve_budget_usd", 100_000.0))
    walk_forward = bool(bt_cfg.get("walk_forward", True))

    ratchet_cfg = RatchetConfig.from_cfg(rules.get("ratchet") or policy.get("bucket_4_ratchet"))
    ratchet_sim = SimRatchetState(cfg=ratchet_cfg)
    borrow_history = load_borrow_history() if bool(bt_cfg.get("pit_borrow", True)) else {}

    ret_cols: dict[str, pd.Series] = {}
    pair_meta: list[dict] = []
    pair_series: dict[str, dict] = {}
    h_state: dict[str, dict] = {}
    h_by_underlying: dict[str, pd.Series] = {}
    row_by_etf: dict[str, pd.Series] = {}

    for _, row in uni.iterrows():
        etf = _norm(row.get("ETF"))
        und = _norm(row.get("Underlying"))
        px = panel.get(etf)
        if px is None or px.empty:
            continue
        cal = pd.DatetimeIndex([d for d in px.index if d >= pd.Timestamp(start)])
        if len(cal) < min_days:
            continue

        sig = get_pair_signal(
            etf,
            und,
            cal,
            history=vol_history,
            underlying_prices=px["b_px"],
            window=signal_window,
            lookahead_shift=1,
        )
        tilt = tilts.get(etf) or tilts.get(und)
        h_daily = build_h_series(sig, cal, knobs=knobs, name_tilt=tilt)
        rb, rb_diag = build_rebal_dates(sig, cal, knobs=knobs, name_tilt=tilt, warmup_bdays=warmup_bdays)

        borrow = _finite_float(row.get("borrow_current"), 0.0)
        beta = _finite_float(row.get("Delta"), -2.0)
        edge = _finite_float(row.get("bucket4_net_edge_annual"), 0.0)
        eng = _pair_engine_kwargs(policy, row, cal, borrow_history)

        bt = run_bucket4_backtest_dynamic_h(
            px.reindex(cal),
            h_daily,
            rb,
            initial_capital=initial_capital,
            gross_multiplier=1.0,
            beta_a=-abs(beta),
            beta_b=1.0,
            **eng,
        )
        ret_cols[etf] = bt["ret"]
        h_by_underlying[und] = h_daily
        row_by_etf[etf] = row

        stats = perf_stats(bt)
        mean_h = float(h_daily.dropna().mean()) if len(h_daily.dropna()) else float(knobs.h_mid)
        last_h = float(h_daily.dropna().iloc[-1]) if len(h_daily.dropna()) else float(knobs.h_mid)
        n_rebal = int(bt["rebalance"].sum()) if "rebalance" in bt.columns else 0
        n_skip = int(bt["rebalance_skipped_below_drift"].sum()) if "rebalance_skipped_below_drift" in bt.columns else 0
        total_borrow = float(bt["borrow_cost"].sum()) if "borrow_cost" in bt.columns else 0.0
        total_fees = float(bt["rebalance_fee"].sum()) if "rebalance_fee" in bt.columns else 0.0
        conc = float(concentration_scores(pd.DataFrame([row])).iloc[0])

        h_state[f"{etf}|{und}"] = {
            "etf": etf,
            "underlying": und,
            "h_last": round(last_h, 4),
            "mean_h": round(mean_h, 4),
            "n_rebalances": n_rebal,
            "n_rebalances_skipped": n_skip,
            "h_series_tail": [round(float(x), 4) for x in h_daily.dropna().tail(5).tolist()],
        }
        if len(rb_diag):
            h_state[f"{etf}|{und}"]["last_interval_days"] = int(rb_diag["interval_days"].iloc[-1])

        meta = {
            "etf": etf,
            "underlying": und,
            "weight": 0.0,
            "effective_weight": 0.0,
            "portfolio_weight": 0.0,
            "borrow": round(borrow, 4),
            "beta": round(beta, 4),
            "bucket4_net_edge_annual": round(edge, 4),
            "vol_underlying_annual": round(_finite_float(row.get("vol_underlying_annual"), np.nan), 4),
            "concentration_score": round(conc, 4) if np.isfinite(conc) else None,
            "n_days": int(len(cal)),
            "cagr": round(float(stats.get("cagr", np.nan)), 4) if np.isfinite(stats.get("cagr", np.nan)) else None,
            "ann_vol": round(float(stats.get("annual_vol", np.nan)), 4) if np.isfinite(stats.get("annual_vol", np.nan)) else None,
            "sharpe": round(float(stats.get("sharpe", np.nan)), 3) if np.isfinite(stats.get("sharpe", np.nan)) else None,
            "max_drawdown": round(float(stats.get("max_drawdown", np.nan)), 4) if np.isfinite(stats.get("max_drawdown", np.nan)) else None,
            "mean_h": round(mean_h, 4),
            "h_last": round(last_h, 4),
            "n_rebalances": n_rebal,
            "n_rebalances_skipped": n_skip,
            "total_borrow": round(total_borrow, 6),
            "total_fees": round(total_fees, 6),
            "final_equity": round(float(bt["equity"].iloc[-1]), 6) if len(bt) else None,
            "ratchet": None,
        }
        pair_meta.append(meta)
        pair_series[etf] = {
            "schema": "bucket4_pair.v1",
            "etf": etf,
            "underlying": und,
            "in_production_book": False,
            "summary": {
                "cagr": meta["cagr"],
                "ann_vol": meta["ann_vol"],
                "sharpe": meta["sharpe"],
                "max_drawdown": meta["max_drawdown"],
                "n_rebalances": n_rebal,
                "n_rebalances_skipped": n_skip,
                "mean_h": round(mean_h, 4),
                "h_last": round(last_h, 4),
                "total_borrow": round(total_borrow, 6),
                "total_fees": round(total_fees, 6),
                "final_equity": meta["final_equity"],
            },
            "screener": {
                "bucket4_net_edge_annual": round(edge, 4),
                "borrow": round(borrow, 4),
                "beta": round(beta, 4),
                "vol_underlying_annual": meta["vol_underlying_annual"],
                "init_pct_short": _round_or_none(row.get("init_pct_short"), 4),
                "maint_pct_short": _round_or_none(row.get("maint_pct_short"), 4),
                "purgatory": str(row.get("purgatory", "")).strip().lower() in {"1", "true", "t", "yes", "y"},
            },
            "daily": _pair_daily_payload(bt),
            "rebalance_log": _rebalance_log(bt),
        }

    if not ret_cols:
        return None

    all_idx = sorted(set().union(*[set(s.index) for s in ret_cols.values()]))
    ret_df = pd.DataFrame(ret_cols).reindex(all_idx)

    weight_history: list[dict] | None = None
    sizing_latest: dict | None = None
    sizing_method = SIZING_METHOD
    deployed_fraction = 0.0
    cash_residual = 1.0
    latest_w: dict[str, float] = {}
    tele_by_etf: dict[str, dict] = {}
    latest_meta: dict = {}

    def _apply_ratchet_and_weights(
        w_map: dict[str, float],
        *,
        attach_crash: bool,
        apply_ratchet: bool = True,
    ) -> dict[str, float]:
        """Attach latest weights to pair_meta.

        When ``apply_ratchet`` is False (walk-forward path already ratcheted
        inside ``build_walk_forward_weights``), weights are used as-is.
        """
        eff: dict[str, float] = {}
        for p in pair_meta:
            etf = p["etf"]
            und = p["underlying"]
            w = float(w_map.get(etf, 0.0))
            if not np.isfinite(w) or w < 0:
                w = 0.0
            edge = _finite_float(p.get("bucket4_net_edge_annual"), 0.0)
            borrow = _finite_float(p.get("borrow"), 0.0)
            if apply_ratchet and w > 1e-12:
                _gross_mult, rat_res = ratchet_sim.apply_gross_multiplier(
                    etf, und, w, fwd_edge=edge, borrow=borrow,
                )
                eff_w = float(w) * float(_gross_mult)
                p["ratchet"] = {
                    "trim_lambda": round(rat_res.trim_lambda, 4),
                    "binding": rat_res.binding,
                    "source": rat_res.source,
                }
            else:
                eff_w = float(w)
                p["ratchet"] = {
                    "trim_lambda": 0.0,
                    "binding": False,
                    "source": "walk_forward" if not apply_ratchet else "solve",
                }
            p["weight"] = round(w, 4)
            p["effective_weight"] = round(eff_w, 4)
            p["portfolio_weight"] = round(eff_w, 4)
            if attach_crash:
                tel = tele_by_etf.get(etf) or {}
                p["opt2_weight"] = _round_or_none(tel.get("weight_solved"), 6)
                p["crash_budget_mult"] = _round_or_none(tel.get("crash_budget_mult"), 4)
                p["cap_usd"] = _round_or_none(tel.get("cap_usd"), 2)
                p["L"] = _round_or_none(tel.get("L"), 6)
                p["C"] = _round_or_none(tel.get("C"), 6)
                p["runup"] = _round_or_none(tel.get("runup"), 6)
                p["tail"] = _round_or_none(tel.get("tail"), 6)
                p["hedge_ratio_at_size"] = _round_or_none(tel.get("hedge_ratio"), 4)
                p["crash_l_source"] = tel.get("crash_l_source")
            if eff_w > 1e-12:
                if apply_ratchet:
                    ratchet_sim.record_rebalance(etf, und, eff_w)
                eff[etf] = eff_w
            ps = pair_series.get(etf)
            if ps is not None:
                ps["in_production_book"] = eff_w > 1e-12
                ps["summary"]["portfolio_weight"] = p["portfolio_weight"]
                ps["summary"]["effective_weight"] = p["effective_weight"]
        return eff

    if legacy_concentration:
        sizing_method = "legacy_concentration"
        uni_sized, weights_arr = score_weights_legacy_concentration(uni, policy)
        w_map: dict[str, float] = {}
        for i, (_, row) in enumerate(uni_sized.iterrows()):
            etf = _norm(row.get("ETF"))
            w_map[etf] = float(weights_arr[i]) if i < len(weights_arr) else 0.0
        eff_map = _apply_ratchet_and_weights(w_map, attach_crash=False)
        gross_w = pd.Series({e: eff_map.get(e, 0.0) for e in ret_df.columns}).fillna(0.0)
        deployed_fraction = float(gross_w.clip(lower=0.0).sum())
        cash_residual = max(0.0, 1.0 - deployed_fraction)
        pr = port_returns(ret_df, gross_w, renormalize=True)
    else:
        if find_ls_algo() is None:
            raise RuntimeError(
                "ls-algo not found (need scripts/bucket4_backtest_api.py). "
                "Set LS_ALGO_ROOT, checkout ls-algo beside this repo, or pass --legacy-concentration."
            )
        csv_path = Path(screened_csv) if screened_csv is not None else DEFAULT_SCREENER
        end_ts = pd.Timestamp(ret_df.index.max())

        if walk_forward:
            wdf, _tele_hist, latest_meta = build_walk_forward_weights(
                uni.loc[uni["ETF"].map(_norm).isin(ret_cols.keys())].copy(),
                panel,
                h_by_underlying,
                screened_csv=csv_path,
                policy=policy,
                start=start,
                end=end_ts,
                sleeve_budget_usd=sleeve_budget_usd,
                warmup_bdays=warmup_bdays,
            )
            w_mat = expand_weights_to_calendar(wdf, pd.DatetimeIndex(ret_df.index))
            pr = port_returns_with_cash(ret_df, w_mat, ret_floor=RET_FLOOR)
            sizing_latest = {
                k: v
                for k, v in (latest_meta or {}).items()
                if not isinstance(v, (pd.DataFrame, pd.Series))
            }
            latest_w = {
                _norm(k): float(v)
                for k, v in (latest_meta.get("weights_by_etf") or wdf.iloc[-1].to_dict()).items()
            }
            tele_by_etf = _telemetry_by_etf(latest_meta.get("telemetry"))
            deployed_fraction = float(latest_meta.get("deployed_fraction", float(wdf.iloc[-1].sum())))
            cash_residual = float(latest_meta.get("cash_residual", max(0.0, 1.0 - deployed_fraction)))
            sizing_method = str(latest_meta.get("sizing_method") or SIZING_METHOD)
            weight_history = []
            for dt, row in wdf.iterrows():
                entry: dict = {
                    "date": pd.Timestamp(dt).strftime("%Y-%m-%d"),
                    "cash": round(max(0.0, 1.0 - float(row.sum())), 4),
                }
                for etf, w in row.items():
                    wf = float(w)
                    if wf > 1e-12:
                        entry[str(etf)] = round(wf, 6)
                weight_history.append(entry)
            # Walk-forward already applied smoothing + crash + ratchet.
            _apply_ratchet_and_weights(latest_w, attach_crash=True, apply_ratchet=False)
            if latest_meta.get("ratchet_state"):
                # Prefer WF sim state for artifact persistence.
                rs = latest_meta["ratchet_state"]
                if isinstance(rs, dict):
                    ratchet_sim.floors = dict(rs.get("inverse_short_usd_by_pair") or {})
                    ratchet_sim.held_gross = dict(rs.get("held_gross_by_pair") or {})
        else:
            live = uni.loc[uni["ETF"].map(_norm).isin(ret_cols.keys())].copy()
            sized = size_production_book(
                live,
                panel,
                h_by_underlying,
                screened_csv=csv_path,
                policy=policy,
                run_date=end_ts,
                sleeve_budget_usd=sleeve_budget_usd,
                ratchet_sim=ratchet_sim,
                borrow_history=borrow_history,
            )
            latest_w = {_norm(k): float(v) for k, v in sized.weights_by_etf().items()}
            if sized.weights_capped:
                latest_w = {_norm(k[0]): float(v) for k, v in sized.weights_capped.items()}
            tele_by_etf = _telemetry_by_etf(sized.telemetry)
            deployed_fraction = float(sized.deployed_fraction)
            cash_residual = float(sized.cash_residual)
            sizing_method = str(sized.sizing_method or SIZING_METHOD)
            sizing_latest = sized.to_dict()
            w_series = pd.Series({e: latest_w.get(e, 0.0) for e in ret_df.columns}).fillna(0.0)
            # Static deployed book with cash residual (no renorm).
            pr = port_returns(ret_df, w_series, renormalize=False)
            _apply_ratchet_and_weights(latest_w, attach_crash=True, apply_ratchet=False)

    # default_weights = effective_weight WITHOUT renormalizing to 1
    default_weights: dict[str, float] = {}
    production_pairs: list[dict] = []
    for p in pair_meta:
        ew = float(p.get("effective_weight") or 0.0)
        if ew > 1e-12:
            default_weights[p["etf"]] = round(ew, 6)
            production_pairs.append(p)

    prv = pr.dropna()
    arr = prv.to_numpy(dtype=float)
    eq = (1.0 + prv).cumprod()
    perf = perf_stats(pd.DataFrame({"equity": eq, "ret": prv, "drawdown": eq / eq.cummax() - 1.0}))

    return {
        "sizing_method": sizing_method,
        "deployed_fraction": round(float(deployed_fraction), 6),
        "cash_residual": round(float(cash_residual), 6),
        "sleeve_budget_usd": round(float(sleeve_budget_usd), 2),
        "weight_history": weight_history,
        "sizing_latest": sizing_latest,
        "pairs": production_pairs,
        "universes": {
            "production_book": {
                "pairs": [p["etf"] for p in production_pairs],
                "count": len(production_pairs),
            },
            "screener_b4": {
                "pairs": [p["etf"] for p in pair_meta],
                "count": len(pair_meta),
                "note": "Current artifact contains production-book pairs with full daily paths.",
            },
        },
        "default_weights": default_weights,
        "pair_series": pair_series,
        "n_pairs": len(production_pairs),
        "n_obs": int(len(arr)),
        "window_start": start,
        "sim_dates": [d.strftime("%Y-%m-%d") for d in prv.index],
        "port_daily_returns": [round(float(x), 6) for x in arr],
        "port_equity": [round(float(x), 6) for x in eq.to_numpy()],
        "realized": {
            "cagr": round(float(perf.get("cagr", np.nan)), 4) if np.isfinite(perf.get("cagr", np.nan)) else None,
            "ann_vol": round(float(perf.get("annual_vol", np.nan)), 4) if np.isfinite(perf.get("annual_vol", np.nan)) else None,
            "sharpe": round(float(perf.get("sharpe", np.nan)), 3) if np.isfinite(perf.get("sharpe", np.nan)) else None,
            "maxdd": round(float(perf.get("max_drawdown", np.nan)), 4) if np.isfinite(perf.get("max_drawdown", np.nan)) else None,
        },
        "cadence": {
            "cadence_signal_col": blk.get("cadence_signal_col"),
            "base_days": blk.get("base_days"),
            "k_tr": blk.get("k_tr"),
            "m_vcr": blk.get("m_vcr"),
            "min_interval": blk.get("min_interval"),
            "max_interval": blk.get("max_interval"),
            "h_mid": blk.get("h_mid"),
            "k_vcr": blk.get("k_vcr"),
        },
        "h_state": h_state,
        "ratchet_state": ratchet_sim.as_dict(),
        "parity": {
            **((latest_meta or {}).get("parity_layers") or {}),
            **pit_meta(borrow_history, enabled=bool(bt_cfg.get("pit_borrow", True))),
            "drift_threshold_share_of_gross": opt2.get("drift_threshold_share_of_gross"),
            "crash_rho": float((opt2.get("crash_budget") or {}).get("rho", 0.087)),
            "custom_book_match_deployed_fraction": bool(
                bt_cfg.get("custom_book_match_deployed_fraction", True)
            ),
            "max_weight": float(opt2.get("max_weight", 0.35)),
        },
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--screener", default=str(DEFAULT_SCREENER))
    ap.add_argument("--policy", default=str(DEFAULT_POLICY))
    ap.add_argument("--start", default=None)
    ap.add_argument("--min-days", type=int, default=None)
    ap.add_argument("--warmup-bdays", type=int, default=None)
    ap.add_argument("--signal-window", type=int, default=None)
    ap.add_argument(
        "--legacy-concentration",
        action="store_true",
        help="Use legacy (edge-borrow)/vol concentration weights instead of ls-algo opt2+crash sizing.",
    )
    args = ap.parse_args(argv)

    policy_path = Path(args.policy)
    policy = load_policy(policy_path)
    phash = policy_hash(policy_path)

    bt_cfg = policy.get("backtest") or {}
    start = args.start or bt_cfg.get("start", "2024-01-01")
    min_days = args.min_days if args.min_days is not None else int(bt_cfg.get("min_days", 60))
    warmup_bdays = args.warmup_bdays if args.warmup_bdays is not None else int(bt_cfg.get("warmup_bdays", 60))
    signal_window = args.signal_window if args.signal_window is not None else int(bt_cfg.get("signal_window", 60))

    all_b4 = load_bucket4_rows(Path(args.screener), policy)
    uni = all_b4.loc[all_b4.get("production_candidate", pd.Series(False, index=all_b4.index)).fillna(False)].copy()
    panel = load_price_panel(min_days=min(PAIR_SHARD_MIN_DAYS, min_days))
    vol_history = load_vol_shape_history(VOL_SHAPE_HISTORY) if VOL_SHAPE_HISTORY.is_file() else {}

    built = build_backtest(
        uni,
        panel,
        policy,
        start=start,
        min_days=min_days,
        warmup_bdays=warmup_bdays,
        signal_window=signal_window,
        vol_history=vol_history,
        screened_csv=args.screener,
        legacy_concentration=bool(args.legacy_concentration),
    )
    if built is None:
        print("[bucket4-bt] no eligible pairs after gates", file=sys.stderr)
        return 1

    generated_at = datetime.now(timezone.utc).isoformat()
    pair_shards, pair_manifest = build_pair_shards(
        all_b4,
        panel,
        policy,
        start=start,
        min_days=PAIR_SHARD_MIN_DAYS,
        warmup_bdays=min(warmup_bdays, max(0, PAIR_SHARD_MIN_DAYS - 1)),
        signal_window=min(signal_window, PAIR_SHARD_MIN_DAYS),
        vol_history=vol_history,
        production_pairs=built.get("pairs", []),
        generated_at_utc=generated_at,
        policy_version=phash[:16],
    )
    built["pair_manifest"] = pair_manifest
    built["pair_shard_base_url"] = "data/bucket4_pairs"
    built["universes"]["screener_b4"] = {
        "pairs": [str(x.get("etf", "")).upper() for x in pair_manifest],
        "count": len(pair_manifest),
        "note": "All screener Bucket 4 rows. Daily report paths live in data/bucket4_pairs/{ETF}.json.",
    }

    payload = {
        "schema": SCHEMA,
        "generated_at_utc": generated_at,
        "policy_version": phash[:16],
        "policy_path": str(policy_path.relative_to(REPO)).replace("\\", "/"),
        **built,
    }
    state_payload = {
        "schema": "bucket4_backtest_state.v1",
        "generated_at_utc": payload["generated_at_utc"],
        "policy_version": phash[:16],
        "sizing_method": built.get("sizing_method"),
        "cash_residual": built.get("cash_residual"),
        "deployed_fraction": built.get("deployed_fraction"),
        "h_by_pair": built.get("h_state", {}),
        "ratchet": built.get("ratchet_state", {}),
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    write_pair_shards(pair_shards)
    OUT_JSON.write_text(json.dumps(_json_safe(payload), indent=2, allow_nan=False), encoding="utf-8")
    OUT_STATE.write_text(json.dumps(_json_safe(state_payload), indent=2, allow_nan=False), encoding="utf-8")
    OUT_HASH.write_text(phash + "\n", encoding="utf-8")

    print(f"[bucket4-bt] wrote {OUT_JSON}")
    print(f"[bucket4-bt] wrote {OUT_STATE}")
    print(f"[bucket4-bt] policy hash {phash[:16]}…")
    print(
        f"[bucket4-bt] pairs={built['n_pairs']} obs={built['n_obs']} "
        f"CAGR={built['realized'].get('cagr')} maxDD={built['realized'].get('maxdd')}"
    )
    print(
        f"[bucket4-bt] sizing={built.get('sizing_method')} "
        f"deployed={built.get('deployed_fraction')} cash={built.get('cash_residual')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

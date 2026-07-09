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
from bucket4.policy_helpers import knobs_from_policy, load_policy, make_knobs  # noqa: E402

DEFAULT_SCREENER = REPO / "data" / "etf_screened_today.csv"
DEFAULT_POLICY = REPO / "config" / "bucket4_backtest_policy.yml"
OUT_JSON = REPO / "data" / "bucket4_backtest.json"
OUT_STATE = REPO / "data" / "bucket4_backtest_state.json"
OUT_HASH = REPO / "data" / "bucket4_backtest_policy_hash.txt"
OUT_PAIR_DIR = REPO / "data" / "bucket4_pairs"
VOL_SHAPE_HISTORY = REPO / "data" / "vol_shape_history.json"
RET_FLOOR = -0.95
SCHEMA = "bucket4_backtest.v2"
PAIR_SCHEMA = "bucket4_pair.v2"
PAIR_SHARD_MIN_DAYS = 20


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


def score_weights(df: pd.DataFrame, policy: dict) -> tuple[pd.DataFrame, np.ndarray]:
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
    opt2 = rules.get("bucket4_weekly_opt2") or {}
    blk = knobs_from_policy(policy)
    _knobs0, tilts, _source = load_policy_from_config(policy)
    knobs = make_knobs(blk)

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
    bt = run_bucket4_backtest_dynamic_h(
        px.reindex(cal),
        h_daily,
        rb,
        initial_capital=float(bt_cfg.get("initial_capital", 1.0)),
        gross_multiplier=1.0,
        beta_a=-abs(_finite_float(row.get("Delta"), -2.0)),
        beta_b=1.0,
        borrow_a_annual=_finite_float(row.get("borrow_current"), 0.0),
        slippage_bps=float(opt2.get("slippage_bps", 20.0)),
        fee_bps=float(opt2.get("fee_bps", 1.0)),
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


def port_returns(ret_df: pd.DataFrame, weights: pd.Series) -> pd.Series:
    w = weights.reindex(ret_df.columns).fillna(0.0)
    if w.sum() <= 1e-12:
        return pd.Series(dtype=float)
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
            json.dumps(payload, indent=2, allow_nan=False),
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

    ratchet_cfg = RatchetConfig.from_cfg(rules.get("ratchet") or policy.get("bucket_4_ratchet"))
    ratchet_sim = SimRatchetState(cfg=ratchet_cfg)

    uni, weights_arr = score_weights(uni, policy)
    ret_cols: dict[str, pd.Series] = {}
    pair_meta: list[dict] = []
    pair_series: dict[str, dict] = {}
    h_state: dict[str, dict] = {}

    for i, (_, row) in enumerate(uni.iterrows()):
        etf, und = row["ETF"], row["Underlying"]
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
        w = float(weights_arr[i]) if i < len(weights_arr) else 0.0
        if w <= 0:
            continue

        _gross_mult, rat_res = ratchet_sim.apply_gross_multiplier(
            etf, und, w, fwd_edge=edge, borrow=borrow,
        )
        eff_w = w * _gross_mult

        bt = run_bucket4_backtest_dynamic_h(
            px.reindex(cal),
            h_daily,
            rb,
            initial_capital=initial_capital,
            gross_multiplier=1.0,
            beta_a=-abs(beta),
            beta_b=1.0,
            borrow_a_annual=borrow,
            slippage_bps=slippage_bps,
            fee_bps=fee_bps,
        )
        ret_cols[etf] = bt["ret"]
        stats = perf_stats(bt)
        mean_h = float(h_daily.dropna().mean()) if len(h_daily.dropna()) else float(knobs.h_mid)
        last_h = float(h_daily.dropna().iloc[-1]) if len(h_daily.dropna()) else float(knobs.h_mid)
        n_rebal = int(bt["rebalance"].sum()) if "rebalance" in bt.columns else 0
        n_skip = int(bt["rebalance_skipped_below_drift"].sum()) if "rebalance_skipped_below_drift" in bt.columns else 0
        total_borrow = float(bt["borrow_cost"].sum()) if "borrow_cost" in bt.columns else 0.0
        total_fees = float(bt["rebalance_fee"].sum()) if "rebalance_fee" in bt.columns else 0.0
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

        pair_meta.append({
            "etf": etf,
            "underlying": und,
            "weight": round(w, 4),
            "effective_weight": round(eff_w, 4),
            "borrow": round(borrow, 4),
            "beta": round(beta, 4),
            "bucket4_net_edge_annual": round(edge, 4),
            "vol_underlying_annual": round(_finite_float(row.get("vol_underlying_annual"), np.nan), 4),
            "concentration_score": round(float(concentration_scores(pd.DataFrame([row])).iloc[0]), 4),
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
            "ratchet": {
                "trim_lambda": round(rat_res.trim_lambda, 4),
                "binding": rat_res.binding,
                "source": rat_res.source,
            },
        })
        pair_series[etf] = {
            "schema": "bucket4_pair.v1",
            "etf": etf,
            "underlying": und,
            "in_production_book": True,
            "summary": {
                "cagr": pair_meta[-1]["cagr"],
                "ann_vol": pair_meta[-1]["ann_vol"],
                "sharpe": pair_meta[-1]["sharpe"],
                "max_drawdown": pair_meta[-1]["max_drawdown"],
                "n_rebalances": n_rebal,
                "n_rebalances_skipped": n_skip,
                "mean_h": round(mean_h, 4),
                "h_last": round(last_h, 4),
                "total_borrow": round(total_borrow, 6),
                "total_fees": round(total_fees, 6),
                "final_equity": pair_meta[-1]["final_equity"],
            },
            "screener": {
                "bucket4_net_edge_annual": round(edge, 4),
                "borrow": round(borrow, 4),
                "beta": round(beta, 4),
                "vol_underlying_annual": pair_meta[-1]["vol_underlying_annual"],
                "init_pct_short": _round_or_none(row.get("init_pct_short"), 4),
                "maint_pct_short": _round_or_none(row.get("maint_pct_short"), 4),
                "purgatory": str(row.get("purgatory", "")).strip().lower() in {"1", "true", "t", "yes", "y"},
            },
            "daily": _pair_daily_payload(bt),
            "rebalance_log": _rebalance_log(bt),
        }
        ratchet_sim.record_rebalance(etf, und, eff_w)

    if not ret_cols:
        return None

    ret_df = pd.DataFrame(ret_cols).reindex(sorted(set().union(*[set(s.index) for s in ret_cols.values()])))
    gross_w = pd.Series({p["etf"]: p["effective_weight"] for p in pair_meta})
    pr = port_returns(ret_df, gross_w)
    prv = pr.dropna()
    arr = prv.to_numpy(dtype=float)

    eq = (1.0 + prv).cumprod()
    perf = perf_stats(pd.DataFrame({"equity": eq, "ret": prv, "drawdown": eq / eq.cummax() - 1.0}))

    total_w = float(gross_w.sum())
    default_weights: dict[str, float] = {}
    for p in pair_meta:
        g = float(gross_w.get(p["etf"], 0.0))
        p["portfolio_weight"] = round(g / total_w, 4) if total_w > 0 else 0.0
        default_weights[p["etf"]] = p["portfolio_weight"]

    return {
        "pairs": pair_meta,
        "universes": {
            "production_book": {
                "pairs": [p["etf"] for p in pair_meta],
                "count": len(pair_meta),
            },
            "screener_b4": {
                "pairs": [p["etf"] for p in pair_meta],
                "count": len(pair_meta),
                "note": "Current artifact contains production-book pairs with full daily paths.",
            },
        },
        "default_weights": default_weights,
        "pair_series": pair_series,
        "n_pairs": len(pair_meta),
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
    }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--screener", default=str(DEFAULT_SCREENER))
    ap.add_argument("--policy", default=str(DEFAULT_POLICY))
    ap.add_argument("--start", default=None)
    ap.add_argument("--min-days", type=int, default=None)
    ap.add_argument("--warmup-bdays", type=int, default=None)
    ap.add_argument("--signal-window", type=int, default=None)
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
        "h_by_pair": built.get("h_state", {}),
        "ratchet": built.get("ratchet_state", {}),
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    write_pair_shards(pair_shards)
    OUT_JSON.write_text(json.dumps(payload, indent=2, allow_nan=False), encoding="utf-8")
    OUT_STATE.write_text(json.dumps(state_payload, indent=2, allow_nan=False), encoding="utf-8")
    OUT_HASH.write_text(phash + "\n", encoding="utf-8")

    print(f"[bucket4-bt] wrote {OUT_JSON}")
    print(f"[bucket4-bt] wrote {OUT_STATE}")
    print(f"[bucket4-bt] policy hash {phash[:16]}…")
    print(
        f"[bucket4-bt] pairs={built['n_pairs']} obs={built['n_obs']} "
        f"CAGR={built['realized'].get('cagr')} maxDD={built['realized'].get('maxdd')}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

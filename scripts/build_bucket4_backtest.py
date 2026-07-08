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
VOL_SHAPE_HISTORY = REPO / "data" / "vol_shape_history.json"
RET_FLOOR = -0.95


def _norm(x: object) -> str:
    return str(x).strip().upper().replace(".", "-")


def _bool_series(s: pd.Series) -> pd.Series:
    if s.dtype == bool:
        return s.fillna(False)
    return s.fillna(False).astype(str).str.lower().isin({"1", "true", "t", "yes", "y"})


def _finite_float(val, default: float = 0.0) -> float:
    v = float(pd.to_numeric(val, errors="coerce"))
    return default if not np.isfinite(v) else v


def policy_hash(policy_path: Path) -> str:
    raw = policy_path.read_bytes()
    return hashlib.sha256(raw).hexdigest()


def load_universe(screener_path: Path, policy: dict) -> pd.DataFrame:
    sc = pd.read_csv(screener_path)
    if sc.empty:
        return sc
    sc = sc.copy()
    sc["ETF"] = sc["ETF"].map(_norm)
    sc["Underlying"] = sc["Underlying"].map(_norm)

    rules = (policy.get("inverse_decay_bucket4") or {}).get("rules") or {}
    b4_rules = (policy.get("bucket_4") or {}).get("rules") or {}
    buckets = (policy.get("bucket_4") or {}).get("screener_buckets") or ["bucket_4"]

    bucket = sc.get("bucket", pd.Series("", index=sc.index)).astype(str).str.lower()
    is_b4 = bucket.isin([str(b).lower() for b in buckets])
    beta = pd.to_numeric(sc.get("Delta"), errors="coerce")
    inv_ok = sc.get("inverse_shortable", pd.Series(True, index=sc.index))
    inv_ok = _bool_series(inv_ok) if inv_ok is not None else pd.Series(True, index=sc.index)

    excluded = {_norm(x) for x in (rules.get("excluded_etfs") or [])}
    not_excluded = ~sc["ETF"].isin(excluded)

    min_edge = float(rules.get("min_net_edge_annual", 0.30))
    edge = pd.to_numeric(sc.get("bucket4_net_edge_annual"), errors="coerce")
    edge_ok = edge.fillna(-1.0) >= min_edge

    min_vol = float(rules.get("min_underlying_vol", b4_rules.get("min_underlying_vol", 0.40)))
    vol = pd.to_numeric(sc.get("vol_underlying_annual"), errors="coerce")
    vol_ok = vol.fillna(0.0) >= min_vol

    if b4_rules.get("exclude_purgatory", True) and "purgatory" in sc.columns:
        not_purg = ~_bool_series(sc["purgatory"])
    else:
        not_purg = pd.Series(True, index=sc.index)

    blk = _bool_series(sc.get("strategy_blacklisted", pd.Series(False, index=sc.index)))

    mask = is_b4 & (beta < 0) & inv_ok & not_excluded & edge_ok & vol_ok & not_purg & ~blk
    out = sc.loc[mask].copy()
    out = out.drop_duplicates(subset=["ETF"], keep="first").reset_index(drop=True)
    return out


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


def port_returns(ret_df: pd.DataFrame, weights: pd.Series) -> pd.Series:
    w = weights.reindex(ret_df.columns).fillna(0.0)
    if w.sum() <= 1e-12:
        return pd.Series(dtype=float)
    w = w / w.sum()
    pr = ret_df.mul(w, axis=1).sum(axis=1)
    return pr.clip(lower=RET_FLOOR, upper=0.95)


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
        last_h = float(h_daily.dropna().iloc[-1]) if len(h_daily.dropna()) else float(knobs.h_mid)
        h_state[f"{etf}|{und}"] = {
            "etf": etf,
            "underlying": und,
            "h_last": round(last_h, 4),
            "n_rebalances": int(bt["rebalance"].sum()) if "rebalance" in bt.columns else 0,
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
            "max_drawdown": round(float(stats.get("max_drawdown", np.nan)), 4) if np.isfinite(stats.get("max_drawdown", np.nan)) else None,
            "ratchet": {
                "trim_lambda": round(rat_res.trim_lambda, 4),
                "binding": rat_res.binding,
                "source": rat_res.source,
            },
        })
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
    for p in pair_meta:
        g = float(gross_w.get(p["etf"], 0.0))
        p["portfolio_weight"] = round(g / total_w, 4) if total_w > 0 else 0.0

    return {
        "pairs": pair_meta,
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

    uni = load_universe(Path(args.screener), policy)
    panel = load_price_panel()
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

    payload = {
        "schema": "bucket4_backtest.v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
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

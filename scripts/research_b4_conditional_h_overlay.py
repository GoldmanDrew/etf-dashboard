#!/usr/bin/env python3
"""Pre-specified, research-only conditional h overlays on full price history.

Variants are evaluated from each pair's first usable joint-price session and
split at its first real ETF borrow observation.  The full-history and pre-borrow
lenses charge no borrow, intentionally isolating hedge mechanics.  They are not
net-performance or production-replay results.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import build_bucket4_backtest as b4  # noqa: E402
from bucket4.bucket4_dynamic_bt import run_bucket4_backtest_dynamic_h  # noqa: E402
from bucket4.bucket4_hedge_cadence import build_h_series, build_rebal_dates  # noqa: E402
from bucket4.bucket4_price_loading import load_price_panel  # noqa: E402
from bucket4.bucket4_vol_shape_signals import get_pair_signal, load_vol_shape_history  # noqa: E402
from bucket4.policy_helpers import knobs_from_policy, load_policy, make_knobs  # noqa: E402
from build_b4_inception_research import _first_valid_borrow_date, _row_for_etf  # noqa: E402
from research_b4_phase1_factorial import (  # noqa: E402
    DEFAULT_INPUT,
    DEFAULT_POLICY,
    VOL_SHAPE_HISTORY,
    _load_cleaned_inputs,
    _perf,
)

DEFAULT_OUTPUT = REPO / "data" / "_phase1_experiments" / "conditional_h_overlay_full_history.json"

# Locked before reading this test's results.  All overlays only raise h; current
# cadence, costs, and the current dynamic h series are otherwise untouched.
VARIANTS: dict[str, dict[str, Any]] = {
    "current": {"mode": "control"},
    "vcr_positive_h075": {"mode": "vcr_positive", "target": 0.75},
    "vcr_top33_h075": {"mode": "vcr_top33", "target": 0.75},
    "trend_weak_h075": {"mode": "trend_weak", "target": 0.75},
    "vcr_top33_and_trend_weak_h075": {"mode": "both", "target": 0.75},
    "vcr_top33_h060": {"mode": "vcr_top33", "target": 0.60},
}


def _condition(signal: pd.DataFrame, mode: str) -> pd.Series:
    dvcr = pd.to_numeric(signal.get("vcr"), errors="coerce") - pd.to_numeric(signal.get("vcr_med"), errors="coerce")
    tr = pd.to_numeric(signal.get("tr_est"), errors="coerce")
    if mode == "control":
        return pd.Series(False, index=signal.index)
    if mode == "vcr_positive":
        return (dvcr > 0).fillna(False)
    # The threshold is expanding and based only on signal values available at
    # that date (the signal itself already carries the one-session shift).
    top33 = dvcr > dvcr.expanding(min_periods=30).quantile(2.0 / 3.0)
    if mode == "vcr_top33":
        return top33.fillna(False)
    weak = tr <= 1.0
    if mode == "trend_weak":
        return weak.fillna(False)
    if mode == "both":
        return (top33 & weak).fillna(False)
    raise ValueError(f"Unknown overlay mode: {mode}")


def _run(
    row: pd.Series,
    prices: pd.DataFrame,
    policy: dict[str, Any],
    *,
    trade_start: pd.Timestamp,
    signal_floor: pd.Timestamp,
    window: int,
    warmup: int,
    vol_history: dict[str, pd.DataFrame],
    variant: dict[str, Any],
) -> tuple[pd.DataFrame, float]:
    cal = pd.DatetimeIndex(prices.index[prices.index >= trade_start])
    sig_cal = pd.DatetimeIndex(prices.index[prices.index >= signal_floor])
    sig = get_pair_signal(str(row["ETF"]), str(row["Underlying"]), sig_cal, history=vol_history,
                          underlying_prices=prices["b_px"], window=window, lookahead_shift=1)
    knobs = make_knobs(knobs_from_policy(policy))
    h = build_h_series(sig, sig_cal, knobs=knobs)
    trigger = _condition(sig, str(variant["mode"])).reindex(h.index).fillna(False)
    if "target" in variant:
        h = h.where(~trigger, np.maximum(h, float(variant["target"])))
    dates, _ = build_rebal_dates(sig, sig_cal, knobs=knobs, warmup_bdays=warmup)
    engine = b4._pair_engine_kwargs(policy, row, cal, {})
    # `row.borrow_current` is zeroed by the caller.  Do not let current fees
    # leak into the gross-history lens through the engine's spot fallback.
    engine["borrow_b_annual"] = 0.0
    bt = run_bucket4_backtest_dynamic_h(
        prices.reindex(cal), h, dates, initial_capital=1.0, gross_multiplier=1.0,
        beta_a=-abs(float(row.get("Delta") or -2.0)), beta_b=1.0,
        membership_start=str(trade_start.date()), capital_mode="unit_equity",
        stop_on_equity_wipeout=True, **engine,
    )
    return bt, float(trigger.reindex(cal).fillna(False).mean())


def _relative(alt: pd.Series, base: pd.Series) -> float | None:
    j = pd.concat([alt, base], axis=1, join="inner").dropna()
    if j.empty:
        return None
    return float(np.expm1((np.log1p(j.iloc[:, 0]) - np.log1p(j.iloc[:, 1])).sum()))


def _period(bt: pd.DataFrame, first_borrow: pd.Timestamp | None, pre: bool) -> dict[str, Any]:
    x = bt.loc[bt.index < first_borrow] if pre and first_borrow is not None else (bt.loc[bt.index >= first_borrow] if not pre and first_borrow is not None else bt.iloc[0:0])
    return _perf(x["ret"] if not x.empty else pd.Series(dtype=float))


def _median(v: list[float]) -> float | None:
    q = [float(x) for x in v if x is not None and math.isfinite(float(x))]
    return float(np.median(q)) if q else None


def _summary(records: list[dict[str, Any]], variant: str) -> dict[str, Any]:
    rows = [r for r in records if r["variant"] == variant]
    rel = [r["relative_full"] for r in rows if r["relative_full"] is not None]
    pre = [r for r in rows if r["pre_days"] >= 60 and r["relative_pre"] is not None]
    cluster_medians: list[float] = []
    for cluster in sorted({r["underlying"] for r in rows}):
        leave = [r["relative_full"] for r in rows if r["underlying"] != cluster and r["relative_full"] is not None]
        m = _median(leave)
        if m is not None:
            cluster_medians.append(m)
    return {
        "variant": variant,
        "pairs": len(rows),
        "full_wins": sum(x > 0 for x in rel),
        "full_median_relative_return": _median(rel),
        "full_cagr_wins": sum(r["cagr_delta_full"] > 0 for r in rows),
        "full_drawdown_improved": sum(r["drawdown_delta_full"] > 0 for r in rows),
        "full_es_improved": sum(r["es_delta_full"] > 0 for r in rows),
        "worst_underlying_leaveout_median_relative_return": min(cluster_medians) if cluster_medians else None,
        "pre_borrow_pairs_60d": len(pre),
        "pre_borrow_wins": sum(r["relative_pre"] > 0 for r in pre),
        "pre_borrow_median_relative_return": _median([r["relative_pre"] for r in pre]),
        "mean_trigger_share": float(np.mean([r["trigger_share"] for r in rows])) if rows else None,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    ap.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT)
    ap.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = ap.parse_args(argv)
    if not _load_cleaned_inputs(args.input_dir):
        raise SystemExit("No cleaned Phase-1 inputs")
    policy = load_policy(args.policy)
    bt_cfg = policy.get("backtest") or {}
    window, warmup = int(bt_cfg.get("signal_window", 60)), int(bt_cfg.get("warmup_bdays", 60))
    panel = load_price_panel(min_days=max(40, window + 1))
    vol_history = load_vol_shape_history(VOL_SHAPE_HISTORY) if VOL_SHAPE_HISTORY.is_file() else {}
    borrow = b4.load_borrow_history()
    records: list[dict[str, Any]] = []
    for etf in sorted(_load_cleaned_inputs(args.input_dir)):
        prices = panel.get(etf)
        if prices is None:
            continue
        prices = prices.dropna(subset=["a_px", "b_px"])
        if len(prices) <= warmup:
            continue
        first_trade, signal_floor = pd.Timestamp(prices.index[warmup]), pd.Timestamp(prices.index[0])
        row = _row_for_etf(etf).copy()
        row["borrow_current"] = 0.0
        first_borrow = _first_valid_borrow_date(borrow, etf)
        runs: dict[str, tuple[pd.DataFrame, float]] = {}
        for name, spec in VARIANTS.items():
            runs[name] = _run(row, prices, policy, trade_start=first_trade, signal_floor=signal_floor,
                              window=window, warmup=warmup, vol_history=vol_history, variant=spec)
        control = runs["current"][0]
        for name, (alt, share) in runs.items():
            full_rel = _relative(alt["ret"], control["ret"])
            pre_alt = alt.loc[alt.index < first_borrow] if first_borrow is not None else alt.iloc[0:0]
            pre_ctl = control.loc[control.index < first_borrow] if first_borrow is not None else control.iloc[0:0]
            p_alt, p_ctl = _perf(alt["ret"]), _perf(control["ret"])
            records.append({
                "etf": etf, "underlying": str(row.get("Underlying") or ""), "variant": name,
                "history_start": control.index.min().strftime("%Y-%m-%d"), "history_end": control.index.max().strftime("%Y-%m-%d"),
                "first_etf_borrow_observation": first_borrow.strftime("%Y-%m-%d") if first_borrow is not None else None,
                "trigger_share": share, "relative_full": full_rel,
                "relative_pre": _relative(pre_alt["ret"], pre_ctl["ret"]), "pre_days": int(len(pd.concat([pre_alt["ret"], pre_ctl["ret"]], axis=1, join="inner").dropna())),
                "cagr_delta_full": float(p_alt["cagr"] - p_ctl["cagr"]),
                "drawdown_delta_full": float(p_alt["max_drawdown"] - p_ctl["max_drawdown"]),
                "es_delta_full": float(p_alt["expected_shortfall_95_daily"] - p_ctl["expected_shortfall_95_daily"]),
            })
    payload = {
        "schema": "bucket4_conditional_h_overlay_full_history.v1", "authoritative": False,
        "disclaimer": "Pre-specified research-only conditional hedge overlays. Full-history and pre-borrow results are gross-only and not production or net-performance results.",
        "methodology": {"variants_locked_before_results": VARIANTS, "history": "first aligned joint-price session plus 60-session warmup", "signals": "PIT expanding VCR median with one-session shift", "cadence": "unchanged current dynamic cadence", "borrow": "zeroed on both legs", "validation": "full history, pre-borrow chronological backcast, pair breadth, and leave-one-underlying-out medians"},
        "policy_sha256": hashlib.sha256(args.policy.read_bytes()).hexdigest(),
        "summary": [_summary(records, v) for v in VARIANTS], "pairs": records,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({"ok": True, "pairs": len({r['etf'] for r in records}), "variants": len(VARIANTS), "output": str(args.output.resolve())}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

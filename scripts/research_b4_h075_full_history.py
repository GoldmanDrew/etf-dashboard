#!/usr/bin/env python3
"""Research-only full-price-history backcast for current policy vs fixed h=.75.

This deliberately has two lenses:
* full-history gross: no ETF or underlying borrow is charged, so it tests only
  hedge/cadence mechanics from the first reliable joint price window;
* observed-borrow net: remains in the Phase-1 experiment and is not extended
  before the first point-in-time ETF borrow observation.

The pre-borrow portion of full-history gross is a chronologically earlier
backcast, not a genuine out-of-sample result and never a production replay.
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

from bucket4.bucket4_price_loading import load_price_panel  # noqa: E402
from bucket4.bucket4_vol_shape_signals import load_vol_shape_history  # noqa: E402
from bucket4.policy_helpers import load_policy  # noqa: E402
import build_bucket4_backtest as b4  # noqa: E402
from build_b4_inception_research import _first_valid_borrow_date, _row_for_etf  # noqa: E402
from research_b4_phase1_factorial import (  # noqa: E402
    DEFAULT_INPUT,
    DEFAULT_POLICY,
    VOL_SHAPE_HISTORY,
    _load_cleaned_inputs,
    _paired_bootstrap,
    _perf,
    _run_pair,
    _variant_policy,
)

DEFAULT_OUTPUT = REPO / "data" / "_phase1_experiments" / "h075_full_history_results.json"


def _metrics_for_period(bt: pd.DataFrame, start: pd.Timestamp | None, end: pd.Timestamp | None) -> dict[str, Any]:
    window = bt
    if start is not None:
        window = window.loc[window.index < start]
    if end is not None:
        window = window.loc[window.index >= end]
    perf = _perf(window["ret"] if not window.empty else pd.Series(dtype=float))
    perf["start"] = window.index.min().strftime("%Y-%m-%d") if not window.empty else None
    perf["end"] = window.index.max().strftime("%Y-%m-%d") if not window.empty else None
    perf["rebalances"] = int(pd.to_numeric(window.get("rebalance"), errors="coerce").fillna(0).sum()) if not window.empty else 0
    return perf


def _compare(base: pd.DataFrame, alt: pd.DataFrame, *, first_borrow: pd.Timestamp | None, seed: int) -> dict[str, Any]:
    joined = pd.concat({"base": base["ret"], "h075": alt["ret"]}, axis=1, join="inner").dropna()
    whole = _paired_bootstrap(joined["h075"], joined["base"], seed=seed)
    pre = joined.loc[joined.index < first_borrow] if first_borrow is not None else joined.iloc[0:0]
    post = joined.loc[joined.index >= first_borrow] if first_borrow is not None else joined.iloc[0:0]
    return {
        "common_days": int(len(joined)),
        "relative_return": float(np.expm1((np.log1p(joined["h075"]) - np.log1p(joined["base"])).sum())) if len(joined) else None,
        "bootstrap": whole,
        "pre_borrow": {
            "common_days": int(len(pre)),
            "relative_return": float(np.expm1((np.log1p(pre["h075"]) - np.log1p(pre["base"])).sum())) if len(pre) else None,
        },
        "post_borrow": {
            "common_days": int(len(post)),
            "relative_return": float(np.expm1((np.log1p(post["h075"]) - np.log1p(post["base"])).sum())) if len(post) else None,
        },
    }


def _median(values: list[float]) -> float | None:
    vals = [float(x) for x in values if x is not None and math.isfinite(float(x))]
    return float(np.median(vals)) if vals else None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    ap.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT)
    ap.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = ap.parse_args(argv)

    cleaned = _load_cleaned_inputs(args.input_dir)
    if not cleaned:
        raise SystemExit(f"No cleaned Phase-1 inputs in {args.input_dir}")
    policy = load_policy(args.policy)
    bt_cfg = policy.get("backtest") or {}
    window = int(bt_cfg.get("signal_window", 60))
    warmup = int(bt_cfg.get("warmup_bdays", 60))
    panel = load_price_panel(min_days=max(40, window + 1))
    vol_history = load_vol_shape_history(VOL_SHAPE_HISTORY) if VOL_SHAPE_HISTORY.is_file() else {}
    borrow_history = b4.load_borrow_history()
    current_policy = _variant_policy(policy, None, None)
    h075_policy = _variant_policy(policy, 0.75, None)

    pairs: dict[str, Any] = {}
    relative_full: list[float] = []
    relative_pre: list[float] = []
    wins_full = wins_pre = eligible_pre = 0
    for etf in sorted(cleaned):
        prices = panel.get(etf)
        if prices is None or len(prices) <= warmup:
            continue
        prices = prices.dropna(subset=["a_px", "b_px"]).copy()
        if len(prices) <= warmup:
            continue
        first_trade = pd.Timestamp(prices.index[warmup])
        signal_floor = pd.Timestamp(prices.index[0])
        row = _row_for_etf(etf)
        # Gross-only: zero every borrow input.  This is intentional, documented,
        # and prevents using a current or future fee before it was observed.
        no_borrow: dict[str, pd.Series] = {}
        gross_row = row.copy()
        gross_row["borrow_current"] = 0.0
        base = _run_pair(gross_row, prices, current_policy, trade_start=str(first_trade.date()), signal_floor=str(signal_floor.date()), warmup_bdays=warmup, signal_window=window, vol_history=vol_history, borrow_history=no_borrow, underlying_borrow_annual=0.0)
        alt = _run_pair(gross_row, prices, h075_policy, trade_start=str(first_trade.date()), signal_floor=str(signal_floor.date()), warmup_bdays=warmup, signal_window=window, vol_history=vol_history, borrow_history=no_borrow, underlying_borrow_annual=0.0)
        if base.empty or alt.empty:
            continue
        first_borrow = _first_valid_borrow_date(borrow_history, etf)
        comparison = _compare(base, alt, first_borrow=first_borrow, seed=int(hashlib.sha256(etf.encode()).hexdigest()[:8], 16))
        whole_rel = comparison["relative_return"]
        pre_rel = comparison["pre_borrow"]["relative_return"]
        if whole_rel is not None:
            relative_full.append(whole_rel)
            wins_full += whole_rel > 0
        if comparison["pre_borrow"]["common_days"] >= 60 and pre_rel is not None:
            eligible_pre += 1
            relative_pre.append(pre_rel)
            wins_pre += pre_rel > 0
        pairs[etf] = {
            "underlying": str(row.get("Underlying") or ""),
            "full_history_start": base.index.min().strftime("%Y-%m-%d"),
            "full_history_end": base.index.max().strftime("%Y-%m-%d"),
            "first_etf_borrow_observation": first_borrow.strftime("%Y-%m-%d") if first_borrow is not None else None,
            "current": _metrics_for_period(base, None, None),
            "h075": _metrics_for_period(alt, None, None),
            "current_pre_borrow": _metrics_for_period(base, first_borrow, None),
            "h075_pre_borrow": _metrics_for_period(alt, first_borrow, None),
            "current_post_borrow": _metrics_for_period(base, None, first_borrow),
            "h075_post_borrow": _metrics_for_period(alt, None, first_borrow),
            "comparison": comparison,
        }

    result = {
        "schema": "bucket4_h075_full_history_backcast.v1",
        "authoritative": False,
        "disclaimer": "Research-only price-history gross backcast. It charges no borrow and is not a production replay or a net-performance result.",
        "methodology": {
            "history_start": "first aligned ETF/underlying price window plus 60-session signal warmup",
            "signals": "60-session VCR/TR, expanding point-in-time VCR median, one-session lookahead shift",
            "comparison": "current dynamic policy versus fixed h=0.75 with the current cadence",
            "pre_borrow_split": "first actual ETF point-in-time borrow observation; earlier interval is a chronologically earlier backcast, not true out-of-sample",
            "borrow": "zeroed for both legs across the full-history gross lens; observed-borrow net results remain separate",
        },
        "policy_sha256": hashlib.sha256(args.policy.read_bytes()).hexdigest(),
        "summary": {
            "eligible_pairs": len(pairs),
            "full_history_h075_wins": wins_full,
            "full_history_median_relative_return": _median(relative_full),
            "pre_borrow_pairs_with_60_days": eligible_pre,
            "pre_borrow_h075_wins": wins_pre,
            "pre_borrow_median_relative_return": _median(relative_pre),
        },
        "pairs": pairs,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({"ok": True, "pairs": len(pairs), "output": str(args.output.resolve()), "summary": result["summary"]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

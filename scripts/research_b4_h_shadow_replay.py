#!/usr/bin/env python3
"""Retrospective shadow replay for the frozen B4 h deadband+slew candidate.

This is the next-stage implementation diagnostic, not a substitute for future
live shadow observations.  It uses only the observed-borrow research window,
keeps the candidate frozen, and decomposes return differences into allocation
effects versus modeled transaction-cost effects.
"""
from __future__ import annotations

import argparse
import copy
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
from bucket4.bucket4_price_loading import load_price_panel  # noqa: E402
from bucket4.bucket4_vol_shape_signals import load_vol_shape_history  # noqa: E402
from bucket4.policy_helpers import load_policy  # noqa: E402
from build_b4_inception_research import _row_for_etf  # noqa: E402
from research_b4_h_stability import VARIANTS, _run_pair, _stability  # noqa: E402
from research_b4_phase1_factorial import DEFAULT_INPUT, DEFAULT_POLICY, VOL_SHAPE_HISTORY, _load_cleaned_inputs, _perf  # noqa: E402

DEFAULT_OUTPUT = REPO / "data" / "_phase1_experiments" / "h_stability_shadow_replay.json"
CANDIDATE = "deadband_005_slew_0025"
UNDERLYING_BORROW_STRESS = 0.05


def _costless_policy(policy: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(policy)
    opt2 = out["inverse_decay_bucket4"]["rules"]["bucket4_weekly_opt2"]
    opt2["fee_bps"] = 0.0
    opt2["slippage_bps"] = 0.0
    return out


def _relative_log(alt: pd.Series, base: pd.Series) -> float:
    joined = pd.concat([alt, base], axis=1, join="inner").dropna()
    if joined.empty:
        return 0.0
    return float((np.log1p(joined.iloc[:, 0]) - np.log1p(joined.iloc[:, 1])).sum())


def _median(values: list[float | None]) -> float | None:
    vals = [float(x) for x in values if x is not None and math.isfinite(float(x))]
    return float(np.median(vals)) if vals else None


def event_diagnostics(bt: pd.DataFrame, all_in_cost_rate: float) -> dict[str, Any]:
    events = bt.loc[bt["rebalance"].astype(bool)].copy()
    h = pd.to_numeric(events["h_used"], errors="coerce")
    dh = h.diff()
    changed = dh.loc[dh.abs() > 1e-9]
    quick: list[dict[str, Any]] = []
    for i in range(1, len(changed)):
        d0, d1 = pd.Timestamp(changed.index[i - 1]), pd.Timestamp(changed.index[i])
        if np.sign(changed.iloc[i]) == np.sign(changed.iloc[i - 1]) or (d1 - d0).days > 21:
            continue
        eq0 = float(bt.loc[d0, "equity"])
        eq1 = float(bt.loc[d1, "equity"])
        quick.append({
            "start": d0.strftime("%Y-%m-%d"), "end": d1.strftime("%Y-%m-%d"),
            "days": int((d1 - d0).days),
            "first_delta_h": float(changed.iloc[i - 1]), "reversal_delta_h": float(changed.iloc[i]),
            "strategy_return_between": float(eq1 / eq0 - 1.0) if abs(eq0) > 1e-12 else None,
        })
    total_fee = float(pd.to_numeric(bt["rebalance_fee"], errors="coerce").fillna(0).sum())
    return {
        "sessions": int(len(bt)),
        "executed_rebalances": int(len(events)),
        "h_change_count": int(len(changed)),
        "h_abs_turnover": float(changed.abs().sum()),
        "h_quick_reversal_count_21d": len(quick),
        "modeled_transaction_cost": total_fee,
        "inferred_traded_notional": float(total_fee / all_in_cost_rate) if all_in_cost_rate > 0 else None,
        "quick_reversals": quick,
    }


def _outer_equal_weight(series: dict[str, pd.Series]) -> pd.Series:
    if not series:
        return pd.Series(dtype=float)
    matrix = pd.concat(series, axis=1, join="outer", sort=False).sort_index()
    return matrix.mean(axis=1, skipna=True).dropna()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    ap.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT)
    ap.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = ap.parse_args(argv)
    inputs = _load_cleaned_inputs(args.input_dir)
    policy = load_policy(args.policy)
    zero_cost_policy = _costless_policy(policy)
    opt2 = policy["inverse_decay_bucket4"]["rules"]["bucket4_weekly_opt2"]
    all_in_rate = (float(opt2.get("fee_bps", 0)) + float(opt2.get("slippage_bps", 0))) / 10_000.0
    panel = load_price_panel(min_days=40)
    vol_history = load_vol_shape_history(VOL_SHAPE_HISTORY) if VOL_SHAPE_HISTORY.is_file() else {}
    borrow_history = b4.load_borrow_history()
    pair_rows: list[dict[str, Any]] = []
    current_returns: dict[str, pd.Series] = {}
    candidate_returns: dict[str, pd.Series] = {}
    rolling_20_log_deltas: list[float] = []
    pair_month_log_deltas: list[float] = []
    for etf, meta in sorted(inputs.items()):
        prices = panel.get(etf)
        if prices is None or prices.empty:
            continue
        row = _row_for_etf(etf)
        common = {
            "trade_start": pd.Timestamp(meta["trade_start"]),
            "signal_floor": pd.Timestamp(meta["signal_floor"]),
            "window": int(meta["signal_window"]), "warmup": int(meta["warmup_bdays"]),
            "vol_history": vol_history, "borrow_history": borrow_history,
            "underlying_borrow_annual": UNDERLYING_BORROW_STRESS,
        }
        current = _run_pair(row, prices, policy, stabilizer=VARIANTS["current"], **common)
        candidate = _run_pair(row, prices, policy, stabilizer=VARIANTS[CANDIDATE], **common)
        current_gross = _run_pair(row, prices, zero_cost_policy, stabilizer=VARIANTS["current"], **common)
        candidate_gross = _run_pair(row, prices, zero_cost_policy, stabilizer=VARIANTS[CANDIDATE], **common)
        current_returns[etf] = current["ret"].rename(etf)
        candidate_returns[etf] = candidate["ret"].rename(etf)
        net_log = _relative_log(candidate["ret"], current["ret"])
        allocation_log = _relative_log(candidate_gross["ret"], current_gross["ret"])
        cost_log = net_log - allocation_log
        cur_evt, alt_evt = event_diagnostics(current, all_in_rate), event_diagnostics(candidate, all_in_rate)
        joined = pd.concat([candidate["ret"], current["ret"]], axis=1, join="inner").dropna()
        daily_delta = np.log1p(joined.iloc[:, 0]) - np.log1p(joined.iloc[:, 1])
        rolling_20_log_deltas.extend(daily_delta.rolling(20).sum().dropna().tolist())
        pair_month_log_deltas.extend(daily_delta.groupby(daily_delta.index.to_period("M")).sum().tolist())
        cur_perf, alt_perf = _perf(current["ret"]), _perf(candidate["ret"])
        pair_rows.append({
            "etf": etf, "underlying": str(row.get("Underlying") or ""),
            "start": current.index.min().strftime("%Y-%m-%d"), "end": current.index.max().strftime("%Y-%m-%d"),
            "sessions": int(len(current)),
            "net_relative_return": float(np.expm1(net_log)),
            "allocation_relative_return_no_cost": float(np.expm1(allocation_log)),
            "transaction_cost_contribution_log": cost_log,
            "max_drawdown_change": float(alt_perf["max_drawdown"] - cur_perf["max_drawdown"]),
            "expected_shortfall_change": float(alt_perf["expected_shortfall_95_daily"] - cur_perf["expected_shortfall_95_daily"]),
            "current": cur_evt, "candidate": alt_evt,
            "traded_notional_change": float((alt_evt["inferred_traded_notional"] or 0) - (cur_evt["inferred_traded_notional"] or 0)),
            "transaction_cost_change": float(alt_evt["modeled_transaction_cost"] - cur_evt["modeled_transaction_cost"]),
            "latest_current_h": float(current["h_used"].iloc[-1]),
            "latest_candidate_h": float(candidate["h_used"].iloc[-1]),
        })
    current_port = _outer_equal_weight(current_returns)
    candidate_port = _outer_equal_weight(candidate_returns)
    portfolio_log = _relative_log(candidate_port, current_port)
    ordered_rel = sorted(r["net_relative_return"] for r in pair_rows)
    trim_n = max(1, int(len(ordered_rel) * 0.10)) if len(ordered_rel) >= 10 else 0
    trimmed_rel = ordered_rel[trim_n:-trim_n] if trim_n else ordered_rel
    largest_positive = max(pair_rows, key=lambda r: r["net_relative_return"])
    largest_negative = min(pair_rows, key=lambda r: r["net_relative_return"])
    total_current_notional = sum(float(r["current"]["inferred_traded_notional"] or 0) for r in pair_rows)
    total_candidate_notional = sum(float(r["candidate"]["inferred_traded_notional"] or 0) for r in pair_rows)
    total_current_cost = sum(float(r["current"]["modeled_transaction_cost"]) for r in pair_rows)
    total_candidate_cost = sum(float(r["candidate"]["modeled_transaction_cost"]) for r in pair_rows)
    summary = {
        "pairs": len(pair_rows),
        "session_range": [min(r["sessions"] for r in pair_rows), max(r["sessions"] for r in pair_rows)],
        "pair_wins": sum(r["net_relative_return"] > 0 for r in pair_rows),
        "median_pair_net_relative_return": _median([r["net_relative_return"] for r in pair_rows]),
        "trim10_mean_pair_net_relative_return": float(np.mean(trimmed_rel)) if trimmed_rel else None,
        "largest_positive_driver": {"etf": largest_positive["etf"], "relative_return": largest_positive["net_relative_return"]},
        "largest_negative_driver": {"etf": largest_negative["etf"], "relative_return": largest_negative["net_relative_return"]},
        "mean_pair_relative_return_excluding_largest_positive": float(np.mean([r["net_relative_return"] for r in pair_rows if r is not largest_positive])),
        "median_pair_allocation_relative_return_no_cost": _median([r["allocation_relative_return_no_cost"] for r in pair_rows]),
        "median_transaction_cost_contribution_log": _median([r["transaction_cost_contribution_log"] for r in pair_rows]),
        "median_traded_notional_change": _median([r["traded_notional_change"] for r in pair_rows]),
        "median_transaction_cost_change": _median([r["transaction_cost_change"] for r in pair_rows]),
        "aggregate_inferred_traded_notional_current": total_current_notional,
        "aggregate_inferred_traded_notional_candidate": total_candidate_notional,
        "aggregate_inferred_traded_notional_reduction": float(1.0 - total_candidate_notional / total_current_notional) if total_current_notional > 0 else None,
        "aggregate_modeled_transaction_cost_current": total_current_cost,
        "aggregate_modeled_transaction_cost_candidate": total_candidate_cost,
        "aggregate_modeled_transaction_cost_reduction": float(1.0 - total_candidate_cost / total_current_cost) if total_current_cost > 0 else None,
        "current_total_quick_reversals_21d": sum(r["current"]["h_quick_reversal_count_21d"] for r in pair_rows),
        "candidate_total_quick_reversals_21d": sum(r["candidate"]["h_quick_reversal_count_21d"] for r in pair_rows),
        "current_total_h_changes": sum(r["current"]["h_change_count"] for r in pair_rows),
        "candidate_total_h_changes": sum(r["candidate"]["h_change_count"] for r in pair_rows),
        "portfolio_active_pair_equal_weight_relative_return": float(np.expm1(portfolio_log)),
        "portfolio_current": _perf(current_port), "portfolio_candidate": _perf(candidate_port),
        "positive_overlapping_20_session_windows_share": float(np.mean(np.asarray(rolling_20_log_deltas) > 0)) if rolling_20_log_deltas else None,
        "positive_pair_months_share": float(np.mean(np.asarray(pair_month_log_deltas) > 0)) if pair_month_log_deltas else None,
    }
    payload = {
        "schema": "bucket4_h_stability_shadow_replay.v1", "authoritative": False,
        "disclaimer": "Retrospective point-in-time shadow replay, not completed forward shadow evidence or production PnL.",
        "candidate": {"name": CANDIDATE, **VARIANTS[CANDIDATE]},
        "methodology": {
            "window": "each pair's cleaned observed-borrow research start through latest common price date",
            "etf_borrow": "actual point-in-time series", "underlying_borrow_annual_stress": UNDERLYING_BORROW_STRESS,
            "cost_model": {"commission_bps": float(opt2.get("fee_bps", 0)), "slippage_bps": float(opt2.get("slippage_bps", 0)), "all_in_rate": all_in_rate},
            "decomposition": "rerun both policies with modeled transaction costs set to zero; remaining relative return is allocation/timing, net minus allocation is the cost contribution",
            "portfolio": "equal weight across all active pairs each session; current high-edge universe remains ex-post and is not point-in-time selected",
        },
        "policy_sha256": hashlib.sha256(args.policy.read_bytes()).hexdigest(),
        "summary": summary, "pairs": pair_rows,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({"ok": True, "pairs": len(pair_rows), "output": str(args.output.resolve())}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

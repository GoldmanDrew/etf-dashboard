#!/usr/bin/env python3
"""Research-only B4 hedge-target stability and anti-whipsaw experiment.

The test keeps the current signal model and cadence intact.  Stabilizers act
only on the h target presented at scheduled resize dates.  Full-history and
pre-borrow results are gross-only so current borrow is never backfilled into
the past.
"""
from __future__ import annotations

import argparse
import hashlib
import json
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
from bucket4.bucket4_h_stability import VARIANTS, stabilize_h_targets  # noqa: E402
from bucket4.bucket4_price_loading import load_price_panel  # noqa: E402
from bucket4.bucket4_vol_shape_signals import get_pair_signal, load_vol_shape_history  # noqa: E402
from bucket4.policy_helpers import knobs_from_policy, load_policy, make_knobs  # noqa: E402
from build_b4_inception_research import _first_valid_borrow_date, _row_for_etf  # noqa: E402
from research_b4_phase1_factorial import DEFAULT_INPUT, DEFAULT_POLICY, VOL_SHAPE_HISTORY, _load_cleaned_inputs, _perf  # noqa: E402

DEFAULT_OUTPUT = REPO / "data" / "_phase1_experiments" / "h_stability_full_history.json"

__all__ = ["VARIANTS", "stabilize_h_targets"]


def _run_pair(
    row: pd.Series,
    prices: pd.DataFrame,
    policy: dict[str, Any],
    *,
    trade_start: pd.Timestamp,
    signal_floor: pd.Timestamp,
    window: int,
    warmup: int,
    vol_history: dict[str, pd.DataFrame],
    stabilizer: dict[str, float | str],
    borrow_history: dict[str, pd.Series] | None = None,
    underlying_borrow_annual: float = 0.0,
) -> pd.DataFrame:
    cal = pd.DatetimeIndex(prices.index[prices.index >= trade_start])
    signal_cal = pd.DatetimeIndex(prices.index[prices.index >= signal_floor])
    signal = get_pair_signal(
        str(row["ETF"]), str(row["Underlying"]), signal_cal,
        history=vol_history, underlying_prices=prices["b_px"],
        window=window, lookahead_shift=1,
    )
    knobs = make_knobs(knobs_from_policy(policy))
    base_h = build_h_series(signal, signal_cal, knobs=knobs)
    dates, _ = build_rebal_dates(signal, signal_cal, knobs=knobs, warmup_bdays=warmup)
    stable_h = stabilize_h_targets(base_h, dates, stabilizer)
    engine = b4._pair_engine_kwargs(policy, row, cal, borrow_history or {})
    engine["borrow_b_annual"] = float(underlying_borrow_annual)
    return run_bucket4_backtest_dynamic_h(
        prices.reindex(cal), stable_h, dates,
        initial_capital=1.0, gross_multiplier=1.0,
        beta_a=-abs(float(row.get("Delta") or -2.0)), beta_b=1.0,
        membership_start=str(trade_start.date()), capital_mode="unit_equity",
        stop_on_equity_wipeout=True, **engine,
    )


def _relative(alt: pd.Series, base: pd.Series) -> float | None:
    joined = pd.concat([alt, base], axis=1, join="inner").dropna()
    if joined.empty:
        return None
    return float(np.expm1((np.log1p(joined.iloc[:, 0]) - np.log1p(joined.iloc[:, 1])).sum()))


def _stability(bt: pd.DataFrame) -> dict[str, float | int | None]:
    executed = bt.loc[bt["rebalance"].astype(bool)].copy()
    h = pd.to_numeric(executed["h_used"], errors="coerce").dropna()
    dh = h.diff().dropna()
    changes = dh.loc[dh.abs() > 1e-9]
    signs = np.sign(changes)
    reversals = int((signs * signs.shift(1) < 0).sum()) if len(signs) > 1 else 0
    quick = 0
    if len(changes) > 1:
        for i in range(1, len(changes)):
            if np.sign(changes.iloc[i]) != np.sign(changes.iloc[i - 1]):
                if (changes.index[i] - changes.index[i - 1]).days <= 21:
                    quick += 1
    fee = float(pd.to_numeric(bt["rebalance_fee"], errors="coerce").fillna(0).sum())
    return {
        "executed_rebalances": int(len(executed)),
        "h_change_count": int(len(changes)),
        "h_abs_turnover": float(changes.abs().sum()),
        "h_median_abs_change": float(changes.abs().median()) if len(changes) else 0.0,
        "h_p95_abs_change": float(changes.abs().quantile(0.95)) if len(changes) else 0.0,
        "h_reversal_count": reversals,
        "h_reversal_rate": float(reversals / max(1, len(changes) - 1)),
        "h_quick_reversal_count_21d": quick,
        "h_quick_reversal_rate_21d": float(quick / max(1, len(changes) - 1)),
        "transaction_cost": fee,
        "mean_abs_beta_exposure_frac": float(pd.to_numeric(bt["beta_exposure_frac"], errors="coerce").abs().mean()),
    }


def _median(values: list[float | None]) -> float | None:
    vals = [float(x) for x in values if x is not None and math.isfinite(float(x))]
    return float(np.median(vals)) if vals else None


def _sign_test_two_sided(wins: int, n: int) -> float | None:
    if n <= 0:
        return None
    lo = min(wins, n - wins)
    tail = sum(math.comb(n, k) for k in range(lo + 1)) / (2**n)
    return float(min(1.0, 2.0 * tail))


def _summarize(records: list[dict[str, Any]], variant: str) -> dict[str, Any]:
    rows = [r for r in records if r["variant"] == variant]
    rel = [r["relative_full"] for r in rows if r["relative_full"] is not None]
    pre = [r for r in rows if r["pre_days"] >= 60 and r["relative_pre"] is not None]
    cluster_leave: list[float] = []
    for cluster in sorted({r["underlying"] for r in rows}):
        m = _median([r["relative_full"] for r in rows if r["underlying"] != cluster])
        if m is not None:
            cluster_leave.append(m)
    signed = [x for x in rel if abs(x) > 1e-12]
    signed_wins = sum(x > 0 for x in signed)
    result = {
        "variant": variant,
        "pairs": len(rows),
        "full_wins": sum(x > 0 for x in rel),
        "full_sign_test_two_sided_p": _sign_test_two_sided(signed_wins, len(signed)),
        "full_median_relative_return": _median(rel),
        "drawdown_improved": sum(r["drawdown_delta"] > 0 for r in rows),
        "es_improved": sum(r["es_delta"] > 0 for r in rows),
        "worst_underlying_leaveout_median_return": min(cluster_leave) if cluster_leave else None,
        "pre_borrow_pairs_60d": len(pre),
        "pre_borrow_wins": sum(r["relative_pre"] > 0 for r in pre),
        "pre_borrow_median_relative_return": _median([r["relative_pre"] for r in pre]),
        "median_h_turnover_reduction": _median([r["h_turnover_reduction"] for r in rows]),
        "median_h_change_count_reduction": _median([r["h_change_count_reduction"] for r in rows]),
        "median_quick_reversal_rate": _median([r["h_quick_reversal_rate"] for r in rows]),
        "median_transaction_cost_change": _median([r["transaction_cost_change"] for r in rows]),
        "median_abs_beta_exposure_change": _median([r["mean_abs_beta_exposure_change"] for r in rows]),
    }
    result["stability_gate"] = bool(
        variant != "current"
        and (result["median_h_turnover_reduction"] or 0) >= 0.25
        and (result["median_transaction_cost_change"] or 0) <= 0
        and (result["full_median_relative_return"] or -1) >= -0.01
        and (result["pre_borrow_median_relative_return"] or -1) >= -0.015
        and (result["worst_underlying_leaveout_median_return"] or -1) >= -0.015
        and result["drawdown_improved"] >= 12
        and result["es_improved"] >= 12
    )
    return result


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    ap.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT)
    ap.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = ap.parse_args(argv)
    inputs = _load_cleaned_inputs(args.input_dir)
    if not inputs:
        raise SystemExit("No cleaned Phase-1 inputs")
    policy = load_policy(args.policy)
    cfg = policy.get("backtest") or {}
    window, warmup = int(cfg.get("signal_window", 60)), int(cfg.get("warmup_bdays", 60))
    panel = load_price_panel(min_days=max(40, window + 1))
    vol_history = load_vol_shape_history(VOL_SHAPE_HISTORY) if VOL_SHAPE_HISTORY.is_file() else {}
    borrow_history = b4.load_borrow_history()
    records: list[dict[str, Any]] = []
    for etf in sorted(inputs):
        prices = panel.get(etf)
        if prices is None:
            continue
        prices = prices.dropna(subset=["a_px", "b_px"])
        if len(prices) <= warmup:
            continue
        trade_start, signal_floor = pd.Timestamp(prices.index[warmup]), pd.Timestamp(prices.index[0])
        first_borrow = _first_valid_borrow_date(borrow_history, etf)
        row = _row_for_etf(etf).copy()
        row["borrow_current"] = 0.0
        runs = {
            name: _run_pair(row, prices, policy, trade_start=trade_start, signal_floor=signal_floor,
                            window=window, warmup=warmup, vol_history=vol_history, stabilizer=spec)
            for name, spec in VARIANTS.items()
        }
        control = runs["current"]
        control_perf, control_stab = _perf(control["ret"]), _stability(control)
        for name, bt in runs.items():
            perf, stab = _perf(bt["ret"]), _stability(bt)
            pre_bt = bt.loc[bt.index < first_borrow] if first_borrow is not None else bt.iloc[0:0]
            pre_control = control.loc[control.index < first_borrow] if first_borrow is not None else control.iloc[0:0]
            base_turnover = float(control_stab["h_abs_turnover"] or 0)
            base_changes = int(control_stab["h_change_count"] or 0)
            records.append({
                "etf": etf, "underlying": str(row.get("Underlying") or ""), "variant": name,
                "history_start": control.index.min().strftime("%Y-%m-%d"), "history_end": control.index.max().strftime("%Y-%m-%d"),
                "first_etf_borrow_observation": first_borrow.strftime("%Y-%m-%d") if first_borrow is not None else None,
                "relative_full": _relative(bt["ret"], control["ret"]),
                "relative_pre": _relative(pre_bt["ret"], pre_control["ret"]),
                "pre_days": int(len(pd.concat([pre_bt["ret"], pre_control["ret"]], axis=1, join="inner").dropna())),
                "drawdown_delta": float(perf["max_drawdown"] - control_perf["max_drawdown"]),
                "es_delta": float(perf["expected_shortfall_95_daily"] - control_perf["expected_shortfall_95_daily"]),
                "h_turnover_reduction": float(1.0 - float(stab["h_abs_turnover"] or 0) / base_turnover) if base_turnover > 0 else 0.0,
                "h_change_count_reduction": float(1.0 - int(stab["h_change_count"] or 0) / base_changes) if base_changes > 0 else 0.0,
                "h_quick_reversal_rate": stab["h_quick_reversal_rate_21d"],
                "transaction_cost_change": float(stab["transaction_cost"] - control_stab["transaction_cost"]),
                "mean_abs_beta_exposure_change": float(stab["mean_abs_beta_exposure_frac"] - control_stab["mean_abs_beta_exposure_frac"]),
                "stability": stab,
            })
    payload = {
        "schema": "bucket4_h_stability_full_history.v1", "authoritative": False,
        "disclaimer": "Research-only hedge-target stability experiment; gross full-history/backcast results are not production or net performance.",
        "methodology": {
            "variants_locked_before_results": VARIANTS,
            "scope": "stabilizers act only on current-policy h at scheduled resize dates; signals, cadence, gross, and costs unchanged",
            "history": "full aligned price history after 60-session warmup, plus pre-first-borrow chronological backcast",
            "anti_overfit_gates": {"median_h_turnover_reduction_min": 0.25, "median_return_floor": -0.01, "pre_borrow_return_floor": -0.015, "leaveout_return_floor": -0.015, "drawdown_and_es_pairs_min": 12, "transaction_cost_nonincrease": True},
            "borrow": "zeroed on both legs",
        },
        "policy_sha256": hashlib.sha256(args.policy.read_bytes()).hexdigest(),
        "summary": [_summarize(records, name) for name in VARIANTS],
        "pairs": records,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({"ok": True, "pairs": len({r['etf'] for r in records}), "variants": len(VARIANTS), "output": str(args.output.resolve())}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

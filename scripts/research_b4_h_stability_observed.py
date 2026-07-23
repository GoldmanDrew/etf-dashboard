#!/usr/bin/env python3
"""Observed-borrow validation for full-history-qualified h stabilizers only."""
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
from bucket4.bucket4_price_loading import load_price_panel  # noqa: E402
from bucket4.bucket4_vol_shape_signals import load_vol_shape_history  # noqa: E402
from bucket4.policy_helpers import load_policy  # noqa: E402
from build_b4_inception_research import _row_for_etf  # noqa: E402
from research_b4_h_stability import VARIANTS, _run_pair, _stability  # noqa: E402
from research_b4_phase1_factorial import DEFAULT_INPUT, DEFAULT_POLICY, VOL_SHAPE_HISTORY, _load_cleaned_inputs, _perf  # noqa: E402

DEFAULT_FULL = REPO / "data" / "_phase1_experiments" / "h_stability_full_history.json"
DEFAULT_OUTPUT = REPO / "data" / "_phase1_experiments" / "h_stability_observed_borrow.json"
STRESSES = (0.0, 0.05, 0.15)


def _relative(alt: pd.Series, base: pd.Series) -> float | None:
    joined = pd.concat([alt, base], axis=1, join="inner").dropna()
    if joined.empty:
        return None
    return float(np.expm1((np.log1p(joined.iloc[:, 0]) - np.log1p(joined.iloc[:, 1])).sum()))


def _median(values: list[float | None]) -> float | None:
    vals = [float(x) for x in values if x is not None and math.isfinite(float(x))]
    return float(np.median(vals)) if vals else None


def _summarize(records: list[dict[str, Any]], stress: float, variant: str) -> dict[str, Any]:
    rows = [r for r in records if r["underlying_borrow_annual"] == stress and r["variant"] == variant]
    rel = [r["relative_return"] for r in rows if r["relative_return"] is not None]
    leaveouts: list[float] = []
    for und in sorted({r["underlying"] for r in rows}):
        m = _median([r["relative_return"] for r in rows if r["underlying"] != und])
        if m is not None:
            leaveouts.append(m)
    out = {
        "underlying_borrow_annual": stress,
        "variant": variant,
        "pairs": len(rows),
        "wins": sum(x > 0 for x in rel),
        "median_relative_return": _median(rel),
        "drawdown_improved": sum(r["drawdown_delta"] > 0 for r in rows),
        "es_improved": sum(r["es_delta"] > 0 for r in rows),
        "worst_underlying_leaveout_median_return": min(leaveouts) if leaveouts else None,
        "median_h_turnover_reduction": _median([r["h_turnover_reduction"] for r in rows]),
        "median_quick_reversal_rate": _median([r["quick_reversal_rate"] for r in rows]),
        "median_transaction_cost_change": _median([r["transaction_cost_change"] for r in rows]),
    }
    out["observed_gate"] = bool(
        variant != "current"
        and out["wins"] >= 12
        and (out["median_relative_return"] or -1) >= -0.01
        and (out["worst_underlying_leaveout_median_return"] or -1) >= -0.015
        and (out["median_h_turnover_reduction"] or 0) >= 0.25
        and (out["median_transaction_cost_change"] or 0) <= 0
        and out["drawdown_improved"] >= 12
        and out["es_improved"] >= 12
    )
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--full-results", type=Path, default=DEFAULT_FULL)
    ap.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT)
    ap.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    ap.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = ap.parse_args(argv)
    full = json.loads(args.full_results.read_text(encoding="utf-8"))
    survivors = [r["variant"] for r in full["summary"] if r.get("stability_gate")]
    variants = ["current", *survivors]
    if len(variants) == 1:
        raise SystemExit("No full-history-qualified stabilizer")
    inputs = _load_cleaned_inputs(args.input_dir)
    policy = load_policy(args.policy)
    panel = load_price_panel(min_days=40)
    vol_history = load_vol_shape_history(VOL_SHAPE_HISTORY) if VOL_SHAPE_HISTORY.is_file() else {}
    borrow_history = b4.load_borrow_history()
    records: list[dict[str, Any]] = []
    for etf, meta in sorted(inputs.items()):
        prices = panel.get(etf)
        if prices is None or prices.empty:
            continue
        row = _row_for_etf(etf)
        for stress in STRESSES:
            runs = {
                name: _run_pair(
                    row, prices, policy,
                    trade_start=pd.Timestamp(meta["trade_start"]),
                    signal_floor=pd.Timestamp(meta["signal_floor"]),
                    window=int(meta["signal_window"]), warmup=int(meta["warmup_bdays"]),
                    vol_history=vol_history, stabilizer=VARIANTS[name],
                    borrow_history=borrow_history, underlying_borrow_annual=stress,
                )
                for name in variants
            }
            control = runs["current"]
            control_perf, control_stab = _perf(control["ret"]), _stability(control)
            base_turnover = float(control_stab["h_abs_turnover"] or 0)
            for name, bt in runs.items():
                perf, stab = _perf(bt["ret"]), _stability(bt)
                records.append({
                    "etf": etf, "underlying": str(row.get("Underlying") or ""),
                    "underlying_borrow_annual": stress, "variant": name,
                    "relative_return": _relative(bt["ret"], control["ret"]),
                    "drawdown_delta": float(perf["max_drawdown"] - control_perf["max_drawdown"]),
                    "es_delta": float(perf["expected_shortfall_95_daily"] - control_perf["expected_shortfall_95_daily"]),
                    "h_turnover_reduction": float(1.0 - float(stab["h_abs_turnover"] or 0) / base_turnover) if base_turnover > 0 else 0.0,
                    "quick_reversal_rate": stab["h_quick_reversal_rate_21d"],
                    "transaction_cost_change": float(stab["transaction_cost"] - control_stab["transaction_cost"]),
                })
    payload = {
        "schema": "bucket4_h_stability_observed_borrow.v1", "authoritative": False,
        "disclaimer": "Research-only observed ETF-borrow validation; underlying borrow is scenario stress, not observed history.",
        "selection": {"source": str(args.full_results.resolve()), "rule": "only stabilizers passing the pre-declared full-history gate", "survivors": survivors},
        "methodology": {"etf_borrow": "point-in-time observed series from each pair's first observation", "underlying_borrow_stresses": list(STRESSES), "signals_and_cadence": "unchanged current policy"},
        "policy_sha256": hashlib.sha256(args.policy.read_bytes()).hexdigest(),
        "summary": [_summarize(records, s, v) for s in STRESSES for v in variants],
        "pairs": records,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({"ok": True, "survivors": survivors, "pairs": len({r['etf'] for r in records}), "output": str(args.output.resolve())}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

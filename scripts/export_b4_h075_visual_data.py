#!/usr/bin/env python3
"""Export research-only current-vs-fixed-h=.75 daily paths for visualization."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
from pathlib import Path

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
from research_b4_phase1_factorial import (  # noqa: E402
    DEFAULT_INPUT,
    DEFAULT_POLICY,
    VOL_SHAPE_HISTORY,
    _load_cleaned_inputs,
    _perf,
    _run_pair,
    _variant_policy,
)

DEFAULT_OUTPUT = REPO / "data" / "_phase1_experiments" / "h075_visual_data.json"
BORROW_STRESSES = (0.00, 0.05, 0.15)


def _compact(values: pd.Series, digits: int = 6) -> list[float | None]:
    out: list[float | None] = []
    for value in pd.to_numeric(values, errors="coerce"):
        out.append(round(float(value), digits) if np.isfinite(value) else None)
    return out


def _path_payload(bt: pd.DataFrame) -> dict:
    perf = _perf(bt["ret"])
    equity = pd.to_numeric(bt["equity"], errors="coerce")
    base = float(equity.iloc[0]) if len(equity) and np.isfinite(equity.iloc[0]) and equity.iloc[0] != 0 else 1.0
    normalized = equity / base
    drawdown = normalized.div(normalized.cummax()).sub(1.0)
    return {
        "dates": [pd.Timestamp(d).strftime("%Y-%m-%d") for d in bt.index],
        "equity": _compact(normalized),
        "drawdown": _compact(drawdown),
        "h": _compact(bt["h_used"], 4),
        "rebalance": [bool(x) for x in bt["rebalance"]],
        "metrics": {
            **perf,
            "mean_h": float(pd.to_numeric(bt["h_used"], errors="coerce").mean()),
            "rebalances": int(pd.to_numeric(bt["rebalance"], errors="coerce").fillna(0).sum()),
            "total_borrow": float(pd.to_numeric(bt["borrow_cost"], errors="coerce").fillna(0).sum()),
            "total_fees": float(pd.to_numeric(bt["rebalance_fee"], errors="coerce").fillna(0).sum()),
        },
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    ap.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT)
    ap.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = ap.parse_args(argv)

    inputs = _load_cleaned_inputs(args.input_dir)
    if not inputs:
        raise SystemExit(f"No Phase-1 inputs in {args.input_dir}")
    base_policy = load_policy(args.policy)
    policies = {
        "current": _variant_policy(base_policy, None, None),
        "h075": _variant_policy(base_policy, 0.75, None),
    }
    panel = load_price_panel(min_days=40)
    vol_history = load_vol_shape_history(VOL_SHAPE_HISTORY) if VOL_SHAPE_HISTORY.is_file() else {}
    borrow_history = b4.load_borrow_history()
    rows = {etf: _row_for_etf(etf) for etf in inputs}
    output: dict[str, dict] = {}
    for etf, meta in inputs.items():
        px = panel.get(etf)
        if px is None or px.empty:
            continue
        und = str(rows[etf].get("Underlying") or "").upper()
        pair = {"etf": etf, "underlying": und, "scenarios": {}}
        for borrow in BORROW_STRESSES:
            scenario: dict[str, dict] = {}
            for label, policy in policies.items():
                bt = _run_pair(
                    rows[etf], px, policy,
                    trade_start=meta["trade_start"],
                    signal_floor=meta["signal_floor"],
                    warmup_bdays=meta["warmup_bdays"],
                    signal_window=meta["signal_window"],
                    vol_history=vol_history,
                    borrow_history=borrow_history,
                    underlying_borrow_annual=borrow,
                )
                scenario[label] = _path_payload(bt)
            pair["scenarios"][f"{borrow:.2f}"] = scenario
        output[etf] = pair

    payload = {
        "schema": "bucket4_h075_visual.v1",
        "authoritative": False,
        "disclaimer": "Research-only current-policy versus fixed h=0.75 comparison; not a production trade plan.",
        "policy_sha256": hashlib.sha256(args.policy.read_bytes()).hexdigest(),
        "borrow_stresses": list(BORROW_STRESSES),
        "pairs": output,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, separators=(",", ":"), allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({"ok": True, "pairs": len(output), "output": str(args.output.resolve())}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

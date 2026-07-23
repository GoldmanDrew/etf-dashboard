#!/usr/bin/env python3
"""Isolated Phase-1 B4 hedge/cadence factorial on cleaned research paths.

This script is deliberately outside every production build and import path.  It
does not write pair shards, book state, or ls-algo inputs.  Results go only to a
caller-supplied research directory.
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
from bucket4.bucket4_dynamic_bt import run_bucket4_backtest_dynamic_h  # noqa: E402
from bucket4.bucket4_hedge_cadence import build_h_series, build_rebal_dates  # noqa: E402
from bucket4.bucket4_price_loading import load_price_panel  # noqa: E402
from bucket4.bucket4_vol_shape_signals import get_pair_signal, load_vol_shape_history  # noqa: E402
from bucket4.policy_helpers import knobs_from_policy, load_policy, make_knobs  # noqa: E402
from build_b4_inception_research import _row_for_etf  # noqa: E402

DEFAULT_POLICY = REPO / "config" / "bucket4_backtest_policy.yml"
DEFAULT_INPUT = REPO / "data" / "_phase1_research"
DEFAULT_OUTPUT = REPO / "data" / "_phase1_experiments"
VOL_SHAPE_HISTORY = REPO / "data" / "vol_shape_history.json"

HEDGE_LEVELS: tuple[tuple[str, float | None], ...] = (
    ("current", None),
    ("h045", 0.45),
    ("h060", 0.60),
    ("h075", 0.75),
    ("h090", 0.90),
    ("h100", 1.00),
)
CADENCE_LEVELS: tuple[tuple[str, int | None], ...] = (
    ("current", None),
    ("d05", 5),
    ("d14", 14),
)
UNDERLYING_BORROW_LEVELS = (0.00, 0.01, 0.05, 0.15)


def _finite(x: object) -> float | None:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _perf(rets: pd.Series) -> dict[str, float | int | None]:
    r = pd.to_numeric(rets, errors="coerce").dropna().astype(float)
    if len(r) < 2:
        return {"n_days": int(len(r)), "cumulative_return": None, "cagr": None,
                "annual_vol": None, "sharpe": None, "max_drawdown": None,
                "expected_shortfall_95_daily": None}
    equity = (1.0 + r).cumprod()
    cumulative = float(equity.iloc[-1] - 1.0)
    cagr = -1.0 if equity.iloc[-1] <= 0 else float(equity.iloc[-1] ** (252.0 / max(1, len(r) - 1)) - 1.0)
    vol = float(r.std(ddof=1) * math.sqrt(252.0))
    sharpe = float(r.mean() * 252.0 / vol) if vol > 0 else None
    dd = equity.div(equity.cummax()).sub(1.0)
    cutoff = float(r.quantile(0.05))
    es = float(r.loc[r <= cutoff].mean())
    return {
        "n_days": int(len(r)),
        "cumulative_return": cumulative,
        "cagr": cagr,
        "annual_vol": vol,
        "sharpe": sharpe,
        "max_drawdown": float(dd.min()),
        "expected_shortfall_95_daily": es,
    }


def _median(values: list[float | None]) -> float | None:
    vals = [float(x) for x in values if x is not None and math.isfinite(float(x))]
    return float(np.median(vals)) if vals else None


def _variant_policy(base: dict[str, Any], h_fixed: float | None, cadence_fixed: int | None) -> dict[str, Any]:
    policy = copy.deepcopy(base)
    rules = policy["inverse_decay_bucket4"]["rules"]
    block = rules["bucket4_weekly_opt2"]["hedge_cadence_policy"]
    if h_fixed is not None:
        block.update({
            "h_mid": float(h_fixed),
            "k_vcr": 0.0,
            "h_min": float(h_fixed),
            "h_max": float(h_fixed),
            "alpha": 1.0,
            "k_z": 0.0,
        })
    if cadence_fixed is not None:
        block.update({
            "base_days": float(cadence_fixed),
            "k_tr": 0.0,
            "m_vcr": 0.0,
            "min_interval": int(cadence_fixed),
            "max_interval": int(cadence_fixed),
            "force_on_max_interval": True,
        })
    return policy


def _run_pair(
    row: pd.Series,
    prices: pd.DataFrame,
    policy: dict[str, Any],
    *,
    trade_start: str,
    signal_floor: str,
    warmup_bdays: int,
    signal_window: int,
    vol_history: dict[str, pd.DataFrame],
    borrow_history: dict[str, pd.Series],
    underlying_borrow_annual: float,
) -> pd.DataFrame:
    etf = str(row.get("ETF") or "").upper()
    und = str(row.get("Underlying") or "").upper()
    cal = pd.DatetimeIndex(prices.index[prices.index >= pd.Timestamp(trade_start)])
    signal_cal = pd.DatetimeIndex(prices.index[prices.index >= pd.Timestamp(signal_floor)])
    sig = get_pair_signal(
        etf,
        und,
        signal_cal,
        history=vol_history,
        underlying_prices=prices["b_px"],
        window=signal_window,
        lookahead_shift=1,
    )
    knobs = make_knobs(knobs_from_policy(policy))
    h_daily = build_h_series(sig, signal_cal, knobs=knobs)
    rebal_dates, _diag = build_rebal_dates(sig, signal_cal, knobs=knobs, warmup_bdays=warmup_bdays)
    engine = b4._pair_engine_kwargs(policy, row, cal, borrow_history)
    engine["borrow_b_annual"] = float(underlying_borrow_annual)
    bt_cfg = policy.get("backtest") or {}
    return run_bucket4_backtest_dynamic_h(
        prices.reindex(cal),
        h_daily,
        rebal_dates,
        initial_capital=float(bt_cfg.get("initial_capital", 1.0)),
        gross_multiplier=1.0,
        beta_a=-abs(float(row.get("Delta") or -2.0)),
        beta_b=1.0,
        membership_start=trade_start,
        capital_mode="unit_equity",
        stop_on_equity_wipeout=True,
        **engine,
    )


def _paired_bootstrap(
    alt: pd.Series,
    base: pd.Series,
    *,
    seed: int,
    block: int = 5,
    draws: int = 4000,
) -> dict[str, float | int | None]:
    joined = pd.concat({"alt": alt, "base": base}, axis=1, join="inner").dropna()
    joined = joined.loc[(joined["alt"] > -1.0) & (joined["base"] > -1.0)]
    n = len(joined)
    if n < max(10, block * 2):
        return {"n_days": n, "observed_relative_return": None, "probability_outperforms": None,
                "ci95_relative_return_lo": None, "ci95_relative_return_hi": None}
    delta = np.log1p(joined["alt"].to_numpy()) - np.log1p(joined["base"].to_numpy())
    observed = float(np.expm1(delta.sum()))
    rng = np.random.default_rng(seed)
    starts = np.arange(n)
    samples = np.empty(draws, dtype=float)
    blocks_needed = int(math.ceil(n / block))
    for i in range(draws):
        chosen = rng.choice(starts, size=blocks_needed, replace=True)
        ix = np.concatenate([(s + np.arange(block)) % n for s in chosen])[:n]
        samples[i] = np.expm1(delta[ix].sum())
    return {
        "n_days": n,
        "observed_relative_return": observed,
        "probability_outperforms": float(np.mean(samples > 0)),
        "ci95_relative_return_lo": float(np.quantile(samples, 0.025)),
        "ci95_relative_return_hi": float(np.quantile(samples, 0.975)),
    }


def _load_cleaned_inputs(path: Path) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for artifact in sorted(path.glob("*.json")):
        payload = json.loads(artifact.read_text(encoding="utf-8"))
        params = payload.get("research_parameters") or (payload.get("summary") or {}).get("research_parameters") or {}
        dates = (payload.get("daily") or {}).get("dates") or []
        if not dates:
            continue
        out[artifact.stem.upper()] = {
            "trade_start": str(params.get("effective_trade_start") or dates[0]),
            "signal_floor": str(params.get("start_floor") or "1900-01-01"),
            "warmup_bdays": int(params.get("warmup_bdays") or 60),
            "signal_window": int(params.get("signal_window") or 60),
        }
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    ap.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT)
    ap.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = ap.parse_args(argv)

    inputs = _load_cleaned_inputs(args.input_dir)
    if not inputs:
        raise SystemExit(f"No cleaned Phase-1 artifacts in {args.input_dir}")
    policy = load_policy(args.policy)
    panel = load_price_panel(min_days=40)
    vol_history = load_vol_shape_history(VOL_SHAPE_HISTORY) if VOL_SHAPE_HISTORY.is_file() else {}
    borrow_history = b4.load_borrow_history()
    rows = {etf: _row_for_etf(etf) for etf in inputs}

    summary_rows: list[dict[str, Any]] = []
    pair_rows: list[dict[str, Any]] = []
    portfolio_returns: dict[tuple[float, str], pd.Series] = {}
    for underlying_borrow in UNDERLYING_BORROW_LEVELS:
        for h_label, h_fixed in HEDGE_LEVELS:
            for c_label, cadence_fixed in CADENCE_LEVELS:
                variant = f"{h_label}__{c_label}"
                pol = _variant_policy(policy, h_fixed, cadence_fixed)
                pair_rets: dict[str, pd.Series] = {}
                pair_metrics: list[dict[str, Any]] = []
                for etf, meta in inputs.items():
                    px = panel.get(etf)
                    if px is None or px.empty:
                        continue
                    bt = _run_pair(
                        rows[etf],
                        px,
                        pol,
                        trade_start=meta["trade_start"],
                        signal_floor=meta["signal_floor"],
                        warmup_bdays=meta["warmup_bdays"],
                        signal_window=meta["signal_window"],
                        vol_history=vol_history,
                        borrow_history=borrow_history,
                        underlying_borrow_annual=underlying_borrow,
                    )
                    if bt is None or bt.empty:
                        continue
                    r = pd.to_numeric(bt["ret"], errors="coerce").rename(etf)
                    pair_rets[etf] = r
                    pm = _perf(r)
                    record = {
                        "underlying_borrow_annual": underlying_borrow,
                        "variant": variant,
                        "hedge": h_label,
                        "cadence": c_label,
                        "etf": etf,
                        **pm,
                        "mean_h": float(pd.to_numeric(bt["h_used"], errors="coerce").mean()),
                        "n_rebalances": int(pd.to_numeric(bt["rebalance"], errors="coerce").fillna(0).sum()),
                        "n_scheduled": int(pd.to_numeric(bt["rebalance_scheduled"], errors="coerce").fillna(0).sum()),
                        "total_borrow": float(pd.to_numeric(bt["borrow_cost"], errors="coerce").fillna(0).sum()),
                        "total_fees": float(pd.to_numeric(bt["rebalance_fee"], errors="coerce").fillna(0).sum()),
                        "wipeout": bool((pd.to_numeric(bt["equity"], errors="coerce") <= 0).any()),
                    }
                    pair_metrics.append(record)
                    pair_rows.append(record)
                matrix = pd.concat(pair_rets.values(), axis=1, join="inner").dropna(how="any")
                port = matrix.mean(axis=1).rename(variant) if not matrix.empty else pd.Series(dtype=float)
                portfolio_returns[(underlying_borrow, variant)] = port
                port_perf = _perf(port)
                summary_rows.append({
                    "underlying_borrow_annual": underlying_borrow,
                    "variant": variant,
                    "hedge": h_label,
                    "cadence": c_label,
                    "n_pairs": len(pair_metrics),
                    **port_perf,
                    "median_pair_cagr": _median([_finite(x.get("cagr")) for x in pair_metrics]),
                    "median_pair_sharpe": _median([_finite(x.get("sharpe")) for x in pair_metrics]),
                    "median_pair_max_drawdown": _median([_finite(x.get("max_drawdown")) for x in pair_metrics]),
                    "positive_pair_count": sum((_finite(x.get("cumulative_return")) or 0.0) > 0 for x in pair_metrics),
                    "wipeout_count": sum(bool(x.get("wipeout")) for x in pair_metrics),
                    "mean_h": float(np.mean([x["mean_h"] for x in pair_metrics])),
                    "total_rebalances": int(sum(x["n_rebalances"] for x in pair_metrics)),
                    "total_scheduled": int(sum(x["n_scheduled"] for x in pair_metrics)),
                    "mean_pair_borrow": float(np.mean([x["total_borrow"] for x in pair_metrics])),
                    "mean_pair_fees": float(np.mean([x["total_fees"] for x in pair_metrics])),
                })

    summary = pd.DataFrame(summary_rows)
    pair_df = pd.DataFrame(pair_rows)
    boot: list[dict[str, Any]] = []
    for row in summary_rows:
        ub = float(row["underlying_borrow_annual"])
        variant = str(row["variant"])
        base_key = (ub, "current__current")
        alt_key = (ub, variant)
        seed = int(hashlib.sha256(f"{ub}:{variant}".encode()).hexdigest()[:8], 16)
        boot.append({
            "underlying_borrow_annual": ub,
            "variant": variant,
            **_paired_bootstrap(portfolio_returns[alt_key], portfolio_returns[base_key], seed=seed),
        })
    boot_df = pd.DataFrame(boot)
    summary = summary.merge(boot_df, on=["underlying_borrow_annual", "variant"], how="left")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    summary.to_csv(args.output_dir / "factorial_summary.csv", index=False)
    pair_df.to_csv(args.output_dir / "factorial_by_pair.csv", index=False)
    payload = {
        "schema": "bucket4_phase1_factorial.v1",
        "authoritative": False,
        "disclaimer": "Research-only same-sample hedge/cadence factorial; not production policy or trade advice.",
        "policy_sha256": hashlib.sha256(args.policy.read_bytes()).hexdigest(),
        "input_dir": str(args.input_dir.resolve()),
        "n_input_pairs": len(inputs),
        "hedge_levels": [{"label": x, "fixed_h": y} for x, y in HEDGE_LEVELS],
        "cadence_levels": [{"label": x, "fixed_days": y} for x, y in CADENCE_LEVELS],
        "underlying_borrow_levels": list(UNDERLYING_BORROW_LEVELS),
        "summary": summary.where(pd.notna(summary), None).to_dict(orient="records"),
    }
    (args.output_dir / "factorial_results.json").write_text(
        json.dumps(payload, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    print(json.dumps({
        "ok": True,
        "pairs": len(inputs),
        "variants": len(HEDGE_LEVELS) * len(CADENCE_LEVELS),
        "borrow_scenarios": len(UNDERLYING_BORROW_LEVELS),
        "output_dir": str(args.output_dir.resolve()),
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

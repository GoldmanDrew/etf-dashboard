#!/usr/bin/env python3
"""Build cash-residual Optimized research paths and nest onto pair shards.

Runs the pair engine in ``sleeve_dollars`` mode with applied-gross pins from
cash-residual crash caps (``scale_to_budget=false``) + optional h-first.

Example:
  python scripts/build_b4_cash_residual_path.py --etfs CONI,HOOZ,CBRZ
  python scripts/build_b4_cash_residual_path.py --etfs CONI --budget 168000 --weight 0.15
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import build_bucket4_backtest as b4  # noqa: E402
from bucket4.bucket4_h_stability import (  # noqa: E402
    FROZEN_OPTIMIZED_STABILIZER,
    resolve_stabilizer,
    stabilizer_metadata,
)
from bucket4.bucket4_price_loading import (  # noqa: E402
    load_etf_underlying_map,
    load_metrics_frame,
    load_price_panel,
    load_underlying_adj_close_series,
)
from bucket4.bucket4_vol_shape_signals import load_vol_shape_history  # noqa: E402
from bucket4.cash_residual_path import CashResidualParams, build_cash_residual_pins  # noqa: E402
from bucket4.pit_inputs import load_borrow_history  # noqa: E402
from bucket4.policy_helpers import load_policy  # noqa: E402
from build_b4_inception_research import (  # noqa: E402
    DEFAULT_POLICY,
    VOL_SHAPE_HISTORY,
    _default_etf_universe,
    _effective_research_start,
    _etf_list,
    _first_valid_borrow_date,
    _row_for_etf,
)

DISCLAIMER = (
    "Cash-residual research path (scale_to_budget=false): crash caps leave cash "
    "undeployed; gross can rise again when L/run-up ease on cadence-DUE. "
    "Optional h-first bumps hedge before cutting gross. Not production book PnL."
)
NEST_KEY = "cash_residual_path"
OUT_DIR_DEFAULT = REPO / "data" / "bucket4_cash_residual_path"
BOOK_VIEW = REPO / "data" / "cash_residual_book_latest.json"


def _now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _compact(vals: list, nd: int = 6) -> list:
    out = []
    for v in vals:
        if v is None:
            out.append(None)
        elif isinstance(v, (int, bool)) and not isinstance(v, bool):
            out.append(int(v) if abs(v) == v else v)
        elif isinstance(v, float):
            if not (v == v):  # NaN
                out.append(None)
            else:
                out.append(round(float(v), nd))
        else:
            out.append(v)
    return out


def _positive_weight(raw: Any) -> float | None:
    """Production/book weight of 0 means 'not in sleeve' — treat as missing for research."""
    try:
        w = float(raw)
    except (TypeError, ValueError):
        return None
    if not (w == w) or w <= 1e-12:
        return None
    return w


def _resolve_budget_weight(etf: str, args: argparse.Namespace) -> tuple[float, float, str]:
    """Return (sleeve_budget, pair_weight, source).

    Zero/negative shard weights (common for out-of-book research names) fall back
    to a default research weight so Optimized cash-residual is not a blank path.
    """
    if args.budget is not None and args.weight is not None:
        w_cli = _positive_weight(args.weight)
        if w_cli is None:
            raise ValueError(f"--weight must be > 0 (got {args.weight})")
        return float(args.budget), float(w_cli), "cli"
    budget = float(args.budget) if args.budget is not None else None
    weight = _positive_weight(args.weight) if args.weight is not None else None
    src = "defaults"
    if BOOK_VIEW.is_file():
        try:
            book = json.loads(BOOK_VIEW.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            book = {}
        sleeve = book.get("sleeve") or {}
        if budget is None and sleeve.get("budget_usd") is not None:
            budget = float(sleeve["budget_usd"])
            src = "cash_residual_book"
        for p in book.get("pairs") or []:
            if str(p.get("etf") or "").upper() == etf:
                if weight is None:
                    w = _positive_weight(p.get("weight"))
                    if w is not None:
                        weight = w
                        src = "cash_residual_book"
                break
    shard_path = REPO / "data" / "bucket4_pairs" / f"{etf}.json"
    if weight is None and shard_path.is_file():
        try:
            shard = json.loads(shard_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            shard = {}
        for key in ("production_weight", "effective_weight", "weight"):
            w = _positive_weight(shard.get(key))
            if w is not None:
                weight = w
                src = "pair_shard"
                break
        if weight is None:
            sm = (shard.get("summary") or {})
            for key in ("production_weight", "effective_weight", "weight"):
                w = _positive_weight(sm.get(key))
                if w is not None:
                    weight = w
                    src = "pair_shard"
                    break
    if budget is None:
        budget = 168_000.0
        if src == "defaults":
            src = "default_budget"
    if weight is None:
        weight = 0.25
        src = (src + "+default_weight") if src != "defaults" else "default_weight"
    return float(budget), float(weight), src


def _write_nest(etf: str, payload: dict) -> None:
    path = REPO / "data" / "bucket4_pairs" / f"{etf}.json"
    if not path.is_file():
        print(f"warn {etf}: no pair shard to nest — wrote standalone only", file=sys.stderr)
        return
    shard = json.loads(path.read_text(encoding="utf-8"))
    nest = {k: payload[k] for k in (
        "schema", "etf", "underlying", "authoritative", "history_basis",
        "disclaimer", "daily", "summary", "rebalance_log",
        "notional_basis_usd", "ledger_mode", "etf_inception_date",
        "research_parameters", "policy", "telemetry",
    ) if k in payload}
    shard[NEST_KEY] = nest
    path.write_text(json.dumps(shard, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    print(f"nested {NEST_KEY} -> {path}")


def _build_one(
    etf: str,
    *,
    args: argparse.Namespace,
    policy: dict,
    panel: dict,
    vol_history: dict,
    borrow_history: dict | None,
    metrics: pd.DataFrame | None = None,
    etf_to_und: dict[str, str] | None = None,
) -> bool:
    row = _row_for_etf(etf)
    und = str(row.get("Underlying") or "").upper()
    if etf not in panel or panel[etf].empty:
        print(f"skip {etf}: missing price panel", file=sys.stderr)
        return False

    budget, weight, bw_src = _resolve_budget_weight(etf, args)
    pair_target = budget * weight
    beta = abs(float(pd.to_numeric(row.get("Delta"), errors="coerce") or 2.0))

    warmup_bdays = int(args.warmup_bdays)
    signal_window = int(args.signal_window)
    min_days = int(args.min_days)
    require_pit = bool(args.require_pit_borrow)
    trade_from_inception = not bool(args.honor_warmup_delay)

    first_borrow = _first_valid_borrow_date(borrow_history or {}, etf)
    research_start = _effective_research_start(
        panel[etf].index,
        start_floor=args.start,
        warmup_bdays=warmup_bdays,
        first_borrow_date=first_borrow,
        require_pit_borrow=require_pit and not trade_from_inception,
        trade_from_inception=trade_from_inception,
    )
    if research_start is None:
        print(f"skip {etf}: no research start", file=sys.stderr)
        return False
    mem_start = research_start.strftime("%Y-%m-%d")
    rebal_warmup = 0 if trade_from_inception else warmup_bdays

    stab = resolve_stabilizer(FROZEN_OPTIMIZED_STABILIZER) if args.stable_h else None

    joint_und = panel[etf]["b_px"] if "b_px" in panel[etf].columns else panel[etf].iloc[:, -1]
    # Pre-listing und history so crash L / TR-VCR h are live on day 1 of the path.
    und_close = load_underlying_adj_close_series(
        und,
        metrics=metrics,
        etf_to_und=etf_to_und,
        panel_fallback=joint_und,
        asof=panel[etf].index.max(),
        entry_date=mem_start,
        min_obs_before_entry=max(252, int(CashResidualParams().anchor_window)),
        yahoo_fallback=not bool(getattr(args, "no_yahoo_und", False)),
    )
    if und_close.empty:
        und_close = joint_und
    entry_ts = pd.Timestamp(mem_start)
    und_hist_n = int(und_close.loc[und_close.index < entry_ts].shape[0]) if len(und_close) else 0

    # Probe pass: unit-equity path to get rebal calendar + h series
    bt0, h_daily, rb_diag, status = b4.run_pair_backtest_for_row(
        row,
        panel,
        policy,
        start=args.start,
        min_days=min_days,
        warmup_bdays=rebal_warmup,
        signal_window=signal_window,
        vol_history=vol_history,
        borrow_history=borrow_history,
        membership_start=mem_start,
        membership_end=None,
        hard_exit=False,
        stop_on_equity_wipeout=not bool(args.continue_through_wipeout),
        h_stabilizer=stab,
        signal_underlying_prices=und_close,
    )
    if status != "ok" or bt0 is None or bt0.empty or h_daily is None:
        print(f"skip {etf}: probe backtest {status}", file=sys.stderr)
        return False

    dates = [pd.Timestamp(d).strftime("%Y-%m-%d") for d in bt0.index]
    rebal = [1 if bool(x) else 0 for x in bt0.get("rebalance", pd.Series(False, index=bt0.index)).to_numpy()]

    edge = None
    try:
        edge = float(pd.to_numeric(row.get("bucket4_net_edge_annual"), errors="coerce"))
        if edge != edge:
            edge = None
    except Exception:  # noqa: BLE001
        edge = None

    cr_params = CashResidualParams(
        rho=float(args.rho),
        h_first_enabled=not bool(args.no_h_first),
        edge_floor=float(args.edge_floor),
        emergency_cut_rel=float(args.emergency_cut_rel),
        l_ema_alpha=float(args.l_ema_alpha),
    )
    pins = build_cash_residual_pins(
        dates=dates,
        rebalance=rebal,
        h_series=h_daily,
        und_close=und_close,
        beta=beta,
        sleeve_budget_usd=budget,
        pair_weight=weight,
        edge_annual=edge,
        params=cr_params,
    )

    # Policy clone: sleeve dollars seed = pair target
    pol = json.loads(json.dumps(policy))
    pol.setdefault("backtest", {})
    pol["backtest"]["initial_capital"] = float(pair_target)

    bt, h_daily2, rb_diag2, status2 = b4.run_pair_backtest_for_row(
        row,
        panel,
        pol,
        start=args.start,
        min_days=min_days,
        warmup_bdays=rebal_warmup,
        signal_window=signal_window,
        vol_history=vol_history,
        borrow_history=borrow_history,
        membership_start=mem_start,
        membership_end=None,
        hard_exit=False,
        stop_on_equity_wipeout=not bool(args.continue_through_wipeout),
        h_stabilizer=stab,
        target_gross_by_date=pins["target_gross_by_date"],
        h_target_by_date=pins["h_target_by_date"],
        capital_mode="sleeve_dollars",
        signal_underlying_prices=und_close,
    )
    if status2 != "ok" or bt is None or bt.empty:
        print(f"skip {etf}: sized backtest {status2}", file=sys.stderr)
        return False

    shard = b4.pair_shard_from_result(
        row,
        bt,
        h_daily2,
        rb_diag2,
        status=status2,
        gate_reason="cash_residual_path",
        in_production_book=False,
        trim_wipeout_tail=not bool(args.continue_through_wipeout),
    )
    daily = dict(shard.get("daily") or {})
    if not daily.get("dates") or len(daily["dates"]) < 2:
        print(f"skip {etf}: insufficient sized path", file=sys.stderr)
        return False

    # Align telemetry to sized daily dates (ffill from pin walk)
    tel = pins["telemetry"]
    pin_dates = dates
    tel_by = {pin_dates[i]: {k: tel[k][i] for k in tel} for i in range(len(pin_dates))}
    aligned: dict[str, list] = {k: [] for k in tel}
    last = {k: None for k in tel}
    for ds in daily["dates"]:
        row_t = tel_by.get(ds)
        if row_t:
            last = row_t
        for k in tel:
            aligned[k].append(last.get(k))

    for k, arr in aligned.items():
        if k in ("cadence_due",):
            daily[k] = [int(x or 0) for x in arr]
        elif k == "reason":
            daily[k] = [str(x or "") for x in arr]
        else:
            daily[k] = _compact(arr)

    # Dollar ledger hints for chart scaler (equity already in $)
    daily["gross_exposure_dollars"] = daily.get("gross_exposure")
    daily["net_pnl_dollars"] = daily.get("net_pnl")

    first_metrics = pd.Timestamp(panel[etf].index.min()).strftime("%Y-%m-%d")
    research_parameters = {
        "budget_source": bw_src,
        "sleeve_budget_usd": budget,
        "pair_weight": weight,
        "pair_target_usd": pair_target,
        "scale_to_budget": False,
        "h_first_enabled": cr_params.h_first_enabled,
        "rho": cr_params.rho,
        "stable_h": bool(args.stable_h),
        "effective_trade_start": daily["dates"][0],
        "first_metrics_date": first_metrics,
        "capital_mode": "sleeve_dollars",
        "und_history_n_before_entry": und_hist_n,
        "und_history_source": "fleet_metrics+yahoo_fallback",
    }
    if stab:
        research_parameters["stabilizer"] = stabilizer_metadata(FROZEN_OPTIMIZED_STABILIZER)

    summary = {
        **(shard.get("summary") or {}),
        **pins["summary"],
        "history_basis": "cash_residual_path",
        "etf_inception_date": first_metrics,
        "entry_date": daily["dates"][0],
        "latest_date": daily["dates"][-1],
        "research_parameters": research_parameters,
    }

    out = {
        "schema": "cash_residual_path.v1",
        "etf": etf,
        "underlying": und or shard.get("underlying"),
        "authoritative": False,
        "history_basis": "cash_residual_path",
        "disclaimer": DISCLAIMER,
        "daily": daily,
        "summary": summary,
        "rebalance_log": shard.get("rebalance_log") or [],
        "notional_basis_usd": float(pair_target),
        "ledger_mode": "actual_dollar",
        "etf_inception_date": first_metrics,
        "research_parameters": research_parameters,
        "policy": {
            "policy_id": "cash_residual_policy",
            "policy_label": "Cash-residual policy",
            "scale_to_budget": False,
            "rho": cr_params.rho,
            "h_first": cr_params.h_first_enabled,
            "generated_at": _now(),
        },
        "telemetry": {k: daily[k] for k in aligned},
    }
    if stab:
        out["stabilizer"] = stabilizer_metadata(FROZEN_OPTIMIZED_STABILIZER)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{etf}.json"
    path.write_text(json.dumps(out, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    print(
        f"wrote {path} ({len(daily['dates'])}d) "
        f"target={pair_target:.0f} end_applied={summary.get('end_gross_applied_usd')} "
        f"resid%={summary.get('end_cash_residual_pct')}"
    )
    if not args.no_nest:
        try:
            _write_nest(etf, out)
        except Exception as nest_exc:  # noqa: BLE001
            print(f"warn {etf}: nest failed: {nest_exc}", file=sys.stderr)
    return True


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--etfs", default="", help="Comma list (default: inception universe)")
    ap.add_argument("--start", default="2020-01-01")
    ap.add_argument("--out-dir", type=Path, default=OUT_DIR_DEFAULT)
    ap.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    ap.add_argument("--budget", type=float, default=None, help="Sleeve budget USD")
    ap.add_argument("--weight", type=float, default=None, help="Pair weight of sleeve")
    ap.add_argument("--rho", type=float, default=0.0075)
    ap.add_argument("--no-h-first", action="store_true")
    ap.add_argument("--edge-floor", type=float, default=0.0)
    ap.add_argument("--emergency-cut-rel", type=float, default=0.25)
    ap.add_argument("--l-ema-alpha", type=float, default=0.4)
    ap.add_argument(
        "--stable-h",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Use frozen deadband+slew hedge controller (default: off — use dynamic Current h)",
    )
    ap.add_argument("--warmup-bdays", type=int, default=21)
    ap.add_argument("--signal-window", type=int, default=60)
    ap.add_argument(
        "--min-days",
        type=int,
        default=20,
        help="Min ETF panel days (default 20 so short IPO names like CBRZ are included)",
    )
    ap.add_argument("--require-pit-borrow", action="store_true")
    ap.add_argument("--honor-warmup-delay", action="store_true")
    ap.add_argument("--continue-through-wipeout", action="store_true")
    ap.add_argument("--no-nest", action="store_true")
    ap.add_argument(
        "--no-yahoo-und",
        action="store_true",
        help="Do not extend thin underlying histories with Yahoo (metrics/fleet only)",
    )
    args = ap.parse_args(argv)

    etfs = _etf_list(args.etfs) if args.etfs else _default_etf_universe()
    # Prefer names that already have inception nests (faster / more relevant)
    if not args.etfs:
        pairs = REPO / "data" / "bucket4_pairs"
        nested = []
        for e in etfs:
            p = pairs / f"{e}.json"
            if not p.is_file():
                continue
            try:
                sh = json.loads(p.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if sh.get("inception_research") or sh.get("inception_research_stable"):
                nested.append(e)
        if nested:
            etfs = nested

    policy = load_policy(args.policy)
    print(f"loading panel for {len(etfs)} ETFs…")
    panel = load_price_panel(min_days=max(10, min(int(args.min_days), 40)))
    metrics = load_metrics_frame()
    etf_to_und = load_etf_underlying_map()
    vol_history = load_vol_shape_history(VOL_SHAPE_HISTORY) if VOL_SHAPE_HISTORY.is_file() else {}
    borrow_history = load_borrow_history()

    ok = 0
    for etf in etfs:
        try:
            if _build_one(
                etf,
                args=args,
                policy=policy,
                panel=panel,
                vol_history=vol_history,
                borrow_history=borrow_history,
                metrics=metrics,
                etf_to_und=etf_to_und,
            ):
                ok += 1
        except Exception as exc:  # noqa: BLE001
            print(f"skip {etf}: {exc}", file=sys.stderr)
    print(f"done: {ok}/{len(etfs)} cash_residual paths")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

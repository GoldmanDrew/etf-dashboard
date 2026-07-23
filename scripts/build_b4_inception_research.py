#!/usr/bin/env python3
"""Build labeled inception-research pair series for Bucket 4 production pairs.

Uses the research-legacy pair engine from first overlapping ETF+underlying
metrics day. Output goes to ``--out-dir/{ETF}.json`` for optional attachment
beside an ls-algo production replay as ``inception_research/{ETF}.json``.

Never authoritative — must not feed B4 Book KPIs.

Example:
  python scripts/build_b4_inception_research.py --etfs QBTZ \\
      --out-dir data/bucket4_inception_research
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

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
from bucket4.bucket4_price_loading import load_price_panel  # noqa: E402
from bucket4.bucket4_vol_shape_signals import load_vol_shape_history  # noqa: E402
from bucket4.layer_a_parity import membership_bounds_from_production  # noqa: E402
from bucket4.policy_helpers import load_policy  # noqa: E402

DISCLAIMER = (
    "Research path from ETF listing/inception (first overlapping metrics session). "
    "Early hedge uses neutral h_mid until TR/VCR warms; borrow uses spot/zero "
    "fallback before first PIT observation. Not production-policy replay. "
    "Not used for book PnL."
)
STABLE_DISCLAIMER = (
    "Optimized research path from ETF listing/inception with frozen deadband+slew "
    f"hedge controller ({FROZEN_OPTIMIZED_STABILIZER}). Early days may use h_mid "
    "until signals warm. Not production book PnL."
)
CONTINUE_WIPEOUT_DISCLAIMER = (
    "Research path continues marking through equity≤0 (no flatten) and resumes "
    "resizing after recovery. Hypothetical margin-call path — not production book PnL."
)
LAYER_A_DISCLAIMER = (
    "Membership-aware research twin clipped to production enter→exit. "
    "Not production-policy replay. Not used for book PnL."
)
VOL_SHAPE_HISTORY = REPO / "data" / "vol_shape_history.json"
DEFAULT_POLICY = REPO / "config" / "bucket4_backtest_policy.yml"
DEFAULT_STABLE_OUT = REPO / "data" / "bucket4_inception_research_stable"
NEST_KEYS = (
    "schema", "etf", "underlying", "authoritative", "history_basis",
    "disclaimer", "daily", "summary", "rebalance_log",
    "notional_basis_usd", "ledger_mode", "etf_inception_date",
    "research_parameters", "stabilizer",
)


def _default_etf_universe() -> list[str]:
    """Book pairs ∪ production shards ∪ screener Bucket 4 dashboard rows."""
    etfs: set[str] = set()
    book = REPO / "data" / "bucket4_backtest.json"
    if book.is_file():
        payload = json.loads(book.read_text(encoding="utf-8"))
        for p in payload.get("pairs") or []:
            etf = str(p.get("etf") or "").strip().upper()
            if etf:
                etfs.add(etf)
        for m in payload.get("membership") or []:
            etf = str(m.get("etf") or "").strip().upper()
            if etf:
                etfs.add(etf)
    pairs_dir = REPO / "data" / "bucket4_pairs"
    if pairs_dir.is_dir():
        for path in pairs_dir.glob("*.json"):
            try:
                shard = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if shard.get("in_production_book") or shard.get("schema") == "bucket4_production_pair.v1":
                etfs.add(path.stem.upper())
    dash = REPO / "data" / "dashboard_data.json"
    if dash.is_file():
        try:
            payload = json.loads(dash.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        for row in payload.get("records") or payload.get("rows") or []:
            if str(row.get("screener_bucket") or "").lower() != "bucket_4":
                continue
            etf = str(row.get("symbol") or "").strip().upper()
            if etf:
                etfs.add(etf)
    return sorted(etfs)


def _etf_list(raw: str) -> list[str]:
    etfs = [e.strip().upper() for e in str(raw).split(",") if e.strip()]
    if etfs:
        return etfs
    return _default_etf_universe()


def _row_for_etf(etf: str) -> pd.Series:
    und = ""
    delta = -2.0
    borrow = 0.0
    shard_path = REPO / "data" / "bucket4_pairs" / f"{etf}.json"
    if shard_path.is_file():
        shard = json.loads(shard_path.read_text(encoding="utf-8"))
        und = str(shard.get("underlying") or (shard.get("summary") or {}).get("underlying") or "").upper()
        try:
            delta = float((shard.get("summary") or {}).get("Delta") or delta)
        except (TypeError, ValueError):
            delta = -2.0
    dash = REPO / "data" / "dashboard_data.json"
    # Always read the dashboard row.  The previous conditional skipped the row
    # whenever a pair shard already supplied underlying/beta, silently leaving
    # borrow_current at 0% for the normal Optimized path.
    if dash.is_file():
        try:
            payload = json.loads(dash.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        for row in payload.get("records") or payload.get("rows") or []:
            if str(row.get("symbol") or "").strip().upper() != etf:
                continue
            if not und:
                und = str(row.get("underlying") or "").strip().upper()
            try:
                d = float(row.get("delta") if row.get("delta") is not None else row.get("beta"))
                if np_isfinite(d):
                    delta = d
            except (TypeError, ValueError):
                pass
            try:
                borrow = float(row.get("borrow_current") or borrow)
            except (TypeError, ValueError):
                pass
            break
    return pd.Series({
        "ETF": etf,
        "Underlying": und,
        "Delta": delta,
        "borrow_current": borrow,
        "bucket4_net_edge_annual": 0.0,
        "vol_underlying_annual": float("nan"),
    })


def _first_valid_borrow_date(history: dict[str, pd.Series], etf: str) -> pd.Timestamp | None:
    ser = history.get(str(etf).strip().upper())
    if ser is None or ser.empty:
        return None
    valid = pd.to_numeric(ser, errors="coerce").dropna()
    valid = valid.loc[valid >= 0]
    return pd.Timestamp(valid.index.min()) if not valid.empty else None


def _effective_research_start(
    price_index: pd.DatetimeIndex,
    *,
    start_floor: str,
    warmup_bdays: int,
    first_borrow_date: pd.Timestamp | None,
    require_pit_borrow: bool,
    trade_from_inception: bool = True,
) -> pd.Timestamp | None:
    """First Optimized trade session.

    Default (``trade_from_inception``): first overlapping price session after
    ``start_floor`` — listing/inception proxy. Signal warmup no longer delays
    entry; early days use neutral ``h_mid`` until TR/VCR is available.

    Legacy (``trade_from_inception=False``): wait ``warmup_bdays`` and optionally
    the first PIT borrow observation before trading.
    """
    floor = pd.Timestamp(start_floor)
    dates = pd.DatetimeIndex([d for d in price_index if pd.Timestamp(d) >= floor]).sort_values()
    if len(dates) < 2:
        return None
    if trade_from_inception:
        start = pd.Timestamp(dates[0])
        # Do not delay entry for PIT borrow — engine uses spot/zero fallback
        # until the first observation, then switches to PIT series.
        return start
    if len(dates) <= int(warmup_bdays):
        return None
    start = pd.Timestamp(dates[int(warmup_bdays)])
    if require_pit_borrow:
        if first_borrow_date is None:
            return None
        eligible = dates[dates >= pd.Timestamp(first_borrow_date)]
        if eligible.empty:
            return None
        start = max(start, pd.Timestamp(eligible[0]))
    return start


def np_isfinite(x: float) -> bool:
    try:
        return bool(x == x and abs(float(x)) != float("inf"))
    except (TypeError, ValueError):
        return False


def _nest_research_block(out: dict, nest_key: str) -> dict:
    return {k: out[k] for k in NEST_KEYS if k in out and out[k] is not None}


def _write_text_retry(path: Path, text: str, *, attempts: int = 8) -> None:
    """Windows can briefly lock pair shards while the local HTTP server serves them."""
    last_exc: OSError | None = None
    for i in range(attempts):
        try:
            path.write_text(text, encoding="utf-8")
            return
        except OSError as exc:
            last_exc = exc
            time.sleep(0.15 * (i + 1))
    assert last_exc is not None
    raise last_exc


def _write_pair_nest(etf: str, out: dict, nest_key: str) -> None:
    nest_block = _nest_research_block(out, nest_key)
    prod_path = REPO / "data" / "bucket4_pairs" / f"{etf}.json"
    if prod_path.is_file():
        prod = json.loads(prod_path.read_text(encoding="utf-8"))
        prod[nest_key] = nest_block
        _write_text_retry(
            prod_path,
            json.dumps(prod, indent=2, sort_keys=True, allow_nan=False) + "\n",
        )
        print(f"nested {nest_key} onto {prod_path.name}")
        return
    research_shard = {
        "schema": "bucket4_research_pair.v1",
        "etf": etf,
        "underlying": out.get("underlying") or "",
        "in_production_book": False,
        "gate_reason": "optimized_backtest_only",
        "model_status": "research",
        "authoritative": False,
        "history_basis": nest_key,
        "disclaimer": out.get("disclaimer") or DISCLAIMER,
        "daily": {},
        "summary": {
            "etf": etf,
            "underlying": out.get("underlying") or "",
            "gate_reason": "optimized_backtest_only",
            "etf_inception_date": out.get("etf_inception_date"),
        },
        "rebalance_log": [],
        "notional_basis_usd": 1.0,
        "ledger_mode": "unit_capital",
        nest_key: nest_block,
        "etf_inception_date": out.get("etf_inception_date"),
    }
    prod_path.parent.mkdir(parents=True, exist_ok=True)
    _write_text_retry(
        prod_path,
        json.dumps(research_shard, indent=2, sort_keys=True, allow_nan=False) + "\n",
    )
    print(f"wrote research-only pair shard {prod_path.name} ({nest_key})")


def _build_one_etf(
    etf: str,
    *,
    panel: dict,
    policy: dict,
    args: argparse.Namespace,
    min_days: int,
    warmup_bdays: int,
    signal_window: int,
    vol_history: dict,
    borrow_history: dict,
    require_pit_borrow: bool,
    resolved_policy_hash: str,
    stabilizer_name: str | None,
    out_dir: Path,
    nest_key: str,
) -> bool:
    row = _row_for_etf(etf)
    if etf not in panel:
        print(f"skip {etf}: no price panel", file=sys.stderr)
        return False
    first_borrow_date = _first_valid_borrow_date(borrow_history, etf)
    trade_from_inception = bool(args.trade_from_inception) and not bool(args.honor_production_membership)
    research_start = _effective_research_start(
        panel[etf].index,
        start_floor=args.start,
        warmup_bdays=warmup_bdays,
        first_borrow_date=first_borrow_date,
        require_pit_borrow=require_pit_borrow and not trade_from_inception,
        trade_from_inception=trade_from_inception,
    )
    if not args.honor_production_membership and research_start is None:
        reason = (
            "missing PIT borrow"
            if require_pit_borrow and first_borrow_date is None and not trade_from_inception
            else "insufficient price history"
        )
        print(f"skip {etf}: {reason}", file=sys.stderr)
        return False
    mem_start = mem_end = None
    hard_exit = False
    membership_meta = None
    # Enter on listing day; keep signal_window for VCR when history exists.
    # Warmup only delayed the *first scheduled resize* in the legacy path.
    rebal_warmup = 0 if trade_from_inception else warmup_bdays
    if args.honor_production_membership:
        prod_path = REPO / "data" / "bucket4_pairs" / f"{etf}.json"
        if not prod_path.is_file():
            print(f"skip {etf}: no production shard for membership", file=sys.stderr)
            return False
        prod_payload = json.loads(prod_path.read_text(encoding="utf-8"))
        prod_daily = prod_payload.get("daily") or {}
        book_end = None
        run_end = None
        book_path = REPO / "data" / "bucket4_backtest.json"
        if book_path.is_file():
            book = json.loads(book_path.read_text(encoding="utf-8"))
            book_end = book.get("window_end")
            for m in book.get("membership") or []:
                if str(m.get("etf") or "").upper() == etf:
                    run_end = m.get("run_end") or m.get("last_plan_date")
                    break
        membership_meta = membership_bounds_from_production(
            prod_daily,
            ignore_hard_exit=not bool(args.honor_hard_exit),
            isolation_end=str(run_end or book_end) if (run_end or book_end) else None,
        )
        mem_start = membership_meta["membership_start"]
        mem_end = membership_meta["membership_end"]
        hard_exit = bool(membership_meta["hard_exit"])
    elif research_start is not None:
        mem_start = research_start.strftime("%Y-%m-%d")

    stab_spec = resolve_stabilizer(stabilizer_name)
    try:
        bt, h_daily, rb_diag, status = b4.run_pair_backtest_for_row(
            row,
            panel,
            policy,
            start=args.start,
            min_days=min_days if not args.honor_production_membership else min(min_days, 5),
            warmup_bdays=rebal_warmup,
            signal_window=signal_window,
            vol_history=vol_history,
            borrow_history=borrow_history,
            membership_start=mem_start,
            membership_end=mem_end,
            hard_exit=hard_exit,
            stop_on_equity_wipeout=not bool(args.continue_through_wipeout),
            h_stabilizer=stab_spec,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"skip {etf}: backtest failed: {exc}", file=sys.stderr)
        return False

    gate_reason = (
        "layer_a_membership_twin"
        if args.honor_production_membership
        else ("inception_research_stable" if stab_spec else "inception_research")
    )
    shard = b4.pair_shard_from_result(
        row,
        bt,
        h_daily,
        rb_diag,
        status=status,
        gate_reason=gate_reason,
        in_production_book=False,
        trim_wipeout_tail=not bool(args.continue_through_wipeout),
    )
    daily = shard.get("daily") or {}
    if not daily.get("dates") or len(daily["dates"]) < 2:
        print(f"skip {etf}: status={status} insufficient path", file=sys.stderr)
        return False

    if args.honor_production_membership:
        history_basis = "layer_a_membership_twin"
        disclaimer = LAYER_A_DISCLAIMER
        schema = "bucket4_layer_a_twin.v1"
    elif stab_spec:
        history_basis = "inception_research_stable"
        disclaimer = STABLE_DISCLAIMER
        schema = "bucket4_inception_research_stable.v1"
    else:
        history_basis = "inception_research"
        disclaimer = CONTINUE_WIPEOUT_DISCLAIMER if args.continue_through_wipeout else DISCLAIMER
        schema = "bucket4_inception_research.v1"

    # First overlapping metrics session (true listing proxy) ≠ research trade start
    # after signal warmup / PIT borrow gate.
    try:
        first_metrics_date = pd.Timestamp(panel[etf].index.min()).strftime("%Y-%m-%d")
    except Exception:  # noqa: BLE001
        first_metrics_date = daily["dates"][0]
    research_parameters = {
        "policy_sha256": resolved_policy_hash,
        "start_floor": str(args.start),
        "min_days": min_days,
        "warmup_bdays": warmup_bdays,
        "signal_window": signal_window,
        "lookahead_shift": 1,
        "vcr_baseline": "expanding_median_point_in_time",
        "effective_trade_start": daily["dates"][0],
        "first_metrics_date": first_metrics_date,
        "trade_from_inception": trade_from_inception,
        "rebal_warmup_bdays": rebal_warmup,
        "signal_warmup_note": (
            "enter at inception; h_mid until TR/VCR available"
            if trade_from_inception
            else "legacy: trade start after warmup_bdays"
        ),
        "pit_borrow_required": require_pit_borrow and not trade_from_inception,
        "first_etf_borrow_observation": (
            first_borrow_date.strftime("%Y-%m-%d") if first_borrow_date is not None else None
        ),
        "underlying_borrow": "not_available_not_charged",
        "etf_borrow_before_pit": "spot_or_zero_fallback" if trade_from_inception else "n/a",
    }
    if stab_spec and stabilizer_name:
        research_parameters["stabilizer"] = stabilizer_metadata(stabilizer_name)

    summary = {
        **(shard.get("summary") or {}),
        "history_basis": history_basis,
        "etf_inception_date": first_metrics_date,
        "first_metrics_date": first_metrics_date,
        "entry_date": daily["dates"][0],
        "research_parameters": research_parameters,
    }
    if stab_spec and stabilizer_name:
        summary["stabilizer"] = stabilizer_metadata(stabilizer_name)
    if args.continue_through_wipeout:
        summary["path_mode"] = "continue_through_wipeout"
        summary["continue_through_wipeout"] = True
        eq = daily.get("equity") or []
        neg_dates = [
            daily["dates"][i]
            for i, e in enumerate(eq)
            if isinstance(e, (int, float)) and e <= 0
        ]
        if neg_dates:
            summary["equity_underwater_first"] = neg_dates[0]
            summary["equity_underwater_days"] = len(neg_dates)

    out = {
        "schema": schema,
        "etf": etf,
        "underlying": shard.get("underlying") or row.get("Underlying"),
        "authoritative": False,
        "history_basis": history_basis,
        "disclaimer": disclaimer,
        "daily": daily,
        "summary": summary,
        "rebalance_log": shard.get("rebalance_log") or [],
        "notional_basis_usd": 1.0,
        "ledger_mode": "unit_capital",
        "etf_inception_date": first_metrics_date,
        "research_parameters": research_parameters,
    }
    if stab_spec and stabilizer_name:
        out["stabilizer"] = stabilizer_metadata(stabilizer_name)
    if membership_meta is not None:
        out["membership"] = membership_meta

    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{etf}.json"
    path.write_text(json.dumps(out, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
    label = f" [{stabilizer_name}]" if stabilizer_name else ""
    print(f"wrote {path}{label} ({len(daily['dates'])} days from {daily['dates'][0]})")

    if not args.no_nest and not args.honor_production_membership:
        _write_pair_nest(etf, out, nest_key)
    return True


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--etfs",
        default="",
        help="Comma-separated ETF list (default: book ∪ production shards ∪ screener B4)",
    )
    ap.add_argument("--out-dir", type=Path, default=REPO / "data" / "bucket4_inception_research")
    ap.add_argument(
        "--stable-out-dir",
        type=Path,
        default=DEFAULT_STABLE_OUT,
        help="Output dir for deadband+slew Optimized path",
    )
    ap.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    ap.add_argument("--start", default="1900-01-01", help="Research signal-history floor (default: no date floor)")
    ap.add_argument("--min-days", type=int, default=None, help="Minimum price observations (default: policy backtest.min_days)")
    ap.add_argument("--warmup-bdays", type=int, default=None, help="Override policy signal warmup")
    ap.add_argument("--signal-window", type=int, default=None, help="Override policy TR/VCR window")
    ap.add_argument(
        "--allow-missing-pit-borrow",
        action="store_true",
        help="Allow spot/zero fallback before the first borrow observation (lookahead-prone; default: fail closed)",
    )
    ap.add_argument(
        "--honor-production-membership",
        action="store_true",
        help="Clip twin to production enter→end (Layer A / isolation calibration mode)",
    )
    ap.add_argument(
        "--honor-hard-exit",
        action="store_true",
        help="With --honor-production-membership, flatten on production hard_exit/blacklist "
             "(default: isolation — ignore book exits and extend to run/book end)",
    )
    ap.add_argument(
        "--no-nest",
        action="store_true",
        help="Do not nest output onto data/bucket4_pairs/{ETF}.json",
    )
    ap.add_argument(
        "--continue-through-wipeout",
        action="store_true",
        help="Keep marking (and resume resizing after equity recovers) instead of "
             "flattening on equity≤0. Research-only: shows path to latest session.",
    )
    ap.add_argument(
        "--stabilizer",
        choices=["none", FROZEN_OPTIMIZED_STABILIZER],
        default="none",
        help="Hedge-target stabilizer for this build (default: none = current Optimized path)",
    )
    ap.add_argument(
        "--with-stable",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Also build nested inception_research_stable with frozen deadband+slew "
             "(default: on; ignored for Layer A membership twin)",
    )
    ap.add_argument(
        "--trade-from-inception",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Optimized default: trade from first metrics session (listing proxy). "
             "Disable to restore legacy warmup_bdays + PIT-borrow start gate.",
    )
    args = ap.parse_args(argv)

    etfs = _etf_list(args.etfs)
    if not etfs:
        print("No ETFs specified and no production pairs found.", file=sys.stderr)
        return 2

    policy = load_policy(args.policy)
    bt_cfg = policy.get("backtest") or {}
    min_days = int(args.min_days if args.min_days is not None else bt_cfg.get("min_days", 60))
    warmup_bdays = int(args.warmup_bdays if args.warmup_bdays is not None else bt_cfg.get("warmup_bdays", 60))
    signal_window = int(args.signal_window if args.signal_window is not None else bt_cfg.get("signal_window", 60))
    panel = load_price_panel(min_days=max(10, min(min_days, 40)))
    vol_history = load_vol_shape_history(VOL_SHAPE_HISTORY) if VOL_SHAPE_HISTORY.is_file() else {}
    borrow_history = b4.load_borrow_history() if bool(bt_cfg.get("pit_borrow", True)) else {}
    require_pit_borrow = bool(bt_cfg.get("pit_borrow", True)) and not bool(args.allow_missing_pit_borrow)
    resolved_policy_hash = b4.policy_hash(args.policy)

    passes: list[tuple[str | None, Path, str]] = []
    if args.stabilizer == "none":
        passes.append((None, args.out_dir, "inception_research"))
        if args.with_stable and not args.honor_production_membership:
            passes.append((FROZEN_OPTIMIZED_STABILIZER, args.stable_out_dir, "inception_research_stable"))
    else:
        passes.append((args.stabilizer, args.out_dir if args.out_dir != REPO / "data" / "bucket4_inception_research" else args.stable_out_dir, "inception_research_stable"))

    written = 0
    common = dict(
        panel=panel,
        policy=policy,
        args=args,
        min_days=min_days,
        warmup_bdays=warmup_bdays,
        signal_window=signal_window,
        vol_history=vol_history,
        borrow_history=borrow_history,
        require_pit_borrow=require_pit_borrow,
        resolved_policy_hash=resolved_policy_hash,
    )
    for stab_name, out_dir, nest_key in passes:
        for etf in etfs:
            if _build_one_etf(
                etf,
                stabilizer_name=stab_name,
                out_dir=out_dir,
                nest_key=nest_key,
                **common,
            ):
                written += 1

    print(json.dumps({
        "ok": written > 0,
        "written": written,
        "passes": [
            {"stabilizer": s or "none", "out_dir": str(d.resolve()), "nest_key": k}
            for s, d, k in passes
        ],
    }))
    return 0 if written else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Layer A calibration: membership-aware research twin vs production ledger.

Builds a research-legacy twin clipped to each pair's production membership
window (enter → last ledger day / hard_exit), then scores:

- production h_used vs realized |und|/(|β|·|etf|) definition check
- realized-h twin vs production
- daily return correlation / equity-norm MAE
- cadence Jaccard by reason (enter_membership / cadence_resize / hard_exit)

Example:
  python scripts/b4_layer_a_parity.py --etfs QBTZ,MSTZ,CLSZ,APLZ,SMZ
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import build_bucket4_backtest as b4  # noqa: E402
from bucket4.bucket4_price_loading import load_price_panel  # noqa: E402
from bucket4.bucket4_vol_shape_signals import load_vol_shape_history  # noqa: E402
from bucket4.layer_a_parity import (  # noqa: E402
    compare_layer_a,
    inject_production_calendar_into_panel,
    membership_bounds_from_production,
    reason_dates,
    remap_keyed_by_snap,
    snap_dates_to_calendar,
)
from bucket4.policy_helpers import load_policy  # noqa: E402

VOL_SHAPE_HISTORY = REPO / "data" / "vol_shape_history.json"
DEFAULT_POLICY = REPO / "config" / "bucket4_backtest_policy.yml"
DEFAULT_OUT = REPO / "data" / "_layer_a_parity"


def _etfs(raw: str) -> list[str]:
    etfs = [e.strip().upper() for e in str(raw).split(",") if e.strip()]
    if etfs:
        return etfs
    book = REPO / "data" / "bucket4_backtest.json"
    if book.is_file():
        payload = json.loads(book.read_text(encoding="utf-8"))
        return [str(p.get("etf") or "").upper() for p in (payload.get("pairs") or []) if p.get("etf")]
    return []


def _row(etf: str, shard: dict) -> pd.Series:
    und = str(shard.get("underlying") or (shard.get("summary") or {}).get("underlying") or "").upper()
    try:
        delta = float((shard.get("summary") or {}).get("Delta") or -2.0)
    except (TypeError, ValueError):
        delta = -2.0
    return pd.Series({
        "ETF": etf,
        "Underlying": und,
        "Delta": delta,
        "borrow_current": 0.0,
        "bucket4_net_edge_annual": 0.0,
        "vol_underlying_annual": float("nan"),
    })


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--etfs", default="QBTZ,MSTZ,CLSZ,APLZ,SMZ")
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    ap.add_argument("--signal-start", default="2024-01-01", help="Floor for TR/VCR warmup prices")
    ap.add_argument("--min-days", type=int, default=5)
    ap.add_argument(
        "--no-pin-production-cadence",
        action="store_true",
        help="Let twin invent TR/VCR cadence (default: pin production cadence_resize dates)",
    )
    ap.add_argument(
        "--honor-hard-exit",
        action="store_true",
        help="Flatten on production hard_exit/blacklist (default: isolation — ignore book exits)",
    )
    ap.add_argument(
        "--isolation-end",
        default="",
        help="End date for isolation twin when ignoring hard_exit (default: book window_end / run_end)",
    )
    args = ap.parse_args(argv)
    pin_cadence = not bool(args.no_pin_production_cadence)
    isolation_mode = not bool(args.honor_hard_exit)

    etfs = _etfs(args.etfs)
    if not etfs:
        print("No ETFs.", file=sys.stderr)
        return 2

    policy = load_policy(args.policy)
    bt_cfg = policy.get("backtest") or {}
    warmup_bdays = int(bt_cfg.get("warmup_bdays", 60))
    signal_window = int(bt_cfg.get("signal_window", 60))
    panel = load_price_panel(min_days=10)
    vol_history = load_vol_shape_history(VOL_SHAPE_HISTORY) if VOL_SHAPE_HISTORY.is_file() else {}

    twins_dir = args.out_dir / "twins"
    twins_dir.mkdir(parents=True, exist_ok=True)
    results = []

    book_end = None
    book_path = REPO / "data" / "bucket4_backtest.json"
    membership_by_etf: dict[str, dict] = {}
    if book_path.is_file():
        book = json.loads(book_path.read_text(encoding="utf-8"))
        book_end = book.get("window_end")
        for m in book.get("membership") or []:
            etf_m = str(m.get("etf") or "").upper()
            if etf_m:
                membership_by_etf[etf_m] = m

    for etf in etfs:
        prod_path = REPO / "data" / "bucket4_pairs" / f"{etf}.json"
        if not prod_path.is_file():
            results.append({"etf": etf, "error": "missing_production_shard"})
            continue
        prod = json.loads(prod_path.read_text(encoding="utf-8"))
        daily = prod.get("daily") or {}
        mem_row = membership_by_etf.get(etf) or {}
        isolation_end = (
            args.isolation_end
            or mem_row.get("run_end")
            or mem_row.get("last_plan_date")
            or book_end
        )
        bounds = membership_bounds_from_production(
            daily,
            ignore_hard_exit=isolation_mode,
            isolation_end=str(isolation_end) if isolation_end else None,
        )
        row = _row(etf, prod)
        if etf not in panel:
            results.append({"etf": etf, "error": "missing_price_panel", **bounds})
            continue
        # Isolation default: never flatten on production blacklist/hard_exit.
        # Pin production enter + cadence_resize dates, gross, and h on those days
        # (sleeve-dollar capital mode) so Layer A matches production path shape.
        enter_dates = sorted(reason_dates(daily, "enter_membership"))
        cadence_dates = sorted(reason_dates(daily, "cadence_resize"))
        pin_dates = sorted(set(enter_dates) | set(cadence_dates)) if pin_cadence else cadence_dates
        # Always include enter so override calendars cannot drop membership open.
        if bounds.get("membership_start"):
            pin_dates = sorted(set(pin_dates or []) | {str(bounds["membership_start"])})

        gross_pin: dict[str, float] = {}
        h_pin: dict[str, float] = {}
        dates = list(daily.get("dates") or [])
        gross = list(daily.get("gross_exposure_dollars") or [])
        h_used = list(daily.get("h_used") or [])
        reasons = list(daily.get("rebalance_reason") or [])
        flags = list(daily.get("rebalance") or [])
        for i, d in enumerate(dates):
            r = str(reasons[i] if i < len(reasons) else "") or ""
            if not (flags[i] if i < len(flags) else False) and r not in (
                "enter_membership",
                "cadence_resize",
            ):
                continue
            if i < len(gross) and gross[i] is not None:
                try:
                    g = float(gross[i])
                    if g > 0:
                        gross_pin[str(d)] = g
                except (TypeError, ValueError):
                    pass
            if i < len(h_used) and h_used[i] is not None:
                try:
                    hv = float(h_used[i])
                    if hv == hv:  # not NaN
                        h_pin[str(d)] = hv
                except (TypeError, ValueError):
                    pass

        # Fill only pin-date holes (enter/cadence) so production resize sessions
        # exist on the twin clock. Do not interpolate every membership day —
        # that invents mid-path prints and can break pairs with dense calendars.
        snap_map: dict[str, str] = {}
        px = panel.get(etf)
        if px is not None and not px.empty and pin_dates:
            filled = inject_production_calendar_into_panel(px, list(pin_dates))
            panel[etf] = filled
            px = filled
        if px is not None and not px.empty and pin_dates:
            cal_days = [pd.Timestamp(d).strftime("%Y-%m-%d") for d in px.index]
            snapped, snap_map = snap_dates_to_calendar(pin_dates, cal_days)
            pin_dates = snapped
            gross_pin = remap_keyed_by_snap(gross_pin, snap_map)
            h_pin = remap_keyed_by_snap(h_pin, snap_map)
            bounds = {**bounds, "pin_snap_map": snap_map}
            if bounds.get("membership_start") and bounds["membership_start"] in snap_map:
                bounds["membership_start"] = snap_map[bounds["membership_start"]]

        # Seed equity at production notional basis so sleeve-dollar gross pins
        # keep ~1× leverage on the NAV (matches prod unit-equity + dollar legs).
        try:
            notional_seed = float((prod.get("summary") or {}).get("notional_basis_usd") or 0.0)
        except (TypeError, ValueError):
            notional_seed = 0.0
        if notional_seed <= 0 and gross_pin:
            notional_seed = float(next(iter(gross_pin.values())))
        if notional_seed <= 0:
            notional_seed = 1.0
        # Temporarily override policy initial_capital for this twin only.
        policy_run = json.loads(json.dumps(policy))
        policy_run.setdefault("backtest", {})["initial_capital"] = notional_seed

        try:
            bt, h_daily, rb_diag, status = b4.run_pair_backtest_for_row(
                row,
                panel,
                policy_run,
                start=args.signal_start,
                min_days=args.min_days,
                warmup_bdays=min(warmup_bdays, 40),
                signal_window=min(signal_window, 40),
                vol_history=vol_history,
                membership_start=bounds["membership_start"],
                membership_end=bounds["membership_end"],
                hard_exit=bool(bounds["hard_exit"]),
                rebal_dates_override=pin_dates if pin_cadence else None,
                target_gross_by_date=gross_pin,
                h_target_by_date=h_pin,
                capital_mode="sleeve_dollars",
            )
        except Exception as exc:  # noqa: BLE001
            results.append({"etf": etf, "error": f"twin_failed:{exc}", **bounds})
            continue
        if status != "ok" or bt is None or bt.empty:
            results.append({"etf": etf, "error": f"status:{status}", **bounds})
            continue
        shard = b4.pair_shard_from_result(
            row,
            bt,
            h_daily,
            rb_diag,
            status=status,
            gate_reason="layer_a_membership_twin",
            in_production_book=False,
        )
        twin_daily = shard.get("daily") or {}
        twin_out = {
            "schema": "bucket4_layer_a_twin.v1",
            "etf": etf,
            "underlying": shard.get("underlying"),
            "membership": bounds,
            "daily": twin_daily,
            "summary": shard.get("summary"),
            "rebalance_log": shard.get("rebalance_log"),
        }
        (twins_dir / f"{etf}.json").write_text(
            json.dumps(twin_out, indent=2, sort_keys=True, allow_nan=False) + "\n",
            encoding="utf-8",
        )
        try:
            beta = abs(float((prod.get("summary") or {}).get("Delta") or 2.0))
        except (TypeError, ValueError):
            beta = 2.0
        cmp_ = compare_layer_a(
            daily,
            twin_daily,
            beta_abs=beta or 2.0,
            etf=etf,
            isolation_mode=isolation_mode,
            prod_date_snap=snap_map,
        )
        cmp_["membership"] = bounds
        results.append(cmp_)
        print(
            f"{etf}: gates {cmp_.get('gates_passed')}/{cmp_.get('gates_total')} "
            f"isolation={isolation_mode} ignored_exit={bounds.get('ignored_hard_exit')} "
            f"window={bounds.get('membership_start')}->{bounds.get('membership_end')} "
            f"realized_h_mae={cmp_.get('realized_h_mae')} "
            f"ret_corr={cmp_.get('ret_corr')} "
            f"enter={cmp_.get('reasons', {}).get('enter_membership', {}).get('jaccard')} "
            f"cadence={cmp_.get('reasons', {}).get('cadence_resize', {}).get('jaccard')}"
        )

    report = {
        "note": (
            "Layer A membership-aware twin (default isolation: ignore production "
            "hard_exit/blacklist). h metrics split; cadence scored by reason type; "
            "parity scored only while production gross was open."
        ),
        "isolation_mode": isolation_mode,
        "pairs": results,
    }
    out_path = args.out_dir / "report.json"
    args.out_dir.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    passed = sum(
        1
        for r in results
        if not r.get("error")
        and r.get("gates_total")
        and r.get("gates_passed") == r.get("gates_total")
    )
    print(json.dumps({"ok": True, "pairs_fully_passing": passed, "n": len(results), "report": str(out_path)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

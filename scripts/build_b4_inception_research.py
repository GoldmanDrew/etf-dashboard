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
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

import build_bucket4_backtest as b4  # noqa: E402
from bucket4.bucket4_price_loading import load_price_panel  # noqa: E402
from bucket4.bucket4_vol_shape_signals import load_vol_shape_history  # noqa: E402
from bucket4.layer_a_parity import membership_bounds_from_production  # noqa: E402
from bucket4.policy_helpers import load_policy  # noqa: E402

DISCLAIMER = (
    "Research path from ETF/underlying overlap. Not production-policy replay. "
    "Not used for book PnL."
)
LAYER_A_DISCLAIMER = (
    "Membership-aware research twin clipped to production enter→exit. "
    "Not production-policy replay. Not used for book PnL."
)
VOL_SHAPE_HISTORY = REPO / "data" / "vol_shape_history.json"
DEFAULT_POLICY = REPO / "config" / "bucket4_backtest_policy.yml"


def _etf_list(raw: str) -> list[str]:
    etfs = [e.strip().upper() for e in str(raw).split(",") if e.strip()]
    if etfs:
        return etfs
    book = REPO / "data" / "bucket4_backtest.json"
    if book.is_file():
        payload = json.loads(book.read_text(encoding="utf-8"))
        return [str(p.get("etf") or "").upper() for p in (payload.get("pairs") or []) if p.get("etf")]
    return []


def _row_for_etf(etf: str) -> pd.Series:
    und = ""
    delta = -2.0
    shard_path = REPO / "data" / "bucket4_pairs" / f"{etf}.json"
    if shard_path.is_file():
        shard = json.loads(shard_path.read_text(encoding="utf-8"))
        und = str(shard.get("underlying") or (shard.get("summary") or {}).get("underlying") or "").upper()
        try:
            delta = float((shard.get("summary") or {}).get("Delta") or delta)
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
    ap.add_argument("--etfs", default="", help="Comma-separated ETF list (default: production book pairs)")
    ap.add_argument("--out-dir", type=Path, default=REPO / "data" / "bucket4_inception_research")
    ap.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    ap.add_argument("--start", default="2020-01-01", help="Research path start floor (default: far back)")
    ap.add_argument("--min-days", type=int, default=20)
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
    args = ap.parse_args(argv)

    etfs = _etf_list(args.etfs)
    if not etfs:
        print("No ETFs specified and no production pairs found.", file=sys.stderr)
        return 2

    policy = load_policy(args.policy)
    bt_cfg = policy.get("backtest") or {}
    warmup_bdays = int(bt_cfg.get("warmup_bdays", 60))
    signal_window = int(bt_cfg.get("signal_window", 60))
    panel = load_price_panel(min_days=max(10, min(args.min_days, 40)))
    vol_history = load_vol_shape_history(VOL_SHAPE_HISTORY) if VOL_SHAPE_HISTORY.is_file() else {}

    args.out_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    for etf in etfs:
        row = _row_for_etf(etf)
        if etf not in panel:
            print(f"skip {etf}: no price panel", file=sys.stderr)
            continue
        mem_start = mem_end = None
        hard_exit = False
        membership_meta = None
        if args.honor_production_membership:
            prod_path = REPO / "data" / "bucket4_pairs" / f"{etf}.json"
            if not prod_path.is_file():
                print(f"skip {etf}: no production shard for membership", file=sys.stderr)
                continue
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
        try:
            bt, h_daily, rb_diag, status = b4.run_pair_backtest_for_row(
                row,
                panel,
                policy,
                start=args.start,
                min_days=args.min_days if not args.honor_production_membership else min(args.min_days, 5),
                warmup_bdays=min(warmup_bdays, max(0, args.min_days - 1)),
                signal_window=min(signal_window, max(5, args.min_days)),
                vol_history=vol_history,
                membership_start=mem_start,
                membership_end=mem_end,
                hard_exit=hard_exit,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"skip {etf}: backtest failed: {exc}", file=sys.stderr)
            continue
        shard = b4.pair_shard_from_result(
            row,
            bt,
            h_daily,
            rb_diag,
            status=status,
            gate_reason="layer_a_membership_twin" if args.honor_production_membership else "inception_research",
            in_production_book=False,
        )
        daily = shard.get("daily") or {}
        if not daily.get("dates") or len(daily["dates"]) < 2:
            print(f"skip {etf}: status={status} insufficient path", file=sys.stderr)
            continue
        history_basis = "layer_a_membership_twin" if args.honor_production_membership else "inception_research"
        out = {
            "schema": "bucket4_layer_a_twin.v1" if args.honor_production_membership else "bucket4_inception_research.v1",
            "etf": etf,
            "underlying": shard.get("underlying") or row.get("Underlying"),
            "authoritative": False,
            "history_basis": history_basis,
            "disclaimer": LAYER_A_DISCLAIMER if args.honor_production_membership else DISCLAIMER,
            "daily": daily,
            "summary": {
                **(shard.get("summary") or {}),
                "history_basis": history_basis,
                "etf_inception_date": daily["dates"][0],
            },
            "rebalance_log": shard.get("rebalance_log") or [],
            "notional_basis_usd": 1.0,
            "ledger_mode": "unit_capital",
            "etf_inception_date": daily["dates"][0],
        }
        if membership_meta is not None:
            out["membership"] = membership_meta
        path = args.out_dir / f"{etf}.json"
        path.write_text(json.dumps(out, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
        print(f"wrote {path} ({len(daily['dates'])} days from {daily['dates'][0]})")
        written += 1

        # Optionally nest onto the local production shard for immediate UI toggle.
        if args.no_nest or args.honor_production_membership:
            continue
        prod_path = REPO / "data" / "bucket4_pairs" / f"{etf}.json"
        if prod_path.is_file():
            prod = json.loads(prod_path.read_text(encoding="utf-8"))
            if prod.get("in_production_book") or prod.get("schema") == "bucket4_production_pair.v1":
                prod["inception_research"] = {
                    k: out[k]
                    for k in (
                        "schema", "etf", "underlying", "authoritative", "history_basis",
                        "disclaimer", "daily", "summary", "rebalance_log",
                        "notional_basis_usd", "ledger_mode", "etf_inception_date",
                    )
                }
                prod_path.write_text(
                    json.dumps(prod, indent=2, sort_keys=True, allow_nan=False) + "\n",
                    encoding="utf-8",
                )
                print(f"nested inception_research onto {prod_path.name}")

    print(json.dumps({"ok": written > 0, "written": written, "out_dir": str(args.out_dir.resolve())}))
    return 0 if written else 1


if __name__ == "__main__":
    raise SystemExit(main())

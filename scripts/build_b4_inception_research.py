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
from bucket4.policy_helpers import load_policy  # noqa: E402

DISCLAIMER = (
    "Research path from ETF/underlying overlap. Not production-policy replay. "
    "Not used for book PnL."
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
        try:
            bt, h_daily, rb_diag, status = b4.run_pair_backtest_for_row(
                row,
                panel,
                policy,
                start=args.start,
                min_days=args.min_days,
                warmup_bdays=min(warmup_bdays, max(0, args.min_days - 1)),
                signal_window=min(signal_window, max(5, args.min_days)),
                vol_history=vol_history,
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
            gate_reason="inception_research",
            in_production_book=False,
        )
        daily = shard.get("daily") or {}
        if not daily.get("dates") or len(daily["dates"]) < 2:
            print(f"skip {etf}: status={status} insufficient path", file=sys.stderr)
            continue
        out = {
            "schema": "bucket4_inception_research.v1",
            "etf": etf,
            "underlying": shard.get("underlying") or row.get("Underlying"),
            "authoritative": False,
            "history_basis": "inception_research",
            "disclaimer": DISCLAIMER,
            "daily": daily,
            "summary": {
                **(shard.get("summary") or {}),
                "history_basis": "inception_research",
                "etf_inception_date": daily["dates"][0],
            },
            "rebalance_log": shard.get("rebalance_log") or [],
            "notional_basis_usd": 1.0,
            "ledger_mode": "unit_capital",
            "etf_inception_date": daily["dates"][0],
        }
        path = args.out_dir / f"{etf}.json"
        path.write_text(json.dumps(out, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
        print(f"wrote {path} ({len(daily['dates'])} days from {daily['dates'][0]})")
        written += 1

        # Optionally nest onto the local production shard for immediate UI toggle.
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

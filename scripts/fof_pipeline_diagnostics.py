#!/usr/bin/env python3
"""CI diagnostics for YieldBOOST FoF (YBTY/YBST) pipeline readiness."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parent
_REPO = _SCRIPTS.parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from yieldboost_fof_constants import FOF_HOLDINGS_LATEST_JSON, YIELDBOOST_FOF_SYMBOLS

DATA_DIR = _REPO / "data"
METRICS_CSV = DATA_DIR / "etf_metrics_daily.csv"
DASHBOARD_JSON = DATA_DIR / "dashboard_data.json"
MIN_METRICS_ROWS = 20


def run_diagnostics(
    *,
    require_dashboard: bool = False,
    min_rows: int = MIN_METRICS_ROWS,
) -> dict:
    issues: list[str] = []
    report: dict = {"symbols": {}, "ok": True}

    # Metrics store
    metrics_rows: dict[str, int] = {s: 0 for s in YIELDBOOST_FOF_SYMBOLS}
    if METRICS_CSV.exists():
        import pandas as pd

        df = pd.read_csv(METRICS_CSV, usecols=["ticker"])
        for sym in YIELDBOOST_FOF_SYMBOLS:
            metrics_rows[sym] = int((df["ticker"].astype(str).str.upper() == sym).sum())
    else:
        issues.append(f"missing metrics file: {METRICS_CSV}")

    for sym, n in metrics_rows.items():
        entry = {"metrics_rows": n}
        if n < min_rows:
            issues.append(f"{sym}: only {n} metrics rows (need >= {min_rows})")
            entry["metrics_ok"] = False
        else:
            entry["metrics_ok"] = True
        report["symbols"][sym] = entry

    # Holdings latest
    hold_path = DATA_DIR / Path(FOF_HOLDINGS_LATEST_JSON).name
    if hold_path.exists():
        payload = json.loads(hold_path.read_text(encoding="utf-8"))
        latest = payload.get("latest") or {}
        for sym in YIELDBOOST_FOF_SYMBOLS:
            snap = latest.get(sym)
            report["symbols"].setdefault(sym, {})["holdings_ok"] = bool(snap and snap.get("n_children"))
            if not snap:
                issues.append(f"{sym}: missing from yieldboost_fof_holdings_latest.json")
    else:
        issues.append(f"missing {hold_path}")

    # Dashboard synthetic rows
    if DASHBOARD_JSON.exists():
        payload = json.loads(DASHBOARD_JSON.read_text(encoding="utf-8"))
        records = payload.get("records") or []
        for sym in YIELDBOOST_FOF_SYMBOLS:
            rec = next((r for r in records if str(r.get("symbol")).upper() == sym), None)
            if not rec:
                issues.append(f"{sym}: missing dashboard record")
                continue
            rp = rec.get("fof_realized_pair") or {}
            ok = bool(rp.get("ok"))
            report["symbols"].setdefault(sym, {})["fof_realized_pair_ok"] = ok
            report["symbols"][sym]["gross_decay_annual"] = rec.get("gross_decay_annual")
            if require_dashboard and not ok:
                issues.append(f"{sym}: fof_realized_pair not ok ({rp.get('error')})")
            if require_dashboard and rec.get("gross_decay_annual") is None:
                issues.append(f"{sym}: gross_decay_annual null on dashboard")
    elif require_dashboard:
        issues.append(f"missing {DASHBOARD_JSON}")

    report["issues"] = issues
    report["ok"] = len(issues) == 0
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="FoF pipeline diagnostics")
    parser.add_argument("--require-dashboard", action="store_true")
    parser.add_argument("--fail", action="store_true", help="Exit 1 when issues found")
    parser.add_argument("--min-rows", type=int, default=MIN_METRICS_ROWS)
    args = parser.parse_args()

    report = run_diagnostics(require_dashboard=args.require_dashboard, min_rows=args.min_rows)
    print(json.dumps(report, indent=2))
    if args.fail and not report["ok"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

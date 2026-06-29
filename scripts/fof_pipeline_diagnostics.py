#!/usr/bin/env python3
"""CI gate: YieldBOOST FoF (YBTY/YBST) metrics coverage."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA = REPO_ROOT / "data"
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from product_taxonomy import yieldboost_fof_symbols  # noqa: E402

MIN_NAV_ROWS = 20


def _load_metrics(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet" and path.exists():
        df = pd.read_parquet(path)
    elif (DATA / "etf_metrics_daily.csv").exists():
        df = pd.read_csv(DATA / "etf_metrics_daily.csv")
    else:
        return pd.DataFrame()
    if "ticker" not in df.columns:
        return pd.DataFrame()
    df["ticker"] = df["ticker"].astype(str).str.upper()
    if "date" in df.columns:
        df["date"] = df["date"].astype(str).str.slice(0, 10)
    return df


def check_fof_metrics(metrics_path: Path, *, min_rows: int) -> tuple[list[str], dict]:
    errors: list[str] = []
    report: dict[str, object] = {"symbols": {}, "min_rows": min_rows}
    df = _load_metrics(metrics_path)
    if df.empty:
        return [f"FoF diagnostics: no metrics at {metrics_path.name}"], report

    for sym in sorted(yieldboost_fof_symbols()):
        sub = df[df["ticker"] == sym]
        nav_col = None
        for col in ("nav", "etf_adj_close", "close_price"):
            if col in sub.columns and sub[col].notna().any():
                nav_col = col
                break
        if nav_col:
            nav_rows = int(sub[sub[nav_col].notna() & (pd.to_numeric(sub[nav_col], errors="coerce") > 0)].shape[0])
        else:
            nav_rows = 0
        latest_date = str(sub["date"].max()) if not sub.empty and "date" in sub.columns else None
        report["symbols"][sym] = {"nav_rows": nav_rows, "latest_date": latest_date, "nav_column": nav_col}
        if nav_rows < min_rows:
            errors.append(f"FoF {sym}: only {nav_rows} NAV rows (need >={min_rows})")

    dash_path = DATA / "dashboard_data.json"
    if dash_path.exists():
        try:
            dash = json.loads(dash_path.read_text(encoding="utf-8"))
            for row in dash.get("records") or []:
                if str(row.get("symbol") or "").upper() not in yieldboost_fof_symbols():
                    continue
                sym = str(row["symbol"]).upper()
                fof_pair = (row.get("fof_realized_pair") or {})
                if isinstance(fof_pair, dict) and fof_pair.get("ok") is not True:
                    errors.append(f"FoF {sym}: fof_realized_pair not ok in dashboard_data.json")
                gd = row.get("gross_decay_annual")
                if gd is None or not isinstance(gd, (int, float)):
                    errors.append(f"FoF {sym}: gross_decay_annual missing in dashboard")
        except Exception as exc:
            errors.append(f"FoF dashboard read failed: {exc}")

    return errors, report


def main() -> int:
    parser = argparse.ArgumentParser(description="FoF pipeline diagnostics")
    parser.add_argument("--metrics", type=Path, default=DATA / "etf_metrics_daily.parquet")
    parser.add_argument("--min-rows", type=int, default=MIN_NAV_ROWS)
    parser.add_argument("--report", type=Path, default=None)
    args = parser.parse_args()

    errors, report = check_fof_metrics(args.metrics, min_rows=args.min_rows)
    report["ok"] = not errors
    report["errors"] = errors
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2), encoding="utf-8")

    if errors:
        for e in errors:
            print(f"[FAIL] {e}", file=sys.stderr)
        return 1
    print("FoF pipeline OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

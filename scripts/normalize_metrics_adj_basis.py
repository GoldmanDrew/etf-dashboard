#!/usr/bin/env python3
"""Apply adj-basis-switch ``etf_adj_close`` normalization to metrics parquet."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

from ingest_etf_metrics import normalize_adj_basis_switch_etf_adj_close  # noqa: E402

DEFAULT_METRICS = SCRIPTS.parent / "data" / "etf_metrics_daily.parquet"
DEFAULT_CORP = SCRIPTS.parent / "data" / "corporate_actions.json"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--metrics", type=Path, default=DEFAULT_METRICS)
    parser.add_argument("--corp-actions", type=Path, default=DEFAULT_CORP)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not args.metrics.exists():
        print(f"metrics missing: {args.metrics}", file=sys.stderr)
        return 1

    df = pd.read_parquet(args.metrics)
    before = df["etf_adj_close"].copy() if "etf_adj_close" in df.columns else None
    out = normalize_adj_basis_switch_etf_adj_close(
        df,
        corporate_actions_path=args.corp_actions,
    )
    changed = 0
    if before is not None and "etf_adj_close" in out.columns:
        changed = int((before != out["etf_adj_close"]).sum())
    print(f"[normalize] {changed} etf_adj_close cell(s) updated")
    if args.dry_run:
        return 0
    out.to_parquet(args.metrics, index=False)
    print(f"[normalize] wrote {args.metrics}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

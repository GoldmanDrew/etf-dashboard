#!/usr/bin/env python3
"""Rebuild ``etf_adj_close`` rows left on a pre-split basis on the metrics parquet.

The ingest applies this repair on every run; this entrypoint heals history that was
already persisted by an earlier build (the 2026-07 forward-split cohort: CRDU, GEVX,
KORU, LABX, MUU, NEBX, SNXX, WDCX).

Usage::

    python scripts/repair_etf_adj_split_basis.py --dry-run
    python scripts/repair_etf_adj_split_basis.py
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

from ingest_etf_metrics import repair_pre_split_basis_etf_adj_close  # noqa: E402

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
    n_rows_before = len(df)
    out, n_repaired = repair_pre_split_basis_etf_adj_close(
        df,
        corporate_actions_path=args.corp_actions,
    )
    # Never let a basis repair change the record count.
    if len(out) != n_rows_before:
        print(
            f"row count changed {n_rows_before} -> {len(out)}; refusing to write",
            file=sys.stderr,
        )
        return 1
    print(f"[repair] {n_repaired} etf_adj_close cell(s) rebuilt onto the latest basis")
    if args.dry_run:
        return 0
    if n_repaired == 0:
        print("[repair] nothing to write")
        return 0
    out.to_parquet(args.metrics, index=False)
    print(f"[repair] wrote {args.metrics}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

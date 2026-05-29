#!/usr/bin/env python3
"""Materialize ``data/etf_metrics_daily.json`` from parquet for GitHub Pages deploy.

The canonical store in git is ``etf_metrics_daily.parquet`` (~3 MB). The JSON
panel can exceed GitHub's 100 MB blob limit, so workflows do not commit it;
this script runs in the deploy action before copying ``data/`` to ``_site/``.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from etf_metrics_format import write_metrics_daily_json

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
PARQUET_PATH = DATA_DIR / "etf_metrics_daily.parquet"
JSON_PATH = DATA_DIR / "etf_metrics_daily.json"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet", type=Path, default=PARQUET_PATH)
    parser.add_argument("--json", type=Path, default=JSON_PATH)
    args = parser.parse_args()

    if not args.parquet.is_file():
        raise SystemExit(f"Missing parquet store: {args.parquet}")

    df = pd.read_parquet(args.parquet)
    out = write_metrics_daily_json(df, args.json)
    size_mb = out.stat().st_size / (1024 * 1024)
    print(f"Wrote {out} ({size_mb:.1f} MB, {len(df)} rows)")


if __name__ == "__main__":
    main()

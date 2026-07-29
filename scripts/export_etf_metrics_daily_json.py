#!/usr/bin/env python3
"""Materialize browser metrics JSON (+ gzip) from parquet for Cloudflare Pages.

The canonical store in git is ``etf_metrics_daily.parquet``. Plain JSON can
exceed Cloudflare's ~25 MiB upload limit, so deploy ships
``etf_metrics_daily.json.gz`` (and optionally the uncompressed file for local
dev). This script runs in the deploy action before copying ``data/`` to
``_site/``.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

SCRIPTS_DIR = Path(__file__).resolve().parent
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from etf_metrics_format import gzip_json_file, write_metrics_daily_json

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
PARQUET_PATH = DATA_DIR / "etf_metrics_daily.parquet"
JSON_PATH = DATA_DIR / "etf_metrics_daily.json"
VOL_SHAPE_JSON = DATA_DIR / "vol_shape_history.json"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet", type=Path, default=PARQUET_PATH)
    parser.add_argument("--json", type=Path, default=JSON_PATH)
    parser.add_argument(
        "--skip-plain-json",
        action="store_true",
        help="Only write .json.gz (skip the large uncompressed panel)",
    )
    parser.add_argument(
        "--vol-shape",
        type=Path,
        default=VOL_SHAPE_JSON,
        help="Also gzip this vol-shape history JSON when present",
    )
    args = parser.parse_args()

    if not args.parquet.is_file():
        raise SystemExit(f"Missing parquet store: {args.parquet}")

    df = pd.read_parquet(args.parquet)
    if args.skip_plain_json:
        from etf_metrics_format import metrics_daily_payload, write_gzip_json

        gz_path = Path(str(args.json) + ".gz")
        write_gzip_json(metrics_daily_payload(df), gz_path)
        gz_mb = gz_path.stat().st_size / (1024 * 1024)
        print(f"Wrote {gz_path} ({gz_mb:.1f} MB gzip, {len(df)} parquet rows)")
    else:
        out = write_metrics_daily_json(df, args.json, also_gzip=True)
        size_mb = out.stat().st_size / (1024 * 1024)
        gz = Path(str(out) + ".gz")
        gz_mb = gz.stat().st_size / (1024 * 1024) if gz.is_file() else 0.0
        print(f"Wrote {out} ({size_mb:.1f} MB plain, {gz_mb:.1f} MB gzip, {len(df)} parquet rows)")

    if args.vol_shape.is_file():
        vz = gzip_json_file(args.vol_shape)
        print(f"Wrote {vz} ({vz.stat().st_size / (1024 * 1024):.1f} MB gzip)")
    else:
        print(f"No {args.vol_shape.name}; skipping vol-shape gzip")


if __name__ == "__main__":
    main()

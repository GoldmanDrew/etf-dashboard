#!/usr/bin/env python3
"""
Options cache diagnostics and quality gate.

Usage:
  python scripts/options_cache_diagnostics.py --path data/options_cache.json
  python scripts/options_cache_diagnostics.py --path data/options_cache.json --fail-when-both-zero
"""
from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from pathlib import Path


def _is_num(v) -> bool:
    return isinstance(v, (int, float)) and math.isfinite(v)


def main() -> int:
    parser = argparse.ArgumentParser(description="Inspect options cache field coverage")
    parser.add_argument("--path", default="data/options_cache.json", help="Path to options_cache.json")
    parser.add_argument(
        "--fail-when-both-zero",
        action="store_true",
        help="Exit non-zero when both mid_nonnull and iv_nonnull are zero",
    )
    args = parser.parse_args()

    p = Path(args.path)
    if not p.exists():
        raise SystemExit(f"options cache file not found: {p}")

    data = json.loads(p.read_text(encoding="utf-8"))
    symbols = data.get("symbols") or {}
    by_source = Counter()
    rows_total = 0
    mid_nonnull = 0
    iv_nonnull = 0

    for payload in symbols.values():
        if not isinstance(payload, dict):
            continue
        source = str(payload.get("source") or "unknown")
        rows = payload.get("options") or []
        by_source[source] += len(rows)
        for r in rows:
            rows_total += 1
            if _is_num(r.get("mid")) and float(r["mid"]) > 0:
                mid_nonnull += 1
            if _is_num(r.get("iv")) and float(r["iv"]) > 0:
                iv_nonnull += 1

    coverage = data.get("option_field_coverage") or {}
    if not coverage:
        coverage = {
            "rows_total": rows_total,
            "mid_nonnull": mid_nonnull,
            "iv_nonnull": iv_nonnull,
        }

    rows_total = int(coverage.get("rows_total") or 0)
    mid_nonnull = int(coverage.get("mid_nonnull") or 0)
    iv_nonnull = int(coverage.get("iv_nonnull") or 0)
    mid_ratio = (mid_nonnull / rows_total) if rows_total > 0 else 0.0
    iv_ratio = (iv_nonnull / rows_total) if rows_total > 0 else 0.0

    print("option_field_coverage:", coverage)
    print(
        "option_field_ratios:",
        {
            "mid_ratio": round(mid_ratio, 4),
            "iv_ratio": round(iv_ratio, 4),
        },
    )
    print("rows_by_source:", dict(by_source))

    errs = data.get("errors_by_symbol") or {}
    if errs:
        sample = list(errs.items())[:8]
        print("sample_errors_by_symbol:", sample)

    if args.fail_when_both_zero and rows_total > 0 and mid_nonnull == 0 and iv_nonnull == 0:
        print(
            "FATAL: option quotes unusable (both mid_nonnull and iv_nonnull are zero). "
            "Check provider entitlements/limits and parser mappings."
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


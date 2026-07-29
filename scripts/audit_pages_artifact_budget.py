#!/usr/bin/env python3
"""Fail closed if Cloudflare Pages ``_site`` is missing required artifacts or over budget.

Cloudflare Pages rejects individual files above ~25 MiB; deploy uses a 20 MB
safety margin (``find -size -20M``). Oversized panels (metrics, vol-shape) must
ship as ``.json.gz``.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DEFAULT_SITE = REPO / "_site"
# Keep in sync with .github/actions/deploy-pages/action.yml find -size -20M
MAX_BYTES = 20 * 1024 * 1024

REQUIRED_REL_PATHS = (
    "data/etf_metrics_daily.json.gz",
    "data/vol_shape_history.json.gz",
    "data/dashboard_data.json",
    "index.html",
)


def audit_site(site: Path, *, max_bytes: int = MAX_BYTES) -> list[str]:
    errors: list[str] = []
    if not site.is_dir():
        return [f"missing site dir: {site}"]
    for rel in REQUIRED_REL_PATHS:
        path = site / rel
        if not path.is_file():
            errors.append(f"missing required artifact: {rel}")
            continue
        size = path.stat().st_size
        if size >= max_bytes:
            errors.append(
                f"over budget ({size / (1024 * 1024):.1f} MB >= {max_bytes / (1024 * 1024):.0f} MB): {rel}"
            )
        elif size <= 0:
            errors.append(f"empty artifact: {rel}")
    # Plain oversized panels must not sneak into _site
    for banned in (
        "data/etf_metrics_daily.json",
        "data/etf_metrics_daily.csv",
    ):
        p = site / banned
        if p.is_file() and p.stat().st_size >= max_bytes:
            errors.append(f"banned oversized plain file in _site: {banned}")
    return errors


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--site", type=Path, default=DEFAULT_SITE)
    ap.add_argument("--max-mb", type=float, default=20.0)
    args = ap.parse_args(argv)
    max_bytes = int(args.max_mb * 1024 * 1024)
    errors = audit_site(args.site, max_bytes=max_bytes)
    if errors:
        for e in errors:
            print(f"FAIL: {e}", file=sys.stderr)
        return 1
    print(f"OK: required Pages artifacts present under {args.site} (budget {args.max_mb:.0f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

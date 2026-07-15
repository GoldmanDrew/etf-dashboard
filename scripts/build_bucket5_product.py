#!/usr/bin/env python3
"""
Build / sync Bucket 5 product dashboard JSON into etf-dashboard.

Prefers copying from ls-algo sibling after running the product builder there.
Falls back to invoking ls-algo's builder in-process.

Usage::

    python scripts/build_bucket5_product.py
    python scripts/build_bucket5_product.py --copy-ls-algo-panel
    python scripts/build_bucket5_product.py --quick
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / "data" / "bucket5_product.json"
LEGACY = REPO / "data" / "bucket5_insurance_backtest.json"


def find_ls_algo() -> Path | None:
    for p in (
        REPO.parent / "ls-algo",
        Path.home() / "Projects" / "quant" / "ls-algo",
        REPO / "ls-algo",
    ):
        if (p / "scripts" / "build_bucket5_product_dashboard.py").is_file():
            return p
    return None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--copy-ls-algo-panel",
        action="store_true",
        help="Copy existing risk_dashboard/data/bucket5_product.json from ls-algo",
    )
    ap.add_argument("--quick", action="store_true")
    args = ap.parse_args(argv)

    ls = find_ls_algo()
    if not ls:
        print("ls-algo checkout not found (need scripts/build_bucket5_product_dashboard.py)", file=sys.stderr)
        return 1

    def _sync_ui() -> None:
        pairs = [
            (ls / "site" / "assets" / "js" / "bucket5_product.js", REPO / "assets" / "bucket5_product.js"),
            (ls / "site" / "assets" / "css" / "bucket5_product.css", REPO / "assets" / "bucket5_product.css"),
        ]
        for s, d in pairs:
            if s.is_file():
                d.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(s, d)
                print(f"synced UI {s.name} -> {d}")

    src = ls / "risk_dashboard" / "data" / "bucket5_product.json"
    if args.copy_ls_algo_panel and src.is_file():
        OUT.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, OUT)
        shutil.copy2(src, LEGACY)
        print(f"copied {src} -> {OUT}")
        _sync_ui()
        return 0

    cmd = [sys.executable, str(ls / "scripts" / "build_bucket5_product_dashboard.py"), "--copy-etf-dashboard"]
    if args.quick:
        cmd.append("--quick")
    print("running:", " ".join(cmd))
    r = subprocess.run(cmd, cwd=str(ls))
    if r.returncode != 0:
        return r.returncode
    if not OUT.is_file():
        print(f"expected {OUT} missing after build", file=sys.stderr)
        return 1
    _sync_ui()
    print(f"ok {OUT} ({OUT.stat().st_size / 1e6:.1f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

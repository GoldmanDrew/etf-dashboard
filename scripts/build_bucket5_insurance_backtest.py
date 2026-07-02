#!/usr/bin/env python3
"""Build Bucket 5 insurance backtest JSON for etf-dashboard (and ls-algo risk dashboard).

Prefers sibling ls-algo checkout; falls back to importing from PYTHONPATH.

Usage::

    python scripts/build_bucket5_insurance_backtest.py
    python scripts/build_bucket5_insurance_backtest.py --copy-ls-algo-panel
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
OUT = REPO / "data" / "bucket5_insurance_backtest.json"
LS_ALGO_CANDIDATES = [
    REPO.parent / "ls-algo",
    Path.home() / "Projects" / "quant" / "ls-algo",
]


def _find_ls_algo() -> Path | None:
    for p in LS_ALGO_CANDIDATES:
        if (p / "scripts" / "bucket5_backtest_api.py").is_file():
            return p
    return None


def _build_via_subprocess(ls_algo: Path) -> dict:
    cmd = [
        sys.executable,
        str(ls_algo / "scripts" / "bucket5_backtest_api.py"),
        "dashboard",
    ]
    subprocess.check_call(cmd, cwd=ls_algo)
    src = ls_algo / "risk_dashboard" / "data" / "bucket5_backtest.json"
    return json.loads(src.read_text(encoding="utf-8"))


def _build_inline() -> dict:
    ls = _find_ls_algo()
    if ls is None:
        raise SystemExit("ls-algo not found; clone sibling repo or set PYTHONPATH")
    if str(ls) not in sys.path:
        sys.path.insert(0, str(ls))
    from scripts.bucket5_backtest_api import build_dashboard_payload  # noqa: E402

    return build_dashboard_payload()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--copy-ls-algo-panel", action="store_true")
    args = ap.parse_args(argv)

    ls = _find_ls_algo()
    if ls and (ls / "scripts" / "bucket5_backtest_api.py").is_file():
        try:
            payload = _build_via_subprocess(ls)
        except Exception:
            payload = _build_inline()
    else:
        payload = _build_inline()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"[etf-dashboard b5] wrote {OUT}")

    if args.copy_ls_algo_panel and ls:
        dest = ls / "risk_dashboard" / "data" / "bucket5_backtest.json"
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(OUT, dest)
        print(f"[etf-dashboard b5] synced -> {dest}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

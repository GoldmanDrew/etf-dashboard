#!/usr/bin/env python3
"""Compatibility wrapper — delegates to build_bucket5_product.py."""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "scripts"))

from build_bucket5_product import main  # noqa: E402

if __name__ == "__main__":
    # Map legacy flag names
    argv = list(sys.argv[1:])
    raise SystemExit(main(argv))

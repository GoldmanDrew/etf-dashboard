#!/usr/bin/env python3
"""Build data/bucket1_underlying_views.json from config + dashboard rows."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

from bucket1_underlying_views import build_payload, load_config  # noqa: E402

DEFAULT_OUT = ROOT / "data" / "bucket1_underlying_views.json"
DEFAULT_DASH = ROOT / "data" / "dashboard_data.json"


def _load_records(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    rows = payload.get("records") or payload.get("rows") or []
    return [r for r in rows if isinstance(r, dict)]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", type=Path, default=None, help="YAML config path")
    p.add_argument("--dashboard", type=Path, default=DEFAULT_DASH, help="dashboard_data.json")
    p.add_argument("--out", type=Path, default=DEFAULT_OUT, help="output JSON path")
    args = p.parse_args(argv)

    cfg = load_config(args.config)
    records = _load_records(args.dashboard)
    payload = build_payload(config=cfg, records=records)
    payload["generated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    payload["dashboard_source"] = str(args.dashboard) if args.dashboard.is_file() else None

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    print(
        f"wrote {args.out} · underlyings={payload['n_underlyings']} "
        f"b1={payload['n_bucket1_underlyings']} active={payload['n_active_views']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

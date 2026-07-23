#!/usr/bin/env python3
"""Inject the research comparison JSON into the literal visualization template."""
from __future__ import annotations

import argparse
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("template", type=Path)
    ap.add_argument("data", type=Path)
    ap.add_argument("output", type=Path)
    args = ap.parse_args()
    template = args.template.read_text(encoding="utf-8")
    data = args.data.read_text(encoding="utf-8").strip().replace("</script", "<\\/script")
    if template.count("__VIS_DATA__") != 1:
        raise SystemExit("template must contain exactly one __VIS_DATA__ marker")
    rendered = template.replace("__VIS_DATA__", data)
    args.output.write_text(rendered, encoding="utf-8")
    print(args.output.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

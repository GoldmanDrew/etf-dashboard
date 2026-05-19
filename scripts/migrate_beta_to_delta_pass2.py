#!/usr/bin/env python3
"""Second-pass fixes after initial Beta→Delta migration."""
from __future__ import annotations

import re
from pathlib import Path

SKIP_FILES = {"migrate_beta_to_delta.py", "migrate_beta_to_delta_pass2.py", "beta_loader.py", "factor_map.py"}


def fix_py(text: str) -> str:
    text = text.replace('row["Beta"]', 'row["Delta"]')
    text = text.replace("row.get('Beta')", "row.get('Delta')")
    text = text.replace('row.get("Beta")', 'row.get("Delta")')
    text = text.replace('["Beta"]', '["Delta"]')
    text = text.replace("'Beta'", "'Delta'")
    text = text.replace('df["Beta"]', 'df["Delta"]')
    text = text.replace('out.get("Beta")', 'out.get("Delta")')
    text = text.replace("res.beta", "res.delta")
    text = text.replace("result.beta", "result.delta")
    text = re.sub(r"ETF,Underlying,Beta,", "ETF,Underlying,Delta,", text)
    text = re.sub(r"ETF,Underlying,Beta\n", "ETF,Underlying,Delta\n", text)
    text = re.sub(r'"beta":\s*beta\b', '"delta": delta', text)
    text = re.sub(r'"beta":\s*float\(', '"delta": float(', text)
    text = re.sub(
        r'cols\.get\("beta"\)',
        'cols.get("delta") or cols.get("beta")',
        text,
    )
    text = re.sub(
        r'cols_lc\.get\("beta"\)',
        'cols_lc.get("delta") or cols_lc.get("beta")',
        text,
    )
    text = re.sub(
        r'cl\.get\("beta"\)',
        'cl.get("delta") or cl.get("beta")',
        text,
    )
    text = re.sub(
        r'cols_lower\.get\("beta"\)',
        'cols_lower.get("delta") or cols_lower.get("beta")',
        text,
    )
    text = text.replace('cands["beta"]', 'cands["delta"]')
    text = text.replace('preview_df["beta"]', 'preview_df["delta"]')
    text = text.replace('p["beta"]', 'p["delta"]')
    text = text.replace('"beta": beta,', '"delta": delta,')
    text = text.replace('row["beta"]', 'row["delta"]')
    return text


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    for p in root.rglob("*.py"):
        if any(x in p.parts for x in ("__pycache__", ".git")):
            continue
        if p.name in SKIP_FILES or "risk_dashboard/beta_loader" in str(p).replace("\\", "/"):
            continue
        t = p.read_text(encoding="utf-8")
        n = fix_py(t)
        if n != t:
            p.write_text(n, encoding="utf-8")
            print("fixed", p.relative_to(root))


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
from pathlib import Path

root = Path(__file__).resolve().parents[1] / "backend"
main = (root / "main.py").read_text(encoding="utf-8")
main = main.replace(
    'beta=float(row["Beta"]) if not _isnan(row.get("Beta")) else None,',
    'delta=float(row["Delta"]) if not _isnan(row.get("Delta")) else None,',
)
main = main.replace(
    'beta_n_obs=int(row["Beta_n_obs"]) if not _isnan(row.get("Beta_n_obs")) else None,',
    'delta_n_obs=int(row["Delta_n_obs"]) if not _isnan(row.get("Delta_n_obs")) else None,',
)
(root / "main.py").write_text(main, encoding="utf-8")
print("fixed backend/main.py")

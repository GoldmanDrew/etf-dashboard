"""Schema pass-through and routing helpers for Bucket 4 dashboard fields."""

from __future__ import annotations

import json
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def test_build_data_exports_bucket4_fields():
    src = (REPO / "scripts" / "build_data.py").read_text(encoding="utf-8")
    for field in (
        "screener_bucket",
        "bucket4_net_edge_annual",
        "init_pct_short",
        "maint_pct_short",
        "inverse_shortable",
        "purgatory",
        "purgatory_net_edge",
    ):
        assert f'"{field}"' in src, f"build_data.py missing {field}"


def test_backend_models_declare_bucket4_fields():
    src = (REPO / "backend" / "models.py").read_text(encoding="utf-8")
    for field in (
        "screener_bucket",
        "bucket4_net_edge_annual",
        "init_pct_short",
        "maint_pct_short",
        "inverse_shortable",
        "purgatory",
    ):
        assert field in src, f"backend/models.py missing {field}"


def test_bucket4_artifact_schema_when_present():
    path = REPO / "data" / "bucket4_backtest.json"
    if not path.is_file():
        return
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload.get("schema") == "bucket4_backtest.v1"
    assert isinstance(payload.get("pairs"), list)
    assert payload.get("n_pairs", 0) == len(payload["pairs"])
    assert isinstance(payload.get("sim_dates"), list)
    assert isinstance(payload.get("port_equity"), list)
    assert len(payload["sim_dates"]) == len(payload["port_equity"])


def test_index_html_wires_bucket4_module():
    html = (REPO / "index.html").read_text(encoding="utf-8")
    assert "assets/bucket4_backtest.js" in html
    assert "isBucket4Record" in html
    assert re.search(r"bucket-4/backtest", html)

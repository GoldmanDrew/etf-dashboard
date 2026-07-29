"""Tests for Cloudflare Pages artifact budget gate."""
from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SCRIPTS = REPO / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from audit_pages_artifact_budget import audit_site  # noqa: E402


def test_audit_site_requires_gzip_metrics(tmp_path: Path):
    site = tmp_path / "_site"
    (site / "data").mkdir(parents=True)
    (site / "index.html").write_text("<html></html>", encoding="utf-8")
    (site / "data" / "dashboard_data.json").write_text("{}", encoding="utf-8")
    errors = audit_site(site)
    assert any("etf_metrics_daily.json.gz" in e for e in errors)
    assert any("vol_shape_history.json.gz" in e for e in errors)

    (site / "data" / "etf_metrics_daily.json.gz").write_bytes(b"x" * 100)
    (site / "data" / "vol_shape_history.json.gz").write_bytes(b"y" * 100)
    assert audit_site(site) == []


def test_audit_site_rejects_oversize(tmp_path: Path):
    site = tmp_path / "_site"
    (site / "data").mkdir(parents=True)
    (site / "index.html").write_text("ok", encoding="utf-8")
    (site / "data" / "dashboard_data.json").write_text("{}", encoding="utf-8")
    big = site / "data" / "etf_metrics_daily.json.gz"
    big.write_bytes(b"z" * (20 * 1024 * 1024 + 1))
    (site / "data" / "vol_shape_history.json.gz").write_bytes(b"y")
    errors = audit_site(site)
    assert any("over budget" in e for e in errors)

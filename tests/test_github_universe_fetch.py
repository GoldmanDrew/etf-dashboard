"""Tests for GitHub universe CSV fetching."""
from __future__ import annotations

import base64
import sys
from pathlib import Path

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import build_data as bd  # noqa: E402


class _Resp:
    def __init__(self, status_code: int, text: str = "", payload: dict | None = None):
        self.status_code = status_code
        self.text = text
        self._payload = payload or {}
        self.ok = 200 <= status_code < 300

    def raise_for_status(self) -> None:
        if not self.ok:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self) -> dict:
        return self._payload


def test_fetch_csv_from_github_falls_back_to_contents_api(tmp_path, monkeypatch):
    csv_text = "ETF,Underlying,Delta\nTQQQ,QQQ,3.0\n"
    encoded = base64.b64encode(csv_text.encode("utf-8")).decode("ascii")
    calls: list[str] = []

    def fake_get(url, **kwargs):
        calls.append(url)
        if "raw.githubusercontent.com" in url:
            return _Resp(404, "Not Found")
        return _Resp(
            200,
            payload={
                "encoding": "base64",
                "content": encoded,
            },
        )

    monkeypatch.setattr(bd, "OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(bd.requests, "get", fake_get)

    df = bd.fetch_csv_from_github()

    assert list(df["ETF"]) == ["TQQQ"]
    assert (tmp_path / "etf_screened_today.csv").read_text(encoding="utf-8") == csv_text
    assert any("raw.githubusercontent.com" in url for url in calls)
    assert any("api.github.com/repos" in url for url in calls)

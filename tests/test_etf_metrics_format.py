from __future__ import annotations

import gzip
import json
from datetime import date
from pathlib import Path
import sys

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from etf_metrics_format import (  # noqa: E402
    browser_metrics_frame,
    metrics_daily_payload,
    prune_browser_audit_columns,
    write_gzip_json,
    write_metrics_daily_json,
)


def test_browser_metrics_frame_excludes_non_sessions_and_issuer_early():
    df = pd.DataFrame(
        [
            {"date": date(2026, 6, 18), "ticker": "AAA", "stale_kind": None},
            {"date": date(2026, 6, 19), "ticker": "AAA", "stale_kind": None},
            {"date": date(2026, 6, 24), "ticker": "AAPU", "stale_kind": "issuer_early"},
        ]
    )
    out = browser_metrics_frame(df)
    assert out["date"].astype(str).tolist() == ["2026-06-18"]


def test_prune_browser_audit_columns_drops_source_url_and_ingested_at():
    df = pd.DataFrame(
        [
            {
                "date": "2026-06-18",
                "ticker": "AAA",
                "nav": 10.0,
                "source_url": "https://example/x",
                "ingested_at_utc": "2026-06-18T12:00:00+00:00",
            }
        ]
    )
    out = prune_browser_audit_columns(df)
    assert "source_url" not in out.columns
    assert "ingested_at_utc" not in out.columns
    assert "nav" in out.columns


def test_metrics_payload_and_gzip_round_trip(tmp_path: Path):
    df = pd.DataFrame(
        [
            {
                "date": date(2026, 6, 18),
                "ticker": "AAA",
                "nav": 10.0,
                "close_price": 10.1,
                "source_url": "https://example/x",
                "ingested_at_utc": "2026-06-18T12:00:00+00:00",
                "stale_kind": None,
            }
        ]
    )
    payload = metrics_daily_payload(df)
    assert "rows" in payload
    assert "source_url" not in payload["rows"][0]
    assert "ingested_at_utc" not in payload["rows"][0]
    gz = tmp_path / "etf_metrics_daily.json.gz"
    write_gzip_json(payload, gz)
    restored = json.loads(gzip.open(gz, "rb").read().decode("utf-8"))
    assert restored["rows"][0]["ticker"] == "AAA"
    plain = tmp_path / "etf_metrics_daily.json"
    write_metrics_daily_json(df, plain, also_gzip=True)
    assert plain.is_file()
    assert Path(str(plain) + ".gz").is_file()

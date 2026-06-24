from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from etf_metrics_format import browser_metrics_frame  # noqa: E402


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

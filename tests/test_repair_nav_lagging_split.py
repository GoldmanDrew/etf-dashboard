"""Regression: repair_nav_lagging_split_basis fixes issuer NAV behind post-split close."""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import ingest_etf_metrics as iem  # noqa: E402


def _row(d: str, *, nav: float, sh: float, close: float, aum: float, ticker: str = "NBIZ") -> dict:
    return {
        "date": d,
        "ticker": ticker,
        "nav": nav,
        "aum": aum,
        "shares_outstanding": sh,
        "shares_traded": None,
        "close_price": close,
        "underlying_adj_close": None,
        "stale": False,
        "stale_age_bdays": None,
        "source_provider": "tradr_axs",
        "source_url": "",
        "ingested_at_utc": "2026-06-05T00:00:00Z",
        "status": "ok",
    }


def test_nbiz_nav_lags_reverse_split_two_sessions(tmp_path: Path):
    """NBIZ Jun 2026: close already 1-for-10 while issuer NAV still pre-split until Jun 3."""
    ca = tmp_path / "ca.json"
    ca.write_text(
        json.dumps(
            {
                "events": [
                    {
                        "type": "reverse_split",
                        "ticker": "NBIZ",
                        "execution_date": "2026-06-03",
                        "ratio_from": 10.0,
                        "ratio_to": 1.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    df = pd.DataFrame(
        [
            _row("2026-05-29", nav=1.2456, sh=12_715_000.0, close=1.255, aum=15_838_032.0),
            _row("2026-06-01", nav=0.8858, sh=36_415_000.0, close=8.600, aum=32_256_959.0),
            _row("2026-06-02", nav=0.9122, sh=3_641_500.0, close=9.100, aum=33_219_360.0),
            _row("2026-06-03", nav=9.7451, sh=2_711_500.0, close=9.760, aum=26_423_661.0),
        ]
    )
    out, n = iem.repair_nav_lagging_split_basis(df, corporate_actions_path=ca)
    assert n >= 2
    jun1 = out.loc[out["date"] == date(2026, 6, 1)].iloc[0]
    jun2 = out.loc[out["date"] == date(2026, 6, 2)].iloc[0]
    assert abs(float(jun1["nav"]) - 8.858) < 0.02
    assert abs(float(jun1["close_price"]) / float(jun1["nav"]) - 1.0) < 0.05
    assert abs(float(jun2["nav"]) - 9.122) < 0.02
    assert abs(float(jun2["close_price"]) / float(jun2["nav"]) - 1.0) < 0.05
    # Idempotent
    again, n2 = iem.repair_nav_lagging_split_basis(out, corporate_actions_path=ca)
    assert n2 == 0
    stamped = iem.stamp_metric_asof_metadata(again)
    for d in (date(2026, 6, 1), date(2026, 6, 2)):
        row = stamped.loc[stamped["date"] == d].iloc[0]
        assert bool(row["premium_discount_eligible"]) is True
        assert row["premium_discount_status"] == "valid"

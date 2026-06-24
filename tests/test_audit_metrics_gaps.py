from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from audit_metrics_gaps import audit_gaps


def test_audit_gaps_separates_expected_issuer_early_market_blanks():
    df = pd.DataFrame(
        [
            {
                "date": date(2026, 6, 24),
                "ticker": "AAPU",
                "status": "ok",
                "stale_kind": "issuer_early",
                "nav": 10.0,
                "aum": 1000.0,
                "shares_outstanding": 100.0,
                "close_price": None,
                "shares_traded": None,
                "etf_adj_close": None,
                "underlying_adj_close": None,
            },
            {
                "date": date(2026, 6, 24),
                "ticker": "AAPL",
                "status": "ok",
                "stale_kind": None,
                "nav": 10.0,
                "aum": 1000.0,
                "shares_outstanding": 100.0,
                "close_price": None,
                "shares_traded": 1000,
                "etf_adj_close": None,
                "underlying_adj_close": 200.0,
            },
        ]
    )

    report = audit_gaps(df)

    assert report["by_field"]["close_price"]["missing_rows"] == 1
    assert report["by_field"]["close_price"]["expected_missing_rows"] == 1
    assert report["by_field"]["etf_adj_close"]["missing_rows"] == 1
    assert report["by_field"]["etf_adj_close"]["expected_missing_rows"] == 1
    assert report["expected_market_gap_rows_by_kind"] == {"issuer_early": 1}
    assert report["top_tickers_with_gaps"] == {"AAPL": 1}


def test_audit_gaps_can_include_expected_market_blanks():
    df = pd.DataFrame(
        [
            {
                "date": date(2026, 6, 24),
                "ticker": "AAPU",
                "status": "ok",
                "stale_kind": "issuer_early",
                "nav": 10.0,
                "aum": 1000.0,
                "shares_outstanding": 100.0,
                "close_price": None,
                "shares_traded": None,
                "etf_adj_close": None,
                "underlying_adj_close": None,
            }
        ]
    )

    report = audit_gaps(df, include_expected_market_gaps=True)

    assert report["by_field"]["close_price"]["missing_rows"] == 1
    assert report["by_field"]["close_price"]["expected_missing_rows"] == 1
    assert report["top_tickers_with_gaps"] == {"AAPU": 1}

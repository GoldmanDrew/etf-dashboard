from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from repair_rex_session_nav_close import reconcile_rex_nav_identity


def test_reconcile_rewrites_aum_when_nav_diverges_from_shares():
    df = pd.DataFrame(
        [
            {
                "date": date(2026, 9, 18),
                "ticker": "FEPI",
                "source_provider": "rex_shares",
                "nav": 25.10,
                "aum": 2_500_000.0,
                "shares_outstanding": 100_000.0,
            }
        ]
    )
    out, n = reconcile_rex_nav_identity(df)
    assert n == 1
    assert float(out.iloc[0]["nav"]) == 25.10
    assert float(out.iloc[0]["shares_outstanding"]) == 100_000.0
    assert float(out.iloc[0]["aum"]) == 2_510_000.0


def test_reconcile_leaves_a_coherent_triple_alone():
    df = pd.DataFrame(
        [
            {
                "date": date(2026, 9, 18),
                "ticker": "FEPI",
                "source_provider": "rex_shares",
                "nav": 25.0,
                "aum": 2_500_000.0,
                "shares_outstanding": 100_000.0,
            },
            {
                "date": date(2026, 9, 18),
                "ticker": "TQQQ",
                "source_provider": "proshares",
                "nav": 50.0,
                "aum": 1.0,
                "shares_outstanding": 1.0,
            },
        ]
    )
    out, n = reconcile_rex_nav_identity(df)
    assert n == 0
    assert float(out.iloc[0]["aum"]) == 2_500_000.0
    assert float(out.iloc[1]["aum"]) == 1.0

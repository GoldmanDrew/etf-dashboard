"""KLAG/KLAC underlying split normalization regression tests."""
from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from ingest_etf_metrics import repair_underlying_adj_close_split_basis  # noqa: E402
from price_basis import (  # noqa: E402
    build_tr_series_from_metrics,
    find_underlying_adj_cliffs,
    underlying_tr_price_for_row,
)
from realized_gross_decay import compute_gross_decay_annual  # noqa: E402


KLAC_SPLIT = (dt.date(2026, 6, 11), 0.1)  # 10-for-1 forward: rf/rt = 1/10


def _klag_cliff_rows() -> list[dict]:
    return [
        {
            "date": "2026-06-10",
            "close_price": 40.509201,
            "etf_adj_close": 40.509201,
            "underlying_adj_close": 2135.639893,
        },
        {
            "date": "2026-06-11",
            "close_price": 50.907001,
            "etf_adj_close": 50.907001,
            "underlying_adj_close": 241.164001,
        },
        {
            "date": "2026-06-12",
            "close_price": 56.341999,
            "etf_adj_close": 56.341999,
            "underlying_adj_close": 254.539993,
        },
    ]


def test_find_underlying_adj_cliff_without_metadata():
    cliffs = find_underlying_adj_cliffs(_klag_cliff_rows(), [])
    assert len(cliffs) >= 1
    assert str(cliffs[0]["date"]) == "2026-06-11"


def test_find_underlying_adj_cliff_explained_by_klac_split():
    cliffs = find_underlying_adj_cliffs(_klag_cliff_rows(), [KLAC_SPLIT])
    assert cliffs == []


def test_underlying_tr_price_scales_pre_split_onto_latest_basis():
    row = {"date": "2026-06-10", "underlying_adj_close": 2135.639893}
    px = underlying_tr_price_for_row(row, [KLAC_SPLIT])
    assert abs(px - 213.563989) < 0.05


def test_build_tr_series_smooth_underlying_after_split_event():
    tr = build_tr_series_from_metrics(_klag_cliff_rows(), [], underlying_split_events=[KLAC_SPLIT])
    assert len(tr) >= 2
    u0 = tr[0]["tr_und_px"]
    u1 = tr[1]["tr_und_px"]
    assert abs(u0 - u1) / u1 < 0.15


def test_gross_decay_not_poisoned_by_klac_cliff_when_split_known():
    rows = _klag_cliff_rows() + [
        {
            "date": f"2026-06-{d:02d}",
            "close_price": 50.0 + d,
            "etf_adj_close": 50.0 + d,
            "underlying_adj_close": 240.0 + d,
        }
        for d in range(13, 20)
    ]
    bad = compute_gross_decay_annual(rows, 2.0, [], min_obs=2)
    good = compute_gross_decay_annual(
        rows,
        2.0,
        [],
        underlying_split_events=[KLAC_SPLIT],
        min_obs=2,
    )
    assert bad is not None and good is not None
    assert bad["gross_decay_annual"] < -1.0
    assert good["gross_decay_annual"] > bad["gross_decay_annual"]


def test_repair_underlying_adj_close_split_basis_scales_store(tmp_path: Path):
    import pandas as pd

    ca = tmp_path / "corporate_actions.json"
    ca.write_text(
        json.dumps(
            {
                "events": [
                    {
                        "type": "forward_split",
                        "ticker": "KLAC",
                        "execution_date": "2026-06-11",
                        "ratio_from": 1.0,
                        "ratio_to": 10.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    df = pd.DataFrame(
        [
            {"date": "2026-06-10", "ticker": "KLAG", "underlying_adj_close": 2135.64},
            {"date": "2026-06-11", "ticker": "KLAG", "underlying_adj_close": 241.16},
        ]
    )
    out, n = repair_underlying_adj_close_split_basis(
        df,
        {"KLAG": "KLAC"},
        corporate_actions_path=ca,
    )
    assert n == 1
    pre = float(out.loc[out["date"] == dt.date(2026, 6, 10), "underlying_adj_close"].iloc[0])
    assert abs(pre - 213.564) < 0.05

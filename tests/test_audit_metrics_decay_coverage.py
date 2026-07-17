from __future__ import annotations

from datetime import date
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from audit_metrics_decay_coverage import (  # noqa: E402
    build_coverage_report,
    classify_ticker,
    coverage_gate_errors,
    is_decay_joint_usable,
)
from ingest_etf_metrics import (  # noqa: E402
    STALE_KIND_MARKET_BACKED,
    promote_carry_forward_rows_with_market,
    stamp_metric_asof_metadata,
)
from realized_gross_decay import _metrics_row_has_usable_prices  # noqa: E402


def test_is_decay_joint_usable_rejects_carry_forward():
    row = {
        "date": "2026-07-14",
        "close_price": 5.0,
        "underlying_adj_close": 15.0,
        "stale_kind": "carry_forward",
        "source_provider": "carry_forward",
        "source_url": "carry_forward://RGTZ?from=2026-06-17",
    }
    assert is_decay_joint_usable(row) is False
    assert _metrics_row_has_usable_prices(row) is False


def test_promote_cf_with_market_is_decay_usable_and_blocks_prem_disc():
    df = pd.DataFrame(
        [
            {
                "date": date(2026, 7, 14),
                "ticker": "RGTZ",
                "nav": 3.65,
                "close_price": 5.47,
                "etf_adj_close": 5.47,
                "underlying_adj_close": 15.36,
                "stale": True,
                "stale_age_bdays": 10,
                "stale_kind": "carry_forward",
                "source_provider": "carry_forward",
                "source_url": "carry_forward://RGTZ?from=2026-06-17",
                "status": "ok",
            }
        ]
    )
    out, n = promote_carry_forward_rows_with_market(df)
    assert n == 1
    row = out.iloc[0].to_dict()
    assert row["stale_kind"] == STALE_KIND_MARKET_BACKED
    assert row["source_provider"] == "market_backed"
    assert is_decay_joint_usable(row) is True
    assert _metrics_row_has_usable_prices(row) is True
    stamped = stamp_metric_asof_metadata(out)
    assert bool(stamped.iloc[0]["premium_discount_eligible"]) is False
    assert stamped.iloc[0]["premium_discount_status"] == "issuer_stale"


def test_promote_skips_cf_without_close_or_und():
    base = {
        "date": date(2026, 7, 14),
        "ticker": "HOLE",
        "nav": 3.65,
        "stale_kind": "carry_forward",
        "source_provider": "carry_forward",
        "source_url": "carry_forward://HOLE?from=2026-07-10",
        "status": "ok",
    }
    no_close = pd.DataFrame([{**base, "close_price": None, "underlying_adj_close": 15.0}])
    out, n = promote_carry_forward_rows_with_market(no_close)
    assert n == 0
    no_und = pd.DataFrame([{**base, "close_price": 5.0, "underlying_adj_close": None}])
    out2, n2 = promote_carry_forward_rows_with_market(no_und)
    assert n2 == 0
    assert out2.iloc[0]["stale_kind"] == "carry_forward"


def test_classify_market_fixable_bucket():
    rows = [
        {
            "date": "2026-06-17",
            "ticker": "RGTZ",
            "close_price": 3.65,
            "underlying_adj_close": 20.0,
            "status": "ok",
            "source_provider": "polygon",
            "stale_kind": None,
        },
        {
            "date": "2026-07-14",
            "ticker": "RGTZ",
            "close_price": 5.47,
            "underlying_adj_close": 15.0,
            "status": "ok",
            "source_provider": "carry_forward",
            "source_url": "carry_forward://RGTZ?from=2026-06-17",
            "stale_kind": "carry_forward",
        },
    ]
    info = classify_ticker(rows, panel_max=date(2026, 7, 14))
    assert info["bucket"] == "market_fixable"
    assert info["market_fixable"] is True
    assert info["sessions_behind"] > 2


def test_coverage_gate_flags_cf_tail_with_market():
    df = pd.DataFrame(
        [
            {
                "date": f"2026-07-{d:02d}",
                "ticker": "AAA",
                "close_price": 10.0,
                "underlying_adj_close": 20.0,
                "source_provider": "carry_forward",
                "source_url": "carry_forward://AAA?from=2026-07-01",
                "stale_kind": "carry_forward",
                "status": "ok",
            }
            for d in (10, 11, 14)
        ]
        + [
            {
                "date": "2026-07-14",
                "ticker": "BBB",
                "close_price": 11.0,
                "underlying_adj_close": 21.0,
                "source_provider": "polygon",
                "stale_kind": None,
                "status": "ok",
            }
        ]
    )
    report = build_coverage_report(df, {"AAA", "BBB"}, tradeable={"AAA", "BBB"})
    assert report["summary"]["cf_tail_ge3_market_fixable_n"] >= 1
    errors = coverage_gate_errors(report)
    assert any("carry_forward tails" in e for e in errors)

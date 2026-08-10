"""Regression tests for the 2026-07 forward-split ``etf_adj_close`` basis bug.

Eight levered wrappers (CRDU, GEVX, KORU, LABX, MUU, NEBX, SNXX, WDCX) served
``etf_adj_close`` on the *pre-split* basis for weeks after their forward splits
executed. ``refresh_underlying_spots.py`` reads that column as ``prior_close`` and
differences it against a raw live quote, so KORU reported a +1878% "daily" return
and the LETF flow math downstream inherited it.

Three independent defects had to line up, and each gets a test here:

1. ``detect_adj_basis_switch_splits`` proposed the ``forward`` remap even when the
   provider had already restated raw close at the split, so the correctly
   back-adjusted post-split rows were scaled by the split factor a second time.
2. Nothing rebuilt a series already persisted on the pre-split basis: adj/close is
   flat across the boundary, so the return-based cliff detectors stay quiet.
3. ``prior_close`` preferred adjusted close over raw close -- a basis mismatch
   against a raw quote regardless of split health.
"""
from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import refresh_underlying_spots as refresh  # noqa: E402
from ingest_etf_metrics import (  # noqa: E402
    backfill_split_adjusted_etf_adj_close,
    normalize_adj_basis_switch_etf_adj_close,
    repair_pre_split_basis_etf_adj_close,
)
from split_adjustments import detect_adj_basis_switch_splits  # noqa: E402


def _corp_actions(tmp_path: Path, ticker: str, execution_date: str, ratio_to: float) -> Path:
    """A single declared forward split, in the shape ``corporate_actions.json`` uses."""
    path = tmp_path / "corporate_actions.json"
    path.write_text(
        json.dumps(
            {
                "events": [
                    {
                        "id": f"polygon_split:{ticker}:{execution_date}",
                        "type": "forward_split",
                        "ticker": ticker,
                        "execution_date": execution_date,
                        "ratio_from": 1.0,
                        "ratio_to": ratio_to,
                        "status": "executed",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    return path


def _split_frame(
    ticker: str,
    *,
    mult: float,
    boundary: str,
    n_pre: int = 12,
    n_post: int = 8,
    pre_close: float = 60.0,
    und_start: float = 200.0,
) -> pd.DataFrame:
    """Raw provider shape: close jumps by ``mult`` at ``boundary``, adj == close.

    This is the healthy input -- the provider restated close at the split, so the
    pre-split rows are the only ones needing back-adjustment.
    """
    bnd = dt.date.fromisoformat(boundary)
    rows: list[dict[str, object]] = []
    for i in range(n_pre, 0, -1):
        close = pre_close * (1.0 + 0.004 * i)
        rows.append(
            {
                "ticker": ticker,
                "date": bnd - dt.timedelta(days=i * 3),
                "close_price": close,
                "etf_adj_close": close,
                "nav": close,
                "underlying_adj_close": und_start * (1.0 + 0.002 * i),
                "stale": False,
            }
        )
    for j in range(n_post):
        close = pre_close * mult * (1.0 + 0.003 * j)
        rows.append(
            {
                "ticker": ticker,
                "date": bnd + dt.timedelta(days=j * 3),
                "close_price": close,
                "etf_adj_close": close,
                "nav": close,
                "underlying_adj_close": und_start * (1.0 - 0.001 * j),
                "stale": False,
            }
        )
    return pd.DataFrame(rows)


def _ratios(df: pd.DataFrame, boundary: str) -> tuple[list[float], list[float]]:
    bnd = dt.date.fromisoformat(boundary)
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"]).dt.date
    ratio = pd.to_numeric(out["etf_adj_close"]) / pd.to_numeric(out["close_price"])
    return (
        ratio[out["date"] < bnd].round(6).tolist(),
        ratio[out["date"] >= bnd].round(6).tolist(),
    )


# ?? 1. detector must not remap a provider-restated forward split ???????????


@pytest.mark.parametrize(
    "ticker,ratio_to,boundary",
    [
        ("CRDU", 4.0, "2026-07-21"),
        ("KORU", 20.0, "2026-07-15"),
        ("LABX", 6.0, "2026-07-21"),
        ("SNXX", 8.0, "2026-06-03"),
    ],
)
def test_restated_forward_split_is_not_an_adj_basis_switch(
    tmp_path: Path, ticker: str, ratio_to: float, boundary: str
):
    """A close series that jumps by the split ratio has no adj basis switch to fix."""
    mult = 1.0 / ratio_to
    df = _split_frame(ticker, mult=mult, boundary=boundary)
    corp = _corp_actions(tmp_path, ticker, boundary, ratio_to)

    points = [
        (r["date"], float(r["close_price"]), float(r["etf_adj_close"]))
        for _, r in df.iterrows()
    ]
    events = [(dt.date.fromisoformat(boundary), mult)]
    assert detect_adj_basis_switch_splits(points, events, metric_rows=df.to_dict("records")) == []

    # End to end: back-adjust pre rows, leave post rows on the raw basis.
    out = normalize_adj_basis_switch_etf_adj_close(
        backfill_split_adjusted_etf_adj_close(df, corporate_actions_path=corp),
        corporate_actions_path=corp,
    )
    pre, post = _ratios(out, boundary)
    assert post == pytest.approx([1.0] * len(post), rel=1e-6), (
        "post-split rows must stay on the raw traded basis"
    )
    assert pre == pytest.approx([round(mult, 6)] * len(pre), rel=1e-6), (
        "pre-split rows must be back-adjusted onto the post-split basis"
    )


def test_market_obscured_forward_split_is_not_an_adj_basis_switch(tmp_path: Path):
    """NEBX: a +29% levered session shrinks the 1-for-3 close jump to ~0.43x.

    Close switches basis on 2026-06-01 while issuer NAV lags to the declared
    2026-06-03. The mechanical ratio no longer matches, so the strict jump test
    misses it and only the NAV-confirmed partial-jump path sees that close was
    restated. Runs the real ingest sequence -- backfill back-adjusts the pre rows,
    and the remap must then leave the post rows alone.
    """
    ticker, declared_date, mult = "NEBX", "2026-06-03", 1.0 / 3.0
    close_bnd = dt.date.fromisoformat("2026-06-01")
    eff = dt.date.fromisoformat(declared_date)
    corp = _corp_actions(tmp_path, ticker, declared_date, 3.0)

    rows: list[dict[str, object]] = []
    for i in range(12, 0, -1):
        close = 130.0 * (1.0 + 0.004 * i)
        rows.append({
            "ticker": ticker, "date": close_bnd - dt.timedelta(days=i * 3),
            "close_price": close, "etf_adj_close": close, "nav": close,
            "underlying_adj_close": 230.0, "shares_outstanding": 3_000_000.0,
            "stale": False,
        })
    # Close on the new basis, NAV still on the old one, on a +29% levered day.
    for j, (d, close, nav) in enumerate([
        (close_bnd, 58.33, 174.60),
        (close_bnd + dt.timedelta(days=1), 56.22, 169.30),
    ]):
        rows.append({
            "ticker": ticker, "date": d, "close_price": close,
            "etf_adj_close": close, "nav": nav,
            "underlying_adj_close": 264.51 - j, "shares_outstanding": 3_000_000.0,
            "stale": False,
        })
    # NAV switches basis on the declared date.
    for j in range(6):
        close = 52.38 * (1.0 - 0.004 * j)
        rows.append({
            "ticker": ticker, "date": eff + dt.timedelta(days=j * 3),
            "close_price": close, "etf_adj_close": close, "nav": close,
            "underlying_adj_close": 251.0 - j, "shares_outstanding": 9_000_000.0,
            "stale": False,
        })
    df = pd.DataFrame(rows)

    observed = 58.33 / (130.0 * 1.004)
    assert observed == pytest.approx(0.447, abs=0.02), "fixture must obscure the split ratio"

    out = normalize_adj_basis_switch_etf_adj_close(
        backfill_split_adjusted_etf_adj_close(df, corporate_actions_path=corp),
        corporate_actions_path=corp,
    )
    pre, post = _ratios(out, "2026-06-01")
    assert post == pytest.approx([1.0] * len(post), rel=1e-6), (
        "post-split rows must stay on the raw traded basis"
    )
    assert pre == pytest.approx([round(mult, 6)] * len(pre), rel=1e-6)


def test_continuous_close_forward_split_still_remaps(tmp_path: Path):
    """The case the remap exists for: close continuous through the split, adj switches.

    Guards the fix against over-reaching -- a provider that never restated close must
    still get its post-split rows mapped onto the back-adjusted basis.
    """
    ticker, boundary, mult = "DUOG", "2026-05-05", 0.1
    bnd = dt.date.fromisoformat(boundary)
    rows: list[dict[str, object]] = []
    for i in range(10, 0, -1):
        close = 40.0 * (1.0 + 0.004 * i)
        rows.append({
            "ticker": ticker, "date": bnd - dt.timedelta(days=i * 3),
            "close_price": close, "etf_adj_close": close * (1.0 / mult),
            "nav": close, "underlying_adj_close": 100.0, "stale": False,
        })
    for j in range(6):
        close = 40.0 * (1.0 - 0.003 * j)
        rows.append({
            "ticker": ticker, "date": bnd + dt.timedelta(days=j * 3),
            "close_price": close, "etf_adj_close": close,
            "nav": close, "underlying_adj_close": 100.0, "stale": False,
        })
    df = pd.DataFrame(rows)
    points = [
        (r["date"], float(r["close_price"]), float(r["etf_adj_close"]))
        for _, r in df.iterrows()
    ]
    variants = detect_adj_basis_switch_splits(
        points, [(bnd, mult)], metric_rows=df.to_dict("records")
    )
    assert [v for _d, _m, v in variants], "continuous close must still yield a remap variant"


# ?? 2. repair a series already persisted on the pre-split basis ????????????


@pytest.mark.parametrize(
    "ticker,ratio_to,boundary",
    [
        ("KORU", 20.0, "2026-07-15"),
        ("MUU", 20.0, "2026-07-15"),
        ("CRDU", 4.0, "2026-07-21"),
        ("WDCX", 3.0, "2026-06-03"),
    ],
)
def test_repair_rebuilds_pre_split_basis_series(
    tmp_path: Path, ticker: str, ratio_to: float, boundary: str
):
    mult = 1.0 / ratio_to
    df = _split_frame(ticker, mult=mult, boundary=boundary)
    corp = _corp_actions(tmp_path, ticker, boundary, ratio_to)
    # Corrupt exactly as the double-scaling did: every row on the pre-split basis.
    df["etf_adj_close"] = pd.to_numeric(df["close_price"]) * mult

    out, n = repair_pre_split_basis_etf_adj_close(df, corporate_actions_path=corp)
    assert n > 0
    assert len(out) == len(df), "a basis repair must never change the record count"
    pre, post = _ratios(out, boundary)
    assert post == pytest.approx([1.0] * len(post), rel=1e-6)
    assert pre == pytest.approx([round(mult, 6)] * len(pre), rel=1e-6)


def test_repair_is_idempotent_on_a_healthy_series(tmp_path: Path):
    df = _split_frame("CRDU", mult=0.25, boundary="2026-07-21")
    corp = _corp_actions(tmp_path, "CRDU", "2026-07-21", 4.0)
    healthy = normalize_adj_basis_switch_etf_adj_close(
        backfill_split_adjusted_etf_adj_close(df, corporate_actions_path=corp),
        corporate_actions_path=corp,
    )
    out, n = repair_pre_split_basis_etf_adj_close(healthy, corporate_actions_path=corp)
    assert n == 0
    pd.testing.assert_series_equal(
        pd.to_numeric(out["etf_adj_close"]),
        pd.to_numeric(healthy["etf_adj_close"]),
    )


def test_repair_leaves_distribution_adjustment_alone(tmp_path: Path):
    """A distribution-adjusted series is not a mis-based one; only split factors match."""
    df = _split_frame("CRDU", mult=0.25, boundary="2026-07-21")
    corp = _corp_actions(tmp_path, "CRDU", "2026-07-21", 4.0)
    bnd = dt.date.fromisoformat("2026-07-21")
    dates = pd.to_datetime(df["date"]).dt.date
    # Post rows carry a 3% distribution haircut -- nowhere near the 0.25 split factor.
    df.loc[dates >= bnd, "etf_adj_close"] = (
        pd.to_numeric(df.loc[dates >= bnd, "close_price"]) * 0.97
    )
    out, n = repair_pre_split_basis_etf_adj_close(df, corporate_actions_path=corp)
    assert n == 0
    _pre, post = _ratios(out, "2026-07-21")
    assert post == pytest.approx([0.97] * len(post), rel=1e-6)


# ?? 3. prior_close must be on the raw traded basis ??????????????????????????


def test_etf_prior_close_uses_raw_close_not_adjusted(tmp_path: Path):
    """``prior_close`` is differenced against a raw quote, so it must be raw close."""
    parquet = tmp_path / "metrics.parquet"
    pd.DataFrame(
        [
            # A split cohort ticker whose adj column is still mis-based.
            {"ticker": "KORU", "date": "2026-08-10", "close_price": 16.84,
             "etf_adj_close": 0.842, "underlying_adj_close": 164.27},
            # An ordinary distribution-adjusted ticker (AAOZ: fake +21% intraday).
            {"ticker": "AAOZ", "date": "2026-08-10", "close_price": 11.35,
             "etf_adj_close": 9.39, "underlying_adj_close": 144.55},
        ]
    ).to_parquet(parquet, index=False)

    priors = refresh.load_etf_prior_closes_from_metrics(parquet, etf_tickers=["KORU", "AAOZ"])
    assert priors["KORU"]["prior_close"] == pytest.approx(16.84)
    assert priors["AAOZ"]["prior_close"] == pytest.approx(11.35)

    # The reported symptom: a live quote against a pre-split prior_close.
    merged = refresh.merge_sources(
        ["KORU", "AAOZ"],
        tradier={
            "KORU": {"last": 16.66, "stale": False, "source": "tradier_spot", "as_of": None},
            "AAOZ": {"last": 11.405, "stale": False, "source": "tradier_spot", "as_of": None},
        },
        polygon={},
        options_cache={},
        metrics_priors=priors,
    )
    assert merged["KORU"]["return_d1_so_far"] == pytest.approx(16.66 / 16.84 - 1.0)
    assert abs(merged["KORU"]["return_d1_so_far"]) < 0.05, "was +1878% off the adj basis"
    assert merged["AAOZ"]["return_d1_so_far"] == pytest.approx(11.405 / 11.35 - 1.0)
    assert abs(merged["AAOZ"]["return_d1_so_far"]) < 0.05, "was +21.5% off the adj basis"


def test_etf_prior_close_falls_back_to_adjusted_when_close_missing(tmp_path: Path):
    parquet = tmp_path / "metrics.parquet"
    pd.DataFrame(
        [
            {"ticker": "FOO", "date": "2026-08-10", "close_price": None,
             "etf_adj_close": 12.5, "underlying_adj_close": 100.0},
        ]
    ).to_parquet(parquet, index=False)
    priors = refresh.load_etf_prior_closes_from_metrics(parquet, etf_tickers=["FOO"])
    assert priors["FOO"]["prior_close"] == pytest.approx(12.5)

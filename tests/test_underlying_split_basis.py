"""KLAG/KLAC underlying split normalization regression tests."""
from __future__ import annotations

import datetime as dt
import math
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / "scripts"
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from ingest_etf_metrics import (  # noqa: E402
    repair_underlying_adj_close_reverting_islands,
    repair_underlying_adj_close_split_basis,
)
from price_basis import (  # noqa: E402
    build_tr_series_from_metrics,
    find_underlying_adj_cliffs,
    infer_underlying_split_events_from_rows,
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


def test_infer_underlying_split_events_from_klac_cliff_rows():
    inferred = infer_underlying_split_events_from_rows(_klag_cliff_rows(), [])
    assert len(inferred) >= 1
    d, mult = inferred[0]
    assert str(d) == "2026-06-11"
    assert abs(mult - 0.1) < 0.02


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


def test_repair_reverting_underlying_island_lcdl_style():
    import pandas as pd

    rows = []
    for i, d in enumerate(["2025-05-09", "2025-05-12", "2025-05-13"]):
        rows.append({
            "date": d,
            "ticker": "LCDL",
            "close_price": 31.0 + i * 0.2,
            "underlying_adj_close": 26.0 + i * 0.2,
        })
    for i, d in enumerate(["2025-05-14", "2025-05-15", "2025-05-16", "2025-05-19", "2025-05-20", "2025-05-21"]):
        rows.append({
            "date": d,
            "ticker": "LCDL",
            "close_price": 32.0 + i * 0.1,
            "underlying_adj_close": 270.0 + i,
        })
    for i, d in enumerate(["2025-05-22", "2025-05-23"]):
        rows.append({
            "date": d,
            "ticker": "LCDL",
            "close_price": 31.5 + i * 0.1,
            "underlying_adj_close": 26.5 + i * 0.1,
        })
    df = pd.DataFrame(rows)
    out, n = repair_underlying_adj_close_reverting_islands(df, {"LCDL": "LCID"})
    assert n >= 5
    island = out[out["date"].astype(str).between("2025-05-14", "2025-05-21")]
    assert float(island["underlying_adj_close"].median()) < 40.0
    cliffs = find_underlying_adj_cliffs(
        [{"date": str(r.date), "underlying_adj_close": r.underlying_adj_close} for r in out.itertuples()],
        [],
    )
    assert cliffs == []


def test_split_basis_skips_reverting_island_when_later_reverse_exists(tmp_path: Path):
    import pandas as pd

    ca = tmp_path / "corporate_actions.json"
    ca.write_text(
        json.dumps(
            {
                "events": [
                    {
                        "type": "reverse_split",
                        "ticker": "LCID",
                        "execution_date": "2025-09-02",
                        "ratio_from": 10.0,
                        "ratio_to": 1.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    rows = []
    for d, und in [
        ("2025-05-13", 26.5),
        ("2025-05-14", 276.0),
        ("2025-05-15", 280.0),
        ("2025-05-16", 275.0),
        ("2025-05-22", 26.6),
        ("2025-09-01", 20.0),
        ("2025-09-02", 17.0),
    ]:
        rows.append({"date": d, "ticker": "LCDL", "underlying_adj_close": und, "close_price": 30.0})
    df = pd.DataFrame(rows)
    repaired, n_island = repair_underlying_adj_close_reverting_islands(df, {"LCDL": "LCID"})
    assert n_island >= 2
    out, _n_scaled = repair_underlying_adj_close_split_basis(
        repaired,
        {"LCDL": "LCID"},
        corporate_actions_path=ca,
    )
    may14 = float(out.loc[out["date"] == dt.date(2025, 5, 14), "underlying_adj_close"].iloc[0])
    assert may14 < 50.0


# --- SOXX 3-for-1 (2026-11-05): partially rescaled store left pre-split remnants ---

SOXX_SPLIT_DATE = "2026-11-05"


def _soxx_corporate_actions(tmp_path: Path) -> Path:
    ca = tmp_path / "corporate_actions.json"
    ca.write_text(
        json.dumps(
            {
                "events": [
                    {
                        "type": "forward_split",
                        "ticker": "SOXX",
                        "execution_date": SOXX_SPLIT_DATE,
                        "ratio_from": 1.0,
                        "ratio_to": 3.0,
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    return ca


def _soxx_true_series() -> list[tuple[str, float, float]]:
    """(date, post-split-basis SOXX, SOXL close) with small, calm daily moves."""
    dates = [
        "2026-01-08", "2026-01-09", "2026-01-12", "2026-01-13",
        "2026-03-05", "2026-03-06", "2026-03-09", "2026-03-10",
        "2026-11-03", "2026-11-04", "2026-11-05", "2026-11-06",
    ]
    out = []
    und, etf = 109.0, 54.0
    for i, d in enumerate(dates):
        step = 0.004 if i % 2 == 0 else -0.003
        und *= 1.0 + step
        etf *= 1.0 + 3.0 * step
        out.append((d, round(und, 6), round(etf, 6)))
    return out


def _soxx_mixed_basis_df(remnant_dates: set[str]):
    import pandas as pd

    rows = []
    for d, und, etf in _soxx_true_series():
        stored = und * 3.0 if d in remnant_dates else und
        rows.append(
            {"date": d, "ticker": "SOXL", "close_price": etf, "underlying_adj_close": stored}
        )
    return pd.DataFrame(rows)


SOXX_REMNANTS = {"2026-01-08", "2026-01-09", "2026-03-06"}


def test_soxx_mixed_basis_store_reproduces_gate_cliffs():
    df = _soxx_mixed_basis_df(SOXX_REMNANTS)
    rows = df.to_dict("records")
    cliffs = find_underlying_adj_cliffs(rows, [(dt.date(2026, 11, 5), 1.0 / 3.0)])
    assert {str(c["date"]) for c in cliffs} == {"2026-01-12", "2026-03-06", "2026-03-09"}


def test_split_basis_scales_soxx_remnants_by_exact_declared_factor(tmp_path: Path):
    ca = _soxx_corporate_actions(tmp_path)
    df = _soxx_mixed_basis_df(SOXX_REMNANTS)
    out, n = repair_underlying_adj_close_split_basis(
        df, {"SOXL": "SOXX"}, corporate_actions_path=ca, as_of=dt.date(2026, 11, 6)
    )
    assert n == len(SOXX_REMNANTS)
    truth = {d: und for d, und, _ in _soxx_true_series()}
    for _, row in out.iterrows():
        assert row["underlying_adj_close"] == pytest.approx(truth[str(row["date"])], rel=1e-12)
    assert find_underlying_adj_cliffs(out.to_dict("records"), [(dt.date(2026, 11, 5), 1 / 3)]) == []


def test_split_basis_scales_each_leg_on_its_own_stored_basis(tmp_path: Path):
    """SEMY already carried 03-06 on the new basis while SOXL/SOXS did not."""
    import pandas as pd

    ca = _soxx_corporate_actions(tmp_path)
    soxl = _soxx_mixed_basis_df(SOXX_REMNANTS)
    semy = _soxx_mixed_basis_df({"2026-01-08", "2026-01-09"}).assign(ticker="SEMY")
    df = pd.concat([semy, soxl], ignore_index=True)
    out, n = repair_underlying_adj_close_split_basis(
        df,
        {"SOXL": "SOXX", "SEMY": "SOXX"},
        corporate_actions_path=ca,
        as_of=dt.date(2026, 11, 6),
    )
    assert n == 5
    truth = {d: und for d, und, _ in _soxx_true_series()}
    for _, row in out.iterrows():
        assert row["underlying_adj_close"] == pytest.approx(truth[str(row["date"])], rel=1e-12)


def test_split_basis_leaves_real_underlying_crash_alone(tmp_path: Path):
    """A 3x underlying move that the ETF leg also makes is not a basis artifact."""
    ca = _soxx_corporate_actions(tmp_path)
    df = _soxx_mixed_basis_df(set())
    df.loc[df["date"] <= "2026-01-09", "underlying_adj_close"] *= 3.0
    df.loc[df["date"] <= "2026-01-09", "close_price"] *= 30.0
    from ingest_etf_metrics import _scale_und_pre_split_remnants

    out = df.copy()
    out["date"] = pd_dates(out["date"])
    handled, n = _scale_und_pre_split_remnants(
        out,
        out["ticker"] == "SOXL",
        "SOXX",
        [(dt.date(2026, 11, 5), 1.0 / 3.0)],
        as_of=dt.date(2026, 11, 6),
    )
    assert n == 0 and handled == set()


def pd_dates(col):
    import pandas as pd

    return pd.to_datetime(col).dt.date


def test_island_repair_does_not_flank_scale_pre_split_remnant(tmp_path: Path):
    """Flank rescale fabricated SOXX 03-26..03-31 (115.15 vs Yahoo 109.62)."""
    ca = _soxx_corporate_actions(tmp_path)
    df = _soxx_mixed_basis_df({"2026-03-06"})
    out, n = repair_underlying_adj_close_reverting_islands(
        df, {"SOXL": "SOXX"}, corporate_actions_path=ca
    )
    assert n == 0
    stored = float(out.loc[out["date"] == dt.date(2026, 3, 6), "underlying_adj_close"].iloc[0])
    truth = {d: und for d, und, _ in _soxx_true_series()}["2026-03-06"]
    assert stored == pytest.approx(truth * 3.0)


def _fresh_frame(rows: list[tuple[str, float]]):
    import pandas as pd

    return pd.DataFrame(
        [{"date": dt.date.fromisoformat(d), "ticker": "SOXX", "underlying_adj_close": px} for d, px in rows]
    )


def test_restate_overwrites_pre_split_rows_from_yahoo(tmp_path: Path):
    from ingest_etf_metrics import restate_underlying_adj_close_after_split

    ca = _soxx_corporate_actions(tmp_path)
    df = _soxx_mixed_basis_df(SOXX_REMNANTS)
    truth = _soxx_true_series()
    yahoo = [(d, und) for d, und, _ in truth]
    calls = []

    def fetch(syms, start, end):
        calls.append((tuple(syms), start, end))
        return _fresh_frame(yahoo)

    out, n = restate_underlying_adj_close_after_split(
        df, {"SOXL": "SOXX"}, corporate_actions_path=ca, as_of=dt.date(2026, 11, 6), fetcher=fetch
    )
    assert calls and calls[0][0] == ("SOXX",)
    assert n == len(SOXX_REMNANTS)
    by_date = {d: und for d, und, _ in truth}
    for _, row in out.iterrows():
        assert row["underlying_adj_close"] == pytest.approx(by_date[str(row["date"])], rel=1e-12)


def test_restate_refuses_fresh_pull_with_its_own_cliffs(tmp_path: Path):
    from ingest_etf_metrics import restate_underlying_adj_close_after_split

    ca = _soxx_corporate_actions(tmp_path)
    df = _soxx_mixed_basis_df(SOXX_REMNANTS)
    bad = [(d, und * (3.0 if d < "2026-03-01" else 1.0)) for d, und, _ in _soxx_true_series()]
    out, n = restate_underlying_adj_close_after_split(
        df,
        {"SOXL": "SOXX"},
        corporate_actions_path=ca,
        as_of=dt.date(2026, 11, 6),
        fetcher=lambda *_a: _fresh_frame(bad),
    )
    assert n == 0
    assert list(out["underlying_adj_close"]) == pytest.approx(list(df["underlying_adj_close"]))


def test_restate_skips_old_split_on_consistent_store(tmp_path: Path):
    """Old splits on a cliff-free store are left to readers (no Yahoo pull)."""
    from ingest_etf_metrics import restate_underlying_adj_close_after_split

    ca = _soxx_corporate_actions(tmp_path)
    df = _soxx_mixed_basis_df(set())

    def fetch(*_a):
        raise AssertionError("must not fetch")

    _out, n = restate_underlying_adj_close_after_split(
        df, {"SOXL": "SOXX"}, corporate_actions_path=ca, as_of=dt.date(2027, 6, 1), fetcher=fetch
    )
    assert n == 0


def test_tr_does_not_double_scale_restated_underlying():
    from price_basis import underlying_split_events_needing_scale

    rows = [
        {"date": d, "close_price": etf, "etf_adj_close": etf, "underlying_adj_close": und}
        for d, und, etf in _soxx_true_series()
    ]
    events = [(dt.date(2026, 11, 5), 1.0 / 3.0)]
    assert underlying_split_events_needing_scale(rows, events) == []
    tr = build_tr_series_from_metrics(rows, [], underlying_split_events=events)
    jumps = [
        abs(math.log(b["tr_und_px"] / a["tr_und_px"])) for a, b in zip(tr, tr[1:])
    ]
    assert jumps and max(jumps) < 0.05


def test_tr_still_scales_raw_pre_split_underlying():
    from price_basis import underlying_split_events_needing_scale

    rows = [
        {
            "date": d,
            "close_price": etf,
            "etf_adj_close": etf,
            "underlying_adj_close": und * (3.0 if d < SOXX_SPLIT_DATE else 1.0),
        }
        for d, und, etf in _soxx_true_series()
    ]
    events = [(dt.date(2026, 11, 5), 1.0 / 3.0)]
    assert underlying_split_events_needing_scale(rows, events) == events

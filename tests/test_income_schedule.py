"""Unit tests for the NAV-normalized YieldBOOST calibration module.

The Bucket 2 research fixture (``tests/fixtures/bucket2_research.json``)
provides 8 canonical (?, d_weekly, borrow) ? (NAV decay, dist, net short)
points. We validate two things end-to-end:

1. The dashboard's closed-form scenario math reproduces those outputs to
   within the published tolerances when ``d`` is set to the research's
   ``actual_distribution_weekly``.
2. The calibration module's NAV-normalization, capture-ratio computation,
   confidence weighting, and cross-fund self-calibration all behave as
   designed on hand-crafted MTYY and CWY fixtures.
"""
from __future__ import annotations

import datetime as dt
import json
import math
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_SCRIPTS = _REPO_ROOT / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

import income_schedule as ic  # noqa: E402


FIXTURE = _REPO_ROOT / "tests" / "fixtures" / "bucket2_research.json"


# ---------------------------------------------------------------------------
# helpers (mirror the closed form used in ``estimateIncomeStyleScenarioReturn``)
# ---------------------------------------------------------------------------
def _closed_form(d_weekly, sigma, borrow, expense_annual=0.0099, weeks=52):
    L = ic.expected_put_spread_loss_weekly(0.0, sigma, 1.0)
    q = max(0.0001, min(1.5, 1.0 - L - expense_annual / weeks))
    nav_decay = 1.0 - q ** weeks
    geom = weeks if abs(1.0 - q) < 1e-9 else (1.0 - q ** weeks) / (1.0 - q)
    dist = d_weekly * geom
    net_short = nav_decay - dist - borrow
    return {"L": L, "nav_decay": nav_decay, "dist": dist, "net_short": net_short}


@pytest.fixture(scope="module")
def research():
    return json.loads(FIXTURE.read_text())


# ---------------------------------------------------------------------------
# 1. Engine-level: closed form reproduces research outputs
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("idx", list(range(8)))
def test_research_closed_form_reproduces_published_outputs(research, idx):
    candidate = research["candidates"][idx]
    tol = research["tolerances"]
    out = _closed_form(
        candidate["actual_distribution_weekly"],
        candidate["sigma_annual"],
        candidate["borrow_annual"],
    )
    assert abs(out["nav_decay"] - candidate["nav_decay_annual"]) <= tol["nav_decay_pp"], (
        f"{candidate['etf']}: NAV decay {out['nav_decay']:.3f} vs "
        f"research {candidate['nav_decay_annual']:.3f}"
    )
    assert abs(out["dist"] - candidate["distributions_paid_annual"]) <= tol["distributions_paid_pp"], (
        f"{candidate['etf']}: distributions paid {out['dist']:.3f} vs "
        f"research {candidate['distributions_paid_annual']:.3f}"
    )
    assert abs(out["net_short"] - candidate["net_short_annual"]) <= tol["net_short_pp"], (
        f"{candidate['etf']}: net short {out['net_short']:.3f} vs "
        f"research {candidate['net_short_annual']:.3f}"
    )


def test_dashboard_l_within_10pct_of_research_bs_premium(research):
    """The dashboard's ``expectedPutSpreadLossWeekly`` uses physical-measure
    leveraged drift (mu_annual = log(1+u)/t with u=0 -> mu=0, m = -2 sigma^2
    tau). The research's BS premium is risk-neutral. The two formulations
    give similar magnitudes (within ~10% relative across realistic sigma);
    document the relationship so callers know which basis their structural
    ratio is computed against.
    """
    deltas = []
    for c in research["candidates"]:
        L = ic.expected_put_spread_loss_weekly(0.0, c["sigma_annual"], 1.0)
        rel = (L - c["bs_premium_weekly"]) / c["bs_premium_weekly"]
        deltas.append(abs(rel))
    median_rel = sorted(deltas)[len(deltas) // 2]
    assert median_rel < 0.10, f"unexpected L vs BS premium gap: {median_rel:.3%}"


# ---------------------------------------------------------------------------
# 2. NAV-normalization on a hand-crafted MTYY-shaped fixture
# ---------------------------------------------------------------------------
def _mk_events(rows):
    return [{"ex_date": d, "amount": a} for d, a in rows]


def _mk_nav(rows):
    return list(rows)


def test_normalize_events_divides_by_nav_at_ex_not_current():
    """Regression for the MTYY 163% bug: a fund whose NAV halves between
    ex-dates must still report ~constant weekly yield once normalized."""
    events = _mk_events([
        ("2025-12-01", 0.40),   # paid at NAV 20  ? 2.0%
        ("2026-01-15", 0.20),   # paid at NAV 10  ? 2.0%
        ("2026-02-15", 0.10),   # paid at NAV  5  ? 2.0%
    ])
    nav_series = _mk_nav([
        ("2025-11-30", 20.0),
        ("2026-01-14", 10.0),
        ("2026-02-14", 5.0),
        ("2026-05-22", 5.0),
    ])
    normalized, missing = ic.normalize_events(events, nav_series, current_sigma=0.88)
    assert missing == 0
    yields = [e["yield_frac"] for e in normalized]
    assert yields == pytest.approx([0.02, 0.02, 0.02], rel=1e-9)
    # The broken legacy formula would produce 0.40/5 + 0.20/5 + 0.10/5 = 14%
    # which compounds to a falsely large annual yield.


def test_normalize_events_skips_when_no_nav_history():
    events = _mk_events([("2023-01-01", 0.10)])
    nav_series = _mk_nav([("2023-06-01", 5.0)])  # all after ex_date
    normalized, missing = ic.normalize_events(events, nav_series, current_sigma=0.5)
    assert normalized == []
    assert missing == 1


def test_normalize_events_carries_capture_ratio_when_sigma_known():
    events = _mk_events([("2026-01-01", 0.10)])
    nav_series = _mk_nav([("2026-01-01", 5.0)])
    normalized, _ = ic.normalize_events(events, nav_series, current_sigma=0.8)
    bs = ic.expected_put_spread_loss_weekly(0.0, 0.8, 1.0)
    assert normalized[0]["ratio"] == pytest.approx(0.02 / bs, rel=1e-6)


# ---------------------------------------------------------------------------
# 3. Cadence detection
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "ex_dates, expected_label, expected_ppy",
    [
        (["2026-01-01", "2026-01-08", "2026-01-15", "2026-01-22"], "weekly", 52),
        (["2026-01-01", "2026-01-15", "2026-01-29", "2026-02-12"], "biweekly", 26),
        (["2026-01-01", "2026-02-01", "2026-03-01", "2026-04-01"], "monthly", 12),
        (["2026-01-01", "2026-04-02", "2026-07-02"], "quarterly", 4),
    ],
)
def test_detect_cadence(ex_dates, expected_label, expected_ppy):
    events = [{"ex_date": d, "amount": 0.1} for d in ex_dates]
    label, ppy = ic.detect_cadence(events)
    assert label == expected_label
    assert ppy == expected_ppy


def test_detect_cadence_defaults_weekly_when_thin():
    assert ic.detect_cadence([]) == ("weekly", 52)
    assert ic.detect_cadence([{"ex_date": "2026-01-01", "amount": 0.1}]) == ("weekly", 52)


# ---------------------------------------------------------------------------
# 4. Capture ratio aggregation + confidence weighting
# ---------------------------------------------------------------------------
def test_capture_ratio_high_confidence_returns_fund_median():
    events = [{"ratio": 0.60 + 0.01 * i} for i in range(14)]
    out = ic.compute_capture_ratio(events, cross_fund_ratio=0.90)
    assert out["fund_ratio_confidence"] == "high"
    assert out["fund_ratio_median"] == pytest.approx(0.665, abs=1e-3)
    # blended_ratio_used == fund_ratio_median at full confidence
    assert out["blended_ratio_used"] == pytest.approx(out["fund_ratio_median"], abs=1e-9)


def test_capture_ratio_medium_confidence_blends_with_prior():
    events = [{"ratio": 0.5} for _ in range(6)]
    out = ic.compute_capture_ratio(events, cross_fund_ratio=0.9)
    assert out["fund_ratio_confidence"] == "medium"
    # weight = 6/12 = 0.5; blended = 0.5 * 0.5 + 0.5 * 0.9 = 0.7
    assert out["blended_ratio_used"] == pytest.approx(0.7, abs=1e-9)


def test_capture_ratio_no_history_returns_prior():
    out = ic.compute_capture_ratio([], cross_fund_ratio=0.65)
    assert out["fund_ratio_confidence"] == "none"
    assert out["blended_ratio_used"] == pytest.approx(0.65, abs=1e-9)
    assert out["fund_ratio_median"] is None


# ---------------------------------------------------------------------------
# 5. End-to-end ``build_income_calibration_row``
# ---------------------------------------------------------------------------
def test_build_income_calibration_row_full_flow():
    today = dt.date(2026, 5, 23)
    events = _mk_events([
        ("2026-03-06", 0.080),
        ("2026-03-13", 0.075),
        ("2026-03-20", 0.072),
        ("2026-03-27", 0.070),
        ("2026-04-03", 0.072),
        ("2026-04-10", 0.071),
        ("2026-04-17", 0.092),
        ("2026-04-24", 0.093),
        ("2026-05-01", 0.086),
        ("2026-05-08", 0.088),
        ("2026-05-15", 0.070),
        ("2026-05-22", 0.064),
    ])
    nav_series = _mk_nav([
        ("2026-03-05", 4.50),
        ("2026-04-01", 4.30),
        ("2026-05-01", 4.20),
        ("2026-05-22", 4.20),
    ])
    block = ic.build_income_calibration_row(
        "MTYY",
        events,
        nav_series,
        current_sigma=0.674,
        today=today,
    )
    assert block is not None
    assert block["cadence_label"] == "weekly"
    assert block["periods_per_year"] == 52
    assert block["events_used"] == 12
    assert block["fund_ratio_confidence"] == "high"
    assert block["nav_missing_count"] == 0
    # Template captures the last 12 events
    assert len(block["template_yields"]) == 12
    # Run-rate ? median weekly  52
    assert block["run_rate_annual_display"] is not None
    assert 0.5 < block["run_rate_annual_display"] < 1.2
    # Latest event references the last entry
    assert block["latest_event"]["ex_date"] == "2026-05-22"


def test_build_income_calibration_row_returns_none_for_no_events():
    block = ic.build_income_calibration_row(
        "EMPTY", [], [], current_sigma=0.5, today=dt.date(2026, 5, 23)
    )
    assert block is None


def test_build_income_calibration_row_thin_history_blends_with_prior():
    """CWY-style fixture: 5 events ? medium confidence, blended ratio."""
    today = dt.date(2026, 5, 23)
    events = _mk_events([
        ("2026-04-17", 0.49),
        ("2026-04-24", 0.49),
        ("2026-05-01", 0.45),
        ("2026-05-08", 0.47),
        ("2026-05-15", 0.35),
    ])
    nav_series = _mk_nav([
        ("2026-04-16", 28.0),
        ("2026-05-15", 22.0),
        ("2026-05-23", 21.5),
    ])
    block = ic.build_income_calibration_row(
        "CWY",
        events,
        nav_series,
        current_sigma=0.93,
        cross_fund_ratio=0.65,
        today=today,
    )
    assert block is not None
    assert block["fund_ratio_confidence"] == "medium"
    # 5/12 blend: 5/12 of fund ratio + 7/12 of prior
    assert block["blended_ratio_used"] is not None
    assert 0.5 < block["blended_ratio_used"] < 1.0


# ---------------------------------------------------------------------------
# 6. Cross-fund self-calibration
# ---------------------------------------------------------------------------
def test_derive_cross_fund_ratio_uses_high_confidence_funds():
    blocks = {
        "A": {"fund_ratio_median": 0.60, "events_used": 20},
        "B": {"fund_ratio_median": 0.70, "events_used": 18},
        "C": {"fund_ratio_median": 0.65, "events_used": 14},
        "D": {"fund_ratio_median": 1.50, "events_used": 3},  # below threshold
    }
    out = ic.derive_cross_fund_ratio(blocks, min_events=12, fallback=0.5)
    assert out == pytest.approx(0.65, abs=1e-6)


def test_derive_cross_fund_ratio_falls_back_when_too_few_qualifying():
    blocks = {
        "A": {"fund_ratio_median": 0.60, "events_used": 4},
        "B": {"fund_ratio_median": 0.70, "events_used": 5},
    }
    out = ic.derive_cross_fund_ratio(blocks, min_events=12, fallback=0.65)
    assert out == 0.65


# ---------------------------------------------------------------------------
# 7. Legacy field rewriting
# ---------------------------------------------------------------------------
def test_build_legacy_yield_fields_rewrites_with_corrected_semantics():
    block = {
        "run_rate_annual_display": 0.78,
        "periods_per_year": 52,
        "events_used": 12,
        "latest_event": {
            "ex_date": "2026-05-22",
            "amount": 0.064,
            "yield_frac": 0.01507,
        },
    }
    out = ic.build_legacy_yield_fields(block)
    assert out["income_yield_trailing_annual"] == pytest.approx(0.78, abs=1e-9)
    # latest weekly  52 ? 0.7836
    assert out["income_yield_recent_annual"] == pytest.approx(0.78364, rel=1e-4)
    assert out["income_distribution_count_1y"] == 12
    assert out["income_latest_distribution"] == pytest.approx(0.064, abs=1e-9)
    assert out["income_latest_ex_date"] == "2026-05-22"


def test_build_legacy_yield_fields_returns_empty_when_no_block():
    assert ic.build_legacy_yield_fields(None) == {}


# ---------------------------------------------------------------------------
# 8. Top-level ``build_all_calibrations`` plumbing
# ---------------------------------------------------------------------------
def test_build_all_calibrations_filters_to_yieldboost_symbols(tmp_path):
    metrics_csv = tmp_path / "metrics.csv"
    metrics_csv.write_text(
        "ticker,date,nav,close_price\n"
        "MTYY,2026-05-01,4.30,4.30\n"
        "MTYY,2026-05-22,4.25,4.20\n"
        "FOO,2026-05-22,10.0,10.0\n"
    )
    payload = {
        "by_symbol": {
            "MTYY": [
                {"ex_date": "2026-05-01", "amount": 0.086},
                {"ex_date": "2026-05-22", "amount": 0.064},
            ],
            "FOO": [
                {"ex_date": "2026-05-22", "amount": 0.20},
            ],
        }
    }
    blocks, prior = ic.build_all_calibrations(
        payload,
        metrics_csv,
        sigma_by_symbol={"MTYY": 0.674, "FOO": 0.30},
        cross_fund_ratio=0.65,
        self_calibrate=True,
        yieldboost_symbols={"MTYY"},
    )
    assert "MTYY" in blocks
    assert "FOO" not in blocks
    # With only 2 events MTYY is low confidence, so prior stays at fallback
    assert prior == pytest.approx(0.65, abs=1e-6)


def test_build_all_calibrations_self_calibrates_from_fleet(tmp_path):
    metrics_csv = tmp_path / "metrics.csv"
    rows = ["ticker,date,nav,close_price"]
    for sym in ("A", "B", "C", "D"):
        for week in range(20):
            date_ = (dt.date(2026, 1, 1) + dt.timedelta(weeks=week)).isoformat()
            rows.append(f"{sym},{date_},10.0,10.0")
    metrics_csv.write_text("\n".join(rows) + "\n")
    by_symbol = {}
    for sym, payout in (("A", 0.20), ("B", 0.22), ("C", 0.18), ("D", 0.21)):
        by_symbol[sym] = [
            {
                "ex_date": (dt.date(2026, 1, 1) + dt.timedelta(weeks=w)).isoformat(),
                "amount": payout,
            }
            for w in range(20)
        ]
    sigma = 0.8
    blocks, prior = ic.build_all_calibrations(
        {"by_symbol": by_symbol},
        metrics_csv,
        sigma_by_symbol={"A": sigma, "B": sigma, "C": sigma, "D": sigma},
        cross_fund_ratio=0.99,  # absurd seed; should be replaced
        self_calibrate=True,
        yieldboost_symbols={"A", "B", "C", "D"},
    )
    assert len(blocks) == 4
    # Prior should reflect the actual fleet median of fund ratios (well below 0.99)
    assert prior < 0.9
    # Computed prior is itself the median fund ratio
    medians = sorted(b["fund_ratio_median"] for b in blocks.values())
    expected_prior = (medians[1] + medians[2]) / 2
    assert prior == pytest.approx(expected_prior, abs=1e-3)

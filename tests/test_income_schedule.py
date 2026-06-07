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


def test_normalize_events_reverse_split_yahoo_restated():
    split_ctx = {
        "mode": "discrete_split",
        "boundary": dt.date(2026, 6, 2),
        "mult": 8.0,
    }
    events = _mk_events([("2026-05-29", 0.24)])
    nav_series = _mk_nav([("2026-05-29", 3.09)])
    normalized, _ = ic.normalize_events(
        events,
        nav_series,
        current_sigma=0.8,
        split_ctx=split_ctx,
    )
    assert normalized[0]["amount_economic"] == pytest.approx(0.03, rel=1e-3)
    assert normalized[0]["amount_basis"] == "yahoo_restated"
    assert normalized[0]["yield_frac"] == pytest.approx(0.03 / 3.09, rel=1e-3)


def test_capture_ratio_uses_cross_fund_after_recent_split_no_post_events():
    today = dt.date(2026, 6, 7)
    events = _mk_events([
        ("2026-05-01", 0.48),
        ("2026-05-08", 0.40),
        ("2026-05-15", 0.39),
        ("2026-05-22", 0.28),
        ("2026-05-29", 0.24),
    ])
    nav_series = _mk_nav([
        ("2026-05-01", 3.17),
        ("2026-05-29", 3.09),
        ("2026-06-02", 22.99),
    ])
    split_ctx = {
        "mode": "discrete_split",
        "boundary": dt.date(2026, 6, 2),
        "mult": 8.0,
    }
    block = ic.build_income_calibration_row(
        "TSYY",
        events,
        nav_series,
        current_sigma=0.70,
        today=today,
        split_events=[(dt.date(2026, 6, 2), 8.0)],
    )
    assert block is not None
    assert block["blended_ratio_used"] == pytest.approx(ic.DEFAULT_CROSS_FUND_RATIO, rel=1e-6)
    assert block["fund_ratio_confidence"] == "none"


def test_tsyy_calibration_sane_after_split_basis_fix():
    today = dt.date(2026, 6, 7)
    events = _mk_events([
        ("2026-05-15", 0.392),
        ("2026-05-22", 0.280),
        ("2026-05-29", 0.240),
    ])
    nav_series = _mk_nav([
        ("2026-05-15", 3.11),
        ("2026-05-29", 3.09),
        ("2026-06-02", 22.99),
    ])
    block = ic.build_income_calibration_row(
        "TSYY",
        events,
        nav_series,
        current_sigma=0.463,
        today=today,
        split_events=[(dt.date(2026, 6, 2), 8.0)],
    )
    assert block is not None
    assert block["blended_ratio_used"] <= ic.MAX_BLENDED_RATIO_AFTER_SPLIT
    pre = [e for e in block["events_recent"] if e["ex_date"] < "2026-06-02"]
    assert pre
    assert max(e["yield_frac"] for e in pre) <= ic.MAX_WEEKLY_YIELD_FRAC


def test_run_rate_uses_post_split_nav_after_reverse_split_jump():
    """Regression: MTYY/TSYY-style reverse split must not annualize pre-split yields."""
    today = dt.date(2026, 6, 5)
    events = _mk_events([
        ("2026-05-01", 0.516),
        ("2026-05-08", 0.528),
        ("2026-05-15", 0.420),
        ("2026-05-22", 0.384),
        ("2026-05-29", 0.378),
    ])
    nav_series = _mk_nav([
        ("2026-05-01", 4.40),
        ("2026-05-29", 4.00),
        ("2026-06-01", 3.93),
        ("2026-06-02", 23.00),
        ("2026-06-03", 22.80),
    ])
    metric_rows = [
        {"date": d, "nav": n, "close_price": n}
        for d, n in nav_series
    ]
    split_events = [(dt.date(2026, 6, 2), 6.0)]
    block = ic.build_income_calibration_row(
        "MTYY",
        events,
        nav_series,
        current_sigma=0.70,
        today=today,
        metric_rows=metric_rows,
        split_events=split_events,
    )
    assert block is not None
    assert block["run_rate_basis"] == "post_split_latest_nav"
    assert block["run_rate_annual_display"] == pytest.approx(0.378 / 22.80 * 52, rel=0.02)
    assert block["run_rate_annual_display"] < ic.MAX_RUN_RATE_ANNUAL
    legacy = ic.build_legacy_yield_fields(block)
    assert legacy["income_yield_trailing_annual"] == block["run_rate_annual_display"]


def test_infer_split_events_from_metric_rows_detects_six_for_one():
    rows = [
        {"date": "2026-06-01", "nav": 3.93, "close_price": 3.93},
        {"date": "2026-06-02", "nav": 23.00, "close_price": 23.00},
    ]
    events = ic.infer_split_events_from_metric_rows(rows)
    assert events
    assert events[0][0] == dt.date(2026, 6, 2)
    assert events[0][1] == pytest.approx(6.0, rel=0.05)


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
    # Run-rate ? median weekly � 52
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
    # latest weekly � 52 ? 0.7836
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


# ---------------------------------------------------------------------------
# Forward pair-trade P&L + inverse-variance blend (decisions A3 + B2 + C2)
# ---------------------------------------------------------------------------
def test_band_to_sigma_matches_normal_identity():
    sig = ic.band_to_sigma(0.20, 0.40)
    assert sig is not None
    # (p90 - p10) / 2 = 1.2816 * sigma  =>  sigma = 0.20 / 2.5631
    assert sig == pytest.approx(0.20 / (2.0 * 1.2815515655446004), rel=1e-9)
    assert ic.band_to_sigma(None, 0.40) is None
    assert ic.band_to_sigma(0.40, 0.40) is None
    assert ic.band_to_sigma(float("nan"), 0.40) is None


def test_inverse_variance_blend_limits():
    res = ic.inverse_variance_blend(
        mu_forward=0.30, sigma_forward=0.10,
        mu_realized=0.10, sigma_realized=0.10,
    )
    assert res is not None
    # equal sigmas -> w_F = 0.5 exactly
    assert res["weight_forward"] == pytest.approx(0.5, abs=1e-12)
    assert res["posterior_mean"] == pytest.approx(0.20, abs=1e-12)
    assert res["method"] == "inverse_variance"

    # sigma_F -> 0 forces full forward weight
    res_f = ic.inverse_variance_blend(
        mu_forward=0.30, sigma_forward=1e-9,
        mu_realized=0.10, sigma_realized=0.10,
    )
    assert res_f["weight_forward"] > 0.999
    assert res_f["posterior_mean"] == pytest.approx(0.30, abs=1e-6)

    # sigma_R -> 0 forces full realized weight
    res_r = ic.inverse_variance_blend(
        mu_forward=0.30, sigma_forward=0.10,
        mu_realized=0.10, sigma_realized=1e-9,
    )
    assert res_r["weight_forward"] < 1e-3
    assert res_r["posterior_mean"] == pytest.approx(0.10, abs=1e-6)

    # No forward band (sigma_F=None) -> anchor-shift fallback (E2): treat
    # forward as a confident point estimate -> w_F = 1, posterior = mu_F.
    res_only_r = ic.inverse_variance_blend(
        mu_forward=0.30, sigma_forward=None,
        mu_realized=0.10, sigma_realized=0.10,
    )
    assert res_only_r["method"] == "anchor_shift_fallback"
    assert res_only_r["weight_forward"] == 1.0
    assert res_only_r["posterior_mean"] == pytest.approx(0.30, abs=1e-9)

    # No realized sigma (sigma_R=None) but forward band present: also fall
    # back to anchor-shift (realized treated as low-confidence).
    res_only_f = ic.inverse_variance_blend(
        mu_forward=0.30, sigma_forward=0.10,
        mu_realized=0.10, sigma_realized=None,
    )
    assert res_only_f["method"] == "anchor_shift_fallback"
    assert res_only_f["weight_forward"] == 1.0
    assert res_only_f["posterior_mean"] == pytest.approx(0.30, abs=1e-9)

    # Both sigmas missing: anchor-shift fallback to forward.
    res_neither = ic.inverse_variance_blend(
        mu_forward=0.30, sigma_forward=None,
        mu_realized=0.10, sigma_realized=None,
    )
    assert res_neither["method"] == "anchor_shift_fallback"
    assert res_neither["weight_forward"] == 1.0


def test_expected_pair_pnl_simple_diagnostics_match_research(research):
    """Magis simple-return diagnostics on ``expected_pair_pnl_annual`` must
    still reproduce the research closed form within tolerance, even though
    they are no longer the headline forward (schema_v=4 ships the MC
    log-axis quantiles instead).

    pair_simple = nav_decay_simple - distributions_simple = net_short + borrow.
    """
    tol = float(research["tolerances"]["net_short_pp"])
    for candidate in research["candidates"]:
        sigma = float(candidate["sigma_annual"])
        actual_d_weekly = float(candidate["actual_distribution_weekly"])
        dashboard_bs_weekly = ic.expected_put_spread_loss_weekly(0.0, sigma, 1.0)
        assert dashboard_bs_weekly > 0, candidate["etf"]
        ratio = actual_d_weekly / dashboard_bs_weekly
        calibration = {
            "blended_ratio_used": ratio,
            "fund_ratio_median": ratio,
            "fund_ratio_confidence": "high",
        }
        result = ic.expected_pair_pnl_annual(
            calibration=calibration,
            sigma_annual=sigma,
            beta=0.4,
            horizon_years=1.0,
            n_paths=2_000,
            seed=ic.stable_seed_from_symbol(candidate["etf"]),
        )
        assert result is not None, candidate["etf"]
        pair_simple = result["nav_decay_simple_annual"] - result["distributions_simple_annual"]
        expected_pair = float(candidate["net_short_annual"]) + float(candidate["borrow_annual"])
        assert abs(pair_simple - expected_pair) <= tol, (
            f"{candidate['etf']}: Magis pair simple {pair_simple:.3f} vs research "
            f"{expected_pair:.3f}"
        )


# ---------------------------------------------------------------------------
# 9. Weekly-rebalanced compound MC headline (schema_v=4)
# ---------------------------------------------------------------------------
MTYY_BASE = dict(
    sigma_annual=0.674, mu_annual=0.0, beta=0.372, capture_ratio=0.80,
    expense_ratio_annual=0.0099, borrow_annual=0.0,
)


def test_simulate_mc_quantiles_strictly_monotone_and_finite():
    out = ic.simulate_weekly_compound_pair_pnl(
        **MTYY_BASE, weeks=52, n_paths=10_000,
        seed=ic.stable_seed_from_symbol("MTYY"),
    )
    assert out is not None
    for k in ("p10_log", "p25_log", "p50_log", "p75_log", "p90_log",
              "mean_log", "std_log"):
        assert math.isfinite(out[k]), k
    assert out["p10_log"] < out["p25_log"] < out["p50_log"] < out["p75_log"] < out["p90_log"]
    assert out["std_log"] > 0
    assert out["axis"] == "log_continuous_annual"
    assert out["basis"] == "weekly_rebalanced_compound"


def test_simulate_mc_mtyy_anchor_within_band():
    """MTYY (sigma=0.674, beta=0.372, capR=0.80) headline p50 with explicit
    distribution debits (no wash). Regression anchor for sigma-regime triplet.
    """
    out = ic.simulate_weekly_compound_pair_pnl(
        **MTYY_BASE, weeks=52, n_paths=20_000,
        seed=ic.stable_seed_from_symbol("MTYY"),
    )
    assert 0.05 <= out["p50_log"] <= 0.35, out["p50_log"]
    assert -0.15 <= out["p10_log"] <= 0.15, out["p10_log"]
    assert 0.25 <= out["p90_log"] <= 0.55, out["p90_log"]
    assert abs(out["mean_log"] - out["p50_log"]) < 0.10
    assert out["distributions_annual"] > 0.5


def test_simulate_mc_capture_ratio_lowers_p50_via_distributions():
    """Higher capture_ratio => larger weekly distribution debit => lower p50."""
    seed = ic.stable_seed_from_symbol("MTYY")
    low = ic.simulate_weekly_compound_pair_pnl(
        sigma_annual=0.674, mu_annual=0.0, beta=0.372, capture_ratio=0.50,
        expense_ratio_annual=0.0099, borrow_annual=0.0,
        weeks=52, n_paths=5_000, seed=seed,
    )
    high = ic.simulate_weekly_compound_pair_pnl(
        sigma_annual=0.674, mu_annual=0.0, beta=0.372, capture_ratio=1.00,
        expense_ratio_annual=0.0099, borrow_annual=0.0,
        weeks=52, n_paths=5_000, seed=seed,
    )
    assert low["p50_log"] > high["p50_log"]
    assert (low["p50_log"] - high["p50_log"]) > 0.3


def test_simulate_mc_zero_capture_ratio_matches_no_distribution_drag():
    """capture_ratio=0 removes distribution debit; p50 near old wash anchor."""
    out = ic.simulate_weekly_compound_pair_pnl(
        sigma_annual=0.674, mu_annual=0.0, beta=0.372, capture_ratio=0.0,
        expense_ratio_annual=0.0099, borrow_annual=0.0,
        weeks=52, n_paths=20_000, seed=ic.stable_seed_from_symbol("MTYY"),
    )
    assert 1.05 <= out["p50_log"] <= 1.35, out["p50_log"]
    assert out["distributions_annual"] == pytest.approx(0.0, abs=1e-12)


def test_simulate_mc_sigma_zero_limit_collapses_to_er_minus_borrow():
    """At sigma -> 0 the put-spread is always OTM (L_t = 0), and the
    underlying is deterministic at mu = 0, so per-week pair = ER/52 -
    borrow/52. Annualized log return -> ER - borrow.
    """
    out = ic.simulate_weekly_compound_pair_pnl(
        sigma_annual=1e-4, mu_annual=0.0, beta=0.4, capture_ratio=0.5,
        expense_ratio_annual=0.0099, borrow_annual=0.0,
        weeks=52, n_paths=2_000, seed=42,
    )
    assert out["p50_log"] == pytest.approx(0.0099, abs=0.0005)

    out_b = ic.simulate_weekly_compound_pair_pnl(
        sigma_annual=1e-4, mu_annual=0.0, beta=0.4, capture_ratio=0.5,
        expense_ratio_annual=0.0099, borrow_annual=0.05,
        weeks=52, n_paths=2_000, seed=42,
    )
    assert out_b["p50_log"] == pytest.approx(0.0099 - 0.05, abs=0.0005)


def test_simulate_mc_borrow_lowers_p50_at_forecast_sigma():
    """Higher borrow -> lower p50 at MTYY-like parameters."""
    seeds = ic.stable_seed_from_symbol("MTYY")
    low = ic.simulate_weekly_compound_pair_pnl(
        sigma_annual=0.674, mu_annual=0.0, beta=0.372, capture_ratio=0.8,
        expense_ratio_annual=0.0099, borrow_annual=0.0,
        weeks=52, n_paths=5_000, seed=seeds,
    )
    high = ic.simulate_weekly_compound_pair_pnl(
        sigma_annual=0.674, mu_annual=0.0, beta=0.372, capture_ratio=0.8,
        expense_ratio_annual=0.0099, borrow_annual=0.05,
        weeks=52, n_paths=5_000, seed=seeds,
    )
    assert low["p50_log"] > high["p50_log"]
    assert high["p50_log"] == pytest.approx(low["p50_log"] - 0.05, abs=0.02)


def test_simulate_mc_sigma_monotone_p50_without_distributions():
    """Without distribution debits (capture_ratio=0), higher vol -> more
    put-spread payoff -> strictly higher p50 (gamma scalp only).
    """
    seeds = ic.stable_seed_from_symbol("MTYY")
    sigmas = [0.674 * k for k in (0.5, 0.7, 1.0, 1.3, 1.5)]
    p50s = []
    for s in sigmas:
        out = ic.simulate_weekly_compound_pair_pnl(
            sigma_annual=s, mu_annual=0.0, beta=0.372, capture_ratio=0.0,
            expense_ratio_annual=0.0099, borrow_annual=0.0,
            weeks=52, n_paths=5_000, seed=seeds,
        )
        p50s.append(out["p50_log"])
    diffs = [p50s[i + 1] - p50s[i] for i in range(len(p50s) - 1)]
    assert all(d > 0 for d in diffs), p50s


def test_simulate_mc_sigma_increases_p50_through_mid_band_with_distributions():
    """With distribution debits, gamma scalp dominates through 1.0x sigma;
    high-sigma tail may flatten as d_weekly scales with BS premium.
    """
    seeds = ic.stable_seed_from_symbol("MTYY")
    sigmas = [0.674 * k for k in (0.5, 0.7, 1.0)]
    p50s = []
    for s in sigmas:
        out = ic.simulate_weekly_compound_pair_pnl(
            sigma_annual=s, mu_annual=0.0, beta=0.372, capture_ratio=0.8,
            expense_ratio_annual=0.0099, borrow_annual=0.0,
            weeks=52, n_paths=5_000, seed=seeds,
        )
        p50s.append(out["p50_log"])
    assert p50s[1] > p50s[0] and p50s[2] > p50s[1], p50s
    out_hi = ic.simulate_weekly_compound_pair_pnl(
        sigma_annual=0.674 * 1.5, mu_annual=0.0, beta=0.372, capture_ratio=0.8,
        expense_ratio_annual=0.0099, borrow_annual=0.0,
        weeks=52, n_paths=5_000, seed=seeds,
    )
    assert out_hi["p50_log"] > p50s[0]


def test_simulate_mc_reproducible():
    """Same seed -> bit-identical output. Different seed -> different draws."""
    a = ic.simulate_weekly_compound_pair_pnl(**MTYY_BASE, weeks=52, n_paths=2_000, seed=11)
    b = ic.simulate_weekly_compound_pair_pnl(**MTYY_BASE, weeks=52, n_paths=2_000, seed=11)
    c = ic.simulate_weekly_compound_pair_pnl(**MTYY_BASE, weeks=52, n_paths=2_000, seed=12)
    assert a["p50_log"] == b["p50_log"]
    assert a["mean_log"] == b["mean_log"]
    assert a["p50_log"] != c["p50_log"]


def test_simulate_mc_invalid_inputs_return_none():
    assert ic.simulate_weekly_compound_pair_pnl(sigma_annual=None) is None
    assert ic.simulate_weekly_compound_pair_pnl(sigma_annual=-0.5) is None
    assert ic.simulate_weekly_compound_pair_pnl(sigma_annual=float("nan")) is None
    assert ic.simulate_weekly_compound_pair_pnl(sigma_annual=0.5, beta=float("nan")) is None


def test_stable_seed_from_symbol_is_deterministic():
    a = ic.stable_seed_from_symbol("MTYY")
    b = ic.stable_seed_from_symbol("mtyy")
    c = ic.stable_seed_from_symbol("MTYY", salt=1)
    d = ic.stable_seed_from_symbol("FBYY")
    assert a == b  # case-insensitive
    assert a != c  # salt differentiates
    assert a != d  # different ticker
    assert 0 <= a < 2 ** 31


# ---------------------------------------------------------------------------
# 10. Scenario grid (sigma_multiplier x drift)
# ---------------------------------------------------------------------------
def test_scenario_grid_shape_and_axes():
    grid = ic.scenario_grid_pair_pnl(
        sigma_annual=0.674, beta=0.372, capture_ratio=0.80,
        n_paths=2_000, seed=ic.stable_seed_from_symbol("MTYY"),
    )
    assert grid is not None
    assert grid["sigma_multipliers"] == [0.5, 0.7, 1.0, 1.3, 1.5]
    assert grid["drifts"] == [-0.5, -0.25, 0.0, 0.25, 0.5]
    assert len(grid["p50_log_grid"]) == 5
    for row in grid["p50_log_grid"]:
        assert len(row) == 5
        for v in row:
            assert math.isfinite(v)
    assert grid["axis"] == "log_continuous_annual"
    assert grid["engine"] == "yieldboost_mc"


def test_scenario_grid_sigma_axis_is_monotone_without_distributions():
    """capture_ratio=0: p50 strictly increases in sigma (gamma only)."""
    grid = ic.scenario_grid_pair_pnl(
        sigma_annual=0.674, beta=0.372, capture_ratio=0.0,
        n_paths=2_500, seed=ic.stable_seed_from_symbol("MTYY"),
    )
    drift_idx = grid["drifts"].index(0.0)
    col = [row[drift_idx] for row in grid["p50_log_grid"]]
    assert all(col[i + 1] > col[i] for i in range(len(col) - 1)), col


def test_scenario_grid_sigma_axis_increases_through_mid_band_with_distributions():
    """With distribution debits, p50 rises through 1.0x sigma; high tail may flatten."""
    grid = ic.scenario_grid_pair_pnl(
        sigma_annual=0.674, beta=0.372, capture_ratio=0.80,
        n_paths=2_500, seed=ic.stable_seed_from_symbol("MTYY"),
    )
    drift_idx = grid["drifts"].index(0.0)
    col = [row[drift_idx] for row in grid["p50_log_grid"]]
    assert col[1] > col[0] and col[2] > col[1], col
    assert col[-1] > col[0]


def test_scenario_grid_returns_none_for_bad_inputs():
    assert ic.scenario_grid_pair_pnl(sigma_annual=None, beta=0.5) is None
    assert ic.scenario_grid_pair_pnl(sigma_annual=0.5, beta=None) is None
    assert ic.scenario_grid_pair_pnl(sigma_annual=-0.1, beta=0.5) is None


def test_put_spread_grid_shape_and_center_anchor():
    anchor = 0.79
    grid = ic.scenario_grid_put_spread_pair(
        sigma_annual=0.674,
        beta=0.372,
        capture_ratio=0.80,
        gross_anchor_p50=anchor,
    )
    assert grid is not None
    assert grid["engine"] == "yieldboost_put_spread_structural"
    assert len(grid["p50_log_grid"]) == 5
    assert len(grid["p50_log_grid"][0]) == 5
    mid = grid["p50_log_grid"][2][2]
    assert mid is not None
    assert abs(float(mid) - anchor) < 1e-6


def test_put_spread_grid_sigma_axis_monotone_at_flat_drift():
    grid = ic.scenario_grid_put_spread_pair(
        sigma_annual=0.674,
        beta=0.372,
        capture_ratio=0.80,
        gross_anchor_p50=0.79,
    )
    assert grid is not None
    drift_idx = grid["drifts"].index(0.0)
    col = [row[drift_idx] for row in grid["p50_log_grid"]]
    assert col[1] > col[0] and col[2] > col[1], col


def test_put_spread_grid_returns_none_for_bad_inputs():
    assert ic.scenario_grid_put_spread_pair(sigma_annual=None, beta=0.5) is None
    assert ic.scenario_grid_put_spread_pair(sigma_annual=0.5, beta=None) is None


# ---------------------------------------------------------------------------
# 11. expected_pair_pnl_annual: log-axis headline + simple-return diagnostics
# ---------------------------------------------------------------------------
def test_expected_pair_pnl_annual_returns_log_axis_headline():
    calibration = {"blended_ratio_used": 0.80, "fund_ratio_confidence": "high"}
    out = ic.expected_pair_pnl_annual(
        calibration=calibration,
        sigma_annual=0.674,
        beta=0.372,
        horizon_years=1.0,
        n_paths=5_000,
        seed=ic.stable_seed_from_symbol("MTYY"),
    )
    assert out is not None
    for k in ("p10_log", "p50_log", "p90_log", "mean_log", "std_log",
              "nav_decay_simple_annual", "distributions_simple_annual"):
        assert k in out
    assert out["p10_log"] < out["p50_log"] < out["p90_log"]
    assert out["axis"] == "log_continuous_annual"
    assert out["basis"] == "weekly_rebalanced_compound"
    # Magis diagnostics are simple returns (always between 0 and ~1)
    assert 0.0 < out["nav_decay_simple_annual"] < 1.5
    assert 0.0 <= out["distributions_simple_annual"] < 1.5


def test_expected_pair_pnl_annual_requires_beta():
    calibration = {"blended_ratio_used": 0.80, "fund_ratio_confidence": "high"}
    out = ic.expected_pair_pnl_annual(
        calibration=calibration, sigma_annual=0.674,
        beta=None, n_paths=200, seed=1,
    )
    assert out is None

"""scripts/income_schedule.py
=================================

NAV-normalized distribution calibration **and** weekly-rebalanced
compound pair-P&L Monte Carlo for YieldBOOST income ETFs.

There are two distinct mathematical objects in this file. Don't confuse
them:

1. **Magis closed form (calibration helpers).**  ``q = 1 - L - f/52``,
   ``NAV decay = 1 - q^N``, ``distributions = d * (1 - q^N)/(1 - q)``.
   This is the *buy-and-hold* short pair P&L for one share held one
   year as the short notional bleeds with NAV. It is used here only
   for the per-event NAV-normalized capture-ratio diagnostics
   (``income_distribution_calibration`` block) and for the
   ``nav_decay_simple_annual`` / ``distributions_simple_annual`` panel
   diagnostics that the Income Scenarios cash-projection engine still
   consumes. **It is no longer the dashboard's headline forward
   pair-P&L number.**

2. **Weekly-rebalanced compound MC (this module's headline forward
   model, schema_v=4).**  Each week we draw a lognormal underlying
   return, evaluate the realised 95/88 put-spread on the 2x sleeve
   ``L_t``, mark the pair (short ETF + beta*long underlying) to
   current equity, and compound:

       d_weekly = capture_ratio * E[put_spread_loss_weekly](0, sigma, 1)
       pair_w   = L_t + ER/52 - borrow/52 - d_weekly + beta * r_und_t
       equity  *= (1 + pair_w)
       pair_log_annual = ln(equity_W) * 52 / W

   **Short distributions** are debited explicitly each week (cash owed
   to the lender). ``capture_ratio`` scales calibrated weekly cash via
   the NAV-normalized yield / BS-premium ratio. This replaces the
   prior "distributions wash" assumption (ex-date price drop = full
   distribution), which overstated edge on high-payout names.

   See ``simulate_weekly_compound_pair_pnl`` and
   ``scenario_grid_pair_pnl`` below; both are deterministic in a
   per-symbol seed via ``stable_seed_from_symbol``.

The NAV-normalized capture-ratio calibration is unchanged (see
``build_income_calibration_row``):

    yield_i  = amount_i / NAV_at_ex_i
    bs_i     = expected_put_spread_loss_weekly(0, sigma_at_ex_i, 1)
    ratio_i  = yield_i / bs_i             # ~0.65 cross-fund per research

When fund-specific history is thin (CWY, new launches) we blend the
fund ratio toward a cross-fund prior. The capture ratio feeds the MC
via ``d_weekly`` and is still consumed by the Income Scenarios cash-
projection helper and Magis simple-return diagnostics.

References
----------
* Magis Capital Partners (April 2026), *Bucket 2 Income ETF Structural
  Decay* (HTML, repo: ``GraniteShares YieldBOOST/Bucket2_Income_ETF_Decay_Research.html``).
* Cross-fund tests: ``tests/fixtures/bucket2_research.json``.
"""
from __future__ import annotations

import datetime as dt
import json
import math
import statistics
import zlib
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd

from price_basis import detect_split_boundary, parse_split_events_from_corp, resolve_split_context
from split_adjustments import (
    DEFAULT_CORPORATE_ACTIONS_PATH,
    dedupe_split_events,
    load_split_events_for_ticker,
    nearest_split_ratio,
)

# ?????????????????????????????????????????????????????????????????
# Constants - mirror ``assets/income_scenario.js`` exactly.
# ?????????????????????????????????????????????????????????????????
PUT_SPREAD_SHORT_STRIKE = 0.95
PUT_SPREAD_LONG_STRIKE = 0.88
PUT_SPREAD_LEVERAGE = 2.0
DEFAULT_EXPENSE_RATIO_ANNUAL = 0.0099
WEEKS_PER_YEAR = 52

# Bucket 2 research -1: median capture ratio across 8 candidates is 0.635
# under risk-neutral BS pricing. The dashboard uses physical-measure leveraged
# drift, which gives ~5% higher BS reference; we round the prior to 0.65 so
# the constant stays close to both interpretations and absorbs measurement
# noise. ``derive_cross_fund_ratio`` will override this with a fleet-median
# whenever ?3 high-confidence funds exist.
DEFAULT_CROSS_FUND_RATIO = 0.65

# Confidence tiers (count of NAV-normalized events used).
RATIO_CONFIDENCE_FULL_N = 12
RATIO_CONFIDENCE_MIN_N = 4

# Annual additive on closed-form NAV decay to close the lognormal-vs-Student-t
# gap. Off by default; expose as a build flag for A/B vs MC.
DEFAULT_TAIL_ADJUSTMENT_ANNUAL = 0.0

# Trailing window over which capture ratio is computed.
TRAILING_WINDOW_DAYS = 365

# Sanity caps — pre-split NAV + unchanged $/share distributions can imply
# >500% run-rates after reverse splits; never ship those to Exp. ETF return.
MAX_WEEKLY_YIELD_FRAC = 0.05
MAX_RUN_RATE_ANNUAL = 1.5
POST_SPLIT_RUN_RATE_LOOKBACK_DAYS = 120
POST_SPLIT_MIN_EVENTS_FOR_MEDIAN = 4


# ?????????????????????????????????????????????????????????????????
# Helpers - keep deliberately small / dependency-light.
# ?????????????????????????????????????????????????????????????????
def _norm_sym(s: object) -> str:
    return str(s or "").strip().upper().replace(".", "-")


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def expected_put_spread_loss_weekly(
    underlying_return: float,
    sigma_annual: float,
    horizon_years: float = 1.0,
) -> float:
    """Expected weekly put-spread loss on the 2- sleeve.

    Mirror of ``expectedPutSpreadLossWeekly`` in ``index.html`` /
    ``assets/income_scenario.js``. Returns ``NaN`` on invalid input so the
    caller can decide whether to fall back to a prior.
    """
    u = float(underlying_return)
    sigma = float(sigma_annual)
    t = float(horizon_years)
    if not math.isfinite(u) or u <= -0.9999:
        return float("nan")
    if not math.isfinite(sigma) or sigma <= 0:
        return float("nan")
    if not math.isfinite(t) or t <= 0:
        return float("nan")
    tau = 1.0 / WEEKS_PER_YEAR
    mu_annual = math.log1p(u) / t
    m = (PUT_SPREAD_LEVERAGE * mu_annual - PUT_SPREAD_LEVERAGE * sigma * sigma) * tau
    s = PUT_SPREAD_LEVERAGE * sigma * math.sqrt(tau)
    if not math.isfinite(m) or not math.isfinite(s) or s <= 0:
        return float("nan")

    def _spread_put(k: float) -> float:
        alpha = (math.log(k) - m) / s
        beta = alpha - s
        forward = math.exp(m + 0.5 * s * s)
        return k * _norm_cdf(alpha) - forward * _norm_cdf(beta)

    loss = _spread_put(PUT_SPREAD_SHORT_STRIKE) - _spread_put(PUT_SPREAD_LONG_STRIKE)
    if not math.isfinite(loss):
        return float("nan")
    max_loss = PUT_SPREAD_SHORT_STRIKE - PUT_SPREAD_LONG_STRIKE
    return max(0.0, min(max_loss, loss))


# ?????????????????????????????????????????????????????????????????
# NAV / ? history loaders
# ?????????????????????????????????????????????????????????????????
def _nav_price_from_metric_row(row: dict) -> float | None:
    for key in ("nav", "close_price"):
        v = row.get(key)
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if math.isfinite(f) and f > 0:
            return f
    return None


def load_metric_rows_by_ticker(metrics_path: Path) -> dict[str, list[dict]]:
    """Return ``{ticker: [metric_row, ...]}`` sorted by date ascending."""
    if not metrics_path.exists():
        return {}
    try:
        df = pd.read_csv(metrics_path)
    except Exception:
        return {}
    if "ticker" not in df.columns or "date" not in df.columns:
        return {}
    df = df.sort_values(["ticker", "date"], kind="stable")
    out: dict[str, list[dict]] = {}
    for _, row in df.iterrows():
        ticker = _norm_sym(row.get("ticker"))
        date_str = str(row.get("date") or "").strip()
        if not ticker or not date_str:
            continue
        out.setdefault(ticker, []).append(row.to_dict())
    return out


def load_nav_series_by_ticker(metrics_path: Path) -> dict[str, list[tuple[str, float]]]:
    """Return ``{ticker ? [(iso_date, price), -]}`` sorted ascending.

    Prefers issuer ``nav`` and falls back to ``close_price`` (for early
    lifecycle rows where the data pipeline only has Yahoo bootstrap close).
    Distribution events that arrive before any NAV record are dropped by
    :func:`normalize_events` (counted as ``nav_missing``).
    """
    out: dict[str, list[tuple[str, float]]] = {}
    for ticker, rows in load_metric_rows_by_ticker(metrics_path).items():
        series: list[tuple[str, float]] = []
        for row in rows:
            date_str = str(row.get("date") or "").strip()
            price = _nav_price_from_metric_row(row)
            if date_str and price is not None:
                series.append((date_str, price))
        if series:
            out[ticker] = series
    return out


def infer_split_events_from_metric_rows(
    metric_rows: list[dict],
    *,
    min_jump_ratio: float = 1.45,
) -> list[tuple[dt.date, float]]:
    """Infer reverse/forward splits from discrete close/NAV jumps in metrics."""
    points: list[tuple[dt.date, float]] = []
    for row in metric_rows or []:
        ds = str(row.get("date") or "")[:10]
        if len(ds) != 10:
            continue
        price = _nav_price_from_metric_row(row)
        if price is None:
            continue
        try:
            points.append((dt.date.fromisoformat(ds), price))
        except ValueError:
            continue
    points.sort(key=lambda x: x[0])
    out: list[tuple[dt.date, float]] = []
    for i in range(1, len(points)):
        prev_px = points[i - 1][1]
        cur_px = points[i][1]
        if prev_px <= 0 or cur_px <= 0:
            continue
        jump = cur_px / prev_px
        if jump < min_jump_ratio and jump > (1.0 / min_jump_ratio):
            continue
        matched = nearest_split_ratio(jump, rel_tol=0.18)
        if matched is None and jump >= min_jump_ratio:
            matched = nearest_split_ratio(jump, rel_tol=0.25)
        if matched is not None and matched > 0:
            out.append((points[i][0], float(matched)))
    return dedupe_split_events(out)


def merge_split_events_for_ticker(
    symbol: str,
    metric_rows: list[dict] | None,
    *,
    corp_actions_path: Path | None = None,
) -> list[tuple[dt.date, float]]:
    corp_path = corp_actions_path or DEFAULT_CORPORATE_ACTIONS_PATH
    corp_payload: dict = {}
    if corp_path.exists():
        try:
            corp_payload = json.loads(corp_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            corp_payload = {}
    declared = load_split_events_for_ticker(symbol, corp_path)
    declared.extend(parse_split_events_from_corp(corp_payload, symbol))
    inferred = infer_split_events_from_metric_rows(metric_rows or [])
    return dedupe_split_events(sorted(declared + inferred))


def resolve_recent_split_context(
    nav_series: list[tuple[str, float]],
    split_events: list[tuple[dt.date, float]],
    metric_rows: list[dict] | None = None,
) -> dict:
    close_pts: list[tuple[dt.date, float]] = []
    for ds, px in nav_series or []:
        if len(str(ds)) != 10:
            continue
        try:
            close_pts.append((dt.date.fromisoformat(str(ds)[:10]), float(px)))
        except (TypeError, ValueError):
            continue
    close_pts.sort(key=lambda x: x[0])
    ctx = resolve_split_context(close_pts, split_events, metric_rows=metric_rows)
    if ctx.get("mode") == "discrete_split" and ctx.get("boundary") and ctx.get("mult"):
        return ctx
    if split_events and close_pts:
        for _eff, mult in sorted(split_events):
            boundary = detect_split_boundary(close_pts, mult)
            if boundary is not None:
                return {
                    "mode": "discrete_split",
                    "boundary": boundary,
                    "mult": float(mult),
                    "filtered": [],
                }
    return {"mode": "continuous", "boundary": None, "mult": None, "filtered": []}


def _cap_run_rate_annual(value: float | None) -> float | None:
    if value is None or not math.isfinite(float(value)):
        return None
    capped = min(float(value), MAX_RUN_RATE_ANNUAL)
    return round(capped, 6) if capped >= 0 else None


def _compute_run_rate_annual(
    *,
    in_window: list[dict],
    all_norm: list[dict],
    nav_series: list[tuple[str, float]],
    split_ctx: dict,
    periods_per_year: int,
    today: dt.date,
) -> tuple[float | None, str]:
    """Run-rate for legacy display fields — post-split aware after reverse splits."""
    boundary = split_ctx.get("boundary")
    mult = split_ctx.get("mult")
    if (
        split_ctx.get("mode") == "discrete_split"
        and boundary is not None
        and mult is not None
        and (today - boundary).days <= POST_SPLIT_RUN_RATE_LOOKBACK_DAYS
    ):
        boundary_s = boundary.isoformat()
        post_split = [e for e in in_window if str(e.get("ex_date") or "") >= boundary_s]
        if len(post_split) >= POST_SPLIT_MIN_EVENTS_FOR_MEDIAN:
            median_yield = statistics.median([e["yield_frac"] for e in post_split])
            return _cap_run_rate_annual(float(median_yield) * periods_per_year), "post_split_events"

        latest_nav = nav_series[-1][1] if nav_series else None
        latest_event = all_norm[-1] if all_norm else None
        if latest_nav and latest_nav > 0 and latest_event:
            try:
                latest_amount = float(latest_event.get("amount"))
            except (TypeError, ValueError):
                latest_amount = float("nan")
            if math.isfinite(latest_amount) and latest_amount > 0:
                weekly = latest_amount / float(latest_nav)
                if weekly <= MAX_WEEKLY_YIELD_FRAC:
                    return _cap_run_rate_annual(weekly * periods_per_year), "post_split_latest_nav"

    if not in_window:
        return None, "none"
    yields = [e["yield_frac"] for e in in_window if e.get("yield_frac") is not None]
    if not yields:
        return None, "none"
    if any(y > MAX_WEEKLY_YIELD_FRAC for y in yields):
        latest_nav = nav_series[-1][1] if nav_series else None
        latest_event = all_norm[-1] if all_norm else None
        if latest_nav and latest_nav > 0 and latest_event:
            try:
                latest_amount = float(latest_event.get("amount"))
            except (TypeError, ValueError):
                latest_amount = float("nan")
            if math.isfinite(latest_amount) and latest_amount > 0:
                weekly = latest_amount / float(latest_nav)
                if weekly <= MAX_WEEKLY_YIELD_FRAC:
                    return _cap_run_rate_annual(weekly * periods_per_year), "latest_nav_suppressed_pre_split"
        return None, "suppressed_split_suspect"
    median_yield = statistics.median(yields)
    return _cap_run_rate_annual(float(median_yield) * periods_per_year), "trailing_median"


def lookup_value_at_or_before(
    series: list[tuple[str, float]], target_date: str
) -> float | None:
    """Linear scan (sorted series, short lists)."""
    if not series:
        return None
    best = None
    for d, v in series:
        if d <= target_date:
            best = v
        else:
            break
    return best


# ?????????????????????????????????????????????????????????????????
# Per-event normalization
# ?????????????????????????????????????????????????????????????????
def normalize_events(
    events: Iterable[dict],
    nav_series: list[tuple[str, float]],
    *,
    sigma_history: list[tuple[str, float]] | None = None,
    current_sigma: float | None = None,
) -> tuple[list[dict], int]:
    """Map raw ``{ex_date, amount}`` into NAV-normalized rows.

    Each row carries the structural ``ratio = yield / E[L](sigma_at_ex)``
    when ? is known. When ``sigma_history`` is missing (v1 default) we use
    ``current_sigma`` for every event - the bias mostly cancels because
    forward projection multiplies by ``E[L](sigma_scenario)`` with the
    same ? basis.
    """
    out: list[dict] = []
    nav_missing = 0
    for ev in events or []:
        if not isinstance(ev, dict):
            continue
        ex_date = str(ev.get("ex_date") or "").strip()
        try:
            amount = float(ev.get("amount"))
        except (TypeError, ValueError):
            continue
        if not ex_date or not math.isfinite(amount) or amount <= 0:
            continue
        nav_at_ex = lookup_value_at_or_before(nav_series, ex_date)
        if nav_at_ex is None or nav_at_ex <= 0:
            nav_missing += 1
            continue
        yield_frac = amount / nav_at_ex
        sigma_at_ex = (
            lookup_value_at_or_before(sigma_history, ex_date)
            if sigma_history
            else None
        )
        if sigma_at_ex is None or not math.isfinite(sigma_at_ex) or sigma_at_ex <= 0:
            sigma_at_ex = current_sigma
        bs_premium = None
        ratio = None
        if sigma_at_ex is not None and math.isfinite(sigma_at_ex) and sigma_at_ex > 0:
            bs = expected_put_spread_loss_weekly(0.0, float(sigma_at_ex), 1.0)
            if math.isfinite(bs) and bs > 0:
                bs_premium = bs
                ratio = yield_frac / bs
        out.append(
            {
                "ex_date": ex_date,
                "amount": round(amount, 6),
                "nav_at_ex": round(nav_at_ex, 6),
                "yield_frac": round(yield_frac, 6),
                "sigma_at_ex": (
                    round(float(sigma_at_ex), 6)
                    if sigma_at_ex is not None
                    and math.isfinite(float(sigma_at_ex))
                    else None
                ),
                "bs_premium": round(bs_premium, 6) if bs_premium is not None else None,
                "ratio": round(ratio, 6) if ratio is not None else None,
            }
        )
    out.sort(key=lambda e: e["ex_date"])
    return out, nav_missing


# ?????????????????????????????????????????????????????????????????
# Cadence + aggregation
# ?????????????????????????????????????????????????????????????????
def detect_cadence(events: list[dict]) -> tuple[str, int]:
    """Infer distribution cadence from median ex-date gap.

    Defaults to weekly when history is too thin (GraniteShares YB norm).
    Used for display labels and for the run-rate annualization; the
    simulation itself remains weekly-stepped.
    """
    if not events or len(events) < 2:
        return ("weekly", WEEKS_PER_YEAR)
    gaps: list[int] = []
    for prev, curr in zip(events[:-1], events[1:]):
        try:
            d_a = dt.date.fromisoformat(prev["ex_date"])
            d_b = dt.date.fromisoformat(curr["ex_date"])
        except (KeyError, ValueError):
            continue
        delta = (d_b - d_a).days
        if delta > 0:
            gaps.append(delta)
    if not gaps:
        return ("weekly", WEEKS_PER_YEAR)
    recent = gaps[-min(len(gaps), 12):]
    median_gap = statistics.median(recent)
    if median_gap <= 10:
        return ("weekly", 52)
    if median_gap <= 20:
        return ("biweekly", 26)
    if median_gap <= 45:
        return ("monthly", 12)
    return ("quarterly", 4)


def _confidence_label(n: int) -> str:
    if n >= RATIO_CONFIDENCE_FULL_N:
        return "high"
    if n >= RATIO_CONFIDENCE_MIN_N:
        return "medium"
    if n > 0:
        return "low"
    return "none"


def compute_capture_ratio(
    events: list[dict],
    *,
    cross_fund_ratio: float = DEFAULT_CROSS_FUND_RATIO,
) -> dict:
    """Fund-level structural capture ratio with confidence weighting.

    ``blended_ratio_used`` is what the scenario engine should consume:
    a confidence-weighted average of fund median and cross-fund prior.
    """
    ratios = [e["ratio"] for e in events if e.get("ratio") is not None]
    n = len(ratios)
    confidence = _confidence_label(n)
    if n == 0:
        return {
            "events_used": 0,
            "fund_ratio_median": None,
            "fund_ratio_p25": None,
            "fund_ratio_p75": None,
            "fund_ratio_confidence": confidence,
            "cross_fund_ratio": round(float(cross_fund_ratio), 6),
            "blended_ratio_used": round(float(cross_fund_ratio), 6),
        }
    sorted_ratios = sorted(ratios)
    median_ratio = statistics.median(sorted_ratios)
    p25 = float(np.percentile(sorted_ratios, 25))
    p75 = float(np.percentile(sorted_ratios, 75))
    w = min(1.0, n / float(RATIO_CONFIDENCE_FULL_N))
    blended = w * median_ratio + (1.0 - w) * cross_fund_ratio
    return {
        "events_used": n,
        "fund_ratio_median": round(float(median_ratio), 6),
        "fund_ratio_p25": round(p25, 6),
        "fund_ratio_p75": round(p75, 6),
        "fund_ratio_confidence": confidence,
        "cross_fund_ratio": round(float(cross_fund_ratio), 6),
        "blended_ratio_used": round(float(blended), 6),
    }


def build_template_yields(events: list[dict], max_len: int = 12) -> list[float]:
    """Most recent NAV-normalized yields, oldest ? newest.

    Consumed by the JS schedule simulator (transparency panel) so the
    user can see what payout pattern the model is being calibrated to.
    """
    tail = events[-max_len:] if events else []
    return [
        round(float(e["yield_frac"]), 6)
        for e in tail
        if e.get("yield_frac") is not None
    ]


def trim_events_for_export(events: list[dict], max_len: int = 16) -> list[dict]:
    """Last N normalized events for compact ``dashboard_data.json`` export."""
    return events[-max_len:]


# ?????????????????????????????????????????????????????????????????
# Top-level row-builder consumed by ``build_data.py``
# ?????????????????????????????????????????????????????????????????
def build_income_calibration_row(
    symbol: str,
    raw_events: list[dict],
    nav_series: list[tuple[str, float]],
    *,
    current_sigma: float | None,
    sigma_history: list[tuple[str, float]] | None = None,
    cross_fund_ratio: float = DEFAULT_CROSS_FUND_RATIO,
    trailing_window_days: int = TRAILING_WINDOW_DAYS,
    today: dt.date | None = None,
    metric_rows: list[dict] | None = None,
    split_events: list[tuple[dt.date, float]] | None = None,
) -> dict | None:
    """Build the per-ETF ``income_distribution_calibration`` block.

    Returns ``None`` when no usable distribution events exist (lets the
    caller leave the field off rather than ship empty objects).
    """
    today = today or dt.datetime.now(dt.UTC).date()
    cutoff = (today - dt.timedelta(days=trailing_window_days)).isoformat()
    if not raw_events:
        return None

    split_events = split_events if split_events is not None else merge_split_events_for_ticker(
        symbol,
        metric_rows,
    )
    split_ctx = resolve_recent_split_context(nav_series, split_events, metric_rows)

    all_norm, nav_missing_all = normalize_events(
        raw_events,
        nav_series,
        sigma_history=sigma_history,
        current_sigma=current_sigma,
    )
    if not all_norm:
        return None
    in_window = [e for e in all_norm if e["ex_date"] >= cutoff]
    if not in_window:
        # Fund has events but none in trailing window - still expose what we have
        # so the UI can show "stale schedule" rather than silently disappearing.
        in_window = all_norm

    capture = compute_capture_ratio(in_window, cross_fund_ratio=cross_fund_ratio)
    cadence_label, periods_per_year = detect_cadence(all_norm)
    template = build_template_yields(all_norm, max_len=12)

    run_rate, run_rate_basis = _compute_run_rate_annual(
        in_window=in_window,
        all_norm=all_norm,
        nav_series=nav_series,
        split_ctx=split_ctx,
        periods_per_year=periods_per_year,
        today=today,
    )

    latest = all_norm[-1] if all_norm else None

    block: dict = {
        "events_used": capture["events_used"],
        "events_total": len(all_norm),
        "fund_ratio_median": capture["fund_ratio_median"],
        "fund_ratio_p25": capture["fund_ratio_p25"],
        "fund_ratio_p75": capture["fund_ratio_p75"],
        "fund_ratio_confidence": capture["fund_ratio_confidence"],
        "cross_fund_ratio": capture["cross_fund_ratio"],
        "blended_ratio_used": capture["blended_ratio_used"],
        "cadence_label": cadence_label,
        "periods_per_year": periods_per_year,
        "nav_missing_count": nav_missing_all,
        "run_rate_annual_display": run_rate,
        "run_rate_basis": run_rate_basis,
        "split_mode": split_ctx.get("mode"),
        "split_boundary": (
            split_ctx.get("boundary").isoformat()
            if split_ctx.get("boundary") is not None
            else None
        ),
        "split_mult": (
            round(float(split_ctx["mult"]), 6)
            if split_ctx.get("mult") is not None and math.isfinite(float(split_ctx["mult"]))
            else None
        ),
        "template_yields": template,
        "events_recent": trim_events_for_export(all_norm, max_len=16),
        "latest_event": latest,
        "current_sigma": (
            round(float(current_sigma), 6)
            if current_sigma is not None and math.isfinite(float(current_sigma))
            else None
        ),
        "trailing_window_days": trailing_window_days,
        "schema_version": 1,
    }
    return block


def derive_cross_fund_ratio(
    fund_blocks: dict[str, dict],
    *,
    min_events: int = RATIO_CONFIDENCE_FULL_N,
    fallback: float = DEFAULT_CROSS_FUND_RATIO,
) -> float:
    """Median ``fund_ratio_median`` across high-confidence funds.

    Lets ``build_data.py`` self-calibrate the cross-fund prior as the
    YieldBOOST universe expands; the research's hardcoded 0.65 stays as
    fallback when fewer than 3 funds qualify.
    """
    qualifying: list[float] = []
    for block in (fund_blocks or {}).values():
        if not block:
            continue
        ratio = block.get("fund_ratio_median")
        n = block.get("events_used", 0) or 0
        if ratio is None:
            continue
        if not math.isfinite(float(ratio)):
            continue
        if int(n) < int(min_events):
            continue
        qualifying.append(float(ratio))
    if len(qualifying) < 3:
        return round(float(fallback), 6)
    return round(float(statistics.median(qualifying)), 6)


# ?????????????????????????????????????????????????????????????????
# Forward pair-trade P&L + inverse-variance blend (decisions A3 + B2 + C2).
# Mirror of `assets/income_scenario.js#expectedPairPnlAnnual` /
# `inverseVarianceBlend` so build_data.py can re-run the math server-side
# for YieldBOOST rows (where the anchor target needs to be overridden with
# calibration data the ls-algo screener does not have access to).
# ?????????????????????????????????????????????????????????????????
_BAND_QUANTILE_Z = 1.2815515655446004


def band_to_sigma(p10: float | None, p90: float | None) -> float | None:
    """Normal-equivalent sigma from a symmetric p10/p90 band."""
    if p10 is None or p90 is None:
        return None
    try:
        lo = float(p10)
        hi = float(p90)
    except (TypeError, ValueError):
        return None
    if not (math.isfinite(lo) and math.isfinite(hi)):
        return None
    width = abs(hi - lo)
    if width <= 0:
        return None
    sigma = width / (2.0 * _BAND_QUANTILE_Z)
    if not math.isfinite(sigma) or sigma <= 0:
        return None
    return float(sigma)


def inverse_variance_blend(
    *,
    mu_forward: float,
    sigma_forward: float | None,
    mu_realized: float,
    sigma_realized: float | None,
) -> dict | None:
    """Normal-Normal conjugate update on the level estimate.

    Returns ``{"posterior_mean", "weight_forward", "posterior_sigma",
    "method"}`` or ``None`` when both sigmas are missing / zero.

    * ``method="inverse_variance"``: both sigmas positive, true blend.
    * ``method="anchor_shift_fallback"``: only forward known, weight_F=1.
    * ``method="realized_only"``: only realized known, weight_F=0.
    """
    try:
        mu_F = float(mu_forward)
        mu_R = float(mu_realized)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(mu_F) or not math.isfinite(mu_R):
        return None
    sig_F = None
    sig_R = None
    if sigma_forward is not None:
        try:
            sig_F = float(sigma_forward)
        except (TypeError, ValueError):
            sig_F = None
    if sigma_realized is not None:
        try:
            sig_R = float(sigma_realized)
        except (TypeError, ValueError):
            sig_R = None
    sigFok = sig_F is not None and math.isfinite(sig_F) and sig_F > 0
    sigRok = sig_R is not None and math.isfinite(sig_R) and sig_R > 0
    if not sigFok and not sigRok:
        # Neither sigma known: cannot determine a posterior; fall back to the
        # forward forecast as a confident point estimate (E2 default).
        if math.isfinite(mu_F):
            return {
                "posterior_mean": mu_F,
                "weight_forward": 1.0,
                "posterior_sigma": None,
                "method": "anchor_shift_fallback",
            }
        return None
    if not sigFok:
        # Forward forecast has no usable band (point estimate). Treat the
        # forecast as a confident point estimate (sigma_F -> 0) so the
        # posterior collapses to the forward mean -- this is the legacy
        # anchor-shift behaviour and the E2 fallback for
        # ``yieldboost_put_spread_point`` rows.
        return {
            "posterior_mean": mu_F,
            "weight_forward": 1.0,
            "posterior_sigma": None,
            "method": "anchor_shift_fallback",
        }
    if not sigRok:
        # Realized dispersion unknown but forward band is present: treat
        # realized as a low-confidence point estimate (sigma_R -> infinity)
        # so the forward forecast dominates -- equivalent to anchor-shift.
        return {
            "posterior_mean": mu_F,
            "weight_forward": 1.0,
            "posterior_sigma": sig_F,
            "method": "anchor_shift_fallback",
        }
    vF = sig_F * sig_F
    vR = sig_R * sig_R
    denom = vF + vR
    if denom <= 0 or not math.isfinite(denom):
        return None
    w_F = vR / denom
    mu = w_F * mu_F + (1.0 - w_F) * mu_R
    posterior_sigma = math.sqrt((vF * vR) / denom)
    return {
        "posterior_mean": float(mu),
        "weight_forward": float(w_F),
        "posterior_sigma": float(posterior_sigma),
        "method": "inverse_variance",
    }


# =============================================================================
# Weekly-rebalanced compound pair-P&L Monte Carlo (schema_v=4 headline model)
# =============================================================================
#
# The ``simulate_weekly_compound_pair_pnl`` MC below replaces the Magis
# closed-form buy-and-hold pair P&L as the dashboard's headline forward
# edge for YieldBOOST rows. It produces a full path-dependent
# distribution on the **log_continuous_annual** axis so it can be
# inverse-variance blended with the screener's ``gross_decay_annual``
# (Stahl ``beta*log(R_und_TR) - log(R_etf_TR)``) without a units
# mismatch. See module docstring for the per-week recursion.

# Coarse upper bound on absolute weekly pair return; the Box-Muller draws
# can produce extreme tail z-scores that send 1 + pair_w_t below zero on
# very high vol underlyings (sigma ~120%+). Clip so log1p stays finite.
# This is a numerical guard, not a model assumption -- we cap the loss at
# -99% per week which is well outside any plausible regime.
_PAIR_WEEK_FLOOR = -0.99
# Same on the upside -- lognormal tails can theoretically produce large
# positive pair_w; cap to keep the MC from being dominated by a handful
# of outliers when n_paths is small.
_PAIR_WEEK_CEIL = 5.0


def stable_seed_from_symbol(symbol: str, *, salt: int = 0) -> int:
    """Deterministic 32-bit seed for reproducible per-symbol MC draws.

    ``hash()`` is not stable across Python sessions because PYTHONHASHSEED
    randomizes; CRC32 is stable and cheap. ``salt`` lets us derive
    independent seeds per call site (headline / sigma_lo / sigma_hi /
    scenario grid) from the same symbol.
    """
    sym = (symbol or "").strip().upper()
    base = zlib.crc32(sym.encode("utf-8")) & 0xFFFFFFFF
    return int((base ^ (int(salt) & 0xFFFFFFFF)) & 0x7FFFFFFF)


def _put_spread_payoff_vec(sleeve_ret: np.ndarray) -> np.ndarray:
    """Vectorized 95/88 put-spread realized payoff on the 2x sleeve.

    ``sleeve_ret`` is the simple weekly return of the leveraged sleeve
    (``(1+r_und)**2 - 1`` for 2x). Output is the spread payoff per
    dollar of sleeve notional -- the short-of-puts realized loss that
    the YB fund eats on a Friday roll. Capped at the structural
    max-loss ``0.95 - 0.88 = 0.07`` and floored at zero.
    """
    end = 1.0 + sleeve_ret
    short_put = np.maximum(0.0, PUT_SPREAD_SHORT_STRIKE - end)
    long_put = np.maximum(0.0, PUT_SPREAD_LONG_STRIKE - end)
    spread = short_put - long_put
    return np.clip(spread, 0.0, PUT_SPREAD_SHORT_STRIKE - PUT_SPREAD_LONG_STRIKE)


def simulate_weekly_compound_pair_pnl(
    sigma_annual: float | None,
    mu_annual: float = 0.0,
    beta: float = 0.0,
    capture_ratio: float = DEFAULT_CROSS_FUND_RATIO,
    *,
    expense_ratio_annual: float = DEFAULT_EXPENSE_RATIO_ANNUAL,
    borrow_annual: float = 0.0,
    weeks: int = WEEKS_PER_YEAR,
    n_paths: int = 20_000,
    seed: int = 0,
    return_samples: bool = False,
) -> dict | None:
    """Path-dependent MC of a weekly-rebalanced delta-hedged YB short.

    For each path of length ``weeks`` we draw i.i.d. lognormal weekly
    underlying returns, compute the realised 95/88 put-spread on the
    2x sleeve, mark the pair (short ETF + beta*long underlying) to
    current equity, and compound::

        z_t        ~ N(0, 1)
        sigma_w    = sigma_annual / sqrt(52)
        mu_w       = mu_annual / 52 - 0.5 * sigma_w^2
        r_und_t    = exp(mu_w + sigma_w * z_t) - 1
        sleeve_t   = (1 + r_und_t)^2 - 1
        L_t        = put_spread_payoff(sleeve_t)            # 95/88, capped 0..0.07
        d_weekly   = capture_ratio * E[put_spread_loss](0, sigma, 1 week)
        pair_w_t   = L_t + ER/52 - borrow/52 - d_weekly + beta * r_und_t
        equity_t+1 = equity_t * (1 + pair_w_t)
        pair_log_annual = ln(equity_W) * (52 / weeks)

    ``capture_ratio`` scales the calibrated weekly distribution cash
    debit (``d_weekly``). Higher capture / vol ⇒ larger short-side
    distribution drag ⇒ lower forward pair P&L.

    Returns a dict with log-continuous-annual quantiles, mean, std, and
    diagnostic moments of the underlying. ``None`` on invalid inputs.

    Reproducibility: ``seed`` is consumed by ``np.random.default_rng``,
    which is deterministic. Combine with ``stable_seed_from_symbol``
    upstream to keep rebuilds bit-identical at the ticker level.
    """
    if sigma_annual is None:
        return None
    try:
        sigma = float(sigma_annual)
        mu = float(mu_annual)
        b = float(beta)
        er = float(expense_ratio_annual)
        borrow = float(borrow_annual)
        cap_r = float(capture_ratio) if capture_ratio is not None else 0.0
    except (TypeError, ValueError):
        return None
    if not math.isfinite(sigma) or sigma <= 0:
        return None
    if not (math.isfinite(mu) and math.isfinite(b) and math.isfinite(er) and math.isfinite(borrow)):
        return None
    weeks_i = int(max(1, weeks))
    n_paths_i = int(max(1, n_paths))

    sigma_w = sigma / math.sqrt(WEEKS_PER_YEAR)
    mu_w = mu / WEEKS_PER_YEAR - 0.5 * sigma_w * sigma_w
    weekly_er = max(0.0, er) / WEEKS_PER_YEAR
    weekly_borrow = max(0.0, borrow) / WEEKS_PER_YEAR
    bs_weekly = expected_put_spread_loss_weekly(0.0, sigma, 1.0)
    if not math.isfinite(bs_weekly):
        bs_weekly = 0.0
    weekly_dist = max(0.0, cap_r * bs_weekly) if cap_r > 0 else 0.0

    rng = np.random.default_rng(int(seed) & 0x7FFFFFFF)
    z = rng.standard_normal(size=(n_paths_i, weeks_i))
    log_und = mu_w + sigma_w * z
    r_und = np.expm1(log_und)
    # 2x sleeve compound (NOT linearized): (1+r)^2 - 1 = exp(2*ln(1+r)) - 1.
    # Computing via 2*log_und keeps the math exact and lets us capture
    # the nonlinearity that drives the put-spread payoff distribution.
    sleeve_ret = np.expm1(2.0 * log_und)
    L = _put_spread_payoff_vec(sleeve_ret)
    pair_w = L + weekly_er - weekly_borrow - weekly_dist + b * r_und
    pair_w = np.clip(pair_w, _PAIR_WEEK_FLOOR, _PAIR_WEEK_CEIL)
    log_pair_week = np.log1p(pair_w)
    log_pair_total = log_pair_week.sum(axis=1)
    annualization = WEEKS_PER_YEAR / float(weeks_i)
    log_pair_annual = log_pair_total * annualization

    # Underlying sanity diagnostic: 1Y simple return median of sampled
    # paths (helps the scenarios heatmap show "what underlying drift this
    # cell corresponds to" alongside the pair P&L).
    log_und_total = log_und.sum(axis=1) * annualization
    und_simple = np.expm1(log_und_total)

    quantiles = np.quantile(log_pair_annual, [0.10, 0.25, 0.50, 0.75, 0.90])
    out: dict = {
        "p10_log": float(quantiles[0]),
        "p25_log": float(quantiles[1]),
        "p50_log": float(quantiles[2]),
        "p75_log": float(quantiles[3]),
        "p90_log": float(quantiles[4]),
        "mean_log": float(log_pair_annual.mean()),
        "std_log": float(log_pair_annual.std(ddof=1)) if n_paths_i > 1 else 0.0,
        "und_p50_simple": float(np.quantile(und_simple, 0.50)),
        "und_mean_simple": float(und_simple.mean()),
        "n_paths": int(n_paths_i),
        "weeks": int(weeks_i),
        "sigma_used": float(sigma),
        "mu_used": float(mu),
        "beta_used": float(b),
        "capture_used": float(cap_r),
        "distributions_weekly": float(weekly_dist),
        "distributions_annual": float(weekly_dist * WEEKS_PER_YEAR),
        "expense_ratio_annual": float(er),
        "borrow_annual": float(borrow),
        "axis": "log_continuous_annual",
        "basis": "weekly_rebalanced_compound",
    }
    if return_samples:
        out["sample_log_array"] = log_pair_annual.astype(float).tolist()
    return out


def estimate_income_style_scenario_return(
    underlying_return: float,
    sigma_annual: float,
    annual_income_yield: float,
    *,
    annual_borrow_cost: float = 0.0,
    horizon_years: float = 1.0,
    expense_ratio_annual: float = DEFAULT_EXPENSE_RATIO_ANNUAL,
) -> dict | None:
    """Mirror of ``estimateIncomeStyleScenarioReturn`` in ``index.html``.

    Returns short-favorable simple-return economics over ``horizon_years``.
    """
    try:
        u = float(underlying_return)
        sigma = float(sigma_annual)
        d_annual = float(annual_income_yield)
        borrow = float(annual_borrow_cost)
        t = float(horizon_years)
        er = float(expense_ratio_annual)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(d_annual) or d_annual < 0 or not math.isfinite(t) or t <= 0:
        return None
    weekly_loss = expected_put_spread_loss_weekly(u, sigma, t)
    if not math.isfinite(weekly_loss):
        return None
    weeks = max(1, int(round(t * WEEKS_PER_YEAR)))
    weekly_expense = max(0.0, er) / WEEKS_PER_YEAR
    weekly_distribution = d_annual / WEEKS_PER_YEAR
    q = max(0.0001, min(1.5, 1.0 - weekly_loss - weekly_expense))
    nav_end_ratio = q**weeks
    nav_decay = 1.0 - nav_end_ratio
    geom_sum = float(weeks) if abs(1.0 - q) < 1e-9 else (1.0 - nav_end_ratio) / (1.0 - q)
    distributions_paid = weekly_distribution * geom_sum
    borrow_cost = borrow * t if borrow > 0 else 0.0
    net_short_pnl = nav_decay - distributions_paid - borrow_cost
    return {
        "weekly_spread_loss": float(weekly_loss),
        "nav_decay": float(nav_decay),
        "nav_return": float(-nav_decay),
        "distributions_paid": float(distributions_paid),
        "borrow_cost": float(borrow_cost),
        "net_short_pnl": float(net_short_pnl),
        "long_total_return": float(-nav_decay + distributions_paid),
        "weeks": int(weeks),
    }


def intrinsic_gross_decay_log_annual(
    sigma_annual: float,
    *,
    mu_annual: float = 0.0,
    horizon_years: float = 1.0,
    expense_ratio_annual: float = DEFAULT_EXPENSE_RATIO_ANNUAL,
) -> float | None:
    """Put-spread structural gross decay on the log/yr axis (short-favorable +)."""
    try:
        sigma = float(sigma_annual)
        mu = float(mu_annual)
        t = float(horizon_years)
        er = float(expense_ratio_annual)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(sigma) or sigma <= 0 or not math.isfinite(t) or t <= 0:
        return None
    und_simple = math.expm1(mu * t) if math.isfinite(mu) else 0.0
    weekly_loss = expected_put_spread_loss_weekly(und_simple, sigma, t)
    if not math.isfinite(weekly_loss):
        return None
    weeks = max(1, int(round(t * WEEKS_PER_YEAR)))
    weekly_expense = max(0.0, er) / WEEKS_PER_YEAR
    q = max(0.0001, min(1.5, 1.0 - weekly_loss - weekly_expense))
    nav_end = q**weeks
    if nav_end <= 0.0 or not math.isfinite(nav_end):
        return None
    return float(-math.log(nav_end) / t)


def structural_pair_gross_log_annual(
    sigma_annual: float,
    mu_annual: float,
    beta: float,
    *,
    horizon_years: float = 1.0,
    expense_ratio_annual: float = DEFAULT_EXPENSE_RATIO_ANNUAL,
) -> float | None:
    """Gross structural pair: put-spread ETF log decay + beta hedge leg."""
    try:
        b = float(beta)
        mu = float(mu_annual)
    except (TypeError, ValueError):
        return None
    gross = intrinsic_gross_decay_log_annual(
        sigma_annual,
        mu_annual=mu,
        horizon_years=horizon_years,
        expense_ratio_annual=expense_ratio_annual,
    )
    if gross is None or not math.isfinite(b):
        return None
    return float(gross + b * mu)


def scenario_grid_put_spread_pair(
    sigma_annual: float | None,
    beta: float | None,
    capture_ratio: float = DEFAULT_CROSS_FUND_RATIO,
    *,
    sigma_multipliers: Sequence[float] = (0.5, 0.7, 1.0, 1.3, 1.5),
    drifts: Sequence[float] = (-0.50, -0.25, 0.00, 0.25, 0.50),
    expense_ratio_annual: float = DEFAULT_EXPENSE_RATIO_ANNUAL,
    borrow_annual: float = 0.0,
    horizon_years: float = 1.0,
    gross_anchor_p50: float | None = None,
) -> dict | None:
    """5x5 grid of put-spread structural gross pair P&L (log/yr).

    Cells use the same closed-form put-spread engine as Exp. decay /
    net-edge anchoring. The center cell (σ×1.0, μ=0) is calibrated to
    ``gross_anchor_p50`` when provided (typically
    ``expected_gross_decay_p50_annual`` from the screener).
    """
    if sigma_annual is None or beta is None:
        return None
    try:
        sigma_base = float(sigma_annual)
        beta_v = float(beta)
        cap = float(capture_ratio)
        er = float(expense_ratio_annual)
        borrow = float(borrow_annual)
        t = float(horizon_years)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(sigma_base) or sigma_base <= 0 or not math.isfinite(beta_v):
        return None
    sigma_mult_list = [float(k) for k in sigma_multipliers]
    drift_list = [float(m) for m in drifts]
    raw_center = structural_pair_gross_log_annual(
        sigma_base,
        0.0,
        beta_v,
        horizon_years=t,
        expense_ratio_annual=er,
    )
    anchor_delta = 0.0
    if (
        gross_anchor_p50 is not None
        and raw_center is not None
        and math.isfinite(float(gross_anchor_p50))
    ):
        anchor_delta = float(gross_anchor_p50) - float(raw_center)
    bs_weekly_base = expected_put_spread_loss_weekly(0.0, sigma_base, 1.0)
    d_annual_base = (
        max(0.0, cap * bs_weekly_base * WEEKS_PER_YEAR)
        if math.isfinite(bs_weekly_base)
        else 0.0
    )
    grid: list[list[float | None]] = []
    und_grid: list[list[float | None]] = []
    for k in sigma_mult_list:
        row: list[float | None] = []
        und_row: list[float | None] = []
        sigma_k = sigma_base * k
        for mu in drift_list:
            cell = structural_pair_gross_log_annual(
                sigma_k,
                mu,
                beta_v,
                horizon_years=t,
                expense_ratio_annual=er,
            )
            if cell is None:
                row.append(None)
                und_row.append(None)
                continue
            row.append(float(cell + anchor_delta))
            und_row.append(float(math.expm1(mu * t)))
        grid.append(row)
        und_grid.append(und_row)
    return {
        "sigma_multipliers": sigma_mult_list,
        "drifts": drift_list,
        "p50_log_grid": grid,
        "und_p50_simple_grid": und_grid,
        "borrow_annual": float(borrow),
        "expense_ratio_annual": float(er),
        "n_paths_per_cell": 0,
        "axis": "log_continuous_annual",
        "basis": "put_spread_gross_anchor",
        "engine": "yieldboost_put_spread_structural",
        "anchor_p50_annual": (
            float(gross_anchor_p50) if gross_anchor_p50 is not None else None
        ),
        "anchor_delta_annual": float(anchor_delta),
        "distributions_annual_at_anchor": float(d_annual_base),
        "capture_ratio_used": float(cap),
        "horizon_years": float(t),
    }


def scenario_grid_pair_pnl(
    sigma_annual: float | None,
    beta: float | None,
    capture_ratio: float = DEFAULT_CROSS_FUND_RATIO,
    *,
    sigma_multipliers: Sequence[float] = (0.5, 0.7, 1.0, 1.3, 1.5),
    drifts: Sequence[float] = (-0.50, -0.25, 0.00, 0.25, 0.50),
    expense_ratio_annual: float = DEFAULT_EXPENSE_RATIO_ANNUAL,
    borrow_annual: float = 0.0,
    n_paths: int = 5_000,
    seed: int = 0,
) -> dict | None:
    """5x5 (sigma_multiplier x drift) grid of MC p50 pair P&L (log/yr).

    ``grid[i][j]`` corresponds to ``sigma_multipliers[i]`` and
    ``drifts[j]``. All cells use the weekly-rebalanced compound MC with
    ``n_paths`` paths each (default 5_000 -> 25 cells = 125k draws per
    row; vectorised numpy keeps this in the millisecond range).

    Returns ``None`` on invalid inputs so the caller can decide whether
    to omit the field on the row.
    """
    if sigma_annual is None or beta is None:
        return None
    try:
        sigma_base = float(sigma_annual)
        beta_v = float(beta)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(sigma_base) or sigma_base <= 0:
        return None
    if not math.isfinite(beta_v):
        return None
    sigma_mult_list = [float(k) for k in sigma_multipliers]
    drift_list = [float(m) for m in drifts]
    grid: list[list[float | None]] = []
    und_grid: list[list[float | None]] = []
    for i, k in enumerate(sigma_mult_list):
        row: list[float | None] = []
        und_row: list[float | None] = []
        for j, mu in enumerate(drift_list):
            mc = simulate_weekly_compound_pair_pnl(
                sigma_annual=sigma_base * k,
                mu_annual=mu,
                beta=beta_v,
                capture_ratio=capture_ratio,
                expense_ratio_annual=expense_ratio_annual,
                borrow_annual=borrow_annual,
                n_paths=n_paths,
                # Per-cell salt so cells do not share Box-Muller paths;
                # each (sigma_mult, drift) gets its own deterministic
                # stream from the same parent seed.
                seed=int(seed) ^ (i * 131 + j * 17),
            )
            if mc is None:
                row.append(None)
                und_row.append(None)
            else:
                row.append(float(mc["p50_log"]))
                und_row.append(float(mc.get("und_p50_simple", float("nan"))))
        grid.append(row)
        und_grid.append(und_row)
    return {
        "sigma_multipliers": sigma_mult_list,
        "drifts": drift_list,
        "p50_log_grid": grid,
        "und_p50_simple_grid": und_grid,
        "borrow_annual": float(borrow_annual),
        "expense_ratio_annual": float(expense_ratio_annual),
        "n_paths_per_cell": int(n_paths),
        "axis": "log_continuous_annual",
        "basis": "weekly_rebalanced_compound",
        "engine": "yieldboost_mc",
    }


def expected_pair_pnl_annual(
    *,
    calibration: dict | None,
    sigma_annual: float | None,
    beta: float | None = None,
    mu_annual: float = 0.0,
    horizon_years: float = 1.0,
    expense_ratio_annual: float = DEFAULT_EXPENSE_RATIO_ANNUAL,
    borrow_annual: float = 0.0,
    cross_fund_ratio: float = DEFAULT_CROSS_FUND_RATIO,
    tail_adjustment_annual: float = DEFAULT_TAIL_ADJUSTMENT_ANNUAL,
    n_paths: int = 20_000,
    seed: int = 0,
    gross_band: dict | None = None,  # noqa: ARG001 - kept for API compat, ignored
) -> dict | None:
    """Forward weekly-rebalanced compound pair P&L for a YB row.

    **Headline (schema_v=4):** log-continuous-annual quantiles from
    :func:`simulate_weekly_compound_pair_pnl`. Same axis as the
    screener's ``gross_decay_annual``.

    **Diagnostics (kept on the dict, no longer headline):** the Magis
    closed-form simple-return ``nav_decay`` and ``distributions`` are
    still computed and returned as ``nav_decay_simple_annual`` and
    ``distributions_simple_annual`` so the Income Scenarios cash
    projection and the calibration audit panel keep working.

    Returns ``None`` when calibration data, sigma, or beta is missing.
    """
    if calibration is None:
        return None
    if sigma_annual is None or beta is None:
        return None
    try:
        sigma = float(sigma_annual)
        beta_v = float(beta)
        t = float(horizon_years)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(sigma) or sigma <= 0:
        return None
    if not math.isfinite(t) or t <= 0:
        return None
    if not math.isfinite(beta_v):
        return None

    ratio = calibration.get("blended_ratio_used")
    try:
        ratio_val = float(ratio) if ratio is not None else None
    except (TypeError, ValueError):
        ratio_val = None
    if ratio_val is None or not math.isfinite(ratio_val) or ratio_val <= 0:
        ratio_val = float(cross_fund_ratio)

    weeks = max(1, int(round(t * WEEKS_PER_YEAR)))

    # ---- Magis closed-form simple-return diagnostics --------------------
    bs_weekly = expected_put_spread_loss_weekly(0.0, sigma, 1.0)
    if not math.isfinite(bs_weekly):
        bs_weekly = 0.0
    d_weekly = max(0.0, ratio_val * bs_weekly)
    L_horizon = expected_put_spread_loss_weekly(0.0, sigma, t)
    if not math.isfinite(L_horizon):
        L_horizon = 0.0
    weekly_expense = max(0.0, float(expense_ratio_annual)) / WEEKS_PER_YEAR
    q = max(0.0001, min(1.5, 1.0 - L_horizon - weekly_expense))
    nav_end_ratio = q ** weeks
    nav_decay_simple = 1.0 - nav_end_ratio
    if tail_adjustment_annual is not None and math.isfinite(float(tail_adjustment_annual)):
        nav_decay_simple += float(tail_adjustment_annual) * t
    if abs(1.0 - q) < 1e-9:
        geom_sum = float(weeks)
    else:
        geom_sum = (1.0 - nav_end_ratio) / (1.0 - q)
    distributions_simple = d_weekly * geom_sum

    # ---- Headline weekly-rebalanced compound MC -------------------------
    mc = simulate_weekly_compound_pair_pnl(
        sigma_annual=sigma,
        mu_annual=mu_annual,
        beta=beta_v,
        capture_ratio=ratio_val,
        expense_ratio_annual=expense_ratio_annual,
        borrow_annual=borrow_annual,
        weeks=weeks,
        n_paths=n_paths,
        seed=seed,
    )
    if mc is None:
        return None

    return {
        # Headline log-axis quantiles (schema_v=4)
        "p10_log": float(mc["p10_log"]),
        "p25_log": float(mc["p25_log"]),
        "p50_log": float(mc["p50_log"]),
        "p75_log": float(mc["p75_log"]),
        "p90_log": float(mc["p90_log"]),
        "mean_log": float(mc["mean_log"]),
        "std_log": float(mc["std_log"]),
        "n_paths": int(mc["n_paths"]),
        # Diagnostic underlying simple-return moments
        "und_p50_simple_annual": float(mc["und_p50_simple"]),
        "und_mean_simple_annual": float(mc["und_mean_simple"]),
        # Magis closed-form diagnostics (no longer headline)
        "nav_decay_simple_annual": float(nav_decay_simple),
        "distributions_simple_annual": float(distributions_simple),
        "ratio_used": float(ratio_val),
        "confidence": calibration.get("fund_ratio_confidence")
            if isinstance(calibration, dict) else None,
        "bs_premium_weekly": float(bs_weekly),
        "weeks": int(weeks),
        "axis": "log_continuous_annual",
        "basis": "weekly_rebalanced_compound",
    }


def build_legacy_yield_fields(
    block: dict | None,
    current_price: float | None = None,
) -> dict[str, float | int | str | None]:
    """Backward-compatible ``income_yield_*`` scalars with **corrected** semantics.

    Kept for one release so older clients (e.g. ``price_azyy_put.py`` pre-
    update) keep working. New meaning:

    * ``income_yield_trailing_annual`` = NAV-normalized median weekly yield
      - periods_per_year (? stable run-rate at current ? regime).
    * ``income_yield_recent_annual``   = latest NAV-normalized yield -
      periods_per_year (? this-week annualized).
    * Old ``?$/today_price`` and ``-12`` formulations are gone.
    """
    out: dict[str, float | int | str | None] = {}
    if not block:
        return out
    run_rate = block.get("run_rate_annual_display")
    if run_rate is not None:
        capped = _cap_run_rate_annual(float(run_rate))
        if capped is not None:
            out["income_yield_trailing_annual"] = capped
    latest = block.get("latest_event") or {}
    latest_yield = latest.get("yield_frac")
    ppy = block.get("periods_per_year") or WEEKS_PER_YEAR
    if latest_yield is not None:
        recent_annual = float(latest_yield) * float(ppy)
        capped_recent = _cap_run_rate_annual(recent_annual)
        if capped_recent is not None:
            out["income_yield_recent_annual"] = capped_recent
    n_events = block.get("events_used")
    if n_events is not None:
        out["income_distribution_count_1y"] = int(n_events)
    if latest.get("amount") is not None:
        out["income_latest_distribution"] = round(float(latest["amount"]), 6)
    if latest.get("ex_date"):
        out["income_latest_ex_date"] = latest["ex_date"]
    return out


# ?????????????????????????????????????????????????????????????????
# High-level entry point used by ``build_data.py``
# ?????????????????????????????????????????????????????????????????
def build_all_calibrations(
    distributions_payload: dict,
    metrics_path: Path,
    *,
    sigma_by_symbol: dict[str, float] | None = None,
    cross_fund_ratio: float = DEFAULT_CROSS_FUND_RATIO,
    self_calibrate: bool = True,
    yieldboost_symbols: set[str] | None = None,
) -> tuple[dict[str, dict], float]:
    """Build all per-ETF calibration blocks + return the (possibly self-
    calibrated) cross-fund ratio that was actually used.

    Two-pass when ``self_calibrate=True`` so the prior reflects the current
    YB fleet:
        1. First pass: build blocks with the seed ``cross_fund_ratio``.
        2. Derive the fleet median over high-confidence funds.
        3. Second pass: rebuild with the derived prior (idempotent for
           high-confidence funds; only changes low/medium-confidence ones).
    """
    by_symbol = (
        distributions_payload.get("by_symbol")
        if isinstance(distributions_payload, dict)
        else None
    )
    if not isinstance(by_symbol, dict):
        return {}, round(float(cross_fund_ratio), 6)

    nav_by_ticker = load_nav_series_by_ticker(metrics_path)
    metrics_by_ticker = load_metric_rows_by_ticker(metrics_path)
    sigma_by_symbol = sigma_by_symbol or {}
    target_set: set[str] | None = (
        {_norm_sym(s) for s in yieldboost_symbols if s} if yieldboost_symbols else None
    )

    def _pass(prior: float) -> dict[str, dict]:
        out: dict[str, dict] = {}
        for raw_sym, events in by_symbol.items():
            sym = _norm_sym(raw_sym)
            if not sym:
                continue
            if target_set is not None and sym not in target_set:
                continue
            if not isinstance(events, list) or not events:
                continue
            block = build_income_calibration_row(
                sym,
                events,
                nav_by_ticker.get(sym, []),
                current_sigma=sigma_by_symbol.get(sym),
                cross_fund_ratio=prior,
                metric_rows=metrics_by_ticker.get(sym, []),
            )
            if block is not None:
                out[sym] = block
        return out

    blocks = _pass(cross_fund_ratio)
    final_prior = float(cross_fund_ratio)
    if self_calibrate and blocks:
        derived = derive_cross_fund_ratio(
            blocks, min_events=RATIO_CONFIDENCE_FULL_N, fallback=cross_fund_ratio
        )
        if abs(derived - final_prior) > 1e-6:
            final_prior = derived
            blocks = _pass(final_prior)
    return blocks, round(final_prior, 6)

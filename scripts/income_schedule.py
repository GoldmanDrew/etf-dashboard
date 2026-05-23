"""scripts/income_schedule.py
=================================

NAV-normalized distribution calibration for YieldBOOST income ETFs.

The Income Scenarios engine in ``index.html`` uses the closed-form
mean-field identity (validated within +1.9pp of Monte Carlo by
Magis Capital Partners - *Bucket 2 Income ETF Structural Decay*,
April 2026)::

    q     = 1 - L - f/52
    decay = 1 - q^N
    dist  = d * (1 - q^N) / (1 - q)
    short = decay - dist - borrow * T

The headline numbers are correct **only** when ``d`` (weekly cash as a
fraction of NAV at the start of the week) is calibrated to actual fund
behaviour. The legacy build divided cumulative dollars by today's
price, which is wrong whenever NAV has drifted between ex-dates.

This module replaces the broken scalar with a **NAV-normalized capture
ratio** so the projected cash scales with the scenario ?:

    yield_i  = amount_i / NAV_at_ex_i
    bs_i     = expected_put_spread_loss_weekly(0, sigma_at_ex_i, 1)
    ratio_i  = yield_i / bs_i             # ~0.65 cross-fund per research

When fund-specific history is thin (CWY, new launches) we blend the
fund ratio toward a cross-fund prior. v1 hardcodes 0.65 as the
fallback; the build can self-calibrate to the current YB fleet via
``derive_cross_fund_ratio``.

The module also produces a compact ``income_distribution_calibration``
block for ``dashboard_data.json``: capture ratio + last 16 normalized
events + a 12-event template the JS schedule simulator can replay for
transparency.

References
----------
* Magis Capital Partners (April 2026), *Bucket 2 Income ETF Structural
  Decay* (HTML, repo: ``GraniteShares YieldBOOST/Bucket2_Income_ETF_Decay_Research.html``).
* Cross-fund tests: ``tests/fixtures/bucket2_research.json``.
"""
from __future__ import annotations

import datetime as dt
import math
import statistics
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

# ?????????????????????????????????????????????????????????????????
# Constants - mirror ``assets/income_scenario.js`` exactly.
# ?????????????????????????????????????????????????????????????????
PUT_SPREAD_SHORT_STRIKE = 0.95
PUT_SPREAD_LONG_STRIKE = 0.88
PUT_SPREAD_LEVERAGE = 2.0
DEFAULT_EXPENSE_RATIO_ANNUAL = 0.0099
WEEKS_PER_YEAR = 52

# Bucket 2 research section 1: median capture ratio across 8 candidates is 0.635
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
    """Expected weekly put-spread loss on the 2x sleeve.

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
def load_nav_series_by_ticker(metrics_path: Path) -> dict[str, list[tuple[str, float]]]:
    """Return ``{ticker ? [(iso_date, price), ...]}`` sorted ascending.

    Prefers issuer ``nav`` and falls back to ``close_price`` (for early
    lifecycle rows where the data pipeline only has Yahoo bootstrap close).
    Distribution events that arrive before any NAV record are dropped by
    :func:`normalize_events` (counted as ``nav_missing``).
    """
    if not metrics_path.exists():
        return {}
    try:
        df = pd.read_csv(metrics_path)
    except Exception:
        return {}
    if "ticker" not in df.columns or "date" not in df.columns:
        return {}
    df = df.sort_values(["ticker", "date"], kind="stable")
    out: dict[str, list[tuple[str, float]]] = {}
    for _, row in df.iterrows():
        ticker = _norm_sym(row.get("ticker"))
        date_str = str(row.get("date") or "").strip()
        if not ticker or not date_str:
            continue
        price = None
        for key in ("nav", "close_price"):
            v = row.get(key)
            try:
                f = float(v)
            except (TypeError, ValueError):
                continue
            if math.isfinite(f) and f > 0:
                price = f
                break
        if price is None:
            continue
        out.setdefault(ticker, []).append((date_str, price))
    return out


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
) -> dict | None:
    """Build the per-ETF ``income_distribution_calibration`` block.

    Returns ``None`` when no usable distribution events exist (lets the
    caller leave the field off rather than ship empty objects).
    """
    today = today or dt.datetime.now(dt.UTC).date()
    cutoff = (today - dt.timedelta(days=trailing_window_days)).isoformat()
    if not raw_events:
        return None

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

    run_rate = None
    if in_window:
        median_yield = statistics.median([e["yield_frac"] for e in in_window])
        run_rate = round(float(median_yield) * periods_per_year, 6)

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


def build_legacy_yield_fields(
    block: dict | None,
    current_price: float | None = None,
) -> dict[str, float | int | str | None]:
    """Backward-compatible ``income_yield_*`` scalars with **corrected** semantics.

    Kept for one release so older clients (e.g. ``price_azyy_put.py`` pre-
    update) keep working. New meaning:

    * ``income_yield_trailing_annual`` = NAV-normalized median weekly yield
      x periods_per_year (? stable run-rate at current ? regime).
    * ``income_yield_recent_annual``   = latest NAV-normalized yield x
      periods_per_year (? this-week annualized).
    * Old ``?$/today_price`` and ``x12`` formulations are gone.
    """
    out: dict[str, float | int | str | None] = {}
    if not block:
        return out
    run_rate = block.get("run_rate_annual_display")
    if run_rate is not None:
        out["income_yield_trailing_annual"] = round(float(run_rate), 6)
    latest = block.get("latest_event") or {}
    latest_yield = latest.get("yield_frac")
    ppy = block.get("periods_per_year") or WEEKS_PER_YEAR
    if latest_yield is not None:
        out["income_yield_recent_annual"] = round(float(latest_yield) * float(ppy), 6)
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

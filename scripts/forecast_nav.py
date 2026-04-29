#!/usr/bin/env python3
"""Multi-model NAV forecaster -- emits one row per (symbol, model) so the
scorer can A/B successive model versions inside the same accuracy log.

Models computed (each row may produce 1-4 of these, whichever have inputs):

  delta_v1                  Avellaneda-Zhang single-day plug-in:
                              nav_anchor * exp(beta * r_und) * (1 - ter_d)
  delta_v2_ito              v1 + Ito vol-drag correction:
                              * exp(-(beta^2 - beta)/2 * sigma^2 * dt_years)
                            sigma from forecast_vol_underlying_annual (HARQ blend)
                            dt_years = max(1/252, business_days_since_anchor / 252)
  delta_v3_swap_mark        Leg-by-leg holdings mark:
                              SWAP/COMMON_STOCK/SYNTHETIC_UNDERLYING/ADR ->
                                shares * spot_now (looking up the position's
                                ticker, falling back to the row's underlying)
                              CASH/TREASURY/MONEY_MARKET/FUND_FEE_COMPONENT/
                                OTHER -> carry forward at market_value
                              OPTION_* -> carry forward (priced in
                                yieldboost_putspread_v1 instead)
                              nav_hat = sum(mv_now) / shares_outstanding
  yieldboost_putspread_v1   Same leg pricing as v3 plus Black-Scholes /
                            options_cache mid for OPTION_CALL / OPTION_PUT
                            legs.  Targets income / yieldboost ETFs whose
                            value flows mostly through the option sleeve.

Default model routing (writes ``by_symbol[SYM]`` in ``_latest.json``):

  income_yieldboost,       yieldboost_putspread_v1
  income_put_spread     -> delta_v3_swap_mark
                        -> delta_v2_ito
                        -> delta_v1

  letf, inverse,           delta_v3_swap_mark
  volatility_etp,       -> delta_v2_ito
  passive_low_beta,     -> delta_v1
  other_structured,
  (fallback)

Sanity gate: a candidate model is accepted as default only if its
``nav_hat / nav_anchor`` lies in [0.5, 2.0]; otherwise we fall through to the
next preference.  This avoids letting a buggy holdings row blow up the UI.

The full per-model record is also written under
``by_symbol_models[SYM][model]`` so we can compare them once realized rows
accumulate (see ``score_nav_forecasts.py``).
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("forecast_nav")

# Feature gates -- default off so behavior matches the conservative day-clock
# build that's been logging realized accuracy. Flip these on once we have a
# few weeks of paired forecasts to compare. Both env vars are also honored by
# diamond-creek-quant's copy of this file (kept in sync intentionally).
INTRADAY_DT_ENABLED = os.getenv("NAV_FORECAST_INTRADAY_DT", "0").lower() in ("1", "true", "yes")
APPLY_DISTRIBUTION_ENABLED = os.getenv("NAV_FORECAST_APPLY_DISTRIBUTION", "0").lower() in ("1", "true", "yes")
TRADING_CALENDAR = os.getenv("NAV_FORECAST_TRADING_CALENDAR", "XNYS")
DISTRIBUTIONS_PATH_ENV = os.getenv("NAV_FORECAST_DISTRIBUTIONS_PATH")

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
DASHBOARD_DATA_PATH = DATA_DIR / "dashboard_data.json"
ETF_METRICS_LATEST_PATH = DATA_DIR / "etf_metrics_latest.json"
OPTIONS_CACHE_PATH = DATA_DIR / "options_cache.json"
HOLDINGS_LATEST_PATH = DATA_DIR / "etf_holdings_latest.json"

NAV_DIR = DATA_DIR / "nav_forecasts"
SNAPSHOT_DIR = NAV_DIR / "snapshots"
ANCHORS_PATH = NAV_DIR / "_anchors.json"
LATEST_PATH = NAV_DIR / "_latest.json"

# Single default expense ratio. Most LETF / inverse / vol-ETP products in our
# universe sit between 0.95% and 1.15% -- using one anchor keeps v1 simple and
# limits TER bias to <=0.7 bp per trading day. Override per-symbol later if
# `score_nav_forecasts._metrics_daily.json` shows persistent signed bias.
DEFAULT_TER_ANNUAL = 0.0098
TER_OVERRIDES: dict[str, float] = {}

CLASS_HIGH_CONFIDENCE = {"letf", "inverse"}
CLASS_MEDIUM_CONFIDENCE = {"volatility_etp", "passive_low_beta"}
CLASS_INCOME = {"income_yieldboost", "income_put_spread"}
CLASS_NA_FOR_BETA_MODELS = CLASS_INCOME | {"other_structured"}

SPOT_FRESH_SECONDS = 30 * 60  # 30 minutes

# Sanity envelope: a model NAV more than 2x or less than 0.5x the anchor NAV
# is almost certainly a bug (bad spot, malformed holdings row, etc.). The
# dispatcher refuses to route to such candidates.
NAV_HAT_RATIO_MIN = 0.5
NAV_HAT_RATIO_MAX = 2.0

EQUITY_LIKE_TYPES = {"SWAP", "COMMON_STOCK", "SYNTHETIC_UNDERLYING", "ADR"}
CASH_LIKE_TYPES = {"CASH", "TREASURY", "MONEY_MARKET", "FUND_FEE_COMPONENT", "OTHER"}
OPTION_TYPES = {"OPTION_CALL", "OPTION_PUT", "OPTION"}

OPTION_MULTIPLIER = 100.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _isfin(x: Any) -> bool:
    try:
        f = float(x)
        return math.isfinite(f)
    except (TypeError, ValueError):
        return False


def _f(x: Any) -> float | None:
    try:
        f = float(x)
        return f if math.isfinite(f) else None
    except (TypeError, ValueError):
        return None


def _load_json(p: Path) -> dict:
    if not p.exists():
        return {}
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), allow_nan=False, sort_keys=True)
    tmp.replace(path)


def _append_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, separators=(",", ":"), allow_nan=False, sort_keys=True))
            f.write("\n")


def _ter_daily_for(symbol: str) -> float:
    annual = TER_OVERRIDES.get(symbol.upper(), DEFAULT_TER_ANNUAL)
    return float(annual) / 252.0


def _compose_notes(parts: list[str]) -> str | None:
    parts = [p for p in (parts or []) if p]
    return "; ".join(parts) if parts else None


def _round(x: Any, n: int = 6) -> float | None:
    f = _f(x)
    return round(f, n) if f is not None else None


def _busday_diff(anchor_iso: str, today_iso: str) -> int:
    """Business-day count, positive when today >= anchor. Falls back to a
    rough 5/7 calendar approximation if numpy isn't available (it is, but
    cheap to be defensive)."""
    try:
        import numpy as np
        return int(np.busday_count(anchor_iso, today_iso))
    except Exception:
        try:
            a = datetime.fromisoformat(anchor_iso).date()
            t = datetime.fromisoformat(today_iso).date()
            return max(0, int((t - a).days * 5 / 7))
        except Exception:
            return 1


def _dt_years(anchor_date: str | None, ts_utc: datetime) -> float:
    """Time elapsed since anchor close, in trading-year units (252).

    Two clocks:
      * **Day clock (default)**: business-days-since-anchor / 252, floored
        at 1/252.  Coarse but robust on snapshots far from RTH.
      * **Intraday clock** (``NAV_FORECAST_INTRADAY_DT=1``): adds a fractional
        in-session adjustment using the exchange calendar (``XNYS`` by
        default).  We measure the share of today's trading session that has
        elapsed up to ``ts_utc`` and add that to the integer business-day
        count.  Holidays and early closes are honored automatically because
        the calendar exposes per-session open/close.

    Floor at 1/252 so the v2 vol-drag term is non-zero even on snapshots
    taken on the same UTC date as the anchor (rare but possible during
    pre-market refreshes).
    """
    if not anchor_date:
        return 1.0 / 252.0

    if INTRADAY_DT_ENABLED:
        try:
            return _dt_years_intraday(anchor_date, ts_utc, TRADING_CALENDAR)
        except Exception as exc:  # pragma: no cover - calendar fallback
            LOGGER.warning("intraday dt fallback (%s)", exc)

    today_iso = ts_utc.date().isoformat()
    bdays = _busday_diff(anchor_date, today_iso)
    if bdays < 0:
        bdays = 0
    return max(1.0 / 252.0, bdays / 252.0)


def _dt_years_intraday(anchor_date: str, ts_utc: datetime, calendar: str) -> float:
    """Trading-time Δt in years using ``exchange_calendars``.

    Returns ``business_days_complete + fraction_of_today_session) / 252``.
    For a snap taken at 11:30 ET on the day after a normal-close anchor we
    get roughly ``(1 + 2/6.5) / 252`` -- close to 1.3 trading days. On the
    half-day before Christmas Eve the session is 3.5 hours so the fraction
    grows faster, which is what we want.
    """
    import exchange_calendars as xcals  # type: ignore

    cal = xcals.get_calendar(calendar)
    a = datetime.fromisoformat(anchor_date).date()
    today = ts_utc.date()
    # Integer business days strictly between anchor and today.
    sessions_between = cal.sessions_in_range(a.isoformat(), today.isoformat())
    bdays_complete = max(0, len(sessions_between) - 1)

    # Fraction of *today's* session elapsed (0 if pre-open, 1 if post-close).
    frac = 0.0
    if cal.is_session(today.isoformat()):
        ts_naive_utc = ts_utc.astimezone(timezone.utc).replace(tzinfo=None)
        try:
            session_open = cal.session_open(today.isoformat()).to_pydatetime().replace(tzinfo=None)
            session_close = cal.session_close(today.isoformat()).to_pydatetime().replace(tzinfo=None)
            if ts_naive_utc <= session_open:
                frac = 0.0
            elif ts_naive_utc >= session_close:
                frac = 1.0
            else:
                total = (session_close - session_open).total_seconds()
                used = (ts_naive_utc - session_open).total_seconds()
                frac = max(0.0, min(1.0, used / total)) if total > 0 else 0.0
        except Exception:
            frac = 0.0
    return max(1.0 / 252.0, (bdays_complete + frac) / 252.0)


def _load_distributions_for_today(today_iso: str) -> dict[str, float]:
    """Return ``{symbol: cash_per_share}`` for ETFs whose ex-date == today.

    Reads ``data/etf_distributions.json`` (or whatever
    ``NAV_FORECAST_DISTRIBUTIONS_PATH`` points to).  Best effort -- a missing
    or malformed file just returns ``{}`` so the caller can fall through to
    the unadjusted anchor NAV.
    """
    path = (
        Path(DISTRIBUTIONS_PATH_ENV)
        if DISTRIBUTIONS_PATH_ENV
        else DATA_DIR / "etf_distributions.json"
    )
    if not path.exists():
        return {}
    try:
        payload = _load_json(path)
    except Exception:
        return {}
    out: dict[str, float] = {}
    by_sym = payload.get("by_symbol") or payload.get("symbols") or payload
    if not isinstance(by_sym, dict):
        return {}
    for sym, entry in by_sym.items():
        events = entry.get("events") if isinstance(entry, dict) else None
        if not isinstance(events, list):
            # Some shapes ship a single ``next_ex_date`` + ``amount``.
            ex_date = (entry or {}).get("next_ex_date") if isinstance(entry, dict) else None
            amt = (entry or {}).get("next_amount") if isinstance(entry, dict) else None
            if ex_date == today_iso and _isfin(amt):
                out[str(sym).upper()] = float(amt)
            continue
        for evt in events:
            if not isinstance(evt, dict):
                continue
            if str(evt.get("ex_date")) == today_iso and _isfin(evt.get("amount")):
                out[str(sym).upper()] = float(evt["amount"])
                break
    return out


def _adjust_anchor_for_ex_distribution(
    nav_anchor: float | None, symbol: str, distributions_today: dict[str, float],
) -> tuple[float | None, float]:
    """Pre-trade adjustment: if today is ETF ``symbol``'s ex-date, the
    market price drops by ~cash distribution at the open.  We mirror that
    in the model NAV so ``premium_bp`` doesn't flash a phantom 30-bp
    premium between 09:30 and the close.

    Returns ``(adjusted_nav_anchor, distribution_applied)``.  When
    ``distribution_applied > 0`` the caller should annotate ``notes``.
    """
    if not APPLY_DISTRIBUTION_ENABLED or nav_anchor is None or nav_anchor <= 0:
        return nav_anchor, 0.0
    amt = distributions_today.get(str(symbol).upper(), 0.0)
    if not _isfin(amt) or amt <= 0:
        return nav_anchor, 0.0
    return max(0.0, float(nav_anchor) - float(amt)), float(amt)


# ---------------------------------------------------------------------------
# Model 1: delta_v1
# ---------------------------------------------------------------------------

def compute_v1(
    nav_anchor: float,
    beta: float,
    und_ret: float,
    ter_daily: float,
) -> float:
    """delta_v1 closed form: ``nav_anchor * exp(beta * r_und) * (1 - ter_daily)``."""
    return float(nav_anchor) * math.exp(float(beta) * float(und_ret)) * (1.0 - float(ter_daily))


# ---------------------------------------------------------------------------
# Model 2: delta_v2_ito (v1 + path-dependent vol drag)
# ---------------------------------------------------------------------------

def compute_v2_ito(
    nav_anchor: float,
    beta: float,
    und_ret: float,
    sigma_annual: float,
    dt_years: float,
    ter_daily: float,
) -> float:
    """v1 multiplied by ``exp(-(beta^2 - beta)/2 * sigma^2 * dt)``.

    For beta in {1, 0} the correction is identically zero (no leverage drag).
    For |beta| > 1 it pulls the model NAV down; for inverse (beta < 0) the
    (beta^2 - beta) term is strictly positive so the drag is also negative.
    """
    base = compute_v1(nav_anchor, beta, und_ret, ter_daily)
    drag_logret = -(float(beta) ** 2 - float(beta)) / 2.0 * float(sigma_annual) ** 2 * float(dt_years)
    return base * math.exp(drag_logret)


# ---------------------------------------------------------------------------
# OCC parsing (yieldboost_putspread_v1)
# ---------------------------------------------------------------------------

# Standard OCC: ROOT YYMMDD<C|P>STRIKE8 (zero-padded, last 3 implied decimals).
# Some issuers prefix the root with a digit/letter after a corp action
# (e.g. ``2AI``, ``AMD1``), and they may or may not include a space.
# We accept either form and strip any leading [0-9]+ from the root, since the
# options_cache underlying key is the canonical equity ticker.
_OCC_RE = re.compile(
    r"^\s*(?P<root>[A-Z0-9][A-Z0-9.\-]{0,9}?)\s*(?P<expiry>\d{6})(?P<type>[CP])(?P<strike8>\d{8})\s*$"
)


def parse_occ(position_ticker: str | None) -> dict | None:
    """Parse an OCC-style option ticker. Returns ``None`` on failure."""
    if not position_ticker:
        return None
    m = _OCC_RE.match(str(position_ticker).upper())
    if not m:
        return None
    root = m.group("root")
    # Strip a single leading digit prefix (e.g. ``2AI`` -> ``AI``) which
    # some issuers add after corporate actions; the underlying ticker in
    # options_cache stays as the equity root.
    while root and root[0].isdigit() and len(root) > 1:
        root = root[1:]
    expiry_raw = m.group("expiry")
    try:
        yy = int(expiry_raw[0:2])
        mm = int(expiry_raw[2:4])
        dd = int(expiry_raw[4:6])
        # 2-digit years: pre-1999 contracts are extinct, so always 20YY.
        expiry_iso = f"20{yy:02d}-{mm:02d}-{dd:02d}"
    except ValueError:
        return None
    strike = int(m.group("strike8")) / 1000.0
    contract_type = "call" if m.group("type") == "C" else "put"
    return {
        "root": root,
        "expiry": expiry_iso,
        "strike": strike,
        "type": contract_type,
    }


def lookup_option_mark(
    parsed: dict,
    options_cache: dict,
) -> tuple[float | None, dict]:
    """Return ``(mid_price_now, meta)`` from options_cache, if present.

    ``meta`` includes the matched contract dict (or empty if unmatched) plus
    a rough quote-age indicator.
    """
    root = parsed.get("root")
    if not root:
        return None, {"matched": False, "reason": "no root"}
    sym_entry = (options_cache.get("symbols") or {}).get(root)
    if not sym_entry:
        return None, {"matched": False, "reason": "underlying not in options_cache"}
    contracts = sym_entry.get("options") or []
    target_expiry = parsed.get("expiry")
    target_strike = parsed.get("strike")
    target_type = parsed.get("type")
    for c in contracts:
        if str(c.get("expiration_date")) != target_expiry:
            continue
        if str(c.get("contract_type", "")).lower() != target_type:
            continue
        if not _isfin(c.get("strike_price")):
            continue
        if abs(float(c["strike_price"]) - float(target_strike)) > 1e-4:
            continue
        mid = _f(c.get("mid"))
        return mid, {
            "matched": True,
            "iv": _f(c.get("iv")),
            "delta": _f(c.get("delta")),
            "ticker": c.get("ticker"),
        }
    return None, {"matched": False, "reason": "expiry/strike/type not in chain"}


# ---------------------------------------------------------------------------
# Holdings mark (delta_v3_swap_mark + yieldboost_putspread_v1)
# ---------------------------------------------------------------------------

def _equity_spot(
    position_ticker: str | None,
    fallback_underlying: str | None,
    options_cache: dict,
) -> tuple[float | None, str | None]:
    """Return the most recent equity spot for a holdings row's underlying.

    Tries ``position_ticker`` first; falls back to the row's own
    ``underlying`` field. Returns ``(spot, source_ticker)``.
    """
    syms = options_cache.get("symbols") or {}
    candidates = []
    if position_ticker:
        candidates.append(str(position_ticker).upper())
    if fallback_underlying:
        candidates.append(str(fallback_underlying).upper())
    for c in candidates:
        e = syms.get(c)
        if not e:
            continue
        s = _f(e.get("spot"))
        if s is not None and s > 0:
            return s, c
    return None, None


def mark_holdings(
    legs: list[dict],
    *,
    fallback_underlying: str | None,
    options_cache: dict,
    price_options: bool,
) -> dict:
    """Compute the **change** in MV per leg vs the anchor (level differencing).

    Why a delta instead of a level mark: issuer holdings files report the
    gross long equity exposure of LETFs (e.g. for a 2x ETF the swap notional
    at full underlying price), but they do NOT show the offsetting swap
    counterparty liability.  Summing ``market_value`` therefore equals
    *gross* exposure, not NAV; differencing per leg cancels the unobserved
    cash liability under the assumption that it is constant between two
    nearby snapshots (true to first order between anchor close and now).

    Per-leg behaviour:
      * EQUITY_LIKE_TYPES (SWAP / COMMON_STOCK / SYNTHETIC_UNDERLYING / ADR):
          spot_anchor = ``leg.price`` from the holdings file
          spot_now    = options_cache spot for ``position_ticker``, falling
                        back to the row's ``underlying`` ticker
          delta_mv   += shares * (spot_now - spot_anchor)
      * CASH_LIKE_TYPES (CASH / TREASURY / MONEY_MARKET / FUND_FEE_COMPONENT
        / OTHER):
          delta_mv += 0  (carry forward)
      * OPTION_TYPES with ``price_options=True`` (yieldboost):
          mid_anchor = market_value / (shares * 100)
          mid_now    = options_cache mid for the matched contract
          delta_mv  += shares * (mid_now - mid_anchor) * 100
        Otherwise OPTION legs carry forward (delta_mv += 0).

    The dispatcher treats a leg as "priced" when its delta could be
    computed; legs that fall back to zero-delta are not counted, which lets
    the confidence routing flag products whose value flows through legs we
    can't see (e.g. swap-only LETF with no spot for the underlying).
    """
    delta_mv = 0.0
    total_mv_anchor = 0.0
    legs_priced = 0
    legs_total = 0
    equity_legs_priced = 0
    equity_legs_total = 0
    option_legs_priced = 0
    option_legs_total = 0
    skipped: list[str] = []

    for leg in legs:
        sec_type = str(leg.get("security_type") or "").upper()
        shares = _f(leg.get("shares"))
        mv_anchor = _f(leg.get("market_value")) or 0.0
        total_mv_anchor += mv_anchor
        legs_total += 1
        leg_delta: float = 0.0
        priced = False

        if sec_type in EQUITY_LIKE_TYPES:
            equity_legs_total += 1
            spot_now, _src = _equity_spot(
                leg.get("position_ticker"), fallback_underlying, options_cache,
            )
            spot_anchor = _f(leg.get("price"))
            if (
                spot_now is not None and shares is not None
                and spot_anchor is not None and spot_anchor > 0
            ):
                leg_delta = shares * (spot_now - spot_anchor)
                equity_legs_priced += 1
                priced = True
            else:
                skipped.append(f"equity:{leg.get('position_ticker') or fallback_underlying}")
        elif sec_type in CASH_LIKE_TYPES:
            # Carry forward: delta_mv += 0. Counts as "priced" because we
            # explicitly know its delta is zero (modulo rate accruals, which
            # we ignore at the trading-day horizon).
            priced = True
        elif sec_type in OPTION_TYPES:
            option_legs_total += 1
            if price_options and shares is not None and abs(shares) > 0:
                parsed = parse_occ(leg.get("position_ticker"))
                if parsed is not None:
                    mid_now, _meta = lookup_option_mark(parsed, options_cache)
                    mid_anchor = mv_anchor / (shares * OPTION_MULTIPLIER)
                    if mid_now is not None and math.isfinite(mid_anchor):
                        leg_delta = shares * (mid_now - mid_anchor) * OPTION_MULTIPLIER
                        option_legs_priced += 1
                        priced = True
                    else:
                        skipped.append(f"opt-no-quote:{leg.get('position_ticker')}")
                else:
                    skipped.append(f"opt-occ-fail:{leg.get('position_ticker')}")
            # OPTION legs in v3 (price_options=False) carry forward but do
            # NOT count as priced -- their movement is a known gap in v3.
        else:
            skipped.append(f"unk:{sec_type}")

        if priced:
            legs_priced += 1
        if math.isfinite(leg_delta):
            delta_mv += leg_delta

    return {
        "delta_mv": delta_mv,
        "mv_anchor_total": total_mv_anchor,
        "legs_priced": legs_priced,
        "legs_total": legs_total,
        "equity_legs_priced": equity_legs_priced,
        "equity_legs_total": equity_legs_total,
        "option_legs_priced": option_legs_priced,
        "option_legs_total": option_legs_total,
        "skipped": skipped[:10],
    }


# ---------------------------------------------------------------------------
# ForecastRecord + per-model builders
# ---------------------------------------------------------------------------

@dataclass
class ForecastRecord:
    ts: str
    symbol: str
    model: str
    is_default: bool
    confidence: str
    product_class: str | None
    und_symbol: str | None
    und_spot_t: float | None
    und_spot_anchor: float | None
    und_anchor_date: str | None
    und_spot_age_sec: float | None
    beta: float | None
    ter_daily: float
    nav_anchor: float | None
    nav_anchor_date: str | None
    nav_hat: float | None
    etf_last: float | None
    etf_last_ts: str | None
    premium_bp: float | None
    notes: str | None
    # v2/yb diagnostics
    sigma_annual: float | None = None
    dt_years: float | None = None
    vol_drag_logret: float | None = None
    sigma_source: str | None = None
    # v3/yb diagnostics
    legs_priced: int | None = None
    legs_total: int | None = None
    equity_legs_priced: int | None = None
    equity_legs_total: int | None = None
    option_legs_priced: int | None = None
    option_legs_total: int | None = None
    mv_total_now: float | None = None
    shares_outstanding: float | None = None
    holdings_skipped: list[str] = field(default_factory=list)


# ---------- inputs builder (shared) ------------------------------------------

def _gather_inputs(
    record: dict,
    anchor: dict | None,
    options_cache: dict,
    distributions_today: dict[str, float] | None = None,
) -> dict:
    """Resolve everything the per-model builders need. ``anchor`` may be None."""
    sym = str(record.get("symbol") or "").upper()
    pc = (str(record.get("product_class") or "").strip().lower() or None)
    is_yb = bool(record.get("is_yieldboost"))
    und = str(record.get("underlying") or "").upper() or None
    distributions_today = distributions_today or {}

    syms = options_cache.get("symbols") or {}
    und_entry = syms.get(und) if und else None
    und_spot_t = None
    und_spot_age = None
    if und_entry:
        s = _f(und_entry.get("spot"))
        a = _f(und_entry.get("cache_age_seconds"))
        if s is not None and s > 0:
            und_spot_t = s
            und_spot_age = a

    etf_entry = syms.get(sym)
    etf_last = None
    etf_last_ts = None
    if etf_entry:
        s = _f(etf_entry.get("spot"))
        if s is not None and s > 0:
            etf_last = s
            etf_last_ts = etf_entry.get("updated_at")

    nav_anchor = _f(anchor.get("nav_close")) if anchor else None
    und_anchor = _f(anchor.get("und_close")) if anchor else None
    nav_anchor_date = anchor.get("as_of_date") if anchor else None
    shares_out = _f(anchor.get("shares_outstanding")) if anchor else None

    # Optional pre-trade adjustment for ETFs that go ex-distribution today.
    nav_anchor, distribution_applied = _adjust_anchor_for_ex_distribution(
        nav_anchor, sym, distributions_today,
    )

    sigma_annual = _f(record.get("forecast_vol_underlying_annual"))
    sigma_source = "forecast_vol_underlying_annual"
    if sigma_annual is None or sigma_annual <= 0:
        sigma_annual = _f(record.get("vol_underlying_annual"))
        sigma_source = "vol_underlying_annual"
    if sigma_annual is None or sigma_annual <= 0:
        sigma_annual = None
        sigma_source = None

    beta = _f(record.get("beta"))

    return {
        "symbol": sym,
        "product_class": pc,
        "is_yieldboost": is_yb,
        "und_symbol": und,
        "und_spot_t": und_spot_t,
        "und_spot_age_sec": und_spot_age,
        "etf_last": etf_last,
        "etf_last_ts": etf_last_ts,
        "nav_anchor": nav_anchor,
        "nav_anchor_date": nav_anchor_date,
        "und_anchor": und_anchor,
        "shares_outstanding": shares_out,
        "sigma_annual": sigma_annual,
        "sigma_source": sigma_source,
        "beta": beta,
        "distribution_applied": distribution_applied,
    }


def _make_record(
    *, ts_iso: str, model: str, inputs: dict, ter_daily: float,
    nav_hat: float | None, confidence: str, notes: list[str],
    extra: dict | None = None,
) -> ForecastRecord:
    extra = extra or {}
    nav_anchor = inputs["nav_anchor"]
    premium_bp = None
    etf_last = inputs["etf_last"]
    if (
        nav_hat is not None and nav_hat > 0
        and etf_last is not None
    ):
        premium_bp = (etf_last - nav_hat) / nav_hat * 1.0e4

    return ForecastRecord(
        ts=ts_iso,
        symbol=inputs["symbol"],
        model=model,
        is_default=False,  # set later by dispatcher
        confidence=confidence,
        product_class=inputs["product_class"],
        und_symbol=inputs["und_symbol"],
        und_spot_t=_round(inputs["und_spot_t"]),
        und_spot_anchor=_round(inputs["und_anchor"]),
        und_anchor_date=inputs["nav_anchor_date"],
        und_spot_age_sec=_round(inputs["und_spot_age_sec"], 1),
        beta=_round(inputs["beta"]),
        ter_daily=round(ter_daily, 8),
        nav_anchor=_round(nav_anchor),
        nav_anchor_date=inputs["nav_anchor_date"],
        nav_hat=_round(nav_hat),
        etf_last=_round(etf_last),
        etf_last_ts=inputs["etf_last_ts"],
        premium_bp=_round(premium_bp, 2),
        notes=_compose_notes(notes),
        sigma_annual=_round(extra.get("sigma_annual"), 6),
        dt_years=_round(extra.get("dt_years"), 6),
        vol_drag_logret=_round(extra.get("vol_drag_logret"), 6),
        sigma_source=extra.get("sigma_source"),
        legs_priced=extra.get("legs_priced"),
        legs_total=extra.get("legs_total"),
        equity_legs_priced=extra.get("equity_legs_priced"),
        equity_legs_total=extra.get("equity_legs_total"),
        option_legs_priced=extra.get("option_legs_priced"),
        option_legs_total=extra.get("option_legs_total"),
        mv_total_now=_round(extra.get("mv_total_now")),
        shares_outstanding=_round(extra.get("shares_outstanding")),
        holdings_skipped=extra.get("holdings_skipped") or [],
    )


# ---------- per-model builders -----------------------------------------------

def _classify_beta_model_confidence(inputs: dict, model: str) -> tuple[str, list[str]]:
    """Confidence routing for the beta-based models (v1, v2)."""
    pc = inputs["product_class"]
    notes: list[str] = []
    if not pc:
        return "na", ["missing product_class"]
    if inputs["is_yieldboost"]:
        return "na", ["yieldboost income product (beta-only model not appropriate)"]
    if pc in CLASS_NA_FOR_BETA_MODELS:
        return "na", [f"product_class={pc} (beta-only model skipped)"]
    if inputs["beta"] is None:
        return "na", ["missing beta"]
    if inputs["nav_anchor"] is None or inputs["nav_anchor"] <= 0:
        return "na", ["missing nav_close in anchor"]
    if inputs["und_anchor"] is None or inputs["und_anchor"] <= 0:
        return "na", ["missing und_close in anchor"]
    if inputs["und_spot_t"] is None:
        return "na", ["missing underlying spot"]

    if (inputs["und_spot_age_sec"] or 0) > SPOT_FRESH_SECONDS:
        notes.append(f"spot stale {int(inputs['und_spot_age_sec'])}s")

    if pc in CLASS_HIGH_CONFIDENCE:
        return ("medium" if notes else "high"), notes
    if pc in CLASS_MEDIUM_CONFIDENCE:
        notes.append(f"product_class={pc}")
        return "medium", notes
    notes.append(f"product_class={pc}")
    return "medium", notes


def build_v1(inputs: dict, ts_iso: str, ter_daily: float) -> ForecastRecord | None:
    confidence, notes = _classify_beta_model_confidence(inputs, "delta_v1")
    nav_hat = None
    if confidence != "na":
        try:
            r_und = math.log(inputs["und_spot_t"] / inputs["und_anchor"])
            nav_hat = compute_v1(inputs["nav_anchor"], inputs["beta"], r_und, ter_daily)
        except (ValueError, ZeroDivisionError) as e:
            confidence = "na"
            notes.append(f"v1 math: {e}")
            nav_hat = None
    return _make_record(
        ts_iso=ts_iso, model="delta_v1", inputs=inputs, ter_daily=ter_daily,
        nav_hat=nav_hat, confidence=confidence, notes=notes,
    )


def build_v2_ito(
    inputs: dict, ts_iso: str, ter_daily: float, ts_utc: datetime,
) -> ForecastRecord | None:
    confidence, notes = _classify_beta_model_confidence(inputs, "delta_v2_ito")
    sigma = inputs["sigma_annual"]
    if confidence != "na" and (sigma is None or sigma <= 0):
        confidence = "na"
        notes.append("missing sigma_annual")
    nav_hat = None
    dt_y = _dt_years(inputs["nav_anchor_date"], ts_utc)
    drag_logret = None
    if confidence != "na":
        try:
            r_und = math.log(inputs["und_spot_t"] / inputs["und_anchor"])
            beta = inputs["beta"]
            drag_logret = -((beta ** 2 - beta) / 2.0) * (sigma ** 2) * dt_y
            nav_hat = compute_v1(inputs["nav_anchor"], beta, r_und, ter_daily) * math.exp(drag_logret)
        except (ValueError, ZeroDivisionError) as e:
            confidence = "na"
            notes.append(f"v2 math: {e}")
            nav_hat = None
    return _make_record(
        ts_iso=ts_iso, model="delta_v2_ito", inputs=inputs, ter_daily=ter_daily,
        nav_hat=nav_hat, confidence=confidence, notes=notes,
        extra={
            "sigma_annual": sigma,
            "dt_years": dt_y,
            "vol_drag_logret": drag_logret,
            "sigma_source": inputs["sigma_source"],
        },
    )


def _build_holdings_model(
    inputs: dict, ts_iso: str, ter_daily: float,
    holdings_legs: list[dict] | None, options_cache: dict,
    *, model_tag: str, price_options: bool, ts_utc: datetime,
) -> ForecastRecord:
    notes: list[str] = []
    confidence = "high"
    nav_hat: float | None = None
    extra: dict[str, Any] = {}
    shares = inputs["shares_outstanding"]
    nav_anchor = inputs["nav_anchor"]

    if not holdings_legs:
        confidence = "na"
        notes.append("no holdings on file")
    elif shares is None or shares <= 0:
        confidence = "na"
        notes.append("missing shares_outstanding in anchor")
    elif nav_anchor is None or nav_anchor <= 0:
        confidence = "na"
        notes.append("missing nav_anchor")
    else:
        marked = mark_holdings(
            holdings_legs,
            fallback_underlying=inputs["und_symbol"],
            options_cache=options_cache,
            price_options=price_options,
        )
        eq_total = marked["equity_legs_total"]
        eq_priced = marked["equity_legs_priced"]
        opt_total = marked["option_legs_total"]
        opt_priced = marked["option_legs_priced"]

        if marked["legs_total"] == 0:
            confidence = "na"
            notes.append("zero legs in holdings")
        elif eq_total == 0 and (not price_options or opt_priced == 0):
            # Nothing to drive a delta -> v3 would just reproduce nav_anchor.
            # Mark na so the dispatcher falls through to v2/v1 instead of
            # showing a "level" that's just the anchor.
            confidence = "na"
            notes.append("no priced equity/option legs")
        else:
            try:
                dt_y = _dt_years(inputs["nav_anchor_date"], ts_utc)
                ter_drag = max(0.0, ter_daily * dt_y * 252.0)
                nav_hat = (nav_anchor + marked["delta_mv"] / shares) * (1.0 - ter_drag)
            except (ValueError, ZeroDivisionError) as e:
                confidence = "na"
                notes.append(f"holdings math: {e}")
                nav_hat = None
            else:
                # Coverage-based confidence routing. Equity-leg coverage is
                # the dominant driver for LETF / inverse / vol-ETP rows.
                if eq_total > 0 and eq_priced / max(1, eq_total) < 0.5:
                    confidence = "medium"
                    notes.append(f"equity legs priced {eq_priced}/{eq_total}")
                if price_options and opt_total > 0 and opt_priced / max(1, opt_total) < 0.5:
                    confidence = "medium"
                    notes.append(f"option legs priced {opt_priced}/{opt_total}")
                if nav_hat is not None and nav_hat <= 0:
                    confidence = "na"
                    notes.append("non-positive nav_hat")
                    nav_hat = None
            extra = {
                "legs_priced": marked["legs_priced"],
                "legs_total": marked["legs_total"],
                "equity_legs_priced": marked["equity_legs_priced"],
                "equity_legs_total": marked["equity_legs_total"],
                "option_legs_priced": marked["option_legs_priced"],
                "option_legs_total": marked["option_legs_total"],
                "mv_total_now": (
                    nav_anchor * shares + marked["delta_mv"]
                    if nav_hat is not None else None
                ),
                "shares_outstanding": shares,
                "holdings_skipped": marked["skipped"],
            }

    return _make_record(
        ts_iso=ts_iso, model=model_tag, inputs=inputs, ter_daily=ter_daily,
        nav_hat=nav_hat, confidence=confidence, notes=notes, extra=extra,
    )


def build_v3_swap_mark(
    inputs: dict, ts_iso: str, ter_daily: float,
    holdings_legs: list[dict] | None, options_cache: dict,
    *, ts_utc: datetime,
) -> ForecastRecord:
    return _build_holdings_model(
        inputs, ts_iso, ter_daily, holdings_legs, options_cache,
        model_tag="delta_v3_swap_mark", price_options=False, ts_utc=ts_utc,
    )


def build_yieldboost_putspread_v1(
    inputs: dict, ts_iso: str, ter_daily: float,
    holdings_legs: list[dict] | None, options_cache: dict,
    *, ts_utc: datetime,
) -> ForecastRecord:
    return _build_holdings_model(
        inputs, ts_iso, ter_daily, holdings_legs, options_cache,
        model_tag="yieldboost_putspread_v1", price_options=True, ts_utc=ts_utc,
    )


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------

def select_default_model(
    records: list[ForecastRecord], product_class: str | None, is_yieldboost: bool,
) -> str | None:
    """Return the model tag we want to surface in `_latest.json[by_symbol]`."""
    if is_yieldboost or product_class in CLASS_INCOME:
        order = ["yieldboost_putspread_v1", "delta_v3_swap_mark", "delta_v2_ito", "delta_v1"]
    else:
        order = ["delta_v3_swap_mark", "delta_v2_ito", "delta_v1"]

    by_model = {r.model: r for r in records if r.confidence != "na" and r.nav_hat is not None}
    for m in order:
        if m in by_model:
            r = by_model[m]
            if r.nav_anchor and r.nav_anchor > 0:
                ratio = r.nav_hat / r.nav_anchor
                if NAV_HAT_RATIO_MIN < ratio < NAV_HAT_RATIO_MAX:
                    return m
    # Sanity violation in every candidate -> better to surface "no fair
    # value" than a known-wrong number. The full per-model rows still land
    # in by_symbol_models for offline diagnostics.
    return None


# ---------------------------------------------------------------------------
# Per-symbol assembly
# ---------------------------------------------------------------------------

def build_forecasts_for_symbol(
    record: dict,
    anchor: dict | None,
    options_cache: dict,
    holdings_legs: list[dict] | None,
    ts_utc: datetime,
    distributions_today: dict[str, float] | None = None,
) -> tuple[list[ForecastRecord], str | None]:
    """Compute every applicable model for a single screener row.

    Returns ``(records, default_model)``. ``records`` always contains at
    least one entry (a fully populated v1 record, even if its confidence is
    ``na``), so the snapshot file documents that we tried.
    """
    sym = str(record.get("symbol") or "").upper()
    if not sym:
        return [], None
    inputs = _gather_inputs(record, anchor, options_cache, distributions_today)
    ter_daily = _ter_daily_for(sym)
    ts_iso = ts_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    rows: list[ForecastRecord] = []
    extra_notes: list[str] = []
    if inputs.get("distribution_applied", 0.0) > 0:
        extra_notes.append(f"ex-dist applied -{inputs['distribution_applied']:.4f}")

    rows.append(build_v1(inputs, ts_iso, ter_daily))
    rows.append(build_v2_ito(inputs, ts_iso, ter_daily, ts_utc))
    rows.append(build_v3_swap_mark(inputs, ts_iso, ter_daily, holdings_legs, options_cache, ts_utc=ts_utc))
    rows.append(build_yieldboost_putspread_v1(inputs, ts_iso, ter_daily, holdings_legs, options_cache, ts_utc=ts_utc))

    if extra_notes:
        for r in rows:
            r.notes = _compose_notes(([r.notes] if r.notes else []) + extra_notes)

    default = select_default_model(rows, inputs["product_class"], inputs["is_yieldboost"])
    if default is not None:
        for r in rows:
            r.is_default = (r.model == default)
    return rows, default


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-model NAV forecaster.")
    parser.add_argument("--dashboard-data", default=str(DASHBOARD_DATA_PATH))
    parser.add_argument("--anchors", default=str(ANCHORS_PATH))
    parser.add_argument("--options-cache", default=str(OPTIONS_CACHE_PATH))
    parser.add_argument("--holdings", default=str(HOLDINGS_LATEST_PATH))
    parser.add_argument("--out-snapshot", default=None)
    parser.add_argument("--out-latest", default=str(LATEST_PATH))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    dashboard = _load_json(Path(args.dashboard_data))
    options_cache = _load_json(Path(args.options_cache))
    anchors = _load_json(Path(args.anchors))
    holdings_payload = _load_json(Path(args.holdings))
    holdings_by_sym = (holdings_payload.get("by_symbol") or {}) if holdings_payload else {}

    records = dashboard.get("records") or dashboard.get("rows") or []
    by_anchor = anchors.get("by_symbol") or {}

    ts = datetime.now(UTC)
    snap_path = (
        Path(args.out_snapshot)
        if args.out_snapshot
        else SNAPSHOT_DIR / f"{ts.date().isoformat()}.jsonl"
    )

    distributions_today: dict[str, float] = {}
    if APPLY_DISTRIBUTION_ENABLED:
        distributions_today = _load_distributions_for_today(ts.date().isoformat())
        if distributions_today:
            LOGGER.info("ex-dist today: %s", distributions_today)

    snapshot_rows: list[dict] = []
    by_symbol_default: dict[str, dict] = {}
    by_symbol_models: dict[str, dict[str, dict]] = {}
    default_model_count: dict[str, int] = {}
    confidence_count: dict[str, int] = {"high": 0, "medium": 0, "na": 0}

    for rec in records:
        sym = str(rec.get("symbol") or "").upper()
        if not sym:
            continue
        try:
            rows, default = build_forecasts_for_symbol(
                rec,
                by_anchor.get(sym),
                options_cache,
                holdings_by_sym.get(sym),
                ts,
                distributions_today,
            )
        except Exception as e:  # pragma: no cover - defensive
            LOGGER.warning("forecast %s failed: %s", sym, e)
            continue
        if not rows:
            continue

        for r in rows:
            d = asdict(r)
            snapshot_rows.append(d)
            by_symbol_models.setdefault(sym, {})[r.model] = d

        if default is None:
            default_model_count["na"] = default_model_count.get("na", 0) + 1
            confidence_count["na"] += 1
            continue

        default_record = next((r for r in rows if r.model == default), None)
        if default_record is None:
            continue
        d = asdict(default_record)
        by_symbol_default[sym] = d
        default_model_count[default] = default_model_count.get(default, 0) + 1
        confidence_count[default_record.confidence] = (
            confidence_count.get(default_record.confidence, 0) + 1
        )

    if snapshot_rows:
        _append_jsonl(snap_path, snapshot_rows)

    payload = {
        "build_time": ts.isoformat().replace("+00:00", "Z"),
        "models_run": ["delta_v1", "delta_v2_ito", "delta_v3_swap_mark", "yieldboost_putspread_v1"],
        "default_models_count": default_model_count,
        "confidence_count": confidence_count,
        "anchor_date": anchors.get("as_of_date"),
        "anchor_symbols": len(by_anchor),
        "holdings_symbols": len(holdings_by_sym),
        "by_symbol": by_symbol_default,
        "by_symbol_models": by_symbol_models,
        "default_model_for_symbol": {
            sym: row["model"] for sym, row in by_symbol_default.items()
        },
    }
    _atomic_write_json(Path(args.out_latest), payload)

    LOGGER.info(
        "forecast_nav: snapshot=%s rows=%d (high=%d medium=%d na=%d) "
        "default_models=%s anchor_date=%s holdings_symbols=%d",
        snap_path, len(snapshot_rows),
        confidence_count.get("high", 0),
        confidence_count.get("medium", 0),
        confidence_count.get("na", 0),
        default_model_count, anchors.get("as_of_date"),
        len(holdings_by_sym),
    )


if __name__ == "__main__":
    main()

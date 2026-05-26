"""Event-vol decomposition for YieldBOOST VRP (earnings + mystery events).

Strips scheduled event jump variance from implied/realized vol on 2x sleeves.
See PLAN_event_devol_and_forward_straddle_decomposition.md.
"""
from __future__ import annotations

import json
import math
import re
import statistics
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

LEVERAGE_BETA = 2.0
LEVERAGE_VAR_MULT = LEVERAGE_BETA ** 2  # 4x for 2x sleeve event variance
TRADING_DAYS = 252
MAD_SCALE = 0.6745


@dataclass(frozen=True)
class CalendarEvent:
    underlying: str
    event_type: str
    event_date: date | None = None
    window_start: date | None = None
    window_end: date | None = None
    source: str = "unknown"
    implied_move_pct: float | None = None
    historical_move_pct_mad: float | None = None
    event_skew_sign: str | None = None
    nearest_known_event: str | None = None

    def dates_in_range(self, start: date, end: date) -> list[date]:
        if self.event_date is not None:
            return [self.event_date] if start <= self.event_date <= end else []
        if self.window_start is not None and self.window_end is not None:
            if self.window_end < start or self.window_start > end:
                return []
            return [self.window_start]
        return []


def _parse_date(val: object) -> date | None:
    if val is None:
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()
    txt = str(val).strip()
    if not txt:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(txt, fmt).date()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(txt.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _parse_float(val: object) -> float | None:
    if val is None:
        return None
    try:
        v = float(val)
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def load_json_calendar(path: Path | str | None) -> dict[str, Any]:
    if path is None:
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def calendar_items_to_events(payload: dict[str, Any] | None) -> list[CalendarEvent]:
    if not payload:
        return []
    out: list[CalendarEvent] = []
    for row in payload.get("items") or []:
        if not isinstance(row, dict):
            continue
        und = str(row.get("underlying") or "").upper().strip()
        if not und:
            continue
        ev_date = _parse_date(row.get("event_date"))
        w_start = _parse_date(row.get("window_start"))
        w_end = _parse_date(row.get("window_end"))
        out.append(CalendarEvent(
            underlying=und,
            event_type=str(row.get("event_type") or "unknown").lower(),
            event_date=ev_date,
            window_start=w_start,
            window_end=w_end,
            source=str(row.get("source") or "unknown"),
            implied_move_pct=_parse_float(row.get("implied_move_pct_today") or row.get("implied_event_move_pct")),
            historical_move_pct_mad=_parse_float(row.get("historical_move_pct_mad")),
            event_skew_sign=row.get("event_skew_sign"),
            nearest_known_event=row.get("nearest_known_event"),
        ))
    return out


def merge_event_calendars(
    *payloads: dict[str, Any] | None,
) -> dict[str, Any]:
    """Merge known + inferred + macro into one combined artifact."""
    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    seen: set[tuple[str, str, str]] = set()
    items: list[dict[str, Any]] = []
    for payload in payloads:
        for ev in calendar_items_to_events(payload):
            key_date = ev.event_date or ev.window_start or date(1970, 1, 1)
            key = (ev.underlying, ev.event_type, key_date.isoformat())
            if key in seen:
                continue
            seen.add(key)
            item: dict[str, Any] = {
                "underlying": ev.underlying,
                "event_type": ev.event_type,
                "source": ev.source,
            }
            if ev.event_date:
                item["event_date"] = ev.event_date.isoformat()
            if ev.window_start:
                item["window_start"] = ev.window_start.isoformat()
            if ev.window_end:
                item["window_end"] = ev.window_end.isoformat()
            if ev.implied_move_pct is not None:
                item["implied_move_pct_today"] = round(ev.implied_move_pct, 6)
            if ev.historical_move_pct_mad is not None:
                item["historical_move_pct_mad"] = round(ev.historical_move_pct_mad, 6)
            if ev.event_skew_sign:
                item["event_skew_sign"] = ev.event_skew_sign
            if ev.nearest_known_event:
                item["nearest_known_event"] = ev.nearest_known_event
            items.append(item)
    items.sort(key=lambda x: (x.get("underlying", ""), x.get("event_date") or x.get("window_start") or ""))
    build_times = [p.get("build_time") for p in payloads if p and p.get("build_time")]
    return {
        "build_time": now,
        "source_build_times": build_times,
        "item_count": len(items),
        "items": items,
    }


def events_for_underlying_in_window(
    calendar: dict[str, Any] | list[CalendarEvent] | None,
    underlying: str,
    start: date,
    end: date,
) -> list[CalendarEvent]:
    if calendar is None:
        return []
    events = calendar if isinstance(calendar, list) else calendar_items_to_events(calendar)
    und = str(underlying or "").upper()
    return [
        ev for ev in events
        if ev.underlying == und and ev.dates_in_range(start, end)
    ]


def _norm_iv(raw: object) -> float | None:
    v = _parse_float(raw)
    if v is None or v <= 0:
        return None
    if v > 5:
        v = v / 100.0
    return v if v <= 5 else None


def _option_chain(options_cache: dict, symbol: str) -> list[dict]:
    entry = (options_cache.get("symbols") or {}).get(str(symbol).upper()) or {}
    return [c for c in (entry.get("options") or []) if isinstance(c, dict)]


def _spot(options_cache: dict, symbol: str) -> float | None:
    entry = (options_cache.get("symbols") or {}).get(str(symbol).upper()) or {}
    return _parse_float(entry.get("spot"))


def lookup_contract_quote(
    options_cache: dict,
    symbol: str,
    expiry: date,
    strike: float,
    put_call: str = "P",
) -> dict[str, Any]:
    """Nearest contract IV/mid (mirrors yieldboost_holdings.lookup_contract_iv)."""
    target_type = "put" if put_call.upper() == "P" else "call"
    expiry_s = expiry.isoformat()
    best = None
    best_dist = None
    for c in _option_chain(options_cache, symbol):
        if str(c.get("expiration_date")) != expiry_s:
            continue
        if str(c.get("contract_type", "")).lower() != target_type:
            continue
        cstrike = _parse_float(c.get("strike_price"))
        if cstrike is None:
            continue
        dist = abs(cstrike - float(strike))
        if best is None or dist < best_dist:
            best = c
            best_dist = dist
    if best is None:
        return {"iv": None, "mid": None, "matched": False}
    return {
        "iv": _norm_iv(best.get("iv")),
        "mid": _parse_float(best.get("mid")),
        "matched": best_dist is not None and best_dist < 0.05,
        "strike_used": _parse_float(best.get("strike_price")),
    }


def atm_straddle_mid(
    options_cache: dict,
    symbol: str,
    expiry: date,
) -> dict[str, Any] | None:
    """ATM straddle mid price and IV from options cache."""
    spot = _spot(options_cache, symbol)
    if spot is None or spot <= 0:
        return None
    expiry_s = expiry.isoformat()
    puts = [
        c for c in _option_chain(options_cache, symbol)
        if str(c.get("expiration_date")) == expiry_s
        and str(c.get("contract_type", "")).lower() == "put"
    ]
    calls = [
        c for c in _option_chain(options_cache, symbol)
        if str(c.get("expiration_date")) == expiry_s
        and str(c.get("contract_type", "")).lower() == "call"
    ]
    if not puts or not calls:
        return None

    def _nearest(contracts: list[dict]) -> dict | None:
        best = None
        best_dist = None
        for c in contracts:
            k = _parse_float(c.get("strike_price"))
            if k is None:
                continue
            dist = abs(k - spot)
            if best is None or dist < best_dist:
                best = c
                best_dist = dist
        return best

    p = _nearest(puts)
    c = _nearest(calls)
    if p is None or c is None:
        return None
    pm = _parse_float(p.get("mid"))
    cm = _parse_float(c.get("mid"))
    if pm is None or cm is None:
        return None
    iv_p = _norm_iv(p.get("iv"))
    iv_c = _norm_iv(c.get("iv"))
    iv_atm = None
    if iv_p is not None and iv_c is not None:
        iv_atm = (iv_p + iv_c) / 2.0
    elif iv_p is not None:
        iv_atm = iv_p
    elif iv_c is not None:
        iv_atm = iv_c
    return {
        "expiry": expiry_s,
        "spot": spot,
        "straddle_mid": pm + cm,
        "straddle_pct": (pm + cm) / spot,
        "iv_atm": iv_atm,
        "strike_put": _parse_float(p.get("strike_price")),
        "strike_call": _parse_float(c.get("strike_price")),
    }


def listed_expiries(options_cache: dict, symbol: str, *, min_days: int = 1) -> list[date]:
    today = date.today()
    seen: set[date] = set()
    for c in _option_chain(options_cache, symbol):
        exp = _parse_date(c.get("expiration_date"))
        if exp is None or exp < today + timedelta(days=min_days - 1):
            continue
        seen.add(exp)
    return sorted(seen)


def forward_variance(
    t1_years: float,
    sigma1: float,
    t2_years: float,
    sigma2: float,
) -> float | None:
    """Annualized forward variance over [T1, T2] from two total variances."""
    if t2_years <= t1_years or t1_years < 0 or sigma1 is None or sigma2 is None:
        return None
    if sigma1 <= 0 or sigma2 <= 0:
        return None
    num = sigma2 * sigma2 * t2_years - sigma1 * sigma1 * t1_years
    den = t2_years - t1_years
    if den <= 0 or num <= 0:
        return None
    return num / den


def strip_iv_to_base(
    iv_full: float,
    horizon_years: float,
    event_var_underlying: float,
    *,
    leverage_var_mult: float = LEVERAGE_VAR_MULT,
) -> float | None:
    """Remove event variance (scaled to 2x sleeve) from full IV."""
    if iv_full is None or iv_full <= 0 or horizon_years <= 0:
        return None
    event_var_sleeve = leverage_var_mult * max(0.0, event_var_underlying)
    total_var = iv_full * iv_full * horizon_years
    base_var = max(0.0, total_var - event_var_sleeve)
    return math.sqrt(base_var / horizon_years)


def estimate_event_var_for_window(
    underlying: str,
    events: list[CalendarEvent],
    options_cache: dict | None,
    *,
    as_of: date | None = None,
    historical_moves: dict[str, list[float]] | None = None,
    peer_median_move: float | None = None,
) -> dict[str, Any]:
    """Estimate total underlying event variance inside the window."""
    if not events:
        return {
            "event_var_underlying": 0.0,
            "event_var_source": "none",
            "event_implied_move_pct": None,
            "event_count": 0,
            "event_types": [],
        }

    today = as_of or date.today()
    event_types = sorted({ev.event_type for ev in events})
    total_var = 0.0
    moves: list[float] = []
    source = "none"

    for ev in events:
        if ev.implied_move_pct is not None and ev.implied_move_pct > 0:
            move = ev.implied_move_pct
            total_var += move * move
            moves.append(move)
            source = "straddle_calendar" if ev.source.startswith("forward") else ev.source
            continue

        hist = (historical_moves or {}).get(underlying.upper(), [])
        if hist:
            mad = statistics.median([abs(x - statistics.median(hist)) for x in hist])
            move = mad / MAD_SCALE if mad > 0 else statistics.median([abs(x) for x in hist])
            total_var += move * move
            moves.append(move)
            source = "historical_8q_mad"
            continue

        if ev.historical_move_pct_mad is not None:
            move = ev.historical_move_pct_mad
            total_var += move * move
            moves.append(move)
            source = "historical_8q_mad"
            continue

        if peer_median_move is not None and peer_median_move > 0:
            total_var += peer_median_move * peer_median_move
            moves.append(peer_median_move)
            source = "peer_median"

    if total_var <= 0 and events:
        earnings = [ev for ev in events if ev.event_type == "earnings"]
        if earnings:
            default_move = 0.05
            total_var = default_move * default_move * len(earnings)
            source = "earnings_default_5pct"

    implied_move = math.sqrt(total_var) if total_var > 0 else None
    if moves and source == "none":
        source = "historical_8q_mad"

    return {
        "event_var_underlying": total_var,
        "event_var_source": source,
        "event_implied_move_pct": round(implied_move, 6) if implied_move else None,
        "event_count": len(events),
        "event_types": event_types,
    }


def scan_mystery_events(
    underlying: str,
    options_cache: dict,
    *,
    known_calendar: dict[str, Any] | None = None,
    base_sigma: float | None = None,
    n_expiries: int = 4,
    mad_threshold: float = 2.0,
) -> list[dict[str, Any]]:
    """Detect forward-vol bumps not explained by known calendar events."""
    und = str(underlying or "").upper()
    expiries = listed_expiries(options_cache, und)[:n_expiries]
    if len(expiries) < 2:
        return []

    straddles: list[dict[str, Any]] = []
    for exp in expiries:
        s = atm_straddle_mid(options_cache, und, exp)
        if s and s.get("iv_atm") is not None:
            straddles.append({**s, "expiry_date": exp})

    if len(straddles) < 2:
        return []

    today = date.today()
    fwd_vars: list[tuple[date, date, float]] = []
    for i in range(len(straddles) - 1):
        s1, s2 = straddles[i], straddles[i + 1]
        t1 = max((s1["expiry_date"] - today).days / TRADING_DAYS, 1 / TRADING_DAYS)
        t2 = max((s2["expiry_date"] - today).days / TRADING_DAYS, t1 + 1 / TRADING_DAYS)
        fv = forward_variance(t1, s1["iv_atm"], t2, s2["iv_atm"])
        if fv is not None:
            fwd_vars.append((s1["expiry_date"], s2["expiry_date"], fv))

    if not fwd_vars:
        return []

    fwd_vals = [x[2] for x in fwd_vars]
    base = base_sigma ** 2 if base_sigma and base_sigma > 0 else statistics.median(fwd_vals)
    med = statistics.median(fwd_vals)
    mad = statistics.median([abs(v - med) for v in fwd_vals]) / MAD_SCALE if len(fwd_vals) > 1 else 0.01
    threshold = max(mad_threshold * mad, 0.0001)

    known = events_for_underlying_in_window(known_calendar, und, today, expiries[-1])
    known_windows = {
        (ev.window_start or ev.event_date, ev.window_end or ev.event_date)
        for ev in known
        if ev.event_date or ev.window_start
    }

    out: list[dict[str, Any]] = []
    for w_start, w_end, fv in fwd_vars:
        excess = fv - base
        dt_years = max((w_end - w_start).days / TRADING_DAYS, 1 / TRADING_DAYS)
        implied_var = excess * dt_years
        if implied_var < threshold:
            continue
        has_known = any(
            kw[0] is not None and kw[1] is not None
            and not (w_end < kw[0] or w_start > kw[1])
            for kw in known_windows
        )
        if has_known:
            continue
        move = math.sqrt(max(implied_var, 0.0))
        out.append({
            "underlying": und,
            "window_start": w_start.isoformat(),
            "window_end": w_end.isoformat(),
            "implied_event_var": round(implied_var, 6),
            "implied_event_move_pct": round(move, 6),
            "event_type": "mystery",
            "source": "forward_straddle_excess",
            "event_skew_sign": "symmetric",
            "nearest_known_event": None,
        })
    return out


def deevent_rv_from_log_returns(
    log_returns: list[float],
    *,
    event_day_indices: set[int] | None = None,
) -> dict[str, Any]:
    """Compute full vs base annualized RV; shrink event days toward local median."""
    if len(log_returns) < 2:
        return {"vol_annual_full": None, "vol_annual_base": None, "event_days_excluded": 0}

    arr = [float(r) for r in log_returns if math.isfinite(r)]
    if len(arr) < 2:
        return {"vol_annual_full": None, "vol_annual_base": None, "event_days_excluded": 0}

    vol_full = statistics.stdev(arr) * math.sqrt(TRADING_DAYS)
    indices = event_day_indices or set()
    if not indices:
        return {
            "vol_annual_full": round(vol_full, 6),
            "vol_annual_base": round(vol_full, 6),
            "event_days_excluded": 0,
        }

    cleaned: list[float] = []
    excluded = 0
    for i, r in enumerate(arr):
        if i in indices:
            lo = max(0, i - 2)
            hi = min(len(arr), i + 3)
            neighbors = [arr[j] for j in range(lo, hi) if j != i]
            cleaned.append(statistics.median(neighbors) if neighbors else 0.0)
            excluded += 1
        else:
            cleaned.append(r)

    vol_base = statistics.stdev(cleaned) * math.sqrt(TRADING_DAYS) if len(cleaned) >= 2 else vol_full
    return {
        "vol_annual_full": round(vol_full, 6),
        "vol_annual_base": round(vol_base, 6),
        "event_days_excluded": excluded,
    }


def bs_put_price(
    spot: float,
    strike: float,
    t_years: float,
    sigma: float,
    *,
    risk_free: float = 0.043,
) -> float:
    if t_years <= 0:
        return max(strike - spot, 0.0)
    if sigma <= 0:
        forward = spot * math.exp(risk_free * t_years)
        return max(strike * math.exp(-risk_free * t_years) - forward, 0.0)
    vol_sqrt_t = sigma * math.sqrt(t_years)
    d1 = (math.log(spot / strike) + (risk_free + 0.5 * sigma * sigma) * t_years) / vol_sqrt_t
    d2 = d1 - vol_sqrt_t
    return strike * math.exp(-risk_free * t_years) * (0.5 * (1 + math.erf(-d2 / math.sqrt(2)))) - spot * (0.5 * (1 + math.erf(-d1 / math.sqrt(2))))


def fair_put_spread_mid_from_iv(
    spot: float,
    strike_long: float,
    strike_short: float,
    iv_base: float,
    horizon_years: float,
    *,
    risk_free: float = 0.043,
) -> float | None:
    """Fair market put-spread mid (short - long) from base IV."""
    if spot <= 0 or iv_base <= 0 or horizon_years <= 0:
        return None
    if strike_long >= strike_short:
        return None
    long_px = bs_put_price(spot, strike_long, horizon_years, iv_base, risk_free=risk_free)
    short_px = bs_put_price(spot, strike_short, horizon_years, iv_base, risk_free=risk_free)
    spread = short_px - long_px
    return float(spread) if math.isfinite(spread) else None


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs_put_greeks(
    spot: float,
    strike: float,
    t_years: float,
    sigma: float,
    *,
    risk_free: float = 0.043,
) -> dict[str, float]:
    """BS greeks for a single LONG put (delta, gamma, vega, theta_per_year).

    - ``vega`` is per 1.00 change in sigma (so 0.01 = 1 vol point change).
    - ``theta_per_year`` is per year; divide by ``TRADING_DAYS`` for per-day.
    """
    if spot <= 0 or strike <= 0 or sigma <= 0 or t_years <= 0:
        return {"delta": 0.0, "gamma": 0.0, "vega": 0.0, "theta_per_year": 0.0}
    sqrt_t = math.sqrt(t_years)
    vol_sqrt_t = sigma * sqrt_t
    d1 = (math.log(spot / strike) + (risk_free + 0.5 * sigma * sigma) * t_years) / vol_sqrt_t
    d2 = d1 - vol_sqrt_t
    pdf_d1 = _norm_pdf(d1)
    delta = -_norm_cdf(-d1)
    gamma = pdf_d1 / (spot * vol_sqrt_t)
    vega = spot * pdf_d1 * sqrt_t
    theta = (-spot * pdf_d1 * sigma) / (2.0 * sqrt_t) + risk_free * strike * math.exp(-risk_free * t_years) * _norm_cdf(-d2)
    return {
        "delta": float(delta),
        "gamma": float(gamma),
        "vega": float(vega),
        "theta_per_year": float(theta),
    }


def bs_put_spread_greeks(
    spot: float,
    strike_long: float,
    strike_short: float,
    t_years: float,
    sigma: float,
    *,
    risk_free: float = 0.043,
) -> dict[str, float]:
    """Greeks of a SHORT put spread (short K_short, long K_long).

    Net position: ``-put(K_short) + put(K_long)``. We collected the spread mid
    as credit; greeks reflect P&L sensitivity *for the seller*. Positive theta
    means we earn from time decay; negative gamma means we are short convexity.
    """
    short_leg = bs_put_greeks(spot, strike_short, t_years, sigma, risk_free=risk_free)
    long_leg = bs_put_greeks(spot, strike_long, t_years, sigma, risk_free=risk_free)
    return {
        "delta": -short_leg["delta"] + long_leg["delta"],
        "gamma": -short_leg["gamma"] + long_leg["gamma"],
        "vega": -short_leg["vega"] + long_leg["vega"],
        "theta_per_year": -short_leg["theta_per_year"] + long_leg["theta_per_year"],
    }


def implied_sigma_for_spread_mid(
    spot: float,
    strike_long: float,
    strike_short: float,
    horizon_years: float,
    target_mid: float,
    *,
    risk_free: float = 0.043,
    lo: float = 0.01,
    hi: float = 5.0,
    tol: float = 1e-5,
    max_iter: int = 80,
) -> float | None:
    """Bisection: find annualized sigma s.t. fair put-spread mid = ``target_mid``.

    Returns ``None`` when the target is outside the achievable range (e.g.,
    target > short_strike - long_strike for at-expiry intrinsic, or target < 0).
    """
    if spot <= 0 or horizon_years <= 0 or target_mid is None:
        return None
    if not (0 < target_mid):
        return None
    if strike_long >= strike_short:
        return None
    intrinsic_cap = max(0.0, strike_short - strike_long)
    if target_mid >= intrinsic_cap:
        # Spread mid can never exceed the strike-distance; flag as out-of-range.
        return None

    def f(sigma: float) -> float:
        fair = fair_put_spread_mid_from_iv(spot, strike_long, strike_short, sigma, horizon_years, risk_free=risk_free)
        if fair is None:
            return float("nan")
        return fair - target_mid

    f_lo = f(lo)
    f_hi = f(hi)
    if not (math.isfinite(f_lo) and math.isfinite(f_hi)):
        return None
    # Both sides same sign => target not bracketed.
    if f_lo * f_hi > 0:
        return None
    a, b = lo, hi
    for _ in range(max_iter):
        m = 0.5 * (a + b)
        fm = f(m)
        if not math.isfinite(fm):
            return None
        if abs(fm) < tol or (b - a) < tol:
            return float(m)
        if f_lo * fm <= 0:
            b = m
            f_hi = fm
        else:
            a = m
            f_lo = fm
    return float(0.5 * (a + b))


# ── Weekly-cycle put-spread loss helpers ────────────────────────────────────
# Magis closed-form expected weekly loss for a YB-style short put spread on the
# 2x sleeve. Per ``Bucket2_Income_ETF_Decay_Research.html``:
#
#   ln R_sleeve_T  ~  N((2μ_und − 2σ_und²) τ , (2σ_und √τ)² )
#
# With μ_und = 0 (week is roughly drift-free at this horizon) and τ = 5/252,
# the expected put-spread *loss* L collected at expiry equals the BS fair value
# of the spread under risk-neutral discounting set to 1.0 — i.e. the same
# function the dashboard already uses to score ``put_spread_fair_event_aware``
# when applied with the SLEEVE-level sigma (``2 σ_underlying``) at horizon τ.
WEEKLY_TAU_YEARS = 5.0 / TRADING_DAYS


def compute_vrp_row_extras(
    *,
    spot: float | None,
    strike_long: float | None,
    strike_short: float | None,
    horizon_years: float | None,
    iv_full_sleeve: float | None,
    iv_base_sleeve: float | None,
    rv_2x_full: float | None,
    rv_2x_base: float | None,
    spread_mid: float | None,
    risk_free: float = 0.043,
) -> dict[str, Any]:
    """Compute the §4.1 'shippable today' VRP metrics for a single front spread.

    Every output is keyed so it can be ``dict.update``'d onto a vrp_live row.
    Missing inputs propagate as ``None`` -- callers should not crash on absent
    fields. Greeks are quoted *per single spread structure* (1 short put + 1
    long put), so they can be multiplied by sizing on the consumer side.
    """
    out: dict[str, Any] = {}

    iv_for_bs = (
        _parse_float(iv_full_sleeve)
        or _parse_float(iv_base_sleeve)
        or _parse_float(rv_2x_full)
        or _parse_float(rv_2x_base)
    )
    iv_source = None
    if _parse_float(iv_full_sleeve):
        iv_source = "iv_full_sleeve"
    elif _parse_float(iv_base_sleeve):
        iv_source = "iv_base_sleeve"
    elif _parse_float(rv_2x_full):
        iv_source = "rv_2x_full"
    elif _parse_float(rv_2x_base):
        iv_source = "rv_2x_base"

    # 1. β=2 inversion: sleeve sigma -> underlying sigma. (Diffusive component
    #    only; the 4x event variance is already stripped elsewhere if present.)
    if _parse_float(iv_full_sleeve):
        out["iv_underlying_implied"] = round(iv_full_sleeve / LEVERAGE_BETA, 6)
    elif _parse_float(iv_base_sleeve):
        out["iv_underlying_implied"] = round(iv_base_sleeve / LEVERAGE_BETA, 6)
    else:
        out["iv_underlying_implied"] = None

    # 2 & 3. Implied weekly 1σ move (sleeve and underlying scale).
    tau = WEEKLY_TAU_YEARS
    if iv_for_bs:
        out["iv_implied_weekly_move_2x_pct"] = round(iv_for_bs * math.sqrt(tau) * 100.0, 4)
        out["iv_implied_weekly_move_underlying_pct"] = round(
            (iv_for_bs / LEVERAGE_BETA) * math.sqrt(tau) * 100.0, 4,
        )
    else:
        out["iv_implied_weekly_move_2x_pct"] = None
        out["iv_implied_weekly_move_underlying_pct"] = None

    # 4. Fair-value / expected loss at the chosen σ (IV preferred, RV fallback).
    s = _parse_float(spot)
    kl = _parse_float(strike_long)
    ks = _parse_float(strike_short)
    h = _parse_float(horizon_years) or tau
    h = max(h, 1.0 / TRADING_DAYS)
    fair_diff = None
    if s and kl and ks and iv_for_bs:
        fair_diff = fair_put_spread_mid_from_iv(s, kl, ks, iv_for_bs, h, risk_free=risk_free)
    out["bs_put_spread_fair_diffusion"] = round(fair_diff, 6) if fair_diff is not None else None
    out["bs_put_spread_sigma_source"] = iv_source if fair_diff is not None else None
    if fair_diff is not None and s and s > 0:
        out["expected_weekly_loss_pct_of_spot"] = round(fair_diff / s * 100.0, 4)
    else:
        out["expected_weekly_loss_pct_of_spot"] = None

    # 5. Breakeven sigma from spread mid (the σ at which IV pricing = realized).
    sm = _parse_float(spread_mid)
    be_sigma = None
    if s and kl and ks and sm and sm > 0:
        be_sigma = implied_sigma_for_spread_mid(s, kl, ks, h, sm, risk_free=risk_free)
    out["spread_breakeven_sigma_annual"] = round(be_sigma, 6) if be_sigma is not None else None
    # Net edge "in vol terms": iv_full minus breakeven sigma. Positive means
    # the market is paying more than the structure's expected loss at that σ.
    if be_sigma is not None and _parse_float(iv_full_sleeve):
        out["iv_minus_breakeven_sigma"] = round(iv_full_sleeve - be_sigma, 6)
    else:
        out["iv_minus_breakeven_sigma"] = None

    # 6 & 7. BS greeks of the SHORT put spread (per single structure).
    if s and kl and ks and iv_for_bs:
        gks = bs_put_spread_greeks(s, kl, ks, h, iv_for_bs, risk_free=risk_free)
        out["bs_delta_spread"] = round(gks["delta"], 6)
        out["bs_gamma_spread"] = round(gks["gamma"], 8)
        out["bs_vega_spread"] = round(gks["vega"], 6)
        out["bs_theta_spread_per_day"] = round(gks["theta_per_year"] / TRADING_DAYS, 6)
        # Dollar gamma per 1% sleeve move = γ · S² · (0.01)². Express as $/spread.
        out["bs_dollar_gamma_per_1pct_sleeve"] = round(gks["gamma"] * s * s * 0.0001, 6)
        # Translate sleeve gamma -> underlying 1% move: 2x sleeve moves 2% per
        # 1% underlying => $-gamma at 1% underlying = γ · (2S)² · (0.01)² · 4
        # but here we already use sleeve S, so the underlying-equivalent is x4.
        out["bs_dollar_gamma_per_1pct_underlying"] = round(
            gks["gamma"] * s * s * 0.0001 * (LEVERAGE_BETA * LEVERAGE_BETA), 6,
        )
    else:
        out["bs_delta_spread"] = None
        out["bs_gamma_spread"] = None
        out["bs_vega_spread"] = None
        out["bs_theta_spread_per_day"] = None
        out["bs_dollar_gamma_per_1pct_sleeve"] = None
        out["bs_dollar_gamma_per_1pct_underlying"] = None

    # 8. Simple Itô variance drag for 2x daily-rebalanced sleeve (per year).
    rv_und_proxy = None
    iv_u = out.get("iv_underlying_implied")
    if iv_u is not None and iv_u > 0:
        rv_und_proxy = iv_u
    elif _parse_float(rv_2x_full):
        rv_und_proxy = rv_2x_full / LEVERAGE_BETA
    elif _parse_float(rv_2x_base):
        rv_und_proxy = rv_2x_base / LEVERAGE_BETA
    if rv_und_proxy is not None:
        # x=2 daily-rebalanced exponent: A_T / A_0 = (S_T/S_0)^2 · exp(-σ² T)
        # so the *gross* drag is σ_underlying² per year for x=2.
        out["sleeve_diffusion_drag_annual"] = round(rv_und_proxy * rv_und_proxy, 6)
        # Inverse-LETF analog (x = -2) for the Bucket-4 short-pair view: 3σ².
        out["inverse_sleeve_drag_annual_x_minus2"] = round(3.0 * rv_und_proxy * rv_und_proxy, 6)
    else:
        out["sleeve_diffusion_drag_annual"] = None
        out["inverse_sleeve_drag_annual_x_minus2"] = None

    return out


def calendar_is_stale(payload: dict[str, Any] | None, *, max_age_hours: float = 24.0) -> bool:
    """Stale if missing, age > max_age_hours, OR empty (item_count == 0).

    The empty-but-fresh case happens when ``ingest_event_calendar.build_known_calendar``
    runs while Nasdaq/Yahoo are blocking the cloud-IP: it writes a payload with a
    fresh ``build_time`` but ``item_count: 0``. Treating that as fresh causes
    ``refresh_event_pipeline`` to skip the rebuild forever, blocking the seed /
    quarterly-projection fallback we explicitly added for that scenario.
    """
    if not payload or not payload.get("build_time"):
        return True
    try:
        bt = datetime.fromisoformat(str(payload["build_time"]).replace("Z", "+00:00"))
        age_h = (datetime.now(timezone.utc) - bt).total_seconds() / 3600.0
        if age_h > max_age_hours:
            return True
    except Exception:
        return True
    # Semantic staleness: even a "fresh" payload is stale if it ingested zero
    # items. Items >= 1 OR items list present-and-empty + an explicit non-zero
    # ``source_stats`` block (e.g., ``missing: N``) is taken as "build attempted,
    # genuinely no events upcoming" -- don't loop on those.
    items = payload.get("items")
    if isinstance(items, list) and len(items) == 0:
        stats = payload.get("source_stats") or {}
        if not stats:
            return True
        # If we recorded any successful source for at least one underlying we
        # trust the empty result; otherwise it's an outright failure and we
        # should retry on the next pipeline run.
        ok = (stats.get("nasdaq") or 0) + (stats.get("yahoo") or 0) + (stats.get("seed") or 0) + (stats.get("projected") or 0)
        if ok == 0:
            return True
    return False


def enrich_vrp_row_with_events(
    row: dict[str, Any],
    *,
    calendar: dict[str, Any] | None,
    options_cache: dict | None = None,
    rv_map_base: dict[str, float] | None = None,
    historical_moves: dict[str, list[float]] | None = None,
    as_of: date | None = None,
) -> dict[str, Any]:
    """Add event-decomposed IV/RV/VRP fields to a vrp_live row."""
    today = as_of or date.today()
    und = str(row.get("underlying") or "").upper()
    expiry = _parse_date(row.get("expiry"))
    if expiry is None:
        return row

    horizon_years = max((expiry - today).days / TRADING_DAYS, 1 / TRADING_DAYS)
    events = events_for_underlying_in_window(calendar, und, today, expiry) if und and calendar else []

    ev_est = estimate_event_var_for_window(
        und, events, options_cache,
        as_of=today,
        historical_moves=historical_moves,
    )

    iv_long = _parse_float(row.get("iv_put_long"))
    iv_short = _parse_float(row.get("iv_put_short"))
    iv_vals = [v for v in (iv_long, iv_short) if v is not None]
    iv_full = sum(iv_vals) / len(iv_vals) if iv_vals else None

    iv_base = None
    if iv_full is not None:
        iv_base = strip_iv_to_base(iv_full, horizon_years, ev_est["event_var_underlying"])

    sleeve = str(row.get("sleeve_2x") or "").upper()
    rv_full = _parse_float(row.get("rv_30d_2x"))
    rv_base = (rv_map_base or {}).get(sleeve) if rv_map_base else rv_full

    vrp_full = row.get("vrp_vol_2x")
    vrp_base = None
    if iv_base is not None and rv_base is not None:
        vrp_base = iv_base - rv_base

    event_var_2x_ann = None
    if ev_est["event_var_underlying"] > 0 and horizon_years > 0:
        event_var_2x_ann = math.sqrt(LEVERAGE_VAR_MULT * ev_est["event_var_underlying"] / horizon_years)

    out = dict(row)
    spread_mid = _parse_float(row.get("spread_mid_market"))
    fair_spread = None
    put_spread_vrp = None
    spot = _parse_float(row.get("spot_2x"))
    k_long = _parse_float(row.get("strike_long"))
    k_short = _parse_float(row.get("strike_short"))
    if iv_base is not None and iv_base > 0 and spot and k_long and k_short:
        fair_spread = fair_put_spread_mid_from_iv(
            spot, k_long, k_short, iv_base, horizon_years,
        )
        if fair_spread is not None and spread_mid is not None:
            put_spread_vrp = spread_mid - fair_spread

    out.update({
        "iv_put_long_full": iv_long,
        "iv_put_short_full": iv_short,
        "iv_put_long_base": strip_iv_to_base(iv_long, horizon_years, ev_est["event_var_underlying"]) if iv_long else None,
        "iv_put_short_base": strip_iv_to_base(iv_short, horizon_years, ev_est["event_var_underlying"]) if iv_short else None,
        "iv_full_proxy": iv_full,
        "iv_base_proxy": iv_base,
        "rv_30d_2x_full": rv_full,
        "rv_30d_2x_base": rv_base,
        "vrp_vol_2x_full": vrp_full,
        "vrp_vol_2x_base": round(vrp_base, 6) if vrp_base is not None else None,
        "event_implied_move_pct": ev_est.get("event_implied_move_pct"),
        "event_var_2x_annualized": round(event_var_2x_ann, 6) if event_var_2x_ann else None,
        "event_count_in_window": ev_est.get("event_count", 0),
        "event_types_in_window": ev_est.get("event_types", []),
        "event_var_source": ev_est.get("event_var_source", "none"),
        "event_calendar_stale": calendar_is_stale(calendar),
        "put_spread_fair_event_aware": round(fair_spread, 6) if fair_spread is not None else None,
        "put_spread_vrp_event_aware": round(put_spread_vrp, 6) if put_spread_vrp is not None else None,
    })
    return out


# Rotating User-Agent strings: GitHub Actions cloud IPs are routinely rate-limited
# (or 403'd) by Nasdaq/Yahoo when sending a thin ``Mozilla/5.0`` header. Cycling
# through plausible browser UAs and adding a normal ``Accept``/``Accept-Language``
# header pair dramatically improves the success rate without any auth setup.
_BROWSER_USER_AGENTS: tuple[str, ...] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
)


def _browser_headers(*, accept: str = "application/json,text/plain,*/*", referer: str | None = None) -> dict[str, str]:
    """Stable-but-rotated header set for public-API GETs."""
    idx = abs(hash(datetime.now(timezone.utc).isoformat()[:10])) % len(_BROWSER_USER_AGENTS)
    headers = {
        "User-Agent": _BROWSER_USER_AGENTS[idx],
        "Accept": accept,
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    if referer:
        headers["Referer"] = referer
    return headers


def fetch_yahoo_earnings_dates(symbol: str, *, timeout: int = 15) -> list[date]:
    """Upcoming/recent earnings dates from Yahoo chart meta (best-effort)."""
    sym = str(symbol).upper().strip()
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
        f"?interval=1d&range=2y&events=earnings"
    )
    req = urllib.request.Request(url, headers=_browser_headers(
        accept="application/json,text/plain,*/*",
        referer=f"https://finance.yahoo.com/quote/{sym}",
    ))
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read())
    except Exception:
        return []

    dates: list[date] = []
    try:
        events = data["chart"]["result"][0].get("events") or {}
        earnings = events.get("earnings") or {}
        for ts in earnings:
            d = datetime.fromtimestamp(int(ts), tz=timezone.utc).date()
            dates.append(d)
    except (KeyError, IndexError, TypeError, ValueError):
        pass
    return sorted(set(dates))


def fetch_nasdaq_earnings_by_date(day: date, *, timeout: int = 20) -> dict[str, date]:
    """Symbol -> earnings date for one calendar day via Nasdaq public API."""
    url = f"https://api.nasdaq.com/api/calendar/earnings?date={day.isoformat()}"
    req = urllib.request.Request(
        url,
        headers=_browser_headers(
            accept="application/json,text/plain,*/*",
            referer="https://www.nasdaq.com/market-activity/earnings",
        ),
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read())
    except Exception:
        return {}

    out: dict[str, date] = {}
    for row in (payload.get("data") or {}).get("rows") or []:
        if not isinstance(row, dict):
            continue
        sym = str(row.get("symbol") or "").upper().strip()
        if sym:
            out[sym] = day
    return out


def fetch_nasdaq_earnings_window(
    symbols: Iterable[str],
    *,
    start: date | None = None,
    days: int = 21,
    sleep_sec: float = 0.12,
) -> dict[str, list[date]]:
    """Upcoming earnings dates for requested symbols over a date window."""
    import time

    want = {str(s).upper() for s in symbols if str(s).strip()}
    found: dict[str, list[date]] = {s: [] for s in want}
    today = start or date.today()
    for offset in range(days):
        day = today + timedelta(days=offset)
        by_sym = fetch_nasdaq_earnings_by_date(day)
        for sym, d in by_sym.items():
            if sym in want:
                found[sym].append(d)
        time.sleep(sleep_sec)
    return {k: sorted(set(v)) for k, v in found.items() if v}


# Calendar quarter heuristic: most US large-caps report on a ~90-day cadence
# (~63 trading days). When upstream APIs return nothing we project the next
# expected earnings date forward from the most recent historical earnings date
# (if known) and flag the entry as ``confirmation: projected``.
EARNINGS_CADENCE_DAYS = 91


def project_next_earnings_date(
    last_known: date,
    *,
    today: date | None = None,
    cadence_days: int = EARNINGS_CADENCE_DAYS,
) -> date | None:
    """Project the next earnings date by stepping cadence forward until it's >= today."""
    if not last_known:
        return None
    cur = today or date.today()
    nxt = last_known
    safety = 0
    while nxt < cur and safety < 12:
        nxt = nxt + timedelta(days=cadence_days)
        safety += 1
    return nxt if nxt >= cur else None


def load_earnings_seed(path: Path | str) -> dict[str, list[dict[str, Any]]]:
    """Load a per-underlying seed of upcoming earnings (used when live feeds fail).

    Schema (JSON):
      {"updated_at": "2026-05-26", "items": [
          {"underlying": "MSTR", "event_date": "2026-07-30",
           "confirmation": "projected", "source": "seed_quarterly",
           "historical_move_pct_mad": 0.063},
          ...]}
    """
    p = Path(path)
    if not p.exists():
        return {}
    try:
        payload = json.loads(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}
    out: dict[str, list[dict[str, Any]]] = {}
    for row in payload.get("items") or []:
        if not isinstance(row, dict):
            continue
        und = str(row.get("underlying") or "").upper().strip()
        if not und:
            continue
        out.setdefault(und, []).append(row)
    return out

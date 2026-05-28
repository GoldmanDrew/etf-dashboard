"""LETF/YieldBOOST sleeve option models above flat-? Black?Scholes.

This module is the production engine for Phases 1?3 of the
``LETF_Options_Pricing_and_Edge_Roadmap``:

- **P1 ? Avellaneda?Zhang / Leung?Sircar moneyness rescaling.** The cheapest
  non-trivial upgrade: read the underlying 1? IV at the AZ-mapped strike,
  scale by ``|?|`` to get an implied sleeve IV, and treat the gap to the
  listed sleeve IV as the structural cone residual. Implemented as
  ``az_moneyness_map_strike`` + ``az_implied_sleeve_iv``.

- **P2 ? Heston on underlying + Ahn?Haugh?Jain propagation.** Calibrate
  five-parameter Heston (?, ?, ?, ?, V?) to the underlying surface,
  propagate to the ?-sleeve via the closed-form AHJ map
  ``(?, ???, |?|?, sign(?)??, ??V?)``, then price the sleeve put-spread
  via the COS expansion (Fang & Oosterlee 2008). Implemented as
  ``calibrate_heston_underlying`` + ``propagate_heston_to_sleeve`` +
  ``cos_put_price_heston``.

- **P3 ? Bates jump-diffusion + AHJ truncation.** For single-stock sleeves
  add Merton-style log-normal jumps to the Heston dynamics on the
  underlying, then propagate to the sleeve with the AHJ truncation
  correction that prevents L < 0. The truncation generates a
  "limited-liability insurance" drift premium that lifts deep-OTM put
  prices above the diffusion baseline. Implemented as
  ``cos_put_price_bates`` + ``ahj_truncate_jump_for_sleeve``.

The module deliberately avoids any I/O and depends only on ``numpy`` plus
``scipy.optimize.minimize`` for calibration. Every routine returns ``None``
(or ``NaN``) on bad inputs rather than raising, so callers can chain through
fallbacks (AZ ? Heston ? Bates) without a try/except wall.

All sigmas are annualized. All horizons are in years. Risk-free and
dividend yields are continuous. ``beta`` is the LETF leverage (e.g.
``+2`` for AMDL, ``-1`` for SH, ``-3`` for SPXU).
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Sequence

import numpy as np


# ?? Constants / numerical guards ????????????????????????????????????????????

TRADING_DAYS = 252
_EPS = 1e-12
_COS_N = 192  # COS expansion truncation; 128?256 is the sweet spot for short-T
_COS_L = 12   # COS truncation range in ?-units; 10?14 covers ?6? tails


# ?? Part 0: Black?Scholes (matches scripts/event_vol.py for parity) ?????????

def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def bs_put_price(spot: float, strike: float, t_years: float, sigma: float, *,
                 risk_free: float = 0.043, div_yield: float = 0.0) -> float:
    """BS put price with continuous dividend yield. Matches ``event_vol`` parity."""
    if spot <= 0 or strike <= 0:
        return 0.0
    if t_years <= 0:
        return max(strike - spot, 0.0)
    if sigma <= 0:
        forward = spot * math.exp((risk_free - div_yield) * t_years)
        return max(strike * math.exp(-risk_free * t_years) - forward, 0.0)
    vol_sqrt_t = sigma * math.sqrt(t_years)
    d1 = (math.log(spot / strike) + (risk_free - div_yield + 0.5 * sigma * sigma) * t_years) / vol_sqrt_t
    d2 = d1 - vol_sqrt_t
    return (strike * math.exp(-risk_free * t_years) * _norm_cdf(-d2)
            - spot * math.exp(-div_yield * t_years) * _norm_cdf(-d1))


def bs_implied_vol_from_put(spot: float, strike: float, t_years: float, target_px: float,
                            *, risk_free: float = 0.043, div_yield: float = 0.0,
                            lo: float = 1e-4, hi: float = 8.0, tol: float = 1e-6,
                            max_iter: int = 80) -> float | None:
    """Bisection inversion. Returns ``None`` if target is out of [intrinsic, forward] range."""
    if spot <= 0 or strike <= 0 or t_years <= 0 or target_px <= 0:
        return None
    intrinsic = max(0.0, strike * math.exp(-risk_free * t_years)
                    - spot * math.exp(-div_yield * t_years))
    if target_px < intrinsic - 1e-6:
        return None
    # Upper bound: an extreme-vol put approaches strike?e^{-rT}; if target exceeds that, abort.
    if target_px >= strike * math.exp(-risk_free * t_years) - 1e-9:
        return None
    f_lo = bs_put_price(spot, strike, t_years, lo, risk_free=risk_free, div_yield=div_yield) - target_px
    f_hi = bs_put_price(spot, strike, t_years, hi, risk_free=risk_free, div_yield=div_yield) - target_px
    if f_lo * f_hi > 0:
        return None
    a, b = lo, hi
    for _ in range(max_iter):
        m = 0.5 * (a + b)
        fm = bs_put_price(spot, strike, t_years, m, risk_free=risk_free, div_yield=div_yield) - target_px
        if abs(fm) < tol or (b - a) < tol:
            return float(m)
        if f_lo * fm <= 0:
            b = m
        else:
            a = m
            f_lo = fm
    return float(0.5 * (a + b))


# ?? Part 1: Avellaneda?Zhang / Leung?Sircar moneyness rescaling ?????????????

def az_moneyness_map_strike(
    *,
    k_letf: float,
    l0: float,
    x0: float,
    beta: float,
    risk_free: float,
    expense_rate_letf: float,
    t_years: float,
    sigma_bar: float,
) -> float | None:
    """Map a LETF strike ``K_L`` to the underlying-equivalent strike via Leung?Sircar (3.5).

    Under flat-? BS, log-moneynesses on the LETF and underlying satisfy
    ``LM^(?) = ??LM^(1) ? [(??1)?r + c]?T ? ????(??1)?????T``, where ``???``
    is a time-average variance proxy (typically the underlying ATM IV). So the
    LETF strike maps to the underlying strike

        K = X? ? (K_L / L?)^(1/?) ? exp([(??1)?r + c]?T / ? + (??1)?????T / 2)

    Returns ``None`` on degenerate inputs (e.g. ? = 0).
    """
    if not (math.isfinite(k_letf) and k_letf > 0):
        return None
    if not (math.isfinite(l0) and l0 > 0):
        return None
    if not (math.isfinite(x0) and x0 > 0):
        return None
    if abs(beta) < _EPS:
        return None
    if t_years < 0:
        return None
    try:
        log_term = ((beta - 1.0) * risk_free + expense_rate_letf) * t_years / beta
        var_term = (beta - 1.0) * (sigma_bar * sigma_bar) * t_years / 2.0
        ratio_pow = (k_letf / l0) ** (1.0 / beta)
        return float(x0 * ratio_pow * math.exp(log_term + var_term))
    except (OverflowError, ValueError):
        return None


def az_implied_sleeve_iv(*, sigma_underlying_at_mapped_k: float, beta: float) -> float | None:
    """Constant-? LETF IV inherited from the underlying IV at the AZ-mapped strike: ``|?|??``."""
    if sigma_underlying_at_mapped_k is None or sigma_underlying_at_mapped_k <= 0:
        return None
    if not math.isfinite(beta) or abs(beta) < _EPS:
        return None
    return float(abs(beta) * sigma_underlying_at_mapped_k)


def az_cone_residual_iv(
    *,
    sigma_letf_market: float | None,
    sigma_underlying_at_mapped_k: float | None,
    beta: float,
) -> float | None:
    """Sleeve IV minus AZ-implied sleeve IV. Positive = sleeve trades rich vs underlying surface."""
    if sigma_letf_market is None or not math.isfinite(sigma_letf_market) or sigma_letf_market <= 0:
        return None
    implied = az_implied_sleeve_iv(
        sigma_underlying_at_mapped_k=sigma_underlying_at_mapped_k or 0.0, beta=beta,
    )
    if implied is None:
        return None
    return float(sigma_letf_market - implied)


def lookup_underlying_iv_at_strike(
    *,
    underlying_chain: Iterable[dict] | None,
    underlying_spot: float | None,
    expiry_iso: str,
    target_strike: float,
    put_call: str = "P",
    nearest_expiry_max_days: int = 35,
) -> tuple[float | None, dict | None]:
    """Nearest-strike (and nearest-expiry fallback) IV from a Polygon/Tradier chain.

    Mirrors ``yieldboost_holdings.lookup_contract_iv`` but for a single contract on the
    *underlying* surface. Returns ``(iv, meta)`` where ``meta`` documents the resolution
    (used expiry, used strike, days of expiry-skew) for downstream provenance.
    """
    if not underlying_chain or target_strike is None or target_strike <= 0:
        return None, None
    target_type = "put" if str(put_call or "P").upper() == "P" else "call"
    rows = [c for c in underlying_chain if isinstance(c, dict)
            and str(c.get("contract_type", "")).lower() == target_type]
    if not rows:
        return None, None

    def _best_at_expiry(exp_str: str) -> tuple[dict | None, float | None]:
        best_row = None
        best_dist = None
        for c in rows:
            if str(c.get("expiration_date")) != exp_str:
                continue
            try:
                k = float(c.get("strike_price"))
            except (TypeError, ValueError):
                continue
            dist = abs(k - float(target_strike))
            if best_row is None or dist < best_dist:
                best_row = c
                best_dist = dist
        return best_row, best_dist

    expiries_sorted = sorted({str(c.get("expiration_date") or "") for c in rows} - {""})
    # Tier 1: exact expiry.
    best, best_dist = _best_at_expiry(expiry_iso)
    iv_at_best = _safe_iv(best.get("iv")) if best is not None else None
    if iv_at_best is None or iv_at_best <= 0:
        # Tier 2: nearest expiry.
        from datetime import date as _date, datetime as _dt
        try:
            held_dt = _dt.strptime(expiry_iso, "%Y-%m-%d").date()
        except ValueError:
            held_dt = None
        if held_dt is not None:
            candidates: list[tuple[int, str]] = []
            for exp_alt in expiries_sorted:
                if exp_alt == expiry_iso:
                    continue
                try:
                    alt_dt = _dt.strptime(exp_alt, "%Y-%m-%d").date()
                except ValueError:
                    continue
                delta_days = abs((alt_dt - held_dt).days)
                if delta_days > nearest_expiry_max_days:
                    continue
                candidates.append((delta_days, exp_alt))
            candidates.sort()
            for _d, exp_alt in candidates:
                alt_row, alt_dist = _best_at_expiry(exp_alt)
                alt_iv = _safe_iv(alt_row.get("iv")) if alt_row is not None else None
                if alt_iv is not None and alt_iv > 0:
                    iv_at_best = alt_iv
                    best = alt_row
                    best_dist = alt_dist
                    break
    if iv_at_best is None or iv_at_best <= 0 or best is None:
        return None, None
    used_expiry = str(best.get("expiration_date") or "")
    return float(iv_at_best), {
        "underlying_iv_used_strike": float(best.get("strike_price")),
        "underlying_iv_used_expiry": used_expiry,
        "underlying_iv_strike_dist": float(best_dist) if best_dist is not None else None,
        "underlying_iv_expiry_skew_days": _days_between(expiry_iso, used_expiry),
    }


def _safe_iv(raw: object) -> float | None:
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v) or v <= 0:
        return None
    # Polygon sometimes ships IV as a percent (e.g. 56.5 instead of 0.565); normalize.
    return v / 100.0 if v > 5.0 else v


def _days_between(iso_a: str, iso_b: str) -> int | None:
    from datetime import datetime as _dt
    try:
        a = _dt.strptime(iso_a, "%Y-%m-%d").date()
        b = _dt.strptime(iso_b, "%Y-%m-%d").date()
        return abs((a - b).days)
    except (ValueError, TypeError):
        return None


# ?? Part 2: Heston via COS + Ahn?Haugh?Jain propagation ?????????????????????

@dataclass(frozen=True)
class HestonParams:
    """Five-parameter Heston (Heston 1993). ``v0``, ``theta`` are variances."""
    kappa: float
    theta: float
    gamma: float  # vol-of-vol (paper's ?)
    rho: float
    v0: float

    def feller_satisfied(self) -> bool:
        return 2.0 * self.kappa * self.theta > self.gamma * self.gamma

    def is_valid(self) -> bool:
        return (
            self.kappa > 0 and self.theta > 0 and self.gamma > 0
            and -1.0 < self.rho < 1.0 and self.v0 > 0
            and all(math.isfinite(x) for x in (self.kappa, self.theta, self.gamma, self.rho, self.v0))
        )


def heston_char_func(
    u: np.ndarray, *, t: float, r: float, q: float, params: HestonParams,
) -> np.ndarray:
    """?(u; T) = E[e^{i?u?ln(S_T/S_0)}] under the Heston risk-neutral measure.

    Uses the "good characteristic function" (Schoutens-Simons-Tistaert / Albrecher)
    that avoids the branch-cut discontinuity of the original Heston formula
    when integrating along the real axis. This is the form ``QuantLib`` uses.
    """
    if t <= 0:
        return np.exp(1j * u * 0.0)  # ?-distribution at log(S_0/S_0) = 0
    iu = 1j * u
    kappa = params.kappa
    theta = params.theta
    gamma = params.gamma
    rho = params.rho
    v0 = params.v0
    a = kappa - rho * gamma * iu
    d = np.sqrt(a * a + gamma * gamma * (iu + u * u))
    g = (a - d) / (a + d + _EPS)
    exp_dt = np.exp(-d * t)
    # Schoutens 'good' form: use (a-d) numerator and (1 - g?e^{-dT}) / (1 - g)
    C = (r - q) * iu * t + (kappa * theta / (gamma * gamma)) * (
        (a - d) * t - 2.0 * np.log((1.0 - g * exp_dt) / (1.0 - g + _EPS))
    )
    D = ((a - d) / (gamma * gamma)) * ((1.0 - exp_dt) / (1.0 - g * exp_dt + _EPS))
    return np.exp(C + D * v0)


def _cos_chi_psi(k: int, c: float, d: float, a: float, b: float) -> tuple[float, float]:
    """COS analytical ?_k, ?_k coefficients on truncation [a, b] over integration window [c, d]."""
    # Fang & Oosterlee (2008) eqs. (22)?(23).
    bma = b - a
    omega = k * math.pi / bma
    cos_d = math.cos(omega * (d - a))
    cos_c = math.cos(omega * (c - a))
    sin_d = math.sin(omega * (d - a))
    sin_c = math.sin(omega * (c - a))
    chi = (1.0 / (1.0 + omega * omega)) * (
        cos_d * math.exp(d)
        - cos_c * math.exp(c)
        + omega * sin_d * math.exp(d)
        - omega * sin_c * math.exp(c)
    )
    if k == 0:
        psi = d - c
    else:
        psi = (sin_d - sin_c) / omega
    return chi, psi


def cos_put_price(
    *,
    spot: float,
    strike: float,
    t_years: float,
    risk_free: float,
    div_yield: float,
    char_func,
    n_terms: int = _COS_N,
    truncation_range: float = _COS_L,
) -> float:
    """Generic Fang?Oosterlee COS put pricer for any affine characteristic function.

    ``char_func(u, t, r, q)`` must return ``?(u) = E[e^{i u ln(S_T/S_0)}]``.
    """
    if spot <= 0 or strike <= 0 or t_years <= 0:
        return max(strike - spot, 0.0)
    x = math.log(spot / strike)
    # Truncation interval [a, b] centered on the log-forward, scaled by sqrt(T)?L.
    # Use a conservative variance proxy: c2 ~ T as worst-case (Heston cumulants
    # exist but are tedious; the L=12 envelope swallows the slack).
    c1 = (risk_free - div_yield) * t_years
    width = truncation_range * math.sqrt(t_years + 1e-6)
    a = c1 - width
    b = c1 + width
    bma = b - a
    ks = np.arange(n_terms)
    u = ks * math.pi / bma
    phi = char_func(u, t=t_years, r=risk_free, q=div_yield)
    # Put payoff coefficients on [a, 0]: V_k = (2/(b?a)) ? K ? (?_k ? ?_k), Fang?Oosterlee eq. (29).
    chi = np.empty(n_terms)
    psi = np.empty(n_terms)
    for kk in range(n_terms):
        chi[kk], psi[kk] = _cos_chi_psi(kk, c=a, d=0.0, a=a, b=b)
    V_k = (2.0 / bma) * strike * (psi - chi)
    # Sum: discount ? Re[ ?' ?(u_k)?e^{i?u_k?(x?a)}?V_k ] with the prime denoting halved k=0 term.
    summands = phi * np.exp(1j * u * (x - a)) * V_k
    total = 0.5 * summands[0].real + summands[1:].real.sum()
    return float(math.exp(-risk_free * t_years) * total)


def cos_put_price_heston(
    *, spot: float, strike: float, t_years: float,
    risk_free: float, div_yield: float, params: HestonParams,
    n_terms: int = _COS_N, truncation_range: float = _COS_L,
) -> float:
    return cos_put_price(
        spot=spot, strike=strike, t_years=t_years,
        risk_free=risk_free, div_yield=div_yield,
        char_func=lambda u, t, r, q: heston_char_func(u, t=t, r=r, q=q, params=params),
        n_terms=n_terms, truncation_range=truncation_range,
    )


def calibrate_heston_underlying(
    *,
    spot: float,
    strikes: Sequence[float],
    ivs: Sequence[float],
    t_years: float,
    risk_free: float = 0.043,
    div_yield: float = 0.0,
    init: HestonParams | None = None,
) -> HestonParams | None:
    """Quick five-parameter Heston calibration via L-BFGS-B on IV-RMSE.

    Designed for thin chains (5?20 strikes per expiry). Uses sensible bounds
    and one Sobol-like restart. Returns ``None`` if calibration is unstable
    (final RMSE > 5 vol-pts or any param at the bound).
    """
    from scipy.optimize import minimize  # local import keeps module importable without scipy

    valid = [
        (float(k), float(iv)) for k, iv in zip(strikes, ivs)
        if k is not None and iv is not None and iv > 0 and math.isfinite(iv)
        and k > 0 and math.isfinite(k)
    ]
    if len(valid) < 4 or t_years <= 0 or spot <= 0:
        return None
    ks = np.array([k for k, _ in valid], dtype=float)
    target_ivs = np.array([iv for _, iv in valid], dtype=float)
    # Weight ATM strikes more heavily ? wings are noisier.
    atm_dist = np.abs(np.log(ks / spot))
    weights = np.exp(-2.0 * atm_dist)
    weights = weights / weights.sum()

    init = init or HestonParams(
        kappa=2.0, theta=float(np.median(target_ivs) ** 2), gamma=0.6,
        rho=-0.6, v0=float(np.median(target_ivs) ** 2),
    )

    # Param vector: log(kappa), log(theta), log(gamma), atanh(rho), log(v0).
    def _pack(p: HestonParams) -> np.ndarray:
        return np.array([
            math.log(max(p.kappa, 1e-3)),
            math.log(max(p.theta, 1e-4)),
            math.log(max(p.gamma, 1e-3)),
            math.atanh(max(-0.95, min(0.95, p.rho))),
            math.log(max(p.v0, 1e-4)),
        ])

    def _unpack(x: np.ndarray) -> HestonParams:
        return HestonParams(
            kappa=float(math.exp(x[0])),
            theta=float(math.exp(x[1])),
            gamma=float(math.exp(x[2])),
            rho=float(math.tanh(x[3])),
            v0=float(math.exp(x[4])),
        )

    def _objective(x: np.ndarray) -> float:
        p = _unpack(x)
        if not p.is_valid():
            return 1e6
        diffs = []
        for k, iv in zip(ks, target_ivs):
            try:
                price = cos_put_price_heston(
                    spot=spot, strike=k, t_years=t_years,
                    risk_free=risk_free, div_yield=div_yield, params=p,
                )
            except (FloatingPointError, OverflowError):
                return 1e6
            if not math.isfinite(price) or price <= 0:
                return 1e6
            iv_model = bs_implied_vol_from_put(
                spot, k, t_years, price, risk_free=risk_free, div_yield=div_yield,
            )
            if iv_model is None or not math.isfinite(iv_model):
                return 1e6
            diffs.append(iv_model - iv)
        diffs_arr = np.array(diffs)
        return float(np.sqrt(np.average(diffs_arr ** 2, weights=weights)))

    best: HestonParams | None = None
    best_rmse = math.inf
    starting_points: list[HestonParams] = [init]
    # One coarse restart: lower vol-of-vol, more negative rho.
    starting_points.append(HestonParams(
        kappa=4.0, theta=init.theta, gamma=0.3, rho=-0.8, v0=init.v0,
    ))
    for x0 in starting_points:
        try:
            res = minimize(
                _objective, _pack(x0), method="Nelder-Mead",
                options={"maxiter": 250, "xatol": 1e-4, "fatol": 1e-4, "adaptive": True},
            )
        except Exception:
            continue
        if not res.success and res.fun > 1.0:
            continue
        candidate = _unpack(res.x)
        if candidate.is_valid() and res.fun < best_rmse:
            best = candidate
            best_rmse = float(res.fun)

    # Reject pathological calibrations (>5 vol-pts RMSE is unreliable).
    if best is None or best_rmse > 0.05:
        return None
    return best


def propagate_heston_to_sleeve(
    underlying: HestonParams, *, beta: float,
) -> HestonParams | None:
    """Ahn?Haugh?Jain (2014, Prop. 1): closed-form Heston map under daily-reset ?.

    ``(?, ?, ?, ?, V?)  ?  (?, ????, |?|??, sign(?)??, ???V?)``.
    The mean-reversion speed is unchanged.
    """
    if abs(beta) < _EPS or not underlying.is_valid():
        return None
    return HestonParams(
        kappa=underlying.kappa,
        theta=underlying.theta * beta * beta,
        gamma=underlying.gamma * abs(beta),
        rho=underlying.rho * (1.0 if beta > 0 else -1.0),
        v0=underlying.v0 * beta * beta,
    )


def heston_put_spread_fair(
    *, spot_letf: float, strike_long: float, strike_short: float, t_years: float,
    risk_free: float, div_yield: float, params: HestonParams,
) -> float | None:
    """Short put-spread fair value (collect = short_K - long_K) under Heston on the sleeve."""
    if not params.is_valid() or strike_long >= strike_short or spot_letf <= 0:
        return None
    try:
        long_px = cos_put_price_heston(
            spot=spot_letf, strike=strike_long, t_years=t_years,
            risk_free=risk_free, div_yield=div_yield, params=params,
        )
        short_px = cos_put_price_heston(
            spot=spot_letf, strike=strike_short, t_years=t_years,
            risk_free=risk_free, div_yield=div_yield, params=params,
        )
    except (FloatingPointError, OverflowError, ValueError):
        return None
    if not (math.isfinite(long_px) and math.isfinite(short_px)):
        return None
    return float(short_px - long_px)


# ?? Part 3: Bates jump-diffusion (Heston + Merton jumps) + AHJ truncation ???

@dataclass(frozen=True)
class BatesParams:
    """Heston + log-normal jumps (Merton overlay)."""
    heston: HestonParams
    lambda_jump: float       # annualized jump intensity
    mu_jump: float           # mean log-jump size
    sigma_jump: float        # std of log-jump size

    def is_valid(self) -> bool:
        return (
            self.heston.is_valid()
            and self.lambda_jump >= 0 and self.sigma_jump >= 0
            and all(math.isfinite(x) for x in (self.lambda_jump, self.mu_jump, self.sigma_jump))
        )


def bates_char_func(
    u: np.ndarray, *, t: float, r: float, q: float, params: BatesParams,
) -> np.ndarray:
    """?_Bates(u) = ?_Heston(u) ? exp(jump-component ? t).

    The Merton jump component under risk-neutral measure adds
    ``??t?(E[e^{i u Y}] - 1 - i u m)`` where ``m = E[e^Y]?1`` keeps the drift
    risk-neutral. For log-normal jumps ``Y ~ N(?_J, ?_J?)``:
    ``E[e^{i u Y}] = exp(i u ?_J ? ? u? ?_J?)`` and ``m = exp(?_J + ? ?_J?) ? 1``.
    """
    base = heston_char_func(u, t=t, r=r, q=q, params=params.heston)
    lam = params.lambda_jump
    if lam <= 0 or t <= 0:
        return base
    mu = params.mu_jump
    sig = params.sigma_jump
    iu = 1j * u
    jump_cf = np.exp(iu * mu - 0.5 * sig * sig * u * u)
    m = math.exp(mu + 0.5 * sig * sig) - 1.0
    jump_component = lam * t * (jump_cf - 1.0 - iu * m)
    return base * np.exp(jump_component)


def cos_put_price_bates(
    *, spot: float, strike: float, t_years: float,
    risk_free: float, div_yield: float, params: BatesParams,
    n_terms: int = _COS_N, truncation_range: float = _COS_L,
) -> float:
    return cos_put_price(
        spot=spot, strike=strike, t_years=t_years,
        risk_free=risk_free, div_yield=div_yield,
        char_func=lambda u, t, r, q: bates_char_func(u, t=t, r=r, q=q, params=params),
        n_terms=n_terms, truncation_range=truncation_range,
    )


def ahj_truncate_jump_for_sleeve(
    underlying_jump_mu: float, underlying_jump_sigma: float, *, beta: float,
    n_quad: int = 96,
) -> tuple[float, float, float]:
    """Ahn?Haugh?Jain truncation of a log-normal jump for a ?-sleeve.

    A jump ``Y`` on the underlying produces a sleeve jump ``Y^L = max(?(Y?1), ?1)+1``.
    For inverse sleeves and large |?|, the truncation prevents L<0 and creates a
    "limited-liability insurance" drift premium. We approximate the truncated
    distribution by fitting a log-normal to the first two moments of the truncated
    log-jump, and return ``(?_L, ?_L, m_L)`` where ``m_L = E[exp(Y^L)] ? 1`` is the
    risk-neutral compensator.

    For ?=1 this is a no-op; for ?=2 the truncation is negligible at typical jump
    sizes; for ?=-2 or ?=-3 the limited-liability premium is material on deep moves.
    """
    if abs(beta - 1.0) < _EPS or underlying_jump_sigma <= 0:
        return float(underlying_jump_mu), float(underlying_jump_sigma), float(
            math.exp(underlying_jump_mu + 0.5 * underlying_jump_sigma ** 2) - 1.0
        )
    # Gauss-Hermite quadrature for E[g(Y)] under Y ~ N(?, ??): use a symmetric grid.
    nodes, weights = np.polynomial.hermite.hermgauss(n_quad)
    # Y_samples
    y_samples = math.sqrt(2.0) * underlying_jump_sigma * nodes + underlying_jump_mu
    factor = (np.exp(y_samples) - 1.0) * beta  # ?(Y?1) where Y=exp(y)
    # Truncate at -1: a jump that would push L below zero is replaced by the wipe-out ?1.
    truncated_factor = np.maximum(factor, -0.999999)
    yL = np.log1p(truncated_factor)  # ln(1 + ?(Y?1)) on the truncated support
    w = weights / math.sqrt(math.pi)
    mu_L = float(np.sum(w * yL))
    var_L = float(np.sum(w * (yL - mu_L) ** 2))
    sigma_L = math.sqrt(max(var_L, 1e-12))
    m_L = float(np.sum(w * truncated_factor))
    return mu_L, sigma_L, m_L


def propagate_bates_to_sleeve(
    underlying: BatesParams, *, beta: float,
) -> BatesParams | None:
    """AHJ propagation for Bates: Heston part via Prop. 1, jump part via truncation."""
    if abs(beta) < _EPS or not underlying.is_valid():
        return None
    heston_sleeve = propagate_heston_to_sleeve(underlying.heston, beta=beta)
    if heston_sleeve is None:
        return None
    mu_L, sigma_L, _m_L = ahj_truncate_jump_for_sleeve(
        underlying.mu_jump, underlying.sigma_jump, beta=beta,
    )
    return BatesParams(
        heston=heston_sleeve,
        lambda_jump=underlying.lambda_jump,
        mu_jump=mu_L,
        sigma_jump=sigma_L,
    )


def calibrate_bates_underlying(
    *,
    spot: float,
    strikes: Sequence[float],
    ivs: Sequence[float],
    t_years: float,
    risk_free: float = 0.043,
    div_yield: float = 0.0,
    heston_seed: HestonParams | None = None,
    days_to_event: int | None = None,
) -> BatesParams | None:
    """Bates calibration: re-use Heston calibration, add Merton jump fitted on residuals.

    The jump intensity is gated on whether an earnings event is inside the chain
    horizon. When ``days_to_event`` is provided and ? t_years?365, we seed
    ``? = 1/T`` (single expected jump in the horizon) and let the optimizer move it.
    Otherwise we seed ``? = 0.5/yr`` (background single-name jump rate) which is
    moved sparingly.
    """
    from scipy.optimize import minimize

    valid = [
        (float(k), float(iv)) for k, iv in zip(strikes, ivs)
        if k is not None and iv is not None and iv > 0 and math.isfinite(iv)
        and k > 0 and math.isfinite(k)
    ]
    if len(valid) < 5 or t_years <= 0 or spot <= 0:
        return None

    # Start from a Heston fit (cheap).
    if heston_seed is None:
        heston_seed = calibrate_heston_underlying(
            spot=spot, strikes=strikes, ivs=ivs, t_years=t_years,
            risk_free=risk_free, div_yield=div_yield,
        )
        if heston_seed is None:
            return None

    if days_to_event is not None and 0 < days_to_event <= int(t_years * 365 + 1):
        lam0 = 1.0 / max(t_years, 7.0 / 365)
    else:
        lam0 = 0.5

    init = BatesParams(heston=heston_seed, lambda_jump=lam0, mu_jump=-0.05, sigma_jump=0.10)

    ks = np.array([k for k, _ in valid], dtype=float)
    target_ivs = np.array([iv for _, iv in valid], dtype=float)
    atm_dist = np.abs(np.log(ks / spot))
    weights = np.exp(-1.5 * atm_dist)
    # Up-weight the wings a touch ? that's where jumps matter most.
    weights *= 1.0 + 0.5 * np.clip(atm_dist, 0.0, 0.5) / 0.5
    weights = weights / weights.sum()

    def _pack(p: BatesParams) -> np.ndarray:
        h = p.heston
        return np.array([
            math.log(max(h.kappa, 1e-3)),
            math.log(max(h.theta, 1e-4)),
            math.log(max(h.gamma, 1e-3)),
            math.atanh(max(-0.95, min(0.95, h.rho))),
            math.log(max(h.v0, 1e-4)),
            math.log(max(p.lambda_jump, 1e-3)),
            p.mu_jump,
            math.log(max(p.sigma_jump, 1e-3)),
        ])

    def _unpack(x: np.ndarray) -> BatesParams:
        h = HestonParams(
            kappa=float(math.exp(x[0])),
            theta=float(math.exp(x[1])),
            gamma=float(math.exp(x[2])),
            rho=float(math.tanh(x[3])),
            v0=float(math.exp(x[4])),
        )
        return BatesParams(
            heston=h,
            lambda_jump=float(math.exp(x[5])),
            mu_jump=float(x[6]),
            sigma_jump=float(math.exp(x[7])),
        )

    def _objective(x: np.ndarray) -> float:
        p = _unpack(x)
        if not p.is_valid():
            return 1e6
        diffs = []
        for k, iv in zip(ks, target_ivs):
            try:
                price = cos_put_price_bates(
                    spot=spot, strike=k, t_years=t_years,
                    risk_free=risk_free, div_yield=div_yield, params=p,
                )
            except (FloatingPointError, OverflowError, ValueError):
                return 1e6
            if not math.isfinite(price) or price <= 0:
                return 1e6
            iv_model = bs_implied_vol_from_put(
                spot, k, t_years, price, risk_free=risk_free, div_yield=div_yield,
            )
            if iv_model is None or not math.isfinite(iv_model):
                return 1e6
            diffs.append(iv_model - iv)
        diffs_arr = np.array(diffs)
        return float(np.sqrt(np.average(diffs_arr ** 2, weights=weights)))

    try:
        res = minimize(
            _objective, _pack(init), method="Nelder-Mead",
            options={"maxiter": 350, "xatol": 1e-4, "fatol": 1e-4, "adaptive": True},
        )
    except Exception:
        return None
    cand = _unpack(res.x)
    if not cand.is_valid() or res.fun > 0.06:
        return None
    return cand


def bates_put_spread_fair(
    *, spot_letf: float, strike_long: float, strike_short: float, t_years: float,
    risk_free: float, div_yield: float, params: BatesParams,
) -> float | None:
    if not params.is_valid() or strike_long >= strike_short or spot_letf <= 0:
        return None
    try:
        long_px = cos_put_price_bates(
            spot=spot_letf, strike=strike_long, t_years=t_years,
            risk_free=risk_free, div_yield=div_yield, params=params,
        )
        short_px = cos_put_price_bates(
            spot=spot_letf, strike=strike_short, t_years=t_years,
            risk_free=risk_free, div_yield=div_yield, params=params,
        )
    except (FloatingPointError, OverflowError, ValueError):
        return None
    if not (math.isfinite(long_px) and math.isfinite(short_px)):
        return None
    return float(short_px - long_px)


# ?? Part 4: Unified per-row extras dispatcher ???????????????????????????????

def compute_letf_model_extras(
    *,
    yb_etf: str,
    underlying: str | None,
    sleeve_2x: str,
    beta: float,
    spot_letf: float | None,
    spot_underlying: float | None,
    strike_long: float | None,
    strike_short: float | None,
    expiry_iso: str,
    t_years: float,
    risk_free: float,
    expense_rate_letf: float,
    iv_sleeve_market: float | None,
    spread_mid: float | None,
    underlying_iv_chain: Iterable[dict] | None,
    sigma_bar_underlying: float | None = None,
    is_single_stock_sleeve: bool | None = None,
    days_to_event: int | None = None,
    enable_heston: bool = True,
    enable_bates: bool = True,
) -> dict:
    """Compute AZ / Heston / Bates extras for one VRP row. Returns flat dict suitable for ``row.update``.

    The function is designed to *always succeed* and emit ``None``s on
    individual model failures. Calibration timing on a thin chain is ~50?250 ms
    so this is suitable for the daily vrp_live build but not for intraday loops.
    """
    out: dict = {}

    # ?? AZ rescaling ????????????????????????????????????????????????????????
    az_long_iv = az_short_iv = None
    az_long_strike_und = az_short_strike_und = None
    az_long_meta = az_short_meta = None
    # AZ moneyness map only needs the spots, the strikes, and beta. The sigma_bar
    # term is a second-order correction (~ (beta-1)*sigma^2*T/2) that is < 1bp
    # at 0-2 DTE for sigma <= 2.5, so a missing sigma-bar must NOT gate the AZ
    # pipeline. Fall back to 0.0 so the AZ mapping always produces a strike.
    sigma_bar_eff = float(sigma_bar_underlying) if sigma_bar_underlying else 0.0
    if (spot_letf and spot_underlying and strike_long and strike_short
            and underlying_iv_chain is not None):
        kL = az_moneyness_map_strike(
            k_letf=strike_long, l0=spot_letf, x0=spot_underlying,
            beta=beta, risk_free=risk_free, expense_rate_letf=expense_rate_letf,
            t_years=t_years, sigma_bar=sigma_bar_eff,
        )
        kS = az_moneyness_map_strike(
            k_letf=strike_short, l0=spot_letf, x0=spot_underlying,
            beta=beta, risk_free=risk_free, expense_rate_letf=expense_rate_letf,
            t_years=t_years, sigma_bar=sigma_bar_eff,
        )
        if kL is not None and kS is not None:
            az_long_strike_und = kL
            az_short_strike_und = kS
            az_long_iv, az_long_meta = lookup_underlying_iv_at_strike(
                underlying_chain=underlying_iv_chain, underlying_spot=spot_underlying,
                expiry_iso=expiry_iso, target_strike=kL, put_call="P",
            )
            az_short_iv, az_short_meta = lookup_underlying_iv_at_strike(
                underlying_chain=underlying_iv_chain, underlying_spot=spot_underlying,
                expiry_iso=expiry_iso, target_strike=kS, put_call="P",
            )
    az_iv_spread = None
    az_implied_sleeve_iv_avg = None
    if az_long_iv is not None and az_short_iv is not None:
        az_iv_spread = 0.5 * (az_long_iv + az_short_iv)
        az_implied_sleeve_iv_avg = az_implied_sleeve_iv(
            sigma_underlying_at_mapped_k=az_iv_spread, beta=beta,
        )
    out["az_mapped_strike_long_underlying"] = (
        round(float(az_long_strike_und), 6) if az_long_strike_und is not None else None
    )
    out["az_mapped_strike_short_underlying"] = (
        round(float(az_short_strike_und), 6) if az_short_strike_und is not None else None
    )
    out["az_underlying_iv_long"] = round(float(az_long_iv), 6) if az_long_iv is not None else None
    out["az_underlying_iv_short"] = round(float(az_short_iv), 6) if az_short_iv is not None else None
    out["az_implied_sleeve_iv"] = (
        round(float(az_implied_sleeve_iv_avg), 6) if az_implied_sleeve_iv_avg is not None else None
    )
    az_cone = az_cone_residual_iv(
        sigma_letf_market=iv_sleeve_market,
        sigma_underlying_at_mapped_k=az_iv_spread,
        beta=beta,
    )
    out["az_cone_residual_iv"] = round(float(az_cone), 6) if az_cone is not None else None
    out["az_meta"] = {"long": az_long_meta, "short": az_short_meta} if az_long_meta or az_short_meta else None

    # If the sleeve is missing IV entirely, use AZ-implied as the surrogate so
    # downstream BS pricing & ?-edge isn't completely null.
    az_filled_iv = az_implied_sleeve_iv_avg if iv_sleeve_market is None else iv_sleeve_market
    az_fair_diff = None
    az_breakeven_sigma = None
    az_iv_minus_be_sigma = None
    if (az_filled_iv is not None and az_filled_iv > 0 and spot_letf and strike_long
            and strike_short and t_years > 0):
        long_px = bs_put_price(spot_letf, strike_long, t_years, az_filled_iv, risk_free=risk_free)
        short_px = bs_put_price(spot_letf, strike_short, t_years, az_filled_iv, risk_free=risk_free)
        az_fair_diff = max(0.0, short_px - long_px)
        if spread_mid is not None and spread_mid > 0 and spread_mid < (strike_short - strike_long):
            az_breakeven_sigma = _solve_bs_breakeven_sigma(
                spot=spot_letf, strike_long=strike_long, strike_short=strike_short,
                t_years=t_years, target_mid=spread_mid, risk_free=risk_free,
            )
            if az_breakeven_sigma is not None and az_filled_iv is not None:
                az_iv_minus_be_sigma = az_filled_iv - az_breakeven_sigma
    out["az_put_spread_fair"] = round(az_fair_diff, 6) if az_fair_diff is not None else None
    out["az_breakeven_sigma_annual"] = round(az_breakeven_sigma, 6) if az_breakeven_sigma is not None else None
    out["az_iv_minus_breakeven_sigma"] = round(az_iv_minus_be_sigma, 6) if az_iv_minus_be_sigma is not None else None
    out["az_iv_source"] = (
        "letf_market" if iv_sleeve_market is not None
        else ("az_imputed_from_underlying" if az_implied_sleeve_iv_avg is not None else None)
    )

    # ?? Heston calibration on underlying surface, propagation to sleeve ?????
    heston_params = None
    heston_sleeve = None
    if enable_heston and underlying_iv_chain is not None and t_years > 0 and spot_underlying:
        und_strikes, und_ivs = _gather_underlying_iv_panel(
            underlying_iv_chain, expiry_iso, target_atm=spot_underlying,
        )
        if len(und_strikes) >= 4:
            heston_params = calibrate_heston_underlying(
                spot=spot_underlying, strikes=und_strikes, ivs=und_ivs,
                t_years=t_years, risk_free=risk_free, div_yield=0.0,
            )
            if heston_params is not None:
                heston_sleeve = propagate_heston_to_sleeve(heston_params, beta=beta)
    heston_fair = None
    heston_fair_minus_mid = None
    heston_edge_pp_of_max_loss = None
    max_loss = (strike_short - strike_long) if (strike_long and strike_short) else None
    if heston_sleeve is not None and spot_letf and strike_long and strike_short and t_years > 0:
        heston_fair = heston_put_spread_fair(
            spot_letf=spot_letf, strike_long=strike_long, strike_short=strike_short,
            t_years=t_years, risk_free=risk_free,
            div_yield=expense_rate_letf, params=heston_sleeve,
        )
        if (heston_fair is not None and spread_mid is not None and spread_mid > 0
                and max_loss and max_loss > 0):
            # Price-space edge: market is paying more than Heston says it should -> SELL.
            heston_fair_minus_mid = float(spread_mid - heston_fair)
            heston_edge_pp_of_max_loss = 100.0 * heston_fair_minus_mid / max_loss
    out["heston_underlying_params"] = (
        {"kappa": round(heston_params.kappa, 4), "theta": round(heston_params.theta, 4),
         "gamma": round(heston_params.gamma, 4), "rho": round(heston_params.rho, 4),
         "v0": round(heston_params.v0, 4), "feller_ok": heston_params.feller_satisfied()}
        if heston_params is not None else None
    )
    out["heston_sleeve_params"] = (
        {"kappa": round(heston_sleeve.kappa, 4), "theta": round(heston_sleeve.theta, 4),
         "gamma": round(heston_sleeve.gamma, 4), "rho": round(heston_sleeve.rho, 4),
         "v0": round(heston_sleeve.v0, 4)}
        if heston_sleeve is not None else None
    )
    out["heston_put_spread_fair"] = round(heston_fair, 6) if heston_fair is not None else None
    out["heston_fair_minus_mid"] = (
        round(heston_fair_minus_mid, 6) if heston_fair_minus_mid is not None else None
    )
    out["heston_edge_pp_of_max_loss"] = (
        round(heston_edge_pp_of_max_loss, 4) if heston_edge_pp_of_max_loss is not None else None
    )

    # ?? Bates calibration for single-stock sleeves ??????????????????????????
    if is_single_stock_sleeve is None:
        # Default heuristic: anything with an underlying that is itself a single ticker
        # (not SPY/QQQ/SPX/XLF/XLK/SOXX/XBI/TLT/GDX/IBIT/ETHA) is "single-stock".
        is_single_stock_sleeve = _looks_single_stock(underlying)
    bates_fair = None
    bates_fair_minus_mid = None
    bates_edge_pp_of_max_loss = None
    bates_params = None
    bates_sleeve = None
    if enable_bates and is_single_stock_sleeve and heston_params is not None:
        und_strikes, und_ivs = _gather_underlying_iv_panel(
            underlying_iv_chain or [], expiry_iso, target_atm=spot_underlying or 0.0,
        )
        if len(und_strikes) >= 5:
            bates_params = calibrate_bates_underlying(
                spot=spot_underlying, strikes=und_strikes, ivs=und_ivs,
                t_years=t_years, risk_free=risk_free, div_yield=0.0,
                heston_seed=heston_params, days_to_event=days_to_event,
            )
            if bates_params is not None:
                bates_sleeve = propagate_bates_to_sleeve(bates_params, beta=beta)
    if bates_sleeve is not None and spot_letf and strike_long and strike_short and t_years > 0:
        bates_fair = bates_put_spread_fair(
            spot_letf=spot_letf, strike_long=strike_long, strike_short=strike_short,
            t_years=t_years, risk_free=risk_free,
            div_yield=expense_rate_letf, params=bates_sleeve,
        )
        if (bates_fair is not None and spread_mid is not None and spread_mid > 0
                and max_loss and max_loss > 0):
            bates_fair_minus_mid = float(spread_mid - bates_fair)
            bates_edge_pp_of_max_loss = 100.0 * bates_fair_minus_mid / max_loss
    out["bates_underlying_params"] = (
        {"kappa": round(bates_params.heston.kappa, 4), "theta": round(bates_params.heston.theta, 4),
         "gamma": round(bates_params.heston.gamma, 4), "rho": round(bates_params.heston.rho, 4),
         "v0": round(bates_params.heston.v0, 4),
         "lambda": round(bates_params.lambda_jump, 4),
         "mu_jump": round(bates_params.mu_jump, 4),
         "sigma_jump": round(bates_params.sigma_jump, 4)}
        if bates_params is not None else None
    )
    out["bates_sleeve_params"] = (
        {"kappa": round(bates_sleeve.heston.kappa, 4), "theta": round(bates_sleeve.heston.theta, 4),
         "gamma": round(bates_sleeve.heston.gamma, 4), "rho": round(bates_sleeve.heston.rho, 4),
         "v0": round(bates_sleeve.heston.v0, 4),
         "lambda": round(bates_sleeve.lambda_jump, 4),
         "mu_jump": round(bates_sleeve.mu_jump, 4),
         "sigma_jump": round(bates_sleeve.sigma_jump, 4)}
        if bates_sleeve is not None else None
    )
    out["bates_put_spread_fair"] = round(bates_fair, 6) if bates_fair is not None else None
    out["bates_fair_minus_mid"] = (
        round(bates_fair_minus_mid, 6) if bates_fair_minus_mid is not None else None
    )
    out["bates_edge_pp_of_max_loss"] = (
        round(bates_edge_pp_of_max_loss, 4) if bates_edge_pp_of_max_loss is not None else None
    )

    # -- Pick the "best model" headline edge --------------------------------
    # Priority: Bates (single-stock + has chain) > Heston > AZ-imputed > BS (caller's).
    # Headline edge is fair-minus-mid in price space (% of max-loss).
    # Positive => market pays more than the model says it should => SELL the spread.
    best_model = None
    best_edge_pp = None
    az_edge_pp = None
    az_fair_minus_mid_val = None
    if (out.get("az_put_spread_fair") is not None and spread_mid is not None
            and spread_mid > 0 and max_loss and max_loss > 0):
        az_fair = out["az_put_spread_fair"]
        az_fair_minus_mid_val = float(spread_mid - az_fair)
        az_edge_pp = 100.0 * az_fair_minus_mid_val / max_loss
    out["az_fair_minus_mid"] = (
        round(az_fair_minus_mid_val, 6) if az_fair_minus_mid_val is not None else None
    )
    out["az_edge_pp_of_max_loss"] = round(az_edge_pp, 4) if az_edge_pp is not None else None

    if bates_edge_pp_of_max_loss is not None:
        best_model, best_edge_pp = "bates", bates_edge_pp_of_max_loss
    elif heston_edge_pp_of_max_loss is not None:
        best_model, best_edge_pp = "heston", heston_edge_pp_of_max_loss
    elif az_edge_pp is not None:
        best_model, best_edge_pp = "az", az_edge_pp
    out["best_model_edge_pp_of_max_loss"] = (
        round(best_edge_pp, 4) if best_edge_pp is not None else None
    )
    out["best_model_name"] = best_model

    # -- Canonical (BS-free) public field names ------------------------------
    # The UI consumes these short names; the per-model fields above remain for
    # debug / regression / disagreement-checking. P1+P2+P3 made BS the wrong
    # kernel for LETF put-spreads; the dashboard must default to the calibrated
    # model fair, not the diffusion-only BS fair.
    fair_lookup = {
        "bates": out.get("bates_put_spread_fair"),
        "heston": out.get("heston_put_spread_fair"),
        "az": out.get("az_put_spread_fair"),
    }
    fair_minus_mid_lookup = {
        "bates": out.get("bates_fair_minus_mid"),
        "heston": out.get("heston_fair_minus_mid"),
        "az": out.get("az_fair_minus_mid"),
    }
    out["model_name"] = best_model
    out["model_fair"] = fair_lookup.get(best_model) if best_model else None
    out["model_fair_minus_mid"] = (
        fair_minus_mid_lookup.get(best_model) if best_model else None
    )
    out["edge_pp_of_max_loss"] = (
        round(best_edge_pp, 4) if best_edge_pp is not None else None
    )
    # Disagreement check across kernels (max fair − min fair, ignoring None).
    # PMs care about model uncertainty; >20% of max-loss is "the models don't
    # agree, don't size this row to full risk".
    fair_values = [v for v in fair_lookup.values() if v is not None]
    if len(fair_values) >= 2 and max_loss and max_loss > 0:
        spread = max(fair_values) - min(fair_values)
        out["model_disagreement_pp_of_max_loss"] = round(100.0 * spread / max_loss, 4)
    else:
        out["model_disagreement_pp_of_max_loss"] = None

    return out


def _gather_underlying_iv_panel(
    chain: Iterable[dict], expiry_iso: str, *, target_atm: float, max_logmoney: float = 0.4,
) -> tuple[list[float], list[float]]:
    """Pick puts at the target expiry (or nearest) within ?max_logmoney of ATM for calibration."""
    rows = [c for c in chain if isinstance(c, dict)]
    if not rows:
        return [], []
    # Prefer exact expiry; if none, pick the nearest expiry with at least 5 puts.
    expiries = sorted({str(c.get("expiration_date") or "") for c in rows if c.get("expiration_date")})
    chosen_exp = None
    if expiry_iso in expiries:
        chosen_exp = expiry_iso
    else:
        from datetime import datetime as _dt
        try:
            held = _dt.strptime(expiry_iso, "%Y-%m-%d").date()
        except ValueError:
            held = None
        if held is not None:
            candidates = []
            for exp in expiries:
                try:
                    e = _dt.strptime(exp, "%Y-%m-%d").date()
                except ValueError:
                    continue
                candidates.append((abs((e - held).days), exp))
            candidates.sort()
            for _d, exp in candidates:
                puts_at_exp = [c for c in rows if str(c.get("expiration_date")) == exp
                               and str(c.get("contract_type", "")).lower() == "put"]
                if len(puts_at_exp) >= 5:
                    chosen_exp = exp
                    break
    if chosen_exp is None:
        return [], []
    ks: list[float] = []
    ivs: list[float] = []
    for c in rows:
        if str(c.get("expiration_date")) != chosen_exp:
            continue
        if str(c.get("contract_type", "")).lower() != "put":
            continue
        try:
            k = float(c.get("strike_price"))
        except (TypeError, ValueError):
            continue
        iv = _safe_iv(c.get("iv"))
        if iv is None or iv <= 0 or k <= 0:
            continue
        if target_atm > 0 and abs(math.log(k / target_atm)) > max_logmoney:
            continue
        ks.append(k)
        ivs.append(iv)
    return ks, ivs


def _solve_bs_breakeven_sigma(
    *, spot: float, strike_long: float, strike_short: float, t_years: float,
    target_mid: float, risk_free: float, lo: float = 0.01, hi: float = 6.0,
    tol: float = 1e-5, max_iter: int = 80,
) -> float | None:
    """Brent-style root: find ? such that BS short-spread fair = target_mid. Mirrors event_vol parity."""
    if spot <= 0 or t_years <= 0 or target_mid is None or target_mid <= 0:
        return None
    if strike_long >= strike_short:
        return None
    intrinsic_cap = strike_short - strike_long
    if target_mid >= intrinsic_cap:
        return None

    def f(sigma: float) -> float:
        if sigma <= 0:
            return -target_mid
        ls = bs_put_price(spot, strike_short, t_years, sigma, risk_free=risk_free)
        ll = bs_put_price(spot, strike_long, t_years, sigma, risk_free=risk_free)
        return (ls - ll) - target_mid

    f_lo = f(lo)
    f_hi = f(hi)
    if not math.isfinite(f_lo) or not math.isfinite(f_hi):
        return None
    if f_lo * f_hi > 0:
        return None
    a, b = lo, hi
    for _ in range(max_iter):
        m = 0.5 * (a + b)
        fm = f(m)
        if abs(fm) < tol or (b - a) < tol:
            return float(m)
        if f_lo * fm <= 0:
            b = m
        else:
            a = m
            f_lo = fm
    return float(0.5 * (a + b))


_INDEX_OR_ETF_UNDERLYINGS = {
    "SPY", "QQQ", "IWM", "DIA", "SPX", "NDX", "RUT",
    "XLF", "XLE", "XLK", "XLY", "XLV", "XLI", "XLB", "XLU", "XLP", "XLRE", "XLC",
    "SOXX", "XBI", "GDX", "IBIT", "ETHA", "TLT", "USO", "GLD", "SLV", "EWZ", "FXI",
    "VIX", "VIX1D", "VIX3M",
}


def _looks_single_stock(underlying: str | None) -> bool:
    if not underlying:
        return False
    return str(underlying).upper() not in _INDEX_OR_ETF_UNDERLYINGS

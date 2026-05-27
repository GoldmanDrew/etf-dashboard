"""Tests for ``scripts/letf_options_models.py`` (P1/P2/P3 of the LETF Options Roadmap).

Covers:
- BS round-trip parity (Black-Scholes put price <-> implied vol inversion).
- AZ moneyness map and AZ-implied sleeve IV scaling.
- Heston char-func + COS pricing degenerates to BS in the gamma->0 limit.
- AHJ propagation (beta=2): ``theta * beta^2``, ``gamma * |beta|``, ``v0 * beta^2``.
- Bates with lambda=0 collapses to Heston.
- Jump premium: adding Merton jumps raises deep-OTM put prices.
- AHJ jump truncation: identity at beta=1, sign-flip on inverse, finite at extreme jumps.
- ``compute_letf_model_extras`` end-to-end smoke test with a synthetic chain.
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from letf_options_models import (  # noqa: E402
    BatesParams,
    HestonParams,
    ahj_truncate_jump_for_sleeve,
    az_cone_residual_iv,
    az_implied_sleeve_iv,
    az_moneyness_map_strike,
    bs_implied_vol_from_put,
    bs_put_price,
    calibrate_heston_underlying,
    compute_letf_model_extras,
    cos_put_price_bates,
    cos_put_price_heston,
    heston_put_spread_fair,
    lookup_underlying_iv_at_strike,
    propagate_bates_to_sleeve,
    propagate_heston_to_sleeve,
)


# ?? BS parity ??????????????????????????????????????????????????????????????

def test_bs_round_trip_atm():
    """BS put -> implied vol must reconstruct the input sigma at ATM."""
    spot, strike, t, sigma = 100.0, 100.0, 30 / 365, 0.25
    px = bs_put_price(spot, strike, t, sigma)
    iv = bs_implied_vol_from_put(spot, strike, t, px)
    assert iv is not None
    assert abs(iv - sigma) < 1e-4


def test_bs_returns_intrinsic_at_zero_t():
    px = bs_put_price(50.0, 60.0, 0.0, 0.3)
    assert pytest.approx(px, abs=1e-9) == 10.0


def test_bs_implied_vol_rejects_above_strike_pv():
    spot, strike, t = 100.0, 100.0, 30 / 365
    # A put price above strike-discount is impossible -> bisection should refuse.
    iv = bs_implied_vol_from_put(spot, strike, t, strike + 1.0)
    assert iv is None


# ?? AZ rescaling (Phase 1) ?????????????????????????????????????????????????

def test_az_map_trivial_case():
    """At r=0, c=0, sigma_bar=0: K_und = X_0 * (K_L / L_0)^(1/beta)."""
    k_und = az_moneyness_map_strike(
        k_letf=80.0, l0=100.0, x0=100.0, beta=2.0,
        risk_free=0.0, expense_rate_letf=0.0, t_years=0.0, sigma_bar=0.0,
    )
    assert k_und is not None
    expected = 100.0 * (0.8 ** 0.5)
    assert pytest.approx(k_und, abs=1e-6) == expected


def test_az_map_includes_drag_correction():
    """Non-zero sigma_bar must shift the mapped strike via the (beta-1)*sigma_bar^2*T/2 term."""
    k_no_drag = az_moneyness_map_strike(
        k_letf=80.0, l0=100.0, x0=100.0, beta=2.0,
        risk_free=0.0, expense_rate_letf=0.0, t_years=1.0, sigma_bar=0.0,
    )
    k_with_drag = az_moneyness_map_strike(
        k_letf=80.0, l0=100.0, x0=100.0, beta=2.0,
        risk_free=0.0, expense_rate_letf=0.0, t_years=1.0, sigma_bar=0.5,
    )
    assert k_with_drag is not None and k_no_drag is not None
    assert k_with_drag > k_no_drag


def test_az_map_returns_none_on_zero_beta():
    """Inverse map blows up at beta=0; the helper must refuse."""
    assert az_moneyness_map_strike(
        k_letf=80.0, l0=100.0, x0=100.0, beta=0.0,
        risk_free=0.0, expense_rate_letf=0.0, t_years=0.0, sigma_bar=0.0,
    ) is None


def test_az_implied_sleeve_iv_scales_by_abs_beta():
    assert pytest.approx(az_implied_sleeve_iv(sigma_underlying_at_mapped_k=0.4, beta=2.0)) == 0.8
    assert pytest.approx(az_implied_sleeve_iv(sigma_underlying_at_mapped_k=0.4, beta=-2.0)) == 0.8


def test_az_cone_residual_signs():
    """Positive residual when sleeve IV exceeds |beta|*sigma_und at the mapped strike."""
    rich = az_cone_residual_iv(sigma_letf_market=0.85, sigma_underlying_at_mapped_k=0.40, beta=2.0)
    cheap = az_cone_residual_iv(sigma_letf_market=0.75, sigma_underlying_at_mapped_k=0.40, beta=2.0)
    assert rich is not None and cheap is not None
    assert rich > 0 > cheap


# ?? Heston + AHJ (Phase 2) ?????????????????????????????????????????????????

def test_heston_degenerates_to_bs_at_zero_vol_of_vol():
    """gamma -> 0 + Feller satisfied: Heston should match BS within ~1bp."""
    p = HestonParams(kappa=2.0, theta=0.04, gamma=1e-4, rho=0.0, v0=0.04)
    sigma = math.sqrt(p.v0)
    T = 0.5
    bs = bs_put_price(100.0, 95.0, T, sigma)
    heston = cos_put_price_heston(
        spot=100.0, strike=95.0, t_years=T,
        risk_free=0.043, div_yield=0.0, params=p,
    )
    assert abs(bs - heston) < 1e-3  # ~1bp on a ~3.5 put


def test_ahj_propagation_formula():
    """AHJ Proposition 1: (kappa, beta^2*theta, |beta|*gamma, sign(beta)*rho, beta^2*v0)."""
    p = HestonParams(kappa=3.0, theta=0.05, gamma=0.6, rho=-0.7, v0=0.04)
    p_letf = propagate_heston_to_sleeve(p, beta=2.0)
    assert p_letf is not None
    assert p_letf.kappa == p.kappa
    assert pytest.approx(p_letf.theta) == p.theta * 4
    assert pytest.approx(p_letf.gamma) == p.gamma * 2
    assert pytest.approx(p_letf.rho) == p.rho
    assert pytest.approx(p_letf.v0) == p.v0 * 4

    # Inverse: rho flips.
    p_inv = propagate_heston_to_sleeve(p, beta=-2.0)
    assert p_inv is not None
    assert pytest.approx(p_inv.rho) == -p.rho


def test_heston_sleeve_consistent_with_bs_sleeve_at_zero_gamma():
    """Heston-on-sleeve at gamma=0 must equal BS at sigma_sleeve = beta * sigma_underlying."""
    p_und = HestonParams(kappa=2.0, theta=0.04, gamma=1e-4, rho=0.0, v0=0.04)
    sigma_und = math.sqrt(p_und.v0)
    T = 0.25
    p_letf = propagate_heston_to_sleeve(p_und, beta=2.0)
    bs_sleeve = bs_put_price(100.0, 85.0, T, 2 * sigma_und)
    heston_sleeve = cos_put_price_heston(
        spot=100.0, strike=85.0, t_years=T,
        risk_free=0.043, div_yield=0.0, params=p_letf,
    )
    assert abs(bs_sleeve - heston_sleeve) < 1e-3


def test_heston_put_spread_fair_positive_short_collect():
    """Short put-spread fair (short_K - long_K) must be > 0 for K_short > K_long."""
    p = HestonParams(kappa=2.0, theta=0.04, gamma=0.5, rho=-0.6, v0=0.04)
    p_letf = propagate_heston_to_sleeve(p, beta=2.0)
    fair = heston_put_spread_fair(
        spot_letf=100.0, strike_long=88.0, strike_short=95.0, t_years=0.1,
        risk_free=0.043, div_yield=0.0099, params=p_letf,
    )
    assert fair is not None and fair > 0


# ?? Bates + AHJ truncation (Phase 3) ???????????????????????????????????????

def test_bates_with_zero_jump_intensity_equals_heston():
    """lambda=0: Bates char-func collapses to pure Heston."""
    h = HestonParams(kappa=2.0, theta=0.04, gamma=0.5, rho=-0.6, v0=0.04)
    b = BatesParams(heston=h, lambda_jump=0.0, mu_jump=0.0, sigma_jump=0.01)
    T = 0.1
    heston_px = cos_put_price_heston(
        spot=100.0, strike=95.0, t_years=T,
        risk_free=0.043, div_yield=0.0, params=h,
    )
    bates_px = cos_put_price_bates(
        spot=100.0, strike=95.0, t_years=T,
        risk_free=0.043, div_yield=0.0, params=b,
    )
    assert abs(heston_px - bates_px) < 1e-6


def test_bates_jumps_lift_deep_otm_puts():
    """Adding a downside-skewed jump must raise deep-OTM put values (insurance premium)."""
    h = HestonParams(kappa=2.0, theta=0.04, gamma=0.5, rho=-0.6, v0=0.04)
    b_no_jump = BatesParams(heston=h, lambda_jump=0.0, mu_jump=0.0, sigma_jump=0.001)
    b_with_jump = BatesParams(heston=h, lambda_jump=3.0, mu_jump=-0.05, sigma_jump=0.08)
    T = 0.1
    p_no = cos_put_price_bates(
        spot=100.0, strike=70.0, t_years=T,
        risk_free=0.043, div_yield=0.0, params=b_no_jump,
    )
    p_with = cos_put_price_bates(
        spot=100.0, strike=70.0, t_years=T,
        risk_free=0.043, div_yield=0.0, params=b_with_jump,
    )
    assert p_with > p_no
    assert p_with - p_no > 1e-4


def test_ahj_truncation_identity_at_beta_one():
    """beta=1: truncation is a no-op so (mu, sigma, m) come out unchanged."""
    mu_L, sigma_L, m_L = ahj_truncate_jump_for_sleeve(-0.05, 0.10, beta=1.0)
    assert pytest.approx(mu_L, abs=1e-9) == -0.05
    assert pytest.approx(sigma_L, abs=1e-9) == 0.10


def test_ahj_truncation_inverse_sleeve_finite_at_extreme_jumps():
    """beta=-2 with extreme jumps: truncation keeps sigma_L finite (limited-liability premium)."""
    mu_L, sigma_L, m_L = ahj_truncate_jump_for_sleeve(-0.30, 0.30, beta=-2.0)
    assert math.isfinite(mu_L) and math.isfinite(sigma_L) and math.isfinite(m_L)
    # Truncation produces a non-zero drift compensator on the inverse sleeve.
    assert m_L > -1.0


def test_propagate_bates_to_sleeve_returns_valid():
    h = HestonParams(kappa=2.0, theta=0.04, gamma=0.5, rho=-0.6, v0=0.04)
    b = BatesParams(heston=h, lambda_jump=0.5, mu_jump=-0.05, sigma_jump=0.10)
    sleeve = propagate_bates_to_sleeve(b, beta=2.0)
    assert sleeve is not None
    assert sleeve.is_valid()
    # Jump intensity unchanged (Poisson rate doesn't depend on beta), but sleeve-side
    # heston has theta * 4, gamma * 2, v0 * 4.
    assert pytest.approx(sleeve.heston.theta) == h.theta * 4
    assert pytest.approx(sleeve.heston.v0) == h.v0 * 4
    assert sleeve.lambda_jump == b.lambda_jump


# ?? Chain lookup helper ?????????????????????????????????????????????????????

def test_lookup_underlying_iv_returns_exact_strike():
    chain = [
        {"expiration_date": "2026-05-29", "contract_type": "put", "strike_price": 280.0, "iv": 0.55},
        {"expiration_date": "2026-05-29", "contract_type": "put", "strike_price": 290.0, "iv": 0.60},
        {"expiration_date": "2026-05-29", "contract_type": "put", "strike_price": 300.0, "iv": 0.65},
    ]
    iv, meta = lookup_underlying_iv_at_strike(
        underlying_chain=chain, underlying_spot=300.0,
        expiry_iso="2026-05-29", target_strike=290.0, put_call="P",
    )
    assert iv == pytest.approx(0.60)
    assert meta and meta["underlying_iv_used_strike"] == 290.0


def test_lookup_underlying_iv_falls_back_to_nearest_expiry():
    chain = [
        # No 5/29 expiry available; fallback should pick 6/05.
        {"expiration_date": "2026-06-05", "contract_type": "put", "strike_price": 290.0, "iv": 0.62},
    ]
    iv, meta = lookup_underlying_iv_at_strike(
        underlying_chain=chain, underlying_spot=300.0,
        expiry_iso="2026-05-29", target_strike=290.0, put_call="P",
    )
    assert iv == pytest.approx(0.62)
    assert meta and meta["underlying_iv_used_expiry"] == "2026-06-05"
    assert meta["underlying_iv_expiry_skew_days"] == 7


# ?? End-to-end compute_letf_model_extras ???????????????????????????????????

def test_compute_letf_model_extras_emits_all_phases():
    """End-to-end: AZ + Heston + Bates emit when underlying chain has enough IV points."""
    und_chain = [
        {
            "expiration_date": "2026-05-29",
            "contract_type": "put",
            "strike_price": k,
            "iv": 0.60 + 0.15 * max(0, 1 - k / 300),
        }
        for k in (240, 250, 260, 270, 280, 290, 300, 310, 320, 330, 340)
    ]
    out = compute_letf_model_extras(
        yb_etf="COYY", underlying="COIN", sleeve_2x="CONL",
        beta=2.0,
        spot_letf=100.0, spot_underlying=300.0,
        strike_long=88.0, strike_short=95.0,
        expiry_iso="2026-05-29",
        t_years=2 / 252,
        risk_free=0.043, expense_rate_letf=0.0099,
        iv_sleeve_market=1.20,
        spread_mid=0.35,
        underlying_iv_chain=und_chain,
        sigma_bar_underlying=0.60,
        is_single_stock_sleeve=True,
    )
    # AZ pipeline must compute mapped strikes + the cone residual.
    assert out["az_mapped_strike_long_underlying"] is not None
    assert out["az_mapped_strike_short_underlying"] is not None
    assert out["az_implied_sleeve_iv"] is not None
    assert out["az_cone_residual_iv"] is not None
    # Heston calibration produced sleeve params.
    assert out["heston_sleeve_params"] is not None
    assert out["heston_put_spread_fair"] is not None
    # Bates calibration produced sleeve params (single-stock).
    assert out["bates_sleeve_params"] is not None
    assert out["bates_put_spread_fair"] is not None
    # Headline pick: Bates wins on single-stock.
    assert out["best_model_name"] == "bates"
    assert out["best_model_edge_pp_of_max_loss"] is not None


def test_compute_letf_model_extras_az_imputed_when_sleeve_iv_missing():
    """Missing-chain sleeve: AZ-implied sleeve IV becomes the BS sigma surrogate."""
    und_chain = [
        {"expiration_date": "2026-05-29", "contract_type": "put", "strike_price": k, "iv": 0.40}
        for k in (90, 95, 100, 105, 110)
    ]
    out = compute_letf_model_extras(
        yb_etf="MTYY", underlying="MSTR", sleeve_2x="MSTU",
        beta=2.0,
        spot_letf=50.0, spot_underlying=100.0,
        strike_long=42.0, strike_short=48.0,
        expiry_iso="2026-05-29",
        t_years=2 / 252,
        risk_free=0.043, expense_rate_letf=0.0099,
        iv_sleeve_market=None,           # no sleeve IV: missing-chain case
        spread_mid=0.25,
        underlying_iv_chain=und_chain,
        sigma_bar_underlying=0.40,
        is_single_stock_sleeve=True,
        enable_heston=False, enable_bates=False,
    )
    assert out["az_implied_sleeve_iv"] is not None
    assert out["az_iv_source"] == "az_imputed_from_underlying"
    # AZ-imputed BS fair must exist so the dashboard has a fair-value to compare.
    assert out["az_put_spread_fair"] is not None


def test_compute_letf_model_extras_returns_empty_safely_when_no_inputs():
    """Pure null inputs must not raise and must emit a dict (possibly with all-None fields)."""
    out = compute_letf_model_extras(
        yb_etf="", underlying=None, sleeve_2x="",
        beta=2.0,
        spot_letf=None, spot_underlying=None,
        strike_long=None, strike_short=None,
        expiry_iso="",
        t_years=0.0,
        risk_free=0.043, expense_rate_letf=0.0099,
        iv_sleeve_market=None,
        spread_mid=None,
        underlying_iv_chain=None,
        sigma_bar_underlying=None,
    )
    assert out["az_implied_sleeve_iv"] is None
    assert out["az_put_spread_fair"] is None
    assert out["heston_put_spread_fair"] is None
    assert out["bates_put_spread_fair"] is None
    assert out["best_model_name"] is None

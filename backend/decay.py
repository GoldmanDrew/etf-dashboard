"""
Decay Analytics Engine.

Computes the realized annual decay of leveraged ETFs relative to their
underlying, using the Stahl log-space methodology from the research notebook.

Since we don't have live price history in this dashboard context, we provide:
  1. A mock decay generator based on leverage/beta (for demo)
  2. Hooks for plugging in real price data + the full Stahl calculation
  3. Spread computation: spread = gross_decay - borrow_net
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

TRADING_DAYS = 252


# ──────────────────────────────────────────────
# Stahl Decay Metrics (full implementation)
# ──────────────────────────────────────────────
def _simple_to_log_annual(b_simple: float) -> float:
    """APR (simple) → continuous annual."""
    if not np.isfinite(b_simple) or b_simple <= -1.0:
        return np.nan
    return float(np.log1p(b_simple))


def _log_to_simple_annual(x_log: float) -> float:
    """Continuous annual → simple annual."""
    if x_log is None or not np.isfinite(x_log):
        return np.nan
    return float(np.expm1(x_log))


def stahl_decay_metrics_one(
    *,
    prices_tr: pd.DataFrame,
    inverse_etf: str,
    benchmark: str,
    leverage: float,
    borrow_annual_simple: float,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> dict:
    """
    Compute Stahl-style decay metrics for one ETF vs its underlying.

    Returns dict with:
      - DecayYield_Simple_Ann: gross annualized decay
      - Borrow_Simple_Ann: borrow cost
      - NetYield_Simple_Ann: decay - borrow (spread)
      - IR_Log: information ratio in log space
      - MaxDD_Net: max drawdown of net carry stream
      - Calmar_Net: net yield / max DD
      - TradingDays: number of observations
    """
    inv_col = f"{inverse_etf.upper()}_TR"
    bmk_col = f"{benchmark.upper()}_TR"

    if inv_col not in prices_tr.columns:
        raise ValueError(f"Missing {inv_col}")
    if bmk_col not in prices_tr.columns:
        raise ValueError(f"Missing {bmk_col}")

    df = pd.concat([prices_tr[inv_col], prices_tr[bmk_col]], axis=1, join="inner").dropna()
    df.columns = ["INV_TR", "BMK_TR"]
    df.index = pd.to_datetime(df.index).tz_localize(None).normalize()
    df = df.sort_index()

    if start:
        df = df.loc[df.index >= pd.Timestamp(start)]
    if end:
        df = df.loc[df.index <= pd.Timestamp(end)]
    if len(df) < 50:
        raise ValueError("Not enough overlapping data")

    r_inv = df["INV_TR"].pct_change().fillna(0.0).to_numpy()
    r_bmk = df["BMK_TR"].pct_change().fillna(0.0).to_numpy()
    L = float(leverage)

    log_inv = np.log1p(r_inv)
    log_bmk = np.log1p(r_bmk)
    log_const = -L * log_bmk  # clean constant-leverage inverse

    decay_log_daily = log_const - log_inv
    n = int(decay_log_daily.size)

    decay_log_ann = float(np.mean(decay_log_daily) * TRADING_DAYS)
    decay_simple_ann = _log_to_simple_annual(decay_log_ann)

    borrow_log_ann = _simple_to_log_annual(borrow_annual_simple)
    borrow_log_daily = borrow_log_ann / TRADING_DAYS if np.isfinite(borrow_log_ann) else np.nan

    net_log_ann = float(decay_log_ann - borrow_log_ann) if np.isfinite(borrow_log_ann) else np.nan
    net_simple_ann = _log_to_simple_annual(net_log_ann)

    vol_log_ann = float(np.std(decay_log_daily, ddof=1) * np.sqrt(TRADING_DAYS)) if n > 1 else np.nan
    ir_log = (net_log_ann / vol_log_ann) if (np.isfinite(net_log_ann) and np.isfinite(vol_log_ann) and vol_log_ann > 0) else np.nan

    # Max drawdown on net carry stream
    maxdd = np.nan
    calmar = np.nan
    if np.isfinite(borrow_log_daily):
        net_log_daily = decay_log_daily - borrow_log_daily
        wealth = np.exp(np.cumsum(net_log_daily))
        peak = np.maximum.accumulate(wealth)
        dd = 1.0 - (wealth / peak)
        maxdd = float(np.max(dd))
        if np.isfinite(net_simple_ann) and maxdd > 0:
            calmar = float(net_simple_ann / maxdd)

    return {
        "InverseETF": inverse_etf,
        "Benchmark": benchmark,
        "Leverage": L,
        "TradingDays": n,
        "DecayYield_Simple_Ann": decay_simple_ann,
        "Borrow_Simple_Ann": borrow_annual_simple,
        "NetYield_Simple_Ann": net_simple_ann,
        "IR_Log": ir_log,
        "MaxDD_Net": maxdd,
        "Calmar_Net": calmar,
    }


# ──────────────────────────────────────────────
# Mock Decay Generator (no price data needed)
# ──────────────────────────────────────────────
def estimate_mock_decay(
    leverage: float,
    beta: float,
    underlying_vol: float = 0.20,
) -> float:
    """
    Rough theoretical decay estimate for a leveraged ETF.

    For a leveraged ETF with factor L tracking an index with annualized vol σ:
      Annual decay ≈ (L² - L) × σ² / 2

    This is the "volatility drag" component. Real decay also includes
    expense ratios, tracking error, rebalancing costs, etc.

    Returns: estimated annual gross decay (positive = ETF loses value)
    """
    L = abs(leverage)
    sigma = underlying_vol * abs(beta) / L if L > 0 else underlying_vol

    # Volatility drag
    vol_drag = (L * L - L) * sigma * sigma / 2.0

    # Add estimated expense ratio and tracking error
    expense_estimate = 0.009 * L  # ~0.9% per unit leverage
    tracking_noise = np.random.uniform(0.002, 0.015)

    decay = vol_drag + expense_estimate + tracking_noise
    return round(float(max(decay, 0.001)), 6)


def compute_mock_decay_for_universe(
    universe_df: pd.DataFrame,
    borrow_map: dict[str, float],
    default_borrow: float = 0.05,
) -> dict[str, dict]:
    """
    Compute mock decay + spread for every symbol in the universe.

    Returns: {symbol: {gross_decay, spread, decay_3m, decay_6m, decay_12m}}
    """
    results = {}
    for _, row in universe_df.iterrows():
        sym = str(row.get("symbol", "")).upper()
        if not sym:
            continue

        lev = float(row.get("Leverage", 2.0) or 2.0)
        beta = float(row.get("Beta", lev) or lev)

        # Estimate underlying vol from beta magnitude
        underlying_vol = 0.18 + 0.03 * np.random.randn()

        gross_decay = estimate_mock_decay(lev, beta, underlying_vol)

        borrow = borrow_map.get(sym, default_borrow)
        spread = round(gross_decay - borrow, 6)

        # Rolling windows: add some variation
        decay_3m = round(gross_decay * (1 + np.random.uniform(-0.15, 0.15)), 6)
        decay_6m = round(gross_decay * (1 + np.random.uniform(-0.10, 0.10)), 6)
        decay_12m = round(gross_decay * (1 + np.random.uniform(-0.05, 0.05)), 6)

        results[sym] = {
            "gross_decay_annual": gross_decay,
            "spread": spread,
            "decay_3m": decay_3m,
            "decay_6m": decay_6m,
            "decay_12m": decay_12m,
        }

    logger.info(f"Computed decay for {len(results)} symbols")
    return results

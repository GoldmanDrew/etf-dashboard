"""Pydantic models for the ETF Borrow Dashboard."""
from __future__ import annotations

import datetime as dt
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class Bucket(str, Enum):
    HIGH_BETA = "bucket_1_high_beta"
    LOW_BETA = "bucket_2_low_beta"
    INVERSE = "bucket_3_inverse"


class ETFRecord(BaseModel):
    symbol: str
    underlying: str
    leverage: float
    expected_leverage: Optional[float] = None
    beta: Optional[float] = None
    beta_n_obs: Optional[int] = None
    bucket: Bucket

    # Borrow data
    borrow_fee_annual: Optional[float] = None
    borrow_rebate_annual: Optional[float] = None
    borrow_net_annual: Optional[float] = None
    shares_available: Optional[int] = None
    borrow_current: Optional[float] = None
    borrow_spiking: bool = False
    borrow_missing: bool = False

    # Decay / analytics
    gross_decay_annual: Optional[float] = None
    expected_gross_decay_annual: Optional[float] = None
    expected_gross_decay_adjusted_annual: Optional[float] = None
    expected_gross_decay_simple_ito_annual: Optional[float] = None
    expected_decay_adjustment_annual: Optional[float] = None
    expected_decay_model: Optional[str] = None
    expected_gross_decay_reliable: Optional[bool] = None
    blended_gross_decay: Optional[float] = None

    # Distributional decay forecast (HARQ-Log anchored on empirical 1y
    # log-IV; see ls-algo/decay_distribution.py).
    expected_gross_decay_p10_annual: Optional[float] = None
    expected_gross_decay_p50_annual: Optional[float] = None
    expected_gross_decay_p90_annual: Optional[float] = None
    expected_gross_decay_mean_annual: Optional[float] = None
    expected_logIV_mu_annual: Optional[float] = None
    expected_logIV_sigma_annual: Optional[float] = None
    expected_gross_decay_dist_model: Optional[str] = None
    expected_gross_decay_dist_n_obs: Optional[float] = None
    expected_gross_decay_dist_horizon_days: Optional[float] = None
    # Anchor-shift bootstrap diagnostics (see ls-algo/screener_v2_fields.py).
    gross_anchor_shift_annual: Optional[float] = None
    gross_anchor_target_annual: Optional[float] = None
    gross_anchor_source: Optional[str] = None

    # Shared forecast volatility used by the main-grid expected ETF return
    # and the Scenarios tab. Built in scripts/build_data.py as a 50/50
    # variance blend of model-implied sigma and robust 6M EWMA when available.
    forecast_vol_underlying_annual: Optional[float] = None
    forecast_vol_model_annual: Optional[float] = None
    forecast_vol_model_source: Optional[str] = None
    forecast_vol_robust_ewma_annual: Optional[float] = None
    forecast_vol_raw_ewma_annual: Optional[float] = None
    forecast_vol_realized_annual: Optional[float] = None
    forecast_vol_blend_weight_model: Optional[float] = None
    forecast_vol_source: Optional[str] = None
    forecast_vol_event_adjusted: Optional[bool] = None
    forecast_vol_note: Optional[str] = None

    spread: Optional[float] = None  # gross_decay - borrow_net
    decay_3m: Optional[float] = None
    decay_6m: Optional[float] = None
    decay_12m: Optional[float] = None

    # ls-algo screener schema v2 (optional; from etf_screened_today.csv)
    asof_date: Optional[str] = None
    product_class: Optional[str] = None
    expected_decay_available: Optional[bool] = None
    is_yieldboost: bool = False
    scenario_style: Optional[str] = None
    income_yield_trailing_annual: Optional[float] = None
    income_yield_recent_annual: Optional[float] = None
    income_distribution_count_1y: Optional[int] = None
    income_latest_distribution: Optional[float] = None
    income_latest_ex_date: Optional[str] = None
    gross_edge_definition: Optional[str] = None
    primary_edge_annual: Optional[float] = None
    gross_for_primary_annual: Optional[float] = None
    borrow_for_net_annual: Optional[float] = None
    borrow_median_60d: Optional[float] = None
    net_edge_p05_annual: Optional[float] = None
    net_edge_p25_annual: Optional[float] = None
    net_edge_p50_annual: Optional[float] = None
    net_edge_p75_annual: Optional[float] = None
    net_edge_p95_annual: Optional[float] = None
    net_edge_hist_json: Optional[str] = None
    net_edge_fan_label: Optional[str] = None
    block_len: Optional[float] = None
    B_reps: Optional[float] = None
    annualization_key: Optional[str] = None
    hac_lag: Optional[float] = None
    sigma_b_annual: Optional[float] = None
    stress_borrow_rho: Optional[float] = None
    borrow_dispersion_type: Optional[str] = None
    high_intraday_risk: Optional[bool] = None
    regime_autocorr_und_21d_proxy: Optional[float] = None
    regime_warning: Optional[str] = None
    decomposition_note: Optional[str] = None
    copula_note: Optional[str] = None
    copula_type: Optional[str] = None
    borrow_weight_halflife_days: Optional[float] = None
    borrow_history_points_used: Optional[float] = None
    borrow_resample_mode: Optional[str] = None
    schema_v: Optional[int] = None
    edge_sign_convention: Optional[str] = None

    # Algo flags from screener
    include_for_algo: bool = False
    strategy_blacklisted: bool = False
    protected: bool = False
    cagr_positive: Optional[bool] = None

    # Freshness
    last_updated: Optional[dt.datetime] = None
    is_stale: bool = False

    class Config:
        use_enum_values = True


class DashboardSummary(BaseModel):
    total_symbols: int = 0
    bucket_1_count: int = 0
    bucket_2_count: int = 0
    bucket_3_count: int = 0
    best_spreads: list[dict] = Field(default_factory=list)
    worst_borrows: list[dict] = Field(default_factory=list)
    pct_stale: float = 0.0
    pct_missing: float = 0.0
    last_refresh: Optional[dt.datetime] = None
    ibkr_connected: bool = False
    errors: list[str] = Field(default_factory=list)


class SystemStatus(BaseModel):
    ibkr_connected: bool = False
    last_fetch_time: Optional[dt.datetime] = None
    last_fetch_duration_ms: Optional[float] = None
    last_decay_calc_time: Optional[dt.datetime] = None
    total_symbols: int = 0
    symbols_with_borrow: int = 0
    symbols_stale: int = 0
    symbols_missing: int = 0
    errors: list[str] = Field(default_factory=list)
    uptime_seconds: float = 0.0


class BorrowHistoryPoint(BaseModel):
    timestamp: dt.datetime
    borrow_net: float
    shares_available: Optional[int] = None

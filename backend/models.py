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
    spread: Optional[float] = None  # gross_decay - borrow_net
    decay_3m: Optional[float] = None
    decay_6m: Optional[float] = None
    decay_12m: Optional[float] = None

    # ls-algo screener schema v2 (optional; from etf_screened_today.csv)
    asof_date: Optional[str] = None
    product_class: Optional[str] = None
    gross_edge_definition: Optional[str] = None
    primary_edge_annual: Optional[float] = None
    gross_for_primary_annual: Optional[float] = None
    borrow_for_net_annual: Optional[float] = None
    borrow_median_60d: Optional[float] = None
    net_edge_p05_annual: Optional[float] = None
    net_edge_p50_annual: Optional[float] = None
    net_edge_p95_annual: Optional[float] = None
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
    schema_v: Optional[int] = None
    edge_sign_convention: Optional[str] = None

    # Algo flags from screener
    include_for_algo: bool = False
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

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

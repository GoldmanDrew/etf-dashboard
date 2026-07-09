"""Bucket 4 backtest engine (ported from ls-algo for static-site artifacts)."""

from .bucket4_dynamic_bt import run_bucket4_backtest_dynamic_h
from .bucket4_hedge_cadence import (
    HedgeCadenceKnobs,
    NameTilt,
    PairPolicy,
    build_h_series,
    build_rebal_dates,
    compute_pair_policy,
    load_policy_from_config,
)
from .bucket4_vol_shape_signals import get_pair_signal, load_vol_shape_history
from .policy_helpers import knobs_from_policy, load_price_panel, make_knobs

__all__ = [
    "HedgeCadenceKnobs",
    "NameTilt",
    "PairPolicy",
    "build_h_series",
    "build_rebal_dates",
    "compute_pair_policy",
    "get_pair_signal",
    "knobs_from_policy",
    "load_policy_from_config",
    "load_price_panel",
    "load_vol_shape_history",
    "make_knobs",
    "run_bucket4_backtest_dynamic_h",
]

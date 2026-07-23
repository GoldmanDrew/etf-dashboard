"""Hedge-target stabilizers for Bucket 4 research paths.

Stabilizers act only on the h target presented at scheduled resize dates.
Signals, cadence, drift threshold, gross sizing, and costs stay unchanged.

Frozen Optimized candidate: ``deadband_005_slew_0025`` (band 0.05, step 0.025).
"""
from __future__ import annotations

import math
from typing import Any

import pandas as pd

# Locked research variants. Absolute h units: 0.05 = five h points.
VARIANTS: dict[str, dict[str, float | str]] = {
    "current": {"mode": "current"},
    "event_ema_025": {"mode": "event_ema", "alpha": 0.25},
    "deadband_005": {"mode": "deadband", "band": 0.05},
    "deadband_005_slew_0025": {"mode": "deadband_slew", "band": 0.05, "step": 0.025},
}

# Default Optimized-tab research controller (not production / GTP).
FROZEN_OPTIMIZED_STABILIZER = "deadband_005_slew_0025"


def resolve_stabilizer(name: str | None) -> dict[str, float | str] | None:
    """Return a stabilizer spec, or None for the unfiltered current path."""
    if name is None:
        return None
    key = str(name).strip().lower()
    if not key or key in {"none", "current", "off"}:
        return None
    if key not in VARIANTS:
        raise ValueError(f"unknown stabilizer: {name}")
    spec = dict(VARIANTS[key])
    if str(spec.get("mode") or "") == "current":
        return None
    return spec


def stabilizer_metadata(name: str) -> dict[str, Any]:
    """Serializable stabilizer block for research artifacts."""
    key = str(name).strip()
    spec = dict(VARIANTS[key]) if key in VARIANTS else {"mode": key}
    return {"name": key, **spec}


def stabilize_h_targets(
    base_h: pd.Series,
    scheduled_dates: pd.DatetimeIndex,
    spec: dict[str, float | str],
) -> pd.Series:
    """Return a target series that changes only at scheduled policy dates."""
    base = pd.to_numeric(base_h, errors="coerce").sort_index().ffill().bfill()
    dates = pd.DatetimeIndex(scheduled_dates).intersection(base.index).sort_values().unique()
    if len(dates) == 0:
        return base
    accepted: dict[pd.Timestamp, float] = {}
    prev: float | None = None
    mode = str(spec.get("mode") or "current")
    for date in dates:
        target = float(base.loc[date])
        if prev is None:
            nxt = target
        else:
            delta = target - prev
            if mode == "current":
                nxt = target
            elif mode == "event_ema":
                alpha = float(spec.get("alpha") or 0.25)
                nxt = prev + alpha * delta
            elif mode == "deadband":
                nxt = target if abs(delta) >= float(spec.get("band") or 0.05) else prev
            elif mode == "deadband_slew":
                band = float(spec.get("band") or 0.05)
                step = float(spec.get("step") or 0.025)
                nxt = prev if abs(delta) < band else prev + math.copysign(min(abs(delta), step), delta)
            else:
                raise ValueError(f"unknown stabilizer: {mode}")
        prev = float(nxt)
        accepted[pd.Timestamp(date)] = prev
    event_series = pd.Series(accepted, dtype=float)
    return event_series.reindex(base.index).ffill().bfill().astype(float)

from __future__ import annotations

import pandas as pd

from scripts.bucket4.bucket4_h_stability import (
    FROZEN_OPTIMIZED_STABILIZER,
    resolve_stabilizer,
    stabilize_h_targets,
    stabilizer_metadata,
)
from scripts.research_b4_h_stability import stabilize_h_targets as research_stabilize


def _series(values: list[float]) -> pd.Series:
    idx = pd.bdate_range("2025-01-02", periods=len(values))
    return pd.Series(values, index=idx)


def test_deadband_holds_small_target_changes_until_cumulative_gap_is_large() -> None:
    base = _series([0.45, 0.47, 0.49, 0.51])
    got = stabilize_h_targets(base, base.index, {"mode": "deadband", "band": 0.05})
    assert list(got.round(4)) == [0.45, 0.45, 0.45, 0.51]


def test_deadband_slew_caps_each_scheduled_adjustment() -> None:
    base = _series([0.45, 0.60, 0.60, 0.60])
    got = stabilize_h_targets(base, base.index, {"mode": "deadband_slew", "band": 0.05, "step": 0.025})
    assert list(got.round(4)) == [0.45, 0.475, 0.50, 0.525]


def test_event_ema_only_updates_on_scheduled_dates() -> None:
    base = _series([0.45, 0.70, 0.70, 0.70])
    scheduled = pd.DatetimeIndex([base.index[0], base.index[2]])
    got = stabilize_h_targets(base, scheduled, {"mode": "event_ema", "alpha": 0.25})
    assert list(got.round(4)) == [0.45, 0.45, 0.5125, 0.5125]


def test_research_reexport_matches_shared_helper() -> None:
    base = _series([0.45, 0.60, 0.60])
    spec = {"mode": "deadband_slew", "band": 0.05, "step": 0.025}
    assert list(research_stabilize(base, base.index, spec)) == list(stabilize_h_targets(base, base.index, spec))


def test_frozen_optimized_stabilizer_resolves() -> None:
    assert FROZEN_OPTIMIZED_STABILIZER == "deadband_005_slew_0025"
    spec = resolve_stabilizer(FROZEN_OPTIMIZED_STABILIZER)
    assert spec == {"mode": "deadband_slew", "band": 0.05, "step": 0.025}
    assert resolve_stabilizer("none") is None
    meta = stabilizer_metadata(FROZEN_OPTIMIZED_STABILIZER)
    assert meta["name"] == FROZEN_OPTIMIZED_STABILIZER
    assert meta["band"] == 0.05

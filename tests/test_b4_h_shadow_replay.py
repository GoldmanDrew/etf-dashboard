from __future__ import annotations

import pandas as pd

from scripts.research_b4_h_shadow_replay import event_diagnostics


def test_event_diagnostics_counts_fast_opposite_h_change_as_reversal() -> None:
    idx = pd.to_datetime(["2025-01-02", "2025-01-10", "2025-01-17"])
    bt = pd.DataFrame({
        "rebalance": [True, True, True],
        "h_used": [0.45, 0.50, 0.46],
        "equity": [1.0, 1.02, 1.01],
        "rebalance_fee": [0.0021, 0.00105, 0.00105],
    }, index=idx)
    got = event_diagnostics(bt, 0.0021)
    assert got["h_change_count"] == 2
    assert got["h_quick_reversal_count_21d"] == 1
    assert got["inferred_traded_notional"] == 2.0


def test_event_diagnostics_does_not_call_same_direction_move_a_reversal() -> None:
    idx = pd.to_datetime(["2025-01-02", "2025-01-10", "2025-01-17"])
    bt = pd.DataFrame({
        "rebalance": [True, True, True], "h_used": [0.45, 0.50, 0.55],
        "equity": [1.0, 1.0, 1.0], "rebalance_fee": [0.0, 0.0, 0.0],
    }, index=idx)
    assert event_diagnostics(bt, 0.0021)["h_quick_reversal_count_21d"] == 0

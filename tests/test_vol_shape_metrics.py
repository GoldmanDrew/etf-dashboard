import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from vol_shape_metrics import (
    VOL_SHAPE_WINDOWS,
    underlying_vol_shape_from_prices,
    underlying_vol_shape_panel_from_prices,
    vol_shape_label,
)


def _prices_from_log_returns(returns):
    idx = pd.bdate_range("2026-01-01", periods=len(returns) + 1)
    levels = [100.0]
    for r in returns:
        levels.append(levels[-1] * math.exp(float(r)))
    return pd.Series(levels, index=idx)


def test_vol_shape_constant_grind_has_high_trend_ratio_low_vcr():
    # Exactly 20 returns → one rolling point (percentiles unset), matches ls-algo test.
    px = _prices_from_log_returns([0.01] * 20)
    out = underlying_vol_shape_from_prices(px, 20)

    assert out["und_vol_shape_20d"] == "quiet_trend"
    assert np.isclose(out["und_trend_ratio_20d"], math.sqrt(5.0), atol=1e-4)
    assert np.isclose(out["und_vcr_20d"], 0.05, atol=1e-4)


def test_vol_shape_single_jump_has_high_vcr():
    px = _prices_from_log_returns(([0.006] * 19) + [0.036277])
    out = underlying_vol_shape_from_prices(px, 20)
    assert out["und_vcr_20d"] > 0.65


def test_vol_shape_panel_covers_both_windows():
    px = _prices_from_log_returns([0.004] * 80)
    panel = underlying_vol_shape_panel_from_prices(px)
    for w in VOL_SHAPE_WINDOWS:
        assert panel.get(f"und_trend_ratio_{w}d") is not None
        assert panel.get(f"und_vol_shape_{w}d")


def test_vol_shape_label_boiling_trend():
    label = vol_shape_label(
        trend_ratio=1.10,
        vcr=0.25,
        abs_return_pctile=0.85,
        rv_pctile=0.5,
        vcr_pctile=0.5,
    )
    assert label == "boiling_trend"


def test_load_vol_shape_by_symbol_joint_gate(tmp_path):
    from vol_shape_metrics import load_vol_shape_by_symbol

    rows = []
    for i in range(70):
        d = (pd.Timestamp("2026-01-01") + pd.Timedelta(days=i)).strftime("%Y-%m-%d")
        rows.append(
            {
                "date": d,
                "ticker": "TEST",
                "nav": 10.0,
                "close_price": 10.0,
                "underlying_adj_close": 100.0 * (1.001**i),
            }
        )
    # Drop underlying on one day — should still compute from joint days only.
    rows[10]["underlying_adj_close"] = None
    pd.DataFrame(rows).to_csv(tmp_path / "metrics.csv", index=False)

    out = load_vol_shape_by_symbol(tmp_path / "metrics.csv", universe_symbols={"TEST"})
    assert "TEST" in out
    assert out["TEST"]["und_vol_shape_source"] == "etf_metrics_daily"
    assert out["TEST"]["und_trend_ratio_20d"] is not None

"""Golden parity: etf-dashboard vol_shape_metrics vs ls-algo vol_shape.py."""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pytest

_SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from vol_shape_metrics import (  # noqa: E402
    VOL_SHAPE_PRIMARY_WINDOW,
    load_vol_shape_by_symbol,
)

_LS_ALGO_ROOTS = [
    Path(__file__).resolve().parents[2] / "ls-algo",
    Path(__file__).resolve().parents[1].parent / "ls-algo",
]
_METRICS = Path(__file__).resolve().parents[1] / "data" / "etf_metrics_daily.csv"


def _ls_algo_on_path() -> Path | None:
    for root in _LS_ALGO_ROOTS:
        if (root / "vol_shape.py").is_file():
            return root
    return None


def _import_ls_vol_shape():
    root = _ls_algo_on_path()
    if root is None:
        return None
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    import vol_shape  # noqa: WPS433

    return vol_shape


_NUMERIC_VOL_SHAPE_SUFFIXES = (
    "daily_annual",
    "weekly_annual",
    "trend_ratio",
    "vcr",
    "return",
    "pctile",
    "median",
)


@pytest.mark.skipif(_ls_algo_on_path() is None, reason="ls-algo sibling repo not present")
@pytest.mark.skipif(not _METRICS.is_file(), reason="etf_metrics_daily.csv missing")
@pytest.mark.parametrize("symbol", ["APLX", "APLZ"])
def test_dashboard_metrics_match_ls_algo_joint_panel(symbol: str):
    ls = _import_ls_vol_shape()
    assert ls is not None
    dash = load_vol_shape_by_symbol(_METRICS, {symbol})
    joint = ls.load_joint_vol_shape_panels_by_etf(_METRICS, {symbol})
    assert symbol in dash and symbol in joint
    w = VOL_SHAPE_PRIMARY_WINDOW
    for key, dv in dash[symbol].items():
        if not key.startswith("und_"):
            continue
        if key == f"und_vol_shape_{w}d" or key.endswith("vol_shape_20d") or key.endswith("vol_shape_60d"):
            assert dash[symbol][key] == joint[symbol][key]
            continue
        if not any(s in key for s in _NUMERIC_VOL_SHAPE_SUFFIXES):
            continue
        lv = joint[symbol].get(key)
        if dv is None or lv is None:
            continue
        try:
            dfv = float(dv)
            lfv = float(lv)
        except (TypeError, ValueError):
            continue
        if not (np.isfinite(dfv) and np.isfinite(lfv)):
            continue
        assert np.isclose(dfv, lfv, rtol=0, atol=1e-6), key

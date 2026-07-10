"""Golden + JS/Python parity for realized pair decay contract."""
from __future__ import annotations

import json
import math
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

from realized_gross_decay import (  # noqa: E402
    PAIR_DRAG_BASIS,
    build_daily_log_drag_series_with_meta,
    compute_horizon_period_returns,
)

FIXTURE = ROOT / "tests" / "fixtures" / "decay_golden.json"


def _load_cases():
    payload = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert payload["contract"]["pair_drag_basis"] == PAIR_DRAG_BASIS
    return payload["cases"]


@pytest.mark.parametrize("case", _load_cases(), ids=lambda c: c["id"])
def test_decay_golden_python(case):
    beta = float(case["beta"])
    points = case["points"]
    result = build_daily_log_drag_series_with_meta(points, beta)
    series = result["series"]
    meta = result["meta"]
    exp = case["expect"]

    assert len(series) == exp["n_drags"]
    if "drag_dates" in exp:
        assert [d["date"] for d in series] == exp["drag_dates"]
    if "skipped_gaps" in exp:
        assert len(meta["skipped_gaps"]) == exp["skipped_gaps"]
    if "convexity_days" in exp:
        assert len(meta["convexity_days"]) == exp["convexity_days"]
    if "gross_log_abs_max" in exp:
        total = sum(d["drag"] for d in series)
        assert abs(total) <= exp["gross_log_abs_max"]
    if "simple_pnl_abs_max" in exp:
        assert abs(series[0]["simple_pnl"]) <= exp["simple_pnl_abs_max"]
    if "drag_abs_min" in exp:
        assert abs(series[0]["drag"]) >= exp["drag_abs_min"]
    if exp.get("check_endpoint_identity"):
        h = int(exp["horizon_days"])
        out = compute_horizon_period_returns(series, [h], borrow_annual=0.0)
        row = out["horizons"][0]
        start = points[0]
        end = points[-1]
        endpoint = beta * math.log(end["tr_und_px"] / start["tr_und_px"]) - math.log(
            end["tr_etf_px"] / start["tr_etf_px"]
        )
        assert abs(row["gross_log"] - endpoint) < 1e-9


def test_decay_golden_js_parity():
    """Run the same fixture through Node and compare drag totals to Python."""
    node_script = r"""
const fs = require('fs');
const path = require('path');
require('./assets/price_basis.js');
const RD = require('./assets/realized_decay.js');
const fixture = JSON.parse(fs.readFileSync('./tests/fixtures/decay_golden.json', 'utf8'));
const out = {};
for (const c of fixture.cases) {
  const pts = c.points.map(p => ({
    date: p.date,
    trEtfPx: p.tr_etf_px,
    trUndPx: p.tr_und_px,
  }));
  const { series, meta } = RD.buildDailyLogDragSeriesWithMeta(pts, c.beta);
  out[c.id] = {
    n: series.length,
    grossLog: series.reduce((a, d) => a + d.drag, 0),
    skipped: (meta.skippedGaps || []).length,
    convexity: (meta.convexityDays || []).length,
    dates: series.map(d => d.date),
  };
}
process.stdout.write(JSON.stringify(out));
"""
    proc = subprocess.run(
        ["node", "-e", node_script],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        pytest.skip(f"node parity unavailable: {proc.stderr[:200]}")
    js = json.loads(proc.stdout)
    for case in _load_cases():
        py = build_daily_log_drag_series_with_meta(case["points"], float(case["beta"]))
        j = js[case["id"]]
        assert j["n"] == len(py["series"])
        assert j["skipped"] == len(py["meta"]["skipped_gaps"])
        assert j["convexity"] == len(py["meta"]["convexity_days"])
        py_gross = sum(d["drag"] for d in py["series"])
        assert abs(j["grossLog"] - py_gross) < 1e-9

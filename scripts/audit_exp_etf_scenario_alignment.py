#!/usr/bin/env python3
"""Gate main-grid Exp. ETF return vs Scenarios 0σ parity for vol-drag LETFs."""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DASHBOARD = REPO / "data" / "dashboard_data.json"

DIV_YIELD_WINDOW_YEARS = {
    "1M": 1 / 12,
    "3M": 3 / 12,
    "6M": 6 / 12,
    "YTD": 6 / 12,
    "12M": 1.0,
    "ALL": 1.0,
}
MAX_LETF_DIV_ADD_ANNUAL = 0.05
HORIZON = "3M"
HORIZON_YEARS = 3 / 12
TOL_STRICT = 0.005
TOL_WARN = 0.02
STRICT_SYMBOLS = frozenset({"SMUP", "BITU", "TQQY", "YSPY"})


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def estimate_etf_return(
    *,
    leverage: float,
    underlying_return: float,
    sigma_annual: float,
    horizon_years: float,
) -> float | None:
    if not all(
        math.isfinite(x)
        for x in (leverage, underlying_return, sigma_annual, horizon_years)
    ):
        return None
    if horizon_years <= 0 or sigma_annual < 0:
        return None
    one_plus = 1.0 + underlying_return
    if one_plus <= 0:
        return None
    drag_log = 0.5 * leverage * (leverage - 1) * (sigma_annual**2) * horizon_years
    etf_log = (leverage * math.log(one_plus)) - drag_log
    raw = math.exp(etf_log) - 1.0
    return max(raw, -0.9999) if math.isfinite(raw) else None


def dividend_cash_additive(rec: dict, window_key: str, horizon_years: float) -> float:
    if rec.get("is_yieldboost"):
        return 0.0
    win = (rec.get("dividend_adjustment") or {}).get(window_key) or {}
    if win.get("dividend_lumpy") is True:
        return 0.0
    yw = win.get("etf_dividend_yield_recurring")
    if yw is None:
        yw = win.get("etf_dividend_yield")
    try:
        yw_f = float(yw)
    except (TypeError, ValueError):
        return 0.0
    if not math.isfinite(yw_f) or yw_f <= 0:
        return 0.0
    win_y = DIV_YIELD_WINDOW_YEARS.get(window_key, 0.5)
    if win_y <= 0:
        return 0.0
    annualized = yw_f / win_y
    if rec.get("scenario_style") == "letf_vol_drag":
        annualized = min(annualized, MAX_LETF_DIV_ADD_ANNUAL)
    return annualized * horizon_years


def expected_exp_etf_return(rec: dict) -> float | None:
    if not rec.get("expected_decay_available"):
        return None
    if rec.get("scenario_style") != "letf_vol_drag":
        return None
    lev = rec.get("delta")
    sigma = rec.get("forecast_vol_underlying_annual")
    try:
        lev_f = float(lev)
        sigma_f = float(sigma)
    except (TypeError, ValueError):
        return None
    nav = estimate_etf_return(
        leverage=lev_f,
        underlying_return=0.0,
        sigma_annual=sigma_f,
        horizon_years=HORIZON_YEARS,
    )
    if nav is None:
        return None
    div = dividend_cash_additive(rec, HORIZON, HORIZON_YEARS)
    return nav + div


def audit_dashboard(*, strict_only: bool = False) -> tuple[list[str], list[str]]:
    payload = _load_json(DASHBOARD)
    failures: list[str] = []
    warnings: list[str] = []
    for rec in payload.get("records") or []:
        if rec.get("scenario_style") != "letf_vol_drag":
            continue
        if not rec.get("expected_decay_available"):
            continue
        sym = str(rec.get("symbol") or "").strip().upper()
        exp = expected_exp_etf_return(rec)
        if exp is None:
            continue
        lev = float(rec["delta"])
        sigma = float(rec["forecast_vol_underlying_annual"])
        scenario = estimate_etf_return(
            leverage=lev,
            underlying_return=0.0,
            sigma_annual=sigma,
            horizon_years=HORIZON_YEARS,
        )
        if scenario is None:
            continue
        scenario_total = scenario + dividend_cash_additive(rec, HORIZON, HORIZON_YEARS)
        delta = abs(exp - scenario_total)
        if delta <= 1e-9:
            continue
        msg = f"{sym}: exp_etf_return vs scenario 0σ delta {delta:.4f} (exp={exp:.4f})"
        if sym in STRICT_SYMBOLS or strict_only:
            if delta > TOL_STRICT:
                failures.append(msg)
        elif delta > TOL_WARN:
            warnings.append(msg)
    return failures, warnings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strict-only", action="store_true")
    parser.add_argument("--warn", action="store_true", help="Print warnings to stderr")
    args = parser.parse_args()
    failures, warnings = audit_dashboard(strict_only=args.strict_only)
    if args.warn and warnings:
        for w in warnings:
            print(f"WARN: {w}", file=sys.stderr)
    if failures:
        for f in failures:
            print(f"FAIL: {f}", file=sys.stderr)
        return 1
    print("audit_exp_etf_scenario_alignment: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

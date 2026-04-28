#!/usr/bin/env python3
"""Simple `delta_v1` NAV forecaster - Stage 1 of the NAV-vs-spot accuracy pipeline.

Identity (Avellaneda-Zhang single-day form, Ito plug-in for one trading day):

    nav_hat_t = nav_anchor * exp(beta * log(spot_und_t / spot_und_anchor)) * (1 - ter_daily)

Inputs (all read from existing dashboard artifacts; no external API calls in
the snapshot loop):

    data/dashboard_data.json          -> records[*] = {symbol, underlying, beta,
                                          product_class, is_yieldboost, ...}
    data/etf_metrics_latest.json      -> by_symbol[SYM] = {nav, close_price, ...}
                                          (used only as fallback diagnostics)
    data/options_cache.json           -> symbols[SYM] = {spot, cache_age_seconds,
                                          updated_at, ...} for ETFs and bucket-3
                                          underlyings
    data/nav_forecasts/_anchors.json  -> by_symbol[SYM] = {nav_close, und_close,
                                          und_symbol, beta, as_of_date, ...}
                                          maintained by `score_nav_forecasts.py`

Outputs:

    data/nav_forecasts/snapshots/<YYYY-MM-DD>.jsonl   -> appended one row/symbol
    data/nav_forecasts/_latest.json                   -> {by_symbol: {SYM: row}}

Confidence routing matches the dashboard's `product_class` taxonomy:

    letf, inverse                -> high
    volatility_etp, passive_low_beta -> medium
    income_yieldboost, income_put_spread, other_structured -> na
    missing inputs / nan beta / yieldboost flag -> na

The schema is intentionally additive: new model versions bump the ``model``
field (e.g. ``delta_v2_swap_mark``) and append; aggregation in
``score_nav_forecasts`` groups by (model, date) so old rows keep their tag.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("forecast_nav")

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
DASHBOARD_DATA_PATH = DATA_DIR / "dashboard_data.json"
ETF_METRICS_LATEST_PATH = DATA_DIR / "etf_metrics_latest.json"
OPTIONS_CACHE_PATH = DATA_DIR / "options_cache.json"

NAV_DIR = DATA_DIR / "nav_forecasts"
SNAPSHOT_DIR = NAV_DIR / "snapshots"
ANCHORS_PATH = NAV_DIR / "_anchors.json"
LATEST_PATH = NAV_DIR / "_latest.json"

MODEL = "delta_v1"

# Single default expense ratio. Most LETF / inverse / vol-ETP products in our
# universe sit between 0.95% and 1.15% - using one anchor keeps v1 simple and
# limits TER bias to <=0.7 bp per trading day. Override per-symbol later if
# `score_nav_forecasts._metrics_daily.json` shows persistent signed bias.
DEFAULT_TER_ANNUAL = 0.0098
TER_OVERRIDES: dict[str, float] = {}

CLASS_HIGH_CONFIDENCE = {"letf", "inverse"}
CLASS_MEDIUM_CONFIDENCE = {"volatility_etp", "passive_low_beta"}
CLASS_NA = {"income_yieldboost", "income_put_spread", "other_structured"}

SPOT_FRESH_SECONDS = 30 * 60  # 30 minutes


@dataclass
class ForecastRecord:
    ts: str
    symbol: str
    model: str
    confidence: str
    product_class: str | None
    und_symbol: str | None
    und_spot_t: float | None
    und_spot_anchor: float | None
    und_anchor_date: str | None
    und_spot_age_sec: float | None
    beta: float | None
    ter_daily: float
    nav_anchor: float | None
    nav_anchor_date: str | None
    nav_hat: float | None
    etf_last: float | None
    etf_last_ts: str | None
    premium_bp: float | None
    notes: str | None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _isfin(x: Any) -> bool:
    try:
        f = float(x)
        return math.isfinite(f)
    except (TypeError, ValueError):
        return False


def _load_json(p: Path) -> dict:
    if not p.exists():
        return {}
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), allow_nan=False, sort_keys=True)
    tmp.replace(path)


def _append_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, separators=(",", ":"), allow_nan=False, sort_keys=True))
            f.write("\n")


def _ter_daily_for(symbol: str) -> float:
    annual = TER_OVERRIDES.get(symbol.upper(), DEFAULT_TER_ANNUAL)
    return float(annual) / 252.0


def _compose_notes(parts: list[str]) -> str | None:
    parts = [p for p in (parts or []) if p]
    return "; ".join(parts) if parts else None


# ---------------------------------------------------------------------------
# Core math + classification
# ---------------------------------------------------------------------------

def compute_nav_hat(
    nav_anchor: float,
    beta: float,
    und_ret: float,
    ter_daily: float,
) -> float:
    """delta_v1 closed form: ``nav_anchor * exp(beta * r_und) * (1 - ter_daily)``.

    `und_ret` is a log return (``log(spot_t / spot_anchor)``). `beta` is the
    target leverage from the screener row (negative for inverse products).
    """
    growth = math.exp(float(beta) * float(und_ret))
    return float(nav_anchor) * growth * (1.0 - float(ter_daily))


def _classify_confidence(
    record: dict,
    anchor: dict | None,
    und_spot_age_sec: float | None,
) -> tuple[str, list[str]]:
    """Return ``(confidence, notes_parts)``. Confidence in {high, medium, na}."""
    pc = str(record.get("product_class") or "").strip().lower()
    notes: list[str] = []

    if not pc:
        return "na", ["missing product_class"]
    if record.get("is_yieldboost"):
        return "na", [f"yieldboost income product (v1 not modeled)"]
    if pc in CLASS_NA:
        return "na", [f"product_class={pc} (v1 skipped)"]

    beta = record.get("beta")
    if not _isfin(beta):
        return "na", ["missing beta"]
    if anchor is None:
        return "na", ["missing anchor"]
    nav_anchor = anchor.get("nav_close")
    und_anchor = anchor.get("und_close")
    if not _isfin(nav_anchor) or float(nav_anchor) <= 0:
        return "na", ["missing nav_close in anchor"]
    if not _isfin(und_anchor) or float(und_anchor) <= 0:
        return "na", ["missing und_close in anchor"]
    if und_spot_age_sec is None:
        return "na", ["missing underlying spot"]

    if und_spot_age_sec > SPOT_FRESH_SECONDS:
        notes.append(f"spot stale {int(und_spot_age_sec)}s")

    if pc in CLASS_HIGH_CONFIDENCE:
        return ("medium" if notes else "high"), notes
    if pc in CLASS_MEDIUM_CONFIDENCE:
        notes.append(f"product_class={pc}")
        return "medium", notes
    # Unknown product_class: be conservative.
    notes.append(f"product_class={pc}")
    return "medium", notes


# ---------------------------------------------------------------------------
# Single-symbol forecast
# ---------------------------------------------------------------------------

def build_forecast(
    record: dict,
    anchor: dict | None,
    options_cache: dict,
    ts_utc: datetime,
) -> ForecastRecord | None:
    sym = str(record.get("symbol") or "").upper()
    if not sym:
        return None
    pc = (str(record.get("product_class") or "").strip().lower() or None)
    und = str(record.get("underlying") or "").upper() or None

    options_symbols = options_cache.get("symbols") or {}
    und_entry = options_symbols.get(und) if und else None
    und_spot_t = None
    und_spot_age = None
    if und_entry:
        sp = und_entry.get("spot")
        age = und_entry.get("cache_age_seconds")
        if _isfin(sp) and float(sp) > 0:
            und_spot_t = float(sp)
            und_spot_age = float(age) if _isfin(age) else None

    etf_entry = options_symbols.get(sym)
    etf_last = None
    etf_last_ts = None
    if etf_entry:
        sp = etf_entry.get("spot")
        if _isfin(sp) and float(sp) > 0:
            etf_last = float(sp)
            etf_last_ts = etf_entry.get("updated_at")

    confidence, notes = _classify_confidence(record, anchor or None, und_spot_age)

    beta = float(record["beta"]) if _isfin(record.get("beta")) else None
    ter_daily = _ter_daily_for(sym)

    nav_anchor = anchor.get("nav_close") if anchor else None
    und_anchor = anchor.get("und_close") if anchor else None
    nav_anchor_date = anchor.get("as_of_date") if anchor else None

    nav_hat: float | None = None
    premium_bp: float | None = None
    if (
        confidence != "na"
        and beta is not None
        and _isfin(nav_anchor)
        and _isfin(und_anchor)
        and _isfin(und_spot_t)
        and float(nav_anchor) > 0
        and float(und_anchor) > 0
        and float(und_spot_t) > 0
    ):
        try:
            r_und = math.log(float(und_spot_t) / float(und_anchor))
            nav_hat = compute_nav_hat(float(nav_anchor), beta, r_und, ter_daily)
            if etf_last is not None and nav_hat > 0:
                premium_bp = (etf_last - nav_hat) / nav_hat * 1.0e4
        except (ValueError, ZeroDivisionError) as e:
            confidence = "na"
            notes.append(f"math failure: {e}")
            nav_hat = None
            premium_bp = None

    ts_iso = ts_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    return ForecastRecord(
        ts=ts_iso,
        symbol=sym,
        model=MODEL,
        confidence=confidence,
        product_class=pc,
        und_symbol=und,
        und_spot_t=round(und_spot_t, 6) if _isfin(und_spot_t) else None,
        und_spot_anchor=round(float(und_anchor), 6) if _isfin(und_anchor) else None,
        und_anchor_date=nav_anchor_date,
        und_spot_age_sec=round(und_spot_age, 1) if _isfin(und_spot_age) else None,
        beta=round(beta, 6) if beta is not None else None,
        ter_daily=round(ter_daily, 8),
        nav_anchor=round(float(nav_anchor), 6) if _isfin(nav_anchor) else None,
        nav_anchor_date=nav_anchor_date,
        nav_hat=round(nav_hat, 6) if nav_hat is not None else None,
        etf_last=round(etf_last, 6) if etf_last is not None else None,
        etf_last_ts=etf_last_ts,
        premium_bp=round(premium_bp, 2) if premium_bp is not None else None,
        notes=_compose_notes(notes),
    )


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Run the delta_v1 NAV forecaster.")
    parser.add_argument("--dashboard-data", default=str(DASHBOARD_DATA_PATH))
    parser.add_argument("--anchors", default=str(ANCHORS_PATH))
    parser.add_argument("--options-cache", default=str(OPTIONS_CACHE_PATH))
    parser.add_argument("--out-snapshot", default=None,
                        help="Override the per-day snapshot JSONL path")
    parser.add_argument("--out-latest", default=str(LATEST_PATH))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    dashboard = _load_json(Path(args.dashboard_data))
    options_cache = _load_json(Path(args.options_cache))
    anchors = _load_json(Path(args.anchors))

    records = dashboard.get("records") or dashboard.get("rows") or []
    by_anchor = anchors.get("by_symbol") or {}

    ts = datetime.now(UTC)
    snap_path = (
        Path(args.out_snapshot)
        if args.out_snapshot
        else SNAPSHOT_DIR / f"{ts.date().isoformat()}.jsonl"
    )

    out_records: list[dict] = []
    latest_map: dict[str, dict] = {}
    counts = {"high": 0, "medium": 0, "na": 0}
    for rec in records:
        sym = str(rec.get("symbol") or "").upper()
        if not sym:
            continue
        try:
            f = build_forecast(rec, by_anchor.get(sym), options_cache, ts)
        except Exception as e:  # pragma: no cover - defensive
            LOGGER.warning("forecast %s failed: %s", sym, e)
            continue
        if f is None:
            continue
        d = asdict(f)
        out_records.append(d)
        latest_map[sym] = d
        counts[f.confidence] = counts.get(f.confidence, 0) + 1

    if out_records:
        _append_jsonl(snap_path, out_records)

    payload = {
        "build_time": ts.isoformat().replace("+00:00", "Z"),
        "model": MODEL,
        "anchor_date": anchors.get("as_of_date"),
        "anchor_symbols": len(by_anchor),
        "by_symbol": latest_map,
    }
    _atomic_write_json(Path(args.out_latest), payload)

    LOGGER.info(
        "forecast_nav: wrote %d rows (high=%d medium=%d na=%d) snapshot=%s anchor_date=%s",
        len(out_records), counts["high"], counts["medium"], counts["na"],
        snap_path, anchors.get("as_of_date"),
    )


if __name__ == "__main__":
    main()

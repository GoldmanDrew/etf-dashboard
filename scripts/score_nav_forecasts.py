#!/usr/bin/env python3
"""Stage 2 of the NAV-forecast pipeline: score yesterday's forecasts against the
freshly-ingested official NAV, refresh rolling accuracy stats, and write the
anchor file the forecaster will use tomorrow.

Run order:
    1. ``ingest_etf_metrics.py``      writes data/etf_metrics_latest.json
    2. ``score_nav_forecasts.py``     <-- this script
       reads:   etf_metrics_latest.json
                dashboard_data.json
                nav_forecasts/snapshots/<TARGET>.jsonl
                nav_forecasts/realized/*.jsonl   (rolling window)
       writes:  nav_forecasts/realized/<TARGET>.jsonl  (one row per scored symbol)
                nav_forecasts/_metrics_daily.json     (per-symbol rolling stats)
                nav_forecasts/_history_panel.json     (60-trading-day series)
                nav_forecasts/_anchors.json           (NEW anchors for next session)

Underlying close prices for the new anchors come from a single yfinance batch
download. yfinance is already a project dependency (see
``ingest_etf_metrics.fetch_close_prices_batch``), so we don't add anything new.
Disable with ``NAV_FORECAST_DISABLE_YFINANCE=1`` for offline/test runs.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import statistics
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("score_nav_forecasts")

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
NAV_DIR = DATA_DIR / "nav_forecasts"
SNAPSHOT_DIR = NAV_DIR / "snapshots"
REALIZED_DIR = NAV_DIR / "realized"
ANCHORS_PATH = NAV_DIR / "_anchors.json"
METRICS_DAILY_PATH = NAV_DIR / "_metrics_daily.json"
HISTORY_PANEL_PATH = NAV_DIR / "_history_panel.json"
LATEST_PATH = NAV_DIR / "_latest.json"
ETF_METRICS_LATEST_PATH = DATA_DIR / "etf_metrics_latest.json"
DASHBOARD_DATA_PATH = DATA_DIR / "dashboard_data.json"

HISTORY_TRADING_DAYS = 60


# ---------------------------------------------------------------------------
# Small file/JSON helpers
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


def _read_jsonl(p: Path) -> list[dict]:
    if not p.exists():
        return []
    out: list[dict] = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def _atomic_write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), allow_nan=False, sort_keys=True)
    tmp.replace(path)


# ---------------------------------------------------------------------------
# Snapshot collapsing + per-day scoring
# ---------------------------------------------------------------------------

def collapse_last_per_symbol(snapshots: list[dict]) -> dict[str, dict]:
    """Pick the latest forecast per symbol (by ``ts`` ISO string)."""
    out: dict[str, dict] = {}
    for r in snapshots:
        sym = str(r.get("symbol") or "").upper()
        if not sym:
            continue
        ts = str(r.get("ts") or "")
        cur = out.get(sym)
        if cur is None or str(cur.get("ts") or "") <= ts:
            out[sym] = r
    return out


def score_one_day(
    snap_rows: list[dict],
    metrics_latest: dict,
    target: date,
) -> list[dict]:
    """Match each end-of-day forecast against today's official NAV."""
    by_sym = collapse_last_per_symbol(snap_rows)
    metrics_by_sym = metrics_latest.get("by_symbol") or {}
    out: list[dict] = []
    target_iso = target.isoformat()

    for sym, snap in sorted(by_sym.items()):
        if (snap.get("confidence") or "na") == "na":
            continue
        nav_hat = snap.get("nav_hat")
        if not _isfin(nav_hat) or float(nav_hat) <= 0:
            continue
        m = metrics_by_sym.get(sym)
        if not m:
            continue
        nav_actual = m.get("nav")
        close_actual = m.get("close_price")
        date_str = m.get("date") or target_iso
        if not _isfin(nav_actual) or float(nav_actual) <= 0:
            continue
        # Defensive: only score on rows whose NAV stamp matches the target
        # date. If etf_metrics_latest is older than expected we skip (rather
        # than score against a wrong day's NAV).
        if str(date_str) != target_iso:
            continue

        nav_actual_f = float(nav_actual)
        nav_hat_f = float(nav_hat)
        err = nav_hat_f - nav_actual_f
        err_bp = err / nav_actual_f * 1.0e4
        out.append({
            "date": date_str,
            "symbol": sym,
            "model": snap.get("model"),
            "product_class": snap.get("product_class"),
            "nav_hat_close": round(nav_hat_f, 6),
            "nav_actual": round(nav_actual_f, 6),
            "close_actual": round(float(close_actual), 6) if _isfin(close_actual) else None,
            "err_bp": round(err_bp, 3),
            "abs_err_bp": round(abs(err_bp), 3),
            "premium_bp_at_snap": snap.get("premium_bp"),
            "ts_snap": snap.get("ts"),
        })
    return out


# ---------------------------------------------------------------------------
# Rolling stats
# ---------------------------------------------------------------------------

def _walk_realized(realized_dir: Path, max_files: int) -> dict[str, list[dict]]:
    files = sorted(realized_dir.glob("*.jsonl"))[-max_files:] if realized_dir.exists() else []
    by_sym: dict[str, list[dict]] = {}
    for fp in files:
        for r in _read_jsonl(fp):
            sym = str(r.get("symbol") or "").upper()
            if not sym:
                continue
            by_sym.setdefault(sym, []).append(r)
    for sym in by_sym:
        by_sym[sym].sort(key=lambda x: str(x.get("date") or ""))
    return by_sym


def update_metrics_daily(realized_dir: Path) -> dict[str, dict]:
    """Roll up per-symbol accuracy stats from the last ``HISTORY_TRADING_DAYS``."""
    by_sym = _walk_realized(realized_dir, HISTORY_TRADING_DAYS + 5)
    summary: dict[str, dict] = {}
    for sym, rows in by_sym.items():
        rows = rows[-HISTORY_TRADING_DAYS:]
        abs_err = [float(r["abs_err_bp"]) for r in rows if _isfin(r.get("abs_err_bp"))]
        signed_err = [float(r["err_bp"]) for r in rows if _isfin(r.get("err_bp"))]
        if not abs_err:
            continue
        n = len(abs_err)
        within_10 = sum(1 for x in abs_err if x <= 10.0) / n
        within_25 = sum(1 for x in abs_err if x <= 25.0) / n
        last_5 = abs_err[-5:]
        last_20 = abs_err[-20:]
        summary[sym] = {
            "model": rows[-1].get("model"),
            "product_class": rows[-1].get("product_class"),
            "n": n,
            "median_abs_err_bp": round(statistics.median(abs_err), 2),
            "median_abs_err_bp_5d": round(statistics.median(last_5), 2),
            "median_abs_err_bp_20d": round(statistics.median(last_20), 2),
            "mean_signed_err_bp": (
                round(sum(signed_err) / len(signed_err), 2) if signed_err else None
            ),
            "hit_rate_within_10bp": round(within_10, 4),
            "hit_rate_within_25bp": round(within_25, 4),
            "last_date": rows[-1].get("date"),
        }
    return summary


def update_history_panel(realized_dir: Path) -> dict:
    by_sym = _walk_realized(realized_dir, HISTORY_TRADING_DAYS)
    out: dict[str, list[dict]] = {}
    for sym, rows in by_sym.items():
        out[sym] = [
            {
                "date": r.get("date"),
                "nav_actual": r.get("nav_actual"),
                "nav_hat_close": r.get("nav_hat_close"),
                "close": r.get("close_actual"),
                "err_bp": r.get("err_bp"),
            }
            for r in rows
        ]
    return {
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "window_trading_days": HISTORY_TRADING_DAYS,
        "by_symbol": out,
    }


# ---------------------------------------------------------------------------
# Anchor builder
# ---------------------------------------------------------------------------

def fetch_underlying_closes(underlyings: list[str], target: date) -> dict[str, float]:
    """Single yfinance batch call -> ``{ticker: close_on_target_or_most_recent_<=target}``."""
    if not underlyings:
        return {}
    if os.getenv("NAV_FORECAST_DISABLE_YFINANCE", "").lower() in ("1", "true", "yes"):
        LOGGER.info("yfinance disabled via env (NAV_FORECAST_DISABLE_YFINANCE)")
        return {}
    try:
        import yfinance as yf
    except Exception as e:  # pragma: no cover - defensive
        LOGGER.warning("yfinance unavailable: %s", e)
        return {}
    start = (target - timedelta(days=6)).isoformat()
    end = (target + timedelta(days=2)).isoformat()
    tickers = sorted({t for t in underlyings if t})
    try:
        raw = yf.download(
            tickers=tickers,
            start=start,
            end=end,
            interval="1d",
            auto_adjust=False,
            actions=False,
            group_by="ticker",
            threads=True,
            progress=False,
        )
    except Exception as e:
        LOGGER.warning("yfinance batch failed: %s", e)
        return {}
    if raw is None or getattr(raw, "empty", True):
        return {}

    target_str = target.isoformat()
    out: dict[str, float] = {}
    columns = getattr(raw, "columns", None)
    is_multi = bool(columns is not None and getattr(columns, "nlevels", 1) > 1)
    if is_multi:
        for sym in tickers:
            if sym not in columns.get_level_values(0):
                continue
            try:
                sub = raw[sym]
            except KeyError:
                continue
            if "Close" not in sub.columns:
                continue
            ser = sub["Close"].dropna()
            picked: float | None = None
            for idx, val in ser.items():
                idx_str = idx.date().isoformat() if hasattr(idx, "date") else str(idx)[:10]
                if idx_str > target_str:
                    break
                try:
                    picked = float(val)
                except (TypeError, ValueError):
                    continue
            if picked is not None and math.isfinite(picked) and picked > 0:
                out[sym.upper()] = picked
    else:
        if "Close" in raw.columns and len(tickers) == 1:
            ser = raw["Close"].dropna()
            picked: float | None = None
            for idx, val in ser.items():
                idx_str = idx.date().isoformat() if hasattr(idx, "date") else str(idx)[:10]
                if idx_str > target_str:
                    break
                try:
                    picked = float(val)
                except (TypeError, ValueError):
                    continue
            if picked is not None and math.isfinite(picked) and picked > 0:
                out[tickers[0].upper()] = picked
    return out


def build_anchors(
    records: list[dict],
    metrics_latest: dict,
    target: date,
    *,
    underlying_closes_fn=fetch_underlying_closes,
) -> dict:
    """Construct the anchor file the forecaster will use during the next session.

    For each symbol whose latest official NAV matches the target trading day:
      * record nav_close (from etf_metrics_latest)
      * record und_close from yfinance for the same date
      * carry product_class + beta + underlying so the forecaster doesn't have to
        re-infer them
    """
    by_metric = metrics_latest.get("by_symbol") or {}
    rec_by_sym: dict[str, dict] = {}
    needed_unds: set[str] = set()
    for rec in records:
        sym = str(rec.get("symbol") or "").upper()
        if not sym:
            continue
        rec_by_sym[sym] = rec
        und = str(rec.get("underlying") or "").upper()
        if und:
            needed_unds.add(und)
    und_closes = underlying_closes_fn(sorted(needed_unds), target)

    out_by_sym: dict[str, dict] = {}
    target_iso = target.isoformat()
    for sym, rec in sorted(rec_by_sym.items()):
        m = by_metric.get(sym)
        if not m:
            continue
        nav = m.get("nav")
        close = m.get("close_price")
        date_str = m.get("date")
        if not _isfin(nav) or float(nav) <= 0:
            continue
        # Only anchor on rows actually stamped at the target date - otherwise the
        # forecaster would compute deltas against a stale official NAV.
        if str(date_str) != target_iso:
            continue
        und = str(rec.get("underlying") or "").upper() or None
        beta = float(rec.get("beta")) if _isfin(rec.get("beta")) else None
        out_by_sym[sym] = {
            "as_of_date": str(date_str),
            "nav_close": round(float(nav), 6),
            "etf_close": round(float(close), 6) if _isfin(close) else None,
            "und_symbol": und,
            "und_close": (
                round(float(und_closes.get(und)), 6)
                if und and _isfin(und_closes.get(und))
                else None
            ),
            "beta": beta,
            "product_class": rec.get("product_class"),
        }
    return {
        "as_of_date": target_iso,
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "n_with_und_close": sum(1 for v in out_by_sym.values() if v["und_close"] is not None),
        "n_total": len(out_by_sym),
        "by_symbol": out_by_sym,
    }


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Score yesterday's NAV forecasts and refresh anchors.")
    parser.add_argument("--target-date", default=None,
                        help="YYYY-MM-DD; default = etf_metrics_latest.latest_date")
    parser.add_argument("--snapshot-dir", default=str(SNAPSHOT_DIR))
    parser.add_argument("--realized-dir", default=str(REALIZED_DIR))
    parser.add_argument("--metrics-out", default=str(METRICS_DAILY_PATH))
    parser.add_argument("--history-out", default=str(HISTORY_PANEL_PATH))
    parser.add_argument("--anchors-out", default=str(ANCHORS_PATH))
    parser.add_argument("--metrics-latest", default=str(ETF_METRICS_LATEST_PATH))
    parser.add_argument("--dashboard-data", default=str(DASHBOARD_DATA_PATH))
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    metrics_latest = _load_json(Path(args.metrics_latest))
    if args.target_date:
        target = datetime.strptime(args.target_date, "%Y-%m-%d").date()
    else:
        latest_str = metrics_latest.get("latest_date")
        if not latest_str:
            raise SystemExit("etf_metrics_latest.latest_date missing; pass --target-date")
        target = datetime.strptime(latest_str, "%Y-%m-%d").date()
    LOGGER.info("scoring target date: %s", target)

    snap_path = Path(args.snapshot_dir) / f"{target.isoformat()}.jsonl"
    snap_rows = _read_jsonl(snap_path)
    LOGGER.info("loaded %d snapshot rows from %s", len(snap_rows), snap_path)

    realized_rows = score_one_day(snap_rows, metrics_latest, target)
    realized_path = Path(args.realized_dir) / f"{target.isoformat()}.jsonl"
    realized_path.parent.mkdir(parents=True, exist_ok=True)
    with realized_path.open("w", encoding="utf-8") as f:
        for r in realized_rows:
            f.write(json.dumps(r, separators=(",", ":"), allow_nan=False, sort_keys=True))
            f.write("\n")
    LOGGER.info("wrote %d realized rows -> %s", len(realized_rows), realized_path)

    metrics_summary = update_metrics_daily(Path(args.realized_dir))
    _atomic_write_json(Path(args.metrics_out), {
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "window_trading_days": HISTORY_TRADING_DAYS,
        "by_symbol": metrics_summary,
    })

    history_panel = update_history_panel(Path(args.realized_dir))
    _atomic_write_json(Path(args.history_out), history_panel)

    dashboard = _load_json(Path(args.dashboard_data))
    records = dashboard.get("records") or dashboard.get("rows") or []
    anchors = build_anchors(records, metrics_latest, target)
    _atomic_write_json(Path(args.anchors_out), anchors)
    LOGGER.info(
        "wrote anchors as_of_date=%s symbols=%d (with und_close=%d)",
        anchors["as_of_date"], anchors["n_total"], anchors["n_with_und_close"],
    )


if __name__ == "__main__":
    main()

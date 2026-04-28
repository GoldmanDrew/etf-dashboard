#!/usr/bin/env python3
"""Stage 2 of the NAV-forecast pipeline.

Now that the forecaster (`forecast_nav.py`) emits one row per
(symbol, model), the scorer:

  1. Collapses the day's last forecast per (symbol, model) and scores
     each one against the freshly-ingested official NAV.
  2. Writes one realized line per (symbol, model, date) into
     ``nav_forecasts/realized/<DATE>.jsonl``.
  3. Rolls 60-trading-day accuracy stats per (symbol, model) into
     ``_metrics_daily.json`` and a per-model history panel into
     ``_history_panel.json``. For UI compatibility we also expose a
     ``by_symbol`` view containing the **default** model's stats / panel
     (the model the forecaster routed to the Stats card most recently).
  4. Builds the next session's ``_anchors.json`` (one yfinance batch
     call for the underlying closes) -- now also captures
     ``shares_outstanding`` so the holdings models can divide cleanly.

Disable yfinance for offline / unit-test runs with
``NAV_FORECAST_DISABLE_YFINANCE=1``.
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
# JSON helpers
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

def collapse_last_per_symbol_model(snapshots: list[dict]) -> dict[tuple[str, str], dict]:
    """Pick the latest forecast per (symbol, model) by ``ts`` ISO string.

    Older snapshots that pre-date the multi-model rollout have no ``model``
    field -- those are bucketed under ``delta_v1`` so the historical accuracy
    log keeps working.
    """
    out: dict[tuple[str, str], dict] = {}
    for r in snapshots:
        sym = str(r.get("symbol") or "").upper()
        if not sym:
            continue
        model = str(r.get("model") or "delta_v1")
        ts = str(r.get("ts") or "")
        key = (sym, model)
        cur = out.get(key)
        if cur is None or str(cur.get("ts") or "") <= ts:
            out[key] = r
    return out


# Backwards compat alias used by the older tests.
collapse_last_per_symbol = collapse_last_per_symbol_model


def score_one_day(
    snap_rows: list[dict],
    metrics_latest: dict,
    target: date,
) -> list[dict]:
    """Match each end-of-day (symbol, model) forecast against today's NAV."""
    by_key = collapse_last_per_symbol_model(snap_rows)
    metrics_by_sym = metrics_latest.get("by_symbol") or {}
    out: list[dict] = []
    target_iso = target.isoformat()

    for (sym, model), snap in sorted(by_key.items()):
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
        if str(date_str) != target_iso:
            continue

        nav_actual_f = float(nav_actual)
        nav_hat_f = float(nav_hat)
        err = nav_hat_f - nav_actual_f
        err_bp = err / nav_actual_f * 1.0e4
        out.append({
            "date": date_str,
            "symbol": sym,
            "model": model,
            "is_default": bool(snap.get("is_default")),
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

def _walk_realized(
    realized_dir: Path, max_files: int,
) -> dict[tuple[str, str], list[dict]]:
    files = sorted(realized_dir.glob("*.jsonl"))[-max_files:] if realized_dir.exists() else []
    by_key: dict[tuple[str, str], list[dict]] = {}
    for fp in files:
        for r in _read_jsonl(fp):
            sym = str(r.get("symbol") or "").upper()
            if not sym:
                continue
            model = str(r.get("model") or "delta_v1")
            by_key.setdefault((sym, model), []).append(r)
    for key in by_key:
        by_key[key].sort(key=lambda x: str(x.get("date") or ""))
    return by_key


def _stats_block(rows: list[dict]) -> dict | None:
    abs_err = [float(r["abs_err_bp"]) for r in rows if _isfin(r.get("abs_err_bp"))]
    signed_err = [float(r["err_bp"]) for r in rows if _isfin(r.get("err_bp"))]
    if not abs_err:
        return None
    n = len(abs_err)
    within_10 = sum(1 for x in abs_err if x <= 10.0) / n
    within_25 = sum(1 for x in abs_err if x <= 25.0) / n
    last_5 = abs_err[-5:]
    last_20 = abs_err[-20:]
    return {
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


def update_metrics_daily(
    realized_dir: Path,
    *,
    default_model_for_symbol: dict[str, str] | None = None,
) -> dict:
    """Roll up per-(symbol, model) accuracy stats from the rolling window.

    Returns a payload with both:
      * ``by_symbol_models[SYM][MODEL]`` -- the full A/B grid
      * ``by_symbol[SYM]`` -- stats for the model the forecaster currently
        routes as default (so the existing Stats-tab card keeps working).
    """
    default_model_for_symbol = default_model_for_symbol or {}
    by_key = _walk_realized(realized_dir, HISTORY_TRADING_DAYS + 5)
    by_symbol_models: dict[str, dict[str, dict]] = {}
    for (sym, model), rows in by_key.items():
        rows = rows[-HISTORY_TRADING_DAYS:]
        block = _stats_block(rows)
        if block is None:
            continue
        by_symbol_models.setdefault(sym, {})[model] = block

    by_symbol_default: dict[str, dict] = {}
    default_model_resolved: dict[str, str] = {}
    for sym, by_model in by_symbol_models.items():
        chosen = default_model_for_symbol.get(sym)
        if chosen and chosen in by_model:
            by_symbol_default[sym] = by_model[chosen]
            default_model_resolved[sym] = chosen
            continue
        # Fallback ordering matches forecast_nav.select_default_model preference.
        for m in (
            "yieldboost_putspread_v1",
            "delta_v3_swap_mark",
            "delta_v2_ito",
            "delta_v1",
        ):
            if m in by_model:
                by_symbol_default[sym] = by_model[m]
                default_model_resolved[sym] = m
                break
    return {
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "window_trading_days": HISTORY_TRADING_DAYS,
        "by_symbol": by_symbol_default,
        "by_symbol_models": by_symbol_models,
        "default_model_for_symbol": default_model_resolved,
    }


def update_history_panel(
    realized_dir: Path,
    *,
    default_model_for_symbol: dict[str, str] | None = None,
) -> dict:
    default_model_for_symbol = default_model_for_symbol or {}
    by_key = _walk_realized(realized_dir, HISTORY_TRADING_DAYS)
    by_symbol_models: dict[str, dict[str, list[dict]]] = {}
    for (sym, model), rows in by_key.items():
        slim = [
            {
                "date": r.get("date"),
                "nav_actual": r.get("nav_actual"),
                "nav_hat_close": r.get("nav_hat_close"),
                "close": r.get("close_actual"),
                "err_bp": r.get("err_bp"),
            }
            for r in rows
        ]
        by_symbol_models.setdefault(sym, {})[model] = slim

    by_symbol_default: dict[str, list[dict]] = {}
    for sym, by_model in by_symbol_models.items():
        chosen = default_model_for_symbol.get(sym)
        if chosen and chosen in by_model:
            by_symbol_default[sym] = by_model[chosen]
            continue
        for m in (
            "yieldboost_putspread_v1",
            "delta_v3_swap_mark",
            "delta_v2_ito",
            "delta_v1",
        ):
            if m in by_model:
                by_symbol_default[sym] = by_model[m]
                break
    return {
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "window_trading_days": HISTORY_TRADING_DAYS,
        "by_symbol": by_symbol_default,
        "by_symbol_models": by_symbol_models,
    }


# ---------------------------------------------------------------------------
# Anchor builder (now captures shares_outstanding)
# ---------------------------------------------------------------------------

def fetch_underlying_closes(underlyings: list[str], target: date) -> dict[str, float]:
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
    """Construct the anchor file the forecaster will use during the next session."""
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
        shares = m.get("shares_outstanding")
        if not _isfin(nav) or float(nav) <= 0:
            continue
        if str(date_str) != target_iso:
            continue
        und = str(rec.get("underlying") or "").upper() or None
        beta = float(rec.get("beta")) if _isfin(rec.get("beta")) else None
        out_by_sym[sym] = {
            "as_of_date": str(date_str),
            "nav_close": round(float(nav), 6),
            "etf_close": round(float(close), 6) if _isfin(close) else None,
            "shares_outstanding": (
                round(float(shares), 4) if _isfin(shares) and float(shares) > 0 else None
            ),
            "und_symbol": und,
            "und_close": (
                round(float(und_closes.get(und)), 6)
                if und and _isfin(und_closes.get(und))
                else None
            ),
            "beta": beta,
            "product_class": rec.get("product_class"),
            "is_yieldboost": bool(rec.get("is_yieldboost")),
        }
    return {
        "as_of_date": target_iso,
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "n_with_und_close": sum(1 for v in out_by_sym.values() if v["und_close"] is not None),
        "n_with_shares": sum(1 for v in out_by_sym.values() if v["shares_outstanding"] is not None),
        "n_total": len(out_by_sym),
        "by_symbol": out_by_sym,
    }


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def _load_default_model_for_symbol(latest_path: Path) -> dict[str, str]:
    payload = _load_json(latest_path)
    direct = payload.get("default_model_for_symbol")
    if isinstance(direct, dict) and direct:
        return {str(k).upper(): str(v) for k, v in direct.items()}
    by_sym = payload.get("by_symbol") or {}
    return {
        str(sym).upper(): str(row.get("model"))
        for sym, row in by_sym.items()
        if row.get("model")
    }


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
    parser.add_argument("--latest-forecast", default=str(LATEST_PATH))
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
    by_model_count: dict[str, int] = {}
    for r in realized_rows:
        m = r.get("model") or "?"
        by_model_count[m] = by_model_count.get(m, 0) + 1
    LOGGER.info(
        "wrote %d realized rows -> %s by_model=%s",
        len(realized_rows), realized_path, by_model_count,
    )

    default_model_for_symbol = _load_default_model_for_symbol(Path(args.latest_forecast))

    metrics_payload = update_metrics_daily(
        Path(args.realized_dir), default_model_for_symbol=default_model_for_symbol,
    )
    _atomic_write_json(Path(args.metrics_out), metrics_payload)

    history_payload = update_history_panel(
        Path(args.realized_dir), default_model_for_symbol=default_model_for_symbol,
    )
    _atomic_write_json(Path(args.history_out), history_payload)

    dashboard = _load_json(Path(args.dashboard_data))
    records = dashboard.get("records") or dashboard.get("rows") or []
    anchors = build_anchors(records, metrics_latest, target)
    _atomic_write_json(Path(args.anchors_out), anchors)
    LOGGER.info(
        "wrote anchors as_of_date=%s symbols=%d (with und_close=%d, with shares=%d)",
        anchors["as_of_date"], anchors["n_total"],
        anchors["n_with_und_close"], anchors["n_with_shares"],
    )


if __name__ == "__main__":
    main()

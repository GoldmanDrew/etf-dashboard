#!/usr/bin/env python3
"""Repair historical NAV-forecast realized rows affected by stale spot quotes.

The live forecaster now refuses to feed stale ``options_cache`` spots into the
beta models.  This migration applies the same policy to already-scored history
using only files committed in this repo:

* ``data/nav_forecasts/snapshots/YYYY-MM-DD.jsonl`` -- model inputs at snap time
* ``data/etf_metrics_daily.json`` -- same/next-session underlying closes
* ``data/nav_forecasts/realized/**/*.jsonl`` -- realized rows to rewrite

Only beta-based rows (``delta_v1`` / ``delta_v2_ito``) whose snapshot had stale
spot diagnostics are recalculated.  Derived rollups
(``_metrics_daily.json`` / ``_history_panel.json``) are rebuilt afterwards.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
from pathlib import Path
from typing import Any

import forecast_nav as fn
import score_nav_forecasts as scorer

LOGGER = logging.getLogger("repair_nav_forecast_stale_spots")

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
NAV_DIR = DATA_DIR / "nav_forecasts"

BETA_MODELS = {"delta_v1", "delta_v2_ito"}


def _isfin(x: Any) -> bool:
    try:
        f = float(x)
        return math.isfinite(f)
    except (TypeError, ValueError):
        return False


def _f(x: Any) -> float | None:
    return float(x) if _isfin(x) else None


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return out


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, separators=(",", ":"), allow_nan=False, sort_keys=True))
            f.write("\n")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), allow_nan=False, sort_keys=True)
    tmp.replace(path)


def load_metrics_by_key(metrics_daily_path: Path) -> dict[tuple[str, str], dict]:
    payload = _read_json(metrics_daily_path)
    rows = payload.get("rows") or []
    out: dict[tuple[str, str], dict] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        date_s = str(row.get("date") or "")
        sym = str(row.get("ticker") or "").upper()
        if not date_s or not sym:
            continue
        out[(date_s, sym)] = row
    return out


def load_latest_snapshots(snapshot_path: Path) -> dict[tuple[str, str], dict]:
    """Return the latest snapshot row per ``(symbol, model)`` for one date."""
    out: dict[tuple[str, str], dict] = {}
    for row in _read_jsonl(snapshot_path):
        sym = str(row.get("symbol") or "").upper()
        model = str(row.get("model") or "delta_v1")
        if not sym:
            continue
        key = (sym, model)
        cur = out.get(key)
        if cur is None or str(cur.get("ts") or "") <= str(row.get("ts") or ""):
            out[key] = row
    return out


def stale_spot_snapshot(row: dict) -> bool:
    notes = str(row.get("notes") or "").lower()
    if "spot stale" in notes:
        return True
    age = _f(row.get("und_spot_age_sec"))
    return age is not None and age > fn.SPOT_FRESH_SECONDS


def recompute_beta_nav_hat(
    snapshot: dict,
    target_date: str,
    metrics_row: dict | None,
) -> float | None:
    """Recompute one historical beta-model ``nav_hat`` from stored inputs."""
    model = str(snapshot.get("model") or "")
    if model not in BETA_MODELS:
        return None
    if not stale_spot_snapshot(snapshot):
        return None

    nav_anchor = _f(snapshot.get("nav_anchor"))
    beta = _f(snapshot.get("beta"))
    und_anchor = _f(snapshot.get("und_spot_anchor"))
    ter_daily = _f(snapshot.get("ter_daily")) or 0.0
    if nav_anchor is None or nav_anchor <= 0:
        return None
    if beta is None:
        return None
    if und_anchor is None or und_anchor <= 0:
        return None

    nav_anchor_date = snapshot.get("nav_anchor_date")
    fallback_metric = dict(metrics_row or {})
    # The scorer settles against target_date.  If a historical metrics row is
    # passed in without a date, pin it so forecast_nav's date gate can reason.
    fallback_metric.setdefault("date", target_date)
    und_spot_t = fn._metrics_fallback_underlying_price(
        fallback_metric,
        str(nav_anchor_date) if nav_anchor_date else None,
        und_anchor,
    )
    if und_spot_t is None or und_spot_t <= 0:
        return None

    try:
        r_und = math.log(float(und_spot_t) / float(und_anchor))
        nav_hat = fn.compute_v1(nav_anchor, beta, r_und, ter_daily)
        if model == "delta_v2_ito":
            sigma = _f(snapshot.get("sigma_annual"))
            dt_years = _f(snapshot.get("dt_years"))
            if (
                sigma is not None
                and sigma > 0
                and dt_years is not None
                and dt_years > 0
            ):
                drag_logret = -((beta ** 2 - beta) / 2.0) * (sigma ** 2) * dt_years
            else:
                drag_logret = _f(snapshot.get("vol_drag_logret")) or 0.0
            nav_hat *= math.exp(drag_logret)
    except (ValueError, ZeroDivisionError, OverflowError):
        return None
    if not math.isfinite(nav_hat) or nav_hat <= 0:
        return None
    return round(nav_hat, 6)


def repair_realized_rows(
    rows: list[dict],
    snapshots: dict[tuple[str, str], dict],
    metrics_by_key: dict[tuple[str, str], dict],
) -> tuple[list[dict], int]:
    repaired = 0
    out: list[dict] = []
    for row in rows:
        new_row = dict(row)
        date_s = str(new_row.get("date") or "")
        sym = str(new_row.get("symbol") or "").upper()
        model = str(new_row.get("model") or "delta_v1")
        if date_s and sym and model in BETA_MODELS:
            snap = snapshots.get((sym, model))
            metrics_row = metrics_by_key.get((date_s, sym))
            nav_hat = recompute_beta_nav_hat(snap or {}, date_s, metrics_row)
            if nav_hat is not None and _f(new_row.get("nav_hat_close")) != nav_hat:
                nav_actual = _f(new_row.get("nav_actual"))
                close_actual = _f(new_row.get("close_actual"))
                new_row["nav_hat_close"] = nav_hat
                if nav_actual is not None and nav_actual > 0:
                    err = nav_hat - nav_actual
                    err_bp = err / nav_actual * 1.0e4
                    new_row["err_bp"] = round(err_bp, 3)
                    new_row["abs_err_bp"] = round(abs(err_bp), 3)
                if close_actual is not None and nav_hat > 0:
                    new_row["premium_bp_at_snap"] = round(
                        (close_actual - nav_hat) / nav_hat * 1.0e4,
                        2,
                    )
                repaired += 1
        out.append(new_row)
    return out, repaired


def repair_history(
    *,
    snapshot_dir: Path,
    realized_dir: Path,
    metrics_daily_path: Path,
    metrics_out: Path,
    history_out: Path,
    latest_forecast_path: Path,
    apply: bool,
) -> dict:
    metrics_by_key = load_metrics_by_key(metrics_daily_path)
    if not metrics_by_key:
        raise SystemExit(f"no metrics rows loaded from {metrics_daily_path}")

    if apply:
        scorer.flatten_nested_realized_jsonl(realized_dir)

    files = scorer._iter_realized_jsonl_paths(realized_dir)
    changed_files = 0
    repaired_rows = 0
    per_file: dict[str, int] = {}
    for realized_path in files:
        date_s = realized_path.stem
        snapshot_path = snapshot_dir / f"{date_s}.jsonl"
        if not snapshot_path.exists():
            continue
        snapshots = load_latest_snapshots(snapshot_path)
        rows = _read_jsonl(realized_path)
        fixed, n = repair_realized_rows(rows, snapshots, metrics_by_key)
        if n:
            changed_files += 1
            repaired_rows += n
            per_file[realized_path.name] = n
            if apply:
                # Always write the canonical top-level realized file; the scorer
                # reads by basename, and flatten_nested_realized_jsonl moves older
                # nested copies here before this loop.
                _write_jsonl(realized_dir / realized_path.name, fixed)

    default_model_for_symbol = scorer._load_default_model_for_symbol(latest_forecast_path)
    metrics_payload = scorer.update_metrics_daily(
        realized_dir, default_model_for_symbol=default_model_for_symbol,
    )
    history_payload = scorer.update_history_panel(
        realized_dir, default_model_for_symbol=default_model_for_symbol,
    )
    if apply:
        _write_json(metrics_out, metrics_payload)
        _write_json(history_out, history_payload)

    return {
        "apply": apply,
        "realized_files_seen": len(files),
        "realized_files_changed": changed_files,
        "repaired_rows": repaired_rows,
        "per_file": per_file,
        "metrics_symbols": len(metrics_payload.get("by_symbol") or {}),
        "history_symbols": len(history_payload.get("by_symbol") or {}),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-dir", default=str(NAV_DIR / "snapshots"))
    parser.add_argument("--realized-dir", default=str(NAV_DIR / "realized"))
    parser.add_argument("--metrics-daily", default=str(DATA_DIR / "etf_metrics_daily.json"))
    parser.add_argument("--metrics-out", default=str(NAV_DIR / "_metrics_daily.json"))
    parser.add_argument("--history-out", default=str(NAV_DIR / "_history_panel.json"))
    parser.add_argument("--latest-forecast", default=str(NAV_DIR / "_latest.json"))
    parser.add_argument("--apply", action="store_true", help="rewrite realized files and rollups")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    summary = repair_history(
        snapshot_dir=Path(args.snapshot_dir),
        realized_dir=Path(args.realized_dir),
        metrics_daily_path=Path(args.metrics_daily),
        metrics_out=Path(args.metrics_out),
        history_out=Path(args.history_out),
        latest_forecast_path=Path(args.latest_forecast),
        apply=bool(args.apply),
    )
    LOGGER.info("repair summary: %s", summary)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()

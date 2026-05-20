#!/usr/bin/env python3
"""Reconcile intraday LETF rebalance estimates against the realised EOD flow.

This is the T+1 scoring loop:
  * For each completed trading day ``T`` we look at the per-trading-day
    snapshot in ``data/letf_rebalance_flows_intraday_snapshots/T.jsonl`` and
    pick the *latest* line whose ``minutes_to_close <= MAX_MINUTES_TO_CLOSE``
    (default 15). That's our "best intraday estimate" of the close print.
  * The realised close-flow comes from ``data/letf_rebalance_flows_latest.json``
    (or its parquet) for the same trading day, written by the EOD pipeline.
  * Per underlying we record signed error (estimate - realised) absolute and
    pct, and we roll a per-underlying bias used by ``build_letf_intraday_flows.py``
    once we have ``MIN_OBS`` reconciled days.

Outputs:
  * ``data/letf_intraday_flow_realized.jsonl`` -- append-only, one record per
    (date, underlying) reconciliation.
  * ``data/letf_intraday_flow_metrics.json``  -- per-underlying rolled stats
    (mean signed error %, MAE %, n_observations).

Designed to run inside ``update-etf-metrics.yml`` after the EOD flow build.
Tolerant of missing inputs (no snapshot dir, no realised JSON) -- exits 0
with a no-op log line.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("score_intraday_flows")

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
SNAPSHOT_DIR = DATA_DIR / "letf_rebalance_flows_intraday_snapshots"
REALIZED_JSON = DATA_DIR / "letf_rebalance_flows_latest.json"
REALIZED_LOG = DATA_DIR / "letf_intraday_flow_realized.jsonl"
METRICS_JSON = DATA_DIR / "letf_intraday_flow_metrics.json"

MAX_MINUTES_TO_CLOSE = 15
ROLLING_WINDOW = 30
MIN_OBS = 5


def _f(v: object) -> float | None:
    try:
        x = float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows


def _append_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, separators=(",", ":"), allow_nan=False, sort_keys=True))
            f.write("\n")


def list_unscored_dates(snapshot_dir: Path, scored: set[str]) -> list[str]:
    if not snapshot_dir.exists():
        return []
    out: list[str] = []
    for p in sorted(snapshot_dir.glob("*.jsonl")):
        d = p.stem
        if d in scored:
            continue
        # Skip today (no realised flow yet) -- only score days strictly older than today.
        try:
            datetime.strptime(d, "%Y-%m-%d")
        except ValueError:
            continue
        out.append(d)
    return out


def pick_close_estimate(snapshot_path: Path, *, max_minutes: int = MAX_MINUTES_TO_CLOSE) -> dict[str, Any] | None:
    """Return the latest snapshot whose minutes_to_close <= max_minutes."""
    rows = _read_jsonl(snapshot_path)
    if not rows:
        return None
    candidates = [
        r for r in rows
        if isinstance(r.get("minutes_to_close"), (int, float)) and 0 <= int(r["minutes_to_close"]) <= max_minutes
    ]
    if not candidates:
        # Fall back to the very last build of the day if no near-close snapshot exists.
        return rows[-1]
    candidates.sort(key=lambda r: int(r.get("minutes_to_close") or 0))
    return candidates[0]


def load_realized_for_date(date_iso: str, realized_json: Path = REALIZED_JSON) -> dict[str, dict[str, Any]]:
    """Per-underlying realised net MOC dollars for ``date_iso``.

    Reads ``letf_rebalance_flows_latest.json``: each by_underlying entry has
    its own ``date`` (post-Phase A), so we filter to the matching trading day.
    Returns ``{}`` if no underlying matches that date.
    """
    if not realized_json.exists():
        return {}
    try:
        payload = json.loads(realized_json.read_text(encoding="utf-8"))
    except Exception as exc:
        LOGGER.warning("failed to parse %s: %s", realized_json, exc)
        return {}
    by_und = payload.get("by_underlying") or {}
    out: dict[str, dict[str, Any]] = {}
    for sym, row in by_und.items():
        if str(row.get("date") or "") != date_iso:
            continue
        out[str(sym).upper()] = row
    return out


def reconcile_one_day(date_iso: str, snapshot_dir: Path, realized_json: Path) -> list[dict[str, Any]]:
    snapshot_path = snapshot_dir / f"{date_iso}.jsonl"
    est = pick_close_estimate(snapshot_path)
    if not est:
        LOGGER.info("no usable snapshot for %s", date_iso)
        return []
    realised = load_realized_for_date(date_iso, realized_json)
    if not realised:
        LOGGER.info("no realised aggregate for %s yet (EOD probably not built)", date_iso)
        return []

    out: list[dict[str, Any]] = []
    by_und_est = est.get("by_underlying") or {}
    for und, real_row in realised.items():
        est_row = by_und_est.get(und)
        if not est_row:
            continue
        est_dollars = _f(est_row.get("estimated_net_close_rebalance_dollars"))
        real_dollars = _f(real_row.get("net_moc_dollars"))
        if est_dollars is None or real_dollars is None:
            continue
        signed_error = est_dollars - real_dollars
        signed_error_pct = signed_error / real_dollars if abs(real_dollars) > 0 else None
        out.append({
            "trading_date": date_iso,
            "underlying": und,
            "estimate_dollars": round(est_dollars, 2),
            "realized_dollars": round(real_dollars, 2),
            "signed_error_dollars": round(signed_error, 2),
            "signed_error_pct": round(signed_error_pct, 6) if signed_error_pct is not None else None,
            "minutes_to_close_at_estimate": est.get("minutes_to_close"),
            "estimate_as_of": est.get("as_of"),
        })
    return out


def roll_metrics(realized_log: Path, *, window: int = ROLLING_WINDOW, min_obs: int = MIN_OBS) -> dict[str, Any]:
    """Per-underlying rolling stats over the last ``window`` reconciled days."""
    rows = _read_jsonl(realized_log)
    if not rows:
        return {"build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"), "by_underlying": {}}
    rows.sort(key=lambda r: r.get("trading_date") or "")
    by_und: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        und = str(r.get("underlying") or "").upper()
        if not und:
            continue
        by_und.setdefault(und, []).append(r)

    out: dict[str, dict[str, Any]] = {}
    for und, lst in by_und.items():
        recent = lst[-window:]
        signed_pcts = [r.get("signed_error_pct") for r in recent if r.get("signed_error_pct") is not None]
        if not signed_pcts:
            continue
        n = len(signed_pcts)
        if n < min_obs:
            # Still surface the count so the UI can show "calibrating (n/min_obs)".
            mean_pct = sum(signed_pcts) / n
            mae_pct = sum(abs(x) for x in signed_pcts) / n
            out[und] = {
                "n_observations": n,
                "mean_signed_error_pct": round(mean_pct, 6),
                "mean_abs_error_pct": round(mae_pct, 6),
                "applied": False,
            }
            continue
        mean_pct = sum(signed_pcts) / n
        mae_pct = sum(abs(x) for x in signed_pcts) / n
        out[und] = {
            "n_observations": n,
            "mean_signed_error_pct": round(mean_pct, 6),
            "mean_abs_error_pct": round(mae_pct, 6),
            "applied": True,
        }
    return {
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "window_days": window,
        "min_observations": min_obs,
        "by_underlying": out,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Reconcile intraday LETF flow estimates against EOD")
    parser.add_argument("--snapshot-dir", type=Path, default=SNAPSHOT_DIR)
    parser.add_argument("--realized-json", type=Path, default=REALIZED_JSON)
    parser.add_argument("--realized-log", type=Path, default=REALIZED_LOG)
    parser.add_argument("--metrics-json", type=Path, default=METRICS_JSON)
    parser.add_argument(
        "--max-minutes-to-close",
        type=int,
        default=MAX_MINUTES_TO_CLOSE,
        help="Pick the snapshot with smallest minutes_to_close in [0, this].",
    )
    parser.add_argument("--rolling-window", type=int, default=ROLLING_WINDOW)
    parser.add_argument("--min-observations", type=int, default=MIN_OBS)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(levelname)s:%(name)s:%(message)s",
    )

    existing = _read_jsonl(args.realized_log)
    scored = {(r.get("trading_date"), r.get("underlying")) for r in existing}

    # Find candidate dates with a snapshot but no full coverage in the log.
    candidate_dates: list[str] = []
    if args.snapshot_dir.exists():
        for p in sorted(args.snapshot_dir.glob("*.jsonl")):
            d = p.stem
            try:
                datetime.strptime(d, "%Y-%m-%d")
            except ValueError:
                continue
            already_scored_count = sum(1 for k in scored if k[0] == d)
            if already_scored_count == 0:
                candidate_dates.append(d)

    new_rows: list[dict[str, Any]] = []
    for d in candidate_dates:
        recs = reconcile_one_day(d, args.snapshot_dir, args.realized_json)
        if recs:
            new_rows.extend(recs)
            LOGGER.info("reconciled %s: %d underlyings", d, len(recs))

    if new_rows:
        _append_jsonl(args.realized_log, new_rows)

    metrics = roll_metrics(
        args.realized_log,
        window=args.rolling_window,
        min_obs=args.min_observations,
    )
    args.metrics_json.parent.mkdir(parents=True, exist_ok=True)
    args.metrics_json.write_text(
        json.dumps(metrics, separators=(",", ":"), allow_nan=False, sort_keys=True),
        encoding="utf-8",
    )

    n_und = len(metrics.get("by_underlying") or {})
    n_applied = sum(1 for v in (metrics.get("by_underlying") or {}).values() if v.get("applied"))
    LOGGER.info(
        "score_intraday_flows: appended=%d new rows; underlyings tracked=%d (applied bias on %d)",
        len(new_rows), n_und, n_applied,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

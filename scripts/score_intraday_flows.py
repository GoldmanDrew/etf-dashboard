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


def _signed_pct(estimate: float | None, realised: float) -> float | None:
    if estimate is None or not abs(realised) > 0:
        return None
    return (estimate - realised) / realised


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
        # The "default" estimate stays under ``estimated_net_close_rebalance_dollars``
        # for backward-compatibility; spot/nav/blend variants are scored
        # separately so we can compute per-source MAE and the optimal blend
        # weight independently of which one the build script surfaced.
        chosen = _f(est_row.get("estimated_net_close_rebalance_dollars"))
        spot = _f(est_row.get("estimated_net_close_rebalance_dollars_spot"))
        nav = _f(est_row.get("estimated_net_close_rebalance_dollars_nav"))
        blend = _f(est_row.get("estimated_net_close_rebalance_dollars_blend"))
        real_dollars = _f(real_row.get("net_moc_dollars"))
        if chosen is None and blend is None and spot is None and nav is None:
            continue
        if real_dollars is None:
            continue
        chosen = chosen if chosen is not None else (blend if blend is not None else (spot if spot is not None else nav))
        signed_error = chosen - real_dollars
        out.append({
            "trading_date": date_iso,
            "underlying": und,
            "estimate_dollars": round(chosen, 2),
            "estimate_dollars_spot": round(spot, 2) if spot is not None else None,
            "estimate_dollars_nav": round(nav, 2) if nav is not None else None,
            "estimate_dollars_blend": round(blend, 2) if blend is not None else None,
            "realized_dollars": round(real_dollars, 2),
            "signed_error_dollars": round(signed_error, 2),
            "signed_error_pct": (
                round(_signed_pct(chosen, real_dollars), 6)
                if _signed_pct(chosen, real_dollars) is not None else None
            ),
            "signed_error_pct_spot": (
                round(_signed_pct(spot, real_dollars), 6)
                if _signed_pct(spot, real_dollars) is not None else None
            ),
            "signed_error_pct_nav": (
                round(_signed_pct(nav, real_dollars), 6)
                if _signed_pct(nav, real_dollars) is not None else None
            ),
            "signed_error_pct_blend": (
                round(_signed_pct(blend, real_dollars), 6)
                if _signed_pct(blend, real_dollars) is not None else None
            ),
            "top_contributors": est_row.get("top_contributors") or [],
            "minutes_to_close_at_estimate": est.get("minutes_to_close"),
            "estimate_as_of": est.get("as_of"),
        })
    return out


def _stats_for(values: list[float]) -> dict[str, float]:
    n = len(values)
    if n == 0:
        return {"n": 0}
    mean = sum(values) / n
    mae = sum(abs(x) for x in values) / n
    mse = sum(x * x for x in values) / n
    return {
        "n": n,
        "mean_signed_error_pct": round(mean, 6),
        "mean_abs_error_pct": round(mae, 6),
        "mse_pct": round(mse, 8),
    }


def _optimal_blend_weight(spot_pcts: list[float], nav_pcts: list[float]) -> float | None:
    """Minimum-variance combination weight on the NAV path.

    For each row that has both signed errors we minimise
    ``var(w·err_nav + (1-w)·err_spot)``. The closed-form minimiser is::

        w* = (var_spot - cov(spot, nav)) / (var_spot + var_nav - 2·cov(spot, nav))

    which collapses to the inverse-MSE rule when the two sources are
    uncorrelated. We clip to ``[0.05, 0.95]`` so neither source ever fully
    drops out of the production estimator -- aligns with the floor/ceil in
    ``build_letf_intraday_flows.py``.
    """
    if not spot_pcts or not nav_pcts or len(spot_pcts) != len(nav_pcts):
        return None
    n = len(spot_pcts)
    if n < 2:
        return None
    mean_s = sum(spot_pcts) / n
    mean_n = sum(nav_pcts) / n
    var_s = sum((x - mean_s) ** 2 for x in spot_pcts) / n
    var_n = sum((x - mean_n) ** 2 for x in nav_pcts) / n
    cov = sum((s - mean_s) * (nv - mean_n) for s, nv in zip(spot_pcts, nav_pcts)) / n
    denom = var_s + var_n - 2.0 * cov
    if denom <= 1e-12:
        return None
    w = (var_s - cov) / denom
    return max(0.05, min(0.95, w))


def roll_metrics(realized_log: Path, *, window: int = ROLLING_WINDOW, min_obs: int = MIN_OBS) -> dict[str, Any]:
    """Per-underlying rolling stats over the last ``window`` reconciled days.

    Emits two levels of detail:

    * ``by_underlying[UND]``  -- chosen-estimate signed-error stats
      (legacy bias map; consumed by the per-underlying bias adjustment in
      ``build_letf_intraday_flows.py``).
    * ``by_underlying[UND].by_source.{spot,nav,blend}`` -- per-source stats so
      the UI can show calibration quality of each path.
    * ``by_ticker[ETF]`` -- per-fund optimal NAV blend weight derived from the
      contributor-level breakdown stored on each reconciled row. Once we have
      ``min_obs`` matching days for a fund's spot and NAV signed errors, we
      surface ``blend_weight_nav`` -- ``build_letf_intraday_flows.py`` will use
      it instead of the model/confidence prior on the next intraday build.
    """
    rows = _read_jsonl(realized_log)
    if not rows:
        return {
            "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "window_days": window,
            "min_observations": min_obs,
            "by_underlying": {},
            "by_ticker": {},
        }
    rows.sort(key=lambda r: r.get("trading_date") or "")
    by_und: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        und = str(r.get("underlying") or "").upper()
        if not und:
            continue
        by_und.setdefault(und, []).append(r)

    out: dict[str, dict[str, Any]] = {}
    contributor_pairs: dict[str, list[tuple[float, float]]] = {}

    for und, lst in by_und.items():
        recent = lst[-window:]
        chosen = [r.get("signed_error_pct") for r in recent if r.get("signed_error_pct") is not None]
        if not chosen:
            continue
        spot = [r.get("signed_error_pct_spot") for r in recent if r.get("signed_error_pct_spot") is not None]
        nav = [r.get("signed_error_pct_nav") for r in recent if r.get("signed_error_pct_nav") is not None]
        blend = [r.get("signed_error_pct_blend") for r in recent if r.get("signed_error_pct_blend") is not None]

        # Pair spot/nav errors per day (for optimal weight at the per-underlying level).
        pair_dates: list[tuple[float, float]] = []
        for r in recent:
            s = r.get("signed_error_pct_spot")
            nv = r.get("signed_error_pct_nav")
            if s is None or nv is None:
                continue
            pair_dates.append((float(s), float(nv)))

        n = len(chosen)
        chosen_stats = _stats_for([float(x) for x in chosen])
        applied = n >= min_obs

        record = {
            "n_observations": n,
            "mean_signed_error_pct": chosen_stats.get("mean_signed_error_pct"),
            "mean_abs_error_pct": chosen_stats.get("mean_abs_error_pct"),
            "applied": bool(applied),
            "by_source": {
                "spot": _stats_for([float(x) for x in spot]),
                "nav": _stats_for([float(x) for x in nav]),
                "blend": _stats_for([float(x) for x in blend]),
            },
        }
        if pair_dates:
            ws = _optimal_blend_weight([s for s, _ in pair_dates], [nv for _, nv in pair_dates])
            if ws is not None and len(pair_dates) >= min_obs:
                record["optimal_blend_weight_nav"] = round(ws, 4)
                record["optimal_blend_weight_n"] = len(pair_dates)
        out[und] = record

        # Per-fund contributor accumulation for by_ticker.
        for r in recent:
            for c in r.get("top_contributors") or []:
                ticker = str(c.get("ticker") or "").upper()
                if not ticker:
                    continue
                # Use the per-underlying spot/nav signed errors as a proxy for
                # the fund-level calibration (fund-specific reconciliation
                # would need a fund-level realised, which we do not have).
                s = r.get("signed_error_pct_spot")
                nv = r.get("signed_error_pct_nav")
                if s is None or nv is None:
                    continue
                contributor_pairs.setdefault(ticker, []).append((float(s), float(nv)))

    by_ticker: dict[str, dict[str, Any]] = {}
    for ticker, pairs in contributor_pairs.items():
        if len(pairs) < min_obs:
            by_ticker[ticker] = {"n_observations": len(pairs), "blend_weight_nav": None}
            continue
        w = _optimal_blend_weight([s for s, _ in pairs], [nv for _, nv in pairs])
        if w is None:
            continue
        by_ticker[ticker] = {
            "n_observations": len(pairs),
            "blend_weight_nav": round(w, 4),
        }

    return {
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "window_days": window,
        "min_observations": min_obs,
        "by_underlying": out,
        "by_ticker": by_ticker,
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

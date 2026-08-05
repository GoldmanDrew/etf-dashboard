#!/usr/bin/env python3
"""Audit ETF/underlying leg alignment that feeds realized pair decay.

Two failure modes corrupt the 20d Realized decay column without a large cliff:

1. ``etf_adj_close`` sampled one session ahead of ``close_price`` (join-key
   asymmetry). Detected by lagged-vs-contemporaneous return correlation — no
   amplitude threshold.
2. Economically impossible single sessions on a well-tracked LETF/inverse pair
   (direction violation). Detected by the same filter that rebuilds daily drag.

A third signal — the roughness scan used by ``repair_shifted_etf_adj_close`` —
localises which sessions are shifted so the report is actionable.

Usage::

    python scripts/audit_pair_leg_alignment.py
    python scripts/audit_pair_leg_alignment.py --fail-on-shifted
    python scripts/audit_pair_leg_alignment.py --since 2026-06-01 --fail-on-shifted
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from ingest_etf_metrics import (  # noqa: E402
    PARQUET_PATH,
    UNIVERSE_CSV,
    load_universe_tickers,
)
from price_basis import detect_shifted_etf_adj_close  # noqa: E402
from realized_gross_decay import (  # noqa: E402
    PAIR_MAX_EXCLUDED_IN_HORIZON,
    build_daily_log_drag_series_with_meta,
)
from repair_shifted_etf_adj_close import scan as scan_shifted_sessions  # noqa: E402

DEFAULT_REPORT = ROOT / "data" / "pair_leg_alignment_report.json"
DASHBOARD_JSON = ROOT / "data" / "dashboard_data.json"
DEFAULT_LOOKBACK_DAYS = 90
# Hard gate: any residual shifted ticker in the lookback window fails the job.
# Soft mode always writes the report; the nightly workflow runs soft then repair
# then hard, matching the Decay coverage pattern.
SHIFTED_TICKER_FAIL_MAX = 0
# Direction-violation suppressions are reported but do not fail the hard gate on
# their own — thin-history / structured products can legitimately suppress. The
# hard invariant is "no residual shifted etf_adj_close".


def _load_betas() -> dict[str, float]:
    """Prefer dashboard_data.json deltas; fall back to the screener CSV."""
    betas: dict[str, float] = {}
    if DASHBOARD_JSON.exists():
        try:
            payload = json.loads(DASHBOARD_JSON.read_text(encoding="utf-8"))
            for row in payload.get("rows") or payload.get("records") or []:
                sym = str(row.get("symbol") or "").upper()
                if not sym:
                    continue
                for key in ("delta", "beta", "expected_leverage", "leverage"):
                    try:
                        val = float(row.get(key))
                    except (TypeError, ValueError):
                        continue
                    if math.isfinite(val) and val != 0:
                        betas[sym] = val
                        break
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            pass
    if UNIVERSE_CSV.exists():
        try:
            csv = pd.read_csv(UNIVERSE_CSV)
            colmap = {c.lower(): c for c in csv.columns}
            sym_col = colmap.get("etf") or colmap.get("symbol") or colmap.get("ticker")
            beta_col = (
                colmap.get("delta")
                or colmap.get("beta")
                or colmap.get("expected_leverage")
                or colmap.get("leverage")
            )
            if sym_col and beta_col:
                for _, row in csv.iterrows():
                    sym = str(row[sym_col]).upper()
                    if sym in betas:
                        continue
                    try:
                        val = float(row[beta_col])
                    except (TypeError, ValueError):
                        continue
                    if math.isfinite(val) and val != 0:
                        betas[sym] = val
        except (OSError, ValueError, pd.errors.ParserError):
            pass
    return betas


def _recent_rows(grp: pd.DataFrame, since: date | None) -> list[dict[str, Any]]:
    work = grp.sort_values("date")
    if since is not None:
        work = work[work["date"] >= since]
    return work.to_dict("records")


def classify_ticker(
    rows: list[dict[str, Any]],
    *,
    beta: float | None,
    shifted_sessions: list[date] | None = None,
) -> dict[str, Any]:
    """Per-ticker alignment diagnosis over the lookback window."""
    shifted = detect_shifted_etf_adj_close(rows)
    info: dict[str, Any] = {
        "shifted": bool(shifted.get("shifted")),
        "corr_aligned": shifted.get("corr_aligned"),
        "corr_lagged": shifted.get("corr_lagged"),
        "shifted_n_obs": shifted.get("n_obs"),
        "shifted_sessions": [d.isoformat() for d in (shifted_sessions or [])],
        "shifted_session_count": len(shifted_sessions or []),
        "beta": beta,
        "pair_tracks_well": None,
        "pair_track_r2": None,
        "pair_track_reason": None,
        "direction_violations": 0,
        "direction_violations_excluded": 0,
        "would_suppress_20d": False,
        "suppress_reason": None,
    }
    if beta is None or not math.isfinite(float(beta)):
        info["pair_track_reason"] = "beta_missing"
        return info

    # Lightweight TR: reuse the same daily-drag builder the dashboard uses, but
    # skip corporate-action loading here — alignment is about contemporaneous
    # market columns, not split scaling. The builder still prefers etf_adj_close.
    from price_basis import build_tr_series_from_metrics  # local import keeps cold path light

    tr = build_tr_series_from_metrics(rows, [])
    drag = build_daily_log_drag_series_with_meta(tr, float(beta))
    meta = drag.get("meta") or {}
    daily = drag.get("series") or []
    track = meta.get("pair_track") or {}
    violations = meta.get("direction_violations") or []
    excluded = meta.get("direction_violations_excluded") or []
    info["pair_tracks_well"] = bool(track.get("tracks_well"))
    info["pair_track_r2"] = track.get("r2")
    info["pair_track_reason"] = track.get("reason")
    info["direction_violations"] = len(violations)
    info["direction_violations_excluded"] = len(excluded)
    info["n_drag_days"] = len(daily)

    # Mirror the 20d suppress policy so the audit and the published number agree.
    if len(excluded) > PAIR_MAX_EXCLUDED_IN_HORIZON:
        info["would_suppress_20d"] = True
        info["suppress_reason"] = "too_many_excluded_days"
    elif not track.get("tracks_well") and violations:
        info["would_suppress_20d"] = True
        info["suppress_reason"] = track.get("reason") or "pair_untracked"
    return info


def build_alignment_report(
    df: pd.DataFrame,
    *,
    universe: set[str] | None = None,
    betas: dict[str, float] | None = None,
    since: date | None = None,
) -> dict[str, Any]:
    work = df.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce").dt.date
    work["ticker"] = work["ticker"].astype(str).str.upper()
    work = work.dropna(subset=["date"])

    uni = universe or set(load_universe_tickers()) if UNIVERSE_CSV.exists() else None
    beta_map = betas if betas is not None else _load_betas()
    session_hits = scan_shifted_sessions(work, since=since)

    rows_out: list[dict[str, Any]] = []
    for ticker, grp in work.groupby("ticker"):
        sym = str(ticker).upper()
        if uni and sym not in uni:
            continue
        recs = _recent_rows(grp, since)
        if len(recs) < 5:
            continue
        info = classify_ticker(
            recs,
            beta=beta_map.get(sym),
            shifted_sessions=session_hits.get(sym),
        )
        info["ticker"] = sym
        rows_out.append(info)

    rows_out.sort(
        key=lambda r: (
            -int(bool(r["shifted"])),
            -int(r.get("shifted_session_count") or 0),
            -int(r.get("direction_violations") or 0),
            r["ticker"],
        )
    )
    shifted_rows = [r for r in rows_out if r.get("shifted") or (r.get("shifted_session_count") or 0) > 0]
    suppressed_rows = [r for r in rows_out if r.get("would_suppress_20d")]
    return {
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "since": since.isoformat() if since else None,
        "summary": {
            "universe_n": len(rows_out),
            "shifted_n": len([r for r in rows_out if r.get("shifted")]),
            "shifted_session_tickers_n": len(
                [r for r in rows_out if (r.get("shifted_session_count") or 0) > 0]
            ),
            "shifted_sessions_n": sum(int(r.get("shifted_session_count") or 0) for r in rows_out),
            "would_suppress_20d_n": len(suppressed_rows),
            "direction_violations_n": sum(int(r.get("direction_violations") or 0) for r in rows_out),
        },
        "shifted": [
            {
                "ticker": r["ticker"],
                "corr_aligned": r.get("corr_aligned"),
                "corr_lagged": r.get("corr_lagged"),
                "shifted_session_count": r.get("shifted_session_count"),
                "shifted_sessions": (r.get("shifted_sessions") or [])[:12],
            }
            for r in shifted_rows[:80]
        ],
        "would_suppress_20d": [
            {
                "ticker": r["ticker"],
                "reason": r.get("suppress_reason"),
                "pair_track_r2": r.get("pair_track_r2"),
                "direction_violations": r.get("direction_violations"),
            }
            for r in suppressed_rows[:80]
        ],
        "tickers": rows_out,
    }


def alignment_gate_errors(report: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    summary = report.get("summary") or {}
    shifted_n = int(summary.get("shifted_n") or 0)
    session_tickers = int(summary.get("shifted_session_tickers_n") or 0)
    # Either detector is enough: correlation catches a persistent lag; roughness
    # catches a short run that the whole-window correlation might dilute.
    residual = max(shifted_n, session_tickers)
    if residual > SHIFTED_TICKER_FAIL_MAX:
        errors.append(
            f"pair leg alignment: {residual} ticker(s) still have etf_adj_close "
            f"shifted vs close_price (lookback since={report.get('since')}). "
            f"Run scripts/repair_shifted_etf_adj_close.py --apply"
        )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet", type=Path, default=PARQUET_PATH)
    parser.add_argument("--report-out", type=Path, default=DEFAULT_REPORT)
    parser.add_argument(
        "--since",
        type=str,
        default=None,
        help="Only audit sessions on/after YYYY-MM-DD (default: last 90 calendar days)",
    )
    parser.add_argument(
        "--fail-on-shifted",
        action="store_true",
        help="Exit 1 when residual shifted etf_adj_close remains",
    )
    args = parser.parse_args()

    if not args.parquet.exists():
        print(f"missing {args.parquet}", file=sys.stderr)
        return 2

    since: date | None
    if args.since:
        try:
            since = date.fromisoformat(str(args.since)[:10])
        except ValueError:
            print(f"bad --since value {args.since!r}", file=sys.stderr)
            return 2
    else:
        since = date.today() - timedelta(days=DEFAULT_LOOKBACK_DAYS)

    df = pd.read_parquet(args.parquet)
    universe = set(load_universe_tickers()) if UNIVERSE_CSV.exists() else None
    report = build_alignment_report(df, universe=universe, since=since)
    args.report_out.parent.mkdir(parents=True, exist_ok=True)
    # Compact on-disk report: drop the full per-ticker grid (can be large).
    on_disk = {k: v for k, v in report.items() if k != "tickers"}
    on_disk["tickers_sample"] = [
        {
            "ticker": r["ticker"],
            "shifted": r.get("shifted"),
            "corr_aligned": r.get("corr_aligned"),
            "corr_lagged": r.get("corr_lagged"),
            "shifted_session_count": r.get("shifted_session_count"),
            "would_suppress_20d": r.get("would_suppress_20d"),
            "suppress_reason": r.get("suppress_reason"),
            "pair_track_r2": r.get("pair_track_r2"),
            "direction_violations": r.get("direction_violations"),
        }
        for r in report["tickers"][:200]
    ]
    args.report_out.write_text(json.dumps(on_disk, indent=2, default=str), encoding="utf-8")

    summary = report["summary"]
    print(
        f"since={report['since']} universe={summary.get('universe_n')} "
        f"shifted={summary.get('shifted_n')} "
        f"shifted_session_tickers={summary.get('shifted_session_tickers_n')} "
        f"shifted_sessions={summary.get('shifted_sessions_n')} "
        f"would_suppress_20d={summary.get('would_suppress_20d_n')} "
        f"direction_violations={summary.get('direction_violations_n')}"
    )
    print(f"wrote {args.report_out}")

    errors = alignment_gate_errors(report) if args.fail_on_shifted else []
    if errors:
        print("Pair leg alignment gate failures:")
        for msg in errors:
            print(f"  - {msg}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

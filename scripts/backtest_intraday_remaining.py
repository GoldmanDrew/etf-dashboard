#!/usr/bin/env python3
"""Backtest intraday remaining-to-close heuristics vs realised EOD flow.

Compares predictors of close rebalance dollars using historical
``letf_rebalance_flows_intraday_snapshots/*.jsonl`` vs aggregated realised
flow from ``letf_rebalance_flows_daily.parquet``.

Models (prediction of realised net MOC $):
  * ``baseline_est`` - use the intraday estimate as-is (default).
  * ``vol_remaining`` - ``est * (1 - min(vol/ADV, 1))`` (legacy volume proxy).
  * ``time_remaining`` - ``est * (minutes_to_close / session_minutes)``.

Writes ``data/letf_intraday_remaining_backtest.json`` with MAE/MAPE and a
``recommended_remaining_model`` used by ``build_letf_intraday_flows.py``.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

LOGGER = logging.getLogger("backtest_intraday_remaining")

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
SNAPSHOT_DIR = DATA_DIR / "letf_rebalance_flows_intraday_snapshots"
DAILY_PARQUET = DATA_DIR / "letf_rebalance_flows_daily.parquet"
OUTPUT_JSON = DATA_DIR / "letf_intraday_remaining_backtest.json"

DEFAULT_SESSION_MINUTES = 390
MAX_MINUTES_TO_CLOSE = 60
TIER0_PCT_ADV = 0.005


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


def load_realized_by_date(parquet_path: Path = DAILY_PARQUET) -> dict[str, dict[str, float]]:
    """``{date_iso: {UND: net_moc_dollars}}`` from fund-level daily parquet."""
    if not parquet_path.exists():
        return {}
    try:
        df = pd.read_parquet(parquet_path)
    except Exception as exc:
        LOGGER.warning("failed to read %s: %s", parquet_path, exc)
        return {}
    if df.empty or "date" not in df.columns:
        return {}
    ok = df.get("included_in_aggregate")
    if ok is not None:
        df = df[ok.astype(bool)]
    df = df.dropna(subset=["underlying", "rebalance_signed_dollars"])
    df["date"] = df["date"].astype(str)
    df["underlying"] = df["underlying"].astype(str).str.upper()
    grouped = (
        df.groupby(["date", "underlying"], as_index=False)["rebalance_signed_dollars"]
        .sum()
        .rename(columns={"rebalance_signed_dollars": "net_moc_dollars"})
    )
    out: dict[str, dict[str, float]] = {}
    for _, row in grouped.iterrows():
        d = str(row["date"])
        und = str(row["underlying"]).upper()
        val = _f(row.get("net_moc_dollars"))
        if val is None:
            continue
        out.setdefault(d, {})[und] = val
    return out


def _pick_snapshot_rows(rows: list[dict[str, Any]], *, max_minutes: int) -> list[dict[str, Any]]:
    """Prefer near-close builds; fall back to last build of the day."""
    if not rows:
        return []
    near = [
        r for r in rows
        if isinstance(r.get("minutes_to_close"), (int, float))
        and 0 <= int(r["minutes_to_close"]) <= max_minutes
    ]
    if near:
        near.sort(key=lambda r: int(r.get("minutes_to_close") or 0))
        return [near[0]]
    return [rows[-1]]


def _pred_baseline(est: float) -> float:
    return est


def _pred_vol_remaining(est: float, vol_pct: float | None) -> float:
    if vol_pct is None:
        return est
    share = max(0.0, min(1.0, vol_pct))
    return est * (1.0 - share)


def _pred_time_remaining(est: float, minutes_to_close: int | None, *, session: int) -> float:
    if minutes_to_close is None or session <= 0:
        return est
    frac = max(0.0, min(1.0, float(minutes_to_close) / float(session)))
    return est * frac


def _model_stats(errors: list[float], pct_errors: list[float]) -> dict[str, Any]:
    if not errors:
        return {"n": 0}
    n = len(errors)
    mae = sum(abs(x) for x in errors) / n
    mape = sum(abs(x) for x in pct_errors) / len(pct_errors) if pct_errors else None
    rmse = math.sqrt(sum(x * x for x in errors) / n)
    return {
        "n": n,
        "mae_dollars": round(mae, 2),
        "rmse_dollars": round(rmse, 2),
        "mape_abs": round(mape, 6) if mape is not None else None,
    }


def collect_observations(
    *,
    snapshot_dir: Path,
    realized_by_date: dict[str, dict[str, float]],
    session_minutes: int,
    max_minutes_to_close: int,
) -> list[dict[str, Any]]:
    obs: list[dict[str, Any]] = []
    if not snapshot_dir.exists():
        return obs
    for path in sorted(snapshot_dir.glob("*.jsonl")):
        date_iso = path.stem
        if date_iso not in realized_by_date:
            continue
        realised_day = realized_by_date[date_iso]
        for snap in _pick_snapshot_rows(_read_jsonl(path), max_minutes=max_minutes_to_close):
            minutes = snap.get("minutes_to_close")
            try:
                minutes_i = int(minutes) if minutes is not None else None
            except (TypeError, ValueError):
                minutes_i = None
            by_und = snap.get("by_underlying") or {}
            for und, est_row in by_und.items():
                sym = str(und).upper()
                real = _f(realised_day.get(sym))
                est = _f(est_row.get("estimated_net_close_rebalance_dollars"))
                if est is None or real is None:
                    continue
                adv = _f(est_row.get("underlying_dollar_adv_20d"))
                vol_pct = _f(est_row.get("volume_so_far_pct_adv"))
                if vol_pct is None and adv and adv > 0:
                    vol_d = _f(est_row.get("volume_so_far_dollars"))
                    if vol_d is not None:
                        vol_pct = vol_d / adv
                pct_adv = abs(est) / adv if adv and adv > 0 else None
                ret = _f(est_row.get("return_d1_so_far"))
                blend_w = _f(est_row.get("mean_blend_weight_nav"))
                obs.append({
                    "trading_date": date_iso,
                    "underlying": sym,
                    "est": est,
                    "realised": real,
                    "minutes_to_close": minutes_i,
                    "vol_pct_adv": vol_pct,
                    "pct_adv": pct_adv,
                    "abs_return_so_far": abs(ret) if ret is not None else None,
                    "mean_blend_weight_nav": blend_w,
                })
    return obs


def evaluate_models(
    observations: list[dict[str, Any]],
    *,
    session_minutes: int,
) -> dict[str, dict[str, Any]]:
    models = {
        "baseline_est": lambda est, o: _pred_baseline(est),
        "vol_remaining": lambda est, o: _pred_vol_remaining(est, o.get("vol_pct_adv")),
        "time_remaining": lambda est, o: _pred_time_remaining(
            est, o.get("minutes_to_close"), session=session_minutes,
        ),
    }
    out: dict[str, dict[str, Any]] = {}
    for name, fn in models.items():
        errors: list[float] = []
        pct_errors: list[float] = []
        for o in observations:
            est = float(o["est"])
            real = float(o["realised"])
            pred = fn(est, o)
            err = pred - real
            errors.append(err)
            if abs(real) > 0:
                pct_errors.append(err / real)
        out[name] = _model_stats(errors, pct_errors)
    return out


def evaluate_subset(
    observations: list[dict[str, Any]],
    *,
    session_minutes: int,
    predicate,
) -> dict[str, dict[str, Any]]:
    subset = [o for o in observations if predicate(o)]
    return evaluate_models(subset, session_minutes=session_minutes)


def pick_recommended(overall: dict[str, dict[str, Any]]) -> str:
    """Return model name with lowest MAE; default ``none`` (= baseline only for remaining)."""
    if not overall or all(v.get("n", 0) == 0 for v in overall.values()):
        return "none"
    baseline_mae = overall.get("baseline_est", {}).get("mae_dollars")
    if baseline_mae is None:
        return "none"
    best_name = "none"
    best_mae = float(baseline_mae)
    for name in ("vol_remaining", "time_remaining"):
        mae = overall.get(name, {}).get("mae_dollars")
        n = overall.get(name, {}).get("n", 0)
        if mae is None or n == 0:
            continue
        if float(mae) < best_mae * 0.995:
            best_mae = float(mae)
            best_name = name
    return best_name


def run_backtest(
    *,
    snapshot_dir: Path = SNAPSHOT_DIR,
    daily_parquet: Path = DAILY_PARQUET,
    output_json: Path = OUTPUT_JSON,
    session_minutes: int = DEFAULT_SESSION_MINUTES,
    max_minutes_to_close: int = MAX_MINUTES_TO_CLOSE,
) -> dict[str, Any]:
    realized = load_realized_by_date(daily_parquet)
    observations = collect_observations(
        snapshot_dir=snapshot_dir,
        realized_by_date=realized,
        session_minutes=session_minutes,
        max_minutes_to_close=max_minutes_to_close,
    )
    overall = evaluate_models(observations, session_minutes=session_minutes)
    tier0 = evaluate_subset(
        observations,
        session_minutes=session_minutes,
        predicate=lambda o: (
            o.get("pct_adv") is not None and float(o["pct_adv"]) >= TIER0_PCT_ADV
        ),
    )
    vol_obs = [o for o in observations if o.get("vol_pct_adv") is not None]
    with_volume = evaluate_models(vol_obs, session_minutes=session_minutes) if vol_obs else {}

    recommended = pick_recommended(overall)
    tier0_recommended = pick_recommended(tier0) if tier0.get("baseline_est", {}).get("n", 0) else "none"

    payload: dict[str, Any] = {
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "session_minutes": session_minutes,
        "max_minutes_to_close": max_minutes_to_close,
        "n_observations": len(observations),
        "n_observations_with_volume": len(vol_obs),
        "n_trading_days": len({o["trading_date"] for o in observations}),
        "models_overall": overall,
        "models_tier0_pct_adv": tier0,
        "models_with_volume_only": with_volume,
        "recommended_remaining_model": recommended,
        "recommended_remaining_model_tier0": tier0_recommended,
        "notes": (
            "recommended_remaining_model applies to remaining_close_rebalance_dollars "
            "decomposition in build_letf_intraday_flows.py; baseline_est wins when "
            "vol/time heuristics do not beat est by at least 0.5% MAE."
        ),
    }
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )
    LOGGER.info(
        "backtest n=%d days=%d recommended=%s tier0=%s baseline_mae=%s",
        len(observations),
        payload["n_trading_days"],
        recommended,
        tier0_recommended,
        overall.get("baseline_est", {}).get("mae_dollars"),
    )
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Backtest intraday remaining heuristics")
    parser.add_argument("--snapshot-dir", type=Path, default=SNAPSHOT_DIR)
    parser.add_argument("--daily-parquet", type=Path, default=DAILY_PARQUET)
    parser.add_argument("--output", type=Path, default=OUTPUT_JSON)
    parser.add_argument("--session-minutes", type=int, default=DEFAULT_SESSION_MINUTES)
    parser.add_argument("--max-minutes-to-close", type=int, default=MAX_MINUTES_TO_CLOSE)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(levelname)s:%(name)s:%(message)s",
    )
    run_backtest(
        snapshot_dir=args.snapshot_dir,
        daily_parquet=args.daily_parquet,
        output_json=args.output,
        session_minutes=int(args.session_minutes),
        max_minutes_to_close=int(args.max_minutes_to_close),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

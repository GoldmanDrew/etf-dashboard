#!/usr/bin/env python3
"""Score saved borrow-spike predictions against realized borrow history."""
from __future__ import annotations

import argparse
import json
import math
from datetime import UTC, date, datetime
import sys
from pathlib import Path
from typing import Any

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from analyze_borrow_spike_accuracy import compute_metrics_df  # noqa: E402
from borrow_spike_model import (  # noqa: E402
    borrow_outcome_at_date,
    compute_borrow_spike_event_by_date,
    risk_band,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
MATURE_LABEL_DAYS = 7


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, separators=(",", ":"), allow_nan=False) + "\n")


def _append_jsonl(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, separators=(",", ":"), allow_nan=False) + "\n")


def _dedupe_realized(path: Path) -> None:
    if not path.exists():
        return
    by_key: dict[tuple[str, str], dict] = {}
    for ln in path.read_text(encoding="utf-8").splitlines():
        ln = ln.strip()
        if not ln:
            continue
        try:
            o = json.loads(ln)
        except json.JSONDecodeError:
            continue
        k = (str(o.get("pred_date") or ""), str(o.get("symbol") or "").upper())
        if k[0] and k[1]:
            by_key[k] = o
    _write_jsonl(
        path,
        sorted(by_key.values(), key=lambda x: (x.get("pred_date"), x.get("symbol"))),
    )


def _parse_pred_date(pred_date: str) -> date | None:
    try:
        return date.fromisoformat(pred_date[:10])
    except ValueError:
        return None


def _is_mature_pred_date(pred_date: str, *, today: date | None = None) -> bool:
    d = _parse_pred_date(pred_date)
    if d is None:
        return False
    ref = today or datetime.now(UTC).date()
    return (ref - d).days >= MATURE_LABEL_DAYS


def score_prediction_file(
    pred_path: Path,
    borrow_symbols: dict[str, list[dict]],
    *,
    horizon_days: int,
    require_mature: bool = True,
    today: date | None = None,
    label_cache: dict[tuple[str, int], dict[str, float | None]] | None = None,
) -> list[dict]:
    payload = _load_json(pred_path)
    pred_date = str(payload.get("as_of") or pred_path.stem)
    if not pred_date:
        return []
    if require_mature and not _is_mature_pred_date(pred_date, today=today):
        return []
    syms = payload.get("symbols") or {}
    if not isinstance(syms, dict):
        return []
    out: list[dict] = []
    for sym_raw, row in syms.items():
        sym = str(sym_raw or "").upper()
        if not sym:
            continue
        hist = borrow_symbols.get(sym) or borrow_symbols.get(sym_raw)
        if not isinstance(hist, list) or not hist:
            continue
        cache_key = (sym, horizon_days)
        if label_cache is not None:
            if cache_key not in label_cache:
                label_cache[cache_key] = compute_borrow_spike_event_by_date(
                    hist, horizon_days=horizon_days,
                )
            labels = label_cache[cache_key]
        else:
            labels = compute_borrow_spike_event_by_date(hist, horizon_days=horizon_days)
        y = labels.get(pred_date)
        if y is None:
            continue
        p = row.get("p_spike_5d") if isinstance(row, dict) else None
        if p is None or not isinstance(p, (int, float)) or not math.isfinite(float(p)):
            continue
        p = float(p)
        rec: dict[str, Any] = {
            "pred_date": pred_date,
            "symbol": sym,
            "horizon_days": horizon_days,
            "p_spike": round(p, 6),
            "y_spike": int(y),
            "risk_band": row.get("risk_band") if isinstance(row, dict) else risk_band(p),
            "scoring_eligible": bool(row.get("scoring_eligible")) if isinstance(row, dict) else False,
            "scored_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        }
        outcome = borrow_outcome_at_date(hist, pred_date, horizon_days=horizon_days)
        if outcome:
            rec.update(outcome)
        out.append(rec)
    return out


def rollup_metrics(realized_path: Path, *, max_lines: int = 50_000) -> dict[str, Any]:
    base = {
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "n_rows": 0,
        "brier_score": None,
        "log_loss": None,
        "positive_rate": None,
        "calibration_by_band": [],
    }
    if not realized_path.exists():
        return base
    lines = realized_path.read_text(encoding="utf-8").splitlines()
    if len(lines) > max_lines:
        lines = lines[-max_lines:]
    rows: list[dict] = []
    for ln in lines:
        ln = ln.strip()
        if not ln:
            continue
        try:
            rows.append(json.loads(ln))
        except json.JSONDecodeError:
            continue
    if not rows:
        return base
    metrics = compute_metrics_df(
        pd.DataFrame(rows), p_col="p_spike", y_col="y_spike", source="live_predictions",
    )
    return {
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "n_rows": metrics.get("n_rows", len(rows)),
        "brier_score": metrics.get("brier_score"),
        "log_loss": metrics.get("log_loss"),
        "positive_rate": metrics.get("positive_rate"),
        "auroc": metrics.get("auroc"),
        "pr_auc": metrics.get("pr_auc"),
        "brier_skill_score": metrics.get("brier_skill_score"),
        "precision_at_k": metrics.get("precision_at_k"),
        "capture_rate_p_ge_0p10": metrics.get("capture_rate_p_ge_0p10"),
        "ece": metrics.get("ece"),
        "calibration_by_band": metrics.get("calibration_by_band") or [],
        "calibration_bins": metrics.get("calibration_bins") or [],
    }


def _existing_realized_keys(path: Path) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    if not path.exists():
        return out
    for ln in path.read_text(encoding="utf-8").splitlines():
        ln = ln.strip()
        if not ln:
            continue
        try:
            o = json.loads(ln)
        except json.JSONDecodeError:
            continue
        pd = str(o.get("pred_date") or "")
        sym = str(o.get("symbol") or "").upper()
        if pd and sym:
            out.add((pd, sym))
    return out


def _collect_all_scores(
    pred_dir: Path,
    borrow_symbols: dict[str, list[dict]],
    *,
    require_mature: bool = True,
    today: date | None = None,
) -> list[dict]:
    all_rows: list[dict] = []
    label_cache: dict[tuple[str, int], dict[str, float | None]] = {}
    if not pred_dir.exists():
        return all_rows
    for pred_path in sorted(pred_dir.glob("*.json")):
        stem = pred_path.stem
        if len(stem) != 10 or stem[4] != "-" or stem[7] != "-":
            continue
        pl = _load_json(pred_path)
        h = int(pl.get("horizon_days") or 5)
        all_rows.extend(
            score_prediction_file(
                pred_path,
                borrow_symbols,
                horizon_days=h,
                require_mature=require_mature,
                today=today,
                label_cache=label_cache,
            )
        )
    return all_rows


def score_from_repo(
    repo_root: Path | None = None,
    *,
    dedupe: bool = True,
    rescore_all: bool = False,
    require_mature: bool = True,
) -> dict[str, Any]:
    repo_root = repo_root or REPO_ROOT
    data_dir = repo_root / "data"
    hist_path = data_dir / "borrow_history.json"
    pred_dir = data_dir / "borrow_spike_predictions"
    realized_path = data_dir / "borrow_spike_realized.jsonl"
    metrics_path = data_dir / "borrow_spike_metrics.json"

    hist_payload = _load_json(hist_path)
    borrow_symbols = hist_payload.get("symbols") or {}
    if not isinstance(borrow_symbols, dict):
        borrow_symbols = {}

    today = datetime.now(UTC).date()

    if rescore_all:
        new_rows = _collect_all_scores(
            pred_dir, borrow_symbols, require_mature=require_mature, today=today,
        )
        _write_jsonl(realized_path, new_rows)
    else:
        existing = _existing_realized_keys(realized_path)
        new_rows: list[dict] = []
        for r in _collect_all_scores(
            pred_dir, borrow_symbols, require_mature=require_mature, today=today,
        ):
            k = (str(r.get("pred_date") or ""), str(r.get("symbol") or "").upper())
            if k in existing:
                continue
            existing.add(k)
            new_rows.append(r)
        if new_rows:
            _append_jsonl(realized_path, new_rows)

    if dedupe and realized_path.exists():
        _dedupe_realized(realized_path)

    metrics = rollup_metrics(realized_path)
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    with metrics_path.open("w", encoding="utf-8") as f:
        json.dump(metrics, f, separators=(",", ":"), allow_nan=False)
    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="Score borrow spike prediction snapshots.")
    parser.add_argument("--repo-root", type=str, default=str(REPO_ROOT))
    parser.add_argument("--no-dedupe", action="store_true")
    parser.add_argument("--rescore-all", action="store_true")
    parser.add_argument("--include-immature", action="store_true")
    args = parser.parse_args()
    m = score_from_repo(
        Path(args.repo_root),
        dedupe=not args.no_dedupe,
        rescore_all=args.rescore_all,
        require_mature=not args.include_immature,
    )
    print(
        f"[OK] borrow_spike_metrics: n_rows={m.get('n_rows')} "
        f"brier={m.get('brier_score')} auroc={m.get('auroc')}",
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Score saved borrow-spike predictions against realized borrow history.

``build_data.py`` writes ``data/borrow_spike_predictions/YYYY-MM-DD.json``
(one per run, keyed by ``as_of``). This script joins each prediction to the
outcome label at that ``as_of`` date using the same spike definition as
``build_borrow_spike_risk_payload`` (via ``compute_borrow_spike_event_by_date``).

Outputs:
  * ``data/borrow_spike_realized.jsonl`` — one JSON object per (pred_date, symbol)
  * ``data/borrow_spike_metrics.json`` — rolling Brier / log-loss / calibration
"""
from __future__ import annotations

import argparse
import json
import math
from datetime import UTC, datetime
import sys
from pathlib import Path
from typing import Any

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from build_data import compute_borrow_spike_event_by_date  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
BORROW_HISTORY_FILE = DATA_DIR / "borrow_history.json"
PRED_DIR = DATA_DIR / "borrow_spike_predictions"
REALIZED_JSONL = DATA_DIR / "borrow_spike_realized.jsonl"
METRICS_JSON = DATA_DIR / "borrow_spike_metrics.json"


def _sigmoid_log_loss(p: float, y: float) -> float:
    p = min(1.0 - 1e-9, max(1e-9, p))
    return -(y * math.log(p) + (1.0 - y) * math.log(1.0 - p))


def _risk_band(p: float | None) -> str:
    if p is None or not math.isfinite(p):
        return "unknown"
    if p >= 0.30:
        return "high"
    if p >= 0.10:
        return "elevated"
    return "low"


def _load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _append_jsonl(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, separators=(",", ":"), allow_nan=False) + "\n")


def _dedupe_realized(path: Path) -> None:
    """Rewrite JSONL dropping duplicate (pred_date, symbol) keys (keep last)."""
    if not path.exists():
        return
    lines = [ln.strip() for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    by_key: dict[tuple[str, str], dict] = {}
    for ln in lines:
        try:
            o = json.loads(ln)
        except json.JSONDecodeError:
            continue
        k = (str(o.get("pred_date") or ""), str(o.get("symbol") or "").upper())
        if k[0] and k[1]:
            by_key[k] = o
    path.write_text(
        "".join(json.dumps(v, separators=(",", ":"), allow_nan=False) + "\n" for v in sorted(by_key.values(), key=lambda x: (x.get("pred_date"), x.get("symbol")))),
        encoding="utf-8",
    )


def score_prediction_file(
    pred_path: Path,
    borrow_symbols: dict[str, list[dict]],
    *,
    horizon_days: int,
) -> list[dict]:
    payload = _load_json(pred_path)
    pred_date = str(payload.get("as_of") or pred_path.stem)
    if not pred_date:
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
        labels = compute_borrow_spike_event_by_date(hist, horizon_days=horizon_days)
        y = labels.get(pred_date)
        if y is None:
            continue
        p = row.get("p_spike_5d") if isinstance(row, dict) else None
        if p is None or not isinstance(p, (int, float)) or not math.isfinite(float(p)):
            continue
        p = float(p)
        out.append({
            "pred_date": pred_date,
            "symbol": sym,
            "horizon_days": horizon_days,
            "p_spike": round(p, 6),
            "y_spike": int(y),
            "risk_band": row.get("risk_band") if isinstance(row, dict) else _risk_band(p),
            "scoring_eligible": bool(row.get("scoring_eligible")) if isinstance(row, dict) else False,
            "scored_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        })
    return out


def rollup_metrics(realized_path: Path, *, max_lines: int = 50_000) -> dict[str, Any]:
    if not realized_path.exists():
        return {
            "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "n_rows": 0,
            "brier_score": None,
            "log_loss": None,
            "positive_rate": None,
            "calibration_by_band": [],
        }
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
        return {
            "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "n_rows": 0,
            "brier_score": None,
            "log_loss": None,
            "positive_rate": None,
            "calibration_by_band": [],
        }
    pairs: list[tuple[float, float, str]] = []
    for r in rows:
        if r.get("p_spike") is None or r.get("y_spike") is None:
            continue
        p = float(r["p_spike"])
        y = float(r["y_spike"])
        b = str(r.get("risk_band") or _risk_band(p))
        pairs.append((p, y, b))
    if not pairs:
        return {
            "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "n_rows": 0,
            "brier_score": None,
            "log_loss": None,
            "positive_rate": None,
            "calibration_by_band": [],
        }
    ps = [x[0] for x in pairs]
    ys = [x[1] for x in pairs]
    brier = sum((ps[i] - ys[i]) ** 2 for i in range(len(ps))) / len(ps)
    log_loss = sum(_sigmoid_log_loss(ps[i], ys[i]) for i in range(len(ps))) / len(ps)
    pos_rate = sum(ys) / len(ys)

    by_band: dict[str, list[tuple[float, float]]] = {}
    for p, y, b in pairs:
        by_band.setdefault(b, []).append((p, y))

    calib = []
    for bname in sorted(by_band):
        pairs = by_band[bname]
        if not pairs:
            continue
        p_avg = sum(x[0] for x in pairs) / len(pairs)
        y_avg = sum(x[1] for x in pairs) / len(pairs)
        calib.append({
            "band": bname,
            "count": len(pairs),
            "avg_pred": round(p_avg, 6),
            "realized_rate": round(y_avg, 6),
        })

    return {
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "n_rows": len(rows),
        "brier_score": round(brier, 6),
        "log_loss": round(log_loss, 6),
        "positive_rate": round(pos_rate, 6),
        "calibration_by_band": calib,
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


def score_from_repo(
    repo_root: Path | None = None,
    *,
    dedupe: bool = True,
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

    existing = _existing_realized_keys(realized_path)
    new_rows: list[dict] = []
    if pred_dir.exists():
        for pred_path in sorted(pred_dir.glob("*.json")):
            stem = pred_path.stem
            if len(stem) != 10 or stem[4] != "-" or stem[7] != "-":
                continue
            pl = _load_json(pred_path)
            h = int(pl.get("horizon_days") or 5)
            batch = score_prediction_file(pred_path, borrow_symbols, horizon_days=h)
            for r in batch:
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
    parser.add_argument(
        "--repo-root",
        type=str,
        default=str(REPO_ROOT),
        help="Repository root (parent of data/)",
    )
    parser.add_argument("--no-dedupe", action="store_true", help="Do not rewrite realized JSONL deduped")
    args = parser.parse_args()
    m = score_from_repo(Path(args.repo_root), dedupe=not args.no_dedupe)
    print(
        f"[OK] borrow_spike_metrics: n_rows={m.get('n_rows')} "
        f"brier={m.get('brier_score')} log_loss={m.get('log_loss')} "
        f"positive_rate={m.get('positive_rate')}",
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Rolling live calibration monitor for L2 borrow stress tiers."""
from __future__ import annotations

import json
import math
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
ELEVATED_TIERS = frozenset({"elevated", "high", "watch"})
ELEVATED_STRICT = frozenset({"elevated", "high"})


def _load_jsonl(path: Path, *, max_lines: int = 100_000) -> list[dict]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
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
    return rows


def _parse_date(s: str) -> date | None:
    try:
        return date.fromisoformat(str(s)[:10])
    except ValueError:
        return None


def build_live_calibration_monitor(
    realized_path: Path,
    *,
    window_days: int = 60,
    today: date | None = None,
    elevated_floor: float = 0.05,
) -> dict[str, Any]:
    """Compute rolling elevated-tier hit rate from scored live predictions."""
    ref = today or datetime.now(UTC).date()
    cutoff = ref - timedelta(days=window_days)
    rows = _load_jsonl(realized_path)
    if not rows:
        return {
            "status": "no_data",
            "window_days": window_days,
            "n_rows": 0,
            "elevated_tier_hit_rate": None,
            "elevated_strict_hit_rate": None,
            "alert": None,
        }

    df = pd.DataFrame(rows)
    if df.empty or "pred_date" not in df.columns:
        return {"status": "no_data", "window_days": window_days, "n_rows": 0}

    df["pred_date_parsed"] = df["pred_date"].map(_parse_date)
    df = df[df["pred_date_parsed"].notna()].copy()
    df = df[df["pred_date_parsed"] >= cutoff]

    # Prefer L2 columns when present; fall back to L0 risk_band.
    y_col = "y_spike_l2" if "y_spike_l2" in df.columns else "y_spike"
    tier_col = "alert_tier" if "alert_tier" in df.columns else "risk_band"
    if y_col not in df.columns:
        return {"status": "no_labels", "window_days": window_days, "n_rows": int(len(df))}

    df[y_col] = pd.to_numeric(df[y_col], errors="coerce")
    df = df[df[y_col].notna()].copy()
    if df.empty:
        return {"status": "no_labels", "window_days": window_days, "n_rows": 0}

    tiers = df[tier_col].astype(str).str.lower() if tier_col in df.columns else pd.Series([""] * len(df))
    elevated_mask = tiers.isin(ELEVATED_TIERS)
    elevated_strict = tiers.isin(ELEVATED_STRICT)
    sub = df[elevated_mask]
    sub_strict = df[elevated_strict]

    hit_rate = float(sub[y_col].mean()) if len(sub) else None
    hit_strict = float(sub_strict[y_col].mean()) if len(sub_strict) else None
    alert = None
    if hit_strict is not None and len(sub_strict) >= 10 and hit_strict < elevated_floor:
        alert = f"elevated_strict_hit_rate {hit_strict:.3f} < floor {elevated_floor}"

    return {
        "status": "ok",
        "window_days": window_days,
        "as_of": ref.isoformat(),
        "cutoff_date": cutoff.isoformat(),
        "n_rows": int(len(df)),
        "n_elevated_tier": int(elevated_mask.sum()),
        "n_elevated_strict": int(elevated_strict.sum()),
        "elevated_tier_hit_rate": round(hit_rate, 6) if hit_rate is not None and math.isfinite(hit_rate) else None,
        "elevated_strict_hit_rate": round(hit_strict, 6) if hit_strict is not None and math.isfinite(hit_strict) else None,
        "elevated_floor": elevated_floor,
        "alert": alert,
        "label_column": y_col,
    }

#!/usr/bin/env python3
"""P50 forecast of 5-observation borrow change from predictor panel."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from borrow_model_common import shrink_delta  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
PANEL_PARQUET = DATA_DIR / "borrow_predictor_panel.parquet"
FORECAST_JSON = DATA_DIR / "borrow_forecast_latest.json"

FORECAST_FEATURE_WEIGHTS = {
    "borrow_current": 0.42,
    "borrow_slope5": 0.28,
    "borrow_vol10": 0.18,
    "borrow_z60": 0.12,
}


def _load_panel(repo_root: Path) -> pd.DataFrame:
    path = repo_root / "data" / "borrow_predictor_panel.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _load_registry(repo_root: Path) -> dict:
    path = repo_root / "data" / "borrow_model_registry.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _fit_pooled_delta_model(panel: pd.DataFrame) -> tuple[dict[str, float] | None, float]:
    if panel.empty or "delta_borrow_5" not in panel.columns:
        return None, 0.05
    work = panel.dropna(subset=["delta_borrow_5"]).copy()
    if len(work) < 100:
        return None, 0.05
    feats = [c for c in FORECAST_FEATURE_WEIGHTS if c in work.columns]
    if not feats:
        return None, 0.05
    y = work["delta_borrow_5"].to_numpy(dtype=float)
    X = work[feats].to_numpy(dtype=float)
    X = np.column_stack([np.ones(len(X)), X])
    try:
        beta, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
    except np.linalg.LinAlgError:
        return None, 0.05
    coef = {"intercept": float(beta[0]), **{f: float(beta[i + 1]) for i, f in enumerate(feats)}}
    resid = y - X @ beta
    resid_std = float(np.std(resid)) if len(resid) > 10 else 0.05
    return coef, resid_std


def _predict_row(row: pd.Series, coef: dict[str, float]) -> float:
    v = float(coef.get("intercept", 0))
    for f, w in coef.items():
        if f == "intercept":
            continue
        x = row.get(f)
        v += float(w) * (float(x) if x is not None and np.isfinite(x) else 0.0)
    return v


def _boosting_forecast(repo_root: Path, panel: pd.DataFrame) -> dict[str, dict] | None:
    reg = _load_registry(repo_root)
    if reg.get("drift", {}).get("winner") != "boosting":
        return None
    try:
        from borrow_boosting_model import build_drift_forecast_from_panel, load_bundle

        bundle = load_bundle(repo_root / "data" / "borrow_boosting_bundle.pkl")
        if bundle is None or bundle.drift_model is None:
            return None
        return build_drift_forecast_from_panel(panel, bundle)
    except Exception:
        return None


def build_forecast_payload(repo_root: Path) -> dict[str, Any]:
    panel = _load_panel(repo_root)
    coef, resid_std = _fit_pooled_delta_model(panel)
    if coef is None:
        coef = {"intercept": 0.0, **FORECAST_FEATURE_WEIGHTS}

    reg = _load_registry(repo_root)
    method = "pooled_ols_top_features_shrunk"
    boosting_by = _boosting_forecast(repo_root, panel) if not panel.empty else None
    if boosting_by:
        method = f"boosting_drift_{reg.get('boosting', {}).get('backend', 'ml')}"

    by_symbol: dict[str, dict] = {}
    if not panel.empty:
        panel = panel.copy()
        panel["date"] = pd.to_datetime(panel["date"], errors="coerce")
        if "obs_count" not in panel.columns:
            panel["obs_count"] = panel.groupby("symbol")["borrow_current"].transform(lambda s: s.notna().sum())
        latest = panel.sort_values("date").groupby("symbol", as_index=False).tail(1)
        for _, row in latest.iterrows():
            sym = str(row["symbol"]).upper()
            obs = row.get("obs_count")
            if boosting_by and sym in boosting_by:
                rec = dict(boosting_by[sym])
                p50 = float(rec.get("delta_borrow_5d_p50") or 0)
                half_iqr = 0.67 * resid_std
                rec["delta_borrow_5d_p25"] = round(p50 - half_iqr, 6)
                rec["delta_borrow_5d_p75"] = round(p50 + half_iqr, 6)
                by_symbol[sym] = rec
                continue
            p50_raw = _predict_row(row, coef)
            p50 = shrink_delta(p50_raw, obs)
            cur = float(row.get("borrow_current") or 0)
            half_iqr = 0.67 * resid_std
            by_symbol[sym] = {
                "delta_borrow_5d_p50": round(p50, 6),
                "delta_borrow_5d_p25": round(p50 - half_iqr, 6),
                "delta_borrow_5d_p75": round(p50 + half_iqr, 6),
                "borrow_current": round(cur, 6),
                "borrow_forecast_5d_p50": round(cur + p50, 6),
                "obs_count": int(obs) if obs is not None and np.isfinite(obs) else None,
                "as_of_date": row["date"].strftime("%Y-%m-%d") if pd.notna(row.get("date")) else None,
                "method": method,
            }

    return {
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "horizon_observations": 5,
        "target": "delta_borrow_5",
        "method": method,
        "ols_fallback_method": "pooled_ols_top_features_shrunk",
        "coefficients": coef,
        "residual_std": round(resid_std, 6),
        "n_symbols": len(by_symbol),
        "by_symbol": by_symbol,
        "registry_winner": reg.get("drift", {}).get("winner"),
        "interpretation": (
            "Positive delta_borrow_5d_p50 means expected increase in annual borrow fee "
            "over the next 5 IBKR/git observations. Low R² (~5%) — use as directional stress, not point forecast."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build borrow level forecast artifact.")
    parser.add_argument("--repo-root", type=str, default=str(REPO_ROOT))
    args = parser.parse_args()
    repo_root = Path(args.repo_root)
    payload = build_forecast_payload(repo_root)
    out = repo_root / "data" / "borrow_forecast_latest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"[OK] borrow_forecast_latest.json: {payload.get('n_symbols')} symbols ({payload.get('method')})")


if __name__ == "__main__":
    main()

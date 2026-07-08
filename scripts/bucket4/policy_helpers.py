"""Policy + price helpers for Bucket 4 backtest builder."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from .bucket4_hedge_cadence import HedgeCadenceKnobs, load_policy_from_config
from .bucket4_price_loading import load_price_panel as _load_price_panel

REPO = Path(__file__).resolve().parents[2]
DEFAULT_POLICY_PATH = REPO / "config" / "bucket4_backtest_policy.yml"


def load_policy(path: Path | str | None = None) -> dict[str, Any]:
    p = Path(path) if path else DEFAULT_POLICY_PATH
    if not p.is_file():
        raise FileNotFoundError(f"Bucket 4 policy not found: {p}")
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}


def knobs_from_policy(policy: dict[str, Any] | None = None) -> dict[str, Any]:
    pol = policy or load_policy()
    rules = (pol.get("inverse_decay_bucket4") or {}).get("rules") or {}
    blk = (
        rules.get("hedge_cadence_policy")
        or (rules.get("bucket4_weekly_opt2") or {}).get("hedge_cadence_policy")
        or {}
    )
    return dict(blk)


def make_knobs(base_blk: dict, **over) -> HedgeCadenceKnobs:
    fields = {f: base_blk[f] for f in HedgeCadenceKnobs.__dataclass_fields__ if f in base_blk}
    fields.update(over)
    return HedgeCadenceKnobs(**fields)


def knobs_and_tilts_from_policy(policy: dict[str, Any] | None = None):
    pol = policy or load_policy()
    return load_policy_from_config(pol)


def load_price_panel(**kwargs) -> dict[str, pd.DataFrame]:
    return _load_price_panel(**kwargs)

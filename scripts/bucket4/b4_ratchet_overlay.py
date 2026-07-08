"""Sim-only grow-only inverse ratchet overlay for Bucket 4 backtests."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

import numpy as np


def _norm(sym: str) -> str:
    return str(sym).strip().upper().replace(".", "-")


def pair_key(etf: str, und: str) -> str:
    return f"{_norm(etf)}|{_norm(und)}"


@dataclass
class RatchetConfig:
    enabled: bool = False
    state_json: str = "data/b4_inverse_ratchet_state.json"
    small_epsilon: float = 100.0
    trim_enabled: bool = True
    trim_max: float = 0.5
    trim_creep_full: float = 1.0
    trim_k_edge: float = 0.3
    trim_edge_floor: float = 0.25
    trim_k_borrow: float = 0.0
    trim_edge_ref: float = 0.30
    trim_borrow_ref: float = 0.30
    release_below_edge: float | None = None

    @classmethod
    def from_cfg(cls, raw: Mapping[str, Any] | None) -> "RatchetConfig":
        raw = dict(raw or {})
        trim = dict(raw.get("trim") or {})
        return cls(
            enabled=bool(raw.get("enabled", False)),
            state_json=str(raw.get("state_json") or "data/b4_inverse_ratchet_state.json"),
            small_epsilon=float(raw.get("small_epsilon", 100.0)),
            trim_enabled=bool(trim.get("enabled", True)),
            trim_max=float(trim.get("trim_max", 0.5)),
            trim_creep_full=float(trim.get("creep_full", 1.0)),
            trim_k_edge=float(trim.get("k_edge", 0.3)),
            trim_edge_floor=float(trim.get("edge_floor", 0.25)),
            trim_k_borrow=float(trim.get("k_borrow", 0.0)),
            trim_edge_ref=float(trim.get("edge_ref", 0.30)),
            trim_borrow_ref=float(trim.get("borrow_ref", 0.30)),
            release_below_edge=(
                float(raw["release_below_edge"])
                if raw.get("release_below_edge") is not None
                else None
            ),
        )


def _continuous_ratchet_trim_rate(
    fwd_edge: float,
    borrow: float,
    creep_ratio: float,
    *,
    trim_max: float,
    creep_full: float,
    k_edge: float,
    edge_floor: float,
    k_borrow: float,
    edge_ref: float,
    borrow_ref: float,
) -> float:
    if trim_max <= 0.0:
        return 0.0
    excess = max(0.0, float(creep_ratio) - 1.0)
    if excess <= 1e-9:
        return 0.0
    gap_term = float(np.clip(excess / max(float(creep_full), 1e-6), 0.0, 1.0))
    e = float(fwd_edge) if (fwd_edge is not None and np.isfinite(fwd_edge)) else float(edge_ref)
    b = float(borrow) if (borrow is not None and np.isfinite(borrow)) else float(borrow_ref)
    edge_mult = float(np.clip(1.0 - float(k_edge) * (e - float(edge_ref)), float(edge_floor), 1.5))
    borrow_mult = float(np.clip(1.0 + float(k_borrow) * (b - float(borrow_ref)), 0.0, 2.0))
    lam = float(trim_max) * gap_term * edge_mult * borrow_mult
    return float(np.clip(lam, 0.0, float(trim_max)))


def load_ratchet_state(path: Path | str) -> dict[str, float]:
    p = Path(path)
    try:
        if p.is_file():
            raw = json.loads(p.read_text(encoding="utf-8"))
            d = raw.get("inverse_short_usd_by_pair", raw) if isinstance(raw, dict) else {}
            return {
                str(k): float(v)
                for k, v in (d or {}).items()
                if v is not None and np.isfinite(float(v))
            }
    except Exception:
        pass
    return {}


def write_ratchet_state(path: Path | str, state: Mapping[str, float], run_date: str) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_date": str(run_date),
        "inverse_short_usd_by_pair": {str(k): round(float(v), 2) for k, v in sorted(state.items())},
    }
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(p)


@dataclass
class RatchetResult:
    inv_short_usd: float
    inv_short_solved_usd: float
    floor_usd: float
    binding: bool
    released: bool
    trim_lambda: float
    source: str
    creep_ratio: float
    gap_usd: float
    trim_usd: float


def apply_inverse_ratchet(
    inv_short_solved: float,
    *,
    held_usd: float,
    persisted_floor_usd: float,
    cfg: RatchetConfig,
    fwd_edge: float = float("nan"),
    borrow: float = float("nan"),
) -> RatchetResult:
    inv_short_solved = max(0.0, float(inv_short_solved))
    if not cfg.enabled:
        return RatchetResult(
            inv_short_usd=inv_short_solved,
            inv_short_solved_usd=inv_short_solved,
            floor_usd=inv_short_solved,
            binding=False,
            released=False,
            trim_lambda=0.0,
            source="solve",
            creep_ratio=1.0,
            gap_usd=0.0,
            trim_usd=0.0,
        )

    held_usd = max(0.0, float(held_usd))
    persisted_floor_usd = max(0.0, float(persisted_floor_usd))
    floor_val = max(inv_short_solved, held_usd, persisted_floor_usd)
    gap = floor_val - inv_short_solved
    creep_ratio = (floor_val / inv_short_solved) if inv_short_solved > 1e-9 else float("inf")

    inv_short_usd = floor_val
    binding = False
    released = False
    trim_lambda = 0.0
    trim_usd = 0.0
    source = "solve"

    if cfg.trim_enabled and gap > float(cfg.small_epsilon):
        trim_lambda = _continuous_ratchet_trim_rate(
            fwd_edge,
            borrow,
            creep_ratio,
            trim_max=cfg.trim_max,
            creep_full=cfg.trim_creep_full,
            k_edge=cfg.trim_k_edge,
            edge_floor=cfg.trim_edge_floor,
            k_borrow=cfg.trim_k_borrow,
            edge_ref=cfg.trim_edge_ref,
            borrow_ref=cfg.trim_borrow_ref,
        )
        inv_short_usd = floor_val - trim_lambda * gap
        trim_usd = trim_lambda * gap
        binding = inv_short_usd > inv_short_solved + 1e-6
        if trim_lambda > 1e-9:
            released = True
            source = "edge_trim"
        elif binding:
            source = "held_position" if held_usd >= persisted_floor_usd else "ratchet_state"
    else:
        edge_release = (
            cfg.release_below_edge is not None
            and np.isfinite(fwd_edge)
            and float(fwd_edge) < float(cfg.release_below_edge)
        )
        if edge_release:
            inv_short_usd = inv_short_solved
            released = True
            source = "edge_release"
        else:
            inv_short_usd = floor_val
            binding = inv_short_usd > inv_short_solved + 1e-6
            if binding:
                source = "held_position" if held_usd >= persisted_floor_usd else "ratchet_state"

    return RatchetResult(
        inv_short_usd=float(max(0.0, inv_short_usd)),
        inv_short_solved_usd=float(inv_short_solved),
        floor_usd=float(floor_val),
        binding=bool(binding),
        released=bool(released),
        trim_lambda=float(trim_lambda),
        source=source,
        creep_ratio=float(creep_ratio),
        gap_usd=float(gap),
        trim_usd=float(trim_usd),
    )


@dataclass
class SimRatchetState:
    """Backtest-only ratchet state keyed by ETF|UND."""

    cfg: RatchetConfig
    floors: dict[str, float] = field(default_factory=dict)
    held_gross: dict[str, float] = field(default_factory=dict)

    def apply_gross_multiplier(
        self,
        etf: str,
        und: str,
        solved_gross: float,
        *,
        fwd_edge: float,
        borrow: float,
    ) -> tuple[float, RatchetResult]:
        """Return effective gross multiplier after ratchet (unit-normalized book)."""
        pk = pair_key(etf, und)
        held = float(self.held_gross.get(pk, 0.0))
        persisted = float(self.floors.get(pk, 0.0))
        res = apply_inverse_ratchet(
            solved_gross,
            held_usd=held,
            persisted_floor_usd=persisted,
            cfg=self.cfg,
            fwd_edge=fwd_edge,
            borrow=borrow,
        )
        eff = res.inv_short_usd / solved_gross if solved_gross > 1e-9 else 1.0
        return float(eff), res

    def record_rebalance(self, etf: str, und: str, final_gross: float) -> None:
        pk = pair_key(etf, und)
        prior = float(self.floors.get(pk, 0.0))
        self.floors[pk] = max(prior, float(max(0.0, final_gross)))
        self.held_gross[pk] = float(max(0.0, final_gross))

    def as_dict(self) -> dict[str, Any]:
        return {
            "enabled": self.cfg.enabled,
            "inverse_short_usd_by_pair": dict(sorted(self.floors.items())),
            "held_gross_by_pair": dict(sorted(self.held_gross.items())),
        }

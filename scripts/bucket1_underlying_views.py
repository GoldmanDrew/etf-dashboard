"""Bucket 1 underlying views — score → hedge coverage h.

Research/ops overlay. Does not rewrite screener delta or production sizing.
See docs/bucket1_underlying_views_plan.md.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Mapping

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "config" / "bucket1_underlying_views.yml"
DEFAULT_ARTIFACT = ROOT / "data" / "bucket1_underlying_views.json"

SCHEMA = "bucket1_underlying_views.v1"
DEFAULT_SCORE_TO_H: dict[int, float] = {
    -2: 0.25,
    -1: 0.625,
    0: 1.0,
    1: 1.25,
    2: 1.50,
}
DEFAULT_H_MIN = 0.25
DEFAULT_H_MAX = 1.75
VALID_SCORES = frozenset(DEFAULT_SCORE_TO_H)

SCORE_LABELS: dict[int, str] = {
    -2: "max short",
    -1: "lean short",
    0: "no opinion",
    1: "lean long",
    2: "max long",
}


def _norm_sym(sym: Any) -> str:
    return str(sym or "").strip().upper()


def _load_yaml(path: Path) -> dict[str, Any]:
    import yaml  # type: ignore

    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise ValueError(f"expected mapping in {path}")
    return raw


def load_config(path: Path | None = None) -> dict[str, Any]:
    cfg_path = Path(path) if path is not None else DEFAULT_CONFIG
    raw = _load_yaml(cfg_path) if cfg_path.is_file() else {}
    score_to_h = dict(DEFAULT_SCORE_TO_H)
    raw_ladder = raw.get("score_to_h") or {}
    if isinstance(raw_ladder, Mapping):
        for k, v in raw_ladder.items():
            score_to_h[int(k)] = float(v)
    h_min = float(raw.get("h_min", DEFAULT_H_MIN))
    h_max = float(raw.get("h_max", DEFAULT_H_MAX))
    if h_min > h_max:
        h_min, h_max = h_max, h_min

    views: dict[str, dict[str, Any]] = {}
    raw_views = raw.get("views") or {}
    if isinstance(raw_views, Mapping):
        for und, spec in raw_views.items():
            key = _norm_sym(und)
            if not key:
                continue
            if not isinstance(spec, Mapping):
                spec = {"score": spec}
            try:
                score = int(spec.get("score", 0))
            except (TypeError, ValueError):
                score = 0
            if score not in VALID_SCORES:
                score = 0
            views[key] = {
                "score": score,
                "note": str(spec.get("note") or "").strip(),
                "updated": str(spec.get("updated") or "").strip() or None,
            }

    return {
        "schema": str(raw.get("schema") or SCHEMA),
        "score_to_h": score_to_h,
        "h_min": h_min,
        "h_max": h_max,
        "views": views,
        "config_path": str(cfg_path),
    }


def clip_h(h: float, *, h_min: float = DEFAULT_H_MIN, h_max: float = DEFAULT_H_MAX) -> float:
    return float(min(h_max, max(h_min, h)))


def h_for_score(
    score: int,
    *,
    score_to_h: Mapping[int, float] | None = None,
    h_min: float = DEFAULT_H_MIN,
    h_max: float = DEFAULT_H_MAX,
) -> float:
    ladder = score_to_h or DEFAULT_SCORE_TO_H
    try:
        s = int(score)
    except (TypeError, ValueError):
        s = 0
    if s not in VALID_SCORES:
        s = 0
    raw = float(ladder.get(s, ladder.get(0, 1.0)))
    return clip_h(raw, h_min=h_min, h_max=h_max)


def resolve_view(
    underlying: str,
    *,
    config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    cfg = dict(config or load_config())
    und = _norm_sym(underlying)
    views = cfg.get("views") or {}
    entry = views.get(und) if isinstance(views, Mapping) else None
    if not isinstance(entry, Mapping):
        entry = {}
    try:
        score = int(entry.get("score", 0))
    except (TypeError, ValueError):
        score = 0
    if score not in VALID_SCORES:
        score = 0
    h = h_for_score(
        score,
        score_to_h=cfg.get("score_to_h"),
        h_min=float(cfg.get("h_min", DEFAULT_H_MIN)),
        h_max=float(cfg.get("h_max", DEFAULT_H_MAX)),
    )
    return {
        "underlying": und,
        "score": score,
        "score_label": SCORE_LABELS.get(score, "no opinion"),
        "h": h,
        "note": str(entry.get("note") or "").strip(),
        "updated": entry.get("updated"),
        "from_config": und in views,
    }


def bucket1_records(records: list[Mapping[str, Any]] | None) -> list[Mapping[str, Any]]:
    out: list[Mapping[str, Any]] = []
    for r in records or []:
        if not isinstance(r, Mapping):
            continue
        if str(r.get("bucket") or "") != "bucket_1_high_beta":
            continue
        out.append(r)
    return out


def build_payload(
    *,
    config: Mapping[str, Any] | None = None,
    records: list[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    cfg = dict(config or load_config())
    b1 = bucket1_records(records)

    sleeves_by_und: dict[str, list[str]] = {}
    for r in b1:
        und = _norm_sym(r.get("underlying"))
        sym = _norm_sym(r.get("symbol"))
        if not und or not sym:
            continue
        sleeves_by_und.setdefault(und, [])
        if sym not in sleeves_by_und[und]:
            sleeves_by_und[und].append(sym)

    # Include configured views even if not currently in B1 (ops visibility).
    all_unds = sorted(set(sleeves_by_und) | set((cfg.get("views") or {})))

    by_underlying: dict[str, dict[str, Any]] = {}
    for und in all_unds:
        resolved = resolve_view(und, config=cfg)
        sleeves = sorted(sleeves_by_und.get(und, []))
        by_underlying[und] = {
            **resolved,
            "sleeves": sleeves,
            "n_sleeves": len(sleeves),
            "in_bucket1": bool(sleeves),
        }

    active = [u for u, row in by_underlying.items() if int(row.get("score") or 0) != 0]
    score_to_h = {str(k): float(v) for k, v in dict(cfg.get("score_to_h") or DEFAULT_SCORE_TO_H).items()}

    return {
        "schema": str(cfg.get("schema") or SCHEMA),
        "h_definition": "long_und_dollars_per_1_short_etf",
        "score_to_h": score_to_h,
        "h_min": float(cfg.get("h_min", DEFAULT_H_MIN)),
        "h_max": float(cfg.get("h_max", DEFAULT_H_MAX)),
        "score_labels": {str(k): v for k, v in SCORE_LABELS.items()},
        "n_underlyings": len(by_underlying),
        "n_bucket1_underlyings": len(sleeves_by_und),
        "n_active_views": len(active),
        "by_underlying": by_underlying,
        "config_path": cfg.get("config_path"),
    }


def normalize_views_patch(raw_views: Mapping[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Normalize UI/API view patches. Score 0 with empty note is dropped."""
    out: dict[str, dict[str, Any]] = {}
    today = date.today().isoformat()
    for und, spec in dict(raw_views or {}).items():
        key = _norm_sym(und)
        if not key:
            continue
        if not isinstance(spec, Mapping):
            try:
                score = int(spec)
            except (TypeError, ValueError):
                score = 0
            spec = {"score": score}
        try:
            score = int(spec.get("score", 0))
        except (TypeError, ValueError):
            score = 0
        if score not in VALID_SCORES:
            score = 0
        note = str(spec.get("note") or "").strip()
        if score == 0 and not note:
            continue
        updated = str(spec.get("updated") or "").strip() or today
        out[key] = {"score": score, "note": note, "updated": updated}
    return out


def save_views_config(
    views_patch: Mapping[str, Any] | None,
    *,
    path: Path | None = None,
    replace: bool = True,
) -> dict[str, Any]:
    """Write ``views`` into the YAML config (preserves ladder / bounds).

    ``replace=True`` (default): patch is the full views map (UI save).
    ``replace=False``: merge patch into existing views.
    """
    import yaml  # type: ignore

    cfg_path = Path(path) if path is not None else DEFAULT_CONFIG
    cfg = load_config(cfg_path)
    patch = normalize_views_patch(views_patch)
    if replace:
        views = patch
    else:
        views = dict(cfg.get("views") or {})
        views.update(patch)
        # Drop neutrals after merge
        views = normalize_views_patch(views)

    score_to_h = {int(k): float(v) for k, v in dict(cfg.get("score_to_h") or DEFAULT_SCORE_TO_H).items()}
    doc = {
        "schema": str(cfg.get("schema") or SCHEMA),
        "score_to_h": {str(k): float(v) for k, v in sorted(score_to_h.items(), key=lambda kv: kv[0])},
        "h_min": float(cfg.get("h_min", DEFAULT_H_MIN)),
        "h_max": float(cfg.get("h_max", DEFAULT_H_MAX)),
        "views": {
            und: {
                "score": int(spec["score"]),
                "note": str(spec.get("note") or ""),
                "updated": str(spec.get("updated") or date.today().isoformat()),
            }
            for und, spec in sorted(views.items())
        },
    }
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    header = (
        "# Bucket 1 underlying views — discretionary hedge coverage overlay.\n"
        "# Research/ops only until explicitly wired into production sizing.\n"
        "# Edited via dashboard B1 Views Save (or hand-edit).\n"
    )
    body = yaml.safe_dump(doc, default_flow_style=False, sort_keys=False, allow_unicode=True)
    cfg_path.write_text(header + body, encoding="utf-8")
    return load_config(cfg_path)


def write_views_artifact(
    *,
    config: Mapping[str, Any] | None = None,
    records: list[Mapping[str, Any]] | None = None,
    out_path: Path | None = None,
) -> dict[str, Any]:
    """Rebuild ``data/bucket1_underlying_views.json`` from config (+ optional rows)."""
    from datetime import datetime, timezone

    payload = build_payload(config=config, records=records)
    payload["generated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    path = Path(out_path) if out_path is not None else DEFAULT_ARTIFACT
    path.parent.mkdir(parents=True, exist_ok=True)
    import json

    path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    return payload


__all__ = [
    "DEFAULT_ARTIFACT",
    "DEFAULT_CONFIG",
    "DEFAULT_SCORE_TO_H",
    "SCORE_LABELS",
    "VALID_SCORES",
    "build_payload",
    "bucket1_records",
    "clip_h",
    "h_for_score",
    "load_config",
    "normalize_views_patch",
    "resolve_view",
    "save_views_config",
    "write_views_artifact",
]

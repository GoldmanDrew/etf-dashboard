"""Split adjustment helpers for price-basis consistency across corporate actions."""
from __future__ import annotations

import datetime as dt
import json
import math
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CORPORATE_ACTIONS_PATH = REPO_ROOT / "data" / "corporate_actions.json"

_INTEGER_SPLIT_FACTORS: tuple[int, ...] = (2, 3, 4, 5, 6, 10, 15, 20, 25, 50)
SPLIT_RATIOS: tuple[float, ...] = tuple(
    sorted(
        set(list(_INTEGER_SPLIT_FACTORS) + [1.0 / f for f in _INTEGER_SPLIT_FACTORS]),
        reverse=True,
    )
)


def load_split_hints_from_corporate_actions(
    path: Path | None = None,
) -> dict[str, dict[dt.date, float]]:
    """Map ticker -> {calendar date -> price mult ``ratio_from / ratio_to``}."""
    out: dict[str, dict[dt.date, float]] = {}
    p = path or DEFAULT_CORPORATE_ACTIONS_PATH
    if not p.exists():
        return out
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return out
    for ev in payload.get("events") or []:
        if str(ev.get("type") or "") not in {"reverse_split", "forward_split"}:
            continue
        ticker = str(ev.get("ticker") or "").strip().upper()
        ed = ev.get("execution_date")
        rf, rt = ev.get("ratio_from"), ev.get("ratio_to")
        if not ticker or not ed or rf is None or rt is None:
            continue
        try:
            d0 = dt.date.fromisoformat(str(ed)[:10])
            mult = float(rf) / float(rt)
        except (ValueError, TypeError, ZeroDivisionError):
            continue
        if mult <= 0:
            continue
        for delta in (-1, 0, 1):
            out.setdefault(ticker, {})[d0 + dt.timedelta(days=delta)] = mult
    return out


def parse_yahoo_split_events(split_map: dict | None) -> list[tuple[dt.date, float]]:
    """Return sorted (execution_date, price_mult) from Yahoo chart ``events.splits``."""
    if not isinstance(split_map, dict):
        return []
    events: dict[dt.date, float] = {}
    for ts_key, payload in split_map.items():
        if not isinstance(payload, dict):
            continue
        num = payload.get("numerator")
        den = payload.get("denominator")
        if num is None or den is None:
            continue
        try:
            numerator = float(num)
            denominator = float(den)
            if numerator <= 0 or denominator <= 0:
                continue
            mult = denominator / numerator
        except (TypeError, ValueError):
            continue
        try:
            ev_ts = int(
                payload.get("date")
                if payload.get("date") is not None
                else ts_key
            )
            ev_date = dt.datetime.fromtimestamp(ev_ts, dt.UTC).date()
        except (ValueError, TypeError, OSError):
            continue
        events[ev_date] = mult
    return sorted(events.items())


def dedupe_split_events(
    events: list[tuple[dt.date, float]],
    *,
    max_day_spread: int = 5,
) -> list[tuple[dt.date, float]]:
    """Collapse duplicate corp-action hints (±N days) and Yahoo into one event per ratio."""
    if not events:
        return []
    clusters: list[list[tuple[dt.date, float]]] = []
    for d, m in sorted(events):
        if m <= 0:
            continue
        placed = False
        for cluster in clusters:
            cd, cm = cluster[0]
            if abs(float(m) - float(cm)) > 1e-6:
                continue
            if abs((d - cd).days) <= max_day_spread:
                cluster.append((d, float(m)))
                placed = True
                break
        if not placed:
            clusters.append([(d, float(m))])
    out: list[tuple[dt.date, float]] = []
    for cluster in clusters:
        # Prefer the latest date in the cluster (typically Yahoo's bar date).
        best = max(cluster, key=lambda x: x[0])
        out.append(best)
    return sorted(out)


def merge_split_events(
    *sources: list[tuple[dt.date, float]] | None,
    hints: dict[dt.date, float] | None = None,
) -> list[tuple[dt.date, float]]:
    """Merge split sources; later explicit entries win on duplicate dates."""
    merged: dict[dt.date, float] = {}
    for src in sources:
        if not src:
            continue
        for d, m in src:
            if m > 0:
                merged[d] = float(m)
    if hints:
        for d, m in hints.items():
            if m > 0:
                merged.setdefault(d, float(m))
    return dedupe_split_events(sorted(merged.items()))


def split_close_jump_ratio(
    points: list[tuple[dt.date, float, float]],
    effective: dt.date,
) -> float | None:
    """``close_after / close_before`` at the first bar on/after ``effective``."""
    if not points:
        return None
    before = [p for p in points if p[0] < effective]
    on_after = [p for p in points if p[0] >= effective]
    if not before or not on_after:
        return None
    c_before = float(before[-1][1])
    c_after = float(on_after[0][1])
    if not (math.isfinite(c_before) and math.isfinite(c_after) and c_before > 0):
        return None
    return c_after / c_before


def filter_splits_needing_close_basis_fix(
    points: list[tuple[dt.date, float, float]],
    events: list[tuple[dt.date, float]],
    *,
    rel_tol: float = 0.15,
) -> list[tuple[dt.date, float]]:
    """
    Keep only splits where Yahoo *close* jumps at the event (not already back-adjusted).

    When Yahoo's close series is continuous through a reverse split (common for recent
    listings), applying a mechanical factor would inflate pre-split closes ~6× and
    produce nonsense returns (e.g. MTYY −99%).
    """
    if not events:
        return []
    out: list[tuple[dt.date, float]] = []
    for eff, mult in events:
        jump = split_close_jump_ratio(points, eff)
        if jump is None:
            continue
        expected = nearest_split_ratio(jump, rel_tol=rel_tol)
        if expected is not None and abs(expected - float(mult)) <= max(1e-6, rel_tol * abs(mult)):
            out.append((eff, float(mult)))
    return out


def cum_split_factor(
    from_date: dt.date,
    to_date: dt.date,
    events: list[tuple[dt.date, float]],
) -> float:
    """Multiply a price at ``from_date`` onto the ``to_date`` raw-close basis."""
    if from_date == to_date or not events:
        return 1.0
    mult = 1.0
    if from_date < to_date:
        for eff, m in events:
            if from_date < eff <= to_date:
                mult *= m
    else:
        for eff, m in events:
            if to_date < eff <= from_date:
                mult /= m
    return mult if math.isfinite(mult) and mult > 0 else 1.0


def nearest_split_ratio(observed: float, *, rel_tol: float = 0.075) -> float | None:
    """Return whitelist ratio nearest ``observed`` when within tolerance."""
    if not math.isfinite(observed) or observed <= 0:
        return None
    best_r: float | None = None
    best_err = 1e9
    for r in SPLIT_RATIOS:
        err = abs(observed / float(r) - 1.0)
        if err < best_err:
            best_err = err
            best_r = float(r)
    if best_r is None or best_err > rel_tol:
        return None
    return best_r


def infer_split_factor_end_to_live(
    live_spot: float,
    end_close: float,
    *,
    known_factor: float | None = None,
    rel_tol: float = 0.075,
) -> float:
    """Infer split factor from live/end ratio when corp-action metadata is stale."""
    if known_factor is not None and math.isfinite(known_factor) and known_factor > 0:
        return float(known_factor)
    if not (math.isfinite(live_spot) and math.isfinite(end_close) and live_spot > 0 and end_close > 0):
        return 1.0
    ratio = live_spot / end_close
    if abs(ratio - 1.0) <= 0.02:
        return 1.0
    guessed = nearest_split_ratio(ratio, rel_tol=rel_tol)
    return guessed if guessed is not None else 1.0


def apply_split_adjustments_to_points(
    points: list[tuple[dt.date, float, float]],
    events: list[tuple[dt.date, float]],
) -> list[tuple[dt.date, float, float]]:
    """Scale (close, adj_close) on bars strictly before each effective split date."""
    if not points or not events:
        return points
    out: list[tuple[dt.date, float, float]] = []
    for d, px_close, px_adj in points:
        mult = 1.0
        for eff, m in events:
            if d < eff:
                mult *= m
        if mult != 1.0:
            out.append((d, float(px_close * mult), float(px_adj * mult)))
        else:
            out.append((d, px_close, px_adj))
    return out


def corporate_actions_build_time(path: Path | None = None) -> dt.datetime | None:
    p = path or DEFAULT_CORPORATE_ACTIONS_PATH
    if not p.exists():
        return None
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
        raw = payload.get("build_time")
        if not raw:
            return None
        return dt.datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (OSError, json.JSONDecodeError, ValueError, TypeError):
        return None

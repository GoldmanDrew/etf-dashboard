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


def load_split_events_for_ticker(
    ticker: str,
    path: Path | None = None,
) -> list[tuple[dt.date, float]]:
    """Deduped split events for one symbol from ``corporate_actions.json``."""
    sym = str(ticker or "").strip().upper()
    hints = load_split_hints_from_corporate_actions(path).get(sym) or {}
    if not hints:
        return []
    return dedupe_split_events(sorted((d, float(m)) for d, m in hints.items() if m > 0))


def load_split_execution_events_for_ticker(
    ticker: str,
    path: Path | None = None,
) -> list[tuple[dt.date, float]]:
    """Declared ``execution_date`` split events only (no ±1 day hint padding).

    Used when scaling pre-split ``etf_adj_close`` onto the latest basis: hint
    clusters prefer the *latest* bar date for close-basis repair, which would
    incorrectly scale rows on the split session if reused for cum factors.
    """
    sym = str(ticker or "").strip().upper()
    if not sym:
        return []
    p = path or DEFAULT_CORPORATE_ACTIONS_PATH
    if not p.exists():
        return []
    try:
        payload = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    events: list[tuple[dt.date, float]] = []
    for ev in payload.get("events") or []:
        if str(ev.get("type") or "") not in {"reverse_split", "forward_split"}:
            continue
        if str(ev.get("ticker") or "").strip().upper() != sym:
            continue
        ed = ev.get("execution_date")
        rf, rt = ev.get("ratio_from"), ev.get("ratio_to")
        if not ed or rf is None or rt is None:
            continue
        try:
            d0 = dt.date.fromisoformat(str(ed)[:10])
            mult = float(rf) / float(rt)
        except (ValueError, TypeError, ZeroDivisionError):
            continue
        if mult > 0:
            events.append((d0, mult))
    return dedupe_split_events(sorted(events))


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


def match_split_to_price_jump(
    jump: float,
    declared_mult: float | None = None,
    *,
    jump_tol: float = 0.18,
    rel_tol: float = 0.075,
) -> float | None:
    """Trust ``declared_mult`` when the observed jump is within tolerance; else whitelist."""
    if not math.isfinite(jump) or jump <= 0:
        return None
    if declared_mult is not None and math.isfinite(declared_mult) and declared_mult > 0:
        if abs(jump / float(declared_mult) - 1.0) <= jump_tol:
            return float(declared_mult)
    return nearest_split_ratio(jump, rel_tol=rel_tol)


def confirm_split_from_shares_or_nav(
    *,
    shares_prev: float | None = None,
    shares_curr: float | None = None,
    nav_prev: float | None = None,
    nav_curr: float | None = None,
    declared_mult: float | None = None,
    rel_tol: float = 0.15,
) -> float | None:
    """Secondary issuer confirmation when the raw close jump is missing or ambiguous."""
    candidates: list[float] = []
    try:
        sh_p = float(shares_prev) if shares_prev is not None else float("nan")
        sh_c = float(shares_curr) if shares_curr is not None else float("nan")
        if math.isfinite(sh_p) and math.isfinite(sh_c) and sh_p > 0 and sh_c > 0:
            candidates.append(sh_p / sh_c)
    except (TypeError, ValueError):
        pass
    try:
        nav_p = float(nav_prev) if nav_prev is not None else float("nan")
        nav_c = float(nav_curr) if nav_curr is not None else float("nan")
        if math.isfinite(nav_p) and math.isfinite(nav_c) and nav_p > 0 and nav_c > 0:
            candidates.append(nav_c / nav_p)
    except (TypeError, ValueError):
        pass
    for obs in candidates:
        matched = match_split_to_price_jump(
            obs,
            declared_mult,
            jump_tol=rel_tol,
            rel_tol=rel_tol,
        )
        if matched is not None:
            if declared_mult is None or abs(matched - float(declared_mult)) <= max(
                1e-6, rel_tol * abs(float(declared_mult))
            ):
                return float(declared_mult) if declared_mult is not None else matched
    return None


def yahoo_adj_looks_back_adjusted(
    points: list[tuple[dt.date, float, float]],
    effective: dt.date,
    mult: float,
    *,
    rel_tol: float = 0.15,
) -> bool:
    """True when Yahoo adj is back-adjusted through a *continuous* raw close series."""
    if not points or mult <= 0:
        return False
    before = [p for p in points if p[0] < effective]
    on_after = [p for p in points if p[0] >= effective]
    if not before or not on_after:
        return False
    close_before = float(before[-1][1])
    close_after = float(on_after[0][1])
    adj_before = float(before[-1][2])
    adj_after = float(on_after[0][2])
    if not all(
        math.isfinite(x) and x > 0
        for x in (close_before, close_after, adj_before, adj_after)
    ):
        return False
    # Raw close jumped at the split → adj≡close listings are unadjusted, not back-adjusted.
    if abs(close_after / close_before - 1.0) > 0.08:
        return False
    ratio = adj_after / adj_before
    # Symmetric: forward splits (mult<1) show ratio≈1/mult; reverse (mult>1) show ratio≈mult.
    return (
        abs(ratio * mult - 1.0) <= rel_tol
        or abs(ratio / mult - 1.0) <= rel_tol
        or abs(adj_before * mult / adj_after - 1.0) <= rel_tol
    )


def adj_basis_switch_tr_price(close: float, mult: float) -> float:
    """Map raw close onto back-adjusted TR basis when adj switches at a split."""
    if not (math.isfinite(close) and close > 0 and math.isfinite(mult) and mult > 0):
        return close
    if mult <= 1.0:
        return close * mult
    return close / mult


def _adj_close_ratio_matches_pre_back_adjusted(
    ratio: float,
    mult: float,
    *,
    rel_tol: float = 0.15,
) -> bool:
    """True when ``adj/close`` on the pre-split side looks back-adjusted for ``mult``."""
    if not (math.isfinite(ratio) and ratio > 0 and math.isfinite(mult) and mult > 0):
        return False
    if mult < 1.0:
        return (
            abs(ratio * mult - 1.0) <= rel_tol
            or abs(ratio / mult - 1.0) <= rel_tol
            or abs(ratio - 1.0 / mult) <= rel_tol
        )
    return abs(ratio / mult - 1.0) <= rel_tol


def _adj_close_ratio_matches_post_split(
    ratio: float,
    mult: float,
    *,
    rel_tol: float = 0.08,
) -> bool:
    """Post-split ``adj/close``: raw Yahoo (≈1) or forward-normalized (≈mult)."""
    if not (math.isfinite(ratio) and ratio > 0 and math.isfinite(mult) and mult > 0):
        return False
    if abs(ratio - 1.0) <= rel_tol:
        return True
    if mult < 1.0 and abs(ratio / mult - 1.0) <= rel_tol:
        return True
    return False


def detect_adj_boundary(
    points: list[tuple[dt.date, float, float]],
    effective: dt.date,
    mult: float,
    *,
    rel_tol: float = 0.15,
    window_days: int = 3,
    close_tol: float = 0.08,
) -> dt.date | None:
    """First session after which adj jumps while close stays continuous (adj basis switch)."""
    if not points or mult <= 0:
        return None
    sorted_pts = sorted(points)
    candidates: list[dt.date] = []
    for i in range(1, len(sorted_pts)):
        d_prev, c_prev, a_prev = sorted_pts[i - 1]
        d_cur, c_cur, a_cur = sorted_pts[i]
        if not all(
            math.isfinite(x) and x > 0
            for x in (c_prev, c_cur, a_prev, a_cur)
        ):
            continue
        ratio = a_cur / a_prev
        if abs(c_cur / c_prev - 1.0) <= close_tol:
            if (
                abs(ratio * mult - 1.0) <= rel_tol
                or abs(ratio / mult - 1.0) <= rel_tol
                or abs(a_prev * mult / a_cur - 1.0) <= rel_tol
            ):
                candidates.append(d_cur)
                continue
        r_prev = a_prev / c_prev
        r_cur = a_cur / c_cur
        if _adj_close_ratio_matches_post_split(r_cur, mult, rel_tol=min(rel_tol, 0.1)):
            if _adj_close_ratio_matches_pre_back_adjusted(r_prev, mult, rel_tol=rel_tol):
                candidates.append(d_cur)
    if not candidates:
        return None
    near = [d for d in candidates if abs((d - effective).days) <= 7]
    pool = near or candidates
    return min(pool)


def find_close_jump_boundary(
    close_points: list[tuple[dt.date, float]],
    mult: float,
    near: dt.date,
    *,
    window_days: int = 7,
    jump_tol: float = 0.18,
    rel_tol: float = 0.15,
) -> dt.date | None:
    """First session where raw close jumps ~``mult`` within ``window_days`` of ``near``."""
    if not close_points or mult <= 0:
        return None
    dated = sorted(close_points)
    candidates: list[dt.date] = []
    for i in range(1, len(dated)):
        d_cur, c_cur = dated[i]
        _, c_prev = dated[i - 1]
        if abs((d_cur - near).days) > window_days:
            continue
        if not (c_prev > 0 and c_cur > 0):
            continue
        jump = c_cur / c_prev
        matched = match_split_to_price_jump(jump, mult, jump_tol=jump_tol, rel_tol=rel_tol)
        if matched is not None and abs(matched - mult) <= max(1e-6, rel_tol * abs(mult)):
            candidates.append(d_cur)
    if not candidates:
        return None
    return min(candidates, key=lambda d: abs((d - near).days))


def detect_staggered_discrete_splits(
    points: list[tuple[dt.date, float, float]],
    close_points: list[tuple[dt.date, float]],
    events: list[tuple[dt.date, float]],
    *,
    rel_tol: float = 0.15,
    window_days: int = 7,
) -> list[tuple[dt.date, float, dt.date]]:
    """Reverse splits where close jumps before adj switches (BAIG, BMNG, …)."""
    if not events:
        return []
    out: list[tuple[dt.date, float, dt.date]] = []
    for eff, mult in events:
        if float(mult) < 1.05:
            continue
        boundary = find_close_jump_boundary(
            close_points,
            float(mult),
            eff,
            window_days=window_days,
            rel_tol=rel_tol,
        )
        if boundary is None:
            continue
        adj_boundary = detect_adj_boundary(points, eff, float(mult), rel_tol=rel_tol) or eff
        if boundary > adj_boundary:
            continue
        out.append((eff, float(mult), boundary))
    return out


def _close_jump_near_date(
    close_points: list[tuple[dt.date, float]],
    mult: float,
    center: dt.date,
    *,
    window_days: int = 7,
) -> bool:
    return find_close_jump_boundary(
        close_points,
        mult,
        center,
        window_days=window_days,
    ) is not None


def _boundary_has_raw_forward_adj(
    points: list[tuple[dt.date, float, float]],
    boundary: dt.date,
    fwd_mult: float,
    *,
    rel_tol: float = 0.15,
) -> bool:
    """True when pre-boundary adj is raw forward (adj/close ≈ 1/mult).

    Mislabeled reverse corp actions (APPX) can still show a market close move at the
    adj boundary; ratio match alone selects ``forward_continuous_close``.
    """
    if not points or not (0 < fwd_mult < 1):
        return False
    sorted_pts = sorted(points)
    for i in range(1, len(sorted_pts)):
        if sorted_pts[i][0] != boundary:
            continue
        _, c_prev, a_prev = sorted_pts[i - 1]
        if not (c_prev > 0 and a_prev > 0):
            return False
        ratio = a_prev / c_prev
        inv = 1.0 / fwd_mult
        if abs(ratio - inv) > rel_tol:
            return False
        close_pts = [(d, c) for d, c, _ in sorted_pts]
        declared_dm = 1.0 / fwd_mult
        if _close_jump_near_date(close_pts, declared_dm, boundary, window_days=7):
            return False
        return True
    return False


def detect_adj_basis_switch_splits(
    points: list[tuple[dt.date, float, float]],
    events: list[tuple[dt.date, float]],
    *,
    rel_tol: float = 0.15,
    metric_rows: list[dict] | None = None,
) -> list[tuple[dt.date, float, str]]:
    """Splits where Yahoo *adj* switches basis at a corp event (APLX 3-for-1, ARCX 1-for-5).

    Returns ``(effective_date, price_mult, variant)`` where *variant* is
    ``forward`` (pre adj, post ``close × mult``), ``forward_continuous_close``
    (raw Yahoo forward with continuous close), or ``reverse_continuous``
    (continuous raw close is the TR series; pre adj was back-adjusted).
    """
    if not events:
        return []
    out: list[tuple[dt.date, float, str]] = []
    seen: set[tuple[dt.date, float, str]] = set()
    for eff, declared in events:
        dm = float(declared)
        if dm <= 0:
            continue
        jump = split_close_jump_ratio(points, eff)
        close_continuous = jump is None or abs(jump - 1.0) <= 0.12
        trials: list[tuple[float, str]] = []
        if dm < 1.0:
            trials.append((dm, "forward"))
        else:
            close_pts = [(d, c) for d, c, _ in points]
            adj_b = detect_adj_boundary(points, eff, dm, rel_tol=rel_tol) or eff
            staggered = _close_jump_near_date(close_pts, dm, adj_b, window_days=7)
            if close_continuous and not staggered:
                trials.append((dm, "reverse_continuous"))
            fwd_mult = 1.0 / dm
            boundary = detect_adj_boundary(points, eff, fwd_mult, rel_tol=rel_tol)
            if boundary is not None and abs((boundary - eff).days) <= 7:
                if _boundary_has_raw_forward_adj(points, boundary, fwd_mult, rel_tol=rel_tol):
                    trials.append((fwd_mult, "forward_continuous_close"))
                elif not staggered:
                    trials.append((fwd_mult, "forward"))
        for mult, variant in trials:
            key = (eff, mult, variant)
            if key in seen:
                continue
            boundary = detect_adj_boundary(points, eff, mult, rel_tol=rel_tol)
            if boundary is None or abs((boundary - eff).days) > 7:
                continue
            if variant == "reverse_continuous":
                close_pts = [(d, c) for d, c, _ in points]
                if _close_jump_near_date(close_pts, mult, boundary, window_days=7):
                    continue
                jump_at = split_close_jump_ratio(close_pts, boundary)
                if jump_at is not None and abs(jump_at - 1.0) > 0.12:
                    continue
            seen.add(key)
            out.append((eff, mult, variant))
    return out


def _metric_row_at_date(
    metric_rows: list[dict] | None,
    effective: dt.date,
) -> tuple[dict | None, dict | None]:
    if not metric_rows:
        return None, None
    dated = sorted(
        (
            (dt.date.fromisoformat(str(r.get("date") or "")[:10]), r)
            for r in metric_rows
            if str(r.get("date") or "")[:10]
        ),
        key=lambda x: x[0],
    )
    prev_row = None
    curr_row = None
    for d0, row in dated:
        if d0 < effective:
            prev_row = row
        elif d0 >= effective and curr_row is None:
            curr_row = row
            break
    return prev_row, curr_row


def filter_splits_needing_close_basis_fix(
    points: list[tuple[dt.date, float, float]],
    events: list[tuple[dt.date, float]],
    *,
    rel_tol: float = 0.15,
    jump_tol: float = 0.18,
    metric_rows: list[dict] | None = None,
) -> list[tuple[dt.date, float]]:
    """
    Keep only splits where Yahoo *close* jumps at the event (not already back-adjusted).

    When Yahoo's close series is continuous through a reverse split (common for recent
    listings), applying a mechanical factor would inflate pre-split closes ~6× and
    produce nonsense returns (e.g. MTYY −99%).

    When ``corporate_actions.json`` supplies a declared ratio, trust it when the observed
    jump is within ``jump_tol`` of that mult — even if a different whitelist ratio is
    nearer (e.g. APLZ 5.64× jump with declared 5×).
    """
    if not events:
        return []
    out: list[tuple[dt.date, float]] = []
    for eff, mult in events:
        jump = split_close_jump_ratio(points, eff)

        # Continuous Yahoo close: adj-basis-switch (APLX 3-for-1) is handled in price_basis;
        # fully back-adjusted continuous listings (MTYY) must not mechanical-scale.
        if jump is not None and abs(jump - 1.0) <= 0.08:
            if yahoo_adj_looks_back_adjusted(points, eff, float(mult), rel_tol=rel_tol):
                continue
            continue

        accepted = False
        if jump is not None:
            matched = match_split_to_price_jump(
                jump,
                float(mult),
                jump_tol=jump_tol,
                rel_tol=rel_tol,
            )
            if (
                matched is not None
                and abs(matched - float(mult)) <= max(1e-6, rel_tol * abs(mult))
                and not yahoo_adj_looks_back_adjusted(points, eff, float(mult), rel_tol=rel_tol)
            ):
                accepted = True

        if not accepted and jump is None:
            prev_row, curr_row = _metric_row_at_date(metric_rows, eff)
            if prev_row is not None and curr_row is not None:
                issuer_mult = confirm_split_from_shares_or_nav(
                    shares_prev=prev_row.get("shares_outstanding"),
                    shares_curr=curr_row.get("shares_outstanding"),
                    nav_prev=prev_row.get("nav"),
                    nav_curr=curr_row.get("nav"),
                    declared_mult=float(mult),
                    rel_tol=rel_tol,
                )
                if issuer_mult is not None:
                    accepted = True

        if accepted:
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
    """Reconcile live spot to window-end close basis across a possible split."""
    if not (math.isfinite(live_spot) and math.isfinite(end_close) and live_spot > 0 and end_close > 0):
        return 1.0
    ratio = live_spot / end_close
    if abs(ratio - 1.0) <= 0.03:
        return 1.0
    if known_factor is not None and math.isfinite(known_factor) and known_factor > 0 and known_factor != 1.0:
        if abs(live_spot / known_factor / end_close - 1.0) <= max(0.05, rel_tol):
            return float(known_factor)
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


def cum_split_factor_to_latest(
    row_date: dt.date,
    events: list[tuple[dt.date, float]],
) -> float:
    """Multiply a price at ``row_date`` onto the latest (post-all-splits) basis."""
    if not events:
        return 1.0
    mult = 1.0
    for eff, m in events:
        if row_date < eff:
            mult *= m
    return mult if math.isfinite(mult) and mult > 0 else 1.0


def etf_adj_close_needs_split_scaling(
    close: float,
    adj: float,
    *,
    eps: float = 1e-4,
) -> bool:
    """True when Yahoo ``etf_adj_close`` is unadjusted (identical to raw close)."""
    if not (math.isfinite(close) and math.isfinite(adj) and close > 0 and adj > 0):
        return False
    return abs(adj / close - 1.0) <= eps


def etf_adj_on_back_adjusted_forward_basis(
    close: float,
    adj: float,
    mult: float,
    *,
    rel_tol: float = 0.15,
) -> bool:
    """True when ``etf_adj_close`` is already on back-adjusted TR basis (adj ≈ close × mult, mult<1)."""
    if not (
        math.isfinite(close)
        and math.isfinite(adj)
        and math.isfinite(mult)
        and close > 0
        and adj > 0
        and 0 < mult < 1.0
    ):
        return False
    return abs((adj / close) / mult - 1.0) <= rel_tol


def etf_adj_on_post_split_basis(
    close: float,
    adj: float,
    mult: float,
    *,
    rel_tol: float = 0.15,
) -> bool:
    """True when ``etf_adj_close`` is already mapped onto the latest post-split basis."""
    if not (
        math.isfinite(close)
        and math.isfinite(adj)
        and math.isfinite(mult)
        and close > 0
        and adj > 0
        and mult > 1.05
    ):
        return False
    return abs((adj / close) / mult - 1.0) <= rel_tol


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


MAX_WINDOW_DIVIDEND_YIELD_FRAC = 0.05


def dividend_undo_mult(price_mult: float) -> float:
    """Map price-basis split factor to Yahoo dividend restatement divisor."""
    if not math.isfinite(price_mult) or price_mult <= 0:
        return 1.0
    if price_mult < 0.95:
        return 1.0 / price_mult
    if price_mult > 1.05:
        return price_mult
    return 1.0


def compound_dividend_undo_mult(
    ex_date: dt.date,
    split_events: list[tuple[dt.date, float]],
) -> float:
    """Product of undo factors for splits strictly after ``ex_date``."""
    undo = 1.0
    for eff, price_mult in split_events:
        if ex_date < eff:
            undo *= dividend_undo_mult(float(price_mult))
    return undo if math.isfinite(undo) and undo > 0 else 1.0


def adjust_window_dividend_for_split(
    amount: float,
    ex_date: dt.date,
    *,
    split_events: list[tuple[dt.date, float]],
    close_on_ex_date: float | None = None,
) -> tuple[float, str]:
    """Return economic dividend amount and basis tag for window yield sums."""
    if not math.isfinite(amount) or amount <= 0:
        return 0.0, "invalid"
    if not split_events:
        return float(amount), "as_paid"
    undo = compound_dividend_undo_mult(ex_date, split_events)
    if undo <= 1.05:
        return float(amount), "as_paid"
    close = float(close_on_ex_date) if close_on_ex_date is not None else float("nan")
    if math.isfinite(close) and close > 0 and amount / close <= MAX_WINDOW_DIVIDEND_YIELD_FRAC:
        return float(amount), "as_paid"
    return float(amount) / undo, "yahoo_restated"


def split_factor_end_to_asof_safe(
    points: list[tuple[dt.date, float, float]],
    end_date: dt.date,
    asof_calendar: dt.date,
    events: list[tuple[dt.date, float]],
) -> float:
    """Only scale live spot when Yahoo *close* actually jumped between window end and asof."""
    if end_date >= asof_calendar or not events:
        return 1.0
    between = [(d, m) for d, m in events if end_date < d <= asof_calendar]
    if not between:
        return 1.0
    verified = filter_splits_needing_close_basis_fix(points, between)
    return cum_split_factor(end_date, asof_calendar, verified)

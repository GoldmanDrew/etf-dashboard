"""IPO float-unlock calendar: resolve seed tranches → built calendar artifact."""
from __future__ import annotations

import json
import math
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from market_calendar import is_nyse_session, next_nyse_session

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
CONFIG_PATH = REPO_ROOT / "config" / "ipo_unlock_model.yml"
SEED_PATH = DATA_DIR / "ipo_float_unlock_seed.json"
CALENDAR_PATH = DATA_DIR / "ipo_float_unlock_calendar.json"

DEFAULT_MODEL: dict[str, Any] = {
    "recently_cleared_lookback_days": 30,
    "material_days_adv_threshold": 3.0,
    "sell_fraction_by_holder_class": {
        "employees": 0.28,
        "directors_officers": 0.18,
        "pre_ipo": 0.22,
        "founder_extended": 0.05,
        "default": 0.20,
    },
    "car_base_3d": -0.015,
    "car_scale_days_adv_ref": 5.0,
    "car_soft_cap": -0.12,
    "event_window_calendar_days": 15,
    "event_uplift_floor": 0.10,
    "event_uplift_log_coef": 0.04,
    "event_uplift_cap": 0.30,
    "compression_log_coef": 0.10,
    "compression_floor": 0.70,
    "trading_days_year": 252,
    "default_adv_shares": 5_000_000,
}


def load_model_config(path: Path | None = None) -> dict[str, Any]:
    cfg_path = path or CONFIG_PATH
    out = dict(DEFAULT_MODEL)
    if not cfg_path.exists():
        return out
    try:
        import yaml  # type: ignore

        raw = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
    except Exception:
        return out
    if isinstance(raw, dict):
        for k, v in raw.items():
            if k == "schema_version":
                continue
            out[k] = v
    return out


def _parse_date(raw: object) -> date | None:
    if raw is None or raw == "":
        return None
    try:
        return date.fromisoformat(str(raw)[:10])
    except ValueError:
        return None


def add_nyse_trading_days(start: date, n: int) -> date:
    """Return the date n NYSE sessions after start (n>=1 → next sessions)."""
    if n <= 0:
        return start
    d = start
    for _ in range(n):
        d = next_nyse_session(d)
    return d


def load_earnings_dates(data_dir: Path | None = None) -> dict[str, date]:
    """Map underlying → earliest upcoming/known earnings date from combined calendar."""
    root = data_dir or DATA_DIR
    path = root / "event_calendar_combined.json"
    out: dict[str, date] = {}
    if not path.exists():
        return out
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return out
    for it in payload.get("items") or []:
        if str(it.get("event_type") or "").lower() not in ("", "earnings"):
            # allow earnings only
            if str(it.get("event_type") or "").lower() != "earnings":
                continue
        und = str(it.get("underlying") or "").upper().strip()
        d = _parse_date(it.get("event_date"))
        if not und or d is None:
            continue
        prev = out.get(und)
        if prev is None or d < prev:
            out[und] = d
    return out


def resolve_tranche_unlock_date(
    tranche: dict[str, Any],
    *,
    ipo_date: date | None,
    earnings_by_und: dict[str, date],
    underlying: str,
) -> tuple[date | None, str]:
    """Return (unlock_date, resolution_note)."""
    rule = str(tranche.get("unlock_rule") or "fixed_date").lower()
    explicit = _parse_date(tranche.get("unlock_date"))
    if explicit is not None and rule in ("fixed_date", "day_offset_from_ipo", "price_trigger"):
        return explicit, "seed_unlock_date"

    earn = _parse_date(tranche.get("earnings_date"))
    if earn is None:
        earn = earnings_by_und.get(underlying.upper())
    n_days = int(tranche.get("trading_days_after_earnings") or 2)

    if rule == "earnings_plus_nd":
        if earn is None:
            return None, "awaiting_earnings_date"
        return add_nyse_trading_days(earn, n_days), "earnings_plus_nd"

    if rule == "earlier_of_earnings_or_fixed":
        fixed = _parse_date(tranche.get("fixed_outside_date")) or explicit
        earn_unlock = add_nyse_trading_days(earn, n_days) if earn is not None else None
        if earn_unlock is not None and fixed is not None:
            return min(earn_unlock, fixed), "earlier_of_earnings_or_fixed"
        if fixed is not None:
            return fixed, "fixed_outside_only"
        if earn_unlock is not None:
            return earn_unlock, "earnings_only"
        return None, "awaiting_earnings_or_fixed"

    if rule == "day_offset_from_ipo":
        if explicit is not None:
            return explicit, "seed_unlock_date"
        if ipo_date is None:
            return None, "missing_ipo_date"
        offset = int(tranche.get("day_offset") or 0)
        d = ipo_date + timedelta(days=offset)
        while not is_nyse_session(d):
            d += timedelta(days=1)
        return d, "day_offset_from_ipo"

    if explicit is not None:
        return explicit, "seed_unlock_date"
    return None, "unresolved"


def _shares(tranche: dict[str, Any]) -> float:
    try:
        v = float(tranche.get("shares_eligible") or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return v if math.isfinite(v) and v > 0 else 0.0


def _data_grade(und_row: dict[str, Any], future: list[dict[str, Any]]) -> str:
    """Grade by next actionable tranche quality (later projected dates do not force C)."""
    if not future:
        return "B"
    dated = [t for t in future if t.get("unlock_date")]
    unresolved = [t for t in future if not t.get("unlock_date")]
    dated_sorted = sorted(dated, key=lambda x: str(x.get("unlock_date")))
    next_t = dated_sorted[0] if dated_sorted else None
    if next_t is None:
        return "C" if unresolved else "B"
    next_conf = str(next_t.get("confirmation") or "")
    if next_conf == "confirmed" and not unresolved:
        confs = [str(t.get("confirmation") or "") for t in dated]
        return "A" if all(c == "confirmed" for c in confs) else "B"
    if next_conf == "confirmed":
        return "B"  # next is solid; some later dates still projected/unresolved
    return "C"


def classify_status(
    *,
    asof: date,
    future_standard: list[dict[str, Any]],
    past: list[dict[str, Any]],
    future_extended: list[dict[str, Any]],
    lookback_days: int,
) -> str:
    if future_standard:
        return "active_unlock"
    if future_extended and not future_standard:
        # Only founder/extended left
        last_past = None
        for t in past:
            d = _parse_date(t.get("unlock_date"))
            if d is not None and (last_past is None or d > last_past):
                last_past = d
        if last_past is not None and (asof - last_past).days <= lookback_days:
            return "recently_cleared"
        return "extended_residual"
    last_past = None
    for t in past:
        d = _parse_date(t.get("unlock_date"))
        if d is not None and (last_past is None or d > last_past):
            last_past = d
    if last_past is not None and (asof - last_past).days <= lookback_days:
        return "recently_cleared"
    return "not_applicable"


def estimate_float_now(und: dict[str, Any], past_tranches: list[dict[str, Any]]) -> float:
    base = float(und.get("free_float_shares_at_ipo") or und.get("shares_offered") or 0.0)
    released = 0.0
    for t in past_tranches:
        if str(t.get("condition_status") or "") == "failed":
            continue
        released += _shares(t)
    return max(base + released, base, 1.0)


def build_calendar(
    *,
    seed: dict[str, Any] | None = None,
    asof: date | None = None,
    data_dir: Path | None = None,
    model_cfg: dict[str, Any] | None = None,
) -> dict[str, Any]:
    root = data_dir or DATA_DIR
    asof_d = asof or date.today()
    cfg = model_cfg or load_model_config()
    if seed is None:
        seed = json.loads((root / "ipo_float_unlock_seed.json").read_text(encoding="utf-8"))
    earnings = load_earnings_dates(root)
    lookback = int(cfg.get("recently_cleared_lookback_days") or 30)

    underlyings_out: list[dict[str, Any]] = []
    for und in seed.get("underlyings") or []:
        sym = str(und.get("underlying") or "").upper().strip()
        if not sym:
            continue
        ipo_d = _parse_date(und.get("ipo_date"))
        resolved: list[dict[str, Any]] = []
        for tr in und.get("tranches") or []:
            t = dict(tr)
            unlock_d, note = resolve_tranche_unlock_date(
                t,
                ipo_date=ipo_d,
                earnings_by_und=earnings,
                underlying=sym,
            )
            # Prefer seed earnings_date for SPCX etc.
            if t.get("earnings_date") and str(t.get("unlock_rule") or "").lower() == "earnings_plus_nd":
                earn = _parse_date(t.get("earnings_date"))
                if earn is not None:
                    unlock_d = add_nyse_trading_days(
                        earn, int(t.get("trading_days_after_earnings") or 2)
                    )
                    note = "earnings_plus_nd_seed"
            t["unlock_date"] = unlock_d.isoformat() if unlock_d else None
            t["resolution_note"] = note
            t["shares_eligible"] = _shares(t)
            resolved.append(t)

        past = []
        future = []
        for t in resolved:
            if str(t.get("condition_status") or "") == "failed":
                # keep in resolved list but not in supply path
                d = _parse_date(t.get("unlock_date"))
                if d is not None and d < asof_d:
                    past.append(t)
                continue
            d = _parse_date(t.get("unlock_date"))
            if d is None:
                future.append(t)  # pending resolution still "future"
            elif d < asof_d:
                past.append(t)
            else:
                future.append(t)

        future_std = [t for t in future if str(t.get("schedule_id") or "standard_180") != "extended_366"]
        future_ext = [t for t in future if str(t.get("schedule_id") or "") == "extended_366"]
        # Also treat founder_extended as extended even if schedule_id missing
        future_std = [
            t
            for t in future_std
            if str(t.get("holder_class") or "") != "founder_extended"
        ]
        future_ext = future_ext + [
            t
            for t in future
            if str(t.get("holder_class") or "") == "founder_extended"
            and t not in future_ext
        ]

        status = classify_status(
            asof=asof_d,
            future_standard=future_std,
            past=past,
            future_extended=future_ext,
            lookback_days=lookback,
        )
        float_now = estimate_float_now(und, past)
        # Next tranche among standard future with a date
        dated_future = sorted(
            [t for t in future_std if t.get("unlock_date")],
            key=lambda x: str(x.get("unlock_date")),
        )
        next_t = dated_future[0] if dated_future else None
        pending = [t for t in future_std if not t.get("unlock_date")]

        cum_by_horizon: dict[str, float] = {}
        for label, days in (("7d", 7), ("30d", 30), ("90d", 90), ("180d", 180), ("365d", 365)):
            end = asof_d + timedelta(days=days)
            cum = 0.0
            for t in future_std:
                if str(t.get("condition_status") or "") == "failed":
                    continue
                d = _parse_date(t.get("unlock_date"))
                if d is not None and asof_d <= d <= end:
                    cum += float(t.get("shares_eligible") or 0.0)
            cum_by_horizon[label] = round(cum, 0)

        grade = _data_grade(und, future_std)
        underlyings_out.append(
            {
                "underlying": sym,
                "company": und.get("company"),
                "ipo_date": und.get("ipo_date"),
                "ipo_price": und.get("ipo_price"),
                "free_float_shares_at_ipo": und.get("free_float_shares_at_ipo"),
                "shares_outstanding_post_ipo": und.get("shares_outstanding_post_ipo"),
                "adv_shares_estimate": und.get("adv_shares_estimate"),
                "source_filing": und.get("source_filing"),
                "float_now_estimate": round(float_now, 0),
                "unlock_status": status,
                "data_grade": grade,
                "is_ipo_float_unlock": status == "active_unlock",
                "days_to_next_ipo_unlock": (
                    (_parse_date(next_t["unlock_date"]) - asof_d).days
                    if next_t and next_t.get("unlock_date")
                    else None
                ),
                "next_ipo_unlock_date": next_t.get("unlock_date") if next_t else None,
                "next_ipo_unlock_shares": (
                    float(next_t.get("shares_eligible") or 0.0) if next_t else None
                ),
                "next_ipo_unlock_tranche_id": next_t.get("tranche_id") if next_t else None,
                "pending_resolution_count": len(pending),
                "cumulative_shares_eligible_by_horizon": cum_by_horizon,
                "tranches": resolved,
                "future_standard_tranche_ids": [t.get("tranche_id") for t in future_std],
                "future_extended_tranche_ids": [t.get("tranche_id") for t in future_ext],
            }
        )

    return {
        "schema_version": 1,
        "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "asof_date": asof_d.isoformat(),
        "source_seed": "ipo_float_unlock_seed.json",
        "underlying_count": len(underlyings_out),
        "underlyings": underlyings_out,
    }


def write_calendar(payload: dict[str, Any], path: Path | None = None) -> Path:
    out = path or CALENDAR_PATH
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return out


def load_calendar(path: Path | None = None) -> dict[str, Any]:
    p = path or CALENDAR_PATH
    if not p.exists():
        return {}
    return json.loads(p.read_text(encoding="utf-8"))


def calendar_by_underlying(payload: dict[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    cal = payload if payload is not None else load_calendar()
    out: dict[str, dict[str, Any]] = {}
    for u in cal.get("underlyings") or []:
        sym = str(u.get("underlying") or "").upper()
        if sym:
            out[sym] = u
    return out

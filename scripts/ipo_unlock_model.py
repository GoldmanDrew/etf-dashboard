"""IPO float-unlock σ / expected-decay overlay (parallel fields; does not replace HARQ)."""
from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import date
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPTS))

from ipo_unlock_calendar import (  # noqa: E402
    CALENDAR_PATH,
    DATA_DIR,
    build_calendar,
    calendar_by_underlying,
    load_calendar,
    load_model_config,
    write_calendar,
    _parse_date,
    _shares,
)

DASHBOARD_PATH = DATA_DIR / "dashboard_data.json"


def ito_gross_annual(beta: float, sigma: float) -> float:
    """Closed-form Itô pair gross (short-favorable +): (β² − β)/2 · σ²."""
    return 0.5 * (beta * beta - beta) * sigma * sigma


def sell_fraction(holder_class: str, cfg: dict[str, Any]) -> float:
    table = cfg.get("sell_fraction_by_holder_class") or {}
    key = str(holder_class or "default")
    try:
        v = float(table.get(key, table.get("default", 0.20)))
    except (TypeError, ValueError):
        v = 0.20
    return min(max(v, 0.0), 1.0)


def _pos(v: object) -> float | None:
    try:
        f = float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) and f > 0 else None


def _finite(v: object) -> float | None:
    try:
        f = float(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def future_standard_tranches(und_cal: dict[str, Any], asof: date) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for t in und_cal.get("tranches") or []:
        if str(t.get("condition_status") or "") == "failed":
            continue
        if str(t.get("schedule_id") or "") == "extended_366":
            continue
        if str(t.get("holder_class") or "") == "founder_extended":
            continue
        d = _parse_date(t.get("unlock_date"))
        if d is None:
            continue  # unresolved — exclude from quantitative path
        if d >= asof:
            out.append(t)
    out.sort(key=lambda x: str(x.get("unlock_date")))
    return out


def tranche_car_long(
    *,
    shares: float,
    alpha: float,
    float_now: float,
    adv: float,
    cfg: dict[str, Any],
) -> float:
    """Expected long-side CAR (negative = price down). Soft-capped."""
    base = float(cfg.get("car_base_3d") or -0.015)
    ref = float(cfg.get("car_scale_days_adv_ref") or 5.0)
    cap = float(cfg.get("car_soft_cap") or -0.12)
    sold = max(shares * alpha, 0.0)
    days_adv = sold / max(adv, 1.0)
    # Concave in relative float expansion
    rel = sold / max(float_now, 1.0)
    scale = math.log1p(max(days_adv / max(ref, 1e-9), 0.0)) + math.log1p(rel)
    raw = base * (1.0 + scale)
    # Soft cap toward cap (more negative)
    if raw < cap:
        # blend toward cap with log
        excess = cap - raw
        raw = cap - 0.25 * math.log1p(excess)
    return max(min(raw, 0.0), cap * 1.5)


def compute_unlock_vol_path(
    *,
    sigma_base: float,
    float_now: float,
    adv: float,
    tranches: list[dict[str, Any]],
    cfg: dict[str, Any],
) -> dict[str, Any]:
    event_days_budget = 0.0
    max_uplift = 0.0
    material_thr = float(cfg.get("material_days_adv_threshold") or 3.0)
    win = float(cfg.get("event_window_calendar_days") or 15)
    uplift_floor = float(cfg.get("event_uplift_floor") or 0.10)
    uplift_coef = float(cfg.get("event_uplift_log_coef") or 0.04)
    uplift_cap = float(cfg.get("event_uplift_cap") or 0.30)
    tdy = float(cfg.get("trading_days_year") or 252)

    cum_eligible = 0.0
    car_sum = 0.0
    for t in tranches:
        sh = _shares(t)
        if sh <= 0:
            continue
        alpha = sell_fraction(str(t.get("holder_class") or "default"), cfg)
        days_adv = (sh * alpha) / max(adv, 1.0)
        cum_eligible += sh
        car_sum += tranche_car_long(
            shares=sh, alpha=alpha, float_now=float_now, adv=adv, cfg=cfg
        )
        if days_adv >= material_thr:
            event_days_budget += win
            uplift = min(uplift_cap, uplift_floor + uplift_coef * math.log1p(days_adv))
            max_uplift = max(max_uplift, uplift)

    compression_coef = float(cfg.get("compression_log_coef") or 0.10)
    compression_floor = float(cfg.get("compression_floor") or 0.70)
    compression = max(
        compression_floor,
        1.0 - compression_coef * math.log1p(cum_eligible / max(float_now, 1.0)),
    )

    event_frac = min(1.0, event_days_budget / tdy)
    sigma_event = sigma_base * (1.0 + max_uplift)
    sigma_ss = sigma_base * compression
    # Variance blend of regimes over the year
    var = event_frac * sigma_event * sigma_event + (1.0 - event_frac) * sigma_ss * sigma_ss
    sigma_unlock = math.sqrt(max(var, 0.0))

    # Band: lower = more compression / less uplift; upper = full event
    sigma_lo = sigma_base * compression * 0.92
    sigma_hi = sigma_base * (1.0 + max_uplift) * 1.05

    return {
        "forecast_vol_unlock_annual": round(sigma_unlock, 6),
        "forecast_vol_unlock_event_annual": round(sigma_event, 6),
        "forecast_vol_unlock_steady_annual": round(sigma_ss, 6),
        "forecast_vol_unlock_p10_annual": round(min(sigma_lo, sigma_unlock), 6),
        "forecast_vol_unlock_p90_annual": round(max(sigma_hi, sigma_unlock), 6),
        "ipo_unlock_event_frac_year": round(event_frac, 4),
        "ipo_unlock_max_uplift": round(max_uplift, 4),
        "ipo_unlock_compression": round(compression, 4),
        "ipo_unlock_cum_eligible_shares": round(cum_eligible, 0),
        "price_car_unlock_p50": round(car_sum, 6),  # long-side sum of tranche CARs
        "price_car_unlock_p50_short_favorable": round(-car_sum, 6),
    }


def compute_unlock_fields_for_record(
    rec: dict[str, Any],
    und_cal: dict[str, Any],
    *,
    asof: date,
    cfg: dict[str, Any],
) -> dict[str, Any]:
    status = str(und_cal.get("unlock_status") or "not_applicable")
    out: dict[str, Any] = {
        "is_ipo_float_unlock": bool(und_cal.get("is_ipo_float_unlock")),
        "ipo_unlock_status": status,
        "ipo_unlock_data_grade": und_cal.get("data_grade"),
        "ipo_unlock_source": "ipo_float_unlock_calendar",
        "days_to_next_ipo_unlock": und_cal.get("days_to_next_ipo_unlock"),
        "next_ipo_unlock_date": und_cal.get("next_ipo_unlock_date"),
        "next_ipo_unlock_shares": und_cal.get("next_ipo_unlock_shares"),
        "next_ipo_unlock_tranche_id": und_cal.get("next_ipo_unlock_tranche_id"),
        "ipo_float_now_estimate": und_cal.get("float_now_estimate"),
        "ipo_unlock_cum_eligible_7d": (und_cal.get("cumulative_shares_eligible_by_horizon") or {}).get("7d"),
        "ipo_unlock_cum_eligible_30d": (und_cal.get("cumulative_shares_eligible_by_horizon") or {}).get("30d"),
        "ipo_unlock_cum_eligible_90d": (und_cal.get("cumulative_shares_eligible_by_horizon") or {}).get("90d"),
    }

    if status != "active_unlock":
        return out

    pc = str(rec.get("product_class") or "").lower()
    if pc in ("income_yieldboost", "income_yieldboost_fof", "passive_low_delta", "passive_low_beta"):
        out["ipo_unlock_model_note"] = "skipped_product_class"
        return out

    beta = _finite(rec.get("delta") if rec.get("delta") is not None else rec.get("beta"))
    if beta is None:
        out["ipo_unlock_model_note"] = "missing_beta"
        return out

    sigma_base = (
        _pos(rec.get("forecast_vol_underlying_annual"))
        or _pos(rec.get("forecast_vol_model_annual"))
        or _pos(rec.get("vol_underlying_annual"))
    )
    if sigma_base is None:
        out["ipo_unlock_model_note"] = "missing_sigma_base"
        return out

    float_now = float(und_cal.get("float_now_estimate") or 1.0)
    adv = float(
        und_cal.get("adv_shares_estimate")
        or cfg.get("default_adv_shares")
        or 5_000_000
    )
    tranches = future_standard_tranches(und_cal, asof)
    if not tranches:
        out["ipo_unlock_model_note"] = "no_dated_future_tranches"
        return out

    vol = compute_unlock_vol_path(
        sigma_base=sigma_base,
        float_now=float_now,
        adv=adv,
        tranches=tranches,
        cfg=cfg,
    )
    out.update(vol)

    sig = float(vol["forecast_vol_unlock_annual"])
    sig_lo = float(vol["forecast_vol_unlock_p10_annual"])
    sig_hi = float(vol["forecast_vol_unlock_p90_annual"])
    decay = ito_gross_annual(beta, sig)
    decay_lo = ito_gross_annual(beta, sig_lo)
    decay_hi = ito_gross_annual(beta, sig_hi)
    # Ensure p10 < p90 on decay (inverse β can flip ordering vs sigma)
    d_p10, d_p90 = sorted([decay_lo, decay_hi])

    out["expected_gross_decay_unlock_p50_annual"] = round(decay, 6)
    out["expected_gross_decay_unlock_p10_annual"] = round(d_p10, 6)
    out["expected_gross_decay_unlock_p90_annual"] = round(d_p90, 6)
    out["expected_pair_pnl_unlock_p50_annual"] = round(decay, 6)
    out["expected_pair_pnl_unlock_p10_annual"] = round(d_p10, 6)
    out["expected_pair_pnl_unlock_p90_annual"] = round(d_p90, 6)
    out["expected_gross_decay_unlock_basis"] = "ito_unlock_vol_path"
    out["forecast_vol_unlock_base_annual"] = round(sigma_base, 6)

    # Alternate net edge: reuse inverse-variance weight when present
    mu_r = _finite(rec.get("gross_realized_mean_annual"))
    if mu_r is None:
        mu_r = _finite(rec.get("gross_decay_annual"))
    w_f = _finite(rec.get("gross_blend_weight_forward"))
    borrow = _finite(rec.get("borrow_for_net_annual"))
    if borrow is None:
        borrow = _finite(rec.get("borrow_fee_annual")) or _finite(rec.get("borrow_current")) or 0.0

    if mu_r is not None:
        if w_f is None:
            # derive from unlock band vs realized sigma if available
            sig_r = _pos(rec.get("gross_sigma_realized_annual"))
            sig_f = max((d_p90 - d_p10) / (2 * 1.2816), 1e-6)
            if sig_r is not None:
                w_f = (sig_r * sig_r) / (sig_f * sig_f + sig_r * sig_r)
            else:
                w_f = 0.55
        w_f = min(max(w_f, 0.0), 1.0)
        posterior = w_f * decay + (1.0 - w_f) * mu_r
        net = posterior - float(borrow)
        out["net_edge_unlock_p50_annual"] = round(net, 6)
        out["net_edge_unlock_gross_anchor_annual"] = round(posterior, 6)
        out["net_edge_unlock_weight_forward"] = round(w_f, 4)
        out["net_edge_unlock_basis"] = "inverse_variance_unlock_anchor_minus_borrow"
    else:
        out["net_edge_unlock_p50_annual"] = round(decay - float(borrow), 6)
        out["net_edge_unlock_basis"] = "unlock_gross_minus_borrow"

    out["ipo_unlock_model_note"] = (
        f"σ_base={sigma_base:.3f}→σ_unlock={sig:.3f}; "
        f"event_frac={vol['ipo_unlock_event_frac_year']}; "
        f"compression={vol['ipo_unlock_compression']}; "
        f"n_tranches={len(tranches)}"
    )
    return out


def enrich_records_with_ipo_unlock(
    records: list[dict[str, Any]],
    *,
    data_dir: Path | None = None,
    calendar: dict[str, Any] | None = None,
    asof: date | None = None,
    model_cfg: dict[str, Any] | None = None,
) -> dict[str, int]:
    """Mutate ETF rows in place with IPO unlock overlay fields."""
    root = data_dir or DATA_DIR
    cfg = model_cfg or load_model_config()
    asof_d = asof or date.today()
    if calendar is None:
        cal_path = root / "ipo_float_unlock_calendar.json"
        if cal_path.exists():
            calendar = load_calendar(cal_path)
        else:
            seed_path = root / "ipo_float_unlock_seed.json"
            seed = json.loads(seed_path.read_text(encoding="utf-8")) if seed_path.exists() else {"underlyings": []}
            calendar = build_calendar(seed=seed, asof=asof_d, data_dir=root, model_cfg=cfg)
            write_calendar(calendar, cal_path)

    by_und = calendar_by_underlying(calendar)
    stats = {"flagged": 0, "modeled": 0, "skipped": 0}
    for rec in records:
        und = str(rec.get("underlying") or "").upper().strip()
        if und not in by_und:
            continue
        fields = compute_unlock_fields_for_record(rec, by_und[und], asof=asof_d, cfg=cfg)
        rec.update(fields)
        if fields.get("is_ipo_float_unlock"):
            stats["flagged"] += 1
        if fields.get("expected_gross_decay_unlock_p50_annual") is not None:
            stats["modeled"] += 1
        elif fields.get("ipo_unlock_model_note"):
            stats["skipped"] += 1
    return stats


def enrich_dashboard_json(
    *,
    dashboard_path: Path | None = None,
    rebuild_calendar: bool = True,
    asof: date | None = None,
) -> dict[str, Any]:
    path = dashboard_path or DASHBOARD_PATH
    cfg = load_model_config()
    asof_d = asof or date.today()
    root = path.parent
    if rebuild_calendar:
        seed = json.loads((root / "ipo_float_unlock_seed.json").read_text(encoding="utf-8"))
        cal = build_calendar(seed=seed, asof=asof_d, data_dir=root, model_cfg=cfg)
        write_calendar(cal, root / "ipo_float_unlock_calendar.json")
    else:
        cal = load_calendar(root / "ipo_float_unlock_calendar.json")

    payload = json.loads(path.read_text(encoding="utf-8"))
    key = "records" if isinstance(payload.get("records"), list) else "rows"
    records = payload.get(key) or []
    stats = enrich_records_with_ipo_unlock(
        records, data_dir=root, calendar=cal, asof=asof_d, model_cfg=cfg
    )
    payload[key] = records
    payload["ipo_float_unlock_meta"] = {
        "asof_date": cal.get("asof_date"),
        "calendar_build_time": cal.get("build_time"),
        "enrich_stats": stats,
        "schema_version": 1,
    }
    path.write_text(json.dumps(payload, allow_nan=False) + "\n", encoding="utf-8")
    return {"stats": stats, "path": str(path), "calendar_asof": cal.get("asof_date")}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--enrich-dashboard", action="store_true", help="Patch data/dashboard_data.json in place")
    p.add_argument("--rebuild-calendar", action="store_true", default=True)
    p.add_argument("--no-rebuild-calendar", action="store_true")
    p.add_argument("--asof", default=None, help="YYYY-MM-DD")
    p.add_argument("--dashboard", type=Path, default=None)
    args = p.parse_args(argv)
    asof = _parse_date(args.asof) if args.asof else date.today()
    rebuild = not args.no_rebuild_calendar
    if args.enrich_dashboard:
        result = enrich_dashboard_json(
            dashboard_path=args.dashboard,
            rebuild_calendar=rebuild,
            asof=asof,
        )
        print(json.dumps(result, indent=2))
        return 0
    # default: build calendar only
    seed = json.loads((DATA_DIR / "ipo_float_unlock_seed.json").read_text(encoding="utf-8"))
    cal = build_calendar(seed=seed, asof=asof or date.today())
    write_calendar(cal)
    print(f"Wrote {CALENDAR_PATH} ({cal.get('underlying_count')} underlyings, asof={cal.get('asof_date')})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

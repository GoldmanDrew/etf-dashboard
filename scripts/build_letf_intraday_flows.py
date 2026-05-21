#!/usr/bin/env python3
"""Estimate today's pending LETF close-rebalance from intraday underlying moves.

Math (Avellaneda-Zhang identity, applied with intraday-so-far return)::

    estimated_close_rebalance = L * (L - 1) * AUM_prior_close * return_d1_so_far

Inputs:
  * ``data/etf_screened_today.csv``                 -- universe, leverage, product class
  * ``data/etf_metrics_daily.parquet``              -- last AUM-bearing row per fund
  * ``data/underlying_intraday_spot.json``          -- live spots + prior closes
  * ``data/underlying_volume_history.parquet``      -- 20d $ ADV (optional)
  * ``data/letf_intraday_flow_metrics.json``        -- per-underlying bias adj. (optional)

Outputs:
  * ``data/letf_rebalance_flows_intraday_latest.json``
        Per-fund + per-underlying snapshot at the current ``as_of``.
  * ``data/letf_rebalance_flows_intraday_snapshots/<YYYY-MM-DD>.jsonl``
        Append-only log: one JSON line per build. Used by ``score_intraday_flows.py``
        (T+1 reconciliation against the EOD realised close-flow).

The "remaining-to-close" decomposition assumes that issuers have already
worked a fraction of their hedge intraday roughly equal to ``volume_so_far /
20d $ ADV`` (a crude proxy -- LETFs are <1% of underlying ADV typically, so
this only matters for very concentrated single-name buckets).
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

_SCRIPTS = Path(__file__).resolve().parent
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from build_letf_rebalance_flows import (  # noqa: E402
    UNDERLYING_VOLUME_PARQUET,
    _ADV_WINDOW_DAYS,
    _f,
    compute_adv_panel,
    load_metrics,
    load_universe,
    norm_sym,
)

LOGGER = logging.getLogger("letf_intraday_flows")

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
SPOT_INPUT = DATA_DIR / "underlying_intraday_spot.json"
NAV_FORECAST_LATEST = DATA_DIR / "nav_forecasts" / "_latest.json"
INTRADAY_OUTPUT = DATA_DIR / "letf_rebalance_flows_intraday_latest.json"
SNAPSHOT_DIR = DATA_DIR / "letf_rebalance_flows_intraday_snapshots"
INTRADAY_BIAS_JSON = DATA_DIR / "letf_intraday_flow_metrics.json"

# Bias-adjustment trigger: only apply per-underlying calibration once we have
# at least this many T+1 reconciliations (see ``score_intraday_flows.py``).
MIN_BIAS_OBSERVATIONS = 5

# Treat a NAV-forecast row as usable when it is at most this old at flow build
# time. The NAV pipeline rebuilds every 30 min, intraday flow every 5 min, so
# 35 min covers the worst gap without bleeding into yesterday's anchor session.
NAV_FRESHNESS_MAX_SEC = 35 * 60

# Default weight on the NAV-implied return inside the blended estimator. The
# spot path is unbiased but noisy; NAV models add structure (vol drag for v2,
# holdings deltas for v3), so we lean on them more when the dispatcher's
# default model is more sophisticated and the row is "high" confidence. These
# numbers are deliberate priors -- per-ticker reconciliation in
# ``score_intraday_flows.py`` overrides them once we have enough samples.
NAV_BLEND_DEFAULT_BY_MODEL: dict[str, float] = {
    "delta_v1": 0.30,
    "delta_v2_ito": 0.50,
    "delta_v3_swap_mark": 0.70,
    "yieldboost_putspread_v1": 0.70,
}
NAV_BLEND_MEDIUM_CONFIDENCE_SCALE = 0.5  # halve the weight on medium-confidence rows
NAV_BLEND_WEIGHT_FLOOR = 0.05
NAV_BLEND_WEIGHT_CEIL = 0.95


# ?? Helpers ??????????????????????????????????????????????????????????????


def _market_close_utc(now: datetime) -> datetime:
    """Today's ~16:00 ET expressed in UTC; used for the ``minutes_to_close`` diagnostic.

    Approximate DST: EDT roughly Mar 9 - Nov 2 (within a few days). The
    diagnostic is informational, so we don't pull pytz for sub-week precision.
    """
    today = now.date()
    march_dst_start = date(now.year, 3, 9)
    november_dst_end = date(now.year, 11, 2)
    is_edt = march_dst_start <= today <= november_dst_end
    close_utc_hour = 20 if is_edt else 21
    return datetime(today.year, today.month, today.day, close_utc_hour, 0, 0, tzinfo=UTC)


def _round(v: object, digits: int = 6) -> float | None:
    f = _f(v)
    return round(f, digits) if f is not None else None


def _json_clean(v: Any) -> Any:
    if isinstance(v, dict):
        return {str(k): _json_clean(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_json_clean(x) for x in v]
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating, float)):
        f = float(v)
        return f if math.isfinite(f) else None
    if v is None:
        return None
    if pd.isna(v):
        return None
    return v


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(_json_clean(payload), separators=(",", ":"), allow_nan=False, sort_keys=True),
        encoding="utf-8",
    )
    tmp.replace(path)


# ?? Inputs ???????????????????????????????????????????????????????????????


def latest_metrics_per_ticker(metrics: pd.DataFrame) -> pd.DataFrame:
    """Return one row per ticker -- the most recent date with AUM > 0."""
    if metrics.empty:
        return metrics
    df = metrics.copy()
    if "aum" in df.columns:
        df["aum"] = pd.to_numeric(df["aum"], errors="coerce")
        df = df[df["aum"].fillna(0) > 0]
    df = df.sort_values(["ticker", "date"])
    return df.groupby("ticker", as_index=False).tail(1).reset_index(drop=True)


def load_intraday_spots(path: Path = SPOT_INPUT) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    if not path.exists():
        LOGGER.error("Intraday spot file missing: %s", path)
        return {}, {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        LOGGER.error("failed to parse %s: %s", path, exc)
        return {}, {}
    by_und = payload.get("by_underlying") or {}
    meta = {k: payload.get(k) for k in ("build_time", "n_underlyings_priced", "n_underlyings_universe", "sources")}
    return by_und, meta


def load_adv_latest(path: Path = UNDERLYING_VOLUME_PARQUET) -> dict[str, dict[str, Any]]:
    """Most recent 20d $ ADV per underlying."""
    if not path.exists():
        return {}
    try:
        df = pd.read_parquet(path)
    except Exception as exc:
        LOGGER.warning("failed to read %s: %s", path, exc)
        return {}
    if df.empty:
        return {}
    panel = compute_adv_panel(df, window=_ADV_WINDOW_DAYS)
    if panel.empty:
        return {}
    panel = panel.sort_values(["underlying", "date"])
    last = panel.groupby("underlying", as_index=False).tail(1)
    out: dict[str, dict[str, Any]] = {}
    for _, r in last.iterrows():
        adv = _f(r.get("underlying_dollar_adv_20d"))
        if adv is None or adv <= 0:
            continue
        out[str(r["underlying"]).upper()] = {
            "underlying_dollar_adv_20d": adv,
            "as_of_date": str(r.get("date") or ""),
        }
    return out


def load_intraday_bias(path: Path = INTRADAY_BIAS_JSON) -> dict[str, dict[str, Any]]:
    """Per-underlying signed-error bias from prior reconciliations.

    Returns ``{UND: {"mean_signed_error_pct": float, "n": int}}`` only when
    ``n >= MIN_BIAS_OBSERVATIONS``. ``mean_signed_error_pct`` is the average
    of ``(estimate - realised) / realised`` from prior trading days.
    """
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for sym, row in (payload.get("by_underlying") or {}).items():
        n = _f(row.get("n_observations"))
        bias = _f(row.get("mean_signed_error_pct"))
        if n is None or bias is None or n < MIN_BIAS_OBSERVATIONS:
            continue
        out[norm_sym(sym)] = {"mean_signed_error_pct": bias, "n": int(n)}
    return out


def load_blend_weight_overrides(path: Path = INTRADAY_BIAS_JSON) -> dict[str, float]:
    """Per-ticker NAV blend weights driven by reconciliation MSE.

    ``score_intraday_flows.py`` writes ``by_ticker[T].blend_weight_nav`` once
    a fund has at least ``MIN_BIAS_OBSERVATIONS`` reconciled days for *both*
    the spot and NAV paths. We honor the optimal weight clipped to
    ``[NAV_BLEND_WEIGHT_FLOOR, NAV_BLEND_WEIGHT_CEIL]`` (so a single bad day
    cannot kill an entire signal source).
    """
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    out: dict[str, float] = {}
    for sym, row in (payload.get("by_ticker") or {}).items():
        if not isinstance(row, dict):
            continue
        n = _f(row.get("n_observations"))
        w = _f(row.get("blend_weight_nav"))
        if n is None or w is None or n < MIN_BIAS_OBSERVATIONS:
            continue
        out[norm_sym(sym)] = max(NAV_BLEND_WEIGHT_FLOOR, min(NAV_BLEND_WEIGHT_CEIL, w))
    return out


def load_nav_forecast_latest(
    path: Path = NAV_FORECAST_LATEST,
    *,
    now: datetime | None = None,
    max_age_sec: float = NAV_FRESHNESS_MAX_SEC,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Read ``data/nav_forecasts/_latest.json`` and return ``(by_ticker, meta)``.

    Each output row keeps just the fields the blender needs: ``nav_hat``,
    ``nav_anchor``, ``model``, ``confidence``, ``ts``, ``delta`` (issuer beta
    actually used by the forecaster), and a ``stale`` flag. Stale rows are
    *kept* so we can surface the diagnostic, but they will not contribute
    weight in the blend.
    """
    if not path.exists():
        return {}, {"build_time": None, "anchor_date": None}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        LOGGER.warning("failed to parse %s: %s", path, exc)
        return {}, {"build_time": None, "anchor_date": None}
    by_sym = payload.get("by_symbol") or {}
    build_time = payload.get("build_time")
    anchor_date = payload.get("anchor_date")
    now = now or datetime.now(UTC)

    out: dict[str, dict[str, Any]] = {}
    for sym, row in by_sym.items():
        if not isinstance(row, dict):
            continue
        ticker = norm_sym(sym)
        if not ticker:
            continue
        ts_iso = row.get("ts") or build_time
        age_sec: float | None = None
        if ts_iso:
            try:
                ts_dt = datetime.fromisoformat(str(ts_iso).replace("Z", "+00:00"))
                if ts_dt.tzinfo is None:
                    ts_dt = ts_dt.replace(tzinfo=UTC)
                age_sec = max(0.0, (now - ts_dt).total_seconds())
            except Exception:
                age_sec = None
        confidence = str(row.get("confidence") or "na").lower()
        nav_hat = _f(row.get("nav_hat"))
        nav_anchor = _f(row.get("nav_anchor"))
        delta = _f(row.get("delta"))
        stale = (
            confidence == "na"
            or nav_hat is None or nav_hat <= 0
            or nav_anchor is None or nav_anchor <= 0
            or delta is None or delta == 0.0
            or (age_sec is not None and age_sec > max_age_sec)
        )
        out[ticker] = {
            "model": row.get("model"),
            "confidence": confidence,
            "ts": ts_iso,
            "age_sec": age_sec,
            "delta": delta,
            "nav_hat": nav_hat,
            "nav_anchor": nav_anchor,
            "und_spot_t": _f(row.get("und_spot_t")),
            "und_spot_anchor": _f(row.get("und_spot_anchor")),
            "und_symbol": str(row.get("und_symbol") or "").upper(),
            "stale": stale,
        }
    meta = {
        "build_time": build_time,
        "anchor_date": anchor_date,
        "n_total": len(out),
        "n_stale": sum(1 for v in out.values() if v.get("stale")),
        "default_models_count": payload.get("default_models_count"),
    }
    return out, meta


def _default_blend_weight(model: str | None, confidence: str | None) -> float:
    """Prior NAV weight before any per-ticker override is applied."""
    if not model:
        return 0.0
    base = NAV_BLEND_DEFAULT_BY_MODEL.get(str(model), 0.0)
    if base <= 0.0:
        return 0.0
    if (confidence or "").lower() == "medium":
        base *= NAV_BLEND_MEDIUM_CONFIDENCE_SCALE
    elif (confidence or "").lower() != "high":
        return 0.0
    return max(NAV_BLEND_WEIGHT_FLOOR, min(NAV_BLEND_WEIGHT_CEIL, base))


def implied_underlying_return_from_nav(
    *, nav_hat: float | None, nav_anchor: float | None, delta: float | None,
) -> float | None:
    """Recover the NAV model's implied ``r_und`` so the rebalance identity can
    be re-applied with structure-aware NAV information.

    For the closed-form ``delta_v1`` model::

        nav_hat = nav_anchor * exp(delta * r_und) * (1 - TER_d)

    so::

        r_und  ~  log(nav_hat / nav_anchor) / delta

    The TER drag (~ 1e-4 per day) and the v2 vol-drag term shift the inversion
    by O(10 bp); we fold them into the residual via the per-ticker bias map
    rather than trying to back them out exactly. For ``delta_v3_swap_mark`` /
    ``yieldboost_putspread_v1`` the relationship is non-linear, but the same
    inversion still gives a sensible "as-if" underlying return that captures
    the holdings-delta information up to a ticker-specific bias.
    """
    if (
        nav_hat is None or nav_anchor is None or delta is None
        or nav_hat <= 0 or nav_anchor <= 0 or delta == 0.0
        or not math.isfinite(nav_hat) or not math.isfinite(nav_anchor) or not math.isfinite(delta)
    ):
        return None
    try:
        return math.log(nav_hat / nav_anchor) / delta
    except (ValueError, ZeroDivisionError):
        return None


# ?? Compute ??????????????????????????????????????????????????????????????


def compute_fund_intraday(
    universe: pd.DataFrame,
    latest_metrics: pd.DataFrame,
    spots: dict[str, dict[str, Any]],
    *,
    nav_forecasts: dict[str, dict[str, Any]] | None = None,
    blend_weight_overrides: dict[str, float] | None = None,
) -> pd.DataFrame:
    """Per-fund estimated close rebalance from the Avellaneda-Zhang identity.

    Computes three estimates per fund and exposes the chosen "best" estimate
    in the legacy fields ``estimated_close_rebalance_dollars`` /
    ``return_d1_so_far`` so downstream code keeps working unchanged:

    * ``rebalance_spot``  -- L*(L-1)*AUM*r_spot   (raw underlying spot)
    * ``rebalance_nav``   -- L*(L-1)*AUM*r_nav    (NAV-implied r_und)
    * ``rebalance_blend`` -- weighted average of the two; weight on NAV
                              comes from the per-ticker reconciliation map
                              when available, otherwise from a model/confidence
                              prior.

    The blend is only as good as its components: when the NAV row is stale,
    ``nav`` weight collapses to 0 and ``blend == spot`` exactly. Callers can
    therefore treat ``rebalance_blend`` as the canonical estimator without
    losing the safety of the pure-spot path.
    """
    if universe.empty or latest_metrics.empty:
        return pd.DataFrame()

    nav_forecasts = nav_forecasts or {}
    blend_weight_overrides = blend_weight_overrides or {}

    df = latest_metrics.merge(universe, on="ticker", how="left")
    df["underlying"] = df["underlying"].fillna("")
    df["leverage"] = pd.to_numeric(df["leverage"], errors="coerce")
    df["aum_prior_close"] = pd.to_numeric(df.get("aum"), errors="coerce")
    df["nav_prior_close"] = pd.to_numeric(df.get("nav"), errors="coerce") if "nav" in df.columns else np.nan

    # Attach spot + NAV-forecast data per row.
    enrich_records = []
    for _, row in df.iterrows():
        und = str(row.get("underlying") or "").upper()
        s = spots.get(und) if und else None
        ticker = norm_sym(row.get("ticker"))
        n = nav_forecasts.get(ticker) if ticker else None
        enrich_records.append({
            "spot_last": _f(s.get("last")) if s else None,
            "spot_prior_close": _f(s.get("prior_close")) if s else None,
            "return_d1_so_far": _f(s.get("return_d1_so_far")) if s else None,
            "spot_as_of": s.get("as_of") if s else None,
            "spot_source": s.get("source") if s else None,
            "spot_stale": bool(s.get("stale")) if s else False,
            "underlying_volume_so_far": _f(s.get("volume_so_far")) if s else None,
            # NAV-forecast row (may be missing, stale, or not applicable for the
            # ETF universe row -- handled in _quality / weight policy below).
            "nav_model": (n or {}).get("model"),
            "nav_confidence": (n or {}).get("confidence"),
            "nav_ts": (n or {}).get("ts"),
            "nav_age_sec": (n or {}).get("age_sec"),
            "nav_delta": (n or {}).get("delta"),
            "nav_hat": (n or {}).get("nav_hat"),
            "nav_anchor": (n or {}).get("nav_anchor"),
            "nav_und_symbol": (n or {}).get("und_symbol"),
            "nav_stale": bool((n or {}).get("stale", True)) if n else True,
            "nav_present": n is not None,
        })
    enrich_df = pd.DataFrame(enrich_records)
    df = pd.concat([df.reset_index(drop=True), enrich_df.reset_index(drop=True)], axis=1)

    # NAV-implied underlying return.
    df["r_nav"] = df.apply(
        lambda r: implied_underlying_return_from_nav(
            nav_hat=_f(r.get("nav_hat")),
            nav_anchor=_f(r.get("nav_anchor")),
            delta=_f(r.get("nav_delta")),
        ),
        axis=1,
    )

    # Blend weight: per-ticker override > model/confidence prior > 0.
    def _weight(row: pd.Series) -> float:
        # No usable NAV row at all → spot only.
        if not bool(row.get("nav_present")) or bool(row.get("nav_stale")):
            return 0.0
        if row.get("r_nav") is None or not math.isfinite(float(row.get("r_nav", np.nan))):
            return 0.0
        ticker = norm_sym(row.get("ticker"))
        override = blend_weight_overrides.get(ticker)
        if override is not None:
            return float(override)
        return _default_blend_weight(row.get("nav_model"), row.get("nav_confidence"))

    df["blend_weight_nav"] = df.apply(_weight, axis=1).astype(float)
    df["blend_source"] = df.apply(
        lambda r: (
            "spot_only" if r["blend_weight_nav"] <= 0
            else ("nav_override" if norm_sym(r.get("ticker")) in blend_weight_overrides
                  else "nav_prior")
        ),
        axis=1,
    )

    def _quality(row: pd.Series) -> str:
        if not bool(row.get("included_in_universe")):
            return str(row.get("universe_exclusion_reason") or "excluded")
        if not math.isfinite(float(row.get("leverage", np.nan))):
            return "missing_leverage"
        aum = _f(row.get("aum_prior_close"))
        if aum is None or aum <= 0:
            return "missing_prior_aum"
        ret = _f(row.get("return_d1_so_far"))
        if ret is None:
            # Allow the row to be priced from NAV alone if the spot is stale
            # but a fresh, non-stale NAV row is available -- this is rare but
            # catches yieldboost holdings where ``options_cache`` is the only
            # spot source and may lag.
            if row.get("r_nav") is not None and float(row.get("blend_weight_nav") or 0) > 0:
                return "ok_nav_only"
            if row.get("spot_stale"):
                return "stale_spot"
            return "missing_spot"
        return "ok"

    df["quality_flag"] = df.apply(_quality, axis=1)
    ok_any = df["quality_flag"].isin(["ok", "ok_nav_only"])

    # Per-fund spot estimate: NaN when the spot return is missing, even if a
    # NAV row exists -- keeps ``rebalance_spot`` strictly tied to the spot path
    # so reconciliation can score it independently. Pre-typed as float64 so
    # subsequent ``df.loc[mask, ...] = series`` does not flip dtype.
    df["rebalance_spot"] = pd.Series(np.nan, index=df.index, dtype="float64")
    spot_ok = df["quality_flag"].eq("ok")
    if spot_ok.any():
        df.loc[spot_ok, "rebalance_spot"] = (
            df.loc[spot_ok, "leverage"].astype(float)
            * (df.loc[spot_ok, "leverage"].astype(float) - 1.0)
            * df.loc[spot_ok, "aum_prior_close"].astype(float)
            * df.loc[spot_ok, "return_d1_so_far"].astype(float)
        )

    # Per-fund NAV-implied estimate -- only surfaced when the NAV forecast row
    # itself is fresh, otherwise reconciliation would average yesterday's NAV
    # against today's realised flow.
    df["rebalance_nav"] = pd.Series(np.nan, index=df.index, dtype="float64")
    nav_ok = ok_any & df["r_nav"].notna() & (~df["nav_stale"].fillna(True).astype(bool))
    if nav_ok.any():
        df.loc[nav_ok, "rebalance_nav"] = (
            df.loc[nav_ok, "leverage"].astype(float)
            * (df.loc[nav_ok, "leverage"].astype(float) - 1.0)
            * df.loc[nav_ok, "aum_prior_close"].astype(float)
            * df.loc[nav_ok, "r_nav"].astype(float)
        )

    # Effective per-fund r_blend ALWAYS sums to 1 across (spot, nav). When a
    # source is missing we shift its weight to the surviving source so the
    # estimator stays well-defined and unbiased.
    def _r_blend(row: pd.Series) -> float | None:
        r_spot = _f(row.get("return_d1_so_far"))
        r_nav = _f(row.get("r_nav"))
        w_nav = float(row.get("blend_weight_nav") or 0.0)
        if r_spot is None and r_nav is None:
            return None
        if r_spot is None:
            return r_nav
        if r_nav is None or w_nav <= 0:
            return r_spot
        w_nav_eff = max(0.0, min(1.0, w_nav))
        return (1.0 - w_nav_eff) * r_spot + w_nav_eff * r_nav

    df["r_blend"] = df.apply(_r_blend, axis=1)

    df["rebalance_blend"] = pd.Series(np.nan, index=df.index, dtype="float64")
    blend_ok = ok_any & df["r_blend"].notna()
    if blend_ok.any():
        df.loc[blend_ok, "rebalance_blend"] = (
            df.loc[blend_ok, "leverage"].astype(float)
            * (df.loc[blend_ok, "leverage"].astype(float) - 1.0)
            * df.loc[blend_ok, "aum_prior_close"].astype(float)
            * df.loc[blend_ok, "r_blend"].astype(float)
        )

    # Surfaced "default" estimate: blend when defined, else spot, else NAV.
    surfaced = df["rebalance_blend"].copy()
    surfaced = surfaced.where(surfaced.notna(), df["rebalance_spot"])
    surfaced = surfaced.where(surfaced.notna(), df["rebalance_nav"])
    df["estimated_close_rebalance_dollars"] = surfaced
    df["estimated_close_rebalance_abs_dollars"] = df["estimated_close_rebalance_dollars"].abs()
    df["estimated_close_rebalance_pct_aum"] = (
        df["estimated_close_rebalance_dollars"] / df["aum_prior_close"]
    )
    df["included_in_aggregate"] = ok_any & df["estimated_close_rebalance_dollars"].notna()
    return df


def aggregate_underlying(
    fund_df: pd.DataFrame,
    *,
    adv_latest: dict[str, dict[str, Any]],
    bias_map: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    if fund_df.empty:
        return pd.DataFrame()
    eligible = fund_df[fund_df["included_in_aggregate"].astype(bool)].copy()
    if eligible.empty:
        return pd.DataFrame()
    grouped = eligible.groupby("underlying", as_index=False)
    agg = grouped.agg(
        estimated_net_close_rebalance_dollars=("estimated_close_rebalance_dollars", "sum"),
        estimated_gross_close_rebalance_dollars=("estimated_close_rebalance_abs_dollars", "sum"),
        estimated_net_close_rebalance_dollars_spot=("rebalance_spot", "sum"),
        estimated_net_close_rebalance_dollars_nav=("rebalance_nav", "sum"),
        estimated_net_close_rebalance_dollars_blend=("rebalance_blend", "sum"),
        total_letf_aum_prior_close=("aum_prior_close", "sum"),
        n_funds_priced=("ticker", "nunique"),
        return_d1_so_far=("return_d1_so_far", "mean"),
        r_blend_mean=("r_blend", "mean"),
        as_of=("spot_as_of", "max"),
        nav_age_sec_max=("nav_age_sec", "max"),
    )

    # Per-fund counts driving the trust signal on the underlying row.
    nav_priced = eligible[eligible["rebalance_nav"].notna()].groupby("underlying").size()
    spot_priced = eligible[eligible["rebalance_spot"].notna()].groupby("underlying").size()
    weighted_nav = (
        eligible.assign(_w=eligible["blend_weight_nav"].fillna(0.0)).groupby("underlying")["_w"].mean()
    )
    agg = agg.set_index("underlying")
    agg["n_funds_priced_spot"] = spot_priced
    agg["n_funds_priced_nav"] = nav_priced
    agg["mean_blend_weight_nav"] = weighted_nav
    agg = agg.reset_index()
    agg["n_funds_priced_spot"] = agg["n_funds_priced_spot"].fillna(0).astype(int)
    agg["n_funds_priced_nav"] = agg["n_funds_priced_nav"].fillna(0).astype(int)
    agg["mean_blend_weight_nav"] = agg["mean_blend_weight_nav"].fillna(0.0).astype(float)

    buys = (
        eligible[eligible["estimated_close_rebalance_dollars"] > 0]
        .groupby("underlying")["estimated_close_rebalance_dollars"].sum()
    )
    sells = (
        eligible[eligible["estimated_close_rebalance_dollars"] < 0]
        .groupby("underlying")["estimated_close_rebalance_dollars"].sum().abs()
    )
    agg = agg.set_index("underlying")
    agg["moc_buy_dollars_est"] = pd.to_numeric(buys, errors="coerce")
    agg["moc_sell_dollars_est"] = pd.to_numeric(sells, errors="coerce")
    agg = agg.reset_index()
    agg["moc_buy_dollars_est"] = agg["moc_buy_dollars_est"].fillna(0.0).astype(float)
    agg["moc_sell_dollars_est"] = agg["moc_sell_dollars_est"].fillna(0.0).astype(float)
    agg["estimated_close_rebalance_pct_aum"] = (
        agg["estimated_net_close_rebalance_dollars"] / agg["total_letf_aum_prior_close"]
    )

    # ADV + intraday volume context.
    agg["underlying_dollar_adv_20d"] = agg["underlying"].map(
        lambda u: adv_latest.get(u, {}).get("underlying_dollar_adv_20d")
    )
    vol_so_far = (
        eligible.groupby("underlying")["underlying_volume_so_far"].max().to_dict()
    )
    last_price = eligible.groupby("underlying")["spot_last"].max().to_dict()
    agg["underlying_volume_so_far"] = agg["underlying"].map(vol_so_far)
    agg["spot_last"] = agg["underlying"].map(last_price)
    agg["volume_so_far_dollars"] = (
        agg["underlying_volume_so_far"].astype(float) * agg["spot_last"].astype(float)
    )
    with np.errstate(divide="ignore", invalid="ignore"):
        agg["volume_so_far_pct_adv"] = (
            agg["volume_so_far_dollars"] / agg["underlying_dollar_adv_20d"]
        )
        agg["estimated_close_rebalance_pct_adv_20d"] = (
            agg["estimated_net_close_rebalance_dollars"] / agg["underlying_dollar_adv_20d"]
        )

    # Already-realised approximation: cap at 1.
    agg["already_realized_share"] = pd.to_numeric(
        agg["volume_so_far_pct_adv"], errors="coerce"
    ).clip(lower=0, upper=1)
    agg["remaining_close_rebalance_dollars"] = (
        (1.0 - agg["already_realized_share"].fillna(0.0))
        * agg["estimated_net_close_rebalance_dollars"]
    )

    # Optional bias adjustment from G.1 reconciliations.
    def _bias(row: pd.Series) -> float | None:
        b = bias_map.get(str(row["underlying"]).upper())
        return b["mean_signed_error_pct"] if b else None

    agg["bias_signed_error_pct"] = agg.apply(_bias, axis=1)
    agg["estimated_close_rebalance_dollars_bias_adj"] = agg.apply(
        lambda r: (
            float(r["estimated_net_close_rebalance_dollars"]) * (1.0 - float(r["bias_signed_error_pct"]))
            if pd.notna(r.get("estimated_net_close_rebalance_dollars"))
            and pd.notna(r.get("bias_signed_error_pct"))
            else None
        ),
        axis=1,
    )
    return agg


# ?? Outputs ??????????????????????????????????????????????????????????????


def _top_contributors(fund_df: pd.DataFrame, underlying: str, *, n: int = 5) -> list[dict[str, Any]]:
    rows = fund_df[
        (fund_df["underlying"].eq(underlying))
        & (fund_df["included_in_aggregate"].astype(bool))
    ].copy()
    if rows.empty:
        return []
    rows["_abs"] = rows["estimated_close_rebalance_dollars"].abs()
    rows = rows.sort_values("_abs", ascending=False).head(n)
    return [
        {
            "ticker": r["ticker"],
            "leverage": _round(r.get("leverage"), 4),
            "estimated_close_rebalance_dollars": _round(r.get("estimated_close_rebalance_dollars"), 2),
            "rebalance_spot": _round(r.get("rebalance_spot"), 2),
            "rebalance_nav": _round(r.get("rebalance_nav"), 2),
            "rebalance_blend": _round(r.get("rebalance_blend"), 2),
            "blend_weight_nav": _round(r.get("blend_weight_nav"), 4),
            "nav_model": r.get("nav_model"),
            "nav_confidence": r.get("nav_confidence"),
            "aum_prior_close": _round(r.get("aum_prior_close"), 2),
        }
        for _, r in rows.iterrows()
    ]


def build_payloads(
    fund_df: pd.DataFrame,
    aggregates: pd.DataFrame,
    spot_meta: dict[str, Any],
    *,
    now: datetime,
    bias_map: dict[str, dict[str, Any]],
    nav_meta: dict[str, Any] | None = None,
    blend_overrides_count: int = 0,
) -> dict[str, Any]:
    close_dt = _market_close_utc(now)
    minutes_to_close = max(0, int(round((close_dt - now).total_seconds() / 60.0)))
    nav_meta = nav_meta or {}

    by_fund: dict[str, dict[str, Any]] = {}
    for _, row in fund_df.iterrows():
        ticker = str(row.get("ticker") or "").upper()
        if not ticker:
            continue
        by_fund[ticker] = {
            "ticker": ticker,
            "underlying": str(row.get("underlying") or "").upper(),
            "product_class": row.get("product_class"),
            "leverage": _round(row.get("leverage"), 4),
            "aum_prior_close": _round(row.get("aum_prior_close"), 2),
            "return_d1_so_far": _round(row.get("return_d1_so_far"), 8),
            "r_nav": _round(row.get("r_nav"), 8),
            "r_blend": _round(row.get("r_blend"), 8),
            "blend_weight_nav": _round(row.get("blend_weight_nav"), 4),
            "blend_source": row.get("blend_source"),
            "rebalance_spot_dollars": _round(row.get("rebalance_spot"), 2),
            "rebalance_nav_dollars": _round(row.get("rebalance_nav"), 2),
            "rebalance_blend_dollars": _round(row.get("rebalance_blend"), 2),
            "estimated_close_rebalance_dollars": _round(row.get("estimated_close_rebalance_dollars"), 2),
            "estimated_close_rebalance_pct_aum": _round(row.get("estimated_close_rebalance_pct_aum"), 8),
            "spot_as_of": row.get("spot_as_of"),
            "spot_source": row.get("spot_source"),
            "spot_stale": bool(row.get("spot_stale") or False),
            "nav_model": row.get("nav_model"),
            "nav_confidence": row.get("nav_confidence"),
            "nav_ts": row.get("nav_ts"),
            "nav_age_sec": _round(row.get("nav_age_sec"), 1),
            "nav_stale": bool(row.get("nav_stale") or False),
            "nav_present": bool(row.get("nav_present") or False),
            "quality_flag": row.get("quality_flag") or "missing",
            "included_in_aggregate": bool(row.get("included_in_aggregate") or False),
        }

    by_underlying: dict[str, dict[str, Any]] = {}
    if not aggregates.empty:
        sorted_agg = aggregates.sort_values(
            "estimated_net_close_rebalance_dollars",
            key=lambda s: s.abs(),
            ascending=False,
        )
        for _, row in sorted_agg.iterrows():
            und = str(row.get("underlying") or "").upper()
            if not und:
                continue
            by_underlying[und] = {
                "underlying": und,
                "as_of": row.get("as_of"),
                "return_d1_so_far": _round(row.get("return_d1_so_far"), 8),
                "r_blend_mean": _round(row.get("r_blend_mean"), 8),
                "spot_last": _round(row.get("spot_last"), 6),
                "n_funds_priced": int(row.get("n_funds_priced") or 0),
                "n_funds_priced_spot": int(row.get("n_funds_priced_spot") or 0),
                "n_funds_priced_nav": int(row.get("n_funds_priced_nav") or 0),
                "mean_blend_weight_nav": _round(row.get("mean_blend_weight_nav"), 4),
                "nav_age_sec_max": _round(row.get("nav_age_sec_max"), 1),
                "total_letf_aum_prior_close": _round(row.get("total_letf_aum_prior_close"), 2),
                "estimated_net_close_rebalance_dollars": _round(row.get("estimated_net_close_rebalance_dollars"), 2),
                "estimated_net_close_rebalance_dollars_spot": _round(
                    row.get("estimated_net_close_rebalance_dollars_spot"), 2,
                ),
                "estimated_net_close_rebalance_dollars_nav": _round(
                    row.get("estimated_net_close_rebalance_dollars_nav"), 2,
                ),
                "estimated_net_close_rebalance_dollars_blend": _round(
                    row.get("estimated_net_close_rebalance_dollars_blend"), 2,
                ),
                "estimated_gross_close_rebalance_dollars": _round(row.get("estimated_gross_close_rebalance_dollars"), 2),
                "moc_buy_dollars_est": _round(row.get("moc_buy_dollars_est"), 2),
                "moc_sell_dollars_est": _round(row.get("moc_sell_dollars_est"), 2),
                "estimated_close_rebalance_pct_aum": _round(row.get("estimated_close_rebalance_pct_aum"), 8),
                "underlying_dollar_adv_20d": _round(row.get("underlying_dollar_adv_20d"), 2),
                "underlying_volume_so_far": _round(row.get("underlying_volume_so_far"), 0),
                "volume_so_far_dollars": _round(row.get("volume_so_far_dollars"), 2),
                "volume_so_far_pct_adv": _round(row.get("volume_so_far_pct_adv"), 8),
                "estimated_close_rebalance_pct_adv_20d": _round(row.get("estimated_close_rebalance_pct_adv_20d"), 8),
                "already_realized_share": _round(row.get("already_realized_share"), 6),
                "remaining_close_rebalance_dollars": _round(row.get("remaining_close_rebalance_dollars"), 2),
                "estimated_close_rebalance_dollars_bias_adj": _round(
                    row.get("estimated_close_rebalance_dollars_bias_adj"), 2,
                ),
                "bias_signed_error_pct": _round(row.get("bias_signed_error_pct"), 6),
                "top_contributors": _top_contributors(fund_df, und),
            }

    payload = {
        "build_time": now.isoformat().replace("+00:00", "Z"),
        "as_of": now.isoformat().replace("+00:00", "Z"),
        "trading_date": now.date().isoformat(),
        "minutes_to_close": minutes_to_close,
        "spot_build_time": spot_meta.get("build_time"),
        "spot_sources": spot_meta.get("sources"),
        "spot_priced_count": spot_meta.get("n_underlyings_priced"),
        "spot_universe_count": spot_meta.get("n_underlyings_universe"),
        "nav_forecast_build_time": nav_meta.get("build_time"),
        "nav_forecast_anchor_date": nav_meta.get("anchor_date"),
        "nav_forecast_default_models_count": nav_meta.get("default_models_count"),
        "nav_funds_total": nav_meta.get("n_total"),
        "nav_funds_stale": nav_meta.get("n_stale"),
        "blend_overrides_count": int(blend_overrides_count or 0),
        "method": (
            "L*(L-1)*prior_close_aum*r_blend; r_blend = w_spot*r_spot + w_nav*r_nav,"
            " r_nav = log(nav_hat/nav_anchor)/delta_forecast"
        ),
        "n_funds_total": int(len(fund_df)),
        "n_funds_priced": int(fund_df["included_in_aggregate"].sum()) if not fund_df.empty else 0,
        "n_funds_priced_spot": int(fund_df["rebalance_spot"].notna().sum()) if not fund_df.empty else 0,
        "n_funds_priced_nav": int(fund_df["rebalance_nav"].notna().sum()) if not fund_df.empty else 0,
        "n_funds_priced_blend": int(fund_df["rebalance_blend"].notna().sum()) if not fund_df.empty else 0,
        "n_underlyings_priced": len(by_underlying),
        "by_fund": by_fund,
        "by_underlying": by_underlying,
        "bias_underlyings_count": len([b for b in bias_map.values() if b]),
    }
    return payload


def read_today_history(snapshot_dir: Path, trading_date: str, *, n: int = 24) -> list[dict[str, Any]]:
    """Compact tail of today's snapshots for the per-ETF convergence chart.

    Returns last-N rows with just ``as_of``, ``minutes_to_close`` and a
    ``net_by_underlying`` map (signed dollars only). Sub-1MB total even for
    a 220-underlying universe over 24 builds.
    """
    path = snapshot_dir / f"{trading_date}.jsonl"
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as f:
            lines = f.readlines()
    except Exception as exc:
        LOGGER.warning("could not read %s: %s", path, exc)
        return []
    out: list[dict[str, Any]] = []
    for line in lines[-int(n):]:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        by_und_obj = obj.get("by_underlying") or {}
        net_by_und: dict[str, float] = {}
        net_spot_by_und: dict[str, float] = {}
        net_nav_by_und: dict[str, float] = {}
        net_blend_by_und: dict[str, float] = {}
        for und, row in by_und_obj.items():
            sym = str(und).upper()
            chosen = _round(row.get("estimated_net_close_rebalance_dollars"), 2)
            spot_v = _round(row.get("estimated_net_close_rebalance_dollars_spot"), 2)
            nav_v = _round(row.get("estimated_net_close_rebalance_dollars_nav"), 2)
            blend_v = _round(row.get("estimated_net_close_rebalance_dollars_blend"), 2)
            if chosen is not None:
                net_by_und[sym] = chosen
            if spot_v is not None:
                net_spot_by_und[sym] = spot_v
            if nav_v is not None:
                net_nav_by_und[sym] = nav_v
            if blend_v is not None:
                net_blend_by_und[sym] = blend_v
        out.append({
            "as_of": obj.get("as_of"),
            "minutes_to_close": obj.get("minutes_to_close"),
            "n_underlyings_priced": obj.get("n_underlyings_priced"),
            "net_by_underlying": net_by_und,
            "net_by_underlying_spot": net_spot_by_und,
            "net_by_underlying_nav": net_nav_by_und,
            "net_by_underlying_blend": net_blend_by_und,
        })
    return out


def append_snapshot(snapshot_dir: Path, payload: dict[str, Any]) -> Path:
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    trading_date = payload.get("trading_date") or datetime.now(UTC).date().isoformat()
    path = snapshot_dir / f"{trading_date}.jsonl"

    # Trim the snapshot to a compact line: full per-underlying payload only
    # (per-fund detail explodes the file size; recovered on-demand via the
    # latest JSON for the most recent build).
    line = {
        "build_time": payload.get("build_time"),
        "as_of": payload.get("as_of"),
        "minutes_to_close": payload.get("minutes_to_close"),
        "n_funds_priced": payload.get("n_funds_priced"),
        "n_underlyings_priced": payload.get("n_underlyings_priced"),
        "by_underlying": payload.get("by_underlying"),
    }
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(_json_clean(line), separators=(",", ":"), allow_nan=False, sort_keys=True))
        f.write("\n")
    return path


# ?? Pipeline ?????????????????????????????????????????????????????????????


def build_all(
    *,
    universe_path: Path,
    metrics_parquet: Path,
    metrics_csv: Path,
    spot_path: Path,
    volume_path: Path,
    bias_path: Path,
    nav_forecast_path: Path = NAV_FORECAST_LATEST,
    now: datetime | None = None,
    nav_max_age_sec: float = NAV_FRESHNESS_MAX_SEC,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    dict[str, Any],
    dict[str, dict[str, Any]],
    dict[str, Any],
    dict[str, float],
]:
    universe = load_universe(universe_path)
    metrics = load_metrics(metrics_parquet, metrics_csv)
    latest = latest_metrics_per_ticker(metrics)
    spots, spot_meta = load_intraday_spots(spot_path)
    adv = load_adv_latest(volume_path)
    bias_map = load_intraday_bias(bias_path)
    blend_overrides = load_blend_weight_overrides(bias_path)
    nav_forecasts, nav_meta = load_nav_forecast_latest(
        nav_forecast_path, now=now, max_age_sec=nav_max_age_sec,
    )

    fund_df = compute_fund_intraday(
        universe, latest, spots,
        nav_forecasts=nav_forecasts,
        blend_weight_overrides=blend_overrides,
    )
    aggregates = aggregate_underlying(fund_df, adv_latest=adv, bias_map=bias_map)
    return fund_df, aggregates, spot_meta, bias_map, nav_meta, blend_overrides


def main() -> int:
    parser = argparse.ArgumentParser(description="Estimate today's pending LETF close rebalance")
    parser.add_argument("--universe", type=Path, default=DATA_DIR / "etf_screened_today.csv")
    parser.add_argument("--metrics-parquet", type=Path, default=DATA_DIR / "etf_metrics_daily.parquet")
    parser.add_argument("--metrics-csv", type=Path, default=DATA_DIR / "etf_metrics_daily.csv")
    parser.add_argument("--spot", type=Path, default=SPOT_INPUT)
    parser.add_argument("--volume", type=Path, default=UNDERLYING_VOLUME_PARQUET)
    parser.add_argument("--bias", type=Path, default=INTRADAY_BIAS_JSON)
    parser.add_argument("--latest-json", type=Path, default=INTRADAY_OUTPUT)
    parser.add_argument("--snapshot-dir", type=Path, default=SNAPSHOT_DIR)
    parser.add_argument("--no-snapshot", action="store_true", help="Skip appending to the daily snapshots file.")
    parser.add_argument(
        "--nav-forecast", type=Path, default=NAV_FORECAST_LATEST,
        help="Path to data/nav_forecasts/_latest.json (set to a missing path to disable blending).",
    )
    parser.add_argument(
        "--nav-max-age-sec", type=float, default=NAV_FRESHNESS_MAX_SEC,
        help="Maximum age in seconds before a NAV forecast row is treated as stale.",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(levelname)s:%(name)s:%(message)s",
    )

    now = datetime.now(UTC)
    fund_df, aggregates, spot_meta, bias_map, nav_meta, blend_overrides = build_all(
        universe_path=args.universe,
        metrics_parquet=args.metrics_parquet,
        metrics_csv=args.metrics_csv,
        spot_path=args.spot,
        volume_path=args.volume,
        bias_path=args.bias,
        nav_forecast_path=args.nav_forecast,
        now=now,
        nav_max_age_sec=args.nav_max_age_sec,
    )
    payload = build_payloads(
        fund_df, aggregates, spot_meta,
        now=now, bias_map=bias_map,
        nav_meta=nav_meta, blend_overrides_count=len(blend_overrides),
    )
    if not args.no_snapshot:
        snap_path = append_snapshot(args.snapshot_dir, payload)
        LOGGER.info("appended snapshot to %s", snap_path)
        # The "today_history" tail is constructed *after* appending so the
        # current build is included on the right edge of the convergence chart.
        payload["today_history"] = read_today_history(args.snapshot_dir, payload["trading_date"], n=24)
    else:
        payload["today_history"] = []
    _write_json(args.latest_json, payload)

    LOGGER.info(
        "intraday flow: funds priced=%d/%d (spot=%d nav=%d blend=%d) underlyings=%d "
        "minutes_to_close=%s biased_underlyings=%d nav_overrides=%d nav_stale=%s",
        payload["n_funds_priced"],
        payload["n_funds_total"],
        payload["n_funds_priced_spot"],
        payload["n_funds_priced_nav"],
        payload["n_funds_priced_blend"],
        payload["n_underlyings_priced"],
        payload["minutes_to_close"],
        payload["bias_underlyings_count"],
        payload["blend_overrides_count"],
        payload.get("nav_funds_stale"),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

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
INTRADAY_OUTPUT = DATA_DIR / "letf_rebalance_flows_intraday_latest.json"
SNAPSHOT_DIR = DATA_DIR / "letf_rebalance_flows_intraday_snapshots"
INTRADAY_BIAS_JSON = DATA_DIR / "letf_intraday_flow_metrics.json"

# Bias-adjustment trigger: only apply per-underlying calibration once we have
# at least this many T+1 reconciliations (see ``score_intraday_flows.py``).
MIN_BIAS_OBSERVATIONS = 5


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


# ?? Compute ??????????????????????????????????????????????????????????????


def compute_fund_intraday(
    universe: pd.DataFrame,
    latest_metrics: pd.DataFrame,
    spots: dict[str, dict[str, Any]],
) -> pd.DataFrame:
    """Per-fund estimated close rebalance from L*(L-1)*AUM*return_so_far."""
    if universe.empty or latest_metrics.empty:
        return pd.DataFrame()

    df = latest_metrics.merge(universe, on="ticker", how="left")
    df["underlying"] = df["underlying"].fillna("")
    df["leverage"] = pd.to_numeric(df["leverage"], errors="coerce")
    df["aum_prior_close"] = pd.to_numeric(df.get("aum"), errors="coerce")
    df["nav_prior_close"] = pd.to_numeric(df.get("nav"), errors="coerce") if "nav" in df.columns else np.nan

    # Attach spot data per row.
    spot_records = []
    for _, row in df.iterrows():
        und = str(row.get("underlying") or "").upper()
        s = spots.get(und) if und else None
        spot_records.append({
            "spot_last": _f(s.get("last")) if s else None,
            "spot_prior_close": _f(s.get("prior_close")) if s else None,
            "return_d1_so_far": _f(s.get("return_d1_so_far")) if s else None,
            "spot_as_of": s.get("as_of") if s else None,
            "spot_source": s.get("source") if s else None,
            "spot_stale": bool(s.get("stale")) if s else False,
            "underlying_volume_so_far": _f(s.get("volume_so_far")) if s else None,
        })
    spot_df = pd.DataFrame(spot_records)
    df = pd.concat([df.reset_index(drop=True), spot_df.reset_index(drop=True)], axis=1)

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
            if row.get("spot_stale"):
                return "stale_spot"
            return "missing_spot"
        return "ok"

    df["quality_flag"] = df.apply(_quality, axis=1)
    ok = df["quality_flag"].eq("ok")
    df["estimated_close_rebalance_dollars"] = np.nan
    df.loc[ok, "estimated_close_rebalance_dollars"] = (
        df.loc[ok, "leverage"]
        * (df.loc[ok, "leverage"] - 1.0)
        * df.loc[ok, "aum_prior_close"]
        * df.loc[ok, "return_d1_so_far"]
    )
    df["estimated_close_rebalance_abs_dollars"] = df["estimated_close_rebalance_dollars"].abs()
    df["estimated_close_rebalance_pct_aum"] = (
        df["estimated_close_rebalance_dollars"] / df["aum_prior_close"]
    )
    df["included_in_aggregate"] = ok
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
        total_letf_aum_prior_close=("aum_prior_close", "sum"),
        n_funds_priced=("ticker", "nunique"),
        return_d1_so_far=("return_d1_so_far", "mean"),
        as_of=("spot_as_of", "max"),
    )

    buys = (
        eligible[eligible["estimated_close_rebalance_dollars"] > 0]
        .groupby("underlying")["estimated_close_rebalance_dollars"].sum()
    )
    sells = (
        eligible[eligible["estimated_close_rebalance_dollars"] < 0]
        .groupby("underlying")["estimated_close_rebalance_dollars"].sum().abs()
    )
    agg = agg.set_index("underlying")
    agg["moc_buy_dollars_est"] = buys
    agg["moc_sell_dollars_est"] = sells
    agg = agg.fillna({"moc_buy_dollars_est": 0.0, "moc_sell_dollars_est": 0.0}).reset_index()
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
) -> dict[str, Any]:
    close_dt = _market_close_utc(now)
    minutes_to_close = max(0, int(round((close_dt - now).total_seconds() / 60.0)))

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
            "estimated_close_rebalance_dollars": _round(row.get("estimated_close_rebalance_dollars"), 2),
            "estimated_close_rebalance_pct_aum": _round(row.get("estimated_close_rebalance_pct_aum"), 8),
            "spot_as_of": row.get("spot_as_of"),
            "spot_source": row.get("spot_source"),
            "spot_stale": bool(row.get("spot_stale") or False),
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
                "spot_last": _round(row.get("spot_last"), 6),
                "n_funds_priced": int(row.get("n_funds_priced") or 0),
                "total_letf_aum_prior_close": _round(row.get("total_letf_aum_prior_close"), 2),
                "estimated_net_close_rebalance_dollars": _round(row.get("estimated_net_close_rebalance_dollars"), 2),
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
        "method": "L*(L-1)*prior_close_aum*return_d1_so_far",
        "n_funds_total": int(len(fund_df)),
        "n_funds_priced": int(fund_df["included_in_aggregate"].sum()) if not fund_df.empty else 0,
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
        net_by_und = {
            str(und).upper(): _round(row.get("estimated_net_close_rebalance_dollars"), 2)
            for und, row in by_und_obj.items()
            if row.get("estimated_net_close_rebalance_dollars") is not None
        }
        out.append({
            "as_of": obj.get("as_of"),
            "minutes_to_close": obj.get("minutes_to_close"),
            "n_underlyings_priced": obj.get("n_underlyings_priced"),
            "net_by_underlying": net_by_und,
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
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any], dict[str, dict[str, Any]]]:
    universe = load_universe(universe_path)
    metrics = load_metrics(metrics_parquet, metrics_csv)
    latest = latest_metrics_per_ticker(metrics)
    spots, spot_meta = load_intraday_spots(spot_path)
    adv = load_adv_latest(volume_path)
    bias_map = load_intraday_bias(bias_path)

    fund_df = compute_fund_intraday(universe, latest, spots)
    aggregates = aggregate_underlying(fund_df, adv_latest=adv, bias_map=bias_map)
    return fund_df, aggregates, spot_meta, bias_map


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
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(levelname)s:%(name)s:%(message)s",
    )

    fund_df, aggregates, spot_meta, bias_map = build_all(
        universe_path=args.universe,
        metrics_parquet=args.metrics_parquet,
        metrics_csv=args.metrics_csv,
        spot_path=args.spot,
        volume_path=args.volume,
        bias_path=args.bias,
    )
    now = datetime.now(UTC)
    payload = build_payloads(fund_df, aggregates, spot_meta, now=now, bias_map=bias_map)
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
        "intraday flow: funds priced=%d/%d underlyings=%d minutes_to_close=%s biased_underlyings=%d",
        payload["n_funds_priced"],
        payload["n_funds_total"],
        payload["n_underlyings_priced"],
        payload["minutes_to_close"],
        payload["bias_underlyings_count"],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

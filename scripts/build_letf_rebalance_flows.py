#!/usr/bin/env python3
"""Build leveraged-ETF close rebalance-flow estimates.

The core estimate is the daily-reset LETF rebalance identity:

    rebalance_notional = L * (L - 1) * prior_close_aum * underlying_return

where L is the fund's target leverage / delta, AUM is measured at the prior
close, and the underlying return is the close-to-close move. Positive values
are expected buy pressure into the close; negative values are sell pressure.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

LOGGER = logging.getLogger("letf_rebalance_flows")

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = REPO_ROOT / "data"
UNIVERSE_CSV = DATA_DIR / "etf_screened_today.csv"
METRICS_PARQUET = DATA_DIR / "etf_metrics_daily.parquet"
METRICS_CSV = DATA_DIR / "etf_metrics_daily.csv"

DAILY_PARQUET = DATA_DIR / "letf_rebalance_flows_daily.parquet"
DAILY_JSON = DATA_DIR / "letf_rebalance_flows_daily.json"
LATEST_JSON = DATA_DIR / "letf_rebalance_flows_latest.json"

INCLUDED_PRODUCT_CLASSES = {"letf", "inverse", "volatility_etp"}
EXCLUDED_PRODUCT_CLASSES = {
    "income_yieldboost",
    "income_put_spread",
    "passive_low_delta",
    "passive_low_beta",
    "other_structured",
}


def norm_sym(v: object) -> str:
    return str(v or "").strip().upper().replace(".", "-")


def _f(v: object) -> float | None:
    try:
        out = float(v)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _truthy(v: object) -> bool:
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    return str(v).strip().lower() in {"1", "true", "yes", "y", "t"}


def rebalance_notional(aum_prior_close: float, leverage: float, underlying_return: float) -> float:
    """Expected close rebalance dollars for one LETF.

    Positive means buy pressure; negative means sell pressure.
    """
    return float(leverage) * (float(leverage) - 1.0) * float(aum_prior_close) * float(underlying_return)


def _leverage_from_row(row: pd.Series) -> float | None:
    """Prefer explicit target leverage, then expected leverage, then fitted delta."""
    for col in ("Leverage", "ExpectedLeverage", "expected_leverage", "Delta", "beta"):
        if col in row.index:
            val = _f(row.get(col))
            if val is not None:
                return val
    return None


def _product_class_from_row(row: pd.Series, leverage: float | None) -> str | None:
    pc = str(row.get("product_class") or "").strip().lower()
    if pc and pc != "nan":
        return pc
    delta_pc = str(row.get("Delta_product_class") or "").strip().lower()
    if delta_pc in {"letf_long", "letf"}:
        return "letf"
    if delta_pc in {"letf_inverse", "inverse"}:
        return "inverse"
    if leverage is not None:
        if leverage < 0:
            return "inverse"
        if leverage > 1.05:
            return "letf"
    return None


def _include_reason(row: pd.Series, leverage: float | None, product_class: str | None) -> tuple[bool, str | None]:
    if _truthy(row.get("is_yieldboost")) or product_class in {"income_yieldboost", "income_put_spread"}:
        return False, "income_overlay"
    if product_class in EXCLUDED_PRODUCT_CLASSES:
        return False, f"product_class:{product_class}"
    if product_class not in INCLUDED_PRODUCT_CLASSES:
        return False, f"product_class:{product_class or 'unknown'}"
    if leverage is None:
        return False, "missing_leverage"
    # 1x passive products have no meaningful daily-reset rebalance term.
    if product_class != "inverse" and abs(leverage) <= 1.05:
        return False, "non_leveraged"
    if product_class == "inverse" and leverage >= 0:
        return False, "inverse_positive_leverage"
    return True, None


def load_universe(path: Path = UNIVERSE_CSV) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Universe CSV missing: {path}")
    raw = pd.read_csv(path)
    if "ETF" not in raw.columns or "Underlying" not in raw.columns:
        raise ValueError(f"Universe CSV must include ETF and Underlying columns: {path}")

    rows: list[dict[str, Any]] = []
    for _, row in raw.iterrows():
        symbol = norm_sym(row.get("ETF"))
        underlying = norm_sym(row.get("Underlying"))
        leverage = _leverage_from_row(row)
        product_class = _product_class_from_row(row, leverage)
        include, reason = _include_reason(row, leverage, product_class)
        rows.append({
            "ticker": symbol,
            "underlying": underlying,
            "leverage": leverage,
            "product_class": product_class,
            "included_in_universe": include,
            "universe_exclusion_reason": reason,
            "is_yieldboost": _truthy(row.get("is_yieldboost")),
        })
    out = pd.DataFrame(rows).drop_duplicates(subset=["ticker"], keep="first")
    return out[out["ticker"].astype(bool)].reset_index(drop=True)


def load_metrics(parquet_path: Path = METRICS_PARQUET, csv_path: Path = METRICS_CSV) -> pd.DataFrame:
    if parquet_path.exists():
        df = pd.read_parquet(parquet_path)
    elif csv_path.exists():
        df = pd.read_csv(csv_path)
    else:
        raise FileNotFoundError(f"Missing ETF metrics panel: {parquet_path} or {csv_path}")
    if "ticker" not in df.columns or "date" not in df.columns:
        raise ValueError("ETF metrics panel must include ticker and date columns")
    out = df.copy()
    out["ticker"] = out["ticker"].map(norm_sym)
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    for col in ("aum", "nav", "shares_outstanding", "shares_traded", "close_price", "underlying_adj_close", "stale_age_bdays"):
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    if "stale" in out.columns:
        out["stale"] = out["stale"].map(_truthy)
    else:
        out["stale"] = False
    return out.dropna(subset=["date", "ticker"]).sort_values(["ticker", "date"]).reset_index(drop=True)


def build_fund_flows(universe: pd.DataFrame, metrics: pd.DataFrame, *, stale_bdays: int = 3) -> pd.DataFrame:
    if universe.empty or metrics.empty:
        return pd.DataFrame()

    df = metrics.merge(universe, on="ticker", how="left")
    df["underlying"] = df["underlying"].fillna("")
    df["leverage"] = pd.to_numeric(df["leverage"], errors="coerce")
    df["aum"] = pd.to_numeric(df.get("aum"), errors="coerce")
    df["underlying_adj_close"] = pd.to_numeric(df.get("underlying_adj_close"), errors="coerce")
    df = df.sort_values(["ticker", "date"]).copy()

    by_ticker = df.groupby("ticker", sort=False)
    df["aum_prior_close"] = by_ticker["aum"].shift(1)
    df["nav_prior_close"] = by_ticker["nav"].shift(1) if "nav" in df.columns else np.nan
    df["shares_outstanding_prior_close"] = (
        by_ticker["shares_outstanding"].shift(1) if "shares_outstanding" in df.columns else np.nan
    )
    df["underlying_adj_close_prior"] = by_ticker["underlying_adj_close"].shift(1)
    stale_prior = by_ticker["stale"].shift(1)
    df["stale_prior_close"] = stale_prior.map(lambda x: bool(x) if pd.notna(x) else False)
    df["stale_age_bdays_prior_close"] = (
        by_ticker["stale_age_bdays"].shift(1) if "stale_age_bdays" in df.columns else np.nan
    )
    df["underlying_return_d1"] = (df["underlying_adj_close"] / df["underlying_adj_close_prior"]) - 1.0

    def quality(row: pd.Series) -> str:
        if not bool(row.get("included_in_universe")):
            return str(row.get("universe_exclusion_reason") or "excluded")
        if not math.isfinite(float(row.get("leverage", np.nan))):
            return "missing_leverage"
        if not math.isfinite(float(row.get("aum_prior_close", np.nan))) or float(row.get("aum_prior_close")) <= 0:
            return "missing_prior_aum"
        if not math.isfinite(float(row.get("underlying_return_d1", np.nan))):
            return "missing_underlying_return"
        stale_age = _f(row.get("stale_age_bdays_prior_close"))
        if bool(row.get("stale_prior_close")) or (stale_age is not None and stale_age > stale_bdays):
            return "stale_aum"
        return "ok"

    df["quality_flag"] = df.apply(quality, axis=1)
    ok = df["quality_flag"].eq("ok")
    df["rebalance_signed_dollars"] = np.nan
    df.loc[ok, "rebalance_signed_dollars"] = (
        df.loc[ok, "leverage"]
        * (df.loc[ok, "leverage"] - 1.0)
        * df.loc[ok, "aum_prior_close"]
        * df.loc[ok, "underlying_return_d1"]
    )
    df["rebalance_abs_dollars"] = df["rebalance_signed_dollars"].abs()
    df["abs_rebalance_pct_prior_aum"] = df["rebalance_abs_dollars"] / df["aum_prior_close"]
    df["included_in_aggregate"] = ok

    cols = [
        "date", "ticker", "underlying", "product_class", "leverage",
        "aum_prior_close", "nav_prior_close", "shares_outstanding_prior_close",
        "underlying_adj_close_prior", "underlying_adj_close", "underlying_return_d1",
        "rebalance_signed_dollars", "rebalance_abs_dollars", "abs_rebalance_pct_prior_aum",
        "included_in_aggregate", "quality_flag", "source_provider", "status",
    ]
    for col in cols:
        if col not in df.columns:
            df[col] = None
    out = df[cols].copy()
    out["date"] = out["date"].dt.strftime("%Y-%m-%d")
    return out


def build_underlying_aggregates(fund_flows: pd.DataFrame) -> pd.DataFrame:
    if fund_flows.empty:
        return pd.DataFrame()
    eligible = fund_flows[fund_flows["included_in_aggregate"].astype(bool)].copy()
    if eligible.empty:
        return pd.DataFrame()

    grouped = eligible.groupby(["date", "underlying"], as_index=False)
    agg = grouped.agg(
        net_moc_dollars=("rebalance_signed_dollars", "sum"),
        gross_moc_dollars=("rebalance_abs_dollars", "sum"),
        total_letf_aum_prior_close=("aum_prior_close", "sum"),
        n_funds=("ticker", "nunique"),
        underlying_return_d1=("underlying_return_d1", "mean"),
    )
    buys = eligible[eligible["rebalance_signed_dollars"] > 0].groupby(["date", "underlying"])["rebalance_signed_dollars"].sum()
    sells = eligible[eligible["rebalance_signed_dollars"] < 0].groupby(["date", "underlying"])["rebalance_signed_dollars"].sum().abs()
    agg = agg.set_index(["date", "underlying"])
    agg["moc_buy_dollars"] = buys
    agg["moc_sell_dollars"] = sells
    agg = agg.fillna({"moc_buy_dollars": 0.0, "moc_sell_dollars": 0.0}).reset_index()
    agg["net_moc_pct_letf_aum"] = agg["net_moc_dollars"] / agg["total_letf_aum_prior_close"]

    agg = agg.sort_values(["underlying", "date"]).copy()
    for window in (5, 20, 60):
        agg[f"net_moc_{window}d_dollars"] = (
            agg.groupby("underlying")["net_moc_dollars"]
            .rolling(window, min_periods=1)
            .sum()
            .reset_index(level=0, drop=True)
        )

    roll_mean = (
        agg.groupby("underlying")["net_moc_dollars"]
        .rolling(60, min_periods=20)
        .mean()
        .reset_index(level=0, drop=True)
    )
    roll_std = (
        agg.groupby("underlying")["net_moc_dollars"]
        .rolling(60, min_periods=20)
        .std(ddof=0)
        .reset_index(level=0, drop=True)
    )
    agg["net_moc_z_60d"] = (agg["net_moc_dollars"] - roll_mean) / roll_std.replace(0.0, np.nan)
    return agg


def _top_contributors(fund_flows: pd.DataFrame, date_iso: str, underlying: str, *, n: int = 5) -> list[dict[str, Any]]:
    rows = fund_flows[
        (fund_flows["date"].eq(date_iso))
        & (fund_flows["underlying"].eq(underlying))
        & (fund_flows["included_in_aggregate"].astype(bool))
    ].copy()
    if rows.empty:
        return []
    rows["_abs"] = rows["rebalance_signed_dollars"].abs()
    rows = rows.sort_values("_abs", ascending=False).head(n)
    return [
        {
            "ticker": r["ticker"],
            "leverage": _round(r.get("leverage"), 4),
            "rebalance_signed_dollars": _round(r.get("rebalance_signed_dollars"), 2),
            "aum_prior_close": _round(r.get("aum_prior_close"), 2),
        }
        for _, r in rows.iterrows()
    ]


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
    if pd.isna(v):
        return None
    return v


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(_json_clean(payload), f, separators=(",", ":"), allow_nan=False, sort_keys=True)
    tmp.replace(path)


def write_outputs(
    fund_flows: pd.DataFrame,
    aggregates: pd.DataFrame,
    *,
    daily_parquet: Path = DAILY_PARQUET,
    daily_json: Path = DAILY_JSON,
    latest_json: Path = LATEST_JSON,
    json_days: int = 20,
) -> None:
    daily_parquet.parent.mkdir(parents=True, exist_ok=True)
    fund_flows.to_parquet(daily_parquet, index=False)

    if fund_flows.empty:
        daily_payload = {"build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"), "rows": []}
        latest_payload = {"build_time": daily_payload["build_time"], "latest_date": None, "by_underlying": {}}
    else:
        dates = sorted(fund_flows["date"].dropna().unique())
        keep_dates = set(dates[-json_days:])
        daily_payload = {
            "build_time": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "rows": fund_flows[fund_flows["date"].isin(keep_dates)].to_dict(orient="records"),
        }
        if aggregates.empty:
            latest_payload = {"build_time": daily_payload["build_time"], "latest_date": dates[-1], "by_underlying": {}}
        else:
            latest_date = str(aggregates["date"].max())
            latest_rows = aggregates[aggregates["date"].eq(latest_date)].sort_values("net_moc_dollars", key=lambda s: s.abs(), ascending=False)
            by_underlying: dict[str, Any] = {}
            for _, row in latest_rows.iterrows():
                und = str(row["underlying"])
                by_underlying[und] = {
                    "date": latest_date,
                    "underlying": und,
                    "net_moc_dollars": _round(row.get("net_moc_dollars"), 2),
                    "gross_moc_dollars": _round(row.get("gross_moc_dollars"), 2),
                    "moc_buy_dollars": _round(row.get("moc_buy_dollars"), 2),
                    "moc_sell_dollars": _round(row.get("moc_sell_dollars"), 2),
                    "total_letf_aum_prior_close": _round(row.get("total_letf_aum_prior_close"), 2),
                    "net_moc_pct_letf_aum": _round(row.get("net_moc_pct_letf_aum"), 8),
                    "underlying_return_d1": _round(row.get("underlying_return_d1"), 8),
                    "n_funds": int(row.get("n_funds") or 0),
                    "net_moc_5d_dollars": _round(row.get("net_moc_5d_dollars"), 2),
                    "net_moc_20d_dollars": _round(row.get("net_moc_20d_dollars"), 2),
                    "net_moc_60d_dollars": _round(row.get("net_moc_60d_dollars"), 2),
                    "net_moc_z_60d": _round(row.get("net_moc_z_60d"), 6),
                    "top_contributors": _top_contributors(fund_flows, latest_date, und),
                }
            latest_payload = {
                "build_time": daily_payload["build_time"],
                "latest_date": latest_date,
                "method": "L*(L-1)*prior_close_aum*underlying_return",
                "by_underlying": by_underlying,
            }

    _write_json(daily_json, daily_payload)
    _write_json(latest_json, latest_payload)


def build_all(
    *,
    universe_path: Path = UNIVERSE_CSV,
    metrics_parquet: Path = METRICS_PARQUET,
    metrics_csv: Path = METRICS_CSV,
    stale_bdays: int = 3,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    universe = load_universe(universe_path)
    metrics = load_metrics(metrics_parquet, metrics_csv)
    fund_flows = build_fund_flows(universe, metrics, stale_bdays=stale_bdays)
    aggregates = build_underlying_aggregates(fund_flows)
    return fund_flows, aggregates


def main() -> int:
    parser = argparse.ArgumentParser(description="Build LETF close rebalance-flow artifacts")
    parser.add_argument("--universe", type=Path, default=UNIVERSE_CSV)
    parser.add_argument("--metrics-parquet", type=Path, default=METRICS_PARQUET)
    parser.add_argument("--metrics-csv", type=Path, default=METRICS_CSV)
    parser.add_argument("--daily-parquet", type=Path, default=DAILY_PARQUET)
    parser.add_argument("--daily-json", type=Path, default=DAILY_JSON)
    parser.add_argument("--latest-json", type=Path, default=LATEST_JSON)
    parser.add_argument("--json-days", type=int, default=20)
    parser.add_argument("--stale-bdays", type=int, default=3)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, str(args.log_level).upper(), logging.INFO), format="%(levelname)s:%(name)s:%(message)s")
    fund_flows, aggregates = build_all(
        universe_path=args.universe,
        metrics_parquet=args.metrics_parquet,
        metrics_csv=args.metrics_csv,
        stale_bdays=args.stale_bdays,
    )
    write_outputs(
        fund_flows,
        aggregates,
        daily_parquet=args.daily_parquet,
        daily_json=args.daily_json,
        latest_json=args.latest_json,
        json_days=args.json_days,
    )
    LOGGER.info(
        "wrote LETF rebalance flows: fund_rows=%d aggregate_rows=%d latest_underlyings=%d",
        len(fund_flows),
        len(aggregates),
        0 if aggregates.empty else int((aggregates["date"] == aggregates["date"].max()).sum()),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

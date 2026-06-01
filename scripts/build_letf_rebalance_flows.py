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
UNDERLYING_VOLUME_PARQUET = DATA_DIR / "underlying_volume_history.parquet"

# Pulled from yfinance once per build with a graceful fallback. We cache the panel so
# a transient outage in CI keeps the previous trading day's ADV in place.
_VOLUME_CACHE_FRESH_HOURS = 18
_ADV_WINDOW_DAYS = 20
_ADV_LOOKBACK_DAYS = 35
_ADV_BATCH_SIZE = 50
_ADV_MIN_PERIODS = 5

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
    if "source_provider" in df.columns:
        df["source_provider_prior_close"] = by_ticker["source_provider"].shift(1)
    else:
        df["source_provider_prior_close"] = None
    df["underlying_return_d1"] = (df["underlying_adj_close"] / df["underlying_adj_close_prior"]) - 1.0

    def _prior_aum_blocks_flow(row: pd.Series) -> bool:
        stale_age = _f(row.get("stale_age_bdays_prior_close"))
        if stale_age is not None and stale_age > stale_bdays:
            return True
        if not bool(row.get("stale_prior_close")):
            return False
        src = str(row.get("source_provider_prior_close") or "").strip().lower()
        if src == "carry_forward":
            return True
        if stale_age is not None and stale_age <= 0:
            return False
        if stale_age is not None and stale_age > 0:
            return True
        return True

    def quality(row: pd.Series) -> str:
        if not bool(row.get("included_in_universe")):
            return str(row.get("universe_exclusion_reason") or "excluded")
        if not math.isfinite(float(row.get("leverage", np.nan))):
            return "missing_leverage"
        if not math.isfinite(float(row.get("aum_prior_close", np.nan))) or float(row.get("aum_prior_close")) <= 0:
            return "missing_prior_aum"
        if not math.isfinite(float(row.get("underlying_return_d1", np.nan))):
            return "missing_underlying_return"
        if _prior_aum_blocks_flow(row):
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


_YF_OHLCV_FIELDS = {"open", "high", "low", "close", "adj close", "volume"}


def _yf_column_name(col: object) -> str:
    return str(col).strip().lower()


def _yf_find_column(columns: pd.Index, *candidates: str) -> str | None:
    wanted = {_yf_column_name(c) for c in candidates}
    for col in columns:
        if _yf_column_name(col) in wanted:
            return str(col)
    return None


def _yf_extract_ticker_subframe(raw: pd.DataFrame, sym: str) -> pd.DataFrame | None:
    """Return one ticker's OHLCV block from a yfinance download frame."""
    up = str(sym).strip().upper()
    if not up:
        return None
    if not isinstance(raw.columns, pd.MultiIndex):
        return raw if not raw.empty else None

    lv0 = raw.columns.get_level_values(0)
    lv1 = raw.columns.get_level_values(1) if raw.columns.nlevels > 1 else pd.Index([])
    lv0_by_upper = {str(x).upper(): x for x in lv0}
    lv1_by_upper = {str(x).upper(): x for x in lv1}
    price_in_l0 = any(_yf_column_name(x) in _YF_OHLCV_FIELDS for x in lv0)

    try:
        if price_in_l0 and up in lv1_by_upper:
            return raw.xs(lv1_by_upper[up], axis=1, level=1)
        if up in lv0_by_upper:
            return raw[lv0_by_upper[up]]
        if sym in lv0:
            return raw[sym]
    except (KeyError, TypeError, ValueError):
        return None
    return None


def _extract_yf_close_volume_long(raw: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    """Pull daily close + volume per ticker from yfinance download into long-form rows."""
    cols = ["date", "underlying", "close", "volume"]
    if raw is None or raw.empty:
        return pd.DataFrame(columns=cols)
    records: list[dict] = []

    def _append_symbol_rows(up: str, sub: pd.DataFrame) -> None:
        if sub is None or sub.empty:
            return
        frame = sub.copy()
        if isinstance(frame.columns, pd.MultiIndex):
            frame.columns = frame.columns.get_level_values(0)
        close_key = _yf_find_column(frame.columns, "close")
        vol_key = _yf_find_column(frame.columns, "volume")
        if close_key is None or vol_key is None:
            return
        for idx, row in frame[[close_key, vol_key]].iterrows():
            try:
                close = float(row[close_key])
                volume = float(row[vol_key])
            except (TypeError, ValueError):
                continue
            if not (math.isfinite(close) and math.isfinite(volume) and close > 0 and volume > 0):
                continue
            d = idx.date() if hasattr(idx, "date") else idx
            records.append({"date": d, "underlying": up, "close": close, "volume": volume})

    if isinstance(raw.columns, pd.MultiIndex):
        for sym in tickers:
            up = str(sym).strip().upper()
            sub = _yf_extract_ticker_subframe(raw, sym)
            _append_symbol_rows(up, sub if sub is not None else pd.DataFrame())
    elif len(tickers) == 1:
        up = str(tickers[0]).strip().upper()
        _append_symbol_rows(up, raw)

    if not records:
        return pd.DataFrame(columns=cols)
    out = pd.DataFrame.from_records(records)
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    return out.dropna(subset=["date", "underlying"]).drop_duplicates(
        subset=["underlying", "date"], keep="last",
    )


def fetch_underlying_volume_panel(
    underlyings: list[str],
    *,
    lookback_days: int = _ADV_LOOKBACK_DAYS,
    batch_size: int = _ADV_BATCH_SIZE,
) -> pd.DataFrame:
    """Best-effort yfinance batch pull of daily close + volume for each underlying.

    Returns a DataFrame with ``date`` (ISO YYYY-MM-DD), ``underlying``, ``close``,
    ``volume``, ``dollar_volume``. Empty on any catastrophic failure -- the
    rest of the pipeline degrades gracefully (``%ADV`` becomes ``NaN``).
    """
    syms = sorted({str(u).strip().upper() for u in (underlyings or []) if str(u).strip()})
    if not syms:
        return pd.DataFrame(columns=["date", "underlying", "close", "volume", "dollar_volume"])
    try:
        import yfinance as yf  # type: ignore
    except Exception as exc:  # pragma: no cover - import-time failure path
        LOGGER.warning("yfinance unavailable for ADV pull (%s)", exc)
        return pd.DataFrame(columns=["date", "underlying", "close", "volume", "dollar_volume"])

    period = f"{int(lookback_days)}d"
    rows: list[pd.DataFrame] = []
    for start in range(0, len(syms), batch_size):
        chunk = syms[start:start + batch_size]
        try:
            data = yf.download(
                chunk,
                period=period,
                interval="1d",
                group_by="ticker",
                auto_adjust=False,
                progress=False,
                threads=False,
            )
        except Exception as exc:
            LOGGER.warning("yfinance ADV batch failed for %d symbols: %s", len(chunk), exc)
            continue
        part = _extract_yf_close_volume_long(data, chunk)
        if not part.empty:
            rows.append(part)

    if not rows:
        return pd.DataFrame(columns=["date", "underlying", "close", "volume", "dollar_volume"])
    out = pd.concat(rows, ignore_index=True)
    out["close"] = pd.to_numeric(out["close"], errors="coerce")
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce")
    out["dollar_volume"] = (out["close"] * out["volume"]).clip(lower=0)
    out = out.dropna(subset=["close", "volume", "dollar_volume"])
    out = out[(out["close"] > 0) & (out["volume"] > 0)]
    out = out.drop_duplicates(subset=["underlying", "date"], keep="last")
    return out.sort_values(["underlying", "date"]).reset_index(drop=True)


def load_or_refresh_underlying_volume_panel(
    underlyings: list[str],
    *,
    cache_path: Path = UNDERLYING_VOLUME_PARQUET,
    lookback_days: int = _ADV_LOOKBACK_DAYS,
    fresh_hours: float = _VOLUME_CACHE_FRESH_HOURS,
    skip_fetch: bool = False,
) -> pd.DataFrame:
    """Read cached underlying volume; refresh from yfinance when stale or missing."""
    cached = pd.DataFrame()
    if cache_path.exists():
        try:
            cached = pd.read_parquet(cache_path)
        except Exception as exc:  # pragma: no cover - cache reload failure
            LOGGER.warning("failed to load %s (%s); will refresh", cache_path, exc)
            cached = pd.DataFrame()

    cache_is_fresh = False
    if not cached.empty and {"date", "underlying", "dollar_volume"}.issubset(cached.columns):
        try:
            cache_max = pd.to_datetime(cached["date"]).max()
        except Exception:
            cache_max = pd.NaT
        if pd.notna(cache_max):
            now = pd.Timestamp.now("UTC").tz_localize(None)
            cache_is_fresh = (now - cache_max).total_seconds() / 3600.0 <= float(fresh_hours)

    if skip_fetch and not cached.empty:
        LOGGER.info("ADV cache reuse forced via --skip-volume-fetch (rows=%d)", len(cached))
        return cached
    if cache_is_fresh:
        LOGGER.info("ADV cache fresh (rows=%d, max_date=%s)", len(cached), cache_max)
        return cached

    fresh = fetch_underlying_volume_panel(underlyings, lookback_days=lookback_days)
    if fresh.empty:
        if not cached.empty:
            LOGGER.warning("ADV pull empty; falling back to cached panel (rows=%d)", len(cached))
            return cached
        return fresh

    if not cached.empty and {"date", "underlying"}.issubset(cached.columns):
        merged = pd.concat([cached, fresh], ignore_index=True)
        merged = merged.drop_duplicates(subset=["underlying", "date"], keep="last")
        merged = merged.sort_values(["underlying", "date"]).reset_index(drop=True)
    else:
        merged = fresh

    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        merged.to_parquet(cache_path, index=False)
    except Exception as exc:  # pragma: no cover - disk write failure
        LOGGER.warning("failed to persist ADV cache to %s (%s)", cache_path, exc)
    return merged


def compute_adv_panel(volume_panel: pd.DataFrame, *, window: int = _ADV_WINDOW_DAYS) -> pd.DataFrame:
    """Trailing-window mean dollar volume per (date, underlying)."""
    full = compute_adv_panel_with_median(volume_panel, window=window)
    if full.empty:
        return pd.DataFrame(columns=["date", "underlying", "underlying_dollar_adv_20d"])
    return full[["date", "underlying", "underlying_dollar_adv_20d"]]


def compute_adv_panel_with_median(
    volume_panel: pd.DataFrame, *, window: int = _ADV_WINDOW_DAYS,
) -> pd.DataFrame:
    """Trailing mean + median dollar volume per (date, underlying)."""
    cols = [
        "date", "underlying",
        "underlying_dollar_adv_20d", "underlying_dollar_median_adv_20d",
    ]
    if volume_panel.empty:
        return pd.DataFrame(columns=cols)
    panel = volume_panel.sort_values(["underlying", "date"]).copy()
    min_p = min(_ADV_MIN_PERIODS, window)
    grouped = panel.groupby("underlying")["dollar_volume"]
    panel["underlying_dollar_adv_20d"] = (
        grouped.rolling(window=window, min_periods=min_p).mean().reset_index(level=0, drop=True)
    )
    panel["underlying_dollar_median_adv_20d"] = (
        grouped.rolling(window=window, min_periods=min_p).median().reset_index(level=0, drop=True)
    )
    return panel[cols]


def annotate_with_adv(
    fund_flows: pd.DataFrame,
    aggregates: pd.DataFrame,
    adv_panel: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Add ``underlying_dollar_adv_20d`` and ``%ADV`` ratios to both frames."""
    if adv_panel.empty:
        if not fund_flows.empty:
            fund_flows = fund_flows.copy()
            fund_flows["underlying_dollar_adv_20d"] = float("nan")
            fund_flows["rebalance_pct_adv_20d"] = float("nan")
        if not aggregates.empty:
            aggregates = aggregates.copy()
            aggregates["underlying_dollar_adv_20d"] = float("nan")
            aggregates["net_moc_pct_adv_20d"] = float("nan")
        return fund_flows, aggregates

    if not fund_flows.empty:
        fund_flows = fund_flows.merge(adv_panel, on=["date", "underlying"], how="left")
        with np.errstate(divide="ignore", invalid="ignore"):
            fund_flows["rebalance_pct_adv_20d"] = (
                fund_flows["rebalance_signed_dollars"] / fund_flows["underlying_dollar_adv_20d"]
            )
    if not aggregates.empty:
        aggregates = aggregates.merge(adv_panel, on=["date", "underlying"], how="left")
        with np.errstate(divide="ignore", invalid="ignore"):
            aggregates["net_moc_pct_adv_20d"] = (
                aggregates["net_moc_dollars"] / aggregates["underlying_dollar_adv_20d"]
            )
    return fund_flows, aggregates


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
            "rebalance_pct_adv_20d": _round(r.get("rebalance_pct_adv_20d"), 8),
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
            global_latest_date = str(aggregates["date"].max())
            # Per-underlying latest aggregate row -- different underlyings have different
            # publish cadences (issuer feed lag, weekend gaps), so a single global filter
            # would silently drop hundreds of underlyings. See AGENTS notes for context.
            idx_per_und = aggregates.groupby("underlying")["date"].idxmax()
            latest_rows = (
                aggregates.loc[idx_per_und]
                .sort_values("net_moc_dollars", key=lambda s: s.abs(), ascending=False)
            )
            by_underlying: dict[str, Any] = {}
            for _, row in latest_rows.iterrows():
                und = str(row["underlying"])
                row_date = str(row.get("date") or "")
                by_underlying[und] = {
                    "date": row_date,
                    "is_latest_global": row_date == global_latest_date,
                    "underlying": und,
                    "net_moc_dollars": _round(row.get("net_moc_dollars"), 2),
                    "gross_moc_dollars": _round(row.get("gross_moc_dollars"), 2),
                    "moc_buy_dollars": _round(row.get("moc_buy_dollars"), 2),
                    "moc_sell_dollars": _round(row.get("moc_sell_dollars"), 2),
                    "total_letf_aum_prior_close": _round(row.get("total_letf_aum_prior_close"), 2),
                    "net_moc_pct_letf_aum": _round(row.get("net_moc_pct_letf_aum"), 8),
                    "underlying_dollar_adv_20d": _round(row.get("underlying_dollar_adv_20d"), 2),
                    "net_moc_pct_adv_20d": _round(row.get("net_moc_pct_adv_20d"), 8),
                    "underlying_return_d1": _round(row.get("underlying_return_d1"), 8),
                    "n_funds": int(row.get("n_funds") or 0),
                    "net_moc_5d_dollars": _round(row.get("net_moc_5d_dollars"), 2),
                    "net_moc_20d_dollars": _round(row.get("net_moc_20d_dollars"), 2),
                    "net_moc_60d_dollars": _round(row.get("net_moc_60d_dollars"), 2),
                    "net_moc_z_60d": _round(row.get("net_moc_z_60d"), 6),
                    "top_contributors": _top_contributors(fund_flows, row_date, und),
                }
            latest_payload = {
                "build_time": daily_payload["build_time"],
                "latest_date": global_latest_date,
                "method": "L*(L-1)*prior_close_aum*underlying_return",
                "adv_window_days": _ADV_WINDOW_DAYS,
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
    volume_cache_path: Path = UNDERLYING_VOLUME_PARQUET,
    skip_volume_fetch: bool = False,
    adv_window: int = _ADV_WINDOW_DAYS,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    universe = load_universe(universe_path)
    metrics = load_metrics(metrics_parquet, metrics_csv)
    fund_flows = build_fund_flows(universe, metrics, stale_bdays=stale_bdays)
    aggregates = build_underlying_aggregates(fund_flows)

    underlyings = sorted({u for u in universe["underlying"].dropna().tolist() if u})
    volume_panel = load_or_refresh_underlying_volume_panel(
        underlyings,
        cache_path=volume_cache_path,
        skip_fetch=skip_volume_fetch,
    )
    adv_panel = compute_adv_panel(volume_panel, window=adv_window)
    fund_flows, aggregates = annotate_with_adv(fund_flows, aggregates, adv_panel)
    return fund_flows, aggregates


def main() -> int:
    parser = argparse.ArgumentParser(description="Build LETF close rebalance-flow artifacts")
    parser.add_argument("--universe", type=Path, default=UNIVERSE_CSV)
    parser.add_argument("--metrics-parquet", type=Path, default=METRICS_PARQUET)
    parser.add_argument("--metrics-csv", type=Path, default=METRICS_CSV)
    parser.add_argument("--daily-parquet", type=Path, default=DAILY_PARQUET)
    parser.add_argument("--daily-json", type=Path, default=DAILY_JSON)
    parser.add_argument("--latest-json", type=Path, default=LATEST_JSON)
    parser.add_argument("--volume-cache", type=Path, default=UNDERLYING_VOLUME_PARQUET)
    parser.add_argument(
        "--skip-volume-fetch",
        action="store_true",
        help="Reuse data/underlying_volume_history.parquet without calling yfinance (offline / unit tests).",
    )
    parser.add_argument("--adv-window", type=int, default=_ADV_WINDOW_DAYS)
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
        volume_cache_path=args.volume_cache,
        skip_volume_fetch=bool(args.skip_volume_fetch),
        adv_window=int(args.adv_window),
    )
    write_outputs(
        fund_flows,
        aggregates,
        daily_parquet=args.daily_parquet,
        daily_json=args.daily_json,
        latest_json=args.latest_json,
        json_days=args.json_days,
    )
    underlyings_in_latest = (
        0
        if aggregates.empty
        else int(aggregates.groupby("underlying")["date"].max().shape[0])
    )
    LOGGER.info(
        "wrote LETF rebalance flows: fund_rows=%d aggregate_rows=%d latest_underlyings=%d",
        len(fund_flows),
        len(aggregates),
        underlyings_in_latest,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

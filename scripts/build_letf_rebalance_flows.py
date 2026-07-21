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
import os
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from etf_providers import prior_stale_aum_blocks_flow
from ingest_etf_metrics import ensure_stale_kind_column, extend_metrics_session_coverage
from market_calendar import nyse_busday_count

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

# ── Additional-stat assumptions (overridable via env for experiments) ────────
# Closing single-price auction (MOC/LOC) is typically a single-digit % of the
# day's total volume. Mechanical LETF hedges concentrate into that auction, so
# "% of auction" is a far more honest impact denominator than full-day ADV.
_AUCTION_SHARE_OF_ADV = float(os.environ.get("LETF_AUCTION_SHARE_OF_ADV", "0.08") or 0.08)
# Most issuer hedging is done via total-return swaps with dealers, not on the
# lit tape. ``physical`` = the slice that plausibly prints in the underlying.
_SWAP_HEDGE_SHARE = float(os.environ.get("LETF_SWAP_HEDGE_SHARE", "0.85") or 0.85)

# ── Float sanity-gate thresholds ─────────────────────────────────────────────
# yfinance ``floatShares`` is frequently broken (e.g. BRK-B returns ~1.16M vs a
# real ~1.4B float). Reject a float-share value that is implausibly small vs
# shares outstanding, or larger than shares outstanding.
_FLOAT_MIN_FRACTION_OF_SHARES = 0.05
# A name cannot routinely trade more than its entire float in a day; if 20d $ADV
# exceeds the float MV the float input is unreliable (stale split, ETF, etc.).
_FLOAT_RELIABLE_MIN_ADV_COVER = 1.0
# Minutes after the cash close before we treat the session's underlying close as
# final (issuer/print settling buffer).
_SESSION_FINAL_BUFFER_MIN = 10

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


def _first_positive(*vals: object) -> float | None:
    for v in vals:
        f = _f(v)
        if f is not None and f > 0:
            return f
    return None


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


def _market_close_utc(d: date, *, now: datetime | None = None) -> datetime:
    """~16:00 ET for date ``d`` expressed in UTC (approximate US DST window)."""
    march_dst_start = date(d.year, 3, 9)
    november_dst_end = date(d.year, 11, 2)
    is_edt = march_dst_start <= d <= november_dst_end
    close_utc_hour = 20 if is_edt else 21
    return datetime(d.year, d.month, d.day, close_utc_hour, 0, 0, tzinfo=UTC)


def session_state_for_date(date_iso: str, *, now: datetime | None = None) -> str:
    """Classify a session as ``final`` (close settled) or ``forming`` (intraday).

    ``final`` once we are at least ``_SESSION_FINAL_BUFFER_MIN`` past the cash
    close for that date; ``forming`` otherwise. This is what lets the EOD view
    stop stamping a still-forming session as a realised "latest" aggregate.
    """
    now = now or datetime.now(UTC)
    try:
        d = datetime.strptime(str(date_iso), "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return "final"
    close_utc = _market_close_utc(d)
    return "final" if now >= close_utc.replace(tzinfo=UTC) + _td_min(_SESSION_FINAL_BUFFER_MIN) else "forming"


def _td_min(minutes: int):
    from datetime import timedelta
    return timedelta(minutes=int(minutes))


def resolve_float_quality(
    *,
    float_shares_raw: float | None,
    shares_out: float | None,
    chosen_shares: float | None,
    float_dollars: float | None,
    adv: float | None,
    is_etf: bool,
    source: str | None,
) -> dict[str, Any]:
    """Sanity-gate the underlying float used for ``% of float`` ratios.

    Returns a dict with the *resolved* ``shares`` / ``dollars`` / ``source`` plus
    a ``reliable`` flag and a ``quality`` reason. When ``reliable`` is False the
    caller should suppress the ``% of float`` ratio (it is misleading).
    """
    price = None
    if chosen_shares and float_dollars and float(chosen_shares) > 0:
        price = float(float_dollars) / float(chosen_shares)

    shares = _f(chosen_shares)
    dollars = _f(float_dollars)
    src = source or None

    # ETF underlyings have an elastic share count (AP creation/redemption), so a
    # fixed "float" is not a real constraint -- never present a % of float.
    if is_etf:
        return {"shares": shares, "dollars": dollars, "source": "etf_shares_outstanding_elastic",
                "reliable": False, "quality": "etf_elastic"}

    if shares is None or shares <= 0 or dollars is None or dollars <= 0:
        return {"shares": shares, "dollars": dollars, "source": src,
                "reliable": False, "quality": "missing"}

    # If yfinance floatShares was used but is implausible vs shares outstanding
    # (BRK-B 1.16M vs 1.4B), fall back to shares outstanding.
    fsr = _f(float_shares_raw)
    so = _f(shares_out)
    if str(src or "").startswith("yfinance_float_shares") and so and so > 0 and price:
        too_small = fsr is not None and fsr < so * _FLOAT_MIN_FRACTION_OF_SHARES
        too_big = fsr is not None and fsr > so * 1.01
        if too_small or too_big:
            shares = so
            dollars = price * so
            src = "yfinance_shares_outstanding_fallback"

    # A name can't routinely trade more than its whole float in a day.
    if adv and float(adv) > 0 and dollars < float(adv) * _FLOAT_RELIABLE_MIN_ADV_COVER:
        return {"shares": shares, "dollars": dollars, "source": src,
                "reliable": False, "quality": "adv_exceeds_float"}

    return {"shares": shares, "dollars": dollars, "source": src,
            "reliable": True, "quality": "ok"}


def _apply_float_quality(df: pd.DataFrame, *, pct_col: str) -> pd.DataFrame:
    """Resolve float quality row-wise; suppress ``pct_col`` when unreliable."""
    if df.empty:
        return df
    out = df.copy()
    for col, default in (
        ("underlying_tradable_float_dollars", np.nan),
        ("tradable_float_shares", np.nan),
        ("shares_outstanding_underlying", np.nan),
        ("float_shares_raw", np.nan),
        ("is_etf", False),
        ("tradable_float_source", None),
    ):
        if col not in out.columns:
            out[col] = default

    resolved = out.apply(
        lambda r: resolve_float_quality(
            float_shares_raw=r.get("float_shares_raw"),
            shares_out=r.get("shares_outstanding_underlying"),
            chosen_shares=r.get("tradable_float_shares"),
            float_dollars=r.get("underlying_tradable_float_dollars"),
            adv=r.get("underlying_dollar_adv_20d"),
            is_etf=bool(r.get("is_etf")),
            source=r.get("tradable_float_source"),
        ),
        axis=1,
    )
    out["tradable_float_shares"] = resolved.map(lambda d: d["shares"])
    out["underlying_tradable_float_dollars"] = resolved.map(lambda d: d["dollars"])
    out["tradable_float_source"] = resolved.map(lambda d: d["source"])
    out["tradable_float_reliable"] = resolved.map(lambda d: bool(d["reliable"]))
    out["tradable_float_quality"] = resolved.map(lambda d: d["quality"])
    if pct_col in out.columns:
        out[pct_col] = out[pct_col].where(out["tradable_float_reliable"], np.nan)
    return out


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
    out = ensure_stale_kind_column(out)
    return out.dropna(subset=["date", "ticker"]).sort_values(["ticker", "date"]).reset_index(drop=True)


def _derive_aum_from_identity(df: pd.DataFrame) -> pd.DataFrame:
    """Fill missing AUM from NAV × shares when both legs are present."""
    out = df.copy()
    nav = pd.to_numeric(out.get("nav"), errors="coerce")
    shares = pd.to_numeric(out.get("shares_outstanding"), errors="coerce")
    aum = pd.to_numeric(out.get("aum"), errors="coerce")
    implied = nav * shares
    use_implied = (~aum.notna() | (aum <= 0)) & implied.notna() & (implied > 0)
    if use_implied.any():
        out.loc[use_implied, "aum"] = implied[use_implied]
    return out


_PARTIAL_AUM_FILL_BDAYS = 8
_SESSION_EXTEND_BDAYS = 2
# Don't let a thin issuer heal (e.g. 62 Defiance rows on a new max date) become
# the session-extend target — that fabricates a global latest day where most
# funds only have market-backed/session-extend priors and trip the freshness gate.
_FLOW_SESSION_MIN_COVERAGE = float(os.environ.get("LETF_FLOW_SESSION_MIN_COVERAGE", "0.20") or 0.20)


def _choose_flow_session_date(metrics: pd.DataFrame) -> date | None:
    """Pick the metrics session used for ``extend_metrics_session_coverage``.

    Prefer the panel max date when it already covers a meaningful share of
    tickers; otherwise fall back to the densest recent session so a partial
    issuer heal cannot advance the flow "global latest" alone.
    """
    if metrics.empty or "date" not in metrics.columns or "ticker" not in metrics.columns:
        return None
    work = metrics.copy()
    work["date"] = pd.to_datetime(work["date"], errors="coerce")
    work = work.dropna(subset=["date"])
    if work.empty:
        return None
    work["ticker"] = work["ticker"].astype(str).str.upper()
    n_tickers = int(work["ticker"].nunique())
    if n_tickers <= 0:
        return None
    by_date = work.groupby(work["date"].dt.normalize())["ticker"].nunique().sort_index()
    if by_date.empty:
        return None
    max_ts = by_date.index.max()
    max_n = int(by_date.loc[max_ts])
    if (max_n / n_tickers) >= _FLOW_SESSION_MIN_COVERAGE:
        return pd.Timestamp(max_ts).date()
    # Densest among the last ~10 calendar sessions present in the panel.
    recent = by_date.tail(10)
    dense_ts = recent.idxmax()
    LOGGER.warning(
        "Flow session date: panel max %s covers only %d/%d tickers (<%.0f%%); "
        "using densest recent session %s (%d tickers) for extend",
        pd.Timestamp(max_ts).date(),
        max_n,
        n_tickers,
        100.0 * _FLOW_SESSION_MIN_COVERAGE,
        pd.Timestamp(dense_ts).date(),
        int(recent.loc[dense_ts]),
    )
    return pd.Timestamp(dense_ts).date()


def _apply_underlying_close_from_volume_panel(
    metrics: pd.DataFrame,
    universe: pd.DataFrame,
    volume_panel: pd.DataFrame,
) -> pd.DataFrame:
    """Patch underlying_adj_close on metrics rows using yfinance close panel."""
    if metrics.empty or volume_panel.empty or "underlying" not in universe.columns:
        return metrics
    out = metrics.copy()
    if "underlying_adj_close" not in out.columns:
        out["underlying_adj_close"] = np.nan
    und_by_ticker = (
        universe.dropna(subset=["ticker", "underlying"])
        .assign(ticker=lambda d: d["ticker"].astype(str).str.upper())
        .set_index("ticker")["underlying"]
        .astype(str)
        .str.upper()
        .to_dict()
    )
    panel = volume_panel.copy()
    panel["date"] = pd.to_datetime(panel["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    close_map = {
        (str(r["underlying"]).upper(), str(r["date"])): float(r["close"])
        for _, r in panel.iterrows()
        if pd.notna(r.get("close")) and float(r["close"]) > 0
    }
    out["date_str"] = pd.to_datetime(out["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    for idx, row in out.iterrows():
        sym = str(row.get("ticker") or "").upper()
        und = und_by_ticker.get(sym)
        d = row.get("date_str")
        if not und or not d:
            continue
        close = close_map.get((str(und).upper(), str(d)))
        if close is None:
            continue
        cur = pd.to_numeric(out.at[idx, "underlying_adj_close"], errors="coerce")
        if pd.isna(cur) or float(cur) <= 0:
            out.at[idx, "underlying_adj_close"] = close
    return out.drop(columns=["date_str"], errors="ignore")


def _busday_gap(start: object, end: object) -> int | None:
    try:
        return nyse_busday_count(start, end)
    except Exception:
        return None


def _fill_partial_aum_for_flow(df: pd.DataFrame, *, max_gap_bdays: int = _PARTIAL_AUM_FILL_BDAYS) -> pd.DataFrame:
    """Carry last ok AUM/shares forward across short partial gaps before prior-close shift.

    Issuer rows often publish NAV-only partials for T+0 while AUM/shares remain on the
    prior ok row. Without this, ``shift(1)`` yields ``missing_prior_aum`` on the next
    session even though usable fund size exists within a few business days.
    """
    if df.empty or max_gap_bdays <= 0:
        return df
    out = df.sort_values(["ticker", "date"]).copy()
    out["aum"] = pd.to_numeric(out.get("aum"), errors="coerce")
    out["shares_outstanding"] = pd.to_numeric(out.get("shares_outstanding"), errors="coerce")

    def _fill_group(g: pd.DataFrame) -> pd.DataFrame:
        last_ok: pd.Series | None = None
        rows: list[dict[str, object]] = []
        for _, row in g.iterrows():
            row = row.copy()
            aum = row.get("aum")
            shares = row.get("shares_outstanding")
            has_aum = pd.notna(aum) and float(aum) > 0
            if not has_aum and last_ok is not None:
                gap = _busday_gap(last_ok["date"], row["date"])
                if gap is not None and gap <= max_gap_bdays:
                    row["aum"] = last_ok["aum"]
                    if (not pd.notna(shares) or float(shares) <= 0) and pd.notna(last_ok.get("shares_outstanding")):
                        row["shares_outstanding"] = last_ok["shares_outstanding"]
            rows.append(row.to_dict())
            if pd.notna(row.get("aum")) and float(row["aum"]) > 0:
                last_ok = pd.Series(row)
        return pd.DataFrame(rows)

    pieces: list[pd.DataFrame] = []
    for _, g in out.groupby("ticker", sort=False):
        pieces.append(_fill_group(g))
    return pd.concat(pieces, ignore_index=True)


def _aggregate_stale_reason(
    *,
    session_lag_bdays: int,
    fund_rows_on_global: int,
    ok_funds_on_global: int,
) -> str:
    if session_lag_bdays <= 0:
        return "current_session"
    if fund_rows_on_global <= 0:
        if session_lag_bdays == 1:
            return "issuer_publish_lag"
        return "missing_metrics"
    if ok_funds_on_global <= 0:
        return "quality_excluded"
    if session_lag_bdays == 1:
        return "issuer_publish_lag"
    return "session_lag"


def _flow_stale_summary(
    by_underlying: dict[str, Any],
    *,
    global_latest_date: str,
    fund_flows: pd.DataFrame,
) -> dict[str, Any]:
    counts: dict[str, int] = {}
    for meta in by_underlying.values():
        if meta.get("is_latest_global") is not False:
            continue
        reason = str(meta.get("aggregate_stale_reason") or "unknown")
        counts[reason] = counts.get(reason, 0) + 1
    return {
        "global_latest_date": global_latest_date,
        "underlyings_stale": sum(counts.values()),
        "underlyings_stale_by_reason": counts,
        "underlyings_actionable_stale": int(
            counts.get("session_lag", 0)
            + counts.get("missing_metrics", 0)
            + counts.get("quality_excluded", 0)
        ),
    }


def _dates_as_yyyy_mm_dd(series: pd.Series) -> pd.Series:
    """Format date-like values as YYYY-MM-DD without requiring a datetime64 dtype.

    Metrics may arrive as ``datetime64``, ``Timestamp``, or plain ``datetime.date``
    (``extend_metrics_session_coverage`` / parquet object columns). Calling
    ``series.dt.strftime`` on object-dtype python dates raises AttributeError.
    """
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d")


def build_fund_flows(universe: pd.DataFrame, metrics: pd.DataFrame, *, stale_bdays: int = 3) -> pd.DataFrame:
    if universe.empty or metrics.empty:
        return pd.DataFrame()

    df = metrics.merge(universe, on="ticker", how="left")
    # Session-extend and some parquet paths leave python ``date`` objects; coerce
    # before sort/shift so downstream .dt access and busday gaps stay consistent.
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])
    df["underlying"] = df["underlying"].fillna("")
    df["leverage"] = pd.to_numeric(df["leverage"], errors="coerce")
    df["aum"] = pd.to_numeric(df.get("aum"), errors="coerce")
    df["underlying_adj_close"] = pd.to_numeric(df.get("underlying_adj_close"), errors="coerce")
    df = _derive_aum_from_identity(df)
    df = _fill_partial_aum_for_flow(df)
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
    if "stale_kind" in df.columns:
        df["stale_kind_prior_close"] = by_ticker["stale_kind"].shift(1)
    else:
        df["stale_kind_prior_close"] = None
    if "source_provider" in df.columns:
        df["source_provider_prior_close"] = by_ticker["source_provider"].shift(1)
    else:
        df["source_provider_prior_close"] = None
    df["underlying_return_d1"] = (df["underlying_adj_close"] / df["underlying_adj_close_prior"]) - 1.0

    implied_prior_aum = pd.to_numeric(df.get("nav_prior_close"), errors="coerce") * pd.to_numeric(
        df.get("shares_outstanding_prior_close"), errors="coerce"
    )
    aum_prior = pd.to_numeric(df["aum_prior_close"], errors="coerce")
    fill_prior = (aum_prior.isna() | (aum_prior <= 0)) & implied_prior_aum.notna() & (implied_prior_aum > 0)
    if fill_prior.any():
        df.loc[fill_prior, "aum_prior_close"] = implied_prior_aum[fill_prior]

    def _prior_aum_blocks_flow(row: pd.Series) -> bool:
        return prior_stale_aum_blocks_flow(
            stale_prior_close=bool(row.get("stale_prior_close")),
            stale_age_bdays_prior_close=_f(row.get("stale_age_bdays_prior_close")),
            stale_kind_prior_close=row.get("stale_kind_prior_close"),
            source_provider_prior_close=row.get("source_provider_prior_close"),
            stale_bdays=stale_bdays,
        )

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
        "stale_kind_prior_close",
    ]
    for col in cols:
        if col not in df.columns:
            df[col] = None
    out = df[cols].copy()
    out["date"] = _dates_as_yyyy_mm_dd(out["date"])
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
    float_meta = fetch_underlying_float_metadata(syms)
    if float_meta:
        meta = pd.DataFrame([
            {"underlying": k, **v}
            for k, v in float_meta.items()
        ])
        out = out.merge(meta, on="underlying", how="left")
    for col in ("tradable_float_shares", "shares_outstanding_underlying", "float_shares_raw"):
        if col not in out.columns:
            out[col] = np.nan
    if "tradable_float_source" not in out.columns:
        out["tradable_float_source"] = None
    if "quote_type" not in out.columns:
        out["quote_type"] = None
    if "is_etf" not in out.columns:
        out["is_etf"] = False
    out["is_etf"] = out["is_etf"].fillna(False).astype(bool)
    shares = pd.to_numeric(out.get("tradable_float_shares"), errors="coerce")
    out["tradable_float_dollars"] = out["close"].astype(float) * shares
    return out.sort_values(["underlying", "date"]).reset_index(drop=True)


def fetch_underlying_float_metadata(
    underlyings: list[str],
    *,
    max_symbols: int = 250,
) -> dict[str, dict[str, Any]]:
    """Best-effort public-float metadata from yfinance.

    ``floatShares`` is preferred. When unavailable, shares outstanding is the
    fallback so downstream capacity ratios remain usable but clearly labelled.
    """
    syms = sorted({str(u).strip().upper() for u in (underlyings or []) if str(u).strip()})
    if not syms or max_symbols <= 0:
        return {}
    try:
        import yfinance as yf  # type: ignore
    except Exception as exc:  # pragma: no cover - import-time failure path
        LOGGER.warning("yfinance unavailable for float metadata (%s)", exc)
        return {}

    out: dict[str, dict[str, Any]] = {}
    for sym in syms[: int(max_symbols)]:
        float_shares = None
        shares_out = None
        quote_type = None
        try:
            t = yf.Ticker(sym)
            try:
                info = t.get_info() or {}
            except Exception:
                info = getattr(t, "info", {}) or {}
            float_shares = _first_positive(info.get("floatShares"), info.get("float_shares"))
            shares_out = _first_positive(info.get("sharesOutstanding"), info.get("shares_outstanding"))
            quote_type = str(info.get("quoteType") or info.get("quote_type") or "").upper() or None
            if shares_out is None:
                try:
                    fast = t.fast_info
                    shares_out = _first_positive(getattr(fast, "shares", None), fast.get("shares") if hasattr(fast, "get") else None)
                except Exception:
                    shares_out = None
        except Exception:
            continue
        shares = float_shares or shares_out
        if shares is None or shares <= 0:
            continue
        is_etf = quote_type in {"ETF", "MUTUALFUND"}
        out[sym] = {
            "tradable_float_shares": shares,
            "float_shares_raw": float_shares,
            "shares_outstanding_underlying": shares_out,
            "tradable_float_source": "yfinance_float_shares" if float_shares else "yfinance_shares_outstanding",
            "quote_type": quote_type,
            "is_etf": bool(is_etf),
        }
    return out


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
        has_float_shape = {"tradable_float_shares", "tradable_float_dollars", "tradable_float_source"}.issubset(cached.columns)
        try:
            cache_max = pd.to_datetime(cached["date"]).max()
        except Exception:
            cache_max = pd.NaT
        if pd.notna(cache_max):
            now = pd.Timestamp.now("UTC").tz_localize(None)
            cache_is_fresh = has_float_shape and (now - cache_max).total_seconds() / 3600.0 <= float(fresh_hours)

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
        "tradable_float_shares", "shares_outstanding_underlying", "float_shares_raw",
        "tradable_float_dollars", "tradable_float_source", "is_etf",
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
    for col in ("tradable_float_shares", "shares_outstanding_underlying", "float_shares_raw", "tradable_float_dollars"):
        if col not in panel.columns:
            panel[col] = np.nan
        panel[col] = pd.to_numeric(panel[col], errors="coerce")
    if "tradable_float_source" not in panel.columns:
        panel["tradable_float_source"] = None
    if "is_etf" not in panel.columns:
        panel["is_etf"] = False
    panel["is_etf"] = panel["is_etf"].fillna(False).astype(bool)
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
            fund_flows["underlying_dollar_auction_est"] = float("nan")
            fund_flows["rebalance_pct_auction_volume"] = float("nan")
            fund_flows["underlying_tradable_float_dollars"] = float("nan")
            fund_flows["rebalance_pct_tradable_float"] = float("nan")
        if not aggregates.empty:
            aggregates = aggregates.copy()
            aggregates["underlying_dollar_adv_20d"] = float("nan")
            aggregates["net_moc_pct_adv_20d"] = float("nan")
            aggregates["underlying_tradable_float_dollars"] = float("nan")
            aggregates["net_moc_pct_tradable_float"] = float("nan")
        return fund_flows, aggregates

    if not fund_flows.empty:
        fund_flows = fund_flows.merge(adv_panel, on=["date", "underlying"], how="left")
        if "tradable_float_dollars" in fund_flows.columns:
            fund_flows = fund_flows.rename(columns={"tradable_float_dollars": "underlying_tradable_float_dollars"})
        elif "underlying_tradable_float_dollars" not in fund_flows.columns:
            fund_flows["underlying_tradable_float_dollars"] = float("nan")
        with np.errstate(divide="ignore", invalid="ignore"):
            fund_flows["rebalance_pct_adv_20d"] = (
                fund_flows["rebalance_signed_dollars"] / fund_flows["underlying_dollar_adv_20d"]
            )
            fund_flows["underlying_dollar_auction_est"] = (
                fund_flows["underlying_dollar_adv_20d"].astype(float) * _AUCTION_SHARE_OF_ADV
            )
            fund_flows["rebalance_pct_auction_volume"] = (
                fund_flows["rebalance_signed_dollars"] / fund_flows["underlying_dollar_auction_est"]
            )
            fund_flows["rebalance_pct_tradable_float"] = (
                fund_flows["rebalance_signed_dollars"] / fund_flows["underlying_tradable_float_dollars"]
            )
        fund_flows = _apply_float_quality(fund_flows, pct_col="rebalance_pct_tradable_float")
    if not aggregates.empty:
        aggregates = aggregates.merge(adv_panel, on=["date", "underlying"], how="left")
        if "tradable_float_dollars" in aggregates.columns:
            aggregates = aggregates.rename(columns={"tradable_float_dollars": "underlying_tradable_float_dollars"})
        elif "underlying_tradable_float_dollars" not in aggregates.columns:
            aggregates["underlying_tradable_float_dollars"] = float("nan")
        with np.errstate(divide="ignore", invalid="ignore"):
            aggregates["net_moc_pct_adv_20d"] = (
                aggregates["net_moc_dollars"] / aggregates["underlying_dollar_adv_20d"]
            )
            aggregates["net_moc_pct_tradable_float"] = (
                aggregates["net_moc_dollars"] / aggregates["underlying_tradable_float_dollars"]
            )
            # Additional impact lenses (see AGENTS §LETF rebalance flow).
            auction_dollars = aggregates["underlying_dollar_adv_20d"].astype(float) * _AUCTION_SHARE_OF_ADV
            aggregates["underlying_dollar_auction_est"] = auction_dollars
            aggregates["net_moc_pct_auction_volume"] = aggregates["net_moc_dollars"] / auction_dollars
            aggregates["net_moc_physical_dollars"] = aggregates["net_moc_dollars"] * (1.0 - _SWAP_HEDGE_SHARE)
            aggregates["net_moc_pct_adv_physical"] = (
                aggregates["net_moc_physical_dollars"] / aggregates["underlying_dollar_adv_20d"]
            )
        aggregates = _apply_float_quality(aggregates, pct_col="net_moc_pct_tradable_float")
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

    # Percentile rank of today's net flow within its trailing-60d history (0..1).
    # Answers "is today unusually large?" without assuming normality (z can be
    # misleading on the fat-tailed flow distribution).
    def _last_pctile(s: pd.Series) -> float:
        if s.empty:
            return float("nan")
        return float(s.rank(pct=True).iloc[-1])

    agg["net_moc_pctile_60d"] = (
        agg.groupby("underlying")["net_moc_dollars"]
        .rolling(60, min_periods=20)
        .apply(_last_pctile, raw=False)
        .reset_index(level=0, drop=True)
    )
    agg["abs_net_moc_pctile_60d"] = (
        agg.assign(_abs=agg["net_moc_dollars"].abs())
        .groupby("underlying")["_abs"]
        .rolling(60, min_periods=20)
        .apply(_last_pctile, raw=False)
        .reset_index(level=0, drop=True)
    )
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
            "rebalance_pct_auction_volume": _round(r.get("rebalance_pct_auction_volume"), 8),
            "rebalance_pct_tradable_float": _round(r.get("rebalance_pct_tradable_float"), 8),
            "tradable_float_reliable": (
                bool(r.get("tradable_float_reliable"))
                if r.get("tradable_float_reliable") is not None and not pd.isna(r.get("tradable_float_reliable"))
                else None
            ),
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


def _flow_quality_summary(fund_flows: pd.DataFrame, latest_date: str) -> dict[str, Any]:
    if fund_flows.empty:
        return {}
    day = fund_flows[fund_flows["date"].astype(str) == str(latest_date)]
    if day.empty:
        return {}
    quality_counts = day["quality_flag"].astype(str).value_counts().astype(int).to_dict()
    stale_aum = day[day["quality_flag"].astype(str) == "stale_aum"]
    by_kind: dict[str, int] = {}
    if not stale_aum.empty and "stale_kind_prior_close" in stale_aum.columns:
        by_kind = (
            stale_aum["stale_kind_prior_close"]
            .fillna("unknown")
            .astype(str)
            .str.strip()
            .str.lower()
            .replace({"": "unknown", "none": "unknown"})
            .value_counts()
            .astype(int)
            .to_dict()
        )
    return {
        "fund_rows_total": int(len(day)),
        "quality_counts": quality_counts,
        "stale_aum_by_prior_kind": by_kind,
        "included_in_aggregate": int(day["included_in_aggregate"].astype(bool).sum()),
    }


def _float_quality_summary(by_underlying: dict[str, Any]) -> dict[str, Any]:
    """Rollup of float-input reliability across the latest by-underlying view."""
    counts: dict[str, int] = {}
    reliable = 0
    total = 0
    for meta in by_underlying.values():
        total += 1
        q = str(meta.get("tradable_float_quality") or "unknown")
        counts[q] = counts.get(q, 0) + 1
        if meta.get("tradable_float_reliable"):
            reliable += 1
    return {
        "underlyings_total": total,
        "float_reliable": reliable,
        "float_unreliable": total - reliable,
        "by_quality": counts,
    }


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
            now = datetime.now(UTC)
            global_latest_date = str(aggregates["date"].max())
            global_session_state = session_state_for_date(global_latest_date, now=now)
            # Per-underlying latest aggregate row -- different underlyings have different
            # publish cadences (issuer feed lag, weekend gaps), so a single global filter
            # would silently drop hundreds of underlyings. See AGENTS notes for context.
            idx_per_und = aggregates.groupby("underlying")["date"].idxmax()
            latest_rows = (
                aggregates.loc[idx_per_und]
                .sort_values("net_moc_dollars", key=lambda s: s.abs(), ascending=False)
            )
            global_day = fund_flows[fund_flows["date"].astype(str) == str(global_latest_date)]
            by_underlying: dict[str, Any] = {}
            for _, row in latest_rows.iterrows():
                und = str(row["underlying"])
                row_date = str(row.get("date") or "")
                session_lag = _busday_gap(row_date, global_latest_date) or 0
                und_day = global_day[global_day["underlying"].astype(str) == und]
                ok_funds = int(und_day["included_in_aggregate"].astype(bool).sum()) if not und_day.empty else 0
                stale_reason = _aggregate_stale_reason(
                    session_lag_bdays=int(session_lag),
                    fund_rows_on_global=int(len(und_day)),
                    ok_funds_on_global=ok_funds,
                )
                # A lagging underlying's own (prior) session is already closed =>
                # complete; only the global-latest session can still be forming.
                row_session_state = (
                    "final" if row_date != global_latest_date else global_session_state
                )
                by_underlying[und] = {
                    "date": row_date,
                    "is_latest_global": row_date == global_latest_date,
                    "session_lag_bdays": int(session_lag),
                    "aggregate_stale_reason": stale_reason,
                    "session_state": row_session_state,
                    "data_complete": bool(row_session_state == "final"),
                    "underlying": und,
                    "net_moc_dollars": _round(row.get("net_moc_dollars"), 2),
                    "gross_moc_dollars": _round(row.get("gross_moc_dollars"), 2),
                    "moc_buy_dollars": _round(row.get("moc_buy_dollars"), 2),
                    "moc_sell_dollars": _round(row.get("moc_sell_dollars"), 2),
                    "total_letf_aum_prior_close": _round(row.get("total_letf_aum_prior_close"), 2),
                    "net_moc_pct_letf_aum": _round(row.get("net_moc_pct_letf_aum"), 8),
                    "underlying_dollar_adv_20d": _round(row.get("underlying_dollar_adv_20d"), 2),
                    "net_moc_pct_adv_20d": _round(row.get("net_moc_pct_adv_20d"), 8),
                    # Additional impact lenses.
                    "underlying_dollar_auction_est": _round(row.get("underlying_dollar_auction_est"), 2),
                    "net_moc_pct_auction_volume": _round(row.get("net_moc_pct_auction_volume"), 8),
                    "net_moc_physical_dollars": _round(row.get("net_moc_physical_dollars"), 2),
                    "net_moc_pct_adv_physical": _round(row.get("net_moc_pct_adv_physical"), 8),
                    "net_moc_pctile_60d": _round(row.get("net_moc_pctile_60d"), 4),
                    "abs_net_moc_pctile_60d": _round(row.get("abs_net_moc_pctile_60d"), 4),
                    "underlying_tradable_float_dollars": _round(row.get("underlying_tradable_float_dollars"), 2),
                    "underlying_tradable_float_shares": _round(row.get("tradable_float_shares"), 0),
                    "underlying_shares_outstanding": _round(row.get("shares_outstanding_underlying"), 0),
                    "tradable_float_source": row.get("tradable_float_source"),
                    "tradable_float_reliable": bool(row.get("tradable_float_reliable")) if row.get("tradable_float_reliable") is not None else None,
                    "tradable_float_quality": row.get("tradable_float_quality"),
                    "net_moc_pct_tradable_float": _round(row.get("net_moc_pct_tradable_float"), 8),
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
                # Honest "as of": the underlying close the realised flow is built
                # on, plus whether that session has settled.
                "as_of_underlying_close": global_latest_date,
                "session_state": global_session_state,
                "method": "L*(L-1)*prior_close_aum*underlying_return",
                "adv_window_days": _ADV_WINDOW_DAYS,
                "auction_share_of_adv_assumption": _AUCTION_SHARE_OF_ADV,
                "swap_hedge_share_assumption": _SWAP_HEDGE_SHARE,
                "flow_quality_on_latest_date": _flow_quality_summary(fund_flows, global_latest_date),
                "flow_stale_summary": _flow_stale_summary(
                    by_underlying,
                    global_latest_date=global_latest_date,
                    fund_flows=fund_flows,
                ),
                "float_quality_summary": _float_quality_summary(by_underlying),
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
    underlyings = sorted({u for u in universe["underlying"].dropna().tolist() if u})
    volume_panel = load_or_refresh_underlying_volume_panel(
        underlyings,
        cache_path=volume_cache_path,
        skip_fetch=skip_volume_fetch,
    )

    metrics = load_metrics(metrics_parquet, metrics_csv)
    session_date = _choose_flow_session_date(metrics)
    if session_date is not None:
        metrics = extend_metrics_session_coverage(
            metrics,
            session_date=session_date,
            tickers=sorted(universe["ticker"].astype(str).str.upper().unique()),
            max_lag_bdays=_SESSION_EXTEND_BDAYS,
        )
        metrics = _apply_underlying_close_from_volume_panel(metrics, universe, volume_panel)

    fund_flows = build_fund_flows(universe, metrics, stale_bdays=stale_bdays)
    aggregates = build_underlying_aggregates(fund_flows)

    # Exclude a still-forming session's partial volume from the trailing ADV so
    # %ADV / %float don't drift during the day. The session is "forming" until
    # ~10 min past the cash close (see session_state_for_date).
    adv_volume_panel = volume_panel
    if not volume_panel.empty and "date" in volume_panel.columns:
        latest_vol_date = str(pd.to_datetime(volume_panel["date"], errors="coerce").max().date())
        if session_state_for_date(latest_vol_date) == "forming":
            adv_volume_panel = volume_panel.copy()
            forming_mask = adv_volume_panel["date"].astype(str).eq(latest_vol_date)
            adv_volume_panel.loc[forming_mask, "dollar_volume"] = np.nan
            LOGGER.info("ADV excludes forming session %s (partial volume)", latest_vol_date)

    adv_panel = compute_adv_panel_with_median(adv_volume_panel, window=adv_window)
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

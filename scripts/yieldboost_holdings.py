"""Granite YieldBOOST holdings XLS parsing, put-spread pairing, and VRP panel."""
from __future__ import annotations

import io
import json
import logging
import math
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd

LOGGER = logging.getLogger("yieldboost_holdings")

GRANITE_BASE = "https://www.graniteshares.com"

_GRANITE_OPT_RE = re.compile(
    r"^(?P<root>\S+)\s+(?P<exp>\d{2}/\d{2}/\d{4})\s+(?P<pc>[PC])(?P<strike>[\d.]+)$",
    re.I,
)

_GRANITE_XLS_HREF_RE = re.compile(
    r'href="(/media/[^"]+holdings[^"]+\.xls)"',
    re.I,
)

# Issuer option roots that do not map cleanly via strip-leading-2.
OPTION_ROOT_TO_SLEEVE: dict[str, str] = {
    "2CWVX": "CRWV",
}

# Known 2x sleeve tickers (Granite / REX / Direxion) for validation.
KNOWN_SLEEVE_TICKERS: set[str] = {
    "MSTU", "MSTP", "TSLL", "TSLR", "NVDL", "CONL", "IONL", "MRAL", "AMDL",
    "CRWV", "BITX", "SMCL", "AMZZ", "BABX", "FBL", "PTIR", "MULL", "INTW",
    "HIMZ", "SPXL", "NUGT", "FAS", "LABU", "SOXL", "TECL", "TMF", "ROBN",
    "ETHU", "SMCX", "RIOX", "RGTX", "QBTX", "CRCA", "TQQQ",
}

DEFAULT_SCREENER_CSV = Path("data/etf_screened_today.csv")

# ~30 calendar-day realized vol window (matches dashboard `realized_vol["1M"]`).
RV_VRP_WINDOWS = ("1M", "30D", "30d", "3M")


@dataclass
class ParsedGraniteOption:
    root: str
    expiry: date
    put_call: str
    strike: float


@dataclass
class PutSpreadLeg:
    yb_etf: str
    underlying: str | None
    option_root: str
    sleeve_2x_etf: str
    expiry: date
    strike_long: float
    strike_short: float
    qty: float
    notional_long: float
    notional_short: float
    weight_net: float
    holdings_as_of: date
    is_front: bool = False
    moneyness_long_pct: float | None = None
    moneyness_short_pct: float | None = None
    days_to_exp: int | None = None


def _parse_float(val: object) -> float | None:
    if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))):
        return None
    txt = str(val).strip().replace(",", "").replace("$", "")
    if not txt or txt == "-":
        return None
    try:
        return float(txt)
    except ValueError:
        return None


def _parse_date(val: object) -> date | None:
    if val is None:
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()
    txt = str(val).strip()
    if not txt:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(txt, fmt).date()
        except ValueError:
            continue
    try:
        parsed = pd.to_datetime(txt, errors="coerce")
        if pd.isna(parsed):
            return None
        return parsed.date()
    except Exception:
        return None


def _parse_weight_pct(val: object) -> float | None:
    w = _parse_float(val)
    if w is None:
        return None
    if abs(w) <= 1.5:
        return w * 100.0
    return w


def parse_granite_option_description(desc: str) -> ParsedGraniteOption | None:
    text = str(desc or "").strip()
    m = _GRANITE_OPT_RE.match(text)
    if not m:
        return None
    expiry = _parse_date(m.group("exp"))
    strike = _parse_float(m.group("strike"))
    if expiry is None or strike is None:
        return None
    return ParsedGraniteOption(
        root=m.group("root").upper(),
        expiry=expiry,
        put_call=m.group("pc").upper(),
        strike=strike,
    )


def extract_rv_30d_annual(
    stats: dict | None,
    *,
    prefer: str = "etf",
) -> float | None:
    """
    Annualized ~30d realized vol from dashboard `realized_vol` or Yahoo window maps.

    Dashboard windows use `etf` / `underlying`; Yahoo builder windows use `vol_annual`.
    """
    if not stats or not isinstance(stats, dict):
        return None

    for key in ("rv_30d", "vol_annual"):
        v = _parse_float(stats.get(key))
        if v is not None and v > 0:
            return float(v)

    etf_keys = ("etf", "vol_annual", "etf_ewma", "etf_robust_ewma")
    und_keys = ("underlying", "vol_annual", "underlying_ewma", "underlying_robust_ewma")
    keys = etf_keys if prefer == "etf" else und_keys

    for window in RV_VRP_WINDOWS:
        win = stats.get(window)
        if not isinstance(win, dict):
            continue
        for key in keys:
            v = _parse_float(win.get(key))
            if v is not None and v > 0:
                return float(v)
    return None


def build_yieldboost_rv_map(
    *,
    realized_vol_by_symbol: dict[str, dict] | None = None,
    dashboard_records: list[dict] | None = None,
) -> dict[str, float]:
    """Symbol -> annualized ~30d RV for VRP panel (sleeves + underlyings)."""
    rv_map: dict[str, float] = {}

    for rec in dashboard_records or []:
        if not isinstance(rec, dict):
            continue
        sym = str(rec.get("symbol") or "").upper().strip()
        if not sym:
            continue
        v = extract_rv_30d_annual(rec.get("realized_vol"), prefer="etf")
        if v is not None:
            rv_map[sym] = v

    for sym, stats in (realized_vol_by_symbol or {}).items():
        ss = str(sym or "").upper().strip()
        if not ss or not isinstance(stats, dict):
            continue
        v = extract_rv_30d_annual(stats, prefer="etf")
        if v is not None:
            rv_map[ss] = v

    return rv_map


def recompute_put_spread_front_flags(
    spreads: list[PutSpreadLeg],
    *,
    as_of: date | None = None,
) -> list[PutSpreadLeg]:
    """Mark nearest (per YB ETF) future expiry as front; repairs stale spreads JSON."""
    if not spreads:
        return spreads
    today = as_of or date.today()
    by_etf: dict[str, list[PutSpreadLeg]] = {}
    for s in spreads:
        by_etf.setdefault(s.yb_etf, []).append(s)
    for legs in by_etf.values():
        future = [s for s in legs if s.expiry >= today]
        front_expiry = min((s.expiry for s in future), default=max(s.expiry for s in legs))
        for s in legs:
            s.is_front = s.expiry == front_expiry
    return spreads


def resolve_sleeve_ticker(
    option_root: str,
    underlying: str | None = None,
    *,
    yb_etf: str | None = None,
    sleeve_by_yb: dict[str, str] | None = None,
) -> str:
    """Map Granite option root (e.g. 2HIMZ) to the equity ticker for options quotes."""
    root = str(option_root or "").upper().strip()
    if root in OPTION_ROOT_TO_SLEEVE:
        return OPTION_ROOT_TO_SLEEVE[root]

    if root.startswith("2") and len(root) > 1:
        candidate = root[1:]
        if candidate in KNOWN_SLEEVE_TICKERS:
            return candidate

        yb = str(yb_etf or "").upper()
        mapped = (sleeve_by_yb or {}).get(yb)
        if mapped:
            mapped_u = str(mapped).upper()
            if mapped_u == candidate or root == f"2{mapped_u}":
                return mapped_u

        # Granite lists options as 2{SLEEVE}; quote the levered sleeve, not cash underlying.
        return candidate

    return root


def load_sleeve_by_yb_from_screener(
    csv_path: Path | str | None = None,
) -> dict[str, str]:
    """YieldBOOST income ETF -> 2x sleeve when the underlying has exactly one letf row."""
    path = Path(csv_path) if csv_path else DEFAULT_SCREENER_CSV
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path)
    except Exception:
        return {}
    needed = {"ETF", "Underlying", "product_class"}
    if not needed.issubset(df.columns):
        return {}

    df = df.copy()
    df["ETF"] = df["ETF"].astype(str).str.upper()
    df["Underlying"] = df["Underlying"].astype(str).str.upper()
    yb_df = df[df["product_class"] == "income_yieldboost"]
    letf_df = df[df["product_class"].isin(["letf", "letf_long"])]

    out: dict[str, str] = {}
    for _, row in yb_df.iterrows():
        yb = row["ETF"]
        und = row["Underlying"]
        matches = sorted(letf_df[letf_df["Underlying"] == und]["ETF"].unique().tolist())
        if len(matches) == 1:
            out[yb] = matches[0]
    return out


def apply_resolved_sleeves(
    spreads: Iterable[PutSpreadLeg],
    *,
    sleeve_by_yb: dict[str, str] | None = None,
) -> list[PutSpreadLeg]:
    """Re-resolve sleeve_2x_etf from option_root (repairs legacy spreads JSON)."""
    resolved: list[PutSpreadLeg] = []
    for s in spreads:
        sleeve = resolve_sleeve_ticker(
            s.option_root,
            s.underlying,
            yb_etf=s.yb_etf,
            sleeve_by_yb=sleeve_by_yb,
        )
        if sleeve == s.sleeve_2x_etf:
            resolved.append(s)
            continue
        d = asdict(s)
        d["sleeve_2x_etf"] = sleeve
        resolved.append(PutSpreadLeg(**d))
    return resolved


def format_occ_ticker(root: str, expiry: date, put_call: str, strike: float) -> str:
    """Standard OCC: ROOT + YYMMDD + P/C + strike times 1000, zero-padded to 8 digits."""
    yy = expiry.year % 100
    strike8 = int(round(float(strike) * 1000))
    return f"{root.upper()}{yy:02d}{expiry.month:02d}{expiry.day:02d}{put_call.upper()}{strike8:08d}"


def fetch_granite_holdings_xls(
    ticker: str,
    session,
    *,
    get_page=None,
) -> tuple[str, pd.DataFrame] | None:
    """Download Granite Fund Holdings XLS linked from the ETF detail page."""
    slug = ticker.strip().lower()
    page_url = f"{GRANITE_BASE}/etfs/{slug}/"
    _get = get_page
    if _get is None:
        from etf_providers import _get as _get  # noqa: WPS433

    pr = _get(session, page_url, extra_headers={"Referer": f"{GRANITE_BASE}/etfs/"})
    if pr.status_code != 200 or not pr.text:
        return None
    m = _GRANITE_XLS_HREF_RE.search(pr.text)
    if not m:
        return None
    xls_url = GRANITE_BASE + m.group(1)
    try:
        resp = session.get(xls_url, timeout=getattr(session, "timeout_sec", 30))
        if resp.status_code != 200 or not resp.content:
            return None
        df = pd.read_excel(io.BytesIO(resp.content))
        if df is None or df.empty:
            return None
        return xls_url, df
    except Exception as exc:
        LOGGER.debug("Granite XLS fetch failed for %s: %s", ticker, exc)
        return None


def granite_xls_rows_to_holdings(
    df: pd.DataFrame,
    *,
    etf_ticker: str,
    fallback_as_of: date,
    underlying: str | None,
    source_url: str,
) -> list[dict]:
    """Convert Granite XLS DataFrame to holding row dicts (HOLDINGS_COLUMNS + option fields)."""
    rows: list[dict] = []
    etf = etf_ticker.upper()
    und = (underlying or "").upper() or None

    for _, raw in df.iterrows():
        asset = str(raw.get("Asset Group") or "").strip().upper()
        desc = str(raw.get("Security Description") or raw.get("Ticker/Cusip") or "").strip()
        cusip = str(raw.get("Ticker/Cusip") or "").strip() or None
        shares = _parse_float(raw.get("Shares/Par"))
        mv = _parse_float(raw.get("Market/Notional Value"))
        weight = _parse_weight_pct(raw.get("Percentage Weighting"))
        row_date = _parse_date(raw.get("Position Date")) or fallback_as_of

        if asset == "O":
            parsed = parse_granite_option_description(desc)
            if parsed is None:
                continue
            sleeve = resolve_sleeve_ticker(parsed.root, und, yb_etf=etf)
            occ = format_occ_ticker(sleeve, parsed.expiry, parsed.put_call, parsed.strike)
            sec_type = "OPTION_PUT" if parsed.put_call == "P" else "OPTION_CALL"
            side = "long" if shares is not None and shares > 0 else "short"
            price = None
            if shares and mv is not None and abs(shares) > 0:
                price = abs(mv / (shares * 100.0))
            rows.append({
                "as_of_date": row_date,
                "etf_ticker": etf,
                "position_ticker": occ,
                "security_name": desc,
                "cusip": cusip,
                "security_type": sec_type,
                "shares": shares,
                "price": price,
                "market_value": mv,
                "weight_pct": weight,
                "source": "granite_shares",
                "source_url": source_url,
                "option_root": parsed.root,
                "option_expiry": parsed.expiry,
                "option_strike": parsed.strike,
                "option_put_call": parsed.put_call,
                "option_side": side,
            })
        elif asset in {"CU", "C"}:
            rows.append({
                "as_of_date": row_date,
                "etf_ticker": etf,
                "position_ticker": None,
                "security_name": desc or "US Dollars",
                "cusip": cusip if cusip and cusip != "USD" else None,
                "security_type": "CASH",
                "shares": shares,
                "price": 1.0 if shares else None,
                "market_value": mv,
                "weight_pct": weight,
                "source": "granite_shares",
                "source_url": source_url,
                "option_root": None,
                "option_expiry": None,
                "option_strike": None,
                "option_put_call": None,
                "option_side": None,
            })
        elif asset == "B":
            rows.append({
                "as_of_date": row_date,
                "etf_ticker": etf,
                "position_ticker": cusip,
                "security_name": desc,
                "cusip": cusip,
                "security_type": "TREASURY",
                "shares": shares,
                "price": (mv / shares) if shares and mv is not None and shares != 0 else None,
                "market_value": mv,
                "weight_pct": weight,
                "source": "granite_shares",
                "source_url": source_url,
                "option_root": None,
                "option_expiry": None,
                "option_strike": None,
                "option_put_call": None,
                "option_side": None,
            })
        else:
            pos_ticker = cusip if cusip and len(cusip) <= 8 and cusip.isalpha() else None
            rows.append({
                "as_of_date": row_date,
                "etf_ticker": etf,
                "position_ticker": pos_ticker,
                "security_name": desc,
                "cusip": cusip,
                "security_type": "OTHER",
                "shares": shares,
                "price": (mv / shares) if shares and mv is not None and shares != 0 else None,
                "market_value": mv,
                "weight_pct": weight,
                "source": "granite_shares",
                "source_url": source_url,
                "option_root": None,
                "option_expiry": None,
                "option_strike": None,
                "option_put_call": None,
                "option_side": None,
            })
    return rows


_HOLDINGS_COLUMN_ALIASES: dict[str, str] = {
    "ETF Ticker": "etf_ticker",
    "etf": "etf_ticker",
    "Ticker": "position_ticker",
    "Security Description": "security_name",
    "Shares/Par": "shares",
    "Market/Notional Value": "market_value",
    "Percentage Weighting": "weight_pct",
    "Position Date": "as_of_date",
    "Mat/Exp Date": "option_expiry",
}


_GRANITE_URL_ETF_RE = re.compile(r"/([a-z]{2,5})_holdings_file_\d+\.xls", re.I)


def infer_etf_ticker_from_source_url(source_url: object) -> str | None:
    m = _GRANITE_URL_ETF_RE.search(str(source_url or ""))
    return m.group(1).upper() if m else None


def normalize_holdings_dataframe(holdings_df: pd.DataFrame | None) -> pd.DataFrame:
    """Normalize holdings CSV/XLS columns for YieldBOOST spread pairing."""
    if holdings_df is None or holdings_df.empty:
        return pd.DataFrame()
    df = holdings_df.copy()
    for src, dst in _HOLDINGS_COLUMN_ALIASES.items():
        if src in df.columns and dst not in df.columns:
            df[dst] = df[src]
    if "etf_ticker" not in df.columns or df["etf_ticker"].isna().all():
        if "source_url" in df.columns:
            inferred = df["source_url"].map(infer_etf_ticker_from_source_url)
            if "etf_ticker" not in df.columns:
                df["etf_ticker"] = inferred
            else:
                df["etf_ticker"] = df["etf_ticker"].fillna(inferred)
    if "etf_ticker" not in df.columns:
        return pd.DataFrame()
    df["etf_ticker"] = df["etf_ticker"].astype(str).str.upper().str.strip()
    df = df[df["etf_ticker"].astype(str).str.len() > 0]
    if "option_expiry" in df.columns:
        df["option_expiry"] = pd.to_datetime(df["option_expiry"], errors="coerce").dt.date
    return df.reset_index(drop=True)


def load_holdings_latest_dataframe(
    *,
    csv_path: Path | None = None,
    parquet_path: Path | None = None,
) -> pd.DataFrame:
    """Load latest holdings snapshot; rebuild from parquet when CSV is legacy/missing etf_ticker."""
    csv_path = csv_path or Path("data/etf_holdings_latest.csv")
    parquet_path = parquet_path or Path("data/etf_holdings_daily.parquet")
    if csv_path.exists():
        df = normalize_holdings_dataframe(pd.read_csv(csv_path))
        if not df.empty:
            return df
    if not parquet_path.exists():
        return pd.DataFrame()
    hist = pd.read_parquet(parquet_path)
    if hist.empty or "etf_ticker" not in hist.columns:
        return pd.DataFrame()
    from etf_holdings_providers import HOLDINGS_COLUMNS

    for c in HOLDINGS_COLUMNS:
        if c not in hist.columns:
            hist[c] = None
    hist = hist[HOLDINGS_COLUMNS]
    hist["as_of_date"] = pd.to_datetime(hist["as_of_date"], errors="coerce").dt.date
    hist["etf_ticker"] = hist["etf_ticker"].astype(str).str.upper()
    latest = hist.sort_values(["etf_ticker", "as_of_date"])
    latest = latest.groupby("etf_ticker", group_keys=False).apply(
        lambda g: g[g["as_of_date"] == g["as_of_date"].max()],
    )
    return normalize_holdings_dataframe(latest.reset_index(drop=True))


def spreads_json_to_put_spread_legs(
    payload: dict[str, Any] | None,
    *,
    sleeve_by_yb: dict[str, str] | None = None,
) -> list[PutSpreadLeg]:
    """Rehydrate PutSpreadLeg objects from committed spreads JSON."""
    if not payload:
        return []
    if sleeve_by_yb is None:
        sleeve_by_yb = load_sleeve_by_yb_from_screener()
    legs: list[PutSpreadLeg] = []
    for row in payload.get("spreads") or []:
        if not isinstance(row, dict):
            continue
        expiry = _parse_date(row.get("expiry"))
        holdings_as_of = _parse_date(row.get("holdings_as_of")) or date.today()
        if expiry is None:
            continue
        strike_long = _parse_float(row.get("strike_long"))
        strike_short = _parse_float(row.get("strike_short"))
        if strike_long is None or strike_short is None:
            continue
        yb = str(row.get("yb_etf") or "").upper()
        und = str(row.get("underlying") or "").upper() or None
        root = str(row.get("option_root") or "").upper()
        sleeve = resolve_sleeve_ticker(root, und, yb_etf=yb, sleeve_by_yb=sleeve_by_yb)
        legs.append(PutSpreadLeg(
            yb_etf=yb,
            underlying=und,
            option_root=root,
            sleeve_2x_etf=sleeve,
            expiry=expiry,
            strike_long=float(strike_long),
            strike_short=float(strike_short),
            qty=float(_parse_float(row.get("qty")) or 0.0),
            notional_long=float(_parse_float(row.get("notional_long")) or 0.0),
            notional_short=float(_parse_float(row.get("notional_short")) or 0.0),
            weight_net=float(_parse_float(row.get("weight_net")) or 0.0),
            holdings_as_of=holdings_as_of,
            is_front=bool(row.get("is_front")),
            moneyness_long_pct=_parse_float(row.get("moneyness_long_pct")),
            moneyness_short_pct=_parse_float(row.get("moneyness_short_pct")),
            days_to_exp=int(row.get("days_to_exp")) if row.get("days_to_exp") is not None else None,
        ))
    return recompute_put_spread_front_flags(legs, as_of=date.today())


def load_yieldboost_target_strikes_by_sleeve(
    target_path: Path | str | None = None,
    *,
    front_only: bool = True,
) -> dict[str, list[float]]:
    """Held put/call strikes per 2x sleeve from yieldboost_options_target.json."""
    path = Path(target_path) if target_path else Path("data/yieldboost_options_target.json")
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    by_sleeve: dict[str, set[float]] = {}
    for row in payload.get("contracts") or []:
        if not isinstance(row, dict):
            continue
        if front_only and not row.get("is_front"):
            continue
        sleeve = str(row.get("sleeve") or "").upper()
        strike = _parse_float(row.get("strike"))
        if sleeve and strike is not None:
            by_sleeve.setdefault(sleeve, set()).add(float(strike))
    return {k: sorted(v) for k, v in by_sleeve.items()}


def load_yieldboost_sleeve_symbols_from_spreads(
    spreads_path: Path | str | None = None,
    *,
    front_only: bool = True,
    screener_csv: Path | str | None = None,
) -> list[str]:
    """Unique 2x sleeve tickers to fetch options for (not YB ETF / underlying symbols)."""
    path = Path(spreads_path) if spreads_path else Path("data/yieldboost_put_spreads_latest.json")
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    sleeve_by_yb = load_sleeve_by_yb_from_screener(screener_csv)
    syms: set[str] = set()
    for row in payload.get("spreads") or []:
        if not isinstance(row, dict):
            continue
        if front_only and not row.get("is_front"):
            continue
        yb = str(row.get("yb_etf") or "").strip().upper()
        und = str(row.get("underlying") or "").strip().upper() or None
        root = str(row.get("option_root") or "").strip().upper()
        sleeve = resolve_sleeve_ticker(root, und, yb_etf=yb, sleeve_by_yb=sleeve_by_yb)
        if sleeve:
            syms.add(sleeve)
    return sorted(syms)


def held_strike_band(
    strikes: list[float],
    spot_value: float | None,
    *,
    pad_pct: float = 0.25,
) -> tuple[float, float]:
    """Inclusive strike band around Granite held legs (allows slightly ITM puts)."""
    lo, hi = min(strikes), max(strikes)
    span = max(hi - lo, 0.01)
    pad = max(span * pad_pct, (spot_value or hi) * 0.05, 0.25)
    return lo - pad, hi + pad


def load_yieldboost_force_symbols_from_spreads(
    spreads_path: Path | None = None,
    *,
    screener_csv: Path | str | None = None,
    include_underlying_yb: bool = False,
) -> list[str]:
    """Symbols to force-refresh in options shard runs (default: 2x sleeves only)."""
    path = spreads_path or Path("data/yieldboost_put_spreads_latest.json")
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    sleeve_by_yb = load_sleeve_by_yb_from_screener(screener_csv)
    syms: set[str] = set()
    for row in payload.get("spreads") or []:
        if not isinstance(row, dict):
            continue
        yb = str(row.get("yb_etf") or "").strip().upper()
        und = str(row.get("underlying") or "").strip().upper() or None
        root = str(row.get("option_root") or "").strip().upper()
        sleeve = resolve_sleeve_ticker(root, und, yb_etf=yb, sleeve_by_yb=sleeve_by_yb)
        if sleeve:
            syms.add(sleeve)
        if include_underlying_yb:
            for sym in (und, yb):
                if sym:
                    syms.add(sym)
    return sorted(syms)


def build_yieldboost_options_target(spreads: Iterable[PutSpreadLeg]) -> dict[str, Any]:
    """Explicit contract list for targeted YieldBOOST quote refresh."""
    contracts: list[dict[str, Any]] = []
    seen: set[tuple[str, str, float, str]] = set()
    for s in spreads:
        for strike, side in ((s.strike_long, "long"), (s.strike_short, "short")):
            key = (s.sleeve_2x_etf, s.expiry.isoformat(), float(strike), "P")
            if key in seen:
                continue
            seen.add(key)
            contracts.append({
                "yb_etf": s.yb_etf,
                "sleeve": s.sleeve_2x_etf,
                "underlying": s.underlying,
                "expiry": s.expiry.isoformat(),
                "strike": float(strike),
                "put_call": "P",
                "leg_side": side,
                "is_front": bool(s.is_front),
            })
    return {
        "build_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "contract_count": len(contracts),
        "contracts": contracts,
    }


def build_vrp_health_payload(
    spreads_payload: dict[str, Any],
    vrp_payload: dict[str, Any],
    *,
    options_cache: dict[str, Any] | None = None,
    errors: list[str] | None = None,
    stale_after_minutes: int = 30,
) -> dict[str, Any]:
    """Coverage + staleness rollup for YieldBOOST VRP lane."""
    now = datetime.now(timezone.utc)
    front_spreads = [
        r for r in (spreads_payload.get("spreads") or [])
        if isinstance(r, dict) and r.get("is_front")
    ]
    vrp_rows = [r for r in (vrp_payload.get("rows") or []) if isinstance(r, dict)]
    iv_ok = sum(
        1 for r in vrp_rows
        if r.get("iv_put_long") is not None and r.get("iv_put_short") is not None
    )
    front_count = len(front_spreads) or len(vrp_rows)
    coverage = (iv_ok / front_count) if front_count else 0.0

    stale_sleeves: list[str] = []
    options_as_of_max: str | None = None
    for row in vrp_rows:
        sleeve = str(row.get("sleeve_2x") or "").upper()
        if row.get("iv_put_long") is None or row.get("iv_put_short") is None:
            if sleeve and sleeve not in stale_sleeves:
                stale_sleeves.append(sleeve)
        ts = row.get("options_as_of")
        if ts and (options_as_of_max is None or str(ts) > options_as_of_max):
            options_as_of_max = str(ts)

    holdings_as_of = None
    for row in front_spreads:
        ha = row.get("holdings_as_of")
        if ha and (holdings_as_of is None or str(ha) > holdings_as_of):
            holdings_as_of = str(ha)

    return {
        "build_time": now.isoformat().replace("+00:00", "Z"),
        "holdings_as_of": holdings_as_of,
        "spreads_front_count": front_count,
        "iv_coverage_front_pct": round(coverage, 4),
        "stale_sleeves": stale_sleeves,
        "last_options_refresh": options_cache.get("build_time") if options_cache else options_as_of_max,
        "last_holdings_refresh": spreads_payload.get("build_time"),
        "last_vrp_refresh": vrp_payload.get("build_time"),
        "stale_after_minutes": int(stale_after_minutes),
        "errors": list(errors or []),
    }


def pair_put_spreads_from_holdings(
    holdings_df: pd.DataFrame,
    *,
    underlying_by_etf: dict[str, str] | None = None,
    sleeve_by_yb: dict[str, str] | None = None,
    as_of: date | None = None,
) -> list[PutSpreadLeg]:
    """Pair Granite OPTION_PUT legs into bull put spreads per (etf, root, expiry)."""
    df = normalize_holdings_dataframe(holdings_df)
    if df.empty:
        return []
    if "security_type" not in df.columns:
        return []
    opts = df[df["security_type"].astype(str).str.upper().isin({"OPTION_PUT", "OPTION"})].copy()
    if opts.empty:
        return []
    if "option_put_call" in opts.columns:
        opts = opts[opts["option_put_call"].fillna("P").astype(str).str.upper() == "P"]
    und_map = {k.upper(): v.upper() for k, v in (underlying_by_etf or {}).items()}
    if sleeve_by_yb is None:
        sleeve_by_yb = load_sleeve_by_yb_from_screener()
    today = as_of or date.today()
    spreads: list[PutSpreadLeg] = []

    for (etf, root, expiry_raw), grp in opts.groupby(
        ["etf_ticker", "option_root", "option_expiry"], dropna=False,
    ):
        if pd.isna(expiry_raw):
            continue
        expiry = _parse_date(expiry_raw)
        if expiry is None:
            continue
        root_s = str(root or "").upper()
        etf_s = str(etf).upper()
        und = und_map.get(etf_s)
        sleeve = resolve_sleeve_ticker(root_s, und, yb_etf=etf_s, sleeve_by_yb=sleeve_by_yb)

        longs = grp[grp["shares"].astype(float) > 0].copy()
        shorts = grp[grp["shares"].astype(float) < 0].copy()
        if longs.empty or shorts.empty:
            continue

        long_row = longs.loc[longs["option_strike"].astype(float).idxmin()]
        short_row = shorts.loc[shorts["option_strike"].astype(float).idxmax()]
        strike_long = float(long_row["option_strike"])
        strike_short = float(short_row["option_strike"])
        if strike_long >= strike_short:
            continue

        qty = float(min(abs(long_row["shares"]), abs(short_row["shares"])))
        w_long = _parse_float(long_row.get("weight_pct")) or 0.0
        w_short = _parse_float(short_row.get("weight_pct")) or 0.0
        hold_date = _parse_date(long_row.get("as_of_date")) or today

        spreads.append(PutSpreadLeg(
            yb_etf=str(etf).upper(),
            underlying=und,
            option_root=root_s,
            sleeve_2x_etf=sleeve,
            expiry=expiry,
            strike_long=strike_long,
            strike_short=strike_short,
            qty=qty,
            notional_long=abs(_parse_float(long_row.get("market_value")) or 0.0),
            notional_short=abs(_parse_float(short_row.get("market_value")) or 0.0),
            weight_net=(w_long or 0.0) + (w_short or 0.0),
            holdings_as_of=hold_date,
            days_to_exp=max(0, (expiry - today).days),
        ))

    return recompute_put_spread_front_flags(spreads, as_of=today)


def spreads_to_json(spreads: Iterable[PutSpreadLeg]) -> dict[str, Any]:
    rows = []
    for s in spreads:
        d = asdict(s)
        d["expiry"] = s.expiry.isoformat()
        d["holdings_as_of"] = s.holdings_as_of.isoformat()
        rows.append(d)
    return {
        "build_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "spreads": rows,
        "front_count": sum(1 for s in spreads if s.is_front),
        "spread_count": len(rows),
    }


def build_put_spreads_payload(
    holdings_df: pd.DataFrame,
    *,
    underlying_by_etf: dict[str, str] | None = None,
    sleeve_by_yb: dict[str, str] | None = None,
) -> dict[str, Any]:
    spreads = pair_put_spreads_from_holdings(
        holdings_df,
        underlying_by_etf=underlying_by_etf,
        sleeve_by_yb=sleeve_by_yb,
    )
    return spreads_to_json(spreads)


def _norm_iv(raw: object) -> float | None:
    v = _parse_float(raw)
    if v is None or v <= 0:
        return None
    if v > 5:
        v = v / 100.0
    return v if v <= 5 else None


def lookup_contract_iv(
    options_cache: dict,
    symbol: str,
    expiry: date,
    strike: float,
    put_call: str = "P",
) -> dict[str, Any]:
    sym = str(symbol).upper()
    entry = (options_cache.get("symbols") or {}).get(sym) or {}
    spot = _parse_float(entry.get("spot"))
    target_type = "put" if put_call.upper() == "P" else "call"
    expiry_s = expiry.isoformat()
    best = None
    best_dist = None
    for c in entry.get("options") or []:
        if str(c.get("expiration_date")) != expiry_s:
            continue
        if str(c.get("contract_type", "")).lower() != target_type:
            continue
        cstrike = _parse_float(c.get("strike_price"))
        if cstrike is None:
            continue
        dist = abs(cstrike - float(strike))
        if best is None or dist < best_dist:
            best = c
            best_dist = dist
    if best is None:
        return {"matched": False, "spot": spot, "iv": None, "mid": None}
    return {
        "matched": best_dist is not None and best_dist < 0.05,
        "strike_used": _parse_float(best.get("strike_price")),
        "strike_interp": best_dist is not None and best_dist >= 1e-4,
        "iv": _norm_iv(best.get("iv")),
        "mid": _parse_float(best.get("mid")),
        "spot": spot,
        "options_as_of": entry.get("updated_at"),
    }


def build_vrp_live_payload(
    spreads: Iterable[PutSpreadLeg],
    options_cache: dict,
    *,
    rv_map: dict[str, float] | None = None,
) -> dict[str, Any]:
    rv_map = rv_map or {}
    rows: list[dict[str, Any]] = []
    for s in spreads:
        if not s.is_front:
            continue
        long_q = lookup_contract_iv(
            options_cache, s.sleeve_2x_etf, s.expiry, s.strike_long, "P",
        )
        short_q = lookup_contract_iv(
            options_cache, s.sleeve_2x_etf, s.expiry, s.strike_short, "P",
        )
        spot = long_q.get("spot") or short_q.get("spot")
        m_long = m_short = None
        if spot and spot > 0:
            m_long = s.strike_long / spot - 1.0
            m_short = s.strike_short / spot - 1.0
        spread_mid = None
        if long_q.get("mid") is not None and short_q.get("mid") is not None:
            spread_mid = float(short_q["mid"]) - float(long_q["mid"])
        sleeve_key = str(s.sleeve_2x_etf or "").upper()
        und_key = str(s.underlying or "").upper() if s.underlying else None
        rv_2x = rv_map.get(sleeve_key)
        rv_u = rv_map.get(und_key) if und_key else None
        iv_long = long_q.get("iv")
        iv_short = short_q.get("iv")
        if iv_long is not None and iv_short is not None:
            iv_spread_proxy = (float(iv_long) + float(iv_short)) / 2.0
        elif iv_long is not None:
            iv_spread_proxy = float(iv_long)
        elif iv_short is not None:
            iv_spread_proxy = float(iv_short)
        else:
            iv_spread_proxy = None
        vrp_2x = (
            (iv_spread_proxy - rv_2x)
            if iv_spread_proxy is not None and rv_2x is not None
            else None
        )
        rows.append({
            "yb_etf": s.yb_etf,
            "underlying": s.underlying,
            "option_root": s.option_root,
            "sleeve_2x": s.sleeve_2x_etf,
            "expiry": s.expiry.isoformat(),
            "strike_long": s.strike_long,
            "strike_short": s.strike_short,
            "qty": s.qty,
            "spot_2x": spot,
            "moneyness_long_pct": round(m_long * 100, 3) if m_long is not None else None,
            "moneyness_short_pct": round(m_short * 100, 3) if m_short is not None else None,
            "iv_put_long": long_q.get("iv"),
            "iv_put_short": short_q.get("iv"),
            "spread_mid_market": spread_mid,
            "rv_30d_2x": rv_2x,
            "rv_30d_underlying": rv_u,
            "vrp_vol_2x": vrp_2x,
            "rv_30d_source": "dashboard_1m" if rv_2x is not None else None,
            "iv_source": "holdings_exact" if long_q.get("matched") and short_q.get("matched") else "holdings_nearest",
            "holdings_as_of": s.holdings_as_of.isoformat(),
            "options_as_of": long_q.get("options_as_of") or short_q.get("options_as_of"),
            "days_to_exp": s.days_to_exp,
        })
    return {
        "build_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "rows": rows,
        "row_count": len(rows),
    }


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, separators=(",", ":"), allow_nan=False)


def load_underlying_map_from_screener(csv_path: Path) -> dict[str, str]:
    if not csv_path.exists():
        return {}
    df = pd.read_csv(csv_path)
    if "ETF" not in df.columns or "Underlying" not in df.columns:
        return {}
    out: dict[str, str] = {}
    for _, row in df.iterrows():
        etf = str(row.get("ETF") or "").strip().upper()
        und = str(row.get("Underlying") or "").strip().upper()
        if etf and und:
            out[etf] = und
    return out

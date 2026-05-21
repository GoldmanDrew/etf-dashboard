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
}


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


def resolve_sleeve_ticker(option_root: str, underlying: str | None = None) -> str:
    root = str(option_root or "").upper().strip()
    if root in OPTION_ROOT_TO_SLEEVE:
        return OPTION_ROOT_TO_SLEEVE[root]
    if root.startswith("2") and len(root) > 1:
        candidate = root[1:]
        if candidate in KNOWN_SLEEVE_TICKERS:
            return candidate
        if underlying:
            und = underlying.upper()
            if candidate.startswith(und) or und in candidate:
                return candidate
            return und
        return candidate
    return root


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
            sleeve = resolve_sleeve_ticker(parsed.root, und)
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


def pair_put_spreads_from_holdings(
    holdings_df: pd.DataFrame,
    *,
    underlying_by_etf: dict[str, str] | None = None,
    as_of: date | None = None,
) -> list[PutSpreadLeg]:
    """Pair Granite OPTION_PUT legs into bull put spreads per (etf, root, expiry)."""
    if holdings_df is None or holdings_df.empty:
        return []
    df = holdings_df.copy()
    df["etf_ticker"] = df["etf_ticker"].astype(str).str.upper()
    if "security_type" not in df.columns:
        return []
    opts = df[df["security_type"].astype(str).str.upper().isin({"OPTION_PUT", "OPTION"})].copy()
    if opts.empty:
        return []
    if "option_put_call" in opts.columns:
        opts = opts[opts["option_put_call"].fillna("P").astype(str).str.upper() == "P"]
    und_map = {k.upper(): v.upper() for k, v in (underlying_by_etf or {}).items()}
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
        und = und_map.get(str(etf).upper())
        sleeve = resolve_sleeve_ticker(root_s, und)

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

    if not spreads:
        return []

    future = [s for s in spreads if s.expiry >= today]
    if future:
        front_expiry = min(s.expiry for s in future)
    else:
        front_expiry = max(s.expiry for s in spreads)
    for s in spreads:
        s.is_front = s.expiry == front_expiry
    return spreads


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
) -> dict[str, Any]:
    spreads = pair_put_spreads_from_holdings(holdings_df, underlying_by_etf=underlying_by_etf)
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
        rv_2x = rv_map.get(s.sleeve_2x_etf)
        rv_u = rv_map.get(s.underlying) if s.underlying else None
        iv_atm_proxy = long_q.get("iv") or short_q.get("iv")
        vrp_2x = (iv_atm_proxy - rv_2x) if iv_atm_proxy is not None and rv_2x is not None else None
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

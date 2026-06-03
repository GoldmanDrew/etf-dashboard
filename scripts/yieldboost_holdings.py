"""Granite YieldBOOST holdings XLS parsing, put-spread pairing, and VRP panel."""
from __future__ import annotations

import io
import json
import logging
import math
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import numpy as np
import pandas as pd

LOGGER = logging.getLogger("yieldboost_holdings")

HELD_STRIKE_EXACT_TOL = 0.011

# ── Held-expiry IV synthesis / grading thresholds ──────────────────────────
# Granite writes weekly / off-cycle put-spreads, but many 2x sleeves only list
# monthly OPRA expiries. When the held expiry is not listed we either
#   (P2) variance-time interpolate across two *bracketing* listed sleeve
#        expiries, or
#   (P1) impute the sleeve IV from the underlying surface (AZ rescale), which
#        usually DOES list the near-dated expiry.
# Both are honest proxies for the held-expiry IV and should grade C (monitor),
# not D (hidden) — but ONLY when built from fresh, near-dated, sane inputs.
# These constants bound "near-dated" and "sane" so a stale or wild proxy can
# never silently promote a row.
NEAR_UNDERLYING_EXPIRY_MAX_DAYS = 7   # underlying expiry must be within this of held to rescue
TERM_INTERP_MIN_SKEW_DAYS = 7         # only term-interp when the nearest sleeve node is beyond this
TERM_INTERP_MAX_BRACKET_DAYS = 45     # refuse to interpolate across a wider listed-expiry gap
SYNTH_IV_MIN = 0.01                   # reject synthesized IVs outside (1%, 800%)
SYNTH_IV_MAX = 8.0
IV_DISAGREEMENT_GRADE_CAP_PCT = 0.35  # far-sleeve vs underlying-derived IV gap that caps grade at C

# ── Short-YieldBOOST structural-edge join + quote-sync gates ───────────────
# The YB-Edge table historically ranked on ``edge_pp_of_max_loss`` -- the
# kernel-aware richness of the fund's FRONT put spread, in % of that spread's
# one-week max-loss. That is a *tactical vol overlay* on the hedge the fund
# holds; it is NOT the edge of shorting the YieldBOOST ETF, and its units are
# not comparable across names. The accurate, relative, apples-to-apples short
# edge already lives in the screener: ``net_edge_p50_annual`` (annualized, nets
# decay/borrow/carry; ``edge_sign_convention = short_favorable_positive`` so
# higher = better short). We join it per YB ticker and keep the vol overlay as
# an explicitly secondary, sign-corrected modifier.
#
# Sign correction: the fund is SHORT its front spread (collects the credit). A
# RICH spread (positive ``edge_pp_of_max_loss``) means the income engine is
# being over-paid for the vol it sells -> a mild HEADWIND to shorting the fund.
# A CHEAP spread is a mild tailwind. So the short-thesis alignment is the
# NEGATIVE of the put-spread edge. We never let the put-spread edge masquerade
# as the short signal.
SHORT_EDGE_SCREENER_COLUMNS = (
    "net_edge_p50_annual",
    "net_edge_p05_annual",
    "net_edge_p25_annual",
    "net_edge_p75_annual",
    "net_edge_p95_annual",
    "expected_gross_decay_p50_annual",
    "expected_gross_decay_annual",
    "borrow_fee_annual",
    "borrow_current",
    "borrow_for_net_annual",
    "borrow_avg_annual",
    "borrow_median_60d",
    "income_distributions_annual",
    "edge_sign_convention",
)
SHORT_EDGE_FLAG_COLUMNS = (
    "inverse_shortable",
    "purgatory",
    "strategy_blacklisted",
    "exclude_borrow_spike",
    "exclude_no_shares",
)
# Sleeve-quote vs underlying-quote timestamp divergence beyond this many hours
# means the row is NOT a synchronized snapshot (the XEY case: ETHU quote fresh,
# ETHA underlying quote ~7d stale). We flag, never silently rank, such rows.
QUOTE_SYNC_MAX_GAP_HOURS = 24.0
# Net-edge thresholds (annualized fraction; short_favorable_positive convention).
SHORT_SIGNAL_STRONG = 0.15
SHORT_SIGNAL_SELL = 0.05

GRANITE_BASE = "https://www.graniteshares.com"


def _variance_time_interp_iv(
    *, iv_lo: float | None, t_lo: float, iv_hi: float | None, t_hi: float, t_held: float,
) -> float | None:
    """Variance-time (total-variance-linear) IV interpolation to the held expiry.

    Interpolates ``σ²·T`` linearly between two bracketing listed expiries, then
    solves for ``σ`` at the held tenor. Returns ``None`` on degenerate inputs or
    when the result falls outside the sane IV band — callers must fall back.
    """
    if iv_lo is None or iv_hi is None:
        return None
    if t_lo <= 0 or t_hi <= 0 or t_held <= 0 or t_hi == t_lo:
        return None
    var_lo = iv_lo * iv_lo * t_lo
    var_hi = iv_hi * iv_hi * t_hi
    w = (t_held - t_lo) / (t_hi - t_lo)
    var_h = var_lo + w * (var_hi - var_lo)
    if var_h <= 0:
        return None
    iv = math.sqrt(var_h / t_held)
    return iv if SYNTH_IV_MIN < iv < SYNTH_IV_MAX else None

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
    vol_kind: str = "full",
) -> float | None:
    """
    Annualized ~30d realized vol from dashboard `realized_vol` or Yahoo window maps.

    Dashboard windows use `etf` / `underlying`; Yahoo builder windows use `vol_annual`.
    ``vol_kind='base'`` prefers event-stripped ``vol_annual_base`` when present.
    """
    if not stats or not isinstance(stats, dict):
        return None

    if vol_kind == "base":
        for key in ("rv_30d_base", "vol_annual_base"):
            v = _parse_float(stats.get(key))
            if v is not None and v > 0:
                return float(v)

    for key in ("rv_30d", "vol_annual"):
        v = _parse_float(stats.get(key))
        if v is not None and v > 0:
            return float(v)

    etf_keys = ("etf", "vol_annual", "etf_ewma", "etf_robust_ewma")
    und_keys = ("underlying", "vol_annual", "underlying_ewma", "underlying_robust_ewma")
    if vol_kind == "base":
        etf_keys = ("vol_annual_base", "etf_robust_ewma", "etf", "vol_annual")
        und_keys = ("vol_annual_base", "underlying_robust_ewma", "underlying", "vol_annual")
    keys = etf_keys if prefer == "etf" else und_keys

    for window in RV_VRP_WINDOWS:
        win = stats.get(window)
        if not isinstance(win, dict):
            continue
        if vol_kind == "base":
            v = _parse_float(win.get("vol_annual_base"))
            if v is not None and v > 0:
                return float(v)
        for key in keys:
            v = _parse_float(win.get(key))
            if v is not None and v > 0:
                return float(v)
    return None


def build_yieldboost_rv_map(
    *,
    realized_vol_by_symbol: dict[str, dict] | None = None,
    dashboard_records: list[dict] | None = None,
    vol_kind: str = "full",
) -> dict[str, float]:
    """Symbol -> annualized ~30d RV for VRP panel (sleeves + underlyings)."""
    rv_map: dict[str, float] = {}

    for rec in dashboard_records or []:
        if not isinstance(rec, dict):
            continue
        sym = str(rec.get("symbol") or "").upper().strip()
        if not sym:
            continue
        v = extract_rv_30d_annual(rec.get("realized_vol"), prefer="etf", vol_kind=vol_kind)
        if v is not None:
            rv_map[sym] = v

    for sym, stats in (realized_vol_by_symbol or {}).items():
        ss = str(sym or "").upper().strip()
        if not ss or not isinstance(stats, dict):
            continue
        v = extract_rv_30d_annual(stats, prefer="etf", vol_kind=vol_kind)
        if v is not None:
            rv_map[ss] = v

    return rv_map


def build_yieldboost_rv_maps(
    *,
    realized_vol_by_symbol: dict[str, dict] | None = None,
    dashboard_records: list[dict] | None = None,
) -> tuple[dict[str, float], dict[str, float]]:
    """Full and event-stripped RV maps for VRP panel."""
    full = build_yieldboost_rv_map(
        realized_vol_by_symbol=realized_vol_by_symbol,
        dashboard_records=dashboard_records,
        vol_kind="full",
    )
    base = build_yieldboost_rv_map(
        realized_vol_by_symbol=realized_vol_by_symbol,
        dashboard_records=dashboard_records,
        vol_kind="base",
    )
    for sym, val in full.items():
        base.setdefault(sym, val)
    return full, base


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


_OCC_SYMBOL_RE = re.compile(r"^([A-Z]{1,6})(\d{6})([PC])(\d{1,8})$")


def normalize_occ_symbol(symbol: object) -> str:
    """Canonical compact OCC symbol for provider matching."""
    s = str(symbol or "").strip().upper()
    if s.startswith("O:"):
        s = s[2:]
    m = _OCC_SYMBOL_RE.match(s)
    if not m:
        return s
    root, yymmdd, pc, strike_raw = m.groups()
    strike8 = strike_raw.zfill(8)[-8:]
    return f"{root}{yymmdd}{pc}{strike8}"


def occ_symbol_keys(symbol: object) -> set[str]:
    """Lookup keys for a provider OCC symbol (handles O: prefix / strike padding)."""
    norm = normalize_occ_symbol(symbol)
    if not norm:
        return set()
    keys = {norm}
    m = _OCC_SYMBOL_RE.match(norm)
    if m:
        root, yymmdd, pc, strike8 = m.groups()
        strike_int = int(strike8)
        keys.add(f"{root}{yymmdd}{pc}{strike_int:08d}")
        keys.add(f"{root}{yymmdd}{pc}{strike_int}")
    return keys


def build_occ_symbol_index(pending: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Map normalized OCC keys -> pending held-leg metadata."""
    index: dict[str, dict[str, Any]] = {}
    for meta in pending:
        occ = str(meta.get("occ") or "")
        for key in occ_symbol_keys(occ):
            index[key] = meta
    return index


def resolve_occ_ticker_for_contract(
    sleeve: str,
    expiry: date,
    strike: float,
    put_call: str,
    chain_rows: Iterable[dict] | None,
) -> str:
    """Prefer Tradier chain ticker; fall back to formatted OCC."""
    expiry_s = expiry.isoformat()
    target_type = "put" if str(put_call or "P").upper() == "P" else "call"
    best_row = None
    best_dist = None
    for row in chain_rows or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("expiration_date")) != expiry_s:
            continue
        if str(row.get("contract_type") or "").lower() != target_type:
            continue
        cstrike = _parse_float(row.get("strike_price"))
        if cstrike is None:
            continue
        dist = abs(cstrike - float(strike))
        if dist <= HELD_STRIKE_EXACT_TOL:
            ticker = normalize_occ_symbol(row.get("ticker"))
            if ticker:
                return ticker
        if best_row is None or dist < best_dist:
            best_row = row
            best_dist = dist
    if best_row is not None and best_dist is not None and best_dist < 0.05:
        ticker = normalize_occ_symbol(best_row.get("ticker"))
        if ticker:
            return ticker
    return format_occ_ticker(sleeve, expiry, put_call, strike)


def extract_option_mark_price(row: dict | None) -> tuple[float | None, str | None]:
    """Best-effort mark from an options row (live mid, last, or prior close)."""
    if not isinstance(row, dict):
        return None, None
    mid = _parse_float(row.get("mid"))
    if mid is not None and mid > 0:
        source = str(row.get("mid_source") or "mid")
        return mid, source
    for field, source in (
        ("last", "last"),
        ("close", "close"),
        ("prevclose", "prevclose"),
        ("prev_close", "prevclose"),
    ):
        val = _parse_float(row.get(field))
        if val is not None and val > 0:
            return val, source
    bid = _parse_float(row.get("bid"))
    ask = _parse_float(row.get("ask"))
    if bid is not None and ask is not None and bid >= 0 and ask >= bid and (bid > 0 or ask > 0):
        return 0.5 * (bid + ask), "bid_ask"
    if bid is not None and bid > 0:
        return bid, "bid"
    if ask is not None and ask > 0:
        return ask, "ask"
    return None, None


def backfill_exact_strike_mid_from_chain(
    chain_rows: Iterable[dict],
    *,
    expiry: date,
    strike: float,
    put_call: str = "P",
) -> dict | None:
    """Return a chain row copy with mid filled from last/close when bid/ask are stale."""
    expiry_s = expiry.isoformat()
    target_type = "put" if str(put_call or "P").upper() == "P" else "call"
    for row in chain_rows or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("expiration_date")) != expiry_s:
            continue
        if str(row.get("contract_type") or "").lower() != target_type:
            continue
        cstrike = _parse_float(row.get("strike_price"))
        if cstrike is None or abs(cstrike - float(strike)) > HELD_STRIKE_EXACT_TOL:
            continue
        if _parse_float(row.get("mid")) is not None:
            return dict(row)
        mark, source = extract_option_mark_price(row)
        if mark is None:
            return dict(row)
        patched = dict(row)
        patched["mid"] = mark
        patched["mid_source"] = source
        return patched
    return None


def load_yieldboost_front_contracts(
    target_path: Path | str | None = None,
    *,
    front_only: bool = True,
) -> list[dict[str, Any]]:
    """Held Granite contracts for targeted OCC quote refresh."""
    path = Path(target_path) if target_path else Path("data/yieldboost_options_target.json")
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for row in payload.get("contracts") or []:
        if not isinstance(row, dict):
            continue
        if front_only and not row.get("is_front"):
            continue
        out.append(dict(row))
    return out


def load_yieldboost_held_expiries_by_sleeve(
    target_path: Path | str | None = None,
    *,
    front_only: bool = True,
) -> dict[str, set[str]]:
    """Expiry dates that must survive chain expiry filters (per 2x sleeve)."""
    by_sleeve: dict[str, set[str]] = {}
    for row in load_yieldboost_front_contracts(target_path, front_only=front_only):
        sleeve = str(row.get("sleeve") or "").upper()
        expiry = str(row.get("expiry") or "").strip()
        if sleeve and expiry:
            by_sleeve.setdefault(sleeve, set()).add(expiry)
    return by_sleeve


HELD_STRIKE_EXACT_TOL = 0.011


def held_contract_needs_occ_quote(
    rows: Iterable[dict],
    *,
    expiry: date,
    strike: float,
    put_call: str = "P",
) -> bool:
    """True when the held leg lacks IV+mid at the exact Granite strike."""
    expiry_s = expiry.isoformat()
    target_type = "put" if str(put_call or "P").upper() == "P" else "call"
    exact_row = None
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        if str(r.get("expiration_date")) != expiry_s:
            continue
        if str(r.get("contract_type") or "").lower() != target_type:
            continue
        cstrike = _parse_float(r.get("strike_price"))
        if cstrike is None:
            continue
        if abs(cstrike - float(strike)) <= HELD_STRIKE_EXACT_TOL:
            exact_row = r
            break
    if exact_row is None:
        return True
    has_iv = _norm_iv(exact_row.get("iv")) is not None
    has_mid = _parse_float(exact_row.get("mid")) is not None
    return not (has_iv and has_mid)


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
    from etf_holdings_providers import latest_holdings_per_etf

    latest = latest_holdings_per_etf(hist)
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


def load_yieldboost_underlying_symbols_from_spreads(
    spreads_path: Path | str | None = None,
    *,
    front_only: bool = True,
) -> list[str]:
    """Unique underlying tickers from YieldBOOST spreads (for mystery-event option chains)."""
    path = Path(spreads_path) if spreads_path else Path("data/yieldboost_put_spreads_latest.json")
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    syms: set[str] = set()
    for row in payload.get("spreads") or []:
        if not isinstance(row, dict):
            continue
        if front_only and not row.get("is_front"):
            continue
        und = str(row.get("underlying") or "").strip().upper()
        if und:
            syms.add(und)
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
    # Worst-of (oldest) timestamps for the per-row freshness pill. The headline
    # ``Fresh`` badge on the dashboard topbar previously used ``build_time`` only;
    # we now compute the *oldest* per-row options_as_of and the holdings_as_of
    # gap so the badge can never claim the dataset is fresher than the oldest
    # feed actually says.
    options_as_of_min: str | None = None
    underlying_options_as_of_min: str | None = None
    worst_underlying_symbol: str | None = None
    per_row_stale_minutes: list[int] = []
    for row in vrp_rows:
        sleeve = str(row.get("sleeve_2x") or "").upper()
        if row.get("iv_put_long") is None or row.get("iv_put_short") is None:
            if sleeve and sleeve not in stale_sleeves:
                stale_sleeves.append(sleeve)
        ts = row.get("options_as_of")
        if ts:
            if options_as_of_max is None or str(ts) > options_as_of_max:
                options_as_of_max = str(ts)
            if options_as_of_min is None or str(ts) < options_as_of_min:
                options_as_of_min = str(ts)
            mins = _ts_age_minutes(str(ts), now)
            if mins is not None:
                per_row_stale_minutes.append(mins)
        und_ts = row.get("underlying_options_as_of")
        if und_ts:
            if underlying_options_as_of_min is None or str(und_ts) < underlying_options_as_of_min:
                underlying_options_as_of_min = str(und_ts)
                worst_underlying_symbol = str(row.get("underlying") or row.get("spot_underlying") or "").upper() or None

    holdings_as_of = None
    for row in front_spreads:
        ha = row.get("holdings_as_of")
        if ha and (holdings_as_of is None or str(ha) > holdings_as_of):
            holdings_as_of = str(ha)
    holdings_age_days = _date_age_days(holdings_as_of, now.date()) if holdings_as_of else None
    worst_options_age_minutes = (
        max(per_row_stale_minutes) if per_row_stale_minutes else None
    )
    worst_underlying_age_minutes = (
        _ts_age_minutes(underlying_options_as_of_min, now) if underlying_options_as_of_min else None
    )

    missing_chain_ybs = sorted({
        str(r.get("yb_etf") or "").upper()
        for r in vrp_rows
        if isinstance(r, dict)
        and r.get("iv_source") == "holdings_missing_chain"
        and r.get("yb_etf")
    })
    leg_iv_ok = 0
    leg_mid_ok = 0
    spread_iv_and_mid_ok = 0
    for row in vrp_rows:
        if not isinstance(row, dict):
            continue
        if row.get("iv_put_long") is not None:
            leg_iv_ok += 1
        if row.get("iv_put_short") is not None:
            leg_iv_ok += 1
        if row.get("spread_mid_market") is not None:
            leg_mid_ok += 2
            if row.get("iv_put_long") is not None and row.get("iv_put_short") is not None:
                spread_iv_and_mid_ok += 1
    legs_total = max(len(vrp_rows) * 2, 1) if vrp_rows else 0
    front_leg_quote_coverage = {
        "legs_total": len(vrp_rows) * 2,
        "leg_iv_ok": leg_iv_ok,
        "leg_mid_ok": leg_mid_ok,
        "spread_iv_and_mid_ok": spread_iv_and_mid_ok,
        "leg_iv_pct": round(leg_iv_ok / legs_total, 4) if vrp_rows else 0.0,
        "leg_mid_pct": round(leg_mid_ok / legs_total, 4) if vrp_rows else 0.0,
        "spread_mid_pct": round(spread_iv_and_mid_ok / front_count, 4) if front_count else 0.0,
    }
    occ_stats = {}
    if options_cache:
        occ_stats = {
            "tradier_api_configured": bool(options_cache.get("tradier_api_configured")),
            "yieldboost_occ_quotes_requested": options_cache.get("yieldboost_occ_quotes_requested"),
            "yieldboost_occ_quotes_filled": options_cache.get("yieldboost_occ_quotes_filled"),
            "yieldboost_occ_quotes_fetched_requests": options_cache.get("yieldboost_occ_quotes_fetched_requests"),
            "yieldboost_occ_skip_reason": options_cache.get("yieldboost_occ_skip_reason"),
        }

    return {
        "build_time": now.isoformat().replace("+00:00", "Z"),
        "holdings_as_of": holdings_as_of,
        "holdings_age_trading_days": holdings_age_days,
        "spreads_front_count": front_count,
        "iv_coverage_front_pct": round(coverage, 4),
        "stale_sleeves": stale_sleeves,
        "missing_chain_ybs": missing_chain_ybs,
        "front_leg_quote_coverage": front_leg_quote_coverage,
        "occ_supplement": occ_stats,
        "last_options_refresh": options_cache.get("build_time") if options_cache else options_as_of_max,
        "last_holdings_refresh": spreads_payload.get("build_time"),
        "last_vrp_refresh": vrp_payload.get("build_time"),
        # P0a — worst-of clock for the dashboard topbar freshness pill.
        "options_as_of_max": options_as_of_max,
        "options_as_of_min": options_as_of_min,
        "underlying_options_as_of_min": underlying_options_as_of_min,
        "worst_underlying_symbol": worst_underlying_symbol,
        "worst_sleeve_options_age_minutes": worst_options_age_minutes,
        "worst_underlying_options_age_minutes": worst_underlying_age_minutes,
        "stale_after_minutes": int(stale_after_minutes),
        "errors": list(errors or []),
    }


def _ts_age_minutes(ts: str | None, now: datetime) -> int | None:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0, int((now - dt).total_seconds() // 60))


def _date_age_days(d_iso: str | None, today: date) -> int | None:
    """Approximate trading-day age (Sat/Sun count as 0)."""
    if not d_iso:
        return None
    try:
        d = date.fromisoformat(str(d_iso))
    except ValueError:
        return None
    days = (today - d).days
    if days <= 0:
        return 0
    # Subtract weekend days that fell inside the interval (rough but useful).
    weekend = 0
    for i in range(days):
        weekday = (d + timedelta(days=i + 1)).weekday()
        if weekday >= 5:
            weekend += 1
    return max(0, days - weekend)


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
    *,
    nearest_expiry_max_days: int = 35,
    as_of: date | None = None,
) -> dict[str, Any]:
    """Resolve a held contract's IV/mid from the cached chain.

    Resolution order:
      1. Exact expiry + exact strike (within ``HELD_STRIKE_EXACT_TOL``).
      2. Exact expiry + nearest strike.
      3. **Nearest expiry** within ``nearest_expiry_max_days`` calendar days
         + nearest strike at that expiry. This case handles two distinct
         scenarios:
           a. Off-cycle (Wed/Tue) Granite expiries that are OTC structures
              not listed on Tradier (nearest is the adjacent Friday weekly,
              ~2-4 days away).
           b. Newer 2x sleeves (AMZZ, BABX, FBL, …) that only have monthly
              expirations -- nearest is the next 3rd-Friday, up to ~30 days
              away. The IV-term-structure error is real for monthly-out
              interpolation on a 1-day-out spread; the UI should warn the
              user on large ``expiry_skew_days`` values.
      3b. **(P2) Term-structure interpolation.** When the nearest listed expiry
         is far (``> TERM_INTERP_MIN_SKEW_DAYS``) but the held date is *bracketed*
         by two listed expiries no more than ``TERM_INTERP_MAX_BRACKET_DAYS``
         apart, variance-time interpolate the IV to the held tenor instead of
         taking the far node's IV verbatim. ``iv_term_interp=True`` flags this so
         the grade treats it as a held-expiry proxy (C) rather than a raw far
         quote (D). The MID is *not* interpolated (time-value across expiries is
         not linear); we keep the nearest node's mid for spread pricing.

    The returned ``iv_source_chain`` field documents which tier produced the
    quote and ``expiry_skew_days`` is the |held - chain| calendar-day gap
    (always non-negative) so consumers can flag risky interpolations.
    """
    as_of = as_of or date.today()
    sym = str(symbol).upper()
    entry = (options_cache.get("symbols") or {}).get(sym) or {}
    spot = _parse_float(entry.get("spot"))
    target_type = "put" if put_call.upper() == "P" else "call"
    expiry_s = expiry.isoformat()
    chain = [c for c in (entry.get("options") or []) if isinstance(c, dict)]
    expiry_in_chain = any(str(c.get("expiration_date")) == expiry_s for c in chain)

    def _best_at_expiry(exp_str: str) -> tuple[dict | None, float | None, bool]:
        """Return (best_row, best_dist, was_exact) for ``exp_str`` in this chain."""
        exact_row = None
        nearest_row = None
        nearest_dist = None
        for c in chain:
            if str(c.get("expiration_date")) != exp_str:
                continue
            if str(c.get("contract_type", "")).lower() != target_type:
                continue
            cstrike = _parse_float(c.get("strike_price"))
            if cstrike is None:
                continue
            dist = abs(cstrike - float(strike))
            if dist <= HELD_STRIKE_EXACT_TOL:
                exact_row = c
                break
            if nearest_row is None or dist < nearest_dist:
                nearest_row = c
                nearest_dist = dist
        if exact_row is not None:
            return exact_row, 0.0, True
        return nearest_row, nearest_dist, False

    iv_source_chain: list[str] = []
    best: dict | None = None
    best_dist: float | None = None
    exact = False
    chain_expiry_used: str | None = None

    # Tier 1 / 2: exact expiry.
    if expiry_in_chain:
        best, best_dist, exact = _best_at_expiry(expiry_s)
        chain_expiry_used = expiry_s
        iv_source_chain.append("holdings_exact" if exact else "holdings_nearest_strike")

    # Tier 3: nearest expiry (only when exact expiry yielded no usable IV/mid).
    iv_at_best = _norm_iv(best.get("iv")) if best is not None else None
    mid_at_best = _parse_float(best.get("mid")) if best is not None else None
    if (best is None or (iv_at_best is None and mid_at_best is None)) and chain:
        candidate_expiries: list[tuple[int, str]] = []
        for c in chain:
            exp_other = str(c.get("expiration_date") or "")
            if not exp_other or exp_other == expiry_s:
                continue
            try:
                exp_dt = datetime.strptime(exp_other, "%Y-%m-%d").date()
            except ValueError:
                continue
            delta_days = abs((exp_dt - expiry).days)
            if delta_days > nearest_expiry_max_days:
                continue
            candidate_expiries.append((delta_days, exp_other))
        candidate_expiries.sort()
        for _delta, exp_alt in candidate_expiries:
            alt_row, alt_dist, alt_exact = _best_at_expiry(exp_alt)
            if alt_row is None:
                continue
            alt_iv = _norm_iv(alt_row.get("iv"))
            alt_mid = _parse_float(alt_row.get("mid"))
            if alt_iv is None and alt_mid is None:
                continue
            best = alt_row
            best_dist = alt_dist
            exact = False
            chain_expiry_used = exp_alt
            iv_source_chain.append("holdings_nearest_expiry")
            break

    if best is None:
        return {
            "matched": False,
            "expiry_in_chain": expiry_in_chain,
            "exact_strike": False,
            "spot": spot,
            "iv": None,
            "mid": None,
            "options_as_of": entry.get("updated_at"),
            "iv_source_chain": iv_source_chain,
            "expiry_skew_days": None,
            "iv_term_interp": False,
            "iv_interp_expiries": None,
        }
    skew_days: int | None = None
    if chain_expiry_used is not None:
        try:
            used_dt = datetime.strptime(chain_expiry_used, "%Y-%m-%d").date()
            skew_days = abs((used_dt - expiry).days)
        except ValueError:
            skew_days = None

    final_iv = _norm_iv(best.get("iv"))

    # ── P2: variance-time term-structure interpolation ─────────────────────
    # Only when the nearest listed node is far AND the held date is bracketed by
    # two listed expiries within the max-bracket span. Refines ``final_iv`` to
    # the held tenor; ``expiry_skew_days`` stays the observed-data distance so
    # the grade remains honest, but ``iv_term_interp`` lets it count as a
    # held-expiry proxy.
    iv_term_interp = False
    iv_interp_expiries: list[str] | None = None
    if (
        final_iv is not None
        and skew_days is not None
        and skew_days > TERM_INTERP_MIN_SKEW_DAYS
        and not exact
    ):
        lo_node: tuple[date, str, float] | None = None  # (exp_date, exp_str, iv)
        hi_node: tuple[date, str, float] | None = None
        for c in chain:
            exp_other = str(c.get("expiration_date") or "")
            if not exp_other:
                continue
            try:
                exp_dt = datetime.strptime(exp_other, "%Y-%m-%d").date()
            except ValueError:
                continue
            # Both bracket nodes must have a positive time-to-expiry from the
            # valuation date — an already-expired listed node carries no usable
            # term-structure information.
            if exp_dt <= as_of:
                continue
            node_row, _node_dist, _node_exact = _best_at_expiry(exp_other)
            node_iv = _norm_iv(node_row.get("iv")) if node_row is not None else None
            if node_iv is None:
                continue
            if exp_dt < expiry:
                if lo_node is None or exp_dt > lo_node[0]:
                    lo_node = (exp_dt, exp_other, node_iv)
            elif exp_dt > expiry:
                if hi_node is None or exp_dt < hi_node[0]:
                    hi_node = (exp_dt, exp_other, node_iv)
        if (
            lo_node is not None and hi_node is not None
            and (hi_node[0] - lo_node[0]).days <= TERM_INTERP_MAX_BRACKET_DAYS
        ):
            t_lo = max((lo_node[0] - as_of).days, 1) / 365.0
            t_hi = max((hi_node[0] - as_of).days, 1) / 365.0
            t_held = max((expiry - as_of).days, 1) / 365.0
            interp_iv = _variance_time_interp_iv(
                iv_lo=lo_node[2], t_lo=t_lo, iv_hi=hi_node[2], t_hi=t_hi, t_held=t_held,
            )
            if interp_iv is not None:
                final_iv = round(interp_iv, 6)
                iv_term_interp = True
                iv_interp_expiries = [lo_node[1], hi_node[1]]
                iv_source_chain.append("holdings_term_interp")

    return {
        "matched": exact or (best_dist is not None and best_dist < 0.05),
        "exact_strike": exact and chain_expiry_used == expiry_s,
        "expiry_in_chain": expiry_in_chain,
        "chain_expiry_used": chain_expiry_used,
        "expiry_skew_days": skew_days,
        "strike_used": _parse_float(best.get("strike_price")),
        "strike_interp": not exact and best_dist is not None and best_dist >= HELD_STRIKE_EXACT_TOL,
        "iv": final_iv,
        "mid": _parse_float(best.get("mid")),
        "spot": spot,
        "options_as_of": entry.get("updated_at"),
        "iv_source_chain": iv_source_chain,
        "iv_term_interp": iv_term_interp,
        "iv_interp_expiries": iv_interp_expiries,
    }


def resolve_iv_source(long_q: dict[str, Any], short_q: dict[str, Any]) -> str:
    """Label how held-leg IV was resolved from the options cache.

    Priority order (best first):
      ``holdings_exact`` -> ``holdings_nearest_strike`` -> ``holdings_nearest_expiry``
      -> ``holdings_missing_quote`` -> ``holdings_missing_chain``.
    """
    if long_q.get("iv") is None and short_q.get("iv") is None:
        if not long_q.get("expiry_in_chain") and not short_q.get("expiry_in_chain"):
            # Note: the nearest-expiry fallback in ``lookup_contract_iv`` may
            # still have produced quotes; if so we'd already have IVs above.
            return "holdings_missing_chain"
        return "holdings_missing_quote"
    # If either leg used the nearest-expiry fallback, label it as such -- this
    # is the most honest label since the IV is an interpolation across expiries.
    chain_l = long_q.get("iv_source_chain") or []
    chain_s = short_q.get("iv_source_chain") or []
    # Term-structure interpolation (P2) is synthesized AT the held tenor, so it
    # ranks above a raw far nearest-expiry read.
    if "holdings_term_interp" in chain_l or "holdings_term_interp" in chain_s:
        return "holdings_term_interp"
    if "holdings_nearest_expiry" in chain_l or "holdings_nearest_expiry" in chain_s:
        return "holdings_nearest_expiry"
    if (
        long_q.get("exact_strike")
        and short_q.get("exact_strike")
        and long_q.get("matched")
        and short_q.get("matched")
    ):
        return "holdings_exact"
    return "holdings_nearest_strike"


def data_grade(
    *,
    options_as_of: str | None,
    underlying_options_as_of: str | None,
    holdings_as_of: str | None,
    iv_source: str | None,
    iv_expiry_skew_days: float | int | None,
    now_utc: datetime | None = None,
    iv_term_interp: bool = False,
    underlying_iv_skew_days: float | int | None = None,
    iv_disagreement_pct: float | None = None,
) -> tuple[str, str]:
    """Return (grade, reason) for a single VRP row.

    Grades collapse the freshness × IV-source × skew × holdings-age matrix
    into a single PM-grade letter that drives sizing rules in
    ``income_schedule.py`` and the UI signal pill on both the YB-Edge page
    and the per-ETF Vol/VRP tab. Anything worse than B should not be
    sized; D rows are display-only.

    Bands:
      * A — tradeable at full size: holdings ≤ 1cd, worst quote ≤ 30 min,
        skew ≤ 3d, IV source is exact.
      * B — half size, sleeve only: holdings ≤ 2cd, worst quote ≤ 4h,
        skew ≤ 7d, IV source is exact or nearest-expiry.
      * C — monitor / pair with underlying hedge only: AZ-imputed source,
        term-interpolated source, or skew 8–21d, or worst quote 4–24h.
        The signal still has information; the magnitude is degraded.
      * D — display only: holdings > 2cd, worst quote > 24h, skew > 21d
        *with no fresh near-dated held-expiry proxy*, or IV source is
        ``holdings_missing_chain``. SELL recommendations must be hard-blocked.

    Held-expiry proxy (P1/P2): many 2x sleeves only list monthly OPRA expiries,
    so a weekly held spread shows ``iv_expiry_skew_days`` > 21 even though we can
    reconstruct an honest held-expiry IV. A large sleeve skew is forgiven from D
    to **C** (never better) when EITHER:
      * ``iv_term_interp`` is True (variance-time interpolated across two
        bracketing listed sleeve expiries ≤ ``TERM_INTERP_MAX_BRACKET_DAYS``
        apart), OR
      * ``underlying_iv_skew_days`` ≤ ``NEAR_UNDERLYING_EXPIRY_MAX_DAYS`` (the
        AZ rescale used a near-dated underlying expiry).
    The worst-quote freshness gate still applies independently, so a stale
    underlying chain cannot rescue anything (its age folds into ``worst_quote``).

    IV-disagreement guardrail (P3): when the raw far-sleeve IV and the
    underlying-derived IV disagree by more than ``IV_DISAGREEMENT_GRADE_CAP_PCT``,
    the row is capped at C — one of the two surfaces is mispriced and the model
    fair is not trustworthy at full size.
    """
    now = now_utc or datetime.now(timezone.utc)
    reasons: list[str] = []
    blockers: list[str] = []

    def _age_minutes(ts: str | None) -> int | None:
        if not ts:
            return None
        try:
            t = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            return max(0, int((now - t).total_seconds() / 60))
        except (ValueError, TypeError):
            return None

    def _holdings_age_cd(d: str | None) -> int | None:
        if not d:
            return None
        try:
            hd = date.fromisoformat(str(d).split("T")[0])
            return max(0, (now.date() - hd).days)
        except (ValueError, TypeError):
            return None

    sleeve_age = _age_minutes(options_as_of)
    und_age = _age_minutes(underlying_options_as_of)
    worst_quote = max(v for v in (sleeve_age, und_age, 0) if v is not None)
    hold_age = _holdings_age_cd(holdings_as_of)
    skew = float(iv_expiry_skew_days) if iv_expiry_skew_days is not None else 0.0
    src = str(iv_source or "")

    # P1/P2: do we have a fresh, near-dated held-expiry IV proxy that justifies
    # forgiving a large *sleeve* skew (monthly-only listings) down to C, not D?
    und_skew = (
        float(underlying_iv_skew_days)
        if underlying_iv_skew_days is not None
        else None
    )
    has_held_proxy = bool(iv_term_interp) or (
        und_skew is not None and und_skew <= NEAR_UNDERLYING_EXPIRY_MAX_DAYS
    )

    # Hard-block conditions → D
    if hold_age is not None and hold_age > 2:
        blockers.append(f"holdings {hold_age}cd > 2")
    if worst_quote > 24 * 60:
        blockers.append(f"quote {worst_quote // 60}h > 24h")
    if src == "holdings_missing_chain":
        blockers.append("missing chain")
    if skew > 21 and not has_held_proxy:
        blockers.append(f"skew {int(skew)}d > 21")
    if blockers:
        return ("D", "; ".join(blockers))

    # Degraded → C
    degraded: list[str] = []
    if src == "az_imputed_from_underlying":
        degraded.append("AZ-imputed IV")
    if src == "holdings_term_interp" or iv_term_interp:
        degraded.append("term-interp IV")
    if skew > 21 and has_held_proxy:
        # Monthly-only sleeve rescued by a held-expiry proxy.
        if iv_term_interp:
            degraded.append(f"sleeve skew {int(skew)}d (term-interp)")
        else:
            degraded.append(
                f"sleeve skew {int(skew)}d (underlying IV {int(und_skew)}d)"
            )
    elif skew > 7:
        degraded.append(f"skew {int(skew)}d")
    if worst_quote > 4 * 60:
        degraded.append(f"quote {worst_quote // 60}h")
    if hold_age is not None and hold_age == 2:
        degraded.append("holdings 2cd")
    if iv_disagreement_pct is not None and iv_disagreement_pct > IV_DISAGREEMENT_GRADE_CAP_PCT:
        degraded.append(f"sleeve/underlying IV disagree {iv_disagreement_pct * 100:.0f}%")
    if degraded:
        return ("C", "; ".join(degraded))

    # Tier B (size half) — exact-or-nearest, fresh enough
    if (
        worst_quote > 30
        or (hold_age is not None and hold_age >= 1)
        or skew > 3
        or src not in ("holdings_exact", "holdings_nearest_expiry")
    ):
        reasons.append(
            f"quote {worst_quote}m"
            + (f", holdings {hold_age}cd" if hold_age else "")
            + (f", skew {int(skew)}d" if skew else "")
            + (f", src={src}" if src else "")
        )
        return ("B", "; ".join(reasons) or "clean")

    return ("A", "exact, fresh, ≤3d skew, holdings 0cd")


def build_vrp_live_payload(
    spreads: Iterable[PutSpreadLeg],
    options_cache: dict,
    *,
    rv_map: dict[str, float] | None = None,
    rv_map_base: dict[str, float] | None = None,
    event_calendar: dict[str, Any] | None = None,
    as_of: date | None = None,
) -> dict[str, Any]:
    rv_map = rv_map or {}
    rv_map_base = rv_map_base or rv_map
    today = as_of or date.today()
    rows: list[dict[str, Any]] = []
    event_mod = None
    if event_calendar:
        try:
            from event_vol import enrich_vrp_row_with_events
            event_mod = enrich_vrp_row_with_events
        except ImportError:
            event_mod = None
    extras_mod = None
    try:
        from event_vol import TRADING_DAYS as _TD, compute_vrp_row_extras
        extras_mod = compute_vrp_row_extras
        trading_days = _TD
    except ImportError:
        trading_days = 252
    for s in spreads:
        if not s.is_front:
            continue
        long_q = lookup_contract_iv(
            options_cache, s.sleeve_2x_etf, s.expiry, s.strike_long, "P", as_of=today,
        )
        short_q = lookup_contract_iv(
            options_cache, s.sleeve_2x_etf, s.expiry, s.strike_short, "P", as_of=today,
        )
        spot = long_q.get("spot") or short_q.get("spot")
        m_long = m_short = None
        if spot and spot > 0:
            m_long = s.strike_long / spot - 1.0
            m_short = s.strike_short / spot - 1.0
        spread_mid = None
        if long_q.get("mid") is not None and short_q.get("mid") is not None:
            spread_mid = float(short_q["mid"]) - float(long_q["mid"])
        spread_mid_unavailable = spread_mid is None and (
            long_q.get("mid") is None or short_q.get("mid") is None
        )
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
        # Choose the larger of the two leg skews for headline warning (consumer
        # cares about the worst-case interpolation gap, not the average).
        skew_l = long_q.get("expiry_skew_days")
        skew_s = short_q.get("expiry_skew_days")
        if skew_l is None and skew_s is None:
            chain_skew_days = None
        else:
            chain_skew_days = max(v for v in (skew_l, skew_s) if v is not None)
        # If both legs landed on the same chain expiry use it directly;
        # otherwise expose both so a UI can decide.
        chain_expiry_used = long_q.get("chain_expiry_used") or short_q.get("chain_expiry_used")
        row = {
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
            "spread_mid_market_unavailable": spread_mid_unavailable,
            "rv_30d_2x": rv_2x,
            "rv_30d_underlying": rv_u,
            "vrp_vol_2x": vrp_2x,
            "rv_30d_source": "dashboard_1m" if rv_2x is not None else None,
            "iv_source": resolve_iv_source(long_q, short_q),
            "iv_chain_expiry_used": chain_expiry_used,
            "iv_expiry_skew_days": chain_skew_days,
            "iv_term_interp": bool(long_q.get("iv_term_interp") or short_q.get("iv_term_interp")),
            "iv_interp_expiries": long_q.get("iv_interp_expiries") or short_q.get("iv_interp_expiries"),
            "holdings_as_of": s.holdings_as_of.isoformat(),
            "options_as_of": long_q.get("options_as_of") or short_q.get("options_as_of"),
            "days_to_exp": s.days_to_exp,
        }
        if event_mod is not None:
            row = event_mod(
                row,
                calendar=event_calendar,
                options_cache=options_cache,
                rv_map_base=rv_map_base,
                as_of=today,
            )
        if extras_mod is not None:
            # Horizon in years from today to held expiry; clamp to >=1 trading day
            # so off-hours 0-DTE rows still produce finite greeks (the actual
            # decay/value is dominated by intrinsic at that point).
            days_to_exp = max(0, (s.expiry - today).days)
            horizon_years = max(days_to_exp / trading_days, 1.0 / trading_days)
            iv_full = row.get("iv_full_proxy") if "iv_full_proxy" in row else iv_spread_proxy
            iv_base = row.get("iv_base_proxy")
            rv_full = row.get("rv_30d_2x_full") if "rv_30d_2x_full" in row else rv_2x
            rv_base_row = row.get("rv_30d_2x_base")

            # ── Model context: pull underlying spot + chain for AZ/Heston/Bates ────
            # (Falls through cleanly to None if the underlying has no cached chain;
            # the model extras then emit nulls instead of crashing.)
            underlying_entry = {}
            spot_underlying_val = None
            underlying_iv_chain_rows: list[dict] = []
            if und_key:
                ucache = (options_cache.get("symbols") or {}).get(und_key) or {}
                if isinstance(ucache, dict):
                    underlying_entry = ucache
                    spot_underlying_val = _parse_float(ucache.get("spot"))
                    raw_chain = ucache.get("options") or []
                    if isinstance(raw_chain, list):
                        underlying_iv_chain_rows = [c for c in raw_chain if isinstance(c, dict)]
            # σ-bar for AZ moneyness map. Priority:
            #  1. ``rv_30d_underlying`` (true 30D realized on the spot leg).
            #  2. fallback to the in-row ``iv_underlying_implied`` (sleeve IV / |β|).
            #  3. last-resort: sleeve IV / 2 directly.
            # We accept *any* finite ?-bar so the AZ branch can fire on every
            # row that has a sleeve IV; bad ?-bar only shifts the mapped strike
            # by ½(β-1)σ²T, which on a 0-2 DTE held leg is a few basis points.
            sigma_bar_und = (
                _parse_float(row.get("rv_30d_underlying"))
                or _parse_float(rv_u)
                or _parse_float(row.get("iv_underlying_implied"))
            )
            if sigma_bar_und is None:
                iv_proxy = (
                    _parse_float(iv_full)
                    or _parse_float(iv_base)
                    or _parse_float(rv_full)
                    or _parse_float(rv_base_row)
                )
                if iv_proxy is not None:
                    sigma_bar_und = float(iv_proxy) / 2.0

            # Pull the per-underlying next-event date + historical MAD-move from
            # the event_calendar payload. Both are on the *underlying* scale;
            # ``compute_vrp_row_extras`` knows the β=2 sleeve→underlying inversion.
            evt_implied_und = None
            evt_hist_und = None
            evt_days = None
            evt_in_window = False
            if und_key and event_calendar:
                items = (event_calendar.get("items") or [])
                # Earliest upcoming earnings event for this underlying.
                soonest = None
                for it in items:
                    if (
                        str(it.get("event_type") or "").lower() == "earnings"
                        and str(it.get("underlying") or "").upper() == und_key
                    ):
                        try:
                            ed = date.fromisoformat(str(it.get("event_date")))
                        except Exception:
                            continue
                        if ed < today:
                            continue
                        if soonest is None or ed < soonest[0]:
                            soonest = (ed, it)
                if soonest is not None:
                    ed, it = soonest
                    evt_days = (ed - today).days
                    evt_in_window = ed <= s.expiry
                    hist = it.get("historical_move_pct_mad")
                    if hist is not None:
                        try:
                            evt_hist_und = float(hist)
                        except (TypeError, ValueError):
                            evt_hist_und = None
                # Convert sleeve event_implied_move_pct → underlying via β=2.
                # `event_implied_move_pct` is already on the sleeve scale; the
                # underlying expected jump-1σ is sleeve/2 in absolute % terms.
                ev_pct = row.get("event_implied_move_pct")
                if ev_pct is not None:
                    try:
                        evt_implied_und = float(ev_pct) / 2.0 / 100.0  # pct → fraction
                    except (TypeError, ValueError):
                        evt_implied_und = None

            extras = extras_mod(
                spot=spot,
                strike_long=s.strike_long,
                strike_short=s.strike_short,
                horizon_years=horizon_years,
                iv_full_sleeve=iv_full,
                iv_base_sleeve=iv_base,
                rv_2x_full=rv_full,
                rv_2x_base=rv_base_row,
                spread_mid=row.get("spread_mid_market"),
                event_implied_move_pct_underlying=evt_implied_und,
                historical_event_move_pct_underlying=evt_hist_und,
                days_to_event=evt_days,
                event_in_horizon=evt_in_window,
                yb_etf=s.yb_etf,
                underlying=s.underlying,
                sleeve_2x=s.sleeve_2x_etf,
                sleeve_beta=2.0,
                spot_underlying=spot_underlying_val,
                expiry_iso=s.expiry.isoformat(),
                underlying_iv_chain=underlying_iv_chain_rows,
                sigma_bar_underlying=sigma_bar_und,
                expense_rate_letf=0.0099,
                is_single_stock_sleeve=None,
            )
            row.update(extras)
            # Expose the underlying chain freshness so the UI can show a single
            # worst-of pill per row (sleeve quote || underlying quote || holdings).
            row["underlying_options_as_of"] = underlying_entry.get("updated_at") if isinstance(underlying_entry, dict) else None
            row["spot_underlying"] = spot_underlying_val
            # Final IV source label includes az_imputed escalation when sleeve IV is null.
            if row.get("iv_source") in ("holdings_missing_chain", "holdings_missing_quote"):
                if extras.get("az_implied_sleeve_iv") is not None:
                    row["iv_source"] = "az_imputed_from_underlying"

        # ── P1/P3: held-expiry IV synthesis provenance + disagreement guard ──
        # ``underlying_iv_skew_days`` is the worst (largest) expiry-skew the AZ
        # rescale had to accept on either leg; it gates the skew-forgiveness in
        # data_grade. ``iv_disagreement_pct`` compares the raw far-sleeve IV vs
        # the underlying-derived sleeve IV and caps over-confident grades.
        underlying_iv_skew_days = None
        iv_disagreement_pct = None
        az_implied_iv = row.get("az_implied_sleeve_iv")
        az_meta = row.get("az_meta") if isinstance(row.get("az_meta"), dict) else None
        if az_meta:
            und_skews = []
            for leg_meta in (az_meta.get("long"), az_meta.get("short")):
                if isinstance(leg_meta, dict):
                    sk = leg_meta.get("underlying_iv_expiry_skew_days")
                    if sk is not None:
                        try:
                            und_skews.append(abs(int(sk)))
                        except (TypeError, ValueError):
                            pass
            if und_skews:
                underlying_iv_skew_days = max(und_skews)
        far_sleeve_iv = _parse_float(row.get("iv_full_proxy"))
        if far_sleeve_iv is None:
            far_sleeve_iv = iv_spread_proxy
        if (
            az_implied_iv is not None
            and far_sleeve_iv is not None
            and far_sleeve_iv > 0
            and float(az_implied_iv) > 0
        ):
            denom = min(float(az_implied_iv), float(far_sleeve_iv))
            if denom > 0:
                iv_disagreement_pct = round(
                    abs(float(az_implied_iv) - float(far_sleeve_iv)) / denom, 4
                )
        iv_term_interp_flag = bool(row.get("iv_term_interp"))
        row["iv_synthesis"] = {
            "method": (
                "sleeve_exact" if row.get("iv_source") == "holdings_exact"
                else "sleeve_nearest_strike" if row.get("iv_source") == "holdings_nearest_strike"
                else "sleeve_term_interp" if iv_term_interp_flag
                else "az_underlying" if row.get("iv_source") == "az_imputed_from_underlying"
                else "sleeve_nearest_expiry" if row.get("iv_source") == "holdings_nearest_expiry"
                else "missing"
            ),
            "sleeve_skew_days": row.get("iv_expiry_skew_days"),
            "sleeve_interp_expiries": row.get("iv_interp_expiries"),
            "underlying_skew_days": underlying_iv_skew_days,
            "iv_far_sleeve": round(far_sleeve_iv, 6) if far_sleeve_iv is not None else None,
            "iv_underlying_derived": round(float(az_implied_iv), 6) if az_implied_iv is not None else None,
            "iv_disagreement_pct": iv_disagreement_pct,
        }
        row["iv_disagreement_pct"] = iv_disagreement_pct

        # ── Canonical data_grade stamp ─────────────────────────────────────
        grade, grade_reason = data_grade(
            options_as_of=row.get("options_as_of"),
            underlying_options_as_of=row.get("underlying_options_as_of"),
            holdings_as_of=row.get("holdings_as_of"),
            iv_source=row.get("iv_source"),
            iv_expiry_skew_days=row.get("iv_expiry_skew_days"),
            iv_term_interp=iv_term_interp_flag,
            underlying_iv_skew_days=underlying_iv_skew_days,
            iv_disagreement_pct=iv_disagreement_pct,
        )
        row["data_grade"] = grade
        row["data_grade_reason"] = grade_reason
        rows.append(row)
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


# ──────────────────────────────────────────────────────────────────────────
# Short-YieldBOOST structural edge: screener join + sync gates
# ──────────────────────────────────────────────────────────────────────────
def _parse_bool(val: object) -> bool | None:
    """CSV booleans arrive as 'True'/'False'/'' — keep None distinct from False."""
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    txt = str(val).strip().lower()
    if txt in ("", "nan", "none"):
        return None
    if txt in ("true", "1", "yes", "y", "t"):
        return True
    if txt in ("false", "0", "no", "n", "f"):
        return False
    return None


def load_short_edge_by_yb_from_screener(
    csv_path: Path | str | None = None,
) -> dict[str, dict[str, Any]]:
    """Per-YB structural short-edge fields from the screener.

    Returns ``{YB_TICKER: {<numeric fields>, <flags>, asof_date}}`` plus a
    special ``"__asof__"`` key holding the max ``asof_date`` across YB rows
    (the screener snapshot date used for the cross-dataset sync gate). The
    numeric ranker is ``net_edge_p50_annual`` (``short_favorable_positive``).
    """
    path = Path(csv_path) if csv_path else DEFAULT_SCREENER_CSV
    if not path.exists():
        return {}
    try:
        df = pd.read_csv(path)
    except Exception:
        return {}
    if "ETF" not in df.columns or "product_class" not in df.columns:
        return {}
    yb_df = df[df["product_class"] == "income_yieldboost"]
    out: dict[str, dict[str, Any]] = {}
    asof_dates: list[date] = []
    for _, row in yb_df.iterrows():
        etf = str(row.get("ETF") or "").strip().upper()
        if not etf:
            continue
        rec: dict[str, Any] = {}
        for col in SHORT_EDGE_SCREENER_COLUMNS:
            if col == "edge_sign_convention":
                val = row.get(col)
                rec[col] = None if val is None or (isinstance(val, float) and math.isnan(val)) else str(val)
                continue
            rec[col] = _parse_float(row.get(col))
        for col in SHORT_EDGE_FLAG_COLUMNS:
            rec[col] = _parse_bool(row.get(col))
        asof = _parse_date(row.get("asof_date"))
        rec["asof_date"] = asof.isoformat() if asof else None
        if asof:
            asof_dates.append(asof)
        out[etf] = rec
    if asof_dates:
        out["__asof__"] = max(asof_dates).isoformat()
    return out


def is_directly_shortable(rec: dict[str, Any] | None) -> bool:
    """Conservative shortability read from screener exclusion flags.

    ``purgatory`` / ``exclude_no_shares`` (no borrowable shares of the YB ETF
    itself), ``strategy_blacklisted`` and a live ``exclude_borrow_spike`` all
    mean the name is not cleanly shortable *right now*. We do not hard-drop
    these rows (Bucket-2 may still locate borrow); we flag and sink them.
    """
    if not rec:
        return False
    blockers = (
        rec.get("purgatory") is True
        or rec.get("exclude_no_shares") is True
        or rec.get("strategy_blacklisted") is True
        or rec.get("exclude_borrow_spike") is True
    )
    return not blockers


def compute_short_thesis_alignment(edge_pp_of_max_loss: float | None) -> dict[str, Any]:
    """Sign-correct the front put-spread edge for the SHORT-the-ETF thesis.

    The YB fund is short its front spread, so a rich spread (positive edge) is a
    headwind to shorting the fund. ``alignment_pp`` is therefore the negative of
    the raw edge; the raw value is preserved separately by the caller.
    """
    e = _parse_float(edge_pp_of_max_loss)
    if e is None:
        return {"alignment_pp": None, "direction": "unknown", "label": "—"}
    alignment = -e
    if alignment > 1.0:
        direction, label = "tailwind", "helps"
    elif alignment < -1.0:
        direction, label = "headwind", "hurts"
    else:
        direction, label = "neutral", "neutral"
    return {"alignment_pp": round(alignment, 2), "direction": direction, "label": label}


def compute_short_signal(
    net_edge_p50: float | None,
    net_edge_p05: float | None = None,
    *,
    shortable: bool = True,
) -> dict[str, Any]:
    """Headline SHORT signal derived from the screener net annual edge.

    Uses ``net_edge_p50_annual`` (short_favorable_positive). A robust STRONG
    SHORT additionally requires the p05 lower band to stay positive so a wide
    downside tail can't earn the top label.
    """
    p50 = _parse_float(net_edge_p50)
    p05 = _parse_float(net_edge_p05)
    if p50 is None:
        return {"label": "no edge data", "tier": "none", "rank": -1e9}
    if not shortable:
        return {"label": "No locate", "tier": "nolocate", "rank": p50}
    if p50 >= SHORT_SIGNAL_STRONG and (p05 is None or p05 > 0):
        tier, label = "top", "Top"
    elif p50 >= SHORT_SIGNAL_SELL:
        tier, label = "good", "Good"
    elif p50 > 0:
        tier, label = "thin", "Thin"
    else:
        tier, label = "skip", "Skip"
    return {"label": label, "tier": tier, "rank": p50}


def evaluate_quote_sync(
    row: dict[str, Any],
    *,
    screener_asof: str | None = None,
    max_gap_hours: float = QUOTE_SYNC_MAX_GAP_HOURS,
) -> dict[str, Any]:
    """Cross-input timestamp synchronization gate for one VRP row.

    The integrity risk (XEY case) is that the sleeve option quote, the
    underlying option quote, the holdings snapshot and the screener snapshot
    describe *different* moments, so model fair and market mid are not a single
    synchronized picture. We measure the largest pairwise gap among the
    available timestamps and flag the row when it exceeds ``max_gap_hours``.

    Note: we deliberately gate on TIMESTAMP divergence, not on a raw
    spot_2x/spot_underlying price ratio. The 2x sleeve NAV is rebased at
    inception, so its price level is not ``beta`` times the underlying level;
    a price-ratio test would be economically meaningless here.
    """
    def _to_dt(val: object, *, end_of_day: bool = False) -> datetime | None:
        if val is None:
            return None
        txt = str(val).strip()
        if not txt:
            return None
        try:
            if "T" in txt or " " in txt:
                return datetime.fromisoformat(txt.replace("Z", "+00:00"))
            d = _parse_date(txt)
            if d is None:
                return None
            hh = 20 if end_of_day else 0  # ~US close in UTC for date-only stamps
            return datetime(d.year, d.month, d.day, hh, 0, tzinfo=timezone.utc)
        except Exception:
            return None

    stamps: dict[str, datetime | None] = {
        "sleeve_quote": _to_dt(row.get("options_as_of")),
        "underlying_quote": _to_dt(row.get("underlying_options_as_of")),
        "holdings": _to_dt(row.get("holdings_as_of"), end_of_day=True),
        "screener": _to_dt(screener_asof, end_of_day=True),
    }
    present = {k: v for k, v in stamps.items() if v is not None}
    # Normalize to tz-aware UTC for safe subtraction.
    norm: dict[str, datetime] = {}
    for k, v in present.items():
        norm[k] = v if v.tzinfo is not None else v.replace(tzinfo=timezone.utc)

    sleeve = norm.get("sleeve_quote")
    und = norm.get("underlying_quote")
    quote_gap_hours = None
    if sleeve is not None and und is not None:
        quote_gap_hours = round(abs((sleeve - und).total_seconds()) / 3600.0, 2)

    max_gap_hours_seen = None
    if len(norm) >= 2:
        vals = list(norm.values())
        span = max(vals) - min(vals)
        max_gap_hours_seen = round(span.total_seconds() / 3600.0, 2)

    reasons: list[str] = []
    sync_ok = True
    if sleeve is None or und is None:
        sync_ok = False
        if sleeve is None:
            reasons.append("missing sleeve quote time")
        if und is None:
            reasons.append("missing underlying quote time")
    elif quote_gap_hours is not None and quote_gap_hours > max_gap_hours:
        sync_ok = False
        reasons.append(f"sleeve vs underlying quote gap {quote_gap_hours:.0f}h > {max_gap_hours:.0f}h")
    if max_gap_hours_seen is not None and max_gap_hours_seen > max_gap_hours and sync_ok:
        # All quote times agree but holdings/screener lag the live quotes.
        sync_ok = False
        reasons.append(f"input span {max_gap_hours_seen:.0f}h > {max_gap_hours:.0f}h")

    return {
        "sync_ok": sync_ok,
        "sync_reason": "; ".join(reasons) if reasons else "inputs within sync window",
        "quote_sync_gap_hours": quote_gap_hours,
        "max_input_gap_hours": max_gap_hours_seen,
    }


def borrow_carry_display_meta(row: dict[str, Any]) -> dict[str, Any]:
    """Live borrow when locate exists; else historical avg / 60d med (~hist)."""
    current = _parse_float(row.get("borrow_fee_annual"))
    if current is None:
        current = _parse_float(row.get("borrow_current"))
    avg = _parse_float(row.get("borrow_avg_annual"))
    med60 = _parse_float(row.get("borrow_median_60d"))
    for_net = _parse_float(row.get("borrow_for_net_annual"))
    if current is not None:
        display, source, label = current, "live", "live"
    elif avg is not None:
        display, source, label = avg, "hist_avg", "~hist avg"
    elif med60 is not None:
        display, source, label = med60, "hist_med60", "~hist 60d"
    else:
        display, source, label = None, "none", "—"
    parts = []
    if current is not None:
        parts.append(f"live borrow {current * 100:.1f}% ann")
    else:
        parts.append("live borrow: none")
    if avg is not None:
        parts.append(f"hist avg {avg * 100:.1f}%")
    if med60 is not None:
        parts.append(f"60d med {med60 * 100:.1f}%")
    if for_net is not None:
        parts.append(f"net-edge model used {for_net * 100:.1f}% borrow")
    return {
        "display_annual": round(display, 6) if display is not None else None,
        "source": source,
        "source_label": label,
        "for_net_annual": round(for_net, 6) if for_net is not None else None,
        "tooltip": "; ".join(parts),
    }


def _short_edge_why_sentence(row: dict[str, Any]) -> str:
    """One-line, auditable rationale for a row's rank (the 'why here?' popover)."""
    def _pct(v: object) -> str:
        f = _parse_float(v)
        return f"{f * 100:.0f}%" if f is not None else "—"

    parts = [f"Net short edge (p50) {_pct(row.get('net_edge_p50_annual'))} ann."]
    band_lo = _parse_float(row.get("net_edge_p05_annual"))
    band_hi = _parse_float(row.get("net_edge_p95_annual"))
    if band_lo is not None and band_hi is not None:
        parts.append(f"band {_pct(band_lo)}…{_pct(band_hi)}")
    parts.append(f"gross decay {_pct(row.get('expected_gross_decay_p50_annual'))}")
    bf = _parse_float(row.get("borrow_fee_annual"))
    parts.append(f"borrow {_pct(bf) if bf is not None else 'n/a'}")
    align = (row.get("short_thesis_alignment") or {}).get("label")
    if align and align != "—":
        parts.append(f"hedge: {align}")
    if not row.get("short_directly_shortable", True):
        parts.append("no locate")
    if not (row.get("quote_sync") or {}).get("sync_ok", True):
        parts.append("NOT SYNCED")
    return "; ".join(parts) + "."


def enrich_vrp_rows_with_short_edge(
    payload: dict[str, Any],
    short_edge_map: dict[str, dict[str, Any]] | None,
    *,
    screener_asof: str | None = None,
) -> dict[str, Any]:
    """Attach structural short-edge fields, sign-corrected overlay, signal and
    sync gate to each row of a ``vrp_live`` payload.

    Non-destructive: every existing field is preserved; we only add keys. The
    primary ranker becomes ``net_edge_p50_annual``; ``edge_pp_of_max_loss`` is
    kept verbatim and additionally exposed as a sign-corrected modifier.
    """
    if not isinstance(payload, dict):
        return payload
    short_edge_map = short_edge_map or {}
    if screener_asof is None:
        screener_asof = short_edge_map.get("__asof__")
    rows = payload.get("rows") or []
    for row in rows:
        yb = str(row.get("yb_etf") or "").strip().upper()
        rec = short_edge_map.get(yb) or {}
        for col in SHORT_EDGE_SCREENER_COLUMNS:
            row[col] = rec.get(col)
        for col in SHORT_EDGE_FLAG_COLUMNS:
            row[col] = rec.get(col)
        row["short_edge_asof"] = rec.get("asof_date") or screener_asof
        shortable = is_directly_shortable(rec) if rec else None
        row["short_directly_shortable"] = shortable
        row["short_thesis_alignment"] = compute_short_thesis_alignment(
            row.get("edge_pp_of_max_loss")
        )
        row["short_signal"] = compute_short_signal(
            rec.get("net_edge_p50_annual"),
            rec.get("net_edge_p05_annual"),
            shortable=bool(shortable) if shortable is not None else True,
        )
        row["quote_sync"] = evaluate_quote_sync(row, screener_asof=screener_asof)
        row["borrow_carry"] = borrow_carry_display_meta(row)
        row["short_edge_why"] = _short_edge_why_sentence(row)
    payload["short_edge_screener_asof"] = screener_asof
    payload["short_edge_join_count"] = sum(
        1 for r in rows if r.get("net_edge_p50_annual") is not None
    )
    return payload

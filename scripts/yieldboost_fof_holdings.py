"""YieldBOOST fund-of-funds (YBTY/YBST) holdings parse, rollup, and persistence."""
from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from yieldboost_fof_constants import (
    FOF_HOLDINGS_HISTORY_JSON,
    FOF_HOLDINGS_LATEST_JSON,
    YIELDBOOST_CHILD_TICKERS,
    YIELDBOOST_CHILD_TO_UNDERLYING,
    YIELDBOOST_FOF_SYMBOLS,
)

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_CASH_TYPES = frozenset({"CASH", "MONEY_MARKET", "TREASURY", "BOND", "OTHER"})
_ETF_DESC_RE = re.compile(
    r"\b([A-Z]{4})\b.*(?:YIELDBOOST|YIELD\s*BOOST)",
    re.I,
)


def norm_sym(s: object) -> str:
    return str(s or "").strip().upper().replace(".", "-")


def infer_yb_child_ticker(
    *,
    position_ticker: object = None,
    security_name: object = None,
    cusip: object = None,
) -> str | None:
    """Best-effort map a FoF holdings row to a child YieldBOOST ETF ticker."""
    for raw in (position_ticker, cusip):
        t = norm_sym(raw)
        if t in YIELDBOOST_CHILD_TICKERS:
            return t
    name = str(security_name or "")
    name_up = name.upper()
    m = _ETF_DESC_RE.search(name)
    if m:
        cand = norm_sym(m.group(1))
        if cand in YIELDBOOST_CHILD_TICKERS:
            return cand
    for m in re.finditer(r"\b([A-Z]{4})\b", name_up):
        cand = m.group(1)
        if cand in YIELDBOOST_CHILD_TICKERS:
            return cand
    if "YIELD" in name_up.replace(" ", ""):
        for yb, und in YIELDBOOST_CHILD_TO_UNDERLYING.items():
            if re.search(rf"\b{re.escape(und)}\b", name_up):
                return yb
            if re.search(rf"\b{re.escape(yb)}\b", name_up):
                return yb
    return None


def load_holdings_frame(
    *,
    csv_path: Path | None = None,
    parquet_path: Path | None = None,
) -> pd.DataFrame:
    csv_path = csv_path or (_DATA_DIR / "etf_holdings_latest.csv")
    parquet_path = parquet_path or (_DATA_DIR / "etf_holdings_daily.parquet")
    if parquet_path.exists():
        try:
            df = pd.read_parquet(parquet_path)
            if not df.empty:
                return df
        except Exception:
            pass
    if csv_path.exists():
        return pd.read_csv(csv_path)
    return pd.DataFrame()


def _parse_as_of(val: object) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        if hasattr(val, "date"):
            return val.date().isoformat()
        s = str(val).strip().split("T")[0]
        if len(s) >= 10:
            return s[:10]
    except (ValueError, TypeError):
        return None
    return None


def extract_fof_children_from_holdings(
    hdf: pd.DataFrame,
    fof_symbol: str,
    *,
    as_of: str | None = None,
) -> dict[str, Any] | None:
    """Build one FoF basket snapshot from holdings rows for ``fof_symbol``."""
    sym = norm_sym(fof_symbol)
    if hdf is None or hdf.empty or "etf_ticker" not in hdf.columns:
        return None
    sub = hdf[hdf["etf_ticker"].astype(str).str.upper() == sym].copy()
    if sub.empty:
        return None
    date_col = sub["as_of_date"].astype(str).str.slice(0, 10)
    if as_of:
        sub = sub[date_col == as_of]
    else:
        as_of = date_col.max()
        sub = sub[date_col == as_of]
    if sub.empty:
        return None

    children: list[dict[str, Any]] = []
    cash_pct = 0.0
    other_pct = 0.0

    for _, row in sub.iterrows():
        sec = str(row.get("security_type") or "OTHER").upper()
        if sec in _CASH_TYPES or "CASH" in str(row.get("security_name") or "").upper():
            w = _safe_float(row.get("weight_pct"))
            if w is not None:
                cash_pct += w
            continue
        yb = infer_yb_child_ticker(
            position_ticker=row.get("position_ticker"),
            security_name=row.get("security_name"),
            cusip=row.get("cusip"),
        )
        if not yb and sec == "ETF":
            yb = infer_yb_child_ticker(position_ticker=row.get("cusip"))
        if not yb:
            w = _safe_float(row.get("weight_pct"))
            if w is not None:
                other_pct += w
            continue
        und = YIELDBOOST_CHILD_TO_UNDERLYING.get(yb)
        children.append({
            "yb_etf": yb,
            "underlying": und,
            "weight_pct": _safe_float(row.get("weight_pct")),
            "market_value": _safe_float(row.get("market_value")),
            "shares": _safe_float(row.get("shares")),
            "security_type": sec,
        })

    if not children:
        return None

    inv_weight = sum(
        c["weight_pct"] for c in children
        if c.get("weight_pct") is not None and c["weight_pct"] > 0
    )
    if inv_weight <= 0:
        inv_weight = 100.0 - cash_pct - other_pct
    if inv_weight <= 0:
        inv_weight = 100.0

    underlying_weights: dict[str, float] = {}
    for c in children:
        w = c.get("weight_pct")
        if w is None or w <= 0:
            continue
        und = c.get("underlying")
        if not und:
            continue
        underlying_weights[und] = underlying_weights.get(und, 0.0) + float(w) / inv_weight

    holdings_as_of = as_of or _parse_as_of(sub["as_of_date"].max())
    return {
        "symbol": sym,
        "as_of": holdings_as_of,
        "children": children,
        "underlying_weights": {k: round(v, 6) for k, v in underlying_weights.items()},
        "cash_pct": round(cash_pct, 4) if cash_pct else 0.0,
        "other_pct": round(other_pct, 4) if other_pct else 0.0,
        "n_children": len(children),
        "weights_source": "granite_xls",
    }


def _safe_float(v: object) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        x = float(v)
        return x if pd.notna(x) and abs(x) < 1e18 else None
    except (TypeError, ValueError):
        return None


def build_fof_holdings_history(
    hdf: pd.DataFrame,
    symbols: tuple[str, ...] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """All distinct as-of baskets per FoF symbol from holdings panel."""
    syms = symbols or YIELDBOOST_FOF_SYMBOLS
    out: dict[str, list[dict[str, Any]]] = {}
    if hdf is None or hdf.empty:
        return out
    for fof in syms:
        fof_u = norm_sym(fof)
        sub = hdf[hdf["etf_ticker"].astype(str).str.upper() == fof_u]
        if sub.empty:
            continue
        dates = sorted(set(sub["as_of_date"].astype(str).str.slice(0, 10).tolist()))
        snaps: list[dict[str, Any]] = []
        for d in dates:
            snap = extract_fof_children_from_holdings(hdf, fof_u, as_of=d)
            if snap:
                snaps.append(snap)
        if snaps:
            out[fof_u] = snaps
    return out


def build_fof_holdings_payload(
    hdf: pd.DataFrame,
    *,
    symbols: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    history = build_fof_holdings_history(hdf, symbols=symbols)
    latest: dict[str, Any] = {}
    for sym, snaps in history.items():
        if snaps:
            latest[sym] = snaps[-1]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "symbols": list(symbols or YIELDBOOST_FOF_SYMBOLS),
        "latest": latest,
        "history": history,
        "version": 1,
    }


def write_fof_holdings_artifacts(
    payload: dict[str, Any],
    *,
    data_dir: Path | None = None,
) -> None:
    data_dir = data_dir or _DATA_DIR
    data_dir.mkdir(parents=True, exist_ok=True)
    latest_path = data_dir / Path(FOF_HOLDINGS_LATEST_JSON).name
    hist_path = data_dir / Path(FOF_HOLDINGS_HISTORY_JSON).name
    latest_path.write_text(json.dumps({
        "generated_at": payload.get("generated_at"),
        "symbols": payload.get("symbols"),
        "latest": payload.get("latest"),
        "version": payload.get("version", 1),
    }, indent=2), encoding="utf-8")
    hist_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def ensure_fof_holdings_in_store(
    *,
    csv_path: Path | None = None,
    parquet_path: Path | None = None,
    fetch_if_missing: bool = True,
) -> pd.DataFrame:
    """Return holdings frame; optionally fetch YBTY/YBST from Granite when absent."""
    hdf = load_holdings_frame(csv_path=csv_path, parquet_path=parquet_path)
    missing = [
        s for s in YIELDBOOST_FOF_SYMBOLS
        if hdf.empty or s not in set(hdf.get("etf_ticker", pd.Series(dtype=str)).astype(str).str.upper())
    ]
    if not missing or not fetch_if_missing:
        return hdf
    try:
        import requests
        from etf_holdings_providers import build_default_holdings_stack, fetch_all_holdings
        from ingest_etf_metrics import save_holdings_outputs

        session = requests.Session()
        session.headers.update({"User-Agent": "etf-dashboard-builder/1.0"})
        stack = build_default_holdings_stack(session, underlying_by_ticker=YIELDBOOST_CHILD_TO_UNDERLYING)
        new_df = fetch_all_holdings(list(missing), as_of=date.today(), stack=stack)
        if new_df is not None and not new_df.empty:
            if not hdf.empty:
                combined = pd.concat([hdf, new_df], ignore_index=True)
            else:
                combined = new_df
            save_holdings_outputs(combined)
            return combined
    except Exception as exc:
        print(f"  Warning: FoF holdings fetch failed: {exc}")
    return hdf


def stale_days(as_of: str | None) -> int | None:
    if not as_of:
        return None
    try:
        d = date.fromisoformat(str(as_of).split("T")[0])
        return max(0, (date.today() - d).days)
    except (ValueError, TypeError):
        return None

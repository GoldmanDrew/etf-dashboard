"""Shared constants for YieldBOOST fund-of-funds (YBTY, YBST) dashboard support."""
from __future__ import annotations

# Dashboard-only FoF tickers (not in ls-algo screener CSV).
YIELDBOOST_FOF_SYMBOLS: tuple[str, ...] = ("YBTY", "YBST")

# Child single-stock YieldBOOST ETFs -> underlying (same as build_data / UI).
YIELDBOOST_CHILD_TO_UNDERLYING: dict[str, str] = {
    "AMYY": "AMD",
    "AZYY": "AMZN",
    "BBYY": "BABA",
    "COYY": "COIN",
    "CWY": "CRWV",
    "HMYY": "HIMS",
    "HOYY": "HOOD",
    "IOYY": "IONQ",
    "MAAY": "MARA",
    "FBYY": "META",
    "MTYY": "MSTR",
    "MUYY": "MU",
    "NUGY": "GDX",
    "NVYY": "NVDA",
    "PLYY": "PLTR",
    "QBY": "QBTS",
    "RGYY": "RGTI",
    "RTYY": "RIOT",
    "SEMY": "SOXX",
    "SMYY": "SMCI",
    "TMYY": "TSM",
    "TQQY": "QQQ",
    "TSYY": "TSLA",
    "XBTY": "IBIT",
    "YSPY": "SPY",
}

YIELDBOOST_CHILD_TICKERS: frozenset[str] = frozenset(YIELDBOOST_CHILD_TO_UNDERLYING.keys())

FOF_HOLDINGS_LATEST_JSON = "data/yieldboost_fof_holdings_latest.json"
FOF_HOLDINGS_HISTORY_JSON = "data/yieldboost_fof_holdings_history.json"

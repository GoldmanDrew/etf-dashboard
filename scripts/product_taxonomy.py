"""Central product taxonomy (vol-ETP, YieldBOOST pairs, FoF, inverse list)."""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
TAXONOMY_FILE = REPO_ROOT / "config" / "product_taxonomy.json"

# Inline fallbacks if config file is missing (keep in sync with config/product_taxonomy.json).
_FALLBACK_VOL_ETP = {
    "UVIX", "SVIX", "UVXY", "SVXY", "VXX", "VIXY", "VIXM", "VIX", "VIX1D", "VIX3M",
}
_FALLBACK_YB_PAIRS = {
    ("AMYY", "AMD"), ("AZYY", "AMZN"), ("BBYY", "BABA"), ("COYY", "COIN"), ("CWY", "CRWV"),
    ("HMYY", "HIMS"), ("HOYY", "HOOD"), ("IOYY", "IONQ"), ("MAAY", "MARA"), ("FBYY", "META"),
    ("MTYY", "MSTR"), ("MUYY", "MU"), ("NUGY", "GDX"), ("NVYY", "NVDA"), ("PLYY", "PLTR"),
    ("QBY", "QBTS"), ("RGYY", "RGTI"), ("RTYY", "RIOT"), ("SEMY", "SOXX"), ("SMYY", "SMCI"),
    ("TMYY", "TSM"), ("TQQY", "QQQ"), ("TSYY", "TSLA"), ("XBTY", "IBIT"), ("YSPY", "SPY"),
}
_FALLBACK_FOF = {"YBTY", "YBST"}


def _load_raw() -> dict:
    if not TAXONOMY_FILE.exists():
        return {}
    try:
        return json.loads(TAXONOMY_FILE.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


@lru_cache(maxsize=1)
def load_taxonomy() -> dict:
    raw = _load_raw()
    vol = {str(s).upper() for s in (raw.get("volatility_etp_symbols") or _FALLBACK_VOL_ETP)}
    fof = {str(s).upper() for s in (raw.get("yieldboost_fof_symbols") or _FALLBACK_FOF)}
    pairs_raw = raw.get("yieldboost_income_pairs") or [
        [a, b] for a, b in sorted(_FALLBACK_YB_PAIRS)
    ]
    pairs = {
        (str(p[0]).upper(), str(p[1]).upper())
        for p in pairs_raw
        if isinstance(p, (list, tuple)) and len(p) >= 2
    }
    inverse = {
        str(s).upper()
        for s in (raw.get("bucket_3_inverse_symbols") or [])
    }
    return {
        "volatility_etp_symbols": vol,
        "yieldboost_fof_symbols": fof,
        "yieldboost_income_pairs": pairs,
        "bucket_3_inverse_symbols": inverse,
    }


def volatility_etp_symbols() -> set[str]:
    return set(load_taxonomy()["volatility_etp_symbols"])


def yieldboost_income_pairs() -> set[tuple[str, str]]:
    return set(load_taxonomy()["yieldboost_income_pairs"])


def yieldboost_fof_symbols() -> set[str]:
    return set(load_taxonomy()["yieldboost_fof_symbols"])


def export_for_spa() -> dict:
    tax = load_taxonomy()
    return {
        "volatility_etp_symbols": sorted(tax["volatility_etp_symbols"]),
        "yieldboost_income_pairs": [f"{a}|{b}" for a, b in sorted(tax["yieldboost_income_pairs"])],
        "yieldboost_fof_symbols": sorted(tax["yieldboost_fof_symbols"]),
        "source": str(TAXONOMY_FILE.relative_to(REPO_ROOT)).replace("\\", "/"),
    }


def write_spa_export(path: Path | None = None) -> Path:
    out = path or (REPO_ROOT / "data" / "product_taxonomy.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = export_for_spa()
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out

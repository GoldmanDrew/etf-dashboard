#!/usr/bin/env python3
"""Audit Defiance issuer routing vs metrics store.

Flags tickers that:
  * are in Defiance ``KNOWN_TICKERS`` / live catalog, but whose latest metrics row
    is not ``source_provider=defiance``
  * have Granite/REX static claims colliding with Defiance ``KNOWN_TICKERS``

Usage::

    python scripts/audit_defiance_issuer_routing.py
    python scripts/audit_defiance_issuer_routing.py --fail-on-misroute
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from etf_providers import DefianceProvider, GraniteSharesProvider, REXSharesProvider  # noqa: E402
from ingest_etf_metrics import CSV_PATH, PARQUET_PATH, load_universe_tickers  # noqa: E402

DEFAULT_REPORT = ROOT / "data" / "defiance_issuer_routing_report.json"


def _load_metrics() -> pd.DataFrame:
    if PARQUET_PATH.exists():
        df = pd.read_parquet(PARQUET_PATH)
    elif CSV_PATH.exists():
        df = pd.read_csv(CSV_PATH)
    else:
        return pd.DataFrame()
    if df.empty:
        return df
    out = df.copy()
    out["ticker"] = out["ticker"].astype(str).str.upper()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    return out


def build_report(*, probe_catalog: bool = True) -> dict:
    provider = DefianceProvider()
    catalog: set[str] = set()
    if probe_catalog:
        try:
            provider._load_catalog()
            catalog = set(provider._catalog or set())
        except Exception as exc:
            catalog = set()
            catalog_error = str(exc)
        else:
            catalog_error = None
    else:
        catalog_error = "skipped"

    supported = set(DefianceProvider.KNOWN_TICKERS) | catalog
    universe = set(load_universe_tickers())
    in_scope = sorted(supported & universe) if universe else sorted(supported)

    granite_collide = sorted(supported & set(GraniteSharesProvider.KNOWN_TICKERS))
    rex_collide = sorted(supported & set(REXSharesProvider.KNOWN_TICKERS))

    df = _load_metrics()
    misroutes: list[dict] = []
    ok_rows: list[dict] = []
    if not df.empty and in_scope:
        sub = df[df["ticker"].isin(in_scope)].copy()
        if not sub.empty:
            idx = sub.groupby("ticker")["date"].idxmax()
            latest = sub.loc[idx]
            for _, row in latest.iterrows():
                prov = str(row.get("source_provider") or "").lower()
                kind = str(row.get("stale_kind") or "").lower()
                sym = str(row["ticker"])
                rec = {
                    "ticker": sym,
                    "date": str(row["date"]),
                    "source_provider": prov,
                    "stale_kind": kind,
                    "nav": float(row["nav"]) if pd.notna(row.get("nav")) else None,
                    "in_known": sym in DefianceProvider.KNOWN_TICKERS,
                    "in_catalog": sym in catalog,
                }
                if prov == "defiance" and kind not in (
                    "market_backed_no_issuer_nav",
                    "carry_forward",
                ):
                    ok_rows.append(rec)
                else:
                    misroutes.append(rec)

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "catalog_n": len(catalog),
        "catalog_error": catalog_error,
        "known_n": len(DefianceProvider.KNOWN_TICKERS),
        "universe_supported_n": len(in_scope),
        "ok_n": len(ok_rows),
        "misroute_n": len(misroutes),
        "granite_collisions": granite_collide,
        "rex_collisions": rex_collide,
        "misroutes": misroutes,
        "ok": ok_rows,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--no-catalog-probe", action="store_true")
    parser.add_argument(
        "--fail-on-misroute",
        action="store_true",
        help="exit 1 when any supported universe ticker is not on defiance issuer",
    )
    parser.add_argument(
        "--fail-on-collision",
        action="store_true",
        help="exit 1 when Granite/REX KNOWN_TICKERS overlaps Defiance support",
    )
    args = parser.parse_args()

    report = build_report(probe_catalog=not args.no_catalog_probe)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(
        f"Defiance routing: supported_in_universe={report['universe_supported_n']} "
        f"ok={report['ok_n']} misroute={report['misroute_n']} "
        f"catalog={report['catalog_n']} known={report['known_n']}"
    )
    if report["granite_collisions"]:
        print("Granite collisions:", ",".join(report["granite_collisions"]))
    if report["rex_collisions"]:
        print("REX collisions:", ",".join(report["rex_collisions"]))
    if report["misroute_n"]:
        sample = ",".join(r["ticker"] for r in report["misroutes"][:20])
        print(f"Misroute sample: {sample}")
    print(f"Wrote {args.report}")

    rc = 0
    if args.fail_on_collision and (report["granite_collisions"] or report["rex_collisions"]):
        rc = 1
    if args.fail_on_misroute and report["misroute_n"] > 0:
        rc = 1
    return rc


if __name__ == "__main__":
    raise SystemExit(main())

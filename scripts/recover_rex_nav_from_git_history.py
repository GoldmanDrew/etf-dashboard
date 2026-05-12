#!/usr/bin/env python3
"""Recover historical REX/T-REX NAV from prior commits of ``etf_metrics_daily.json``.

When the Wayback Machine has no captures for a recent fund-page session, prior
runs of the ingest pipeline often *did* observe the issuer page on the next
calendar day with ``As of YYYY-MM-DD`` lagging by one session. Those rows were
stored at ``date == ingest_calendar`` but carried the true valuation session in
``source_url`` (``#as_of=YYYY-MM-DD``).

The migration in :mod:`migrate_etf_metrics_valuation_dates` already relabels /
merges *currently stored* rows. But ``collapse_redundant_consecutive_rows`` and
later overwrites can drop a session entirely if the next stale ingest produced
the same ``(nav, aum, shares)`` triple. This recovery walks **all commits** of
``data/etf_metrics_daily.json`` (and ``etf_metrics_latest.json``) on every
branch, extracts the issuer-published NAV for each ``(ticker, as_of_date)``,
and fills any session row missing from the current store.

Default is **dry-run** (writes a JSON report). Pass ``--apply`` to update the
parquet/csv/json store via :func:`ingest_etf_metrics.save_outputs` along with
the standard postprocess chain.

Examples::

    python3 scripts/recover_rex_nav_from_git_history.py --since 2026-04-01
    python3 scripts/recover_rex_nav_from_git_history.py --tickers EOSU,MSTU --apply
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import re
import subprocess
import sys
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
_SCRIPTS = _REPO / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from etf_providers import REXSharesProvider  # noqa: E402
from ingest_etf_metrics import (  # noqa: E402
    REQUIRED_COLUMNS,
    backfill_underlying_adj_close_gaps,
    collapse_redundant_consecutive_rows,
    enforce_status_consistency,
    fetch_close_prices_batch,
    fetch_underlying_adj_close_batch,
    load_existing,
    load_universe_underlying_map,
    merge_close_prices,
    merge_underlying_adj_close,
    repair_close_price_split_basis_mismatch,
    repair_close_price_vs_issuer_session,
    repair_shares_vs_aum_nav,
    save_outputs,
    validate_df,
)

LOGGER = logging.getLogger("recover_rex_nav_from_git_history")

DEFAULT_REX_TICKERS = sorted(REXSharesProvider.KNOWN_TICKERS)


def parse_as_of_from_url(url: str | None) -> date | None:
    if not url or not isinstance(url, str):
        return None
    for part in url.split("|"):
        m = re.search(r"#as_of=(\d{4}-\d{2}-\d{2})", part)
        if m:
            try:
                return date.fromisoformat(m.group(1))
            except ValueError:
                continue
    return None


def list_commits_touching(json_relpaths: list[str]) -> list[tuple[str, str]]:
    """Return ``[(sha, iso_committer_date), ...]`` over **all** branches and HEAD."""
    cmd = [
        "git", "log", "--all", "--pretty=%H|%cI",
        "--", *json_relpaths,
    ]
    out = subprocess.check_output(cmd, cwd=_REPO, text=True)
    commits: list[tuple[str, str]] = []
    seen: set[str] = set()
    for line in out.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        sha, iso = line.split("|", 1)
        if sha in seen:
            continue
        seen.add(sha)
        commits.append((sha, iso))
    return commits


def _git_show(sha: str, rel: str) -> str | None:
    try:
        return subprocess.check_output(
            ["git", "show", f"{sha}:{rel}"], cwd=_REPO, text=True,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        return None


def _safe_float(v: object) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def _is_rex_url(url: str | None) -> bool:
    return bool(url) and "rexshares.com" in str(url).lower()


def extract_session_table(
    commits: Iterable[tuple[str, str]],
    rel_paths: list[str],
    *,
    tickers_filter: set[str] | None,
    since: date | None,
    until: date | None,
) -> dict[tuple[str, date], dict]:
    """Build ``{(ticker, as_of_date): best_observation}`` from commit history.

    We treat the *latest commit's* observation as authoritative (issuer revisions
    occasionally happen — e.g. an explicit NAV row replaces an AUM/shares-implied
    one). When NAV is missing, AUM and shares are still preserved if positive.
    """
    out: dict[tuple[str, date], dict] = {}
    counts = defaultdict(int)
    # Walk newest -> oldest from `git log` order, but track "last seen" preference.
    # We invert: oldest -> newest, so newer overrides.
    for sha, iso in reversed(list(commits)):
        for rel in rel_paths:
            blob = _git_show(sha, rel)
            if blob is None:
                continue
            try:
                data = json.loads(blob)
            except Exception:
                continue
            rows = data.get("rows") or []
            for r in rows:
                t = str(r.get("ticker") or "").strip().upper()
                if not t:
                    continue
                if tickers_filter is not None and t not in tickers_filter:
                    continue
                url = r.get("source_url") or ""
                if not _is_rex_url(url):
                    continue
                vd = parse_as_of_from_url(url)
                if vd is None:
                    continue
                if since is not None and vd < since:
                    continue
                if until is not None and vd > until:
                    continue
                nav = _safe_float(r.get("nav"))
                aum = _safe_float(r.get("aum"))
                shares = _safe_float(r.get("shares_outstanding"))
                if not (nav and nav > 0) and not (aum and aum > 0) and not (
                    shares and shares > 0
                ):
                    continue
                key = (t, vd)
                prior = out.get(key)
                # Always store the latest observation, but keep prior NAV if newer
                # commit dropped to a fallback that lacks NAV (defensive).
                rec = {
                    "nav": nav,
                    "aum": aum,
                    "shares_outstanding": shares,
                    "source_url": f"https://www.rexshares.com/{t}/#as_of={vd.isoformat()}",
                    "from_sha": sha,
                    "from_commit_iso": iso,
                }
                if prior is None:
                    out[key] = rec
                else:
                    merged = dict(prior)
                    if rec["nav"] is not None and rec["nav"] > 0:
                        merged["nav"] = rec["nav"]
                    elif merged.get("nav") is None and rec["nav"]:
                        merged["nav"] = rec["nav"]
                    if rec["aum"] is not None and rec["aum"] > 0:
                        merged["aum"] = rec["aum"]
                    if rec["shares_outstanding"] is not None and rec["shares_outstanding"] > 0:
                        merged["shares_outstanding"] = rec["shares_outstanding"]
                    merged["from_sha"] = sha
                    merged["from_commit_iso"] = iso
                    out[key] = merged
                counts[t] += 1
    LOGGER.info(
        "Extracted %d (ticker,session) records from history; sample tickers=%s",
        len(out), sorted(counts.keys())[:8],
    )
    return out


def _row_template(t: str, d: date, rec: dict, *, ingested_iso: str) -> dict:
    """Build a fresh DataFrame row with REQUIRED_COLUMNS using historical values."""
    nav = rec.get("nav")
    aum = rec.get("aum")
    shares = rec.get("shares_outstanding")
    if (aum is None or aum <= 0) and nav and shares:
        aum = float(nav) * float(shares)
    if (shares is None or shares <= 0) and aum and nav and float(nav) > 0:
        shares = float(aum) / float(nav)
    if (nav is None or nav <= 0) and aum and shares and float(shares) > 0:
        nav = float(aum) / float(shares)
    has_nav = nav is not None and float(nav) > 0
    has_aum = aum is not None and float(aum) > 0
    has_sh = shares is not None and float(shares) > 0
    if has_nav and has_aum and has_sh:
        status = "ok"
    elif has_nav or has_aum or has_sh:
        status = "partial"
    else:
        status = "missing"
    return {
        "date": d,
        "ticker": t,
        "nav": nav,
        "aum": aum,
        "shares_outstanding": shares,
        "shares_traded": None,
        "close_price": None,
        "underlying_adj_close": None,
        "stale": False,
        "stale_age_bdays": None,
        "source_provider": "rex_shares_history",
        "source_url": rec["source_url"],
        "ingested_at_utc": ingested_iso,
        "status": status,
    }


def merge_history_into_store(
    df: pd.DataFrame,
    sessions: dict[tuple[str, date], dict],
) -> tuple[pd.DataFrame, list[dict]]:
    """Insert/update store rows with historically observed NAV/AUM/shares.

    Rules:
      - If no row exists for ``(ticker, session)`` → insert a new row.
      - If a row exists with NAV null/<=0 and history has NAV → fill it.
      - If row exists with NAV present but disagrees by > 0.5% from history's NAV
        and history is from a NEWER commit, prefer history (issuer revision).
      - aum/shares are filled when missing or grossly inconsistent.
    """
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    out["ticker"] = out["ticker"].astype(str).str.upper()
    report: list[dict] = []
    ingested_iso = datetime.now(timezone.utc).isoformat()

    new_rows: list[dict] = []
    for (t, d), rec in sorted(sessions.items()):
        m = (out["ticker"] == t) & (out["date"] == d)
        nav_h = rec.get("nav")
        aum_h = rec.get("aum")
        sh_h = rec.get("shares_outstanding")
        if not m.any():
            tmpl = _row_template(t, d, rec, ingested_iso=ingested_iso)
            new_rows.append(tmpl)
            report.append({
                "ticker": t, "session": d.isoformat(), "action": "insert_new_row",
                "nav": tmpl.get("nav"), "aum": tmpl.get("aum"),
                "shares_outstanding": tmpl.get("shares_outstanding"),
                "from_sha": rec.get("from_sha"),
            })
            continue

        idx = out.index[m][0]
        nav_cur = _safe_float(out.at[idx, "nav"])
        aum_cur = _safe_float(out.at[idx, "aum"])
        sh_cur = _safe_float(out.at[idx, "shares_outstanding"])
        changed = {}
        if (nav_cur is None or nav_cur <= 0) and nav_h and nav_h > 0:
            out.at[idx, "nav"] = nav_h
            changed["nav"] = {"from": nav_cur, "to": nav_h}
        elif (
            nav_cur and nav_h and nav_cur > 0 and nav_h > 0
            and abs(nav_cur - nav_h) / max(abs(nav_h), 1e-9) > 5e-3
        ):
            # Preserve store NAV by default (it may be a more recent migration result).
            # Only prefer history if store's source URL is missing / non-issuer.
            su = str(out.at[idx, "source_url"] or "")
            if not _is_rex_url(su):
                out.at[idx, "nav"] = nav_h
                changed["nav"] = {"from": nav_cur, "to": nav_h, "reason": "store_url_non_rex"}
        if (aum_cur is None or aum_cur <= 0) and aum_h and aum_h > 0:
            out.at[idx, "aum"] = aum_h
            changed["aum"] = {"from": aum_cur, "to": aum_h}
        if (sh_cur is None or sh_cur <= 0) and sh_h and sh_h > 0:
            out.at[idx, "shares_outstanding"] = sh_h
            changed["shares_outstanding"] = {"from": sh_cur, "to": sh_h}

        # Stamp the issuer URL when missing; leave provider alone when URL already
        # encodes the issuer attempt (``merged``, ``rex_shares``, ...).
        cur_url = str(out.at[idx, "source_url"] or "")
        cur_prov = str(out.at[idx, "source_provider"] or "")
        if not _is_rex_url(cur_url):
            out.at[idx, "source_url"] = rec["source_url"]
            changed["source_url"] = {"from": cur_url, "to": rec["source_url"]}
            if not cur_prov.startswith("rex") and cur_prov != "merged":
                out.at[idx, "source_provider"] = "rex_shares_history"
                changed["source_provider"] = {"from": cur_prov, "to": "rex_shares_history"}

        if changed:
            report.append({
                "ticker": t, "session": d.isoformat(),
                "action": "update_existing_row",
                "from_sha": rec.get("from_sha"),
                "changed": changed,
            })

    if new_rows:
        new_df = pd.DataFrame(new_rows, columns=REQUIRED_COLUMNS)
        out = pd.concat([out, new_df], ignore_index=True)

    out = out.sort_values(["date", "ticker"]).reset_index(drop=True)
    # Coerce ingested_at_utc to timestamps so concat with parquet-typed rows is clean.
    out["ingested_at_utc"] = pd.to_datetime(out["ingested_at_utc"], errors="coerce", utc=True)
    out = enforce_status_consistency(out)
    return out, report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--since", default=None,
        help="Only consider sessions / store rows >= YYYY-MM-DD.",
    )
    parser.add_argument(
        "--until", default=None,
        help="Only consider sessions <= YYYY-MM-DD (inclusive).",
    )
    parser.add_argument(
        "--tickers", default=None,
        help=("Comma-separated tickers. Default: REXSharesProvider.KNOWN_TICKERS."),
    )
    parser.add_argument("--apply", action="store_true",
                        help="Persist parquet/csv/json (default: dry-run).")
    parser.add_argument("--report", default=None,
                        help="Write JSON report to this path.")
    parser.add_argument(
        "--skip-yahoo-refresh", action="store_true",
        help="Skip the Yahoo close/volume refresh after merging history.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    since = date.fromisoformat(args.since) if args.since else None
    until = date.fromisoformat(args.until) if args.until else None

    if args.tickers:
        tickers_filter = {t.strip().upper() for t in args.tickers.split(",") if t.strip()}
    else:
        tickers_filter = set(DEFAULT_REX_TICKERS)

    rel_paths = ["data/etf_metrics_daily.json", "data/etf_metrics_latest.json"]
    commits = list_commits_touching(rel_paths)
    LOGGER.info("Found %d commits touching %s", len(commits), rel_paths)

    sessions = extract_session_table(
        commits, rel_paths,
        tickers_filter=tickers_filter, since=since, until=until,
    )

    df = load_existing()
    if df.empty:
        LOGGER.error("metrics store is empty; nothing to recover into")
        sys.exit(1)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date

    merged, report = merge_history_into_store(df, sessions)

    insert_count = sum(1 for r in report if r["action"] == "insert_new_row")
    update_count = sum(1 for r in report if r["action"] == "update_existing_row")

    summary = {
        "build_time": datetime.now(timezone.utc).isoformat(),
        "since": since.isoformat() if since else None,
        "until": until.isoformat() if until else None,
        "n_commits_scanned": len(commits),
        "n_sessions_observed": len(sessions),
        "inserts": insert_count,
        "updates": update_count,
        "tickers_filter_size": len(tickers_filter),
        "apply": bool(args.apply),
        "details": report,
    }
    print(json.dumps({k: v for k, v in summary.items() if k != "details"}, indent=2))
    if args.report:
        Path(args.report).parent.mkdir(parents=True, exist_ok=True)
        with open(args.report, "w") as f:
            json.dump(summary, f, indent=2)
            f.write("\n")
        LOGGER.info("Report -> %s", args.report)

    if not args.apply:
        LOGGER.info("Dry-run; not writing store. Re-run with --apply.")
        return

    if insert_count + update_count == 0:
        LOGGER.info("No changes to persist.")
        return

    underlying_map = load_universe_underlying_map()

    if not args.skip_yahoo_refresh:
        d_min = min(d for (_, d) in sessions.keys()) if sessions else None
        d_max = max(d for (_, d) in sessions.keys()) if sessions else None
        if d_min and d_max:
            tickers = sorted({t for (t, _) in sessions.keys()})
            close_df = fetch_close_prices_batch(tickers, d_min, d_max)
            if not close_df.empty:
                merged = merge_close_prices(merged, close_df)
                LOGGER.info("close refresh: %d rows", len(close_df))
            unds = sorted({
                str(underlying_map.get(t, "")).strip().upper()
                for t in tickers if underlying_map.get(t)
            })
            unds = [u for u in unds if u]
            if unds:
                und_df = fetch_underlying_adj_close_batch(unds, d_min, d_max)
                if not und_df.empty:
                    merged = merge_underlying_adj_close(merged, und_df, underlying_map)
                    LOGGER.info("underlying refresh: %d rows", len(und_df))

    merged, n_sess = repair_close_price_vs_issuer_session(merged, underlying_map)
    if n_sess:
        LOGGER.info("session-close repairs: %d", n_sess)
    merged, _ = repair_shares_vs_aum_nav(merged)
    merged, _ = repair_close_price_split_basis_mismatch(merged)
    merged, n_collapse = collapse_redundant_consecutive_rows(merged)
    if n_collapse:
        LOGGER.info("collapse_redundant_consecutive_rows dropped %d rows", n_collapse)
    merged = backfill_underlying_adj_close_gaps(merged, underlying_map)
    validate_df(merged)
    save_outputs(merged)
    LOGGER.info("Wrote outputs (inserts=%d updates=%d)", insert_count, update_count)


if __name__ == "__main__":
    main()

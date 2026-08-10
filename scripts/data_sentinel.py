#!/usr/bin/env python3
"""Deterministic data-health sentinel: validate published JSON artifacts before commit/deploy.

Three subcommands:

  gate   — validate exactly the files a CI tick wants to commit (market-hours.yml runs
           this between `ci_tick.py` and the commit-data action). BLOCK-severity
           findings drop that artifact from the commit list so corrupt payloads never
           reach main, while healthy artifacts (plus the quarantine manifest and this
           report) still commit — a gate failure must not become a data-loss event.
  sweep  — full scan of the published artifact surface plus cross-artifact checks the
           gate cannot do: market-calendar-aware staleness, universe coverage, provider
           mix drift, quarantine lifecycle (entries auto-clear after N clean sweeps),
           provider-health ledger under .github/data-memory/.
  alert  — file/update a GitHub issue (label ops/data-sentinel) from the latest report;
           auto-closes the issue once a later run passes.

Severity model:
  block      — artifact must not be committed/trusted (unparseable, NaN/Infinity tokens,
               schema break, catastrophic record-count regression, coverage collapse).
  quarantine — a specific ticker's data is suspect (zombie spot, uncorroborated >20%%
               move); the ticker goes into data/quarantine.json for the UI to gray out.
               Records are NEVER removed from dashboard_data.json: cross-sectional
               percentiles are computed server-side over the full universe, so dropping
               rows would silently shift every other ticker's scores.
  warn       — recorded in the report and the alert issue; never blocks.

Design constraints inherited from repo history (.github/data-memory/anti_patterns.md):
  * never rewrite data files in place (git history is the recovery store of last resort);
  * reuse the existing stale_kind taxonomy rather than inventing a parallel one;
  * exempt declared splits before flagging price jumps (reverse splits are routine here);
  * config is JSON because requirements.txt does not install PyYAML.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import statistics
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from market_calendar import is_nyse_session  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = REPO_ROOT / "config" / "sentinel.json"
DEFAULT_REPORT = REPO_ROOT / "data" / "sentinel_report.json"
DEFAULT_QUARANTINE = REPO_ROOT / "data" / "quarantine.json"
DEFAULT_LEDGER = REPO_ROOT / ".github" / "data-memory" / "provider_health.json"
UNIVERSE_CSV = REPO_ROOT / "data" / "etf_screened_today.csv"
ISSUE_LABEL = "ops/data-sentinel"

BLOCK = "block"
QUARANTINE = "quarantine"
WARN = "warn"

# RTH window mirrors config/ci.yaml rth.utc_hours; holiday-aware via market_calendar
# (an improvement over ci_tick.is_rth, which has no holiday calendar).
RTH_UTC_HOURS = range(13, 23)

# Providers in etf_metrics_health.latest_provider_counts that are NOT issuer feeds;
# drops in these are fallback churn, not issuer-adapter breakage.
NON_ISSUER_PROVIDERS = {
    "yfinance", "polygon", "market_backed", "carry_forward", "merged",
    "fof_child_synthetic",
}

# ---------------------------------------------------------------------------
# Artifact specs: which top-level keys must exist and where the record set lives.
# `records` is a dotted locator for the collection used in count-regression checks.
SPECS: dict[str, dict] = {
    "data/dashboard_data.json": {
        "required": ["build_time", "schema_v", "summary", "records"],
        "records": "records",
    },
    "data/borrow_history.json": {
        "required": ["symbols", "meta"],
        "records": "symbols",
    },
    "data/options_cache.json": {
        "required": ["build_time", "symbols"],
        "records": "symbols",
    },
    "data/vrp_live.json": {
        "required": ["build_time", "rows", "row_count"],
        "records": "rows",
    },
    "data/vrp_health.json": {"required": ["build_time"], "records": None},
    "data/nav_forecasts/_latest.json": {
        "required": ["anchor_date", "build_time", "by_symbol", "confidence_count"],
        "records": "by_symbol",
    },
    "data/underlying_intraday_spot.json": {
        "required": ["build_time", "by_symbol", "n_symbols_priced", "n_symbols_universe"],
        "records": "by_symbol",
    },
    "data/underlying_intraday_volume.json": {
        "required": ["build_time", "by_underlying"],
        "records": "by_underlying",
    },
    "data/letf_rebalance_flows_intraday_latest.json": {
        "required": ["build_time", "by_fund", "trading_date"],
        "records": "by_fund",
    },
    "data/corporate_actions.json": {
        "required": ["build_time", "events"],
        "records": "events",
    },
    "data/ci_state.json": {"required": [], "records": None},
}
GENERIC_SPEC = {"required": [], "records": None}

# Artifacts every sweep validates (the gate validates whatever the tick emits).
SWEEP_ARTIFACTS = [
    "data/dashboard_data.json",
    "data/borrow_history.json",
    "data/options_cache.json",
    "data/vrp_live.json",
    "data/vrp_health.json",
    "data/nav_forecasts/_latest.json",
    "data/underlying_intraday_spot.json",
    "data/underlying_intraday_volume.json",
    "data/letf_rebalance_flows_intraday_latest.json",
    "data/corporate_actions.json",
]


def utcnow() -> datetime:
    return datetime.now(UTC)


def iso_z(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def load_config(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def finding(severity: str, code: str, artifact: str, detail: str,
            ticker: str | None = None, observed=None, threshold=None) -> dict:
    out = {"severity": severity, "code": code, "artifact": artifact, "detail": detail}
    if ticker:
        out["ticker"] = ticker
    if observed is not None:
        out["observed"] = observed
    if threshold is not None:
        out["threshold"] = threshold
    return out


# ---------------------------------------------------------------------------
# Time helpers

def parse_ts(value) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        txt = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(txt)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    except ValueError:
        return None


def market_age_hours(ts: datetime, now: datetime | None = None) -> float:
    """Age in hours excluding full non-session (weekend/holiday) days in between.

    Same idea as freshness_diagnostics._market_age_minutes: a Friday-evening
    build_time should not look 60h old on Monday morning.
    """
    now = now or utcnow()
    if ts >= now:
        return 0.0
    raw_hours = (now - ts).total_seconds() / 3600.0
    d = ts.date() + timedelta(days=1)
    skipped = 0
    while d < now.date():
        if not is_nyse_session(d):
            skipped += 1
        d += timedelta(days=1)
    return max(0.0, raw_hours - 24.0 * skipped)


def is_market_hours(now: datetime | None = None) -> bool:
    now = now or utcnow()
    return is_nyse_session(now.date()) and now.hour in RTH_UTC_HOURS


# ---------------------------------------------------------------------------
# Baselines (previous committed version of a file, via git)

def git_baseline(rel_path: str) -> bytes | None:
    """Bytes of the file at HEAD, or None when unavailable (new file, no git)."""
    try:
        proc = subprocess.run(
            ["git", "show", f"HEAD:{rel_path.replace(os.sep, '/')}"],
            cwd=REPO_ROOT, capture_output=True, timeout=60,
        )
    except Exception:
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout


def record_count(payload, spec: dict) -> int | None:
    locator = spec.get("records")
    if not locator or not isinstance(payload, dict):
        return None
    node = payload
    for part in locator.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    if isinstance(node, (list, dict)):
        return len(node)
    return None


# ---------------------------------------------------------------------------
# Per-file integrity checks

def _reject_constant(name: str):
    raise ValueError(f"non-finite JSON token {name!r}")


def load_strict(path: Path):
    """Parse with browser semantics: bare NaN/Infinity tokens are a hard error.

    Python's json module accepts them by default; browsers' JSON.parse does not —
    a single NaN token once blanked the whole dashboard (see anti_patterns.md #6,
    and note build_data writes options_cache.json without allow_nan=False).
    """
    text = path.read_text(encoding="utf-8")
    return json.loads(text, parse_constant=_reject_constant)


def check_file_integrity(rel_path: str, cfg: dict, *,
                         baseline_bytes: bytes | None = "unset") -> tuple[list[dict], dict | None]:
    """Parse + schema + size/count/build_time regression for one artifact.

    Returns (findings, parsed payload or None). `baseline_bytes` is injectable for
    tests; the sentinel default resolves it from `git show HEAD:<path>`.
    """
    findings: list[dict] = []
    path = REPO_ROOT / rel_path
    spec = SPECS.get(rel_path, GENERIC_SPEC)

    if not path.exists():
        findings.append(finding(BLOCK, "missing_artifact", rel_path, "expected artifact missing from disk"))
        return findings, None
    if path.stat().st_size == 0:
        findings.append(finding(BLOCK, "empty_artifact", rel_path, "artifact is zero bytes"))
        return findings, None

    try:
        payload = load_strict(path)
    except Exception as exc:
        findings.append(finding(
            BLOCK, "parse_error", rel_path,
            f"not browser-parseable JSON: {exc}"))
        return findings, None

    missing = [k for k in spec.get("required", []) if not (isinstance(payload, dict) and k in payload)]
    if missing:
        findings.append(finding(
            BLOCK, "schema_missing_keys", rel_path,
            f"missing required top-level keys: {', '.join(missing)}", observed=missing))
        return findings, payload

    if baseline_bytes == "unset":
        baseline_bytes = git_baseline(rel_path)
    if baseline_bytes:
        reg = cfg.get("size_regression", {})
        new_bytes = path.stat().st_size
        old_bytes = len(baseline_bytes)
        max_byte_drop = float(reg.get("max_byte_drop_frac", 0.6))
        if old_bytes > 4096 and new_bytes < old_bytes * (1.0 - max_byte_drop):
            findings.append(finding(
                BLOCK, "byte_size_regression", rel_path,
                f"artifact shrank {old_bytes:,}B -> {new_bytes:,}B (>{max_byte_drop:.0%} drop)",
                observed=new_bytes, threshold=int(old_bytes * (1.0 - max_byte_drop))))
        try:
            old_payload = json.loads(baseline_bytes.decode("utf-8"))
        except Exception:
            old_payload = None
        if old_payload is not None:
            old_n = record_count(old_payload, spec)
            new_n = record_count(payload, spec)
            max_rec_drop = float(reg.get("max_record_drop_frac", 0.5))
            if old_n and new_n is not None and old_n >= 10 and new_n < old_n * (1.0 - max_rec_drop):
                findings.append(finding(
                    BLOCK, "record_count_regression", rel_path,
                    f"record count fell {old_n} -> {new_n} (>{max_rec_drop:.0%} drop); "
                    "refusing to overwrite deeper history (generalizes the borrow depth guard)",
                    observed=new_n, threshold=int(old_n * (1.0 - max_rec_drop))))
            old_bt = parse_ts(old_payload.get("build_time")) if isinstance(old_payload, dict) else None
            new_bt = parse_ts(payload.get("build_time")) if isinstance(payload, dict) else None
            if old_bt and new_bt and new_bt < old_bt - timedelta(minutes=1):
                findings.append(finding(
                    BLOCK, "build_time_regression", rel_path,
                    f"build_time moved backwards {iso_z(old_bt)} -> {iso_z(new_bt)} "
                    "(would replace fresher committed data with staler data)"))

    if isinstance(payload, dict):
        bt = parse_ts(payload.get("build_time"))
        if bt and bt > utcnow() + timedelta(minutes=15):
            findings.append(finding(WARN, "build_time_future", rel_path,
                                    f"build_time {iso_z(bt)} is in the future"))
    return findings, payload


# ---------------------------------------------------------------------------
# Cross-artifact context

def load_context() -> dict:
    """Shared lookups for anomaly checks; every piece is optional/best-effort."""
    ctx: dict = {"splits": {}, "metrics_close": {}, "metrics_date": {}, "delta": {}, "universe_count": None}

    try:
        ca = json.loads((REPO_ROOT / "data" / "corporate_actions.json").read_text(encoding="utf-8"))
        for ev in ca.get("events", []) or []:
            if ev.get("type") not in ("split", "reverse_split", "forward_split"):
                continue
            tkr = str(ev.get("ticker") or "").upper()
            when = ev.get("execution_date")
            mult = None
            try:
                a = float(ev.get("ratio_from") or 0)
                b = float(ev.get("ratio_to") or 0)
                if a > 0 and b > 0:
                    mult = max(a, b) / min(a, b)
            except (TypeError, ValueError):
                pass
            if tkr and when:
                ctx["splits"].setdefault(tkr, []).append({"date": str(when), "mult": mult})
    except Exception:
        pass

    try:
        latest = json.loads((REPO_ROOT / "data" / "etf_metrics_latest.json").read_text(encoding="utf-8"))
        for sym, row in (latest.get("by_symbol") or {}).items():
            close = row.get("close_price")
            if isinstance(close, (int, float)) and close > 0:
                ctx["metrics_close"][sym.upper()] = float(close)
                ctx["metrics_date"][sym.upper()] = row.get("date")
    except Exception:
        pass

    try:
        dash = json.loads((REPO_ROOT / "data" / "dashboard_data.json").read_text(encoding="utf-8"))
        for rec in dash.get("records", []) or []:
            sym = str(rec.get("symbol") or "").upper()
            delta = rec.get("delta")
            und = rec.get("underlying")
            if sym and isinstance(delta, (int, float)):
                ctx["delta"][sym] = (float(delta), str(und or "").upper())
    except Exception:
        pass

    try:
        with open(UNIVERSE_CSV, encoding="utf-8", errors="replace") as fh:
            n = sum(1 for _ in fh) - 1
        ctx["universe_count"] = max(0, n)
    except Exception:
        pass
    return ctx


def _split_events(ctx: dict, symbol: str) -> list[dict]:
    out = []
    for ev in ctx["splits"].get(symbol.upper(), []):
        if isinstance(ev, str):  # tolerate legacy [date, ...] shape
            ev = {"date": ev, "mult": None}
        out.append(ev)
    return out


def has_recent_split(ctx: dict, symbol: str, days: int, today: date | None = None) -> bool:
    today = today or utcnow().date()
    for ev in _split_events(ctx, symbol):
        try:
            d = date.fromisoformat(str(ev.get("date"))[:10])
        except ValueError:
            continue
        if abs((today - d).days) <= days:
            return True
    return False


def matching_split_multiple(ctx: dict, symbol: str, ratio: float, tol: float = 0.18) -> dict | None:
    """Declared split whose multiple explains an observed price ratio (either direction).

    The 18%% tolerance mirrors split_adjustments.py's declared-ratio trust window.
    Deliberately NO date window: basis bugs persist weeks after execution (KORU/MUU/
    SNXX prior_close was still pre-split three weeks after their splits).
    """
    if not (isinstance(ratio, (int, float)) and ratio > 0):
        return None
    for ev in _split_events(ctx, symbol):
        m = ev.get("mult")
        if not (isinstance(m, (int, float)) and m > 1.0):
            continue
        if abs(ratio / m - 1.0) <= tol or abs(ratio * m - 1.0) <= tol:
            return ev
    return None


# ---------------------------------------------------------------------------
# Anomaly checks

def check_spot_anomalies(payload: dict, ctx: dict, cfg: dict, *, now: datetime | None = None) -> list[dict]:
    """Statistical outlier + zombie-spot detection on underlying_intraday_spot.json."""
    findings: list[dict] = []
    rel = "data/underlying_intraday_spot.json"
    now = now or utcnow()
    ro = cfg.get("return_outlier", {})
    zs = cfg.get("zombie_spot", {})
    abs_max = float(ro.get("abs_return_max", 0.20))
    z_max = float(ro.get("mad_z_max", 6.0))
    breaker = float(ro.get("market_event_breaker_frac", 0.10))
    lev_tol = float(ro.get("leverage_tolerance", 0.10))
    split_days = int(ro.get("split_exemption_days", 5))
    zombie_ratio = float(zs.get("max_ratio", 2.5))
    metrics_lag_days = int(zs.get("max_metrics_lag_calendar_days", 7))

    by_symbol = payload.get("by_symbol") or {}
    by_underlying = payload.get("by_underlying") or {}
    # Split exemptions are judged against the artifact's own trading day, not sweep
    # time — a sweep over a days-old file must not un-exempt a split-day move.
    file_dt = parse_ts(payload.get("build_time"))
    ref_date = (file_dt or now).date()
    # underlying -> [(levered fund symbol, delta)] for mutual corroboration.
    funds_on: dict[str, list[tuple[str, float]]] = {}
    for sym, (delta, und) in ctx["delta"].items():
        if und:
            funds_on.setdefault(und, []).append((sym, delta))
    returns: dict[str, float] = {}
    for sym, ent in by_symbol.items():
        r = (ent or {}).get("return_d1_so_far")
        if isinstance(r, (int, float)) and math.isfinite(r):
            returns[sym.upper()] = float(r)

    # Market-event circuit breaker: when a large slice of the fleet moves hard,
    # that is the market, not a data artifact — flagging would be noise.
    big = [s for s, r in returns.items() if abs(r) > abs_max]
    if returns and len(big) / len(returns) > breaker:
        findings.append(finding(
            WARN, "market_event_breaker", rel,
            f"{len(big)}/{len(returns)} symbols moved >|{abs_max:.0%}| — cross-market event, "
            "outlier check suppressed this run"))
    elif len(returns) >= 20:
        med = statistics.median(returns.values())
        mad = statistics.median(abs(r - med) for r in returns.values()) or 1e-9
        for sym in big:
            r = returns[sym]
            z = abs(r - med) / (1.4826 * mad)
            if z <= z_max:
                continue
            if has_recent_split(ctx, sym, split_days, ref_date):
                continue
            # Split-basis diagnosis: when 1+return matches a declared split multiple,
            # prior_close is being served on the wrong basis (anti_patterns.md #1) —
            # a sharper verdict than "bad quote", and it routes to metrics repair,
            # not a spot re-fetch.
            ev = matching_split_multiple(ctx, sym, 1.0 + r)
            if ev:
                findings.append(finding(
                    QUARANTINE, "split_basis_prior_close", rel,
                    f"{sym} intraday return {r:+.1%} matches its declared "
                    f"{ev.get('mult'):.4g}x split of {ev.get('date')} — prior_close is on "
                    "the pre-split basis; return/flow math for this ticker is corrupt",
                    ticker=sym, observed=round(r, 4)))
                continue
            # Leverage corroboration: a 2x ETF moving ~2x its underlying is real.
            pair = ctx["delta"].get(sym)
            if pair:
                delta, und = pair
                und_ent = by_underlying.get(und) or by_symbol.get(und) or {}
                und_r = und_ent.get("return_d1_so_far")
                if isinstance(und_r, (int, float)) and math.isfinite(und_r):
                    if abs(r - delta * float(und_r)) <= lev_tol:
                        continue
            # Mutual corroboration for underlyings: a big move on a stock that its
            # levered funds confirm (fund_ret ~= delta * stock_ret) is a real market
            # move (earnings day), not a data artifact.
            corroborated = False
            for fund_sym, delta in funds_on.get(sym, []):
                f_r = returns.get(fund_sym.upper())
                if f_r is not None and abs(f_r - delta * r) <= lev_tol:
                    corroborated = True
                    break
            if corroborated:
                continue
            findings.append(finding(
                QUARANTINE, "return_outlier", rel,
                f"{sym} moved {r:+.1%} intraday (MAD z={z:.1f}) with no declared split and no "
                f"leverage corroboration — suspected bad quote",
                ticker=sym, observed=round(r, 4), threshold=abs_max))

    # Zombie spot: live spot wildly off the last known metrics close (NBIZ-class,
    # anti_patterns.md #5) without a declared split explaining the level change.
    for sym, ent in by_symbol.items():
        sym_u = sym.upper()
        last = (ent or {}).get("last")
        base = ctx["metrics_close"].get(sym_u)
        if not (isinstance(last, (int, float)) and last > 0 and base):
            continue
        mdate = ctx["metrics_date"].get(sym_u)
        try:
            if mdate and (now.date() - date.fromisoformat(str(mdate)[:10])).days > metrics_lag_days:
                continue  # metrics too stale to be a fair baseline
        except ValueError:
            pass
        ratio = float(last) / base
        if (ratio > zombie_ratio or ratio < 1.0 / zombie_ratio) and not has_recent_split(ctx, sym_u, split_days, ref_date):
            ev = matching_split_multiple(ctx, sym_u, ratio)
            if ev:
                findings.append(finding(
                    QUARANTINE, "split_basis_metrics_close", rel,
                    f"{sym_u} spot {last} vs metrics close {base} (x{ratio:.2f}) matches its "
                    f"declared {ev.get('mult'):.4g}x split of {ev.get('date')} — the metrics "
                    "store still carries the pre-split basis for this ticker",
                    ticker=sym_u, observed=round(ratio, 3)))
            else:
                findings.append(finding(
                    QUARANTINE, "zombie_spot", rel,
                    f"{sym_u} spot {last} vs last metrics close {base} (x{ratio:.2f}) with no declared "
                    "split — suspected stale/zombie quote",
                    ticker=sym_u, observed=round(ratio, 3), threshold=zombie_ratio))
    return findings


def check_dashboard(payload: dict, ctx: dict, cfg: dict) -> list[dict]:
    findings: list[dict] = []
    rel = "data/dashboard_data.json"
    cov = cfg.get("coverage", {})
    records = payload.get("records") or []
    summary = payload.get("summary") or {}

    # WARN, not BLOCK: the screener legitimately lists symbols the builder has not
    # onboarded yet (healthy steady state observed at ~94.8%). Catastrophic drops
    # are BLOCKed by record_count_regression vs HEAD, which is the true signal.
    uni = ctx.get("universe_count")
    min_frac = float(cov.get("dashboard_min_universe_frac", 0.90))
    if uni and uni >= 50:
        frac = len(records) / uni
        if frac < min_frac:
            findings.append(finding(
                WARN, "universe_coverage_drop", rel,
                f"records {len(records)} cover only {frac:.1%} of the {uni}-symbol screener universe",
                observed=len(records), threshold=int(uni * min_frac)))

    pct_missing = summary.get("pct_missing")
    max_missing = float(cov.get("dashboard_max_pct_missing", 20.0))
    if isinstance(pct_missing, (int, float)) and pct_missing > max_missing:
        findings.append(finding(
            WARN, "pct_missing_high", rel,
            f"summary.pct_missing {pct_missing} exceeds {max_missing}",
            observed=pct_missing, threshold=max_missing))

    if payload.get("ibkr_ftp_success") is True:
        floor = int(cov.get("ibkr_min_symbols_when_success", 5000))
        got = payload.get("ibkr_symbols_fetched") or 0
        if isinstance(got, int) and got < floor:
            findings.append(finding(
                WARN, "ibkr_partial_file", rel,
                f"IBKR FTP reported success but only {got} symbols parsed (< {floor}) — "
                "possible truncated usa.txt",
                observed=got, threshold=floor))

    seen: set[str] = set()
    dupes: set[str] = set()
    null_critical = 0
    for rec in records:
        sym = str(rec.get("symbol") or "").upper()
        if not sym:
            null_critical += 1
            continue
        if sym in seen:
            dupes.add(sym)
        seen.add(sym)
        if not rec.get("borrow_missing") and rec.get("borrow_current") is not None:
            bc = rec.get("borrow_current")
            if isinstance(bc, (int, float)) and bc < 0:
                findings.append(finding(
                    QUARANTINE, "negative_borrow", rel,
                    f"{sym} borrow_current {bc} is negative", ticker=sym, observed=bc))
    if dupes:
        findings.append(finding(
            WARN, "duplicate_symbols", rel,
            f"{len(dupes)} duplicate record symbol(s): {', '.join(sorted(dupes)[:10])}"))
    fleet_frac = float(cfg.get("critical_null", {}).get("fleet_block_frac", 0.20))
    if records and null_critical / len(records) > fleet_frac:
        findings.append(finding(
            BLOCK, "fleet_null_symbols", rel,
            f"{null_critical}/{len(records)} records missing symbol — malformed build"))
    return findings


def check_nav(payload: dict, cfg: dict) -> list[dict]:
    findings: list[dict] = []
    rel = "data/nav_forecasts/_latest.json"
    cov = cfg.get("coverage", {})
    counts = payload.get("confidence_count") or {}
    total = sum(v for v in counts.values() if isinstance(v, int)) or 0
    na = counts.get("na") or 0
    if total >= 50:
        frac = na / total
        warn_at = float(cov.get("nav_max_na_frac_warn", 0.60))
        block_at = float(cov.get("nav_max_na_frac_block", 0.85))
        if frac > block_at:
            findings.append(finding(
                BLOCK, "nav_na_collapse", rel,
                f"{na}/{total} forecasts have confidence=na — upstream spot/metrics feed outage",
                observed=round(frac, 3), threshold=block_at))
        elif frac > warn_at:
            findings.append(finding(
                WARN, "nav_na_elevated", rel,
                f"{na}/{total} forecasts have confidence=na",
                observed=round(frac, 3), threshold=warn_at))
    return findings


def check_vrp(payload: dict, cfg: dict, *, now: datetime | None = None) -> list[dict]:
    findings: list[dict] = []
    rel = "data/vrp_live.json"
    now = now or utcnow()
    rows = payload.get("rows") or []
    row_count = payload.get("row_count")
    if isinstance(row_count, int) and row_count != len(rows):
        findings.append(finding(
            WARN, "vrp_row_count_mismatch", rel,
            f"row_count={row_count} but rows has {len(rows)} entries"))
    expired_actionable = []
    for row in rows:
        exp = row.get("expiry")
        try:
            exp_d = date.fromisoformat(str(exp)[:10]) if exp else None
        except ValueError:
            exp_d = None
        if exp_d and exp_d < now.date() and row.get("actionable") is True:
            expired_actionable.append(str(row.get("yb_etf") or "?"))
    if expired_actionable:
        findings.append(finding(
            WARN, "vrp_expired_actionable", rel,
            f"{len(expired_actionable)} actionable row(s) reference expired spreads: "
            f"{', '.join(expired_actionable[:8])}"))
    return findings


def check_spot_coverage(payload: dict, cfg: dict, *, rth: bool) -> list[dict]:
    findings: list[dict] = []
    rel = "data/underlying_intraday_spot.json"
    cov = cfg.get("coverage", {})
    priced = payload.get("n_symbols_priced") or 0
    universe = payload.get("n_symbols_universe") or 0
    min_frac = float(cov.get("spot_min_priced_frac_rth", 0.90))
    if universe >= 50 and priced / universe < min_frac:
        sev = QUARANTINE if rth else WARN
        findings.append(finding(
            sev, "spot_coverage_drop", rel,
            f"only {priced}/{universe} symbols priced ({priced / universe:.1%})",
            observed=priced, threshold=int(universe * min_frac)))
    return findings


def check_staleness(cfg: dict, *, now: datetime | None = None) -> list[dict]:
    """Sweep-only: age gates on the artifacts freshness_diagnostics does not cover."""
    findings: list[dict] = []
    now = now or utcnow()
    rth = is_market_hours(now)
    rth_only = set(cfg.get("staleness_rth_only", []))
    for rel, max_hours in (cfg.get("staleness_market_hours_warn") or {}).items():
        if rel in rth_only and not rth:
            continue
        path = REPO_ROOT / rel
        if not path.exists():
            continue  # missing artifacts are reported by integrity checks
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue  # parse failures are reported by integrity checks
        bt = parse_ts(payload.get("build_time") if isinstance(payload, dict) else None)
        if bt is None and isinstance(payload, dict):
            bt = parse_ts(((payload.get("meta") or {}).get("build_time")))
        if bt is None:
            continue
        age = market_age_hours(bt, now)
        if age > float(max_hours):
            findings.append(finding(
                WARN, "artifact_stale", rel,
                f"build_time {iso_z(bt)} is {age:.1f} market-hours old (max {max_hours}h)",
                observed=round(age, 1), threshold=max_hours))
    return findings


# ---------------------------------------------------------------------------
# Provider health ledger + drift

def build_provider_snapshot(now: datetime | None = None) -> dict:
    now = now or utcnow()
    snap: dict = {"date": now.date().isoformat(), "ts": iso_z(now)}

    def _load(rel: str) -> dict:
        try:
            data = json.loads((REPO_ROOT / rel).read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    health = _load("data/etf_metrics_health.json")
    snap["metrics_providers"] = health.get("latest_provider_counts") or {}
    snap["metrics_status"] = {
        "ok": health.get("latest_ok"), "partial": health.get("latest_partial"),
        "missing": health.get("latest_missing"),
    }
    spot = _load("data/underlying_intraday_spot.json")
    snap["spot_sources"] = spot.get("sources") or {}
    dash = _load("data/dashboard_data.json")
    snap["ibkr"] = {
        "success": dash.get("ibkr_ftp_success"),
        "symbols": dash.get("ibkr_symbols_fetched"),
    }
    oc = _load("data/options_cache.json")
    snap["options"] = {
        "symbols_count": oc.get("symbols_count") or (len(oc.get("symbols") or {}) or None),
        "errors": len(oc.get("errors") or []),
        "polygon_configured": oc.get("polygon_api_configured"),
        "tradier_configured": oc.get("tradier_api_configured"),
    }
    vh = _load("data/vrp_health.json")
    snap["vrp_iv_coverage_front_pct"] = vh.get("iv_coverage_front_pct")
    return snap


def update_provider_ledger(ledger_path: Path, snap: dict, cfg: dict, *,
                           dry_run: bool = False) -> list[dict]:
    findings: list[dict] = []
    pd_cfg = cfg.get("provider_drift", {})
    try:
        ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    except Exception:
        ledger = {"schema_v": 1, "entries": []}
    entries = ledger.get("entries") or []

    # Drift baseline must be the most recent PRIOR-date entry: same-day sweeps
    # overwrite entries[-1], and comparing today's snapshot against itself would
    # erase a drift finding on the very next sweep of the day (verify included).
    prev = next((e for e in reversed(entries) if e.get("date") != snap.get("date")), None)
    if prev:
        min_prev = int(pd_cfg.get("min_prev_count", 5))
        max_drop = float(pd_cfg.get("max_drop_frac", 0.5))
        prev_counts = prev.get("metrics_providers") or {}
        new_counts = snap.get("metrics_providers") or {}
        for provider, old_n in prev_counts.items():
            if provider in NON_ISSUER_PROVIDERS or not isinstance(old_n, int) or old_n < min_prev:
                continue
            new_n = new_counts.get(provider, 0) or 0
            if new_n < old_n * (1.0 - max_drop):
                findings.append(finding(
                    WARN, "provider_drift", "data/etf_metrics_health.json",
                    f"issuer provider '{provider}' covered {old_n} tickers previously but only "
                    f"{new_n} now — possible issuer-site drift silently rerouting to market "
                    "fallback (Defiance-class incident, anti_patterns.md #7)",
                    observed=new_n, threshold=int(old_n * (1.0 - max_drop))))
        if snap.get("ibkr", {}).get("success") is False and (prev.get("ibkr") or {}).get("success") is False:
            findings.append(finding(
                WARN, "ibkr_down_repeat", "data/dashboard_data.json",
                "IBKR shortstock FTP failed on consecutive sweeps — borrow data riding the "
                "screener-CSV fallback"))

    if entries and entries[-1].get("date") == snap.get("date"):
        entries[-1] = snap
    else:
        entries.append(snap)
    cap = int(pd_cfg.get("ledger_max_entries", 120))
    ledger["entries"] = entries[-cap:]
    ledger["schema_v"] = 1
    ledger["updated_at"] = snap["ts"]
    if not dry_run:
        ledger_path.parent.mkdir(parents=True, exist_ok=True)
        ledger_path.write_text(json.dumps(ledger, indent=2) + "\n", encoding="utf-8")
    return findings


# ---------------------------------------------------------------------------
# Quarantine manifest lifecycle

def load_quarantine(path: Path) -> dict:
    try:
        q = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(q, dict):
            q.setdefault("tickers", {})
            q.setdefault("artifacts", {})
            return q
    except Exception:
        pass
    return {"schema_v": 1, "tickers": {}, "artifacts": {}}


def apply_quarantine(manifest: dict, findings: list[dict], cfg: dict, *,
                     now: datetime | None = None, full_sweep: bool) -> dict:
    """Merge new findings into the manifest.

    Ticker entries auto-clear after `recovery_sweeps` consecutive full sweeps
    without a re-flag; gate runs only add/refresh entries (they see one task's
    files, so absence of a flag there is not evidence of health).
    """
    now = now or utcnow()
    ts = iso_z(now)
    q_cfg = cfg.get("quarantine", {})
    recovery = int(q_cfg.get("recovery_sweeps", 2))
    max_entries = int(q_cfg.get("max_entries", 200))

    flagged_tickers: dict[str, list[dict]] = {}
    flagged_artifacts: dict[str, list[dict]] = {}
    for f in findings:
        if f["severity"] == QUARANTINE and f.get("ticker"):
            flagged_tickers.setdefault(f["ticker"], []).append(f)
        elif f["severity"] == QUARANTINE:
            flagged_artifacts.setdefault(f["artifact"], []).append(f)
        elif f["severity"] == BLOCK and full_sweep:
            # A committed artifact failing integrity in a sweep can't be un-committed;
            # hold it so the UI can badge the affected panel.
            flagged_artifacts.setdefault(f["artifact"], []).append(f)

    def _merge(store: dict, flagged: dict[str, list[dict]]):
        for key, fs in flagged.items():
            ent = store.get(key) or {"first_seen": ts, "clean_streak": 0, "reasons": []}
            ent["last_seen"] = ts
            ent["clean_streak"] = 0
            ent["reasons"] = [
                {"code": f["code"], "artifact": f["artifact"], "detail": f["detail"],
                 "observed": f.get("observed")}
                for f in fs
            ]
            store[key] = ent
        if full_sweep:
            for key in list(store.keys()):
                if key in flagged:
                    continue
                ent = store[key]
                ent["clean_streak"] = int(ent.get("clean_streak", 0)) + 1
                if ent["clean_streak"] >= recovery:
                    del store[key]

    _merge(manifest["tickers"], flagged_tickers)
    _merge(manifest["artifacts"], flagged_artifacts)

    if len(manifest["tickers"]) > max_entries:
        keep = sorted(manifest["tickers"].items(), key=lambda kv: kv[1].get("last_seen", ""), reverse=True)
        manifest["tickers"] = dict(keep[:max_entries])

    manifest["schema_v"] = 1
    manifest["build_time"] = ts
    manifest["generated_by"] = "scripts/data_sentinel.py"
    manifest["note"] = (
        "Per-ticker/per-artifact data-quality holds. The UI badges/grays these; records are "
        "never removed from dashboard_data.json so cross-sectional scores stay stable. "
        f"Entries auto-clear after {recovery} clean sweeps."
    )
    return manifest


# ---------------------------------------------------------------------------
# Verdict / report / outputs

def verdict_of(findings: list[dict]) -> str:
    if any(f["severity"] == BLOCK for f in findings):
        return "block"
    if any(f["severity"] == QUARANTINE for f in findings):
        return "quarantine"
    if findings:
        return "warn"
    return "pass"


def write_report(path: Path, *, mode: str, task: str | None, findings: list[dict],
                 checked: list[str], dry_run: bool) -> dict:
    report = {
        "schema_v": 1,
        "build_time": iso_z(utcnow()),
        "mode": mode,
        "task": task,
        "verdict": verdict_of(findings),
        "stats": {
            "checked_files": len(checked),
            "block": sum(1 for f in findings if f["severity"] == BLOCK),
            "quarantine": sum(1 for f in findings if f["severity"] == QUARANTINE),
            "warn": sum(1 for f in findings if f["severity"] == WARN),
        },
        "checked": checked,
        "findings": findings,
    }
    if not dry_run:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def write_github_output(pairs: dict[str, str]) -> None:
    out = os.environ.get("GITHUB_OUTPUT")
    if not out:
        return
    with open(out, "a", encoding="utf-8") as fh:
        for k, v in pairs.items():
            fh.write(f"{k}={v}\n")


# ---------------------------------------------------------------------------
# Modes

def run_checks_for_files(rel_files: list[str], cfg: dict, ctx: dict, *,
                         now: datetime | None = None) -> tuple[list[dict], dict[str, object]]:
    """Integrity + content checks for a set of artifacts; returns (findings, payloads)."""
    now = now or utcnow()
    findings: list[dict] = []
    payloads: dict[str, object] = {}
    for rel in rel_files:
        fs, payload = check_file_integrity(rel, cfg)
        findings.extend(fs)
        if payload is not None:
            payloads[rel] = payload

    rth = is_market_hours(now)
    for rel, payload in payloads.items():
        if not isinstance(payload, dict):
            continue
        if rel == "data/underlying_intraday_spot.json":
            findings.extend(check_spot_anomalies(payload, ctx, cfg, now=now))
            findings.extend(check_spot_coverage(payload, cfg, rth=rth))
        elif rel == "data/dashboard_data.json":
            findings.extend(check_dashboard(payload, ctx, cfg))
        elif rel == "data/nav_forecasts/_latest.json":
            findings.extend(check_nav(payload, cfg))
        elif rel == "data/vrp_live.json":
            findings.extend(check_vrp(payload, cfg, now=now))
        elif rel == "data/underlying_intraday_volume.json" and rth:
            n_vol = payload.get("n_underlyings_with_volume")
            if isinstance(n_vol, int) and n_vol == 0:
                findings.append(finding(
                    WARN, "volume_feed_dead", rel,
                    "n_underlyings_with_volume=0 during market hours — intraday volume path dead"))
    return findings, payloads


def cmd_gate(args, cfg: dict) -> int:
    rel_files = [f.strip().replace("\\", "/") for f in (args.files or "").split() if f.strip()]
    mode = os.environ.get("SENTINEL_MODE", "enforce").lower()
    ctx = load_context()
    findings, _ = run_checks_for_files(rel_files, cfg, ctx)
    verdict = verdict_of(findings)

    blocked = sorted({f["artifact"] for f in findings if f["severity"] == BLOCK})
    if mode == "report":
        kept = list(rel_files)
    else:
        kept = [f for f in rel_files if f not in blocked]

    manifest = load_quarantine(args.quarantine)
    manifest = apply_quarantine(manifest, findings, cfg, full_sweep=False)
    if not args.dry_run:
        args.quarantine.parent.mkdir(parents=True, exist_ok=True)
        args.quarantine.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    write_report(args.report_out, mode="gate", task=args.task, findings=findings,
                 checked=rel_files, dry_run=args.dry_run)

    # Always ship the manifest + report alongside whatever survives the gate —
    # even when everything was blocked, committing just these two makes the
    # incident visible on the Ops page instead of silently skipping the tick.
    if rel_files and not args.dry_run:
        for extra in (str(args.quarantine.relative_to(REPO_ROOT)).replace("\\", "/"),
                      str(args.report_out.relative_to(REPO_ROOT)).replace("\\", "/")):
            if extra not in kept:
                kept.append(extra)

    for f in findings:
        tag = {"block": "::error", "quarantine": "::warning", "warn": "::notice"}[f["severity"]]
        print(f"{tag} title=sentinel {f['code']}::{f['artifact']}: {f['detail']}")
    print(f"[sentinel] gate verdict={verdict} mode={mode} kept={len(kept)}/{len(rel_files)} "
          f"blocked={','.join(blocked) or 'none'}")
    write_github_output({
        "verdict": verdict,
        "files": " ".join(kept),
        "blocked": " ".join(blocked),
    })
    return 0


def cmd_sweep(args, cfg: dict) -> int:
    ctx = load_context()
    now = utcnow()
    findings, _ = run_checks_for_files(SWEEP_ARTIFACTS, cfg, ctx, now=now)
    findings.extend(check_staleness(cfg, now=now))

    snap = build_provider_snapshot(now)
    findings.extend(update_provider_ledger(args.ledger, snap, cfg, dry_run=args.dry_run))

    # A --verify re-sweep (same workflow run, minutes later) must not advance
    # quarantine recovery: clean_streak counts independent observation windows,
    # and two sweeps of the same data snapshot are one observation.
    manifest = load_quarantine(args.quarantine)
    manifest = apply_quarantine(manifest, findings, cfg, now=now,
                                full_sweep=not args.verify)
    if not args.dry_run:
        args.quarantine.parent.mkdir(parents=True, exist_ok=True)
        args.quarantine.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    write_report(args.report_out, mode="sweep", task=None, findings=findings,
                 checked=SWEEP_ARTIFACTS, dry_run=args.dry_run)
    verdict = verdict_of(findings)

    # Heal targets: deterministic Tier-0 failover the workflow can run without any LLM.
    heal: set[str] = set()
    adapter: set[str] = set()
    for f in findings:
        if f["code"] in ("spot_coverage_drop", "zombie_spot", "return_outlier") or (
                f["code"] == "artifact_stale" and f["artifact"] == "data/underlying_intraday_spot.json"):
            heal.add("spot")
        if f["code"] == "volume_feed_dead":
            heal.add("volume")
        if f["code"] == "artifact_stale" and f["artifact"] == "data/corporate_actions.json":
            heal.add("corporate_actions")
        if f["code"] in ("provider_drift", "parse_error", "schema_missing_keys"):
            adapter.add(f"{f['artifact']}:{f['code']}")

    for f in findings:
        print(f"[{f['severity'].upper()}] {f['code']} {f['artifact']}"
              + (f" {f['ticker']}" if f.get("ticker") else "") + f" — {f['detail']}")
    print(f"[sentinel] sweep verdict={verdict} findings={len(findings)} "
          f"quarantined_tickers={len(manifest['tickers'])} heal={','.join(sorted(heal)) or 'none'}")
    blocked = sorted({f["artifact"] for f in findings if f["severity"] == BLOCK})
    write_github_output({
        "verdict": verdict,
        "heal_targets": " ".join(sorted(heal)),
        "adapter_failures": " ".join(sorted(adapter)),
        "quarantined": str(len(manifest["tickers"])),
        "blocked": " ".join(blocked),
    })
    if args.fail_on_block and verdict == "block":
        return 1
    return 0


def _findings_fingerprint(findings: list[dict]) -> str:
    key = json.dumps(sorted(
        (f.get("code", ""), f.get("artifact", ""), f.get("ticker") or "", f.get("severity", ""))
        for f in findings))
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]


def _marker(fp: str, mode: str, verdict: str, ts: str) -> str:
    return f"<!-- sentinel fp={fp} mode={mode} verdict={verdict} ts={ts} -->"


_MARKER_RE = re.compile(
    r"<!-- sentinel fp=(?P<fp>\w+) mode=(?P<mode>[\w-]+) verdict=(?P<verdict>\w+) "
    r"ts=(?P<ts>[\w:.+-]+) -->")


def cmd_alert(args, cfg: dict) -> int:
    try:
        report = json.loads(args.report_out.read_text(encoding="utf-8"))
    except Exception as exc:
        print(f"no sentinel report to alert on ({exc})", file=sys.stderr)
        return 0

    def _gh(cmd: list[str]) -> subprocess.CompletedProcess:
        return subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)

    def _find_open_issue() -> int | None:
        if os.environ.get("GITHUB_ACTIONS") != "true" and not os.environ.get("GH_TOKEN"):
            return None
        proc = _gh(["gh", "issue", "list", "--label", ISSUE_LABEL, "--state", "open",
                    "--json", "number", "--limit", "5"])
        if proc.returncode != 0:
            print(proc.stderr or proc.stdout, file=sys.stderr)
            return None
        try:
            items = json.loads(proc.stdout or "[]")
        except json.JSONDecodeError:
            return None
        for item in items:
            if isinstance(item.get("number"), int):
                return item["number"]
        return None

    def _markers(issue_num: int) -> list[dict]:
        proc = _gh(["gh", "issue", "view", str(issue_num), "--json", "comments,body"])
        if proc.returncode != 0:
            return []
        try:
            payload = json.loads(proc.stdout or "{}")
        except json.JSONDecodeError:
            return []
        texts = [payload.get("body") or ""]
        texts += [c.get("body") or "" for c in payload.get("comments") or []]
        found: list[dict] = []
        for text in texts:
            for m in _MARKER_RE.finditer(text):
                found.append(m.groupdict())
        return found

    verdict = report.get("verdict", "pass")
    mode = report.get("mode", "?")
    findings = report.get("findings", [])
    build_time = report.get("build_time", "")
    fp = _findings_fingerprint(findings)
    mark = _marker(fp, mode, verdict, build_time)

    if verdict == "pass":
        existing = None if args.dry_run else _find_open_issue()
        if existing is None:
            print("sentinel pass — no alert")
            return 0
        # A passing SWEEP validates the committed artifacts on main — which may be
        # healthy precisely because the per-tick gate keeps blocking a corrupt
        # writer. Never auto-close over a recent gate block/quarantine.
        marks = _markers(existing)
        last_gate = next((m for m in reversed(marks)
                          if m.get("mode") == "gate" and m.get("verdict") in (BLOCK, QUARANTINE)),
                         None)
        if last_gate:
            recent = parse_ts(last_gate.get("ts"))
            if recent and (utcnow() - recent) < timedelta(hours=6):
                already_held = bool(marks) and marks[-1].get("mode") == "hold"
                if not already_held:
                    _gh(["gh", "issue", "comment", str(existing), "--body",
                         "Sweep of committed artifacts passed, but the per-tick gate "
                         f"reported `{last_gate.get('verdict')}` within the last 6h — keeping "
                         f"this issue open.\n\n{_marker(fp, 'hold', 'pass', build_time)}"])
                print(f"holding issue #{existing} open (recent gate {last_gate.get('verdict')})")
                return 0
        _gh(["gh", "issue", "comment", str(existing), "--body",
             f"Sentinel run at `{build_time}` passed — data health recovered. "
             f"Auto-closing.\n\n{mark}"])
        proc = _gh(["gh", "issue", "close", str(existing)])
        print(f"closed issue #{existing}" if proc.returncode == 0 else proc.stderr)
        return 0

    by_sev: dict[str, list[dict]] = {}
    for f in findings:
        by_sev.setdefault(f["severity"], []).append(f)
    title = f"Data sentinel: {len(findings)} finding(s), verdict={verdict} — {build_time[:10]}"
    lines = [f"## Sentinel {report.get('mode', '?')} report", "",
             f"Verdict: **{verdict}** · run `{build_time}` · task `{report.get('task') or '-'}`", ""]
    for sev in (BLOCK, QUARANTINE, WARN):
        fs = by_sev.get(sev)
        if not fs:
            continue
        lines.append(f"### {sev.upper()} ({len(fs)})")
        for f in fs:
            tk = f" `{f['ticker']}`" if f.get("ticker") else ""
            lines.append(f"- `{f['code']}` {f['artifact']}{tk} — {f['detail']}")
        lines.append("")
    lines.extend([
        "---",
        "Auto-filed by `scripts/data_sentinel.py`. Auto-closes when a later run passes.",
        "Runbook: `docs/data-health-sentinel.md` · thresholds: `config/sentinel.json` · "
        "known failure modes: `.github/data-memory/anti_patterns.md`",
        "", "<details><summary>Raw report</summary>", "", "```json",
        json.dumps(report, indent=2)[:12000], "```", "</details>", "", mark,
    ])
    body = "\n".join(lines)

    if args.dry_run:
        print(f"[dry-run] would alert: {title}")
        return 0
    existing = _find_open_issue()
    if existing is not None:
        # Persistent incidents fire the gate every 15 minutes; identical finding
        # sets must not stack ~96 near-duplicate comments a day on the ops issue.
        marks = _markers(existing)
        if any(m.get("fp") == fp and m.get("verdict") == verdict and m.get("mode") == mode
               for m in marks[-3:]):
            print(f"issue #{existing}: unchanged finding set (fp={fp}) — skipping duplicate comment")
            return 0
        proc = _gh(["gh", "issue", "comment", str(existing), "--body",
                    f"**Update {build_time}** — verdict {verdict}\n\n" + "\n".join(
                        f"- `{f['code']}` {f['artifact']} — {f['detail']}" for f in findings[:40])
                    + f"\n\n{mark}"])
        print(f"commented on issue #{existing}" if proc.returncode == 0 else proc.stderr)
        return 0 if proc.returncode == 0 else proc.returncode
    # gh hard-fails issue creation when the label is missing; bootstrap it
    # idempotently (--force updates in place when it already exists).
    _gh(["gh", "label", "create", ISSUE_LABEL, "--color", "D93F0B",
         "--description", "Filed by scripts/data_sentinel.py", "--force"])
    proc = _gh(["gh", "issue", "create", "--title", title, "--body", body, "--label", ISSUE_LABEL])
    print(proc.stdout.strip() if proc.returncode == 0 else (proc.stderr or proc.stdout))
    return 0 if proc.returncode == 0 else proc.returncode


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Deterministic data-health sentinel.")
    parser.add_argument("mode", choices=["gate", "sweep", "alert"])
    parser.add_argument("--files", default="", help="gate: space-separated repo-relative paths")
    parser.add_argument("--task", default=None, help="gate: ci_tick task name (labeling only)")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--report-out", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--quarantine", type=Path, default=DEFAULT_QUARANTINE)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--fail-on-block", action="store_true",
                        help="sweep: exit 1 when verdict is block (marks the CI run red)")
    parser.add_argument("--verify", action="store_true",
                        help="sweep: post-heal re-check in the same run — do not advance "
                             "quarantine recovery streaks")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    if args.mode == "gate":
        return cmd_gate(args, cfg)
    if args.mode == "sweep":
        return cmd_sweep(args, cfg)
    return cmd_alert(args, cfg)


if __name__ == "__main__":
    raise SystemExit(main())

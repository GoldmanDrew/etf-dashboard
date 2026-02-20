"""
ETF Borrow Dashboard — FastAPI Backend
=======================================
Single-process server with background scheduler for periodic data refresh.

Architecture:
  ┌──────────┐   ┌──────────────┐   ┌──────────┐
  │  React   │◄──│   FastAPI     │◄──│  IBKR    │
  │  SPA     │   │  /api/*       │   │  FTP     │
  └──────────┘   │  + Scheduler  │   └──────────┘
                 │  + SQLite     │
                 └──────────────┘
"""
from __future__ import annotations

import datetime as dt
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import numpy as np
import yaml
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from backend.models import ETFRecord, DashboardSummary, SystemStatus, Bucket
from backend.universe import load_universe, load_inverse_etfs, assign_buckets, norm_sym
from backend.ibkr_fetcher import fetch_ibkr_ftp, fetch_mock, BorrowSnapshot
from backend.decay import compute_mock_decay_for_universe
from backend.db import DashboardDB
from backend.github_sync import sync_universe_from_github, get_last_commit_info, resolve_github_token

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("dashboard")

# ──────────────────────────────────────────────
# Global State
# ──────────────────────────────────────────────
CONFIG: dict = {}
UNIVERSE_DF = None       # pd.DataFrame
INVERSE_SET: set = set()
RECORDS: dict[str, ETFRecord] = {}  # symbol → ETFRecord
LAST_SNAPSHOT: Optional[BorrowSnapshot] = None
DECAY_DATA: dict = {}    # symbol → decay dict
DB: Optional[DashboardDB] = None
START_TIME = time.monotonic()
ERRORS: list[str] = []
GITHUB_SYNC_STATUS: dict = {}  # last sync result


def load_config(path: str = "config/config.yaml") -> dict:
    p = Path(path)
    if p.exists():
        with open(p) as f:
            return yaml.safe_load(f)
    logger.warning(f"Config not found at {p}, using defaults")
    return {}


def sync_github_universe() -> dict:
    """Pull latest etf_screened_today.csv from GitHub and reload if changed."""
    global GITHUB_SYNC_STATUS

    gh_cfg = CONFIG.get("github", {})
    if not gh_cfg.get("enabled", False):
        logger.info("GitHub sync disabled in config")
        return {"success": True, "updated": False, "error": "disabled"}

    token = resolve_github_token(CONFIG)

    result = sync_universe_from_github(
        repo=gh_cfg.get("repo", "GoldmanDrew/ls-algo"),
        branch=gh_cfg.get("branch", "main"),
        remote_path=gh_cfg.get("remote_path", "data/etf_screened_today.csv"),
        local_path=CONFIG.get("universe_csv", "data/etf_screened_today.csv"),
        backup_dir=gh_cfg.get("backup_dir", "data/universe_history"),
        github_token=token,
    )

    # Try to get last commit info (when was the screener last run?)
    commit_info = get_last_commit_info(
        repo=gh_cfg.get("repo", "GoldmanDrew/ls-algo"),
        file_path=gh_cfg.get("remote_path", "data/etf_screened_today.csv"),
        github_token=token,
    )
    result["last_commit"] = commit_info

    GITHUB_SYNC_STATUS = result

    # If file was updated, reload universe + rebucket + recompute
    if result.get("updated"):
        logger.info("Universe file updated from GitHub — reloading data pipeline")
        try:
            _reload_universe()
            refresh_borrow()
            refresh_decay()
        except Exception as e:
            msg = f"Reload after GitHub sync failed: {e}"
            logger.exception(msg)
            ERRORS.append(msg)

    return result


def _reload_universe():
    """Reload universe from CSV and reassign buckets (after a GitHub sync)."""
    global UNIVERSE_DF, INVERSE_SET
    universe_path = CONFIG.get("universe_csv", "data/etf_screened_today.csv")
    UNIVERSE_DF = load_universe(universe_path)

    inverse_path = CONFIG.get("inverse_etf_csv", "config/inverse_etfs.csv")
    INVERSE_SET = load_inverse_etfs(inverse_path)

    bucket_cfg = CONFIG.get("buckets", {})
    threshold = bucket_cfg.get("high_beta_threshold", 1.5)
    blacklist = CONFIG.get("blacklist", [])
    UNIVERSE_DF = assign_buckets(UNIVERSE_DF, INVERSE_SET, threshold, blacklist)

    _build_records_from_csv()


def init_data():
    """Load universe, assign buckets, compute initial decay."""
    global UNIVERSE_DF, INVERSE_SET, RECORDS, DECAY_DATA, DB

    cfg = CONFIG

    # ── GitHub sync on startup ──
    gh_cfg = cfg.get("github", {})
    if gh_cfg.get("enabled") and gh_cfg.get("sync_on_startup", True):
        logger.info("Syncing universe from GitHub on startup…")
        try:
            result = sync_github_universe()
            if result.get("success"):
                if result.get("updated"):
                    logger.info("Universe updated from GitHub")
                else:
                    logger.info("Universe unchanged on GitHub")
            else:
                logger.warning(f"GitHub sync issue: {result.get('error')}")
        except Exception as e:
            logger.warning(f"GitHub sync failed on startup (using local file): {e}")

    # Load universe
    universe_path = cfg.get("universe_csv", "data/etf_screened_today.csv")
    UNIVERSE_DF = load_universe(universe_path)

    # Load inverse ETFs
    inverse_path = cfg.get("inverse_etf_csv", "config/inverse_etfs.csv")
    INVERSE_SET = load_inverse_etfs(inverse_path)

    # Assign buckets
    bucket_cfg = cfg.get("buckets", {})
    threshold = bucket_cfg.get("high_beta_threshold", 1.5)
    blacklist = cfg.get("blacklist", [])
    UNIVERSE_DF = assign_buckets(UNIVERSE_DF, INVERSE_SET, threshold, blacklist)

    # Init DB
    db_path = cfg.get("database", {}).get("path", "data/dashboard.db")
    DB = DashboardDB(db_path)
    DB.connect()

    # Build initial records from CSV
    _build_records_from_csv()

    # Initial fetch
    refresh_borrow()

    # Initial decay
    refresh_decay()


def _build_records_from_csv():
    """Build ETFRecord objects from the loaded universe DataFrame."""
    global RECORDS
    RECORDS = {}

    for _, row in UNIVERSE_DF.iterrows():
        sym = str(row.get("symbol", ""))
        if not sym:
            continue

        rec = ETFRecord(
            symbol=sym,
            underlying=str(row.get("underlying", "")),
            leverage=float(row.get("Leverage", 0) or 0),
            expected_leverage=float(row["ExpectedLeverage"]) if "ExpectedLeverage" in row and not _isnan(row.get("ExpectedLeverage")) else None,
            beta=float(row["Beta"]) if not _isnan(row.get("Beta")) else None,
            beta_n_obs=int(row["Beta_n_obs"]) if not _isnan(row.get("Beta_n_obs")) else None,
            bucket=str(row.get("bucket", Bucket.LOW_BETA.value)),
            borrow_fee_annual=float(row["borrow_fee_annual"]) if not _isnan(row.get("borrow_fee_annual")) else None,
            borrow_rebate_annual=float(row["borrow_rebate_annual"]) if not _isnan(row.get("borrow_rebate_annual")) else None,
            borrow_net_annual=float(row["borrow_net_annual"]) if not _isnan(row.get("borrow_net_annual")) else None,
            shares_available=int(row["shares_available"]) if not _isnan(row.get("shares_available")) else None,
            borrow_current=float(row["borrow_current"]) if not _isnan(row.get("borrow_current")) else None,
            borrow_spiking=bool(row.get("borrow_spiking", False)),
            borrow_missing=bool(row.get("borrow_missing_from_ftp", False)),
            include_for_algo=bool(row.get("include_for_algo", False)),
            protected=bool(row.get("protected", False)),
            cagr_positive=bool(row.get("cagr_positive")) if not _isnan(row.get("cagr_positive")) else None,
            last_updated=dt.datetime.utcnow(),
            is_stale=False,
        )
        RECORDS[sym] = rec


def _isnan(v) -> bool:
    if v is None:
        return True
    try:
        return np.isnan(float(v))
    except (TypeError, ValueError):
        return False


# ──────────────────────────────────────────────
# Refresh functions
# ──────────────────────────────────────────────
def refresh_borrow():
    """Fetch latest borrow rates and update records."""
    global LAST_SNAPSHOT, ERRORS
    try:
        ibkr_cfg = CONFIG.get("ibkr", {})
        use_mock = ibkr_cfg.get("use_mock", True)

        if use_mock:
            snap = fetch_mock(UNIVERSE_DF, seed_from_csv=True)
        else:
            snap = fetch_ibkr_ftp(
                host=ibkr_cfg.get("ftp_host", "ftp2.interactivebrokers.com"),
                user=ibkr_cfg.get("ftp_user", "shortstock"),
                passwd=ibkr_cfg.get("ftp_pass", ""),
                filename=ibkr_cfg.get("filename", "usa.txt"),
            )

        LAST_SNAPSHOT = snap

        if snap.success:
            stale_sec = CONFIG.get("refresh", {}).get("stale_threshold_seconds", 300)

            for sym, rec in RECORDS.items():
                if sym in snap.borrow_map:
                    rec.borrow_net_annual = snap.borrow_map[sym]
                    rec.borrow_fee_annual = snap.fee_map.get(sym)
                    rec.borrow_rebate_annual = snap.rebate_map.get(sym)
                    rec.shares_available = snap.available_map.get(sym)
                    rec.borrow_current = snap.borrow_map[sym]
                    rec.last_updated = snap.timestamp
                    rec.is_stale = False
                    rec.borrow_missing = False
                else:
                    rec.borrow_missing = True
                    if rec.last_updated:
                        age = (dt.datetime.utcnow() - rec.last_updated).total_seconds()
                        rec.is_stale = age > stale_sec

                # Recompute spread
                if rec.gross_decay_annual is not None and rec.borrow_net_annual is not None:
                    rec.spread = round(rec.gross_decay_annual - rec.borrow_net_annual, 6)

            # Store in DB
            if DB:
                DB.insert_borrow_snapshot(
                    snap.borrow_map, snap.fee_map,
                    snap.rebate_map, snap.available_map,
                    snap.timestamp,
                )

            logger.info(f"Borrow refresh complete: {len(snap.borrow_map)} symbols, {snap.duration_ms:.0f}ms")
        else:
            ERRORS.append(f"Borrow fetch failed: {snap.error}")

    except Exception as e:
        ERRORS.append(f"Borrow refresh error: {str(e)}")
        logger.exception("Borrow refresh failed")


def refresh_decay():
    """Compute/refresh decay metrics."""
    global DECAY_DATA

    borrow_map = {}
    for sym, rec in RECORDS.items():
        if rec.borrow_net_annual is not None:
            borrow_map[sym] = rec.borrow_net_annual

    default_borrow = CONFIG.get("analytics", {}).get("default_borrow_annual", 0.05)
    DECAY_DATA = compute_mock_decay_for_universe(UNIVERSE_DF, borrow_map, default_borrow)

    # Update records
    for sym, rec in RECORDS.items():
        if sym in DECAY_DATA:
            d = DECAY_DATA[sym]
            rec.gross_decay_annual = d.get("gross_decay_annual")
            rec.decay_3m = d.get("decay_3m")
            rec.decay_6m = d.get("decay_6m")
            rec.decay_12m = d.get("decay_12m")

            if rec.borrow_net_annual is not None and rec.gross_decay_annual is not None:
                rec.spread = round(rec.gross_decay_annual - rec.borrow_net_annual, 6)

    logger.info(f"Decay refresh complete: {len(DECAY_DATA)} symbols")


# ──────────────────────────────────────────────
# Background Scheduler
# ──────────────────────────────────────────────
from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler()


def start_scheduler():
    refresh_cfg = CONFIG.get("refresh", {})
    borrow_interval = refresh_cfg.get("borrow_interval_seconds", 60)
    decay_interval = refresh_cfg.get("decay_interval_seconds", 86400)

    scheduler.add_job(refresh_borrow, "interval", seconds=borrow_interval, id="borrow_refresh")
    scheduler.add_job(refresh_decay, "interval", seconds=decay_interval, id="decay_refresh")

    # GitHub universe sync
    gh_cfg = CONFIG.get("github", {})
    gh_interval = gh_cfg.get("sync_interval_seconds", 3600)
    if gh_cfg.get("enabled") and gh_interval > 0:
        scheduler.add_job(sync_github_universe, "interval", seconds=gh_interval, id="github_sync")
        logger.info(f"GitHub sync scheduled every {gh_interval}s")

    scheduler.start()
    logger.info(f"Scheduler started: borrow every {borrow_interval}s, decay every {decay_interval}s")


# ──────────────────────────────────────────────
# FastAPI App
# ──────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global CONFIG
    CONFIG = load_config()
    init_data()
    start_scheduler()
    yield
    scheduler.shutdown(wait=False)
    if DB:
        DB.close()


app = FastAPI(
    title="ETF Borrow Dashboard",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── API Endpoints ─────────────────────────────

@app.get("/api/records")
def get_records(
    bucket: Optional[str] = Query(None, description="Filter by bucket"),
    search: Optional[str] = Query(None, description="Search by symbol/underlying"),
    sort_by: Optional[str] = Query(None, description="Sort column"),
    sort_desc: bool = Query(False, description="Sort descending"),
    algo_only: bool = Query(False, description="Only include_for_algo=True"),
) -> list[dict]:
    """Return all ETF records, optionally filtered."""
    records = list(RECORDS.values())

    if bucket:
        records = [r for r in records if r.bucket == bucket]

    if algo_only:
        records = [r for r in records if r.include_for_algo]

    if search:
        s = search.upper()
        records = [r for r in records if s in r.symbol or s in r.underlying]

    if sort_by:
        def sort_key(r):
            v = getattr(r, sort_by, None)
            if v is None:
                return float("inf") if not sort_desc else float("-inf")
            return v
        records.sort(key=sort_key, reverse=sort_desc)

    return [r.model_dump() for r in records]


@app.get("/api/summary")
def get_summary() -> dict:
    """Top-level dashboard summary."""
    all_recs = list(RECORDS.values())
    total = len(all_recs)

    b1 = [r for r in all_recs if r.bucket == Bucket.HIGH_BETA.value]
    b2 = [r for r in all_recs if r.bucket == Bucket.LOW_BETA.value]
    b3 = [r for r in all_recs if r.bucket == Bucket.INVERSE.value]

    # Best spreads (top 5)
    with_spread = [r for r in all_recs if r.spread is not None]
    with_spread.sort(key=lambda r: r.spread or 0, reverse=True)
    best_spreads = [
        {"symbol": r.symbol, "spread": r.spread, "borrow": r.borrow_net_annual, "decay": r.gross_decay_annual}
        for r in with_spread[:5]
    ]

    # Worst borrows (highest borrow cost, top 5)
    with_borrow = [r for r in all_recs if r.borrow_net_annual is not None]
    with_borrow.sort(key=lambda r: r.borrow_net_annual or 0, reverse=True)
    worst_borrows = [
        {"symbol": r.symbol, "borrow": r.borrow_net_annual, "shares": r.shares_available}
        for r in with_borrow[:5]
    ]

    stale = sum(1 for r in all_recs if r.is_stale)
    missing = sum(1 for r in all_recs if r.borrow_missing)

    return DashboardSummary(
        total_symbols=total,
        bucket_1_count=len(b1),
        bucket_2_count=len(b2),
        bucket_3_count=len(b3),
        best_spreads=best_spreads,
        worst_borrows=worst_borrows,
        pct_stale=round(stale / total * 100, 1) if total else 0,
        pct_missing=round(missing / total * 100, 1) if total else 0,
        last_refresh=LAST_SNAPSHOT.timestamp if LAST_SNAPSHOT else None,
        ibkr_connected=LAST_SNAPSHOT.success if LAST_SNAPSHOT else False,
        errors=ERRORS[-10:],
    ).model_dump()


@app.get("/api/status")
def get_status() -> dict:
    """System health / status page."""
    all_recs = list(RECORDS.values())
    status = {
        "ibkr_connected": LAST_SNAPSHOT.success if LAST_SNAPSHOT else False,
        "last_fetch_time": LAST_SNAPSHOT.timestamp.isoformat() if LAST_SNAPSHOT and LAST_SNAPSHOT.timestamp else None,
        "last_fetch_duration_ms": LAST_SNAPSHOT.duration_ms if LAST_SNAPSHOT else None,
        "total_symbols": len(all_recs),
        "symbols_with_borrow": sum(1 for r in all_recs if r.borrow_net_annual is not None),
        "symbols_stale": sum(1 for r in all_recs if r.is_stale),
        "symbols_missing": sum(1 for r in all_recs if r.borrow_missing),
        "errors": ERRORS[-20:],
        "uptime_seconds": round(time.monotonic() - START_TIME, 1),
        "github_sync": GITHUB_SYNC_STATUS or None,
    }
    return status


@app.get("/api/history/{symbol}")
def get_history(symbol: str, limit: int = Query(100, ge=1, le=1000)) -> list[dict]:
    """Get borrow history for a symbol."""
    if DB is None:
        raise HTTPException(500, "Database not initialized")
    return DB.get_borrow_history(norm_sym(symbol), limit)


@app.post("/api/refresh/borrow")
def trigger_borrow_refresh():
    """Manually trigger a borrow rate refresh."""
    refresh_borrow()
    return {"status": "ok", "timestamp": dt.datetime.utcnow().isoformat()}


@app.post("/api/refresh/decay")
def trigger_decay_refresh():
    """Manually trigger a decay recalculation."""
    refresh_decay()
    return {"status": "ok", "timestamp": dt.datetime.utcnow().isoformat()}


@app.post("/api/sync/github")
def trigger_github_sync():
    """Manually trigger a GitHub universe sync."""
    result = sync_github_universe()
    return result


@app.get("/api/sync/status")
def get_github_sync_status():
    """Get the last GitHub sync result."""
    return GITHUB_SYNC_STATUS or {"success": False, "error": "No sync has run yet"}


# ── Serve Frontend ─────────────────────────────

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"

@app.get("/", response_class=HTMLResponse)
def serve_index():
    index = FRONTEND_DIR / "index.html"
    if index.exists():
        return index.read_text()
    return "<h1>ETF Borrow Dashboard</h1><p>Frontend not found. Check frontend/index.html</p>"


# ──────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)

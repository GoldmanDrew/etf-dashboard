# ETF Borrow Rate Dashboard

Real-time IBKR short stock borrow rate monitoring with decay-vs-borrow spread analysis for leveraged and inverse ETFs.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        BROWSER (React SPA)                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌───────────────┐  │
│  │ Summary  │  │ Bucket   │  │ Sortable │  │  Detail /     │  │
│  │ Panel    │  │ Tabs     │  │ Table    │  │  Sparklines   │  │
│  └──────────┘  └──────────┘  └──────────┘  └───────────────┘  │
│         ▲            ▲            ▲               ▲             │
└─────────┼────────────┼────────────┼───────────────┼─────────────┘
          │  JSON/REST │            │               │
┌─────────▼────────────▼────────────▼───────────────▼─────────────┐
│                    FastAPI Backend (:8000)                       │
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────────────┐ │
│  │ /api/records │  │ /api/summary │  │ /api/status           │ │
│  │ /api/history │  │ /api/refresh │  │ /api/refresh/decay    │ │
│  └──────┬───────┘  └──────┬───────┘  └───────────┬───────────┘ │
│         │                 │                       │             │
│  ┌──────▼─────────────────▼───────────────────────▼──────────┐ │
│  │                 In-Memory Record Store                     │ │
│  │          dict[symbol] → ETFRecord (Pydantic)              │ │
│  └──────────────────────┬────────────────────────────────────┘ │
│                         │                                      │
│  ┌──────────┐  ┌────────▼────────┐  ┌──────────────────────┐  │
│  │ Universe │  │   APScheduler   │  │   Decay Engine       │  │
│  │ Loader + │  │  (Background)   │  │   (Stahl / Mock)     │  │
│  │ Bucketer │  │                 │  │                      │  │
│  └──────────┘  │ • Borrow: 60s  │  └──────────────────────┘  │
│                │ • Decay: daily  │                             │
│                └────────┬────────┘                             │
│                         │                                      │
│  ┌──────────────────────▼────────────────────────────────────┐ │
│  │                  IBKR Fetcher                             │ │
│  │  ┌─────────────┐          ┌──────────────────┐           │ │
│  │  │  Mock Mode  │    OR    │  Live FTP Mode   │           │ │
│  │  │ (from CSV)  │          │ ftp2.ibkr.com    │           │ │
│  │  └─────────────┘          └──────────────────┘           │ │
│  └───────────────────────────────────────────────────────────┘ │
│                         │                                      │
│  ┌──────────────────────▼────────────────────────────────────┐ │
│  │              SQLite (data/dashboard.db)                    │ │
│  │  • borrow_history — timestamped snapshots                 │ │
│  │  • system_state   — key/value config                      │ │
│  └───────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘

Data Sources:
  ┌───────────────────┐   ┌─────────────────────┐   ┌──────────────────────┐
  │ etf_screened_      │   │ inverse_etfs.csv    │   │ GitHub (ls-algo)     │
  │ today.csv          │   │ (Bucket 3 source    │   │ Auto-synced hourly   │
  │ (354 symbols)      │   │  of truth, ~32 syms)│   │ → data/etf_screened_ │
  └───────────────────┘   └─────────────────────┘   │   today.csv          │
                                                     └──────────────────────┘
```

## File Structure

```
etf-borrow-dashboard/
├── backend/
│   ├── __init__.py
│   ├── main.py              # FastAPI app, endpoints, scheduler
│   ├── models.py            # Pydantic models (ETFRecord, Summary, Status)
│   ├── universe.py          # CSV loading, symbol normalization, bucketing
│   ├── ibkr_fetcher.py      # IBKR FTP fetcher + mock fetcher
│   ├── decay.py             # Stahl decay engine + mock decay estimator
│   ├── db.py                # SQLite storage for borrow history
│   └── github_sync.py       # Auto-sync universe CSV from ls-algo repo
├── frontend/
│   └── index.html           # Single-file React SPA dashboard
├── config/
│   ├── config.yaml          # All configuration (refresh rates, thresholds, etc.)
│   └── inverse_etfs.csv     # Curated inverse ETF list (Bucket 3)
├── data/
│   └── etf_screened_today.csv  # Universe file from your screener
├── tests/
│   └── test_bucketing.py    # Unit tests for bucketing, freshness, decay
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── run.py                   # Convenience launcher
└── README.md
```

## Quick Start

### 1. Install Dependencies

```bash
cd etf-borrow-dashboard
pip install -r requirements.txt
```

### 2. Run Locally

```bash
python run.py
# → Dashboard at http://localhost:8000
# → On startup, pulls latest universe from GitHub automatically
```

### 3. Run with Docker

```bash
docker-compose up --build
# → Dashboard at http://localhost:8000
```

### 4. Run Tests

```bash
cd etf-borrow-dashboard
python -m pytest tests/ -v
```

## GitHub Universe Sync

The dashboard automatically pulls the latest `etf_screened_today.csv` from your [`GoldmanDrew/ls-algo`](https://github.com/GoldmanDrew/ls-algo) repo:

```
GitHub Actions (daily_pipeline.yml)
  └── etf_screener.py runs → writes data/etf_screened_today.csv → git push
                                        │
Dashboard (on startup + every hour)     │
  └── github_sync.py ──────────────────►│ fetches raw file via HTTPS
      ├── SHA256 change detection (skip if unchanged)
      ├── Backup old CSV to data/universe_history/
      ├── Reload universe → re-bucket → re-fetch borrows → re-compute decay
      └── UI auto-refreshes every 15s
```

**Config** (`config/config.yaml`):
```yaml
github:
  enabled: true
  repo: "GoldmanDrew/ls-algo"
  branch: "main"
  remote_path: "data/etf_screened_today.csv"
  sync_on_startup: true
  sync_interval_seconds: 3600   # hourly
  backup_dir: "data/universe_history"
```

**Manual sync**: Click "Sync Now" in the Status page, or `POST /api/sync/github`.

**Private repo?** Set `GITHUB_TOKEN` env var or add `github.token` to config.yaml.

**Offline fallback**: If GitHub is unreachable, the dashboard uses whatever local `data/etf_screened_today.csv` exists.

## Configuration

All settings are in `config/config.yaml`:

| Setting | Default | Description |
|---------|---------|-------------|
| `ibkr.use_mock` | `true` | Set `false` for live IBKR FTP |
| `refresh.borrow_interval_seconds` | `60` | Borrow rate refresh cadence |
| `refresh.stale_threshold_seconds` | `300` | Mark data stale after 5 min |
| `buckets.high_beta_threshold` | `1.5` | Beta cutoff for Bucket 1 vs 2 |
| `blacklist` | `[]` | Symbols to exclude |

## Bucket Definitions

| Bucket | Rule | Description |
|--------|------|-------------|
| **Bucket 1** | Beta > 1.5 | High-beta leveraged ETFs (more vol drag) |
| **Bucket 2** | Beta ≤ 1.5 | Lower-beta leveraged ETFs |
| **Bucket 3** | In `inverse_etfs.csv` | Curated inverse ETFs (overrides beta rule) |

## Switching to Live IBKR Data

1. Edit `config/config.yaml`:
   ```yaml
   ibkr:
     use_mock: false
   ```

2. The fetcher connects to `ftp2.interactivebrokers.com` (public, no auth needed) and parses `usa.txt`.

3. For the full Stahl decay calculation with real price data, provide a `prices_tr` DataFrame with `{TICKER}_TR` columns and call `stahl_decay_metrics_one()` from `backend/decay.py`. The mock decay estimator uses a volatility-drag formula as a placeholder.

## GitHub Universe Sync

The dashboard auto-syncs `etf_screened_today.csv` from your [`ls-algo`](https://github.com/GoldmanDrew/ls-algo) repo so the universe stays current after each daily screener run.

**How it works:**
1. On startup, pulls `data/etf_screened_today.csv` from `main` branch via `raw.githubusercontent.com`
2. Compares SHA-256 hash — skips download if file hasn't changed
3. On change: backs up old file to `data/universe_history/`, writes new file atomically, then reloads the full pipeline (re-bucket → re-fetch borrow → re-compute decay)
4. Runs on a schedule (default: every hour) via APScheduler
5. Fetches last commit metadata (author, date, message) from GitHub API so you can see when the screener last ran

**Config (`config/config.yaml`):**
```yaml
github:
  enabled: true
  repo: "GoldmanDrew/ls-algo"
  branch: "main"
  remote_path: "data/etf_screened_today.csv"
  backup_dir: "data/universe_history"
  sync_on_startup: true
  sync_interval_seconds: 3600   # check every hour
  # token: null                 # set for private repos or higher rate limits
```

**Manual trigger:**
- UI: Click **⬡ Sync GitHub** button in the top bar
- API: `POST /api/sync/github`
- Status: `GET /api/sync/status`

**Authentication (optional):**
For private repos or higher API rate limits, set a GitHub personal access token:
```bash
export GITHUB_TOKEN=ghp_xxxxxxxxxxxxx
```
Or in `config.yaml` → `github.token`. The public repo works without any token.

**Failure handling:**
If GitHub is unreachable, the dashboard falls back to whatever local `etf_screened_today.csv` exists — it never crashes or shows empty data due to a sync failure.

## Plugging In Your Real IBKR Methods

The `backend/ibkr_fetcher.py` module has two modes:

- **`fetch_ibkr_ftp()`** — Already implements the exact FTP logic from your notebook (`fetch_ibkr_shortstock_file` + `build_ibkr_short_maps`)
- **`fetch_mock()`** — Uses your CSV values with jitter for offline development

To add a custom IBKR WebSocket or TWS API fetcher, implement a function that returns a `BorrowSnapshot` dataclass and wire it into `refresh_borrow()` in `backend/main.py`.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/records` | All ETF records (supports `?bucket=`, `?search=`, `?sort_by=`, `?algo_only=`) |
| GET | `/api/summary` | Dashboard summary (best spreads, worst borrows, staleness) |
| GET | `/api/status` | System health (IBKR connection, fetch timing, errors) |
| GET | `/api/history/{symbol}` | Borrow rate history for sparklines |
| POST | `/api/refresh/borrow` | Manually trigger borrow refresh |
| POST | `/api/refresh/decay` | Manually trigger decay recalculation |
| POST | `/api/sync/github` | Manually trigger GitHub universe sync |
| GET | `/api/sync/status` | Last GitHub sync result + commit info |
| POST | `/api/sync/github` | Pull latest universe from GitHub and reload |
| GET | `/api/sync/status` | Last GitHub sync result (timestamp, updated, commit info) |

## Design Decisions

**Why FastAPI + single-file React (not Streamlit)?**

| Factor | FastAPI + React | Streamlit |
|--------|----------------|-----------|
| Latency | Sub-50ms API responses | Full page rerenders |
| Sorting/filtering | Client-side, instant | Server roundtrip |
| Real-time updates | Polling / WebSocket ready | Polling only |
| Deploy | Docker, any host | Streamlit Cloud or Docker |
| Long-term | Add auth, WebSocket, multi-user | Limited scaling |
| Complexity | More files, but clean separation | Single file but tangled |

The single-file React frontend avoids a build step entirely — just HTML + Babel in-browser transform. For production, you could eject to a proper Vite/Next.js setup.

## Correctness Safeguards

- **Freshness timestamps**: Every record has `last_updated` + `is_stale` flag
- **Stale visual indicator**: Rows fade + show "STALE" badge when data exceeds threshold
- **Missing data**: `borrow_missing` flag when symbol not found in IBKR response
- **Symbol normalization**: `BRK.B` → `BRK-B` consistently via `norm_sym()`
- **Status page**: Shows IBKR connection state, last fetch time, error log
- **Non-negative borrows**: `net_borrow = max(fee - rebate, 0)` — never shows phantom carry

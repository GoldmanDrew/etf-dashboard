# ETF Borrow Dashboard

Live at: **https://goldmandrew.github.io/etf-dashboard/**

Real-time IBKR short stock borrow rate monitoring with **distributional decay forecasts** and **product-class-aware** decay/edge routing for leveraged, inverse, volatility, and YieldBOOST income ETFs. Deployed as a static site via GitHub Pages — no server required.

**Forks (e.g. diamond-creek-quant):** if your site copies this `index.html` and `scripts/ingest_etf_metrics.py`, merge or cherry-pick the same **chunked Yahoo underlying fetch** and **Stats-tab table column CSS** so `underlying_adj_close` stays populated for full-universe ingests and the metrics grid column widths stay balanced.

> **New agent in this repo?** Read **[AGENTS.md](./AGENTS.md)** first. It explains the full data pipeline, product taxonomy, decay models, and how the front-end routes them.

## How It Works

```
┌──────────────────────────────────────────────────────────────────────┐
│  GoldmanDrew/ls-algo  (daily GitHub Action)                          │
│    daily_screener.py                                                 │
│      ├─ Pulls daily prices, IBKR borrow snapshot, holdings           │
│      ├─ Computes realized & expected gross decay                     │
│      ├─ Computes HARQ-Log anchored distributional decay (p10/p50/p90)│
│      ├─ Bootstraps net edge p05/p25/p50/p75/p95 + histogram          │
│      ├─ Tags product_class + expected_decay_available                │
│      └─ Writes data/etf_screened_today.csv (~520 KB) → git push      │
└────────────────────────────┬─────────────────────────────────────────┘
                             │  raw.githubusercontent.com
┌────────────────────────────▼─────────────────────────────────────────┐
│  GoldmanDrew/etf-dashboard  (this repo)                              │
│                                                                       │
│  scripts/build_data.py                                               │
│    Daily full build:                                                 │
│      1. Fetch CSV from ls-algo                                       │
│      2. Refresh IBKR FTP borrow rates + shares available             │
│      3. Pull realized vol (Yahoo, EWMA + window stats)               │
│      4. Reconstruct borrow_history.json from screener git commits    │
│      5. Run borrow-spike risk model → borrow_spike_risk.json         │
│      6. Refresh options chains (Polygon + Tradier)                   │
│      7. Bucket assignment (B1 high-β, B2 low-β/income, B3 inverse)   │
│      8. Pass-through product_class, expected_decay_available, etc.   │
│      9. Write data/dashboard_data.json (~2.9 MB)                     │
│     10. Commit + Deploy to GitHub Pages                              │
│                                                                       │
│    --borrow-only:   refresh borrow + shares in dashboard_data.json   │
│    --options-only:  refresh data/options_cache.json shard            │
│                                                                       │
│  index.html (React SPA, single file, served as-is)                   │
│    Reads dashboard_data.json + the smaller per-feature JSONs and     │
│    renders the interactive dashboard. Routes Exp. decay / Net edge / │
│    Scenarios per product_class (see "Product Taxonomy" below).       │
└──────────────────────────────────────────────────────────────────────┘
```

## Product Taxonomy & Decay Routing

`ls-algo`'s `screener_v2_fields._product_class` tags every row with one of:

| `product_class` | Trigger | Headline `Exp. decay` cell | Net edge | Scenarios tab |
|---|---|---|---|---|
| `letf` | β > 1.5 | HARQ-Log p50 (fallback: simple Itô) | Bootstrap, **anchor-shifted** to expected p50 | LETF vol-drag grid |
| `inverse` | β < 0 | HARQ-Log p50 (fallback: simple Itô) | Bootstrap, **anchor-shifted** to expected p50 | LETF vol-drag grid |
| `volatility_etp` | UVIX/SVIX/VXX/VIXY/etc. | Simple Itô + empirical roll/tracking adjustment | Bootstrap, **anchor-shifted** to expected p50 | LETF vol-drag grid |
| `income_yieldboost` | `is_yieldboost = true` | **Put-spread Monte-Carlo p50** (server-side, `yieldboost_decay.py`) | Bootstrap, **anchor-shifted** to put-spread p50 | Income put-spread grid |
| `income_put_spread` | Lev ≈ 1.0 (legacy) | HARQ-Log p50 (fallback: simple Itô) | Bootstrap, anchor-shifted when expected p50 available | LETF grid |
| `passive_low_delta` | 0 < β ≤ 1.5 (no income overlay) | **`—` (N/A by policy)** | Realized-only bootstrap (no anchor-shift) | Hidden |
| `other_structured` | Fallback | `—` | Realized fallback | Hidden |

The CSV also ships a boolean `expected_decay_available` flag derived from this taxonomy. The dashboard front-end uses it (with a graceful fallback for older builds) to drive the `—`-rendering policy in the Exp. decay / Net edge / Exp. ETF return (3M) columns.

**Shared forecast volatility:** the main-grid `Exp. ETF return` column and the Scenarios tab now use the same default `forecast_vol_underlying_annual` built in `scripts/build_data.py`. When both legs are available, the builder blends **variance** 50/50:

```text
sigma_forecast^2 = 0.5 * sigma_model^2 + 0.5 * sigma_robust_ewma^2
```

`sigma_model` is implied from `expected_gross_decay_p50_annual` (or inverted from the YieldBOOST put-spread p50). `sigma_robust_ewma` is a 6M EWMA on underlying total-return log returns after clipping unusually large one-day moves, so recent earnings/news gaps influence the forecast but do not dominate it. Raw EWMA and realized volatility remain visible as diagnostics in `realized_vol`.

**Why passive low-β is `—`:** the simple Itô identity says expected gross decay ≈ `(β² − β)/2 · σ²`, which collapses to noise around β ≈ 1. We ship the realized gross drag in the `Gross (realized)` column instead — that's the only honest signal for these products. See `daily_screener.py` Step 5d ("passive_low_delta policy") and `screener_v2_fields._expected_decay_available`.

**Why YieldBOOST gets a dedicated decay model:** YieldBOOST income ETFs (AMYY, AZYY, BBYY, COYY, …) have β ≈ 0.4–0.6 because the 2× LETF NAV is sleeved with a weekly 95/88 SPX-style put-spread. A vanilla HARQ-Log vol-drag p50 of ~2–3% badly understates the actual NAV decay mechanism, which is dominated by the put-spread premium. As of the YB-unification refactor (`ls-algo/yieldboost_decay.py`), the screener now ships a **put-spread Monte-Carlo distribution** for YieldBOOST rows: the underlying's HARQ-Log lognormal moments are sampled, fed through the same `expectedPutSpreadLossWeekly` mechanics that the Scenarios tab uses, compounded 52× minus the 0.99% expense ratio, and exported as `expected_gross_decay_p10/p50/p90/mean_annual` with `expected_gross_decay_dist_model = yieldboost_put_spread` (or `yieldboost_put_spread_point` when the underlying TR history is too short for a full HARQ-Log fit). This means YieldBOOST `Exp. decay` and Net edge ride through the **same column shape** as every other product class — the dashboard simply reads `dist.p50` for the headline and the Net-edge bootstrap is anchor-shifted to that p50 so the pair-trade P&L reflects the put-spread forecast, not just realized noise. The client-side `yieldBoostIntrinsicAnnualDecay` helper survives only as a fallback for rows where the server didn't ship the distribution.

**Anchor-shift bootstrap:** every product class with a meaningful expected component runs a stationary-block bootstrap of realized daily log-hedge-drag and borrow histories, then re-centers the empirical mean on `expected_gross_decay_p50_annual`. Result: dispersion comes from history, location comes from the forward-looking forecast (HARQ-Log p50 for LETF/inverse/vol-ETP, put-spread MC p50 for YieldBOOST). Diagnostics are exposed as `gross_anchor_shift_annual`, `gross_anchor_target_annual`, and `gross_anchor_source` on every row. Passive low-β rows are realized-only by policy (no shift). See `ls-algo/screener_v2_fields.enrich_screener_v2_fields` and `tests/test_anchor_shift_bootstrap.py` for the implementation.

## Decay Models in Use

| Model | Source | Used by |
|---|---|---|
| **Realized log-drag** | `gross_decay_annual = mean(β·log(R_und) − log(R_etf)) × 252` | Headline `Gross (realized)` column on every row |
| **Simple Itô** | `(β² − β)/2 · σ²` | Fallback when distributional model is unavailable; volatility ETP base term |
| **Volatility ETP empirical roll/tracking** | `simple_ito + empirical_roll_tracking_component` | UVIX / SVIX / VXX / VIXY / etc. |
| **HARQ-Log anchored empirical lognormal** | `decay_distribution.py` (lognormal IV_T quantiles) | `letf`, `inverse`, `income_put_spread`; emits p10/p50/p90/mean |
| **YieldBOOST put-spread Monte-Carlo** | `yieldboost_decay.py` (HARQ-Log σ-draws → weekly 95/88 put-spread NAV-decay → 52× compounded − expense ratio) | All `is_yieldboost = True` rows; emits p10/p50/p90/mean (model `yieldboost_put_spread` / `yieldboost_put_spread_point`) |
| **Net edge bootstrap** | Block-bootstrap of daily log-drag + weighted borrow resample, **anchor-shifted to `expected_gross_decay_p50_annual`** | `net_edge_p05/p25/p50/p75/p95_annual` + `net_edge_hist_json` (+ `gross_anchor_*` diagnostics) |
| **`delta_v1` NAV forecast** | `nav_anchor · exp(β · log(spot_und / spot_und_anchor)) · (1 − TER_daily)` | Closed-form baseline for LETF / inverse / vol-ETP / passive-β rows; emitted alongside v2/v3 for A/B comparison |
| **`delta_v2_ito` NAV forecast** | `delta_v1 · exp(−(β² − β)/2 · σ² · Δt)` (σ from `forecast_vol_underlying_annual`, Δt = bdays since anchor / 252) | Adds path-dependent vol drag — pulls down LETFs (β > 1) and inverse (β < 0); same eligibility as `delta_v1` |
| **`delta_v3_swap_mark` NAV forecast** | `nav_anchor + Σ_legs shares · (spot_now − spot_anchor) / shares_outstanding`, OPTION legs at zero delta | Holdings-aware mark from `etf_holdings_latest.json`; default for LETF / inverse / vol-ETP / passive-β rows when holdings + spots resolve |
| **`yieldboost_putspread_v1` NAV forecast** | Same as `delta_v3_swap_mark` but OPTION legs are repriced from `options_cache` mid via parsed OCC components | Default for `income_yieldboost` / `income_put_spread` rows (Stats-tab Fair-value card uses whichever model the dispatcher routes per symbol — see [`data/nav_forecasts/README.md`](data/nav_forecasts/README.md)) |

The HARQ-Log model has plausibility caps (`_LOG_IV_SIGMA_ANNUAL_CAP`), winsorized realized variance/quarticity (`_R2_WINSOR_CAP`), and a horizon-panel-ratio threshold (`_HORIZON_PANEL_RATIO_MIN`) that gates fallback to simple Itô when the underlying history is thin.

**Multi-model NAV forecaster (Stats-tab Fair-value card).** A two-stage pipeline writes **one model NAV per (symbol, model) every 30 min** during the US session and scores them against the next-day official NAV at 5 AM ET. The dashboard surfaces a single "default" routed model per symbol; every applicable model is logged alongside it for A/B comparison.

* `scripts/forecast_nav.py` reads `dashboard_data.json` (β, σ, product class) + `options_cache.json` (current underlying spot, option chains) + `data/etf_holdings_latest.json` (per-leg shares + price + market_value) + `data/nav_forecasts/_anchors.json` (last close, shares_outstanding). For every screener row it computes whichever of `delta_v1`, `delta_v2_ito`, `delta_v3_swap_mark`, and `yieldboost_putspread_v1` have inputs available, writes one line per `(symbol, model)` to `data/nav_forecasts/snapshots/<DATE>.jsonl`, and selects a default per a product-class-aware preference list (income → `yieldboost_putspread_v1` → `delta_v3_swap_mark` → `delta_v2_ito` → `delta_v1`; everything else → `delta_v3_swap_mark` → `delta_v2_ito` → `delta_v1`). Candidates whose `nav_hat / nav_anchor` falls outside [0.5, 2.0] are rejected by a sanity envelope before being routed as default. Triggered by `.github/workflows/refresh-nav-forecast.yml` (`*/30 13-22 * * 1-5`).
* `scripts/score_nav_forecasts.py` runs inside `update-etf-metrics.yml` after the daily NAV ingest. It collapses the day's last forecast per `(symbol, model)`, diffs each one against `etf_metrics_latest.json[SYM].nav`, writes per-pair lines to `data/nav_forecasts/realized/<DATE>.jsonl`, and rolls 60-trading-day per-(symbol, model) stats into `_metrics_daily.json` and the NAV-vs-model chart panel into `_history_panel.json` (both expose a default-model `by_symbol[SYM]` view alongside the full `by_symbol_models[SYM][MODEL]` grid). It then refreshes `_anchors.json` with each symbol's `nav_close`, `und_close` (one yfinance batch call), and `shares_outstanding` for the next trading day.

The dashboard fetches only the three small JSON blobs (`_latest.json`, `_metrics_daily.json`, `_history_panel.json`) — JSONL snapshots stay on disk for offline accuracy audits. Schema details, the delta-differencing rationale for the holdings-mark models, and the upgrade path for further model versions live in [`data/nav_forecasts/README.md`](data/nav_forecasts/README.md).

## Setup (one-time)

### 1. Enable GitHub Pages

Go to **Settings → Pages** in this repo:
- **Source**: GitHub Actions
- That's it — the workflow handles deployment automatically.

### 2. Push to main

```bash
git add -A && git commit -m "initial dashboard" && git push
```

The `build-and-deploy.yml` Action runs on push, builds the data, and deploys. Your site will be live at `https://goldmandrew.github.io/etf-dashboard/` within a few minutes.

## Schedule

- `build-and-deploy.yml` runs once daily on weekdays (plus push/manual) for full rebuild.
- `refresh-borrow.yml` runs every 10 minutes for borrow + shares refresh only (`build_data.py --borrow-only`).
- `refresh-options.yml` runs on a **5-minute cron** (GitHub Actions minimum) for a throttled options shard focused on Bucket 3 inverse ETFs, with YieldBOOST sleeve symbols included when `OPTIONS_INCLUDE_YIELDBOOST=1`. **Do not treat this as true 5-minute freshness** — GitHub schedule jitter often delays runs by 15–60+ minutes, and each run refreshes only one shard (~16 symbols). YieldBOOST IV at held strikes may lag until Phase 3 adds a dedicated targeted workflow.
- `update-etf-metrics.yml` runs daily at **5:00 AM ET** to ingest NAV / AUM / shares outstanding panel data, plus per-ticker distribution history (`etf_distributions.json`) for the Total-Return NAV chart on the Stats tab. The same workflow then scores yesterday's NAV forecasts per `(symbol, model)` (`scripts/score_nav_forecasts.py`) and refreshes `data/nav_forecasts/_anchors.json` (with `shares_outstanding` for the holdings-aware models) for the next trading day.
- `refresh-nav-forecast.yml` runs **every 30 minutes Mon-Fri 13:00-22:00 UTC** and snapshots the multi-model NAV forecaster — `delta_v1`, `delta_v2_ito`, `delta_v3_swap_mark`, and `yieldboost_putspread_v1` — via `scripts/forecast_nav.py`. See the Decay Models section + `data/nav_forecasts/README.md`.
- `update-corporate-actions.yml` runs **every 6 hours** to ingest structured corporate-action events (splits, reverse splits, delistings, symbol changes, mergers) into `corporate_actions.json` and a filtered news feed into `etf_news.json`. Both artifacts power the top-level **News** tab (`#/news`). Dividends/distributions are explicitly excluded from the news feed because the Stats tab already visualizes them via the Total-Return NAV series.
- Refresh workflows commit JSON only; GitHub Pages deployment is handled by `build-and-deploy.yml` to avoid queue contention.

## Vol / VRP tab (YieldBOOST)

The **Vol / VRP** panel on YieldBOOST income rows reads two artifacts:

| File | What it carries | Typical cadence |
|------|-----------------|-----------------|
| `data/yieldboost_put_spreads_latest.json` | Paired put-spread legs scraped from Granite XLS holdings (exact strikes, expiry, sleeve) | Daily ETF metrics ingest + each `build_data` run |
| `data/vrp_live.json` | IV, spread mid, RV, and VRP at those held strikes | Each `build_data` / `--options-only` run |

**UI behavior:** the tab prefers `vrp_live.json`. If that file is missing (404 on Pages), it falls back to spreads JSON and shows holdings strikes with **IV pending** rather than an HTTP error.

**CI gate:** `scripts/vrp_pipeline_diagnostics.py --require-vrp-file --fail-on-missing-vrp-when-spreads` runs after successful builds in `build-and-deploy.yml` and `refresh-options.yml`.

**Operator recovery:** run Update ETF Metrics → Build & Deploy → Refresh Options; see `AGENTS.md` §12.8.

**Known limitation:** options refresh is sharded and schedule-jittered; IV coverage at YieldBOOST sleeves improves when the dedicated targeted workflow (long-term plan Phase 3) lands.

## Running Locally

```bash
# 1. Install deps
pip install -r requirements.txt

# 2. Generate the data (pulls CSV from ls-algo upstream)
python scripts/build_data.py

# 3. Serve the site
python -m http.server 8000
# → open http://localhost:8000
```

Optional FastAPI backend (used by the dev `run.py` server, not by GitHub Pages):

```bash
pip install fastapi uvicorn pyyaml apscheduler pydantic
python run.py
# → http://localhost:8000
```

The static `index.html` deployed to GitHub Pages is the production surface. The FastAPI app in `backend/` is a thin local-dev mirror with the same Pydantic models that build_data.py uses to pass-through the screener CSV — handy when iterating on field plumbing.

## File Structure

```
etf-dashboard/
├── README.md                           # ← you are here
├── AGENTS.md                           # full architecture guide for AI/human agents
├── index.html                          # React SPA (single file, no build step)
├── assets/
│   ├── expected_decay.js               # standalone (β² − β)/2·σ² calculator
│   ├── scenario_returns.js             # vol/shock grid + LETF return model
│   ├── trade_lab.js                    # Trade Lab leg builder + Black-Scholes
│   └── options_data.js                 # options cache helpers
├── data/
│   ├── dashboard_data.json             # PRIMARY — fed to the SPA
│   ├── etf_screened_today.csv          # cached upstream from ls-algo
│   ├── borrow_history.json             # daily borrow per symbol (for net_edge bootstrap)
│   ├── borrow_spike_risk.json          # spike-risk model output per symbol
│   ├── options_cache.json              # Polygon/Tradier options snapshots
│   ├── etf_metrics_daily.{parquet,csv,json}  # NAV/AUM/shares panel
│   ├── etf_metrics_latest.json         # latest snapshot per symbol
│   ├── etf_distributions.json          # per-ticker distribution history
│   ├── corporate_actions.json          # splits/delistings/etc. (News tab)
│   ├── etf_news.json                   # classified news (News tab)
│   └── investors.json                  # auth: bcrypt-hashed user accounts
├── backend/                            # FastAPI dev server
│   ├── main.py                         # FastAPI app + scheduler
│   ├── models.py                       # ETFRecord / DashboardSummary / SystemStatus
│   ├── universe.py                     # CSV loader + bucket assignment
│   ├── decay.py                        # Stahl decay metrics + mock decay
│   ├── ibkr_fetcher.py                 # IBKR FTP shortstock fetcher
│   ├── github_sync.py                  # ls-algo → local sync helper
│   └── db.py                           # SQLite history store (dev-only)
├── scripts/
│   ├── build_data.py                   # PRIMARY — fetches/transforms → JSON
│   ├── ingest_etf_metrics.py           # ETF NAV/AUM/shares ingestion
│   ├── ingest_distributions.py         # Yahoo distribution history
│   ├── ingest_corporate_actions.py     # Polygon corporate-actions + news
│   ├── etf_providers.py                # provider cascade (Tradier/Polygon/Yahoo)
│   ├── etf_holdings_providers.py       # holdings ingestion
│   ├── backfill_close_prices.py        # one-off price backfills
│   ├── dedupe_etf_metrics.py           # parquet maintenance
│   ├── options_cache_diagnostics.py    # CI gate for options coverage
│   └── hash_investor_password.py       # bcrypt utility for investors.json
├── tests/
│   ├── test_bucketing.py               # bucket-assignment unit tests
│   ├── test_ingest_corporate_actions.py
│   ├── test_expected_decay.js          # JS expected_decay.js tests
│   ├── test_scenario_returns.js        # JS scenario_returns.js tests
│   └── test_trade_lab.js               # JS trade_lab.js tests
├── .github/workflows/
│   ├── build-and-deploy.yml            # daily + push: full build → Pages deploy
│   ├── refresh-borrow.yml              # every 10 min: --borrow-only
│   ├── refresh-options.yml             # every 5 min: --options-only shard
│   ├── update-etf-metrics.yml          # daily 5 AM ET: NAV/AUM/distributions
│   └── update-corporate-actions.yml    # every 6 h: corporate actions + news
├── config/
│   └── (yaml configs for the FastAPI dev server)
├── Dockerfile                          # FastAPI backend dev image
├── docker-compose.yml
├── run.py                              # uvicorn launcher for the FastAPI app
└── requirements.txt                    # pandas, numpy, requests, pyarrow, yfinance
```

## Bucket Definitions (UI buckets, separate from `product_class`)

| Bucket | Rule | Description |
|--------|------|-------------|
| **Bucket 1** | β > 1.5 | High-beta leveraged ETFs |
| **Bucket 2** | 0 ≤ β ≤ 1.5 | Lower-beta leveraged ETFs (incl. all YieldBOOST income strategies) |
| **Bucket 3** | β < 0 or curated inverse list | ~32 inverse ETFs |

`bucket` is a UI grouping (drives the bucket pill colour and the tabs in the main table). `product_class` is the **routing key** for decay/edge/scenarios — see the table at the top.

## Data Sources

- **Universe**: `GoldmanDrew/ls-algo` → `data/etf_screened_today.csv` (HARQ-Log distributional decay, net-edge bootstrap, product taxonomy)
- **Borrow rates**: IBKR public FTP (`ftp2.interactivebrokers.com/usa.txt`) — fee-only borrow with shares available
- **Spot + options**: Tradier REST (spot primary) + Polygon REST (options snapshots/contracts, spot fallback)
- **Realized vol & price history**: Yahoo Finance via `yfinance`

### Borrow history and ls-algo `net_edge_*`

`scripts/build_data.py` maintains **`data/borrow_history.json`** (append-only daily `borrow_current` per symbol, reconstructed from screener git commits). To have ls-algo's schema v2 **`net_edge_p05_annual` / `p50` / `p95`** use **weighted resampling** over that full history (recent-heavy half-life, default 90 calendar days), run the daily screener with either:

- `python daily_screener.py --borrow-history-path ../etf-dashboard/data/borrow_history.json`, or
- `BORROW_HISTORY_PATH` set to the same file before `daily_screener.py`.

Without that file, ls-algo still **auto-detects** a sibling `../etf-dashboard/data/borrow_history.json` or `ls-algo/data/borrow_history.json` (the ls-algo GitHub Action curls the public dashboard JSON into `data/` before the screener). New CSV columns include `borrow_resample_mode`, `borrow_weight_halflife_days`, `borrow_history_points_used`, **`net_edge_p25_annual` / `net_edge_p75_annual`**, and **`net_edge_hist_json`** (compact histogram of bootstrap net draws for the dashboard).

## Expected Decay Calculator

The dashboard includes a standalone **Expected Decay Calculator** that uses the simple Itô identity:

`expected_decay(T) = 0.5 * abs(beta) * abs(beta - 1) * (sigma_annual^2) * T_years`

Where `T_years = days/252`, `weeks/52`, `months/12`, or `years`. Volatility input accepts either percent style (`130`) or decimal (`1.3`).

This is the same model as the simple Itô fallback in `decay_distribution.py`. For a richer forecast (median / p10 / p90), the main grid uses the HARQ-Log distribution for LETF/inverse/vol-ETP rows and the put-spread Monte-Carlo distribution (`yieldboost_decay.py`) for YieldBOOST rows. For a YieldBOOST product, the calculator's pure Itô output should be **ignored** — read the headline `Exp. decay` cell (which is now the put-spread MC p50) or open the put-spread Scenarios tab instead.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `UNIVERSE_REPO` | `GoldmanDrew/ls-algo` | Source repo for universe CSV |
| `UNIVERSE_BRANCH` | `main` | Branch to fetch from |
| `UNIVERSE_PATH` | `data/etf_screened_today.csv` | Source CSV path in repo |
| `HIGH_DELTA_THRESHOLD` | `1.5` | Beta cutoff for Bucket 1 vs 2 |
| `GITHUB_TOKEN` | (from Actions) | Auto-set in CI; needed locally for private repos |
| `POLYGON_API_KEY` | *(none)* | Polygon key for options snapshots/contracts |
| `TRADIER_TOKEN` | *(none)* | Tradier token for primary spot quotes |
| `REALIZED_VOL_RANGE` | `2y` | Yahoo lookback for realized-vol panel |
| `REALIZED_VOL_EWMA_LAMBDA` | `0.94` | EWMA decay for realized-vol panel |
| `BORROW_HISTORY_MAX_COMMITS` | `400` | Max commits to walk when reconstructing borrow_history.json |

A long tail of `POLYGON_*` / `TRADIER_*` / `OPTIONS_*` env vars control the options-cache shard (rate limits, strike bands, expiries, moneyness). Defaults are tuned for GitHub Actions minute budgets — see `scripts/build_data.py` `# Config` block at the top of the file for the full list.

### Spot fallback order

For each symbol in `options_cache.json`:

1. Tradier spot quote (if `TRADIER_TOKEN` present and quote available)
2. Polygon-derived spot (`underlying_asset.price`, then Polygon spot endpoints)
3. Prior cached symbol entry from previous `data/options_cache.json`

If Polygon returns HTTP 429 for snapshots, the contracts fallback is skipped for that symbol in the same run, and stale cache is preferred to preserve request budget.

For symbols listed in `TRADIER_CHAIN_SYMBOLS`, the builder requests Tradier chains first (no websocket), then falls back only if needed.

The options workflow can override shard settings (e.g., 5-minute cadence with smaller `OPTIONS_SYMBOLS_PER_RUN`) to improve freshness while keeping request budgets bounded.

## ETF Metrics Coverage (All ETFs)

The ETF metrics pipeline covers the full universe with a provider cascade:

1. `tradr_axs` for Tradr/AXS symbols (`NSDEAXS2` + holdings fallback)
2. `polygon` fallback for non-Tradr ETFs (daily close + shares/market-cap metadata)

This ensures Stats-tab NAV/AUM/shares fields are populated for substantially more symbols across the entire dashboard universe.

## Where to Look First

| Task | File |
|------|------|
| Add a new column to the main table | `index.html` → `COLS` array (~line 2235) |
| Change the headline `Exp. decay` routing | `index.html` → `expectedDecayHeadlineValue`, `expectedDecayDisplayLabel`, `decayDistTooltip` (~line 1670–1750) |
| Change the YieldBOOST decay distribution | `ls-algo/yieldboost_decay.py` (server-side put-spread Monte-Carlo) — front-end just consumes `dist.p50/p10/p90` |
| Change the YieldBOOST client-side fallback | `index.html` → `yieldBoostIntrinsicAnnualDecay` (~line 1631) — only used when server distribution missing |
| Change the put-spread scenario model | `index.html` → `expectedPutSpreadLossWeekly`, `estimateIncomeStyleScenarioReturn` (~line 1790–1845) |
| Change the net-edge anchor-shift gate | `ls-algo/screener_v2_fields.enrich_screener_v2_fields` (looks at `expected_decay_available` + valid `expected_gross_decay_p50_annual`) |
| Add a new product_class | `ls-algo/screener_v2_fields.py` → `_product_class`, `_EXPECTED_DECAY_CLASSES` |
| Plumb a new CSV column to the SPA | `scripts/build_data.py` → `rec = { … }` block, then read it in `index.html` |
| Plumb a new CSV column to FastAPI | `backend/models.py` → `ETFRecord`, then `backend/main.py` → `_build_records_from_csv` |
| Tweak bucket logic | `backend/universe.py` → `assign_buckets` (server side) **and** `scripts/build_data.py` (static side — the source of truth for prod) |
| Investigate a stuck CI deploy | `.github/workflows/build-and-deploy.yml` (note: `build_data` step is `continue-on-error`; Pages still deploys with last-known-good JSON) |

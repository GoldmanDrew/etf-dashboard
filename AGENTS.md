# AGENTS.md — Architecture & Onboarding Guide

This file is the **single source of truth** for any future agent (AI or human) working on the ETF Borrow Dashboard. Read this first. Everything else in the repo (the Pydantic models, the React SPA, the build script, the GitHub Actions) is downstream of the concepts here.

If you only have time for one section, read **§3 Data Flow** and **§5 Product Taxonomy**.

---

## Contents

1. [System overview](#1-system-overview)
2. [Two-repo split: ls-algo vs etf-dashboard](#2-two-repo-split-ls-algo-vs-etf-dashboard)
3. [Data flow](#3-data-flow-end-to-end)
4. [Decay models — the math you need to know](#4-decay-models--the-math-you-need-to-know)
5. [Product taxonomy & routing matrix](#5-product-taxonomy--routing-matrix)
6. [The ten data files in `data/`](#6-the-ten-data-files-in-data)
7. [Schema map — every column the dashboard reads](#7-schema-map--every-column-the-dashboard-reads)
8. [`scripts/build_data.py` walkthrough](#8-scriptsbuild_datapy-walkthrough)
9. [`index.html` (React SPA) component map](#9-indexhtml-react-spa-component-map)
10. [`backend/` (FastAPI dev server)](#10-backend-fastapi-dev-server)
11. [GitHub Actions deployment topology](#11-github-actions-deployment-topology)
12. [Common tasks — recipes](#12-common-tasks--recipes)
13. [Gotchas & pitfalls](#13-gotchas--pitfalls)
14. [Recent changes & invariants you must preserve](#14-recent-changes--invariants-you-must-preserve)

---

## 1. System overview

The dashboard tracks **leveraged, inverse, volatility, and YieldBOOST income ETFs** for a short-borrow strategy. For every ticker we want to know:

1. **Borrow rate** — what does it cost to short? (IBKR FTP feed, refreshed every 10 min)
2. **Gross decay** — how fast does the ETF lose value to volatility drag, expense ratio, tracking error, and (for income strategies) put-spread NAV decay? (multiple models, see §4)
3. **Net edge** — `gross_decay − borrow_cost`, with both sides bootstrapped to give a fan of quantiles. (block bootstrap on log-drag + weighted resample on borrow history)
4. **Scenario behaviour** — what does the ETF do under a 0%, ±σ, ±3σ underlying shock at 1M / 3M / 6M / 1Y horizons?

Everything else (the News tab, the Stats tab, the Trade Lab, options chains) is supporting context for those four numbers.

The dashboard is **deployed as a fully static site** to GitHub Pages. The "backend" is a daily GitHub Action that runs `scripts/build_data.py`, which compiles all of the above into a single `data/dashboard_data.json` file. The React SPA in `index.html` reads that JSON at page-load and renders. **No server is involved in production.** A FastAPI mirror exists in `backend/` for local dev only.

---

## 2. Two-repo split: ls-algo vs etf-dashboard

There are **two repositories** that produce this site. Most code changes touch only one of them. Knowing which is which is the most important thing on this page.

```
GoldmanDrew/ls-algo                 GoldmanDrew/etf-dashboard
─────────────────────               ──────────────────────────
daily_screener.py                   scripts/build_data.py
screener_v2_fields.py               index.html
decay_distribution.py               backend/{main,models,…}.py
tests/                              tests/
data/etf_screened_today.csv  ───►   data/etf_screened_today.csv (cached)
                                    data/dashboard_data.json
                                    GitHub Pages site
```

| Lives in `ls-algo` | Lives in `etf-dashboard` |
|---|---|
| Universe selection (which tickers we cover) | UI / charting / tabs |
| Beta + leverage estimation | Bucket assignment for the UI (β cutoffs) |
| Realized volatility and gross decay calc | Borrow-rate refresh (IBKR FTP) |
| HARQ-Log distributional decay model | Borrow history reconstruction from git |
| Net edge bootstrap | Borrow-spike risk model |
| `product_class` taxonomy + `expected_decay_available` flag | YieldBOOST put-spread Monte-Carlo decay (server-side, `yieldboost_decay.py`) |
| Anchor-shift bootstrap (recenters net-edge on expected p50) | Front-end YB intrinsic helper (only used as fallback) |
| Volatility-ETP empirical roll/tracking adjustment | Scenarios tab (vol/shock grid) |
| Distribution / yield aggregation for income ETFs | Trade Lab + options snapshot integration |
| `etf_screened_today.csv` schema | `dashboard_data.json` schema |

**The boundary:** `ls-algo` produces a CSV. `etf-dashboard` consumes that CSV, augments it with live borrow + options data, and renders the UI.

If you change a column in the CSV (e.g., add a new field, rename one, change semantics), you almost always need to touch **both repos in sequence**:

1. Update the schema in `ls-algo/screener_v2_fields.py` (or the relevant module).
2. Re-run `daily_screener.py`, sanity-check `data/etf_screened_today.csv`.
3. Push to `ls-algo` main.
4. Pull / refetch in `etf-dashboard`, update `scripts/build_data.py` to pass the new field through to `dashboard_data.json`.
5. Update `backend/models.py` + `backend/main.py` (FastAPI) to mirror the field.
6. Update `index.html` to render the field.
7. Push to `etf-dashboard` main → GitHub Pages redeploys automatically.

Steps 4–7 are the "etf-dashboard sweep". You will do this often. There is a recipe for it in §12.

---

## 3. Data flow (end-to-end)

```
┌────────────────────────────── ls-algo (upstream) ──────────────────────────────┐
│                                                                                │
│   Yahoo / IBKR / yfinance ─► daily_screener.py                                 │
│                                ├─ load universe (manual list + screens)        │
│                                ├─ download prices (1y+ history)                │
│                                ├─ fit beta vs underlying (OLS)                 │
│                                ├─ compute realized gross decay (mean log-drag) │
│                                ├─ compute simple-Itô expected decay            │
│                                ├─ compute HARQ-Log distributional decay        │
│                                │     (decay_distribution.py)                   │
│                                ├─ enrich w/ schema v2 + bootstrap net_edge_*   │
│                                │     (screener_v2_fields.enrich_…)             │
│                                ├─ tag product_class + expected_decay_available │
│                                ├─ Step 5d: passive_low_beta nulling policy     │
│                                └─ write data/etf_screened_today.csv            │
│                                                                                │
│   git push to GoldmanDrew/ls-algo:main                                         │
└────────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     │ ls-algo CSV is publicly fetchable from
                                     │ raw.githubusercontent.com
                                     ▼
┌────────────────────── etf-dashboard (this repo, prod) ─────────────────────────┐
│                                                                                │
│   .github/workflows/build-and-deploy.yml  (daily 4:30 PM ET, push, manual)     │
│                                                                                │
│   scripts/build_data.py                                                        │
│     1. fetch ls-algo CSV → data/etf_screened_today.csv                         │
│     2. parse CSV → DataFrame                                                   │
│     3. fetch IBKR FTP (usa.txt) → borrow rate, fee, rebate, shares avail.      │
│     4. realized vol panel from Yahoo (EWMA, σ_W, σ_M, σ_Y)                    │
│     5. reconstruct borrow_history.json by walking screener git commits         │
│     6. borrow-spike risk model → borrow_spike_risk.json                        │
│     7. options refresh shard (Polygon + Tradier) → options_cache.json          │
│     8. assign UI buckets (1=high-β, 2=low-β/income, 3=inverse)                 │
│     9. build per-symbol record dict (passes through product_class,             │
│        expected_decay_available, all schema-v2 cols, net_edge_*, …)            │
│    10. write data/dashboard_data.json                                          │
│    11. commit data/* and push                                                  │
│    12. assemble _site/ (index.html + assets/ + data/) and deploy to Pages      │
│                                                                                │
│   .github/workflows/refresh-borrow.yml      (every 10 min, --borrow-only)      │
│   .github/workflows/refresh-options.yml     (every 5 min,  --options-only)     │
│   .github/workflows/update-etf-metrics.yml  (daily 5:00 AM ET)                 │
│   .github/workflows/update-corporate-actions.yml (every 6 h)                   │
│                                                                                │
│   PUBLIC SITE: https://goldmandrew.github.io/etf-dashboard/                    │
│     ├─ fetches data/dashboard_data.json on load                                │
│     ├─ fetches data/borrow_history.json on demand (chart page)                 │
│     ├─ fetches data/options_cache.json on demand (chart + trade-lab)           │
│     ├─ fetches data/etf_metrics_*.json on demand (Stats tab)                   │
│     ├─ fetches data/corporate_actions.json + etf_news.json (News tab)          │
│     └─ fetches data/etf_distributions.json (Total-Return NAV chart)            │
└────────────────────────────────────────────────────────────────────────────────┘
```

The **critical invariant**: `data/dashboard_data.json` is the only file the SPA needs to render the main grid. Everything else is optional context fetched on-demand by individual panels.

---

## 4. Decay models — the math you need to know

We have **five** different "expected decay" values in play. Knowing which one drives which UI cell saves a huge amount of debugging time.

### 4.1 Realized log-drag (always shown)

For every ETF with ≥ 40 days of overlapping history with its underlying:

```
daily_drag_t = β · log(R_und_t) − log(R_etf_t)
gross_decay_annual = mean(daily_drag_t) · 252
```

- Lives in `gross_decay_annual` (CSV) → `Gross (realized)` column on the main table.
- This is **always** shown if available. It is the only number we ship for `passive_low_beta` and `other_structured` rows where the model-based expected decay is meaningless.

### 4.2 Simple Itô identity (legacy fallback)

```
expected_gross_decay_simple_ito = (β² − β)/2 · σ²_annual
```

- Lives in `expected_gross_decay_simple_ito_annual`.
- This is the **fallback** when the distributional model is unavailable.
- It is **deliberately suppressed** for `passive_low_beta` because (β² − β)/2 collapses near β ≈ 1, producing near-zero decay that misleadingly suggests free money. See `daily_screener.py` Step 5d.

### 4.3 Volatility-ETP empirical roll/tracking adjustment

For UVIX, SVIX, UVXY, SVXY, VXX, VIXY, VIXM, VIX, VIX1D, VIX3M:

```
expected_gross_decay = simple_ito + empirical_roll_tracking_component
```

- Lives in `expected_gross_decay_annual` for these symbols.
- The adjustment captures the contango roll-down that the simple Itô identity completely misses.
- Tagged with `product_class = "volatility_etp"` and `expected_decay_model = "volatility_etp_empirical_roll_adjusted"`.

### 4.4 HARQ-Log anchored empirical lognormal (the headline model)

This is the model `ls-algo/decay_distribution.py` builds. It produces a **distribution**, not a point.

In one paragraph:

> For each ETF/underlying pair, take 1y of daily underlying squared log returns r²_t. Winsorize at `_R2_WINSOR_CAP` (default 6 σ). Compute realized variance `RV_t` and realized quarticity `RQ_t` over rolling 21-day windows. Fit a HARQ-Log model: `log(IV_T) ~ log(RV_t) + log(RV_5d) + log(RV_22d) + sqrt(RQ_t)`. Use the fitted residuals to back out an empirical distribution of `log(IV_T)` at the 1-year horizon. Plug each quantile of `IV_T` into the simple Itô identity (with the leverage cb-factor `(β² − β)/2`) to get a quantile of expected decay. Cap the lognormal sigma at `_LOG_IV_SIGMA_ANNUAL_CAP` (default `log(8)`) for plausibility.

The resulting CSV columns (per symbol):

| Column | Meaning |
|---|---|
| `expected_gross_decay_p10_annual` | 10th percentile of forecasted 1y gross decay |
| `expected_gross_decay_p50_annual` | **median** — drives the headline "Exp. decay" cell on the dashboard |
| `expected_gross_decay_p90_annual` | 90th percentile |
| `expected_gross_decay_mean_annual` | mean (lognormal: ≠ median) |
| `expected_logIV_mu_annual` | μ of `log(IV_T)` |
| `expected_logIV_sigma_annual` | σ of `log(IV_T)`, capped |
| `expected_gross_decay_dist_model` | `harq_log_anchored` / `empirical_lognormal` / `simple_ito_fallback` / `no_drag_beta` / `yieldboost_put_spread` / `yieldboost_put_spread_point` |
| `expected_gross_decay_dist_n_obs` | days of underlying history used |
| `expected_gross_decay_dist_horizon_days` | typically 252 |

If the underlying has too thin a panel (`_HORIZON_PANEL_RATIO_MIN`), the model falls back to `simple_ito_fallback` and the p10/p50/p90 are NaN.

### 4.5 YieldBOOST put-spread Monte-Carlo decay distribution

YieldBOOST income ETFs (AMYY, AZYY, BBYY, COYY, CWY, FBYY, HMYY, HOYY, IOYY, MAAY, MTYY, MUYY, NUGY, NVYY, PLYY, QBY, RGYY, RTYY, SEMY, SMYY, TMYY, TQQY, TSYY, XBTY, YSPY) work by writing a weekly 95/88 SPX-style put-spread on top of a 2× LETF. Their realized β is ~0.4–0.6, so the simple Itô / HARQ-Log vol-drag estimate of ~2–3% is **wildly wrong**. The actual NAV bleeds the put-spread premium every week.

As of the YB-unification refactor, this is now modeled **server-side** in `ls-algo/yieldboost_decay.py` and exported through the **same** `expected_gross_decay_p10/p50/p90/mean_annual` columns that every other product class uses. The front-end therefore reads `dist.p50` for the headline cell exactly like LETF / inverse / vol-ETP — no special-case fork.

```
# yieldboost_decay.yieldboost_decay_distribution
sigma_annual_draws ~ Lognormal(μ_logIV, σ_logIV)         # HARQ-Log moments of underlying
weekly_loss = ∫ max(0.95 − R, 0) − max(0.88 − R, 0)      # under 2× lognormal R
              for R | sigma_annual_draws
annual_decay = 1 − (1 − weekly_loss − ER_per_week)^52
expected_gross_decay_p10/p50/p90/mean_annual = quantiles(annual_decay)
```

with `expense_ratio_annual = 0.99%`. When the underlying's TR history is too short for a full HARQ-Log fit, `yieldboost_decay_point_estimate` is used instead, producing a single σ point (p10 = p50 = p90); the `expected_gross_decay_dist_model` column then reads `yieldboost_put_spread_point`.

**Where this surfaces on the dashboard:**

- Headline `Exp. decay` cell — `dist.p50` (sub-label `Exp. (median)`); same code path as every other product class.
- Chart-page detail header (`chart-stat`) — same `dist.p50` value with sub-label `put-spread MC · p10/p90 …`.
- Tooltip — calls out `yieldboost_put_spread` / `yieldboost_put_spread_point` model and notes that the Net edge bootstrap is anchor-shifted to this p50.
- Income Scenarios table (`Intrinsic NAV Decay` column) — still uses the front-end `yieldBoostIntrinsicAnnualDecay` mechanism for shock/horizon combinations.

The front-end helper `yieldBoostIntrinsicAnnualDecay` survives only as a **fallback** for rows where the server didn't ship the distribution (e.g. mid-day pipeline rerun, missing σ).

Routing logic: `index.html` → `expectedDecayHeadlineValue(r)` → `expectedDecayDistribution(r)` → `dist.p50` (same as every other class). YB fallback fires only when `dist` is null.

### 4.6 Net edge bootstrap with anchor-shift to expected p50

`screener_v2_fields.enrich_screener_v2_fields` block-bootstraps the daily log-drag time series **and** weighted-resamples the borrow-rate history (with stress correlation grid `ρ ∈ {0.0, 0.2, 0.4}`). On top of the empirical block bootstrap, the realized gross draws are then **anchor-shifted** to re-center their mean on the forward-looking forecast `expected_gross_decay_p50_annual`:

```
gross_draws_shifted = gross_draws − mean(gross_draws) + expected_gross_decay_p50_annual
```

The result: dispersion (block autocorrelation, regime mixing, vol clustering) is preserved from history, but the *location* of the distribution is the forecast. For LETF / inverse / volatility_etp the anchor is the HARQ-Log p50; for `income_yieldboost` it is the put-spread MC p50. The shift is gated on `expected_decay_available = True` AND a finite anchor — so `passive_low_beta` rows are deliberately left realized-only ("expected decay is N/A → don't pretend we have a structural edge").

Diagnostics on every row:

- `gross_anchor_shift_annual` — signed shift applied (NaN if not applied).
- `gross_anchor_target_annual` — the p50 anchor used (NaN if not applied).
- `gross_anchor_source` — `harq_log_anchored` / `empirical_lognormal` / `yieldboost_put_spread` / `yieldboost_put_spread_point` / `realized_only_passive_low_beta` / `realized_only_no_anchor`.
- `copula_note` — appends `anchor_shift_to_expected_p50=±X.XXXX` when the shift fired.

Output (always present):

```
net_edge_p05_annual / p25 / p50 / p75 / p95 / hist_json
```

`net_edge_hist_json` is a compact (bin_centers, counts) histogram of the bootstrap draws — the dashboard's `NetEdgeBootstrapViz` component renders it as a small density curve on the chart page.

**Sign convention:** **short-favorable positive**. A `net_edge_p50 = +0.10` means a 10% annual structural edge to the short, after borrow.

`gross_edge_definition` per class: `letf` / `inverse` / `volatility_etp` / `income_yieldboost` → `blended_realized_expected`; `passive_low_beta` / `income_put_spread` → `realized_daily_log_drag`.

For `passive_low_beta`, the front-end **also** masks the headline `Net edge` cell to `—` because the policy is "expected decay is N/A → fall back to realized → don't pretend we have a structural edge".

---

## 5. Product taxonomy & routing matrix

The single most important variable in this codebase is `product_class`. It determines what the dashboard renders for each row.

The taxonomy lives in `ls-algo/screener_v2_fields.py`:

```python
def _product_class(lev, beta, *, is_yieldboost=False):
    if is_yieldboost:           return "income_yieldboost"
    if beta < 0:                return "inverse"
    if beta > 1.5:              return "letf"
    if 0 < beta <= 1.5:         return "passive_low_beta"
    if abs(lev - 1.0) < 0.01:   return "income_put_spread"
    if lev is finite:           return "letf"
    return "other_structured"
```

There is **also** a `volatility_etp` class — it is assigned by `scripts/build_data.py` and `backend/main.py` based on a hard-coded symbol list (UVIX, SVIX, UVXY, SVXY, VXX, VIXY, VIXM, VIX, VIX1D, VIX3M). It overrides whatever `_product_class` produced.

`expected_decay_available` is a derived boolean flag emitted alongside `product_class`:

```python
_EXPECTED_DECAY_CLASSES = {"letf", "inverse", "income_yieldboost",
                           "income_put_spread", "volatility_etp"}
expected_decay_available = product_class in _EXPECTED_DECAY_CLASSES
```

`passive_low_beta` and `other_structured` are deliberately `False`.

### Full routing matrix

The dashboard reads `product_class` and `expected_decay_available` and routes every cell on the row through the following table:

| `product_class` | `Gross (realized)` | `Exp. decay` cell | Sub-label | `Net edge` cell | Scenarios tab | Chart-page pill |
|---|---|---|---|---|---|---|
| `letf` | realized | HARQ-Log p50 | `p50 · p10/p90` | bootstrap fan | LETF vol-drag grid | `LETF` |
| `inverse` | realized | HARQ-Log p50 | `p50 · p10/p90` | bootstrap fan | LETF vol-drag grid | `Inverse` |
| `volatility_etp` | realized | simple Itô + roll/tracking | `vol-adj` | bootstrap fan | LETF vol-drag grid | `Volatility ETP` |
| `income_yieldboost` | realized | **Put-spread MC p50** | `put-spread · p10/p90` | bootstrap fan, **anchor-shifted to put-spread p50** | **Income put-spread grid** | `YieldBOOST (income)` |
| `income_put_spread` | realized | HARQ-Log p50 | `p50 · p10/p90` | bootstrap fan (anchor-shifted when expected p50 available) | LETF vol-drag grid | `Income (put-spread)` |
| `passive_low_beta` | realized | **`—` (N/A)** | `passive low-β: N/A by policy` | **`—` (N/A)** | hidden | `Passive low-β` |
| `other_structured` | realized | `—` | — | realized fallback | hidden | `Structured` |

The front-end helpers that implement this:

- `expectedDecayAvailableForRow(r)` — checks `expected_decay_available` flag with a fallback for older builds (`product_class !== passive_low_beta && product_class !== other_structured`).
- `isYieldBoostIncomeStrategy(r)` — checks `is_yieldboost === true` or the YIELDBOOST_INCOME_PAIRS Set (~25 known sym/und pairs).
- `isVolatilityEtp(r)` — checks `product_class === "volatility_etp"` or symbol/underlying in VOLATILITY_ETP_SYMBOLS.
- `isPassiveLowBetaRow(r)` — checks `product_class === "passive_low_beta"` or `expected_decay_available === false` or bucket-2 + non-YieldBOOST + non-volatility-ETP.

`expectedDecayHeadlineValue(r)` is the central decision function. Its order of operations:

```
if !expectedDecayAvailableForRow(r): return null   // → "—"
if isYieldBoostIncomeStrategy(r):   return yieldBoostIntrinsicAnnualDecay(r)
if expectedDecayDistribution(r):    return dist.p50
return expectedDecayDisplayValue(r) // simple Itô / vol-ETP adjusted
```

This same function is reused for **sorting** the column. If you change the rendering, you must keep this function consistent.

---

## 6. The ten data files in `data/`

| File | Producer | Consumer | Refresh cadence |
|---|---|---|---|
| `dashboard_data.json` | `build_data.py` | `index.html` (always loaded) | daily, plus `--borrow-only` every 10 min |
| `etf_screened_today.csv` | `ls-algo/daily_screener.py` | `build_data.py` | daily (cached copy of upstream) |
| `borrow_history.json` | `build_data.py` (walks ls-algo git history) | `ChartPage`, `ls-algo` net-edge bootstrap | daily |
| `borrow_spike_risk.json` | `build_data.py` (spike model) | `ChartPage`, `BorrowMonitor` | daily |
| `borrow_spike_predictions/<DATE>.json` | `build_data.py` (full + `--borrow-only`) | `scripts/score_borrow_spikes.py` | daily / every 10 min |
| `borrow_spike_realized.jsonl` | `scripts/score_borrow_spikes.py` | ops / calibration audits | append on workflow |
| `borrow_spike_metrics.json` | `scripts/score_borrow_spikes.py` | Brier, log-loss, band calibration rollup | daily / borrow refresh |
| `options_cache.json` | `build_data.py --options-only` | `ChartPage`, `Trade Lab` | every 5 min (sharded) |
| `etf_metrics_daily.{parquet,csv,json}` | `ingest_etf_metrics.py` | Stats tab (NAV/AUM/shares chart) | daily 5 AM ET |
| `etf_metrics_latest.json` | `ingest_etf_metrics.py` | Stats tab (snapshot panel) | daily 5 AM ET |
| `etf_distributions.json` | `ingest_distributions.py` | Stats tab Total-Return NAV line | daily 5 AM ET |
| `corporate_actions.json` | `ingest_corporate_actions.py` | News tab pinned events | every 6 h |
| `etf_news.json` | `ingest_corporate_actions.py` | News tab feed | every 6 h |
| `investors.json` | manual (bcrypt via `hash_investor_password.py`) | `LoginScreen` | manual |
| `polygon_ticker_events_cache.json` | `ingest_corporate_actions.py` | (internal) | every 6 h |
| `ticker_alias_map.json` | manual | `ingest_corporate_actions.py` | manual |
| `etf_holdings_daily.parquet` + `etf_holdings_latest.csv` | `etf_holdings_providers.py` | (internal, used for Tradr/AXS NAV reconstruction) | daily |
| `nav_forecasts/_latest.json` | `scripts/forecast_nav.py` | Stats tab Fair-value card | every 30 min Mon-Fri 13:00-22:00 UTC |
| `nav_forecasts/_metrics_daily.json` | `scripts/score_nav_forecasts.py` | Stats tab "Forecast accuracy (20d)" cell | daily 5 AM ET |
| `nav_forecasts/_history_panel.json` | `scripts/score_nav_forecasts.py` | Stats tab NAV-vs-model overlay chart | daily 5 AM ET |
| `nav_forecasts/_anchors.json` | `scripts/score_nav_forecasts.py` | `scripts/forecast_nav.py` (input only) | daily 5 AM ET |
| `nav_forecasts/snapshots/<DATE>.jsonl` | `scripts/forecast_nav.py` | `scripts/score_nav_forecasts.py`; offline audits | every 30 min |
| `nav_forecasts/realized/<DATE>.jsonl` | `scripts/score_nav_forecasts.py` | rolling-stats build | daily 5 AM ET |

**NAV realized rollup:** `score_nav_forecasts.py` collects every `YYYY-MM-DD.jsonl` under `nav_forecasts/realized/` (including nested paths from older CI copies), flattens duplicates into the top-level file, then builds `_metrics_daily.json` / `_history_panel.json`.

`dashboard_data.json` is large (~2.9 MB) because it contains every row plus the bootstrap histogram blobs. Keep it that way — the SPA expects to fetch a single primary JSON.

**`nav_forecasts/` is its own self-describing folder** — see `data/nav_forecasts/README.md` for the full schema. The flow is: `forecast_nav.py` snapshots **one record per `(symbol, model)` every 30 min**, computing whichever of `delta_v1`, `delta_v2_ito`, `delta_v3_swap_mark`, and `yieldboost_putspread_v1` have inputs available. It uses `_anchors.json` (refreshed nightly with `nav_close`, `und_close`, `shares_outstanding`) + live spot / option chains from `options_cache.json` + per-leg holdings from `etf_holdings_latest.json` (the holdings-aware models difference shares × (spot_now − spot_anchor) per leg — see the README's "Why delta differencing" note). A product-class-aware dispatcher picks one model as default per row (income → `yieldboost_putspread_v1` first; everything else → `delta_v3_swap_mark` first; `delta_v2_ito` and `delta_v1` are fallbacks). Candidates whose `nav_hat / nav_anchor` falls outside [0.5, 2.0] are rejected by a sanity envelope; if every candidate violates that envelope `by_symbol[SYM]` is omitted from `_latest.json` and the Stats card shows `—`. `score_nav_forecasts.py` runs after the daily NAV ingest, scores per `(symbol, model)`, rolls 60-trading-day accuracy by `(symbol, model)` into `_metrics_daily.json` + `_history_panel.json` (each exposing a `by_symbol[SYM]` view tied to the routed default model and a full `by_symbol_models[SYM][MODEL]` grid for A/B comparison), and writes the next session's anchors. Confidence per row is `high` (fresh inputs, full coverage), `medium` (stale spot or sub-50% leg coverage), or `na` (model not applicable / inputs missing). Add a new model by appending a `compute_<tag>` + `build_<tag>` to `forecast_nav.py` and slotting its tag into `select_default_model`'s preference list — the scorer rolls it up automatically without schema breakage.

---

## 7. Schema map — every column the dashboard reads

`backend/models.py::ETFRecord` is the source of truth for the schema the SPA consumes. Every field there has a corresponding column in `etf_screened_today.csv` (most), or is computed in `build_data.py` (borrow refresh, bucket assignment, scenario inputs).

The high-importance fields, grouped by purpose:

### Identity
- `symbol`, `underlying`, `leverage`, `expected_leverage`, `beta`, `beta_n_obs`, `bucket`
- `asof_date`, `last_updated`, `is_stale`
- `product_class`, `expected_decay_available`, `is_yieldboost`, `scenario_style`

### Borrow
- `borrow_fee_annual`, `borrow_rebate_annual`, `borrow_net_annual`
- `shares_available`, `borrow_current`, `borrow_spiking`, `borrow_missing`
- `borrow_median_60d`, `borrow_for_net_annual`, `borrow_resample_mode`
- `borrow_weight_halflife_days`, `borrow_history_points_used`

### Decay / analytics (the heart of it)
- `gross_decay_annual` (realized — always shown)
- `expected_gross_decay_annual` (model-aware point estimate, or simple Itô fallback)
- `expected_gross_decay_simple_ito_annual` (raw simple Itô, before any adjustment)
- `expected_gross_decay_adjusted_annual` (= `expected_gross_decay_annual` for vol ETPs)
- `expected_decay_adjustment_annual` (the empirical roll/tracking add-on for vol ETPs)
- `expected_decay_model` (`simple_ito` / `volatility_etp_empirical_roll_adjusted` / …)
- `expected_gross_decay_reliable` (boolean — false for vol ETPs, false for passive low-β)
- `blended_gross_decay` (legacy weighted avg of realized + expected; kept for back-compat)

### Distributional decay (HARQ-Log)
- `expected_gross_decay_p10_annual`, `_p50_annual`, `_p90_annual`, `_mean_annual`
- `expected_logIV_mu_annual`, `expected_logIV_sigma_annual`
- `expected_gross_decay_dist_model` — one of:
  - `harq_log_anchored_empirical`
  - `simple_ito_fallback` (panel too thin)
  - `passive_low_beta_na` (policy-suppressed)
- `expected_gross_decay_dist_n_obs`, `expected_gross_decay_dist_horizon_days`

### Net edge (bootstrap fan)
- `net_edge_p05_annual`, `_p25_annual`, `_p50_annual`, `_p75_annual`, `_p95_annual`
- `net_edge_hist_json` (compact bin-centers + counts JSON string for the histogram viz)
- `net_edge_fan_label` (computed in `build_data.py` for alt-text)
- `gross_edge_definition` (`realized_daily_log_drag` / `blended_realized_expected` / `expected_only`)
- `primary_edge_annual`, `gross_for_primary_annual`
- `block_len`, `B_reps`, `annualization_key`, `hac_lag`, `sigma_b_annual`, `stress_borrow_rho`
- `borrow_dispersion_type`

### Income ETF specifics
- `income_yield_trailing_annual`, `income_yield_recent_annual`
- `income_distribution_count_1y`, `income_latest_distribution`, `income_latest_ex_date`

### Diagnostics / regime
- `high_intraday_risk`, `regime_autocorr_und_21d_proxy`, `regime_warning`
- `decomposition_note` (strings like `income_dist_missing`, `low_beta_realized_only`)
- `copula_note`, `copula_type`
- `schema_v` (currently `2`), `edge_sign_convention` (`short_favorable_positive`)

### Algo flags
- `include_for_algo`, `protected`, `cagr_positive`

### Trailing volatility (computed in `build_data.py`, not in CSV)
- `vol_underlying_annual`, `vol_etf_annual` (and shorter-horizon variants)

When you add a field to ls-algo's CSV, the typical path is: CSV → `build_data.py` rec dict → `dashboard_data.json` → `index.html`. If you also want it to flow through the FastAPI dev server, add it to `backend/models.py::ETFRecord` and `backend/main.py::_build_records_from_csv`.

---

## 8. `scripts/build_data.py` walkthrough

This is the single most important file in this repo. ~2840 lines. Don't be intimidated — most of it is provider-specific glue. The high-level structure:

1. **Top of file — config block (`UNIVERSE_REPO`, `POLYGON_*`, `TRADIER_*`, `OPTIONS_*`)**: every env var the build is sensitive to. Defaults are tuned for GitHub Actions minute budgets.

2. **`fetch_universe_csv()`**: pulls `etf_screened_today.csv` from `ls-algo:main` via the GitHub Contents API (or anonymous raw URL if no token). Caches to `data/etf_screened_today.csv`.

3. **`fetch_borrow_snapshot()`**: connects to IBKR FTP `ftp2.interactivebrokers.com/usa.txt`, parses the file, returns `{symbol: (fee, rebate, net, shares_available, borrow_current)}`.

4. **`fetch_realized_vol_panel()`**: yfinance bulk download. Computes `σ_W`, `σ_M`, `σ_Y`, EWMA σ for each symbol.

5. **`reconstruct_borrow_history()`**: walks the last `BORROW_HISTORY_MAX_COMMITS` commits of `ls-algo:data/etf_screened_today.csv`, extracts daily borrow per symbol, writes `borrow_history.json`. This is the input that powers the weighted-resample option in ls-algo's net-edge bootstrap.

6. **`build_borrow_spike_risk()`**: rolling stats on the borrow history → spike-risk scores per symbol → `borrow_spike_risk.json`.

7. **Options refresh (`--options-only` capable)**: huge, throttled section. Tradier first for spot, Polygon for chains. Rate-limit-aware shard logic. Writes `options_cache.json`. Coverage is gated by `options_cache_diagnostics.py` in CI.

8. **`assign_buckets()`**: B1 (β > 1.5), B2 (rest), B3 (curated inverse list or β < 0).

9. **Per-symbol record build**: massive dict comprehension that combines CSV row + borrow snapshot + realized vol + bucket + product_class override (vol ETP). Calls `is_volatility_etp()` to override `product_class` and `expected_decay_model`. Reads `expected_decay_available` from CSV with the fallback `product_class not in ("passive_low_beta", "other_structured")`.

10. **`build()` writes `dashboard_data.json`** with structure:
    ```json
    {
      "generated_at": "2026-04-26T11:53:47Z",
      "high_beta_threshold": 1.5,
      "rows": [ { ETFRecord-shaped }, … ],
      "summary": { … },
      "freshness": { … }
    }
    ```

11. **`refresh_borrow_only()`** and **`refresh_options_only()`** are partial reruns that read the existing `dashboard_data.json` / `options_cache.json`, mutate only the relevant fields, and write back. They are what the 10-min and 5-min cron workflows call.

The CLI:

```bash
python scripts/build_data.py                 # full build
python scripts/build_data.py --borrow-only   # only refresh borrow rows
python scripts/build_data.py --options-only  # only refresh options shard
```

---

## 9. `index.html` (React SPA) component map

`index.html` is a single file (~245 KB / 4900 lines) with the React app inlined under a `<script type="text/babel">` tag. Babel-standalone compiles JSX in the browser at load. This is intentional — no build step, no Node toolchain, deploys cleanly to GitHub Pages.

Top-level structure:

```
index.html
├── <style> CSS variables, layout, table, chart styles            (lines 1–1234)
├── <body>
│   ├── React UMD + ReactDOM UMD CDN                              (lines 1240–1241)
│   ├── assets/expected_decay.js     (standalone Itô calculator)
│   ├── assets/scenario_returns.js   (vol/shock grid + LETF model)
│   ├── assets/trade_lab.js          (Black-Scholes + leg builder)
│   ├── assets/options_data.js       (options cache helpers)
│   ├── babel-standalone CDN
│   └── <script type="text/babel">                                (lines 1248–end)
│       ├── Constants
│       │   ├── DATA_URL = "data/dashboard_data.json"
│       │   ├── BORROW_HISTORY_URL, OPTIONS_CACHE_URL, …
│       │   ├── YIELDBOOST_INCOME_PAIRS (Set of "SYM|UND" strings)
│       │   ├── VOLATILITY_ETP_SYMBOLS  (Set of vol-ETP tickers)
│       │   └── COLS (main-table column definitions)
│       │
│       ├── Auth
│       │   ├── LoginScreen
│       │   ├── readAuthSession / writeAuthSession / clearAuthSession
│       │   └── timingSafeEqualBytes (bcrypt-style compare)
│       │
│       ├── Decay routing helpers (THE IMPORTANT ONES)
│       │   ├── expectedDecayDisplayValue(r)
│       │   ├── expectedDecayDistribution(r)        // p10/p50/p90 from CSV
│       │   ├── yieldBoostUnderlyingSigmaAnnual(r)
│       │   ├── yieldBoostIntrinsicAnnualDecay(r)
│       │   ├── isPassiveLowBetaRow(r)
│       │   ├── expectedDecayAvailableForRow(r)
│       │   ├── expectedDecayHeadlineValue(r)       // central router
│       │   ├── decayDistTooltip(r)
│       │   └── expectedDecayDisplayLabel(r)
│       │
│       ├── Scenario helpers
│       │   ├── normalCdf(x)
│       │   ├── expectedPutSpreadLossWeekly(...)
│       │   ├── estimateIncomeStyleScenarioReturn(...)
│       │   ├── realizedVolPointForScenarioColumn(r, range)
│       │   ├── computeExpectedEtfReturnFlatUnd(r, range, horizonLabel)
│       │   └── computeScenarioEtf3mFlatUnd(r, range)   // headline 3M wrapper
│       │
│       ├── Visualizations
│       │   ├── MonteCarloPlot
│       │   ├── SimpleSeriesPlot
│       │   ├── GrossDecayComparisonStrip       // realized vs expected
│       │   ├── NetEdgeBootstrapViz             // hist_json density curve
│       │   ├── NetEdgeFanGlyph                 // mini whisker plot
│       │   ├── NetEdgeMergedCell               // merged median + fan glyph
│       │   ├── NetEdgeQuantileTable            // p5/p25/p50/p75/p95 table
│       │   └── EdgeFactsStrip                  // diagnostic strip
│       │
│       ├── Tools
│       │   ├── SummaryCard
│       │   └── ExpectedDecayCalculator         // standalone Itô calculator
│       │
│       ├── Pages
│       │   ├── DashboardRoot                   // main grid
│       │   │   ├── Filter bar (bucket pills, search)
│       │   │   ├── Main table (uses COLS array)
│       │   │   └── DetailRow (expandable per-row)
│       │   ├── ChartPage                       // hash route #/chart/:symbol
│       │   │   ├── Detail header w/ bucket + product_class pill
│       │   │   ├── chart-stat row (5 cells: borrow, gross, expected, …)
│       │   │   ├── TradingView iframe (built by buildWidgetHtml)
│       │   │   ├── Borrow history chart
│       │   │   ├── Borrow spike risk
│       │   │   ├── Net Edge bootstrap histogram
│       │   │   ├── Scenarios tab (heatmap of 5 vols × 7 shocks × 4 horizons)
│       │   │   ├── Stats tab (NAV/AUM, distributions)
│       │   │   ├── Trade Lab tab (option leg builder)
│       │   │   └── Options chains tab
│       │   ├── InfoPage                        // hash route #/info
│       │   ├── NewsPage                        // hash route #/news
│       │   └── App                             // top-level router
│       └── ReactDOM.createRoot(...).render(<App/>)
```

### Where to make common UI changes

| Change | Where |
|---|---|
| Add a column to the main table | `COLS` array (line ~2235) |
| Change how `Exp. decay` cell renders | `expectedDecayHeadlineValue` + the `<td title={decayDistTooltip(r)}>` block in the main table render (search for "expected_gross_decay_p50_annual") |
| Change how `Net edge` cell renders | `NetEdgeMergedCell`, `NetEdgeFanGlyph` |
| Change the chart-page detail header | `ChartPage` near "chart-bucket-badge" |
| Change which σ feeds the Scenarios tab | `realizedVolPointForScenarioColumn(r, chartVolLookbackRange)` and the `chartVolLookbackRange` state in `App` |
| Tweak the YieldBOOST intrinsic decay | `yieldBoostIntrinsicAnnualDecay` and `expectedPutSpreadLossWeekly` |
| Change YieldBOOST symbol set | `YIELDBOOST_INCOME_PAIRS` constant |
| Change vol-ETP symbol set | `VOLATILITY_ETP_SYMBOLS` constant **AND** `backend/main.py::VOLATILITY_ETP_SYMBOLS` **AND** `scripts/build_data.py::is_volatility_etp` |

### State management

There is **no state library**. Everything is `useState` + `useEffect` + URL hash routing. Filtering / sorting / bucket selection lives in `DashboardRoot`. The chart timeframe (`chartVolLookbackRange`) lives in `App` and is propagated down for TradingView embeds and realized-vol diagnostics. The Scenarios tab and main-table `Exp. ETF return (3M)` default to `forecast_vol_underlying_annual` from `dashboard_data.json` (50/50 variance blend of model-implied σ and robust 6M EWMA when both exist), with chart EWMA only as a backward-compatible fallback for older data.

---

## 10. `backend/` (FastAPI dev server)

The FastAPI app exists for **local development only**. It is not in the deployment path. GitHub Pages does not run any server.

| File | Purpose |
|---|---|
| `backend/main.py` | FastAPI app + APScheduler (refresh borrow / decay / GitHub sync on intervals) |
| `backend/models.py` | Pydantic `ETFRecord`, `DashboardSummary`, `SystemStatus`, `Bucket` enum |
| `backend/universe.py` | Loads `data/etf_screened_today.csv`, normalizes columns, assigns buckets |
| `backend/decay.py` | Stahl-style decay metrics + a mock decay generator (for when no price history is available) |
| `backend/ibkr_fetcher.py` | IBKR FTP fetch + mock fallback |
| `backend/github_sync.py` | Pulls fresh CSV from `ls-algo:main` |
| `backend/db.py` | SQLite store for borrow history (dev-only; prod uses `borrow_history.json`) |

If you change the schema in the static path (`build_data.py` + `index.html`), you should also keep `backend/models.py` and `backend/main.py::_build_records_from_csv` in sync. They mirror the same logic, including the `expected_decay_available` flag derivation, the YieldBOOST detection, and the volatility-ETP override.

The dev server runs at `http://localhost:8000` via `python run.py` and exposes `/api/records`, `/api/summary`, `/api/status`, `/api/history/{symbol}`, etc.

---

## 11. GitHub Actions deployment topology

Five workflows, four cron schedules:

| Workflow | Cadence | What it does |
|---|---|---|
| `build-and-deploy.yml` | Daily 4:30 PM ET (Mon–Fri) + on every `push` to `main` | Full `build_data.py` rebuild + commit `data/*` + deploy `_site/` to GitHub Pages |
| `refresh-borrow.yml` | Every 10 min | `build_data.py --borrow-only` + commit `data/dashboard_data.json` |
| `refresh-options.yml` | Every 5 min | `build_data.py --options-only` + commit `data/options_cache.json` |
| `update-etf-metrics.yml` | Daily 5:00 AM ET | `ingest_etf_metrics.py` + `ingest_distributions.py` |
| `update-corporate-actions.yml` | Every 6 h | `ingest_corporate_actions.py` (writes `corporate_actions.json` + `etf_news.json`) |

**Critical safety rail:** `build-and-deploy.yml`'s `Build dashboard data` step is `continue-on-error: true`. If `build_data.py` flakes (Polygon 429, Yahoo timeout, IBKR FTP outage), the workflow falls back to whatever JSON is currently committed to `main` and still ships a Pages deploy. This means a transient API outage will not blank out the site — but it also means a bad data push **will** stick until the next successful build. Always sanity-check `data/dashboard_data.json` after committing.

The four refresh workflows commit JSON only and **do not** trigger a Pages deploy themselves. The deploy is consolidated to `build-and-deploy.yml` to avoid concurrency contention on the `pages-deploy` group.

### Required repo secrets

| Secret | Used by |
|---|---|
| `LS_ALGO_TOKEN` | `build-and-deploy.yml` (read CSV from ls-algo private repo) |
| `POLYGON_API_KEY` (or `POLYGON_IO_API_KEY`) | options + spot fallback |
| `TRADIER_TOKEN` | spot quotes + Tradier chains |

---

## 12. Common tasks — recipes

### 12.1 Add a new product_class to the taxonomy

1. **In `ls-algo`**: edit `screener_v2_fields.py::_product_class` to recognize the new class. Add it to `_EXPECTED_DECAY_CLASSES` if model-based expected decay is meaningful for it. If not, mirror the `passive_low_beta` policy in `daily_screener.py` Step 5d (null out the expected decay columns for these rows).
2. **In `ls-algo`**: add a regression test in `tests/test_product_class_taxonomy.py`.
3. **In `ls-algo`**: re-run `daily_screener.py`, sanity-check the CSV. Push to `main`.
4. **In `etf-dashboard`**: re-run `python scripts/build_data.py` (or just wait for the next daily build).
5. **In `etf-dashboard`**: extend `index.html`:
   - Add the class to the routing helper functions (`expectedDecayHeadlineValue`, `expectedDecayDisplayLabel`, `decayDistTooltip`, `isPassiveLowBetaRow` if applicable).
   - Add a pill label / tooltip on the chart-page detail header (search for `chart-bucket-badge` to find the pill block).
6. Push to `main` → Pages redeploys.

### 12.2 Add a new column from the screener CSV to the dashboard

1. **In `ls-algo`**: emit the column from `daily_screener.py` (or `screener_v2_fields.enrich_screener_v2_fields`).
2. **In `etf-dashboard/scripts/build_data.py`**: in the per-symbol record-build dict, add `"my_new_field": _v2f(row, "my_new_field"),` (or `_v2s` / `_v2bool`).
3. **In `etf-dashboard/backend/models.py`**: add `my_new_field: Optional[float] = None` to `ETFRecord` (mirrors the static path).
4. **In `etf-dashboard/backend/main.py::_build_records_from_csv`**: pass it through.
5. **In `etf-dashboard/index.html`**: read it from `r.my_new_field` in your component.

If the field is **derived** (computed in `build_data.py` rather than coming from the CSV), skip step 1.

### 12.3 Change which σ drives the Scenarios tab

The Scenarios tab uses `realizedVolPointForScenarioColumn(r, chartVolLookbackRange)` (in `index.html`). `chartVolLookbackRange` is a TIMEFRAMES key (`'1M'`, `'3M'`, `'6M'`, `'1Y'`, …), set by the timeframe pills above the chart. The function looks up the matching `vol_underlying_annual_*` field on the row — those are computed in `build_data.py::fetch_realized_vol_panel`.

To change the EWMA λ, set the env var `REALIZED_VOL_EWMA_LAMBDA` (default 0.94). To change the lookback range available, edit `TIMEFRAMES` in `index.html` and add the corresponding fields in `fetch_realized_vol_panel`.

### 12.4 Investigate a wrong number on a specific row

1. Pull `data/dashboard_data.json` and grep for the symbol. Check `product_class`, `expected_decay_available`, `is_yieldboost`, `bucket`.
2. If `Exp. decay` looks wrong: check `expected_gross_decay_p50_annual` (HARQ-Log p50), then `expected_gross_decay_simple_ito_annual` (Itô fallback), then for YieldBOOST run `yieldBoostIntrinsicAnnualDecay(r)` mentally with `vol_underlying_annual` as input.
3. If `Net edge` looks wrong: inspect `net_edge_p05_annual / p25 / p50 / p75 / p95` and `net_edge_hist_json`. The fan should be **short-favorable positive**.
4. If `Gross (realized)` looks wrong: that came from ls-algo's `gross_decay_annual`. Bug fix needs to happen in `daily_screener.py`.
5. If only the UI is wrong but the JSON is right: the bug is in `index.html`'s display logic (formatter, routing helper, COLS).

### 12.5 Push both repos in the right order after a cross-cutting change

```bash
# 1. Push ls-algo first (CSV producer)
cd ../ls-algo
git add -A && git commit -F .git/COMMIT_MSG.txt && git push

# 2. Pull the fresh CSV into etf-dashboard, rebuild
cd ../etf-dashboard
git pull
python scripts/build_data.py
git add -A && git commit -F .git/COMMIT_MSG.txt && git push
```

The order matters because `build_data.py` fetches the CSV from `ls-algo:main`. If you push `etf-dashboard` first, the next build will use the **old** CSV and your changes won't appear until the daily cron picks up the new one.

### 12.6 Run the full test suite

**ls-algo:**

```bash
cd ../ls-algo
python -m pytest tests/ -v
```

**etf-dashboard (Python):**

```bash
python -m pytest tests/test_bucketing.py tests/test_ingest_corporate_actions.py -v
```

**etf-dashboard (JS):**

```bash
node tests/test_expected_decay.js
node tests/test_scenario_returns.js
node tests/test_trade_lab.js
```

The JS tests are plain Node scripts (no test runner). They use `assert` and exit non-zero on failure.

---

## 13. Gotchas & pitfalls

### 13.1 The CSV is the contract

The schema of `etf_screened_today.csv` is the contract between `ls-algo` and `etf-dashboard`. **Renaming a column without coordinating both repos will silently NaN out fields on the dashboard.** Always grep for the column name in both repos before renaming.

### 13.2 `passive_low_beta` is suppressed in *both* the CSV and the SPA

`ls-algo/daily_screener.py` Step 5d nulls out the entire `expected_*` family for `passive_low_beta` rows **and** sets `expected_gross_decay_dist_model = "passive_low_beta_na"`. The SPA also has a defensive fallback (`expectedDecayAvailableForRow` checks `product_class` directly when the flag is missing) so an older CSV doesn't accidentally show stale model values. Keep both layers in sync.

### 13.3 YieldBOOST is **not** classified as `letf` even if leverage = 2

`is_yieldboost = True` is the **first** check in `_product_class`, ahead of beta/leverage. The dashboard relies on this — if you remove or reorder that check, every YieldBOOST row will fall through to `letf` and get a HARQ-Log p50 in the Exp. decay column instead of the intrinsic put-spread number.

### 13.4 Volatility ETPs override `product_class` on the consumer side

`scripts/build_data.py::is_volatility_etp` and `backend/main.py` both check a hard-coded symbol list (`UVIX`, `SVIX`, …) and override `product_class = "volatility_etp"` regardless of what ls-algo emitted. The `index.html` `VOLATILITY_ETP_SYMBOLS` constant must match. This is duplicated in three places — be careful when editing.

### 13.5 The static `build-and-deploy.yml` step is fail-tolerant

`continue-on-error: true` on `Build dashboard data`. A failed build does **not** prevent the Pages deploy — it just deploys the previous JSON. This is intentional (rate-limit-friendly), but it means you must verify a successful build pushed before assuming your change is live. Check the workflow log AND `data/dashboard_data.json` git history.

### 13.6 The 10-min borrow refresh writes `dashboard_data.json` directly

`build_data.py --borrow-only` mutates only the borrow-related fields in the existing `dashboard_data.json`. If the daily full build hasn't run yet today, `--borrow-only` is operating on yesterday's full dataset. The schema for the partial-update path lives at `refresh_borrow_only()` near line 2410.

### 13.7 The HARQ-Log model can produce `simple_ito_fallback` rows with NaN p10/p50/p90

If a single-name underlying has too thin a panel (`_HORIZON_PANEL_RATIO_MIN`), the distributional fields are all NaN and `expected_gross_decay_dist_model = "simple_ito_fallback"`. The SPA falls back to `expectedDecayDisplayValue(r)` (legacy point estimate) in that case. Tooltips reflect this.

### 13.8 `bucket` ≠ `product_class`

`bucket` is for the UI grouping in the main table (B1/B2/B3 pills). `product_class` is the **routing key** for decay/edge/scenarios. A YieldBOOST income strategy lives in **Bucket 2** (β ≤ 1.5) but has `product_class = "income_yieldboost"`. Always use `product_class` for routing logic, not `bucket`.

### 13.9 `borrow_history.json` is reconstructed from git history, not stored

`build_data.py::reconstruct_borrow_history` walks the last `BORROW_HISTORY_MAX_COMMITS` commits of `etf_screened_today.csv` in `ls-algo` and rebuilds the daily borrow series. If you reset `ls-algo`'s git history (e.g., squash, rebase, force-push), you will lose history depth. Don't.

### 13.10 The dashboard depends on `ls-algo`'s git history (for borrow_history)

The ls-algo workflow curls the public dashboard `borrow_history.json` into ls-algo's `data/` folder before running the screener (so the bootstrap can use the weighted resample). This creates a circular dependency: ls-algo reads the dashboard's borrow history, the dashboard reads ls-algo's CSV. The first run of the day breaks the circle by ls-algo running first (the screener cron is set earlier than the dashboard cron).

### 13.11 PowerShell ≠ bash

This repo lives on a Windows machine. Many of the obvious one-liners (heredocs, `head`, `tail`, `cp -r`) don't work in PowerShell. Use `Copy-Item -Force`, `Get-Content -Head N`, etc. Or pipe through `git bash` / WSL if you have it. Multi-line commit messages: write to a file first (`.git/COMMIT_MSG.txt`) and `git commit -F` it.

### 13.12 The SPA is single-file Babel-in-browser

There is no build step. Editing `index.html` is editing production. Validate locally with `python -m http.server 8000` and open the browser dev console — Babel will surface JSX errors at parse time.

---

## 14. Recent changes & invariants you must preserve

(In rough chronological order, most recent first.)

- **Apr 2026 — Refined product taxonomy + passive low-β policy.** `_product_class` was expanded from `{standard_letf, inverse, …}` to the seven-class taxonomy in §5. `expected_decay_available` flag was introduced. Passive low-β rows have their entire `expected_*` family nulled at the `daily_screener.py` Step 5d boundary. The dashboard renders `—` for those rows in the `Exp. decay` and `Net edge` columns. **Do not** silently fall back to simple Itô for passive low-β. Do not show a tooltip that suggests there is a hidden number — the policy is "this is N/A by design."
- **Apr 2026 — YieldBOOST intrinsic decay.** YieldBOOST rows now show the put-spread NAV decay (1y compounded weekly loss minus expense ratio) in the `Exp. decay` column. The HARQ-Log p50 is preserved in the tooltip for context. The Income Scenarios table relabels `NAV Decay Profit` → `Intrinsic NAV Decay` to make the model explicit. **Do not** revert the YieldBOOST cell to use HARQ-Log p50 — the cb-factor ((β² − β)/2 ≈ 0 at β ≈ 0.5) crushes the intrinsic put-spread mechanism to ~2% which is wrong.
- **Apr 2026 — `product_class` pill on chart-page detail header.** A small badge next to the bucket pill indicates the product class (`LETF`, `Inverse`, `Volatility ETP`, `YieldBOOST (income)`, `Income (put-spread)`, `Passive low-β`, `Structured`). Tooltip explains the routing.
- **Apr 2026 — SBTU plausibility caps.** `decay_distribution.py` now winsorizes daily squared returns at `_R2_WINSOR_CAP` and caps `μ_logIV` via `_cap_mu_log_iv` to keep the distributional model from emitting impossible 800%+ p50 numbers on thin-history single-name ETFs.
- **Apr 2026 — Chart timeframe button fix.** `chartVolLookbackRange` is now lifted to `App` and propagated down to both `ChartPage` and `DashboardRoot`. It drives TradingView range and realized-vol diagnostics. The default Scenarios / `Exp. ETF return (3M)` σ now comes from `forecast_vol_underlying_annual`, with chart EWMA retained only as fallback.

- **Apr 2026 — shared forecast volatility.** `scripts/build_data.py` now exports `forecast_vol_underlying_annual` plus component diagnostics (`forecast_vol_model_annual`, `forecast_vol_robust_ewma_annual`, `forecast_vol_raw_ewma_annual`, `forecast_vol_source`, `forecast_vol_event_adjusted`). The forecast blends variance 50/50 between model-implied σ (from `expected_gross_decay_p50_annual`, or YieldBOOST put-spread inversion) and robust 6M EWMA. Robust EWMA clips unusually large one-day returns before applying λ≈0.94. Both the main-grid `Exp. ETF return` and Scenarios tab use this same σ.

- **Apr 2026 — `Scen. 1M ETF` → `Exp. ETF return (3M)` rebrand.** The main-table scenario column was switched from a 1M horizon to a **3M horizon** (CV scales as 1/√T → 3M is ~42% less noisy in relative terms while structural decay accumulates ~3× more visibly), and the YieldBOOST branch was switched from `netShortPnl` (a pair-trade short P&L, mostly positive) to `navReturn = −navDecay` (NAV-only erosion, always negative). Both branches now report a unified **long holder's expected ETF return**, with borrow excluded (short-side cost) and distributions excluded (passed through to longs as cash) — both of those live in Net edge instead. Coloring unified to `scenShortFavCls` everywhere. Sort key renamed `SCENARIO_ETF_1M_FLAT_SORT_KEY` → `SCENARIO_ETF_3M_FLAT_SORT_KEY`; helper renamed `computeScenarioEtf1mFlatUnd1yRealized` → `computeExpectedEtfReturnFlatUnd` (parametric in horizon) with a `computeScenarioEtf3mFlatUnd` thin wrapper. Tooltip on the cell now shows 1M / 3M / 6M companion values.
- **Mar 2026 — HARQ-Log distributional decay.** `decay_distribution.py` introduced. The `Exp. decay` column on the main table now shows the lognormal median (`p50`) by default, with `p10 / p90` as the sublabel.
- **Mar 2026 — Schema v2 net-edge bootstrap.** `screener_v2_fields.py` added the block-bootstrap `net_edge_p05/p25/p50/p75/p95_annual` + `net_edge_hist_json`. The dashboard switched to a fan visualization (`NetEdgeFanGlyph`).
- **Volatility-ETP empirical roll/tracking adjustment.** UVIX/SVIX/VXX/etc. use `simple_ito + empirical_roll_tracking_component` for expected decay because pure Itô badly understates the contango roll-down. Symbol list is duplicated in three places — keep them in sync.

### Invariants

1. **`product_class` is the routing key.** Never branch UI logic on `bucket` when you mean `product_class`.
2. **`expected_decay_available = false` ⇒ `Exp. decay` cell renders `—`.** No fallback. No hidden tooltip with a number. The realized gross drag is the only honest signal.
3. **YieldBOOST always uses intrinsic put-spread decay for the headline.** HARQ-Log is preserved in the tooltip only.
4. **`Gross (realized)` is always shown if available**, regardless of `product_class`.
5. **Sign convention is short-favorable positive throughout.** A higher `net_edge_p50` is better for the short side. Don't flip signs in display logic.
6. **The CSV schema is the contract.** Both `build_data.py` and `backend/main.py` must mirror any column you read in `index.html`.
7. **The dashboard is fail-soft.** A flaky daily build does not blank out the site. Keep `continue-on-error: true` on the build step and the `Restore committed data/index.html before deploy` step.
8. **`borrow_history.json` is reconstructed from ls-algo's git history.** Don't reset that history.

---

If anything in this document drifts out of date, **fix this document as part of your PR** — that's the easiest thing for the next agent.

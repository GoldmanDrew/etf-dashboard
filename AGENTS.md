# AGENTS.md — Architecture & Onboarding Guide

This file is the **single source of truth** for any future agent (AI or human) working on the ETF Borrow Dashboard. Read this first. Everything else in the repo (the Pydantic models, the React SPA, the build script, the GitHub Actions) is downstream of the concepts here.

If you only have time for one section, read **§3 Data Flow** and **§5 Product Taxonomy**.

For the **trader-facing** complement to this engineering doc (entry/exit rules, sizing from greeks, earnings-week playbook, risk caveats), see [`STRATEGY.md`](STRATEGY.md). The two files are deliberately separate: AGENTS.md explains *how the pipeline works*; STRATEGY.md explains *how to act on what it produces*.

---

## Contents

1. [System overview](#1-system-overview)
2. [Two-repo split: ls-algo vs etf-dashboard](#2-two-repo-split-ls-algo-vs-etf-dashboard)
3. [Data flow](#3-data-flow-end-to-end)
4. [Decay models — the math you need to know](#4-decay-models--the-math-you-need-to-know)
5. [Product taxonomy & routing matrix](#5-product-taxonomy--routing-matrix)
6. [Data artifact registry (`data/`)](#6-data-artifact-registry-data)
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

1. **Borrow rate** — what does it cost to short? (IBKR FTP feed, refreshed via `market-hours.yml` ~every 30 min)
2. **Gross decay** — how fast does the ETF lose value to volatility drag, expense ratio, tracking error, and (for income strategies) put-spread NAV decay? (multiple models, see §4)
3. **Exp. edge (fwd)** — forward gross pair-trade P&L (short ETF + β-long underlying), in **log-continuous-annual** units on the **same axis as Realized**. LETF/inverse/vol-ETP: closed-form Itô `(β² − β)/2 · σ²` (no cash leg, pre-borrow). YieldBOOST: **put-spread structural gross** from screener (`expected_gross_decay_p*` mirrored to `expected_pair_pnl_p50_annual`); weekly compound MC is diagnostic-only — see §4.5. Anchor for the Net edge blend.
4. **Net edge** — best estimate of edge after borrow. Inverse-variance Bayesian blend of *Exp. edge (fwd)* and the realized bootstrap, minus borrow. Weight `w_F = sigma_R^2 / (sigma_F^2 + sigma_R^2)`.
5. **Scenario behaviour** — what does the ETF do under a 0%, ±σ, ±3σ underlying shock at 1M / 3M / 6M / 1Y horizons?

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
| Net edge — inverse-variance blend | Borrow-spike risk model |
| `product_class` taxonomy + `expected_decay_available` flag | YieldBOOST put-spread Monte-Carlo decay (server-side, `yieldboost_decay.py`) |
| Inverse-variance blend (B2) + anchor-shift fallback (E2) | YB dashboard mirrors screener gross into `expected_pair_pnl_p*`; weekly MC diagnostic in `expected_pair_pnl_weekly_mc_*` |
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
│                                ├─ Step 5d: passive_low_delta nulling policy     │
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
│   .github/workflows/nightly.yml             (Tue-Sat 6 AM ET: metrics + full build) │
│   .github/workflows/market-hours.yml        (every 15 min: ci_tick.py orchestrator) │
│   .github/workflows/build-and-deploy.yml    (4:30 PM ET + code push: full build)   │
│   .github/workflows/update-corporate-actions.yml (every 6 h)                       │
│   .github/workflows/deploy-pages-data.yml   (hourly Pages safety net)              │
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

A second invariant for YieldBOOST: `data/vrp_live.json` is the BS-free single-row contract between the server pricing kernel (`scripts/letf_options_models.compute_letf_model_extras` and `scripts/event_vol.compute_vrp_row_extras`) and the UI. Each row carries:

- **Quality:** `data_grade` (A/B/C/D) + `data_grade_reason`
- **Pricing:** `model_name` (`bates`/`heston`/`az`), `model_fair`, `edge_pp_of_max_loss`, `expected_weekly_carry_usd`, `model_disagreement_pp_of_max_loss`
- **Greeks:** `dollar_gamma_per_1pct_underlying`, `theta_per_day`, `greeks_kernel`
- **Diagnostics:** BS-only outputs sit under `row["debug"]["bs"]` — never read these from the UI

The UI (both the YB-Edge ranking page and the per-ETF Vol/VRP tab) consumes only the canonical fields above. Black-Scholes is not used as a pricing kernel for LETF put-spreads.

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
- This is **always** shown if available. It is the only number we ship for `passive_low_delta` and `other_structured` rows where the model-based expected decay is meaningless.

### 4.2 Simple Itô identity (legacy fallback)

```
expected_gross_decay_simple_ito = (β² − β)/2 · σ²_annual
```

- Lives in `expected_gross_decay_simple_ito_annual`.
- This is the **fallback** when the distributional model is unavailable.
- It is **deliberately suppressed** for `passive_low_delta` because (β² − β)/2 collapses near β ≈ 1, producing near-zero decay that misleadingly suggests free money. See `daily_screener.py` Step 5d.

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

### 4.5 YieldBOOST decay & forward pair-P&L (schema_v=4)

YieldBOOST income ETFs (AMYY, AZYY, BBYY, COYY, CWY, FBYY, HMYY, HOYY, IOYY, MAAY, MTYY, MUYY, NUGY, NVYY, PLYY, QBY, RGYY, RTYY, SEMY, SMYY, TMYY, TQQY, TSYY, XBTY, YSPY) write a weekly 95/88 SPX-style put-spread on top of a 2× LETF. Their realized β is ~0.4–0.6, so the simple Itô / HARQ-Log vol-drag estimate of ~2–3% is **wildly wrong**. The actual NAV bleeds the put-spread premium every week.

There are **two distinct numbers** for YB:

1. **Headline Exp. edge (fwd) / gross decay band** (`expected_gross_decay_p10/p50/p90/mean_annual` → mirrored `expected_pair_pnl_p50_annual`) — server-side put-spread structural gross MC in `ls-algo/yieldboost_decay.py` (§4.5.1). Net-edge anchor.
2. **Weekly compound pair MC (diagnostic)** (`expected_pair_pnl_weekly_mc_p10/p50/p90_annual`) — dashboard `simulate_weekly_compound_pair_pnl` with distributions + borrow debited weekly (§4.5.2). Shown in tooltips / Scenarios panel — **not** the main-grid headline.

Up through schema_v=3 the forward edge was the buy-and-hold short P&L (`NAV_decay − calibrated_distributions − borrow`). schema_v=4 retired that framing for the **pair-P&L axis** because the realized series (`gross_decay_annual`) is a daily-rebalanced compound log P&L. The two had been on different axes and were 5–15× apart on high-vol underlyings. The Magis closed-form quantities are still emitted as diagnostic-only fields (`expected_pair_pnl_nav_decay_simple_annual`, `expected_pair_pnl_distributions_simple_annual`) but are **not** used in the headline forward or in net edge.

### 4.5.1 YieldBOOST gross decay (server-side put-spread MC)

As of the YB-unification refactor, this is modeled **server-side** in `ls-algo/yieldboost_decay.py` and exported through `expected_gross_decay_p10/p50/p90/mean_annual`. `build_data.py` mirrors these into `expected_pair_pnl_p50_annual` for schema uniformity — that is the **headline Exp. edge (fwd)** cell.

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

- Headline **Exp. edge (fwd)** — `expectedPairPnlDistribution(r).p50` (= screener put-spread structural gross).
- Chart-page detail header, Net edge blend, Scenarios heatmap center — same anchor.
- Income Scenarios table (`Intrinsic NAV Decay`) — closed-form **long lens** NAV path; distinct from Exp. edge (fwd).
- Weekly pair MC — `expected_pair_pnl_weekly_mc_*` in tooltips / Scenarios diagnostic panel.

The front-end helper `yieldBoostIntrinsicAnnualDecay` survives only as a **fallback** when pair fields are missing.

Routing: `expectedDecayHeadlineValue(r)` → `expectedPairPnlDistribution(r).p50` first; legacy gross-band / intrinsic fallbacks when absent.

### 4.5.2 YieldBOOST forward edge (put-spread structural gross headline, schema_v=4)

The main-grid **Exp. edge (fwd)** column for YieldBOOST mirrors screener **`expected_gross_decay_p*`** (put-spread structural gross MC from ls-algo). This is the **same anchor** as net edge and the Scenarios heatmap center cell. Units are **log-continuous-annual** (`ln(equity_1Y / equity_0)` on the gross pair axis).

**Headline fields** (mirrored from screener at `build_data.py` step 5c):

```
expected_pair_pnl_p10/p50/p90_annual     ← expected_gross_decay_p10/p50/p90_annual
expected_pair_pnl_mean_annual            ← expected_gross_decay_mean_annual
expected_pair_pnl_std_annual             ← band_to_sigma(p10, p90) or gross_sigma_forward_annual
expected_pair_pnl_basis = "put_spread_structural_gross"
expected_pair_pnl_source                 ← expected_gross_decay_dist_model
expected_pair_pnl_at_sigma_lo/mid/hi_p50_annual   structural gross at 0.7×/1.0×/1.3× forecast σ
```

**Weekly-rebalanced pair MC (diagnostic only).** `simulate_weekly_compound_pair_pnl(...)` still runs at build time and is stored in:

```
expected_pair_pnl_weekly_mc_p10/p50/p90_annual
expected_pair_pnl_weekly_mc_n_paths
expected_pair_pnl_nav_decay_annual / expected_pair_pnl_distributions_annual  (Magis simple-return diagnostics from the MC run)
```

This path simulates short ETF + β-long underlying with **weekly rebalance**, debiting calibrated distribution cash and borrow inside each week. It captures economics the structural gross number intentionally ignores (distribution debits, path dependence). Shown in Scenarios anchor panel and tooltips — **not** the main-grid headline.

**Per-week recursion** (vectorized over 20,000 paths × 52 weeks — diagnostic engine only):

```
for each path:
    equity_0 = 1
    log_pair = 0
    for each week t in 1..52:
        z_t      ~ N(0, 1)
        sigma_w  = sigma_annual / sqrt(52)
        mu_w     = mu_annual / 52 - 0.5 * sigma_w^2
        log_und  = mu_w + sigma_w * z_t
        r_und    = expm1(log_und)                              # underlying weekly simple
        sleeve   = expm1(2 * log_und)                          # 2x sleeve, NOT linearized
        L_t      = max(0, min(0.07,                             # 95/88 put-spread payoff
                       max(0, 0.95 - (1 + sleeve))
                     - max(0, 0.88 - (1 + sleeve))))
        pair_w   = L_t + ER/52 - borrow/52 - d_weekly + beta * r_und
        # d_weekly = capture_ratio * E[put_spread_loss](0, sigma, 1 week)
        log_pair += log1p(pair_w)
    sample_log_ann = log_pair * (52 / weeks)
return percentiles(samples)
```

**Short distributions.** Each week debits calibrated distribution cash owed to the lender:
`d_weekly = capture_ratio × E[put_spread_loss_weekly](0, sigma, 1)`. This replaces the prior
assumption that ex-date price drops fully offset cash distributions under weekly rebalance.
`capture_ratio` therefore directly lowers forward pair P&L in `simulate_weekly_compound_pair_pnl`.
(See `tests/test_income_schedule.py::test_simulate_mc_capture_ratio_lowers_p50_via_distributions`.)

**Output fields** (weekly MC diagnostic — not headline):

```
expected_pair_pnl_weekly_mc_p10/p50/p90_annual
expected_pair_pnl_weekly_mc_n_paths
```

**Diagnostic-only (from weekly MC run, not fed into headline):**

```
expected_pair_pnl_nav_decay_annual / expected_pair_pnl_distributions_annual   Magis closed-form simple-return diagnostics
expected_pair_pnl_sigma_forward_annual          forecast sigma used
```

These fields are preserved so anyone investigating "why isn't headline equal to weekly MC" has the answer in the JSON. They are **not** the headline forward and **not** in the net-edge blend anchor.

**Reproducibility.** `seed = stable_seed_from_symbol(symbol)` (`zlib.crc32` in Python, FNV-1a in `assets/income_scenario.js`) so rebuilds are deterministic per ticker.

**Borrow handling.** The simulator subtracts `borrow / 52` inside the per-week PnL. This is the only forward-axis place borrow appears for YB. **Do not subtract borrow again** when forming `net_edge_p50_annual` — the existing E2 branch in `build_data.py` is gated on `product_class == "income_yieldboost"` and skips the final borrow subtraction.

**Forecast σ source.** `forecast_vol_underlying_annual` (50/50 blend of model-implied σ and robust 6M EWMA — see §4.4 / §10).

**Performance budget.** 20,000 paths × 52 weeks × NumPy-vectorized = ~30 ms per ticker on the build runner; ×25 YB tickers = sub-second on top of the existing build.

### 4.5.3 Pair scenario grid (5×5 σ-multiplier × drift heatmap)

Both YB and LETF rows now carry a `pair_scenario_grid` block in `dashboard_data.json`:

```json
{
  "sigma_multipliers": [0.5, 0.7, 1.0, 1.3, 1.5],
  "drifts": [-0.50, -0.25, 0.00, 0.25, 0.50],
  "p50_log_grid": [[...], [...], [...], [...], [...]],
  "und_p50_simple_grid": [[...], ...],
  "borrow_annual": 0.056,
  "engine": "yieldboost_put_spread_structural" | "ito_closed_form",
  "axis": "log_continuous_annual",
  "basis": "put_spread_gross_anchor" | "letf_ito_analytic"
}
```

`p50_log_grid[i][j]` is gross structural pair P&L (log/yr) at `σ * sigma_multipliers[i]` and `μ_annual = drifts[j]`. Code paths:

- **YB (Scenarios heatmap):** `scenario_grid_put_spread_pair(...)` — put-spread closed form + `β·μ`, center cell calibrated to `expected_gross_decay_p50_annual`. Aligns with Exp. decay / net-edge anchoring.
- **YB (main grid Exp. edge fwd):** `expected_pair_pnl_annual` / weekly path MC — separate metric.
- **LETF / inverse / vol-ETP:** analytic Itô `(β² − β)/2 · (k·σ)² + β·μ` per cell.

YB `net_edge_*` is passed through from the screener (no dashboard re-blend onto weekly pair MC).

Both paths emit the same keys so `PairScenarioHeatmap` renders uniformly. Client mirror: `assets/income_scenario.js::scenarioGridPutSpreadPair`.

The `pair_scenario_grid_meta` global field on the JSON envelope documents the canonical axes for any tooling that wants to build alternative grids.

### 4.5.4 YieldBOOST distribution calibration (NAV-normalized capture ratio)

The Scenarios-tab cash projection (`estimateIncomeStyleScenarioReturn`) depends on `d`, the weekly distribution as a fraction of NAV. The original `load_distribution_income_yields` computed `d ≈ Σ$/today_price`, which collapsed under heavy NAV decay — e.g. MTYY's $4.20 NAV reported a ~163% annual yield because cumulative dollars were divided by the post-decay price. That fed the closed form a `d` that was 2x too large, breaking long-TR and net-short across the YB universe.

The fix is calibration, not engine math. The closed form was already correct (validated within +1.9pp of MC by Magis Capital, April 2026 *Bucket 2 Income ETF Structural Decay*).

**Pipeline (`scripts/income_schedule.py`):**

```
for each YieldBOOST ETF, each historical ex-date i:
    yield_i  = amount_i / NAV_at_ex_i                        # NAV-normalized
    bs_i     = expected_put_spread_loss_weekly(0, σ_at_ex_i) # research methodology
    ratio_i  = yield_i / bs_i                                # ~0.65 cross-fund

fund_ratio_median           = median(ratio_i over trailing 365d)
confidence                  = high (≥12 events) / medium / low / none
blended_ratio_used          = w * fund_median + (1-w) * cross_fund_prior
                              w = min(1, events_used / 12)
```

`build_data.py` ships an `income_distribution_calibration` block on every YB row with `events_used`, `events_total`, `fund_ratio_median / p25 / p75`, `fund_ratio_confidence`, `blended_ratio_used`, `cadence_label`, `template_yields` (last 12 normalized yields for schedule transparency), and `events_recent` (last 16 normalized events for the Distributions panel). A two-pass build self-calibrates `cross_fund_ratio` from the fleet median over high-confidence funds; absent ≥3 qualifying funds the research constant 0.65 is used as fallback.

**Client (`assets/income_scenario.js`):**

```
d_weekly_t = blended_ratio_used * expected_put_spread_loss_weekly(0, σ_scenario)
d_annual_t = d_weekly_t * 52
```

so the projected cash automatically scales with the scenario σ — increasing into vol shocks (more option premium captured) and falling into vol crashes. `incomeAnnualYieldForScenario(r, lookback, sigma_scenario)` in `index.html` consumes the block when present and falls back to the (now NAV-normalized) `income_yield_trailing_annual` / `income_yield_recent_annual` scalars for one release.

**UI surfaces:**

- **Scenarios tab subheader:** capture-ratio strong/typical/weak/adverse band (thresholds 0.50 / 0.75 / 1.00) + source label (fund vs cross-fund prior) + event count + run-rate annualization.
- **Cash distributions panel:** appended columns `NAV at ex` / `Yield (NAV-norm)` / `Capture ratio` for the last 16 events when calibration is present.

**Tail-fatness:** `IncomeScenario.DEFAULT_TAIL_ADJUSTMENT_ANNUAL` is `0.0` by default; setting it to `~0.02` adds a constant to closed-form NAV decay to close the lognormal-vs-Student-t gap reported by the research's MC.

**Tests:** `tests/test_income_schedule.py` (Python) and `tests/test_income_scenario.js` (Node) both consume `tests/fixtures/bucket2_research.json` — 8 candidates from the research table validate that the closed form reproduces published outputs within ±4pp NAV decay and ±5pp net short.

### 4.6 Net edge — inverse-variance Bayesian blend (schema_v=4)

`screener_v2_fields.enrich_screener_v2_fields` block-bootstraps the daily log-drag time series **and** weighted-resamples the borrow-rate history (with stress correlation grid `ρ ∈ {0.0, 0.2, 0.4}`). The realized gross draws then enter an **inverse-variance Bayesian blend** with the forward forecast:

```
sigma_F = (expected_gross_decay_p90 - expected_gross_decay_p10) / (2 · 1.2816)
sigma_R = std(gross_draws)
mu_F    = expected_gross_decay_p50_annual
mu_R    = mean(gross_draws)
w_F     = sigma_R^2 / (sigma_F^2 + sigma_R^2)       # weight on forward forecast
posterior_mu = w_F · mu_F + (1 − w_F) · mu_R
gross_draws_shifted = gross_draws − mu_R + posterior_mu
```

Conceptually: the **shape** (block autocorrelation, regime mixing, vol clustering) is preserved from history; the **level** is the Normal-Normal posterior of two noisy estimators. Limits:
- `sigma_F → 0`  ⇒ `w_F → 1` ⇒ pure forward anchor (legacy anchor-shift behaviour).
- `sigma_R → 0` ⇒ `w_F → 0` ⇒ realized-only (no shift).

When the forward forecast has no usable band (`yieldboost_put_spread_point`: single sigma → p10 = p50 = p90), the blend degenerates to the legacy anchor-shift (B1 / E2 fallback). The shift is gated on `expected_decay_available = True` AND a finite anchor — `passive_low_delta` rows are deliberately left realized-only.

For LETF / inverse / volatility_etp / `income_yieldboost`-with-band: `gross_blend_method = "inverse_variance"`.
For `yieldboost_put_spread_point` (no band): `gross_blend_method = "anchor_shift_fallback"`.

Diagnostics on every row:

- `gross_anchor_target_annual` — the **posterior mean** (`posterior_mu`) the bootstrap was shifted to.
- `gross_anchor_shift_annual` — signed shift (`posterior_mu − mu_R`).
- `gross_anchor_source` — legacy label (forecast model: `harq_log_anchored` / `empirical_lognormal` / `yieldboost_put_spread` / `yieldboost_put_spread_point`).
- `gross_blend_method` — `inverse_variance` / `anchor_shift_fallback` / `realized_only` / `""`.
- `gross_blend_weight_forward` — `w_F` (NaN if not blended).
- `gross_sigma_forward_annual`, `gross_sigma_realized_annual` — the two sigmas that drove the weight.
- `gross_realized_mean_annual` — `mu_R` before the shift (lets downstream re-blend with a different anchor).
- `copula_note` — appends `inverse_variance_blend(w_F=0.XX),shift=±X.XXXX` when the blend fires.

#### YieldBOOST dashboard fields (schema_v=4, step 5c)

For YieldBOOST rows, `build_data.py` step 5c:

1. **Mirrors** screener `expected_gross_decay_p*` into `expected_pair_pnl_p50_annual` (and p10/p90) — headline Exp. edge (fwd), basis `put_spread_structural_gross`.
2. **Runs** `simulate_weekly_compound_pair_pnl` and stores `expected_pair_pnl_weekly_mc_p10/p50/p90_annual` — diagnostic only (distributions + borrow inside weekly sim).
3. **Builds** put-spread structural `pair_scenario_grid` anchored to screener Exp. edge p50.
4. **Does not re-blend** net-edge quantiles on the dashboard — net edge stays on screener output anchored to structural gross.

Diagnostics per YB row: `expected_pair_pnl_weekly_mc_*`, `expected_pair_pnl_nav_decay_annual`, `expected_pair_pnl_distributions_annual`, σ-regime triplet on structural gross, `pair_scenario_grid`.

For LETF / inverse / vol-ETP rows: `expected_pair_pnl_p* = expected_gross_decay_p*` by identity (no cash leg) — emitted by `build_data.py` step 5c for schema uniformity. Their `pair_scenario_grid` is populated **analytically** via Itô (`(β² − β)/2 · (k·σ)² + β·μ − borrow`); no MC required.

**What changed in schema_v=4 vs schema_v=3:**

| Field | schema_v=3 | schema_v=4 |
|---|---|---|
| YB `expected_pair_pnl_p50_annual` | Weekly-rebalanced compound MC (log space) | **Mirrors screener put-spread structural gross** (`expected_gross_decay_p50`) |
| YB realized anchor | `gross_decay_annual − distributions_annual_run_rate` (D1) | `gross_decay_annual` (no subtraction) |
| YB `expected_pair_pnl_std_annual` | (degenerate / absent) | MC sample std on log axis |
| YB net-edge final borrow subtraction | yes | **no** (borrow already inside forward) |
| `pair_scenario_grid` per row | absent | 5×5 σ_mult × drift grid |
| σ-regime triplet | absent | three p50_annual values at 0.7/1.0/1.3× σ |
| Removed fields | — | `expected_pair_pnl_*_simple_*`, `gross_decay_annual_pair_adjusted` |

#### Output (always present, schema_v=4)

```
net_edge_p05_annual / p25 / p50 / p75 / p95 / hist_json
expected_pair_pnl_p10_annual / p25 / p50 / p75 / p90 / mean / std (all log-continuous-annual)
expected_pair_pnl_at_sigma_lo_p50_annual / mid / hi
pair_scenario_grid {sigma_multipliers, drifts, p50_log_grid, ...}
expected_pair_pnl_units = "log_continuous_annual"
expected_pair_pnl_basis = "put_spread_structural_gross" (YB headline) | "letf_ito_analytic" (LETF/inv/vol-ETP)
expected_pair_pnl_weekly_mc_p10/p50/p90_annual   (YB diagnostic only)
```

`net_edge_hist_json` is a compact (bin_centers, counts) histogram. For YB rows it has been level-shifted on the dashboard side to the pair-P&L axis; shape comes from the screener. The frontend gates on `schema_v >= 4` before reading the new fields and shows a "Data file is from an older build; rerun scripts/build_data.py" banner if the JSON is older.

**Sign convention:** **short-favorable positive**. A `net_edge_p50 = +0.10` means a 10% annual posterior best-estimate edge to the short, after borrow.

`gross_edge_definition` per class: `letf` / `inverse` / `volatility_etp` / `income_yieldboost` → `blended_realized_expected`; `passive_low_delta` / `income_put_spread` → `realized_daily_log_drag`.

For `passive_low_delta`, the front-end **also** masks the headline `Net edge` cell to `—` because the policy is "expected decay is N/A → fall back to realized → don't pretend we have a structural edge".

---

## 5. Product taxonomy & routing matrix

The single most important variable in this codebase is `product_class`. It determines what the dashboard renders for each row.

The taxonomy lives in `ls-algo/screener_v2_fields.py`:

```python
def _product_class(lev, beta, *, is_yieldboost=False):
    if is_yieldboost:           return "income_yieldboost"
    if beta < 0:                return "inverse"
    if beta > 1.5:              return "letf"
    if 0 < beta <= 1.5:         return "passive_low_delta"
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

`passive_low_delta` and `other_structured` are deliberately `False`.

### Full routing matrix

The dashboard reads `product_class` and `expected_decay_available` and routes every cell on the row through the following table:

| `product_class` | `Gross (realized)` | `Exp. edge (fwd)` cell | Sub-label | `Net edge` cell | Scenarios tab | Chart-page pill |
|---|---|---|---|---|---|---|
| `letf` | realized | HARQ-Log p50 (= pair P&L) | `pair · p10/p90` | bootstrap fan | LETF vol-drag grid + pair heatmap | `LETF` |
| `inverse` | realized | HARQ-Log p50 (= pair P&L) | `pair · p10/p90` | bootstrap fan | LETF vol-drag grid + pair heatmap | `Inverse` |
| `volatility_etp` | realized | simple Itô + roll/tracking | `vol-adj` | bootstrap fan | LETF vol-drag grid + pair heatmap | `Volatility ETP` |
| `income_yieldboost` | realized | **Weekly compound MC pair P&L p50** | `pair · p10/p90 log/yr` | bootstrap fan, **re-blended to pair MC anchor** | **Income put-spread grid + pair heatmap** | `YieldBOOST (income)` |
| `income_yieldboost_fof` | realized (FoF vs basket) | **Weighted child NAV decay − FoF ER − cash drag** | `FoF fwd · p10/p90` | bootstrap fan from FoF daily drags − FoF borrow | **Same Scenarios layout as `income_yieldboost`** (basket-weighted heatmap + income table); Decay tab = realized pair horizons; Basket tab = holdings | `YieldBOOST FoF` |
| `income_put_spread` | realized | HARQ-Log p50 | `pair · p10/p90` | bootstrap fan (anchor-shifted when expected p50 available) | LETF vol-drag grid | `Income (put-spread)` |
| `passive_low_delta` | realized | **`—` (N/A)** | `passive low-β: N/A by policy` | **`—` (N/A)** | hidden | `Passive low-β` |
| `other_structured` | realized | `—` | — | realized fallback | hidden | `Structured` |

The front-end helpers that implement this:

- `expectedDecayAvailableForRow(r)` — checks `expected_decay_available` flag with a fallback for older builds (`product_class !== passive_low_delta && product_class !== other_structured`).
- `isYieldBoostIncomeStrategy(r)` — checks `is_yieldboost === true` or the YIELDBOOST_INCOME_PAIRS Set (~25 known sym/und pairs). **Excludes FoF** (`income_yieldboost_fof`).
- `isYieldBoostFoF(r)` — checks `product_class === "income_yieldboost_fof"` or YBTY/YBST symbols. Synthetic dashboard rows from `scripts/yieldboost_fof_pair_pnl.py`.
- `isVolatilityEtp(r)` — checks `product_class === "volatility_etp"` or symbol/underlying in VOLATILITY_ETP_SYMBOLS.
- `isPassiveLowBetaRow(r)` — checks `product_class === "passive_low_delta"` or `expected_decay_available === false` or bucket-2 + non-YieldBOOST + non-volatility-ETP.

`expectedDecayHeadlineValue(r)` is the central decision function. Its order of operations:

```
if !expectedDecayAvailableForRow(r): return null   // → "—"
if expectedPairPnlDistribution(r): return pair.p50   // schema_v=4 headline (all classes with forward)
if expectedDecayDistribution(r):    return dist.p50  // legacy fallback
return expectedDecayDisplayValue(r) // simple Itô / vol-ETP adjusted
```

This same function is reused for **sorting** the column. If you change the rendering, you must keep this function consistent.

---

## 6. Data artifact registry (`data/`)

Every user-facing JSON under `data/` has a single producer workflow, a primary consumer, and an expected refresh cadence. CI gate: `scripts/vrp_pipeline_diagnostics.py` (YieldBOOST VRP lane).

| File | Producer | Consumer | Refresh cadence |
|---|---|---|---|
| `dashboard_data.json` | `build_data.py` | `index.html` (always loaded) | daily full build + borrow tick ~30 min |
| `vol_shape_history.json` | `build_data.py` (`vol_shape_metrics.py`) | Chart page vol-shape history plots (prefetched); falls back to client recompute from `etf_metrics_daily` | full `build_data` after metrics ingest |
| `etf_screened_today.csv` | `ls-algo/daily_screener.py` | `build_data.py` | daily (cached copy of upstream) |
| `borrow_history.json` | `build_data.py` (walks ls-algo git history) | `ChartPage`, `ls-algo` net-edge bootstrap | daily |
| `borrow_spike_risk.json` | `build_data.py` (spike model) | `ChartPage`, `BorrowMonitor` | daily |
| `borrow_spike_predictions/<DATE>.json` | `build_data.py` (full build only) | `scripts/score_borrow_spikes.py` | daily (`nightly.yml` / `build-and-deploy.yml`) |
| `borrow_spike_realized.jsonl` | `scripts/score_borrow_spikes.py` | ops / calibration audits | append on workflow |
| `borrow_spike_metrics.json` | `scripts/score_borrow_spikes.py` | Brier, log-loss, band calibration rollup | daily |
| `options_cache.json` | `build_data.py --options-only` / `--yieldboost-vrp-only` | `ChartPage`, `Trade Lab`, `vrp_live.json` builder | `market-hours.yml` ~15 min rotation |
| `yieldboost_put_spreads_latest.json` | `ingest_etf_metrics.py` + `build_data.py` (`refresh_yieldboost_vrp_files`) | Vol / VRP tab (fallback), `vrp_live.json` builder | daily metrics + each `build_data` / `--options-only` run |
| `vrp_live.json` | `build_data.py` (`refresh_yieldboost_vrp_files`) | Vol / VRP tab (`index.html`) | each `build_data` / `--options-only` run; deployed via `build-and-deploy.yml` |
| `vrp_health.json` | `build_data.py` (`refresh_yieldboost_vrp_files`) | Vol / VRP tab health panel | each VRP refresh |
| `event_calendar_known.json` | `ingest_event_calendar.py` | `event_vol_decomposition.py`, VRP de-eventing | nightly + when stale (>24h) during VRP refresh |
| `event_calendar_inferred.json` | `event_vol_decomposition.py` (mystery scanner) | combined calendar merge | each VRP refresh |
| `event_calendar_combined.json` | `event_vol_decomposition.py` | `vrp_live.json` builder, `ls-algo/decay_distribution.py` | each VRP refresh |
| `macro_event_calendar.json` | manual (FOMC/CPI dates) | combined calendar merge | manual |
| `etf_metrics_daily.{parquet,csv,json}` | `ingest_etf_metrics.py` | Stats tab … | **`nightly.yml`** Tue–Sat 6 AM ET |
| `etf_metrics_latest.json` | `ingest_etf_metrics.py` | Stats tab (snapshot panel) | **`nightly.yml`** |
| `ci_state.json` | `scripts/ci_tick.py` | staleness gates for `market-hours.yml` | each market tick |
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
- `symbol`, `underlying`, `leverage`, `expected_leverage`, `delta` (hedge ratio; legacy JSON may use `beta`), `delta_n_obs`, `bucket`
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
  - `passive_low_delta_na` (policy-suppressed)
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
- `decomposition_note` (strings like `income_dist_missing`, `low_delta_realized_only`)
- `copula_note`, `copula_type`
- Underlying volatility-shape diagnostics from `ls-algo` at two horizons.
  **60d is the primary display window** (aligned with
  `risk_dashboard/beta_loader.DEFAULT_WINDOW_DAYS = 60` so trend, VCR, and
  betas share the same clock); 20d is retained for short-term context.
  For each window `W ∈ {20, 60}` the screener exports:
  `und_rv_Wd_daily_annual`, `und_rv_Wd_weekly_annual`, `und_trend_ratio_Wd`,
  `und_vcr_Wd`, `und_return_Wd`, percentile context
  (`und_abs_return_Wd_pctile`, `und_rv_Wd_pctile`,
  `und_trend_ratio_Wd_pctile`, `und_vcr_Wd_pctile`),
  the rolling-history `und_vcr_Wd_median`, and `und_vol_shape_Wd`
  (`boiling_trend`, `jumpy_trend`, `choppy_volatile`, etc.). The Trend
  Ratio is the ratio of weekly-sampled to daily-sampled realized vol,
  annualized from the 5-day chunk size so it is window-independent
  (iid ≈ 1, perfect daily drift ≈ √5).
- **Production headline alignment (Tier 2):** `build_data.py` recomputes
  the vol-shape block above from `data/etf_metrics_daily.csv` on **joint
  ETF+underlying days** (`underlying_adj_close` with ETF `close_price` or
  `nav`), using `scripts/vol_shape_metrics.py` (same formulas as
  `ls-algo/screener_v2_fields.py` and the chart helper in `index.html`).
  When enough joint history exists, it **overwrites** the screener CSV
  values and sets `und_vol_shape_source` = `etf_metrics_daily` plus
  `und_vol_shape_metrics_asof` / `und_vol_shape_joint_days`. Otherwise
  fields stay on the screener export (`und_vol_shape_source` =
  `screener`). Percentiles and labels are recomputed on the metrics
  joint series (may differ slightly from ls-algo's full underlying TR
  panel).   Run **`nightly.yml`** before afternoon **`build-and-deploy.yml`** so
  metrics are fresh. Rolling TR/VCR/RV series (last ~252 points per
  symbol) ship in `data/vol_shape_history.json`; charts prefer that
  file over browser recomputation.

- **Tier 3 (ls-algo alignment):** Canonical math lives in
  `ls-algo/vol_shape.py`. `daily_screener` / `enrich_screener_v2_fields`
  prefer **per-ETF joint** `etf_metrics_daily` underlying prices when
  that CSV is present (`und_vol_shape_price_basis` =
  `joint_etf_metrics`), else full underlying total-return
  (`underlying_total_return`). `etf-dashboard/scripts/vol_shape_metrics.py`
  must stay in sync; `tests/test_vol_shape_ls_algo_parity.py` golden-tests
  APLX/APLZ (same underlying) when the sibling `ls-algo` checkout exists.

- **Tier 4 (metadata / UI clarity):** `build_data.py` exports provenance for
  headline realized σ: `vol_*_annual_source` (`yahoo_realized_vol` |
  `screener_csv`), `vol_*_annual_window` (ladder key, e.g. `12M`), and
  `vol_*_annual_screener` (CSV value when Yahoo overwrote it). The SPA
  (`index.html`) labels **realized vs forward** consistently: Gross /
  net-spot decay = trailing screener; Exp. decay / net-edge fan = forward
  ls-algo export. Borrow tooltips compare grid spot to
  `borrow_history.json` tail (`borrowDisplay` / `borrowSourceHint`). NAV
  fair-value sublabel includes forecast `ts` / `as_of` when present
  (`navForecastAsOfLabel`). Rebuild `dashboard_data.json` after pulling
  Tier 4 builder changes so grid rows carry the new fields.
- `schema_v` (currently `4`), `edge_sign_convention` (`short_favorable_positive`)
- `expected_pair_pnl_units` (`log_continuous_annual`), `expected_pair_pnl_basis` (`put_spread_structural_gross` for YB headline, `letf_ito_analytic` for LETF/inverse/vol-ETP)
- `pair_scenario_grid_meta` (sigma multipliers and drift axes for the Scenarios-tab heatmap)

### Algo flags
- `include_for_algo`, `protected`, `cagr_positive`

### Trailing volatility (computed in `build_data.py`, not in CSV)
- `vol_underlying_annual`, `vol_etf_annual` (and shorter-horizon variants)
- Tier 4: `vol_*_annual_source`, `vol_*_annual_window`, `vol_*_annual_screener`

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

9. **Per-symbol record build**: massive dict comprehension that combines CSV row + borrow snapshot + realized vol + bucket + product_class override (vol ETP). Calls `is_volatility_etp()` to override `product_class` and `expected_decay_model`. Reads `expected_decay_available` from CSV with the fallback `product_class not in ("passive_low_delta", "other_structured")`. After each record dict is built, `apply_vol_shape_to_record()` may replace the `und_*` vol-shape fields from `etf_metrics_daily.csv` (see `scripts/vol_shape_metrics.py`).

10. **`build()` writes `dashboard_data.json`** with structure:
    ```json
    {
      "generated_at": "2026-04-26T11:53:47Z",
      "high_delta_threshold": 1.5,
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
│   ├── assets/pair_backtest.js      (`simulateInversePairBacktest`: **Diamond-Creek-Quant parity** on sizing, β-adj rebalance, trapezoid borrow drag, and `Math.max(0, slippageBps)` t-cost only — no inverse floor/impact fallback; `toNum`/IIFE global match DCQ. h = |MV_ETF|/|MV_und| (Chart prefills **1/|β|**); **borrow** stays **etf-dashboard / IBKR**: `borrow_current` forward-fill + `borrowHistoryPointForAvg` / row fallbacks, **short_favorable_positive**. Chart `Backtest` recomputes sim each render via IIFE like DCQ. `buildTotalReturnBacktestSeries`; legacy `runPairBacktest`)
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
│       │   │   ├── Options chains tab
│       │   │   └── Backtest tab (`PairBacktestChart`, `PairBacktestExposureChart`, risk metrics, `simulateInversePairBacktest`; `#/chart/SYM/backtest`)
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

Five workflows + shared actions (`commit-data`, `deploy-pages`):

| Workflow | Cadence | What it does |
|---|---|---|
| `nightly.yml` | Tue–Sat 6 AM ET | ETF metrics ingest + scoring + **full** `build_data.py` → one commit + deploy |
| `market-hours.yml` | Every 15 min | `scripts/ci_tick.py` runs **NAV then intraday** when stale (~30m / ~5m RTH cadence), then one other stale rotation task (borrow / options / YB VRP) → commit + deploy |
| `build-and-deploy.yml` | Mon–Fri 4:30 PM ET + code push + manual | Full `build_data.py` after ls-algo screener + deploy |
| `update-corporate-actions.yml` | Every 6 h | News / corp actions → commit + deploy |
| `deploy-pages-data.yml` | Hourly | Safety-net Pages deploy (no rebuild) |

**Orchestrator:** `scripts/ci_tick.py` reads `config/ci.yaml` + `data/ci_state.json` staleness gates. Manual override: `workflow_dispatch` on `market-hours.yml` with `mode` + optional `force`.

**Removed (2026 simplification):** `refresh-borrow`, `refresh-options` (A/B), `refresh-yieldboost-vrp`, `intraday-flow`, `refresh-nav-forecast`, `update-etf-metrics`, `repository-dispatch-refresh`.

**Critical safety rail:** `build-and-deploy.yml` and `nightly.yml` mark `build_data.py` as `continue-on-error: true` where noted — Pages still deploys committed JSON on failure.

**Deploy path:** Inline `deploy-pages` action after successful commits in `market-hours`, `nightly`, `corporate-actions`, and `build-and-deploy`. No deploy-on-every-`data/**` push.

### Required repo secrets

| Secret | Used by |
|---|---|
| `LS_ALGO_TOKEN` | `build-and-deploy.yml` (read CSV from ls-algo private repo) |
| `POLYGON_API_KEY` (or `POLYGON_IO_API_KEY`) | options + spot fallback |
| `TRADIER_TOKEN` | spot quotes + Tradier chains |

---

## 12. Common tasks — recipes

### 12.1 Add a new product_class to the taxonomy

1. **In `ls-algo`**: edit `screener_v2_fields.py::_product_class` to recognize the new class. Add it to `_EXPECTED_DECAY_CLASSES` if model-based expected decay is meaningful for it. If not, mirror the `passive_low_delta` policy in `daily_screener.py` Step 5d (null out the expected decay columns for these rows).
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

### 12.7 Sync **Diamond-Creek-Quant** (`GoldmanDrew/Diamond-Creek-Quant`) with this repo’s ETF metrics / Stats pipeline

Cloud agents cannot push to that repo (token has no `push` on it). From your laptop (path to your **Diamond-Creek-Quant** clone):

```bash
cd /path/to/etf-dashboard   # this repo
./scripts/sync_diamond_creek_quant_metrics.sh /path/to/Diamond-Creek-Quant
cd /path/to/Diamond-Creek-Quant
python3 -m pytest tests/test_backfill_underlying_adj_close_script.py tests/test_etf_metrics_shares_repair.py -v
git add -A && git commit -m "sync: etf-dashboard ETF metrics + underlying adj close pipeline" && git push origin master
```

Then run **Update ETF Metrics** on Diamond-Creek-Quant (or `python scripts/ingest_etf_metrics.py` then `python scripts/backfill_underlying_adj_close.py`) so `data/etf_metrics_daily.json` picks up filled `underlying_adj_close`, and let **build-and-deploy** publish Pages.

### 12.8 Recover YieldBOOST Vol / VRP after a deploy gap

If the Vol / VRP tab shows holdings strikes but **IV pending**, or the browser console reports `vrp live HTTP 404`:

1. **Update ETF Metrics Daily** — refreshes Granite XLS holdings → `etf_holdings_latest.csv` → `yieldboost_put_spreads_latest.json`.
2. **Build Data & Deploy Pages** — runs full `build_data.py`, writes `vrp_live.json`, commits both VRP artifacts, deploys to Pages.
3. **Refresh Options** (optional, for live IV) — sharded bucket-3 refresh with `OPTIONS_INCLUDE_YIELDBOOST=1`; also rebuilds `vrp_live.json`.

Verify:

```bash
curl -sI https://goldmandrew.github.io/etf-dashboard/data/vrp_live.json | head -1
curl -sI https://goldmandrew.github.io/etf-dashboard/data/yieldboost_put_spreads_latest.json | head -1
```

Local gate:

```bash
python scripts/vrp_pipeline_diagnostics.py --require-vrp-file --fail-on-missing-vrp-when-spreads
```

The UI falls back to `yieldboost_put_spreads_latest.json` when `vrp_live.json` is missing, so users still see Granite-reported strikes with an **IV pending** badge instead of a raw HTTP error.

#### Additive VRP row fields (event_vol.compute_vrp_row_extras)

`refresh_yieldboost_vrp_files` adds the following per-row keys on top of the
event-decomposition fields (`iv_full_proxy`, `iv_base_proxy`, …). They are
*best-effort*: any input missing flows through as `null` rather than erroring.

**Canonical (kernel-agnostic, BS-free) fields — what the UI consumes:**

| Field | Meaning |
|---|---|
| `data_grade` | A / B / C / D server-side PM-grade quality letter from `yieldboost_holdings.data_grade()`. Collapses sleeve quote age × underlying quote age × holdings age × IV source × expiry skew. **A** = full size, **B** = half size, **C** = monitor only, **D** = blocked. |
| `data_grade_reason` | Human-readable string explaining why a row earned its grade ("quote 120m; skew 14d"). |
| `model_name` | `bates` › `heston` › `az` — which calibrated kernel produced this row's `model_fair`. `null` if no kernel converged. |
| `model_fair` | Fair value of the short spread in sleeve currency from the chosen kernel. NOT Black-Scholes. |
| `model_fair_minus_mid` | `spread_mid_market − model_fair` in sleeve currency. Positive = market overpays vs model. |
| `edge_pp_of_max_loss` | `(spread_mid_market − model_fair) / (K_short − K_long) · 100`. **Primary headline signal.** Positive ⇒ SELL. |
| `model_disagreement_pp_of_max_loss` | Max − min model fair across calibrated kernels, in % of max-loss. >20pp = model uncertainty. |
| `expected_weekly_carry_usd` | `model_fair · 100` = $ carry per OCC contract per week. |
| `dollar_gamma_per_1pct_underlying` | $ P&L per 1% adverse move in the underlying (short spread = negative). Sizing input. |
| `theta_per_day` | Theta per calendar day. Positive = collecting decay. |
| `greeks_kernel` | `"heston"` / `"bates"` / `"az"` when greeks are central finite-diff on the chosen kernel's `model_fair`. `"bs_proxy"` is the fallback when no kernel converged (BS finite-diff at sleeve IV). `null` when neither path produced greeks. |

**Supporting (kernel-specific & event) fields:**

| Field | Meaning |
|---|---|
| `iv_underlying_implied` | Sleeve IV inverted to underlying space via β=2 (`σ_sleeve / 2`). The like-for-like vol to compare to `rv_30d_underlying`. |
| `iv_implied_weekly_move_2x_pct` | `σ_sleeve · √(5/252) · 100`. The implied 1σ one-week move on the **sleeve** at current IV. |
| `iv_implied_weekly_move_underlying_pct` | Same but on the **underlying** (1× space). |
| `az_put_spread_fair`, `heston_put_spread_fair`, `bates_put_spread_fair` | Per-kernel fair values. Used to populate `model_fair` based on priority and for disagreement diagnostics. |
| `az_cone_residual_iv` | Sleeve IV minus AZ-implied sleeve IV (`|β| · σ_underlying` at mapped strike). Cross-surface arbitrage signal: positive = sleeve trades rich vs underlying. |
| `az_mapped_strike_long_underlying`, `az_mapped_strike_short_underlying` | Underlying-equivalent strikes after AZ moneyness rescale. |
| `az_implied_sleeve_iv` | `|β| · σ_underlying_at_mapped_K` — AZ-imputed sleeve IV used when the sleeve chain is missing. Triggers `iv_source = "az_imputed_from_underlying"`. |
| `heston_sleeve_params`, `bates_sleeve_params` | AHJ-propagated Heston / Bates parameter dicts for the 2× sleeve. |
| `sleeve_diffusion_drag_annual` | `σ_underlying²` — the simple Itô gross decay for x=2 daily-rebalanced. |
| `event_implied_move_pct_underlying` | Implied 1σ event-day move on the **underlying** scale (from the variance split between `iv_full_proxy` and `iv_base_proxy`, β=2 inverted). Quoted as %. |
| `event_historical_move_pct_underlying` | Mean absolute deviation of \|return\| on prior earnings days for this underlying, from the combined event calendar's `historical_move_pct_mad`. Quoted as %. |
| `event_move_richness_pct` | `(event_implied / event_historical) − 1`. ≥+30% = market over-paying the move (SELL the event premium); ≤−20% = market under-paying (BUY the move). |
| `event_jump_share_of_variance` | `(var_full − var_base) / var_full` over the held horizon. ≥0.40 means the event jump dominates IV — use Bates `model_fair`, not Heston / AZ. |
| `days_to_event` | Calendar days until the next earnings event for this underlying (combined-calendar earliest upcoming, with seed/projection fallback). |
| `event_in_held_horizon` | `true` if the next event date ≤ held expiry. UI flags these rows red. |

**Diagnostics only (NOT for production consumers):**

The following live under `row["debug"]["bs"]`. They are Black-Scholes diffusion-only outputs kept for regression / disagreement audits and are the source of the `bs_proxy` greeks when no kernel converged (the fallback path). Do not introduce new UI dependencies on these keys.

| Field (under `debug.bs`) | Meaning |
|---|---|
| `put_spread_fair_diffusion` | BS fair at chosen σ. Wrong for LETF puts (no vol drag / skew / jumps). |
| `put_spread_sigma_source` | Which σ fed the BS fair value. |
| `spread_breakeven_sigma_annual` | Annualized σ that makes `BS_fair(σ) == spread_mid_market`. Legacy σ-space pricing — replaced by `edge_pp_of_max_loss` in price space. |
| `iv_minus_breakeven_sigma` | Net short-vol edge in σ-space. Legacy headline metric — replaced by `edge_pp_of_max_loss`. |
| `expected_weekly_loss_pct_of_spot` | `BS_fair / spot_2x · 100`. Replaced by `expected_weekly_carry_usd` ($ space) in the production UI. |
| `delta_spread`, `gamma_spread`, `vega_spread`, `theta_spread_per_day` | BS greeks per single structure. Fallback source of `theta_per_day` (canonical) under the `"bs_proxy"` flag when no kernel converged. |
| `dollar_gamma_per_1pct_sleeve`, `dollar_gamma_per_1pct_underlying` | BS finite-diff $-γ. Fallback source of canonical `dollar_gamma_per_1pct_underlying` under the `"bs_proxy"` flag. |

#### Earnings calendar seed fallback (`data/earnings_calendar_seed.json`)

`ingest_event_calendar.py::build_known_calendar` chains:

1. **Nasdaq** earnings window (21 days fwd) — `confirmed`.
2. **Yahoo** chart-meta earnings — `confirmed`.
3. **Seed JSON** at `data/earnings_calendar_seed.json` — `projected`. Hand-curated next earnings date per YB underlying, refreshed weekly.
4. **Quarterly projection** from `etf_metrics_daily.csv` last-known earnings + 91 calendar days — `projected`.

Each item now carries a `confirmation: "confirmed" | "projected"` field plus a `source_stats` rollup so the UI can badge projected dates separately. The seed file should be regenerated whenever a YB earnings date is missed or shifted — the file is small (25 rows) and committed alongside the data artifacts.

**Staleness semantics.** `event_vol.calendar_is_stale` returns `True` not just when `build_time` ages out (>24h by default) but **also when the payload is empty and `source_stats` shows no successful fetch**. This matters because a 403 from Nasdaq/Yahoo combined with a missing seed file would otherwise write a fresh-timestamped empty file and lock out `refresh_event_pipeline` for 24h. The check distinguishes "build attempted, genuinely nothing upcoming" (e.g. `projected > 0` but no events in the 21-day window) from "build failed silently".

#### Held-leg IV: nearest-expiry fallback

`yieldboost_holdings.lookup_contract_iv` resolves IV/mid in three tiers:

1. **`holdings_exact`** — exact expiry + exact strike (within ~$0.01).
2. **`holdings_nearest_strike`** — exact expiry + nearest listed strike.
3. **`holdings_nearest_expiry`** — held expiry not listed; fall back to the nearest listed expiry within 35 calendar days, then nearest strike at that expiry.

Tier 3 exists because Granite's holdings file references off-cycle (Wednesday/Tuesday) expirations and non-round strikes (e.g. `47.89`) that are OTC structures, not exchange-listed. Without the fallback every YB row reported `iv_source: holdings_missing_chain` and `iv: null`, which collapsed every BS-fair and breakeven-σ derivation downstream. The interpolated IV is honest about the proxy via the `iv_source_chain` audit list returned by `lookup_contract_iv` and the `holdings_nearest_expiry` enum surfaced in the SPA's `vrpIvSourceLabel`.

If the user actually wants the *exact* held-contract IV they need OTC quotes (broker, not Tradier) — the chain-side fallback is the best we can do from public data.

---

## 13. Gotchas & pitfalls

### 13.1 The CSV is the contract

The schema of `etf_screened_today.csv` is the contract between `ls-algo` and `etf-dashboard`. **Renaming a column without coordinating both repos will silently NaN out fields on the dashboard.** Always grep for the column name in both repos before renaming.

### 13.2 `passive_low_delta` is suppressed in *both* the CSV and the SPA

`ls-algo/daily_screener.py` Step 5d nulls out the entire `expected_*` family for `passive_low_delta` rows **and** sets `expected_gross_decay_dist_model = "passive_low_delta_na"`. The SPA also has a defensive fallback (`expectedDecayAvailableForRow` checks `product_class` directly when the flag is missing) so an older CSV doesn't accidentally show stale model values. Keep both layers in sync.

### 13.3 YieldBOOST is **not** classified as `letf` even if leverage = 2

`is_yieldboost = True` is the **first** check in `_product_class`, ahead of beta/leverage. The dashboard relies on this — if you remove or reorder that check, every YieldBOOST row will fall through to `letf` and get a HARQ-Log p50 in the Exp. decay column instead of the intrinsic put-spread number.

### 13.4 Volatility ETPs override `product_class` on the consumer side

`scripts/build_data.py::is_volatility_etp` and `backend/main.py` both check a hard-coded symbol list (`UVIX`, `SVIX`, …) and override `product_class = "volatility_etp"` regardless of what ls-algo emitted. The `index.html` `VOLATILITY_ETP_SYMBOLS` constant must match. This is duplicated in three places — be careful when editing.

### 13.5 The static `build-and-deploy.yml` step is fail-tolerant

`continue-on-error: true` on `Build dashboard data`. A failed build does **not** prevent the Pages deploy — it just deploys the previous JSON. This is intentional (rate-limit-friendly), but it means you must verify a successful build pushed before assuming your change is live. Check the workflow log AND `data/dashboard_data.json` git history.

**YieldBOOST options cache:** the full `build()` path now runs a **second** `build_polygon_options_cache(..., yieldboost_targeted=True)` pass after the bucket-3 sweep, and preserves prior YB sleeve chains when a budget-limited sweep would otherwise write `spot_only` + `[]`. The nightly workflow's Tradier env must stay aligned with `config/ci.yaml` (`TRADIER_MAX_TOTAL_REQUESTS=500`, `TRADIER_CHAIN_MAX_TOTAL_REQUESTS=400`) — a low cap (e.g. 140) starves sleeves alphabetically after ~MRAL and blanks MTYY/MSTU in `vrp_live.json`.

**Second Tradier API key:** only helps if Tradier's **per-token rate limit** (25 req/min) is the binding constraint. Our outages were mostly the **in-repo request budgets** and the non-targeted nightly sweep overwriting good YB chains — fix those first. A second key is optional sharding (e.g. bucket-3 on key A, YB sleeves on key B) if you later parallelize fetches.

### 13.6 `commit-data` snapshots before rebasing onto `origin/main`

Build steps modify `data/*.json` in the working tree. The shared **`.github/actions/commit-data`** action must **never** run `git checkout` on a dirty tree — it backs up artifacts to a temp dir, `git reset --hard origin/<branch>`, restores the backup, then commits. Without that order, concurrent pushes produce `would be overwritten by checkout` and the workflow fails after a successful build.

### 13.7 Borrow-only refresh scope

`build_data.py --borrow-only` mutates only `dashboard_data.json` and `borrow_history.json` (no borrow-spike snapshots). Spike scoring runs on **`nightly.yml`** / **`build-and-deploy.yml`** only. If the daily full build has not run yet, `--borrow-only` operates on yesterday's dataset. See `refresh_borrow_only()` in `scripts/build_data.py`.

### 13.8 The HARQ-Log model can produce `simple_ito_fallback` rows with NaN p10/p50/p90

If a single-name underlying has too thin a panel (`_HORIZON_PANEL_RATIO_MIN`), the distributional fields are all NaN and `expected_gross_decay_dist_model = "simple_ito_fallback"`. The SPA falls back to `expectedDecayDisplayValue(r)` (legacy point estimate) in that case. Tooltips reflect this.

### 13.8 `bucket` ≠ `product_class`

`bucket` is for the UI grouping in the main table (B1/B2/B3 pills). `product_class` is the **routing key** for decay/edge/scenarios. A YieldBOOST income strategy lives in **Bucket 2** (β ≤ 1.5) but has `product_class = "income_yieldboost"`. Always use `product_class` for routing logic, not `bucket`.

**Bucket 2 UI scope (2026-06):** The B2 pill/tab shows **YieldBOOST + FoF only** (`bucket_2_ui_visible = true`). Passive low-β rows (`passive_low_delta`) remain in `dashboard_data.json` → `records[]` for borrow history, metrics, and `#/chart/SYM` deep links — they are filtered out in `DashboardRoot` when `activeTab === 'bucket_2'`. `summary.bucket_2_count` counts visible rows; `bucket_2_archived_count` is the hidden passive-low-β remainder.

### 13.9 `borrow_history.json` is reconstructed from git history, not stored

`build_data.py::reconstruct_borrow_history` walks the last `BORROW_HISTORY_MAX_COMMITS` commits of `etf_screened_today.csv` in `ls-algo` and rebuilds the daily borrow series. If you reset `ls-algo`'s git history (e.g., squash, rebase, force-push), you will lose history depth. Don't.

### 13.10 The dashboard depends on `ls-algo`'s git history (for borrow_history)

The ls-algo workflow curls the public dashboard `borrow_history.json` into ls-algo's `data/` folder before running the screener (so the bootstrap can use the weighted resample). This creates a circular dependency: ls-algo reads the dashboard's borrow history, the dashboard reads ls-algo's CSV. The first run of the day breaks the circle by ls-algo running first (the screener cron is set earlier than the dashboard cron).

### 13.11 PowerShell ≠ bash

This repo lives on a Windows machine. Many of the obvious one-liners (heredocs, `head`, `tail`, `cp -r`) don't work in PowerShell. Use `Copy-Item -Force`, `Get-Content -Head N`, etc. Or pipe through `git bash` / WSL if you have it. Multi-line commit messages: write to a file first (`.git/COMMIT_MSG.txt`) and `git commit -F` it.

### 13.12 The SPA is single-file Babel-in-browser

There is no build step. Editing `index.html` is editing production. Validate locally with `python -m http.server 8000` and open the browser dev console — Babel will surface JSX errors at parse time.

### 13.13 Reverse / forward split TR invariants

Split-aware TR must stay in sync across **`scripts/split_adjustments.py`**, **`assets/price_basis.js`**, and **`scripts/price_basis.py`**.

- **`corporate_actions.json` is authoritative** for the declared ratio (`ratio_from / ratio_to`). When the observed close jump is within ~18% of that mult, accept the event even if a different whitelist ratio is nearer (APLZ 5.64× jump with declared 5×).
- **Mechanical pre-split scaling applies only when raw close jumps** at the event. Continuous Yahoo close through the split (MTYY) must remain `splitMode=continuous` — never double-scale.
- **`etf_adj_close == close_price` on recent listings is not back-adjusted.** Ingest/backfill scales pre-split `etf_adj_close` via `backfill_split_adjusted_etf_adj_close`; Decay / Backtest / Stats route through `PriceBasis.buildTrSeriesFromMetrics`.
- **Issuer confirmation** (`shares_outstanding`, NAV step) is a fallback when price bars do not span the split session.
- **Regression gate:** `scripts/audit_split_tr_quality.py` (nightly, `continue-on-error`) flags `maxAbsLogReturn > 35%` within ±7 days of corp splits and `splitMode=continuous` when adj≡close.

---

## 14. Recent changes & invariants you must preserve

(In rough chronological order, most recent first.)

- **May 2026 — YB Exp. edge (fwd) = Exp. decay gross.** YieldBOOST main-grid **Exp. edge (fwd)** now mirrors screener **`expected_gross_decay_p*`** (put-spread structural gross), aligned with net edge and Scenarios heatmap. Weekly-rebalanced pair MC remains as **`expected_pair_pnl_weekly_mc_*`** diagnostic fields only. See §4.5.2 / §4.5.3 / §4.6.
- **May 2026 — schema_v=4: YB forward edge on the pair-axis.** The YieldBOOST forward edge moved from buy-and-hold Magis closed-form to the log-continuous-annual pair axis. Weekly MC is now diagnostic-only; headline is put-spread structural gross from the screener. See §4.5.2 / §4.5.3 / §4.6.
- **May 2026 — ETF metrics NAV vs market close on the same calendar row.** Issuer HTML/CSV rows (REX, YieldMax, ProShares fallback, Roundhill `Rate Date`, Direxion `TradeDate`, Granite `NavDate`) now set `ProviderResult.date` to the **valuation session** parsed from the feed, not the ingest run day. `merge_provider_attempts` stamps merged rows with that same date. Ingest skips `_anchor` for those providers (and `merged`) so Yahoo `close_price` joins on `(date, ticker)` without a one-day offset. `apply_stale_carry_forward` clears `close_price` / `shares_traded` when only NAV is carried so prem/disc does not spike. **Legacy store repair:** run `python scripts/migrate_etf_metrics_valuation_dates.py` (default **dry-run**: in-memory diff + optional `--report` JSON only; **`--apply`** runs Yahoo refresh, postprocess, and `save_outputs`) to relabel/merge rows where `source_url` `#as_of` / `#nav_date` / `#date=` disagrees with row `date`, and clear stale `carry_forward` closes. Use `--since`, `--tickers`, `--report path.json` for phased rollouts and audit.
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
2. **`expected_decay_available = false` ⇒ `Exp. edge (fwd)` cell renders `—`.** No fallback. No hidden tooltip with a number. The realized gross drag is the only honest signal.
3. **YieldBOOST headline Exp. edge (fwd) = put-spread structural gross** (`expected_pair_pnl_p50_annual` mirrored from screener `expected_gross_decay_p50`). Weekly compound MC (`expected_pair_pnl_weekly_mc_*`) is diagnostic-only. Do not revert YB headline to HARQ-Log vol-drag.
4. **`Gross (realized)` is always shown if available**, regardless of `product_class`.
5. **Sign convention is short-favorable positive throughout.** A higher `net_edge_p50` is better for the short side. Don't flip signs in display logic.
6. **The CSV schema is the contract.** Both `build_data.py` and `backend/main.py` must mirror any column you read in `index.html`.
7. **The dashboard is fail-soft.** A flaky daily build does not blank out the site. Keep `continue-on-error: true` on the build step and the `Restore committed data/index.html before deploy` step.
8. **`borrow_history.json` is reconstructed from ls-algo's git history.** Don't reset that history.

---

If anything in this document drifts out of date, **fix this document as part of your PR** — that's the easiest thing for the next agent.

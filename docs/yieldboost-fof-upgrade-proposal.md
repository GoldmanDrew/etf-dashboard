# YieldBOOST FoF (YBTY / YBST) — Audit & Complete Upgrade Proposal

**Date:** 2026-06-04  
**Scope:** GraniteShares **YBTY** (5-name concentrated FoF) and **YBST** (17-name diversified FoF) — dashboard-only synthetic rows added in `build_data.py` step 5d.

---

## 1. Executive summary

YBTY and YBST appear on the main grid with **holdings, forward edge, and borrow**, but the implementation is **v1 / partial**:

| Area | Status |
|------|--------|
| Granite FoF holdings ingest | **Working** (XLS → `etf_holdings_latest.csv`) |
| FoF basket rollup | **Working** (child YB ETF weights + underlying map) |
| ETF metrics / NAV history for YBTY/YBST | **Missing** (0 rows in `etf_metrics_daily.csv`) |
| Realized gross (`gross_decay_annual`, Decay tab) | **Broken** (`fof_realized_pair.ok = false`) |
| Forward Exp. edge | **Misleading** (weighted child *pair* P&L, not FoF economics) |
| Net edge | **Approximate** (child-weighted net − FoF borrow; no bootstrap fan) |
| Scenarios / pair heatmap / VRP / NAV forecast | **Absent or hidden** |
| Chart compare | **Wrong lens** (FoF vs single top underlying, not basket) |

**Recommendation:** Treat current FoF support as **Phase 0 scaffolding**. Ship a **Phase 1–3 upgrade** that (1) ingests FoF NAV, (2) computes realized pair from FoF NAV vs weighted basket, (3) replaces forward edge with a **bottom-up FoF NAV model**, and (4) aligns UI tabs with that model.

---

## 2. Product context

| Ticker | Structure | Latest basket (2026-06-04) |
|--------|-----------|----------------------------|
| **YBTY** | 5 child YB ETFs (~97% invested) | SMYY 20.3%, IOYY 20.2%, HOYY 20.0%, TSYY 19.8%, COYY 19.4% · cash 0.26% |
| **YBST** | 17 child YB ETFs (~67% invested) | Equal-ish weights ~5.6–6.0% each · cash **33.4%** |

Child mapping lives in `scripts/yieldboost_fof_constants.py` (`YIELDBOOST_CHILD_TO_UNDERLYING`).

**Trader pair of interest:**

> **Short FoF (YBTY/YBST) + long weighted underlying basket** (proportional to Granite child weights, excluding cash).

This is **not** the same as shorting each child YB ETF individually — the FoF holds child shares, so NAV path = weighted child NAV + cash − FoF fees.

---

## 3. Current architecture (v1)

```
Granite XLS ──► etf_holdings_providers ──► etf_holdings_latest.csv
                                              │
build_data.py step 5d ◄───────────────────────┘
  ├─ yieldboost_fof_holdings.py  → yieldboost_fof_holdings_{latest,history}.json
  └─ yieldboost_fof_pair_pnl.py  → synthetic dashboard rows (append to records)

Synthetic row fields:
  product_class: income_yieldboost_fof
  expected_pair_pnl_* : weighted_child_forward_metrics()   ← child screener rows
  gross_decay_annual  : compute_fof_realized_pair_metrics() ← needs FoF NAV series
  net_edge_p50        : weighted child net − FoF borrow
  fof_basket / fof_realized_pair / fof_underlyings
```

**Not in ls-algo screener CSV** — entirely `etf-dashboard` synthetic.

---

## 4. Production state (2026-06-04 audit)

### 4.1 Dashboard rows (`data/dashboard_data.json`)

| Field | YBTY | YBST |
|-------|------|------|
| `gross_decay_annual` | `null` | `null` |
| `expected_pair_pnl_p50_annual` | **0.730** | **0.693** |
| `net_edge_p50_annual` | 0.491 | 0.249 |
| `borrow_current` | 6.9% | **24.3%** |
| `fof_realized_pair.ok` | **false** | **false** |
| `fof_realized_pair.error` | insufficient FoF price history | same |
| `pair_scenario_grid` | absent | absent |
| `net_edge_hist_json` | absent | absent |

Forward p50 ≈ **73% log/yr** because v1 averages child `expected_pair_pnl_p50` (e.g. SMYY 0.766, IOYY 0.781) — these are **single-name short-YB + long-underlying pair** anchors, not FoF NAV decay.

### 4.2 ETF metrics (`data/etf_metrics_daily.csv`)

```
YBTY rows: 0
YBST rows: 0
```

Granite provider **does** return NAV (verified live):

```
YBTY  nav=13.51  aum=2.70M  (product id 1163)
YBST  nav=14.76  aum=1.92M  (product id 1162)
```

Tickers are in `load_universe_tickers()` via `YIELDBOOST_FOF_SYMBOLS`, but **nightly metrics ingest has not populated the store** (likely: code merged after last `ingest_etf_metrics.py` run, or ingest never backfilled history).

### 4.3 Holdings

- `etf_holdings_latest.csv`: YBTY 5 children + cash; YBST 17 children + cash ✓
- `yieldboost_fof_holdings_history.json`: **1 snapshot each** (no rebalance history yet)

---

## 5. Root causes (ranked)

### P0 — No FoF NAV time series

`compute_fof_realized_pair_metrics()` reads `_fof_nav_series(metrics, symbol)` → empty → **all realized / Decay paths dead**.

Without metrics ingest + backfill, every downstream FoF analytics feature is blocked.

### P1 — Forward model uses wrong economic object

`weighted_child_forward_metrics()` blends child **`expected_pair_pnl_p50_annual`** (short child ETF + β-long underlying pair gross).

FoF short edge should reflect:

```
FoF NAV decay  vs  weighted underlying basket
≈ Σ w_i · (child_i NAV return)  + cash return  − FoF fees
   compared to Σ w_i · (underlying_i return)
```

Child **pair P&L** (76% log/yr) ≠ child **NAV decay** (SMYY realized gross ~38%). v1 forward **overstates** FoF edge by ~2× vs realized child gross and ignores cash + fee layers.

### P2 — Net edge is a heuristic, not a distribution

```python
net_edge = weighted_child_net_edge - fof_borrow
```

Problems:

- Child net already includes **child borrow**, not FoF borrow.
- No `net_edge_p05/p95`, no `net_edge_hist_json`.
- YBST borrow 24% dominates → net 25% despite 69% forward gross.

### P3 — Realized pair engine limitations (even after NAV fix)

Current `compute_fof_realized_pair_metrics`:

- Uses **piecewise-constant** weights from holdings history (OK with 1 snap = static weights).
- Builds underlying series from **all** `etf_metrics_daily` rows with matching `underlying` — works once child ETFs have `underlying_adj_close`.
- Does **not** model cash drag explicitly.
- Does **not** handle child reverse splits inside basket (inherits child split bugs — see `docs/reverse-split-fix-prompt.md`).
- Requires ≥5 FoF NAV days — new listings need bootstrap.

### P4 — UI / tab gaps

| Tab | FoF behavior | Issue |
|-----|--------------|-------|
| Main grid | Shows forward + net; gross `—` | Misleading forward |
| Chart | YBTY vs **SMCI only** (`fofPrimaryChartCompareSymbol`) | Should show basket index or top-3 |
| Decay | FoF panel only if `fof_realized_pair.ok` | Always empty today |
| Backtest | **Disabled** (`showBacktestPanel = !isFoF`) | No synthetic pair backtest |
| Scenarios | **Hidden** (`suppressScenarioForRow` = bucket-2 non-income) | OK for now; needs FoF-specific grid later |
| Stats | No metrics rows | Empty |
| Trade Lab | Prefills short FoF + weighted long underlyings | Good start; spots for 17 names heavy |
| Vol/VRP | N/A | FoF has no put-spread sleeve |
| Basket panel | **Working** | Best current UI |

### P5 — Operational / data pipeline

- FoF holdings history not accumulated daily (only latest XLS date in store).
- No CI test on “FoF rows must have `fof_realized_pair.ok` when metrics ≥30d”.
- `product_class: income_yieldboost_fof` undocumented in `AGENTS.md` routing matrix.
- `isYieldBoostIncomeStrategy()` explicitly excludes FoF → correct for YB put-spread Scenarios, but blocks reuse of income tooling.

---

## 6. Target economics (v2 model)

### 6.1 Realized gross (short-favorable positive)

Daily aligned series:

```
r_fof_t   = NAV_FoF(t) / NAV_FoF(t-1)
r_basket_t = Σ_u w_u(t-1) · Und_u(t) / Und_u(t-1)   [renormalize weights ex-cash]
drag_t    = log(r_basket_t) - log(r_fof_t)
gross_annual = mean(drag) · 252
```

Optional: subtract implied **cash yield** on `cash_pct` if cash earns ≠ 0.

### 6.2 Forward gross (FoF-native)

**Preferred — bottom-up NAV simulation:**

```
NAV_hat_FoF ≈ (1 - cash_pct) · Σ w_child_i · NAV_child_i / NAV_child_i_anchor
              + cash_pct · 1.0
              - expense_ratio_fof · dt
```

Compare to weighted underlying basket at scenario σ/horizon using child **`expected_gross_decay_p50`** (NAV structural decay) not pair P&L:

```
exp_fof_gross ≈ Σ w_i · child_expected_nav_decay_p50
                - fof_expense_ratio
                - cash_drag(w_cash, rf)
                + tracking_error_buffer
```

**Alternative — holdings-implied MC (diagnostic):**

Monte Carlo weekly rebalance: FoF holds child shares; child NAVs evolve via existing YB put-spread MC; aggregate to FoF NAV path.

### 6.3 Net edge

```
net_fof ≈ blend(realized_fof_gross, forward_fof_gross) - borrow_fof
```

Reuse screener inverse-variance blend **on FoF axis** once forward band exists; until then block-bootstrap daily `drag_t` series.

---

## 7. Upgrade plan (phased)

### Phase 1 — Unblock data (P0) — **1–2 days**

**Goal:** YBTY/YBST rows in `etf_metrics_daily` with ≥60 NAV days.

1. **`ingest_etf_metrics.py`**
   - Confirm FoF tickers in nightly universe (already in `load_universe_tickers`).
   - Add **`bootstrap_fof_nav_history()`**: pull Granite product JSON historical NAV if available; else Yahoo `etf_adj_close` fallback for FoF tickers.
   - Set `underlying` column to `BASKET` (or leave blank) — FoF rows don't need single `underlying_adj_close`.

2. **One-time backfill script** `scripts/backfill_fof_metrics.py`:
   - Fetch NAV for YBTY/YBST from listing date → today via Granite + Yahoo.
   - Append to parquet/csv/json store.

3. **CI / diagnostics**
   - `scripts/fof_pipeline_diagnostics.py`: fail if FoF symbols missing from metrics or `< 20` NAV rows.

4. **Verify** after nightly + build:
   - `fof_realized_pair.ok === true`
   - `gross_decay_annual` finite on main grid

### Phase 2 — Fix realized + Decay (P1/P3) — **2–3 days**

1. **`yieldboost_fof_pair_pnl.py`**
   - Prefer `nav` from metrics; document precedence (`etf_adj_close` → `nav` → `close_price`).
   - Explicit **cash leg**: reduce invested weight by `cash_pct`; optional cash return = 0 or SOFR.
   - Emit `fof_realized_pair.cash_pct`, `fof_realized_pair.effective_invested_pct`.

2. **Holdings history**
   - Ensure daily XLS ingest **appends** snapshots to parquet (don't overwrite single date).
   - `build_fof_holdings_history` should grow >1 snap/week.

3. **Decay tab**
   - Show FoF horizons from `fof_realized_pair.horizons` (already wired in `index.html` when `ok`).
   - Warning badge when `< 40` aligned days or weights stale >14d.

4. **Tests**
   - Extend `tests/test_yieldboost_fof.py` with production-shaped fixture (multi-underlying basket, 60+ NAV days).
   - Golden: YBTY gross in plausible band vs weighted child realized.

### Phase 3 — Forward model v2 (P1/P2) — **1 week**

1. **New module** `scripts/yieldboost_fof_forward.py`:
   - `weighted_child_nav_decay_forward(basket, child_records)` using `expected_gross_decay_p50_annual` (put-spread structural NAV decay), **not** `expected_pair_pnl_p50`.
   - Subtract FoF expense ratio (Granite product JSON or constant ~99 bps).
   - Cash drag term from `cash_pct`.

2. **`build_fof_dashboard_record`**
   - Replace `weighted_child_forward_metrics` pair-P&L blend with v2 forward.
   - Store both for transition:
     - `expected_pair_pnl_p50_annual` → v2 FoF forward gross
     - `fof_forward_meta.child_pair_p50_blend` → legacy diagnostic

3. **Net edge v2**
   - Bootstrap `daily_drags` → `net_edge_p05/p25/p50/p75/p95` + compact hist JSON.
   - Final: `posterior_gross - borrow_fof` (borrow only once).

4. **Optional:** `pair_scenario_grid` for FoF — 5×5 grid on basket σ vs drift using weighted child grids (if children expose `pair_scenario_grid`).

### Phase 4 — UI completeness (P4) — **3–5 days**

1. **Chart tab**
   - Replace single-underlying compare with **synthetic basket index** (% change of weighted underlying TR) or multi-line overlay.
   - Label: “FoF vs weighted basket (holdings as-of …)”.

2. **Main grid**
   - Gross column populated; tooltip explains FoF pair vs child pair.
   - Sub-label: `FoF basket · p10/p90` when band exists.

3. **Scenarios tab (FoF-specific)**
   - New `FoFScenarioPanel`: horizon × σ on **basket**, show FoF NAV decay vs basket (not hidden passive-low-β suppression).
   - Re-enable tab for `income_yieldboost_fof` only.

4. **Backtest**
   - Either enable `simulateInversePairBacktest` with synthetic basket TR + FoF TR, or keep disabled with clear “use Decay horizons” message.

5. **Stats tab**
   - Show FoF NAV/AUM/shares from metrics; link to basket panel.

6. **AGENTS.md**
   - Add `income_yieldboost_fof` to product routing matrix.

### Phase 5 — ls-algo alignment (optional, **later**)

Move FoF from dashboard-only synthetic to upstream screener if YBTY/YBST enter ls-algo universe:

- Export FoF realized gross + forward from ls-algo with same definitions.
- Dashboard becomes consumer, not producer.

---

## 8. Acceptance criteria

### Phase 1 done when

- [ ] `etf_metrics_daily.csv` has ≥60 rows each for YBTY, YBST.
- [ ] `fof_realized_pair.ok === true` in `dashboard_data.json`.
- [ ] `gross_decay_annual` is finite (not `null`).

### Phase 2 done when

- [ ] Decay tab shows horizon bars for YBTY/YBST without error banner.
- [ ] Realized gross within plausible range (e.g. 15–50% log/yr — validate vs manual basket calc).
- [ ] Holdings history ≥4 weekly snapshots after 1 month of ingest.

### Phase 3 done when

- [ ] Forward Exp. edge uses **child NAV decay** blend, not child pair P&L blend.
- [ ] YBTY forward < weighted child pair P&L (currently ~73%).
- [ ] Net edge fan (p5–p95) present on FoF rows.
- [ ] YBST net edge reflects high FoF borrow without double-counting child borrow.

### Phase 4 done when

- [ ] Chart compares FoF to **basket index**, not SMCI alone.
- [ ] FoF Scenarios panel live (or documented deferral).
- [ ] `AGENTS.md` updated.

---

## 9. Implementation prompt (for agent)

```
Upgrade YieldBOOST FoF (YBTY/YBST) from v1 scaffolding to production-quality.

Current bugs:
- etf_metrics_daily has ZERO rows for YBTY/YBST → fof_realized_pair fails, gross_decay null, Decay empty.
- Forward edge wrongly averages child expected_pair_pnl_p50 (short-YB pair) instead of FoF NAV economics.
- net_edge = weighted child net - FoF borrow (no bootstrap, double borrow layer confusion).
- Chart compares FoF to top underlying only; Scenarios hidden; no pair_scenario_grid.

Read: docs/yieldboost-fof-upgrade-proposal.md

Phase 1 (do first):
1. backfill_fof_metrics.py + ingest hook — Granite NAV history for YBTY/YBST.
2. fof_pipeline_diagnostics.py — CI gate.
3. Re-run build_data; verify fof_realized_pair.ok.

Phase 2:
4. Fix compute_fof_realized_pair_metrics cash + weight handling.
5. Accumulate holdings history daily.
6. Tests with 60-day synthetic fixture.

Phase 3:
7. yieldboost_fof_forward.py — forward from weighted child expected_gross_decay_p50 minus FoF ER and cash drag.
8. Bootstrap net_edge fan from daily drag series.

Phase 4 (UI):
9. Basket index chart compare; FoF scenario panel; AGENTS.md row.

Do NOT add YBTY/YBST to ls-algo in this pass.
Preserve FoFBasketPanel; extend don't replace.
Child split bugs: use split-aware child NAV from metrics once reverse-split fix lands.
```

---

## 10. Related work

- Reverse splits on basket children (MTYY, TSYY, etc.): [`docs/reverse-split-fix-prompt.md`](reverse-split-fix-prompt.md)
- YieldBOOST single-name model: `AGENTS.md` §4.5, `expected_pair_pnl_basis = put_spread_structural_gross`
- Existing tests: `tests/test_yieldboost_fof.py` (7 cases — extend, don't delete)

---

## 11. Risk notes

| Risk | Mitigation |
|------|------------|
| FoF listing too new for meaningful realized | Show “N days” badge; require ≥20d before ranking |
| YBST 33% cash depresses gross | Model explicitly; show cash % in Decay header |
| Child ticker map misses renamed YB | Extend `infer_yb_child_ticker`; alert on `other_pct` > 5% |
| Granite XLS lag | `weights_stale_days` badge already exists; escalate >14d |
| YBST borrow 24% | Display prominently; don't mix with child borrow in tooltip |

# YBTY / YBST — Income Scenario Returns alignment plan

**Status:** implemented 2026-06-07

**Goal:** Make the **Income Scenario Returns** panel on YBTY and YBST reflect a **holdings-weighted average of child YieldBOOST ETF scenarios** — the same closed-form engine used for MTYY/TSYY — **without** embedding a synthetic **short FoF + long underlying basket** pair in that panel.

**Not in scope:** Realized gross decay, Decay tab pair backtest, or chart “short FoF vs basket” analytics — those stay on the basket pair axis by design.

---

## Product facts (prospectus / holdings)

| Fund | Granite product | Structure | Child count | Cash (holdings) |
|------|-----------------|-----------|-------------|-----------------|
| **YBTY** | [YieldBoost TopYielders ETF](https://www.graniteshares.com/product/1163/en-us/) (id 1163) | **Fund of YieldBOOST ETFs** | **5** (~20% each) | ~0.26% |
| **YBST** | [YieldBoost Broad ETF](https://www.graniteshares.com/product/1162/en-us/) (id 1162) | **Fund of YieldBOOST ETFs** | **17** (~5.9% each) | ~0.33% |

**YBTY children (2026-06-04):** SMYY, IOYY, HOYY, TSYY, COYY → SMCI, IONQ, HOOD, TSLA, COIN.

**YBST** holds those same five names at ~5.9% each **plus twelve others** (RTYY, BBYY, AMYY, …). It is **not** “the same five ETFs at equal weight” — only ~30% overlap with YBTY’s sleeve.

**Implication for “same returns”:**

- **Same methodology** for YBTY and YBST: yes.
- **Same numeric grid** for YBTY and YBST: **no** (different weights and child sets).
- **YBTY grid ≈ weighted average of its five children** at each σ-shock row: yes — that is the target.

Granite structure: FoF holds **other Granite YieldBOOST ETFs**, not a direct proportional long in TSLA/SMCI/etc. The Income Scenario panel should mirror that for scenario P&amp;L, not the trader pair lens.

---

## Current behavior (why it feels wrong)

### Income Scenario grid (`index.html` → `computeFoFIncomeScenarioGrid`)

Today the grid **does** loop children and run `estimateIncomeStyleScenarioReturn` per child (with each child’s σ and calibrated yield). It accumulates:

- `wNavDecay`, `wDist`, `wLongTr`, **`wNetShortChild`**

But the row it **displays** throws away the child short average and substitutes **basket pair math**:

```javascript
// index.html ~3365 — TODAY (bug for this panel)
const netShortPnl = basketUndReturn - longTotalReturn - borrow * t;
```

Where `basketUndReturn = exp(σ_multiple × σ_FoF × √t) − 1` — one FoF-level underlying shock, **not** per-child shocks already used inside the loop.

So the panel mixes:

1. Child-weighted NAV decay / distributions / long TR (good)
2. FoF-level synthetic **long basket** return minus that long TR (bad for this panel)

That is the **Trade Lab / pair** framing (`short FoF + long basket`), which you explicitly do **not** want here.

### Contrast: Long-holder path already correct

`computeFoFLongHolderReturn` (~3250) **only** weights child `computeExpectedEtfReturnFlatUnd` results — no basket pair term. FoF Income Scenarios should follow that pattern for **short** side too: weight child `netShortPnl`, then apply FoF ER and FoF borrow once.

### Secondary bug: `cash_pct` scale

Holdings store `cash_pct` as **percent** (e.g. `0.2603` = **0.26%**, not 26%).

| Layer | Parser | YBTY invested fraction |
|-------|--------|------------------------|
| Python `_cash_fraction()` | `v ≤ 1` → treat as fraction | **74%** invested (wrong) |
| JS `computeFoFIncomeScenarioGrid` | same | **74%** invested (wrong) |
| True from holdings | `cash_pct / 100` | **~99.7%** invested |

This deflates NAV decay, distributions, and long TR in the scenario grid (~×0.74). Fix before or with the main change.

### Build-time forward metrics (separate from scenario grid)

`yieldboost_fof_forward.py` already rolls child **NAV structural decay** for Exp. edge / heatmap (`weighted_child_nav_decay_forward`). That is consistent with “average of children” for **gross structural** forward, minus FoF ER. It does **not** drive the Income Scenario Returns table today (client-only).

---

## Target semantics

For each FoF (YBTY, YBST), at horizon `t` and σ-shock row `k`:

```
For each child i with weight w_i (normalized to invested sleeve):
  Run the SAME single-name income scenario as MTYY:
    estimateIncomeStyleScenarioReturn({
      underlyingReturn: exp(k × σ_i × √t) − 1,   // per-child σ
      sigmaAnnual: σ_i,
      annualIncomeYield: calibratedYield_i,
      annualBorrowCost: borrow_i,               // child borrow (informational)
      horizonYears: t,
    })

FoF row (invested sleeve only):
  navDecay_FoF      = (Σ w_i × navDecay_i) × investedFrac
  distributions_FoF = (Σ w_i × dist_i) × investedFrac
  longTR_FoF        = (Σ w_i × longTR_i) × investedFrac − ER_FoF × t
  netShort_FoF      = (Σ w_i × netShort_i) × investedFrac − ER_FoF × t − borrow_FoF × t
```

**Do not use:** `basketUndReturn − longTR_FoF − borrow`.

**Borrow:** Apply **FoF borrow** once at the FoF row (dashboard `borrow_for_net_annual`), not summed child borrows — matches “short the FoF share” while scenarios are child-averaged.

**σ header:** Keep FoF-level scenario σ from weighted child forecast vol for display; per-child σ still used inside each child scenario (already done).

**Modeled cash yield header:** Keep weighted child calibration rollup (`incomeAnnualYieldForScenario(r, …)`); cap at 150% (`MAX_INCOME_YIELD_ANNUAL`).

---

## Implementation plan

### Phase 1 — Fix scenario grid (client, required)

**File:** `index.html` → `computeFoFIncomeScenarioGrid`

1. **Replace net short formula**
   ```javascript
   const netShortPnl = (wNetShortChild / wgot) * investedFrac - erDrag - borrow * t;
   ```
   Optionally expose `childNetShortBlended: wNetShortChild / wgot` for debugging.

2. **Fix `cash_pct` parsing** (also in `computeFoFLongHolderReturn`)
   ```javascript
   const cashFrac = Number.isFinite(cashPct) ? Math.min(1, Math.max(0, cashPct / 100)) : 0;
   ```
   Granite always stores percent on 0–100 scale (including values &lt; 1).

3. **UI copy** under Income Scenario Returns for FoF:
   > “Child-weighted YieldBOOST scenarios (holdings average). Not the short-FoF vs underlying-basket pair.”

4. **Regression test (new):** `tests/test_fof_income_scenario.js` (or extend `test_yieldboost_fof_forward.js` with exported helper)
   - Mock 2-child basket; assert FoF net short at σ=0 equals weighted child net shorts minus ER − FoF borrow.
   - Assert net short **≠** `basketUndReturn − longTR − borrow` when basket σ ≠ child blend.

### Phase 2 — Python parity (recommended)

**File:** `scripts/yieldboost_fof_forward.py`

Add `build_fof_income_scenario_grid()` mirroring Phase 1 math (reuse `scenario_grid_put_spread_pair` per child or read precomputed child grids).

**File:** `scripts/yieldboost_fof_pair_pnl.py` / `build_data.py`

- Attach `income_scenario_grid` on FoF records (optional compact: 5 σ rows × horizons).
- Lets CI validate without browser.

Fix `_cash_fraction()` for `cash_pct < 1`:

```python
# Always percent scale from Granite holdings
return min(1.0, max(0.0, float(raw) / 100.0))
```

Re-run `build_data.py` → refresh forward p50 / heatmap `invested_fraction` (~+25–30 pts).

### Phase 3 — Calibration rollup sanity (optional, separate)

YBTY `blended_ratio_used` ~4.16× vs YBST ~2.03× inflates modeled cash yield. Overlaps with [`reverse-split-distribution-fix-prompt.md`](reverse-split-distribution-fix-prompt.md) Tier-3 (TSYY/COYY in YBTY sleeve).

Options (pick one):

- Winsorize per-child ratios in `build_fof_income_distribution_calibration`
- Use fleet cross-fund prior when any child ratio &gt; 2.5×
- Show **per-child** capture in FoF panel instead of one rolled headline

This affects **distribution cash** in scenarios, not the basket-pair bug.

### Phase 4 — Documentation

- **AGENTS.md** §5 / income FoF routing: Income Scenario Returns = child average; pair backtest = basket.
- Update `docs/yieldboost-fof-upgrade-proposal.md` cash % correction (0.33% not 33%).

---

## Acceptance criteria

| Check | YBTY | YBST |
|-------|------|------|
| At σ=0, net short ≈ Σ(w×child net short)×invested − ER − FoF borrow | ✓ | ✓ |
| No `basketUndReturn − longTR` in scenario net short | ✓ | ✓ |
| `investedFrac` ≈ 0.997 (not ~0.74) | ✓ | ✓ |
| YBTY 5-name grid ≈ manual weighted average of SMYY…COYY single-name grids | ✓ | n/a |
| YBST grid uses 17 children, **different numbers** from YBTY | n/a | ✓ |
| Long-holder FoF chart unchanged (already child-weighted) | ✓ | ✓ |
| Decay / realized pair tabs still use basket pair | ✓ | ✓ |

**Manual sanity (YBTY, 3M, flat underlying):**

1. Open Scenarios for TSYY, COYY, … record each net short at σ=0.
2. Weight by YBTY holdings (~20% each).
3. Compare to YBTY grid net short — should match within rounding after ER/borrow.

---

## Agent implementation prompt (copy-paste)

```
Align YBTY/YBST Income Scenario Returns with holdings-weighted child ETF scenarios.

Problem: computeFoFIncomeScenarioGrid (index.html) weights child navDecay/dist/longTR but sets
  netShortPnl = basketUndReturn - longTotalReturn - borrow*t
which embeds short-FoF-vs-long-basket pair math. YBTY/YBST are funds OF YieldBOOST ETFs; this panel should be the weighted average of each child's estimateIncomeStyleScenarioReturn (same as MTYY), minus FoF ER and FoF borrow once — NOT long the underlying basket.

Also fix cash_pct: Granite holdings use percent (0.26 = 0.26% cash). JS and Python currently treat values <1 as fractions (~74% invested). Use cash_pct/100 everywhere (_cash_fraction in yieldboost_fof_forward.py, computeFoFIncomeScenarioGrid, computeFoFLongHolderReturn).

Steps:
1. index.html computeFoFIncomeScenarioGrid: netShortPnl = (wNetShortChild/wgot)*investedFrac - erDrag - borrow*t; fix cashFrac.
2. yieldboost_fof_forward.py: fix _cash_fraction; optional build_fof_income_scenario_grid for build_data parity.
3. Tests: weighted child net short at sigma=0; YBTY two-child fixture; cash_frac 0.26 -> 0.997 invested.
4. FoF UI subtitle clarifying child-weighted vs pair backtest.

Do NOT change Decay tab, compute_fof_realized_pair_metrics, or basket TR series.
YBTY and YBST should use the SAME formula but will NOT have identical numbers (5 vs 17 children).
```

---

## Files touched (expected)

| File | Change |
|------|--------|
| `index.html` | `computeFoFIncomeScenarioGrid`, `computeFoFLongHolderReturn` cash + net short |
| `scripts/yieldboost_fof_forward.py` | `_cash_fraction`, optional grid builder |
| `scripts/build_data.py` | Optional embed scenario grid on FoF rows |
| `tests/test_fof_income_scenario.js` or `.py` | New regression tests |
| `docs/yieldboost-fof-upgrade-proposal.md` | Cash % note |
| `AGENTS.md` | FoF scenario semantics one paragraph |

---

## Related

- Pair / Decay framing: `docs/yieldboost-fof-upgrade-proposal.md`
- Child calibration rollup: `scripts/yieldboost_fof_forward.py::build_fof_income_distribution_calibration`
- Distribution basis: `docs/reverse-split-distribution-fix-prompt.md`

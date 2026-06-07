# LETF Exp. ETF return vs Scenarios alignment — audit & implementation plan

**Status:** implemented 2026-06-07

**Goal:** Make the main-grid **Exp. ETF return (3M)** and the Scenarios tab **0σ row** agree for vol-drag LETF names (SMUP, BITU, …) — same horizon, same forecast σ, same cash layer — and eliminate **phantom positive returns** caused by split-basis bugs and one-time distributions.

Companion to [`reverse-split-distribution-fix-prompt.md`](reverse-split-distribution-fix-prompt.md) (YieldBOOST distribution calibration) and [`reverse-split-fix-prompt.md`](reverse-split-fix-prompt.md) (YieldBOOST run-rate). This doc covers the **LETF / inverse** path: `dividend_adjustment` window yields feeding `dividendCashAdditiveForHorizon`.

---

## Executive summary

The Scenarios tab and main grid **look** like the same metric but are not:

| Layer | Scenarios tab (0σ) | Main grid Exp. ETF return (3M) |
|-------|-------------------|--------------------------------|
| Vol drag @ flat und | ✓ `estimateEtfReturn` | ✓ same engine |
| Forecast σ | ✓ `forecast_vol_underlying_annual` | ✓ same |
| Horizon | ✓ user-selected (3M in screenshot) | ✓ fixed 3M headline |
| **Cash dividends** | **✗ omitted** | **✓ added via `dividendCashAdditiveForHorizon`** |
| Dividend yield source | n/a | **chart lookback window** (often **6M**, not 3M) |

For **SMUP** at 3M / σ ≈ 107.6% / L ≈ 2:

| Component | Value |
|-----------|-------|
| 3M vol drag (flat und) | **−25.0%** |
| Phantom dividend add (6M window, annualized × 3M) | **+39.8%** |
| **Main grid total** | **+14.8%** ← user confusion |
| **Scenarios 0σ** | **−25.8%** ← structurally correct for flat spot |

The +39.8% is **not** recurring income. It comes from **`dividend_adjustment['6M'].etf_dividend_yield ≈ 79.6%`**: a **$52.75** Yahoo dividend in the 6M window divided by split-adjusted start price **$66.25**, without per-event split-basis correction. That dividend predates / spans SMUP reverse splits and is treated like a recurring yield when annualized.

**BITU** shows the same pattern at smaller magnitude: 3M vol drag **−5.0%**, 6M dividend add **+9.9%** → main grid **+4.9%** vs Scenarios **−5%**.

---

## Evidence — SMUP (2026-06-07 dashboard)

### Window diagnostics (`dividend_adjustment`)

| Window | `etf_total_dividends` | `etf_dividend_yield` | `etf_start_close_on_end_basis` |
|--------|----------------------:|---------------------:|-------------------------------:|
| 1M | 0 | 0 | 11.02 |
| 3M | 0 | 0 | 13.93 |
| **6M** | **52.75** | **0.796** | **66.25** |
| 12M | 52.75 | 0.085 | 622.90 |

Only the **6M** window is polluted. Because the main grid reads dividend yield from **`chartVolLookbackRange`** (default **6M** chart timeframe), the headline **3M** return inherits a **6M** dividend story.

### Split metadata conflict (SMUP)

| Source | Event | Notes |
|--------|-------|-------|
| `_MANUAL_SPLIT_OVERRIDES` | 2026-01-26, factor **0.1** (1-for-10) | Used for Yahoo adj-close repair |
| `corporate_actions.json` | 2026-04-08, **1-for-25** (Polygon) | Not reconciled with manual hint |

Price-basis helpers already scale **closes** through splits; **dividend amounts are still summed raw**. Reconcile SMUP split events during this work so dividend adjustment uses the same execution timeline as price basis.

---

## Root cause map

```
Yahoo chart dividends (per ex-date, possibly restated)
        │
        ▼
_build_market_windows()  ──► total_dividends += amount   ◄── NO split-basis fix today
        │
        ▼
dividend_yield = total_dividends / start_close_on_end_basis   ◄── denominator IS split-adjusted
        │
        ▼
dashboard_data.json → dividend_adjustment[window].etf_dividend_yield
        │
        ├─► Scenarios tab: NOT USED (vol drag only)
        │
        └─► computeExpectedEtfReturnFlatUnd():
              nav = estimateEtfReturn(und=0, σ, T=3M)
              div = dividendCashAdditiveForHorizon(r, chartVolLookbackRange, T=3M)
                    └── reads yield from chart window (6M), not headline horizon (3M)
              return nav + div
```

### Bug 1 — Split basis on window dividends (primary)

Same class of bug as YieldBOOST `normalize_events`: Yahoo **restates** historical `$ / share` onto the post-split basis while our window denominator uses **`start_close_on_end_basis`**. Summing restated amounts without undoing the split factor inflates yield — up to **~8×** for recent Granite-style reverse splits, and worse when multiple splits stack.

**Fix location:** `scripts/build_data.py` → `_build_market_windows()` dividend loop (~3496–3500).

### Bug 2 — Horizon / lookback mismatch

Headline column is **3M**, but dividend yield is pulled from **`chartVolLookbackRange`** (typically **6M**). SMUP **3M yield is 0%**; **6M yield is 79.6%**. The column title says 3M; the cash leg silently uses a different window.

**Fix location:** `index.html` → `dividendCashAdditiveForHorizon` / `computeExpectedEtfReturnFlatUnd`.

### Bug 3 — Scenarios omit cash layer

Scenarios footer: *"cells = long **total return** without borrow"*. Grid cells use **`estimateEtfReturn` only** (NAV path). Main grid adds cash for non–YieldBOOST rows. Labels claim shared σ; they do not share the return definition.

**Fix location:** `index.html` scenario grid render path (~7407) and/or `assets/scenario_returns.js` `buildScenarioGrid`.

### Bug 4 — One-time / special distributions annualized as recurring

SMUP’s **$52.75** is almost certainly a **lumpy capital return or pre-split special**, not a weekly income program. Window yield logic treats any in-window cash as recurring when annualized into Exp. ETF return.

**Fix location:** `_build_market_windows` and/or client-side guard in `dividendCashAdditiveForHorizon`.

### Bug 5 — Product policy gap

`scenario_style = letf_vol_drag` names are **structural decay products**, not income ETFs. The dividend add path was built for ordinary dividend payers but also runs on **2× vol-drag LETFs** with occasional issuer distributions.

---

## Target semantics (after fix)

For **`letf_vol_drag`** rows (SMUP, BITU, TQQY, …):

1. **Exp. ETF return (3M)** at flat underlying = **3M NAV vol drag** + **3M economic cash yield** (if any), using **`forecast_vol_underlying_annual`**, borrow excluded.
2. **Scenarios 0σ row** (same horizon) = **same number** as (1) at σ multiplier **1.0×** (± rounding).
3. Dividend yield must be **split-adjusted** per ex-date before window aggregation.
4. **One-time specials** must not annualize into multi-month forward expectations.
5. **YieldBOOST income rows** unchanged — they already use `estimateIncomeStyleScenarioReturn` / `longTotalReturn`.

For rows with **no recurring cash** (SMUP today): both surfaces show **negative** flat-spot return (~vol drag only).

---

## Implementation plan

### Phase 1 — Split-adjust window dividends (required, Python)

**1.1 Shared helper** — add to `scripts/split_adjustments.py` (preferred) or reuse from `income_schedule.py`:

```python
def adjust_window_dividend_for_split(
    amount: float,
    ex_date: dt.date,
    *,
    split_events: list[tuple[dt.date, float]],
    close_on_ex_date: float | None = None,
) -> tuple[float, str]:
    """Return (amount_economic, basis) for realized-window yield sums."""
```

Logic (mirror `adjust_distribution_amount_for_split`):

- Build discrete split context from `split_events` (latest reverse split before `ex_date` if multiple).
- If `ex_date >= boundary`: return amount unchanged (`post_split`).
- If `close_on_ex_date > 0` and `amount / close_on_ex_date <= MAX_WEEKLY_YIELD_FRAC` (~5%): treat as as-paid (`as_paid`).
- Else if Yahoo restated pre-split row: return `amount / mult` (`yahoo_restated`).

Export `MAX_WINDOW_DIVIDEND_YIELD_FRAC` (e.g. **0.05** per event) for one-time detection.

**1.2 Wire into `_build_market_windows`**

For each `(ex_date, amount)` in window:

1. Optionally map `close_on_ex_date` from nearest bar in `scoped` points.
2. `amount_ec, basis = adjust_window_dividend_for_split(...)`.
3. `total_dividends += amount_ec`.
4. Persist diagnostics on window dict:
   - `etf_total_dividends_economic`
   - `etf_dividend_yield_economic` (replace or shadow `etf_dividend_yield`)
   - `dividend_split_basis` summary (counts by basis)

Keep legacy fields during transition; dashboard builder can prefer `*_economic` when present.

**1.3 SMUP split reconciliation**

- Merge manual hint (2026-01-26) with Polygon (2026-04-08) via existing `merge_split_events` / `dedupe_split_events`.
- Add regression test: SMUP-style window with $52.75 pre-split restated div → economic yield **≪ 79%**.
- Verify `filter_splits_needing_close_basis_fix` still correct for SMUP price path.

**1.4 Rebuild**

Run `build_data.py` → refresh `data/dashboard_data.json`.

**Acceptance (Phase 1):**

- SMUP `dividend_adjustment['6M'].etf_dividend_yield` drops from **~80%** to **≤ ~8%** (economic one-time) or **0%** if special is excluded in Phase 2.
- BITU 6M yield drops from **~20%** to low single digits if splits/specials corrected.

---

### Phase 2 — Horizon match + one-time guard (required, JS + optional Python)

**2.1 Match dividend window to headline horizon**

In `computeExpectedEtfReturnFlatUnd`:

```javascript
// Today: chartVolLookbackRange drives dividend window
// Target: horizonLabel drives dividend window ('3M' headline → dividend_adjustment['3M'])
const divAdd = dividendCashAdditiveForHorizon(r, horizonLabel, horizonYears);
```

Update `dividendCashAdditiveForHorizon(r, windowKey, horizonYears)` to accept explicit window key; keep chart lookback only as fallback for legacy tooltips if needed.

**Immediate SMUP win:** 3M headline uses **3M yield = 0%** → Exp. ETF return ≈ **−25%** even before Phase 1 ships (partial fix). Still implement Phase 1 so 6M chart / tooltips are not wrong.

**2.2 One-time / lumpy distribution guard**

Python (preferred, at build time):

- If a single adjusted dividend exceeds **`MAX_WINDOW_DIVIDEND_YIELD_FRAC × start_close_on_end_basis`**, flag `dividend_lumpy: true` on window.
- When lumpy and window has **≤ 2** dividend events, set `etf_dividend_yield_recurring = 0` (or cap window yield at median of non-outlier events).

JS (belt-and-suspenders):

- In `dividendCashAdditiveForHorizon`, skip add when `r.dividend_adjustment[vk].dividend_lumpy === true`.
- Cap annualized additive contribution for `letf_vol_drag` at e.g. **`MAX_LETF_DIV_ADD_ANNUAL = 0.05`** (5%/yr) unless product is explicitly income.

**2.3 Product policy (recommended default)**

For `scenario_style === 'letf_vol_drag'`:

- Use **horizon-matched** window yield only (no cross-window annualization from 6M chart).
- Do **not** add dividend layer when horizon window yield is 0 or lumpy flag set.

Passive **bucket_2_low_beta** rows already suppressed — no change.

**Acceptance (Phase 2):**

- SMUP Exp. ETF return (3M) **negative**, within **0.5 pp** of Scenarios 0σ @ 1.0× vol.
- BITU same.
- Changing chart timeframe from 6M → 3M does **not** flip SMUP sign.

---

### Phase 3 — Scenarios grid alignment (required, JS)

**3.1 Add cash layer to standard Scenario Returns grid**

After Phase 1–2 data is clean, extend scenario grid cells:

```javascript
const navCell = estimateEtfReturn({ ... });
const divAdd = dividendCashAdditiveForHorizon(r, scenarioHorizonKey, horizonYears);
const totalReturn = navCell.value + (divAdd || 0);
```

Apply in:

- `index.html` → `scenarioGrid` consumer / cell formatter (~8795+).
- Optionally centralize in `assets/scenario_returns.js` → `buildScenarioGrid` via optional `dividendAddAnnual` callback or precomputed per-horizon add (same for all σ columns at flat und; at non-zero und shocks, cash add stays constant — distributions do not scale with underlying shock in this model).

**3.2 Copy / tooltip**

- Scenarios subtitle: clarify **"NAV vol path + trailing cash (ex-borrow)"** for LETF grid.
- Main grid tooltip: show decomposition **NAV / cash / total** on hover (mirror YieldBOOST income tooltip pattern).

**Acceptance (Phase 3):**

- SMUP: Exp. ETF return (3M) === Scenarios cell (0σ, 1.0× vol, 3M horizon) within **0.1 pp**.
- Non-zero σ rows: cash add constant across columns; vol drag varies by σ multiplier (unchanged).

---

### Phase 4 — Tests & CI (required)

**4.1 Python — `tests/test_build_data_splits.py`**

| Test | Assert |
|------|--------|
| `test_window_dividend_reverse_split_restated_smup` | Pre-split restated $52.75 with ×10 reverse → economic total **≈ $5.28**, yield **≪ 0.08** on $66.25 basis |
| `test_window_dividend_post_split_unchanged` | ex ≥ split boundary: amount not divided |
| `test_window_dividend_lumpy_flag` | Single div > 5% of start → `dividend_lumpy: true`, recurring yield 0 |

**4.2 JS — new `tests/test_exp_etf_return_alignment.js` (or extend existing scenario tests)**

- Mock record: SMUP-like `dividend_adjustment`, `forecast_vol`, `delta`.
- Assert `computeExpectedEtfReturnFlatUnd(r, '6M', '3M')` ≈ `estimateEtfReturn(0σ)` + matched div add.
- Assert SMUP sign negative after fix.

**4.3 Audit script — `scripts/audit_exp_etf_scenario_alignment.py`**

For each record with `scenario_style === 'letf_vol_drag'` and `expected_decay_available`:

1. Recompute Python-side or read persisted fields for **Exp. ETF return NAV** and **dividend add**.
2. Compare to stored / recomputed **scenario 0σ @ 1.0×** (3M horizon).
3. **Fail** if `|delta| > 0.005` (50 bp) for symbols in allowlist gate, or `> 0.02` fleet-wide warn.

Seed allowlist: **SMUP, BITU, TQQY, YSPY** (high-visibility regressions).

**4.4 CI**

Wire audit into `scripts/ci_tick.py` (non-blocking warn first; blocking after one green nightly).

---

## Files to touch

| File | Change |
|------|--------|
| `scripts/split_adjustments.py` | `adjust_window_dividend_for_split`, constants |
| `scripts/build_data.py` | `_build_market_windows` dividend loop; optional lumpy flag; SMUP split merge |
| `index.html` | `dividendCashAdditiveForHorizon`, `computeExpectedEtfReturnFlatUnd`, scenario cell formatter, tooltips |
| `assets/scenario_returns.js` | Optional hook for dividend add in `buildScenarioGrid` |
| `tests/test_build_data_splits.py` | SMUP/BITU split dividend cases |
| `tests/test_exp_etf_return_alignment.js` | New parity tests |
| `scripts/audit_exp_etf_scenario_alignment.py` | New CI gate |
| `scripts/ci_tick.py` | Register audit |
| `data/dashboard_data.json` | Rebuild after Phase 1 |
| `README.md` / `AGENTS.md` | Document unified long-TR definition |

---

## Rollout order

1. **Phase 2.1 only** (horizon-matched window) — quick SMUP sign fix, low risk.
2. **Phase 1** (split-adjust dividends) — fixes 6M/chart yields fleet-wide.
3. **Phase 2.2** (lumpy guard + LETF cap) — prevents future specials.
4. **Phase 3** (scenario grid cash layer) — full parity.
5. **Phase 4** (tests + CI) — lock it in.

Phases 1 + 2.1 can ship in one PR; Phase 3 in a follow-up if needed.

---

## Out of scope

- YieldBOOST income calibration (`income_schedule.py`) — already fixed separately.
- FoF income scenarios (YBTY/YBST) — fixed in [`ybty-ybst-income-scenario-plan.md`](ybty-ybst-income-scenario-plan.md).
- Modeling **forward** dividend expectations for LETFs (only trailing window cash, same as today).
- Borrow / Net edge columns — unchanged.

---

## Verification checklist (manual)

After deploy, on **SMUP** / **BITU**:

- [ ] Main grid **Exp. ETF return (3M)** **negative** at flat spot.
- [ ] Scenarios **3M**, **0σ**, **1.0× vol** cell matches main grid within rounding.
- [ ] Chart timeframe toggle **6M → 3M** does not change main-grid sign for SMUP.
- [ ] Decay tab / Exp. edge (fwd) / Net edge unchanged.
- [ ] YieldBOOST rows (TSYY, …) unchanged on Scenarios income panel.

---

## References

- User report: SMUP Scenarios 0σ **−25.8%** vs Exp. ETF return **+14.77%** (2026-06-07).
- Code: `computeExpectedEtfReturnFlatUnd` (~3211), `dividendCashAdditiveForHorizon` (~3492), `_build_market_windows` (~3462), `estimateEtfReturn` (`assets/scenario_returns.js` ~81).
- Prior art: [`reverse-split-distribution-fix-prompt.md`](reverse-split-distribution-fix-prompt.md) → `adjust_distribution_amount_for_split`.

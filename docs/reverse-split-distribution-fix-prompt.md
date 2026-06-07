# Reverse-split distribution basis fix — audit & implementation prompt

**Status:** implemented 2026-06-07

Companion to [`reverse-split-fix-prompt.md`](reverse-split-fix-prompt.md), which fixed **Exp. ETF return run-rate** only. This doc covers the remaining **cash/share**, **capture ratio**, and **modeled cash yield** bugs on the Scenarios tab.

---

## Executive summary

Yahoo Finance **retroactively restates** historical dividend amounts onto the **post-split share basis**, while our issuer **NAV at ex-date** in `etf_metrics_daily.csv` remains on the **pre-split basis** through the split date.

`normalize_events()` divides restated amount by pre-split NAV:

```
yield_frac = amount_yahoo / nav_pre_split   # WRONG after reverse split
```

Correct economic yield:

```
yield_frac = (amount_yahoo / split_mult) / nav_pre_split
           = amount_yahoo / (nav_pre_split × split_mult)
```

For TSYY (1-for-8 on 2026-06-02), this inflates weekly yield ~**8×**, capture ratio ~**4–12×**, and modeled cash yield to **~1109%/yr** — while GraniteShares shows **~1.6%/wk** pre-split and **~1%/wk** post-split.

**Run-rate** (`run_rate_annual_display`) was partially patched via `post_split_latest_nav`; **capture ratio**, **events_recent**, and **modeled cash yield** were not.

---

## Evidence — TSYY vs GraniteShares

| Ex-date | Yahoo / dashboard Cash/share | Issuer (÷ 8) | NAV at ex | Wrong yield | Fixed yield |
|---------|-------------------------------|--------------|-----------|-------------|-------------|
| 2026-05-29 | **$0.24** | **$0.030** | $3.09 | 7.8%/wk | **1.0%/wk** |
| 2026-05-22 | $0.28 | $0.035 | $3.09 | 9.1%/wk | 1.1%/wk |
| 2026-05-15 | $0.39 | $0.049 | $3.11 | 12.6%/wk | 1.6%/wk |
| 2026-02-13 | $0.94 | $0.118 | ~$4.37 | 21.5%/wk | 2.7%/wk |

Post-split (Granite 2026-06-05): **$0.229**/share — aligns with Yahoo ~$0.23 on post-split basis (no adjustment needed for `ex_date ≥ split_boundary`).

Header today: *"modeled cash yield **1109.85%/yr** · ratio **11.39×**"* — because `11.39 × BS(σ) × 52 ≈ 1109%`.

---

## Affected tickers (fleet audit)

### Tier 1 — active YieldBOOST, broken today

| Ticker | Split | Mult | Events | `blended_ratio_used` | Modeled yield symptom | `run_rate` (partial fix) |
|--------|-------|------|--------|----------------------|------------------------|--------------------------|
| **TSYY** | 2026-06-02 | 8× | 58 | **11.39×** | ~1109%/yr | 52% (`post_split_latest_nav`) |
| **COYY** | 2026-06-02 | 6× | 44 | **6.83×** | ~894%/yr | 74% |
| **MTYY** | 2026-06-02 | 6× | 36 | **4.55×** | ~614%/yr | 86% |

All three: `split_boundary=2026-06-02` in calibration; **zero post-split distribution ex-dates yet** (last event 2026-05-29), so the pipeline cannot self-heal from post-split history alone.

### Tier 2 — reverse splits + distribution data, no YB calibration (latent)

**47 tickers** in `corporate_actions.json` since 2025-01-01 have rows in `etf_distributions.json` but no `income_distribution_calibration` block (not YieldBOOST / not built). Same Yahoo basis bug would apply if calibration is ever enabled — e.g. TSLY, MSTY, MRNY, CONY, PYPY, XYZY, AMDY, SOLT, SOXS, etc.

### Tier 3 — high capture ratio, no recent split (separate drivers)

| Ticker | `blended_ratio_used` | Likely cause |
|--------|---------------------|--------------|
| YSPY | 7.53× | Special/lumpy distributions, thin σ history |
| YBTY | 4.16× | Same |
| FIYY | 3.41× | Same |

Do not conflate with the 2026-06-02 Granite cluster; may need a separate sanity cap on ratio regardless.

### TSYY secondary issue — large 2025 specials

Yahoo history includes one-off amounts up to **$20.34** (2025-02-28). Even after split-basis fix, trailing capture-ratio median may stay elevated until those events age out of the window. Consider excluding outliers (`amount / nav > MAX_WEEKLY_YIELD_FRAC`) or capping per-event ratio.

---

## Root cause map

| Layer | File | What happens | Status |
|-------|------|--------------|--------|
| Ingest | `scripts/ingest_distributions.py` | Yahoo chart `events=div` → raw `{ex_date, amount}` | No split awareness |
| Storage | `data/etf_distributions.json` | Restated Yahoo amounts stored as-is | Expected |
| NAV | `data/etf_metrics_daily.csv` | Issuer pre-split NAV through split date | Expected |
| Splits | `data/corporate_actions.json` | TSYY/COYY/MTYY 1-for-8 / 1-for-6 @ 2026-06-02 | OK |
| **Normalize** | `scripts/income_schedule.py::normalize_events` | `yield = amount / nav` with no basis alignment | **BUG** |
| Capture ratio | `scripts/income_schedule.py::compute_capture_ratio` | Median of inflated pre-split ratios | **BUG** |
| Run-rate | `scripts/income_schedule.py::_compute_run_rate_annual` | Post-split NAV fallback | Partial fix |
| Build | `scripts/build_data.py` ~4772 | Writes `income_distribution_calibration` | Passes through bugs |
| UI table | `index.html::ScenarioDistributionsPanel` | Cash/share = raw Yahoo amount | Misleading vs Granite |
| Modeled yield | `index.html::incomeAnnualYieldForScenario` + `assets/income_scenario.js::calibratedWeeklyDistribution` | `ratio × BS × 52`, no cap on calibrated path | **BUG** |

**Not in scope:** `price_basis.js` TR/decay split handling (fixed separately in `cad020b`). Distributions are a distinct basis problem.

---

## Recommended fix — one rule, one place

### Core rule (Python + JS parity)

In `normalize_events`, when `split_ctx.mode == "discrete_split"` and event is **reverse split** with `ex_date < split_boundary`:

```python
amount_economic = amount / mult          # undo Yahoo restatement
yield_frac = amount_economic / nav_at_ex
```

Export in each normalized event:

- `amount` — keep raw Yahoo (audit trail)
- `amount_economic` — issuer-comparable per-share cash
- `amount_basis` — `"yahoo_restated"` | `"as_paid"` | `"post_split"`

Post-split events (`ex_date ≥ boundary`): use amount as-is (already on current share count).

**Do not** apply to continuous Yahoo close tickers where `filter_splits_needing_close_basis_fix` returns empty (MTYY price path) — but distributions still need amount ÷ mult when issuer NAV is pre-split.

### Capture ratio after recent split

Mirror run-rate logic in `build_income_calibration_row`:

1. Prefer post-split-only normalized events (same lookback as `_compute_run_rate_annual`).
2. If `< POST_SPLIT_MIN_EVENTS_FOR_MEDIAN` post-split events → use **`cross_fund_ratio`** (≈0.65), not contaminated pre-split median.
3. Optionally blend with confidence weight only when post-split sample is sufficient.

### Scenario yield caps (defense in depth)

- Apply existing `MAX_INCOME_YIELD_ANNUAL = 1.5` in `calibratedWeeklyDistribution` (JS) and Python scenario helpers.
- Cap `blended_ratio_used` at fleet sanity bound (e.g. **2.5×**) when `split_boundary` within 120 days.

### UI (minimal)

In `ScenarioDistributionsPanel`:

- Show **`amount_economic`** in Cash/share when `split_boundary` set and `ex_date < boundary`, **or**
- Add subtitle: *"Cash/share: Yahoo split-adjusted; pre-split rows scaled ÷{mult} for issuer comparison"*
- Fix modeled-yield header to use capped calibrated yield.

### Optional ingest diagnostic

`scripts/ingest_distributions.py`: log warning when `amount / nav > 0.05` on dates before known split boundary (does not block ingest).

---

## Implementation checklist

### Phase 1 — correctness (required)

- [ ] Add `adjust_distribution_amount_for_split(amount, ex_date, split_ctx) -> (amount_economic, basis)` in `scripts/income_schedule.py` (or `split_adjustments.py`).
- [ ] Pass `split_ctx` into `normalize_events`; apply rule above.
- [ ] Filter / fallback in `compute_capture_ratio` input when recent split + thin post-split history.
- [ ] Re-export `amount_economic`, `amount_basis` in `events_recent`.
- [ ] Cap `calibratedWeeklyDistribution` annual output at `MAX_INCOME_YIELD_ANNUAL`.
- [ ] Rebuild `dashboard_data.json`.

### Phase 2 — UI & tests

- [ ] Update `ScenarioDistributionsPanel` to display economic amount or basis label.
- [ ] Python tests in `tests/test_income_schedule.py` (see below).
- [ ] JS tests in `tests/test_income_scenario.js`.
- [ ] Parity: scenario grid TSYY modeled yield < 150%.

### Phase 3 — CI gate

- [ ] New `scripts/audit_distribution_split_basis.py`:
  - For each YB row with `split_boundary` within 120d: fail if `blended_ratio_used > 2.5` or modeled yield > 150%.
  - Spot-check TSYY/COYY/MTYY: last pre-split event `yield_frac < 0.03`.
- [ ] Wire into `scripts/ci_tick.py`.

---

## Test cases

```python
# tests/test_income_schedule.py

def test_normalize_events_reverse_split_yahoo_restated():
    """TSYY 2026-05-29: yahoo=0.24, nav=3.09, mult=8 → yield ≈ 0.03/3.09."""
    split_ctx = {"mode": "discrete_split", "boundary": "2026-06-02", "mult": 8.0}
    ...

def test_capture_ratio_uses_cross_fund_after_recent_split_no_post_events():
    """0 post-split ex-dates → blended_ratio ≈ cross_fund (0.65), not 11×."""
    ...

def test_post_split_event_unchanged():
    """ex_date >= boundary: amount not divided."""
    ...

def test_tsyy_modeled_yield_sane_after_rebuild():
    """build_income_calibration_row TSYY fixture → blended_ratio < 2.0."""
```

```javascript
// tests/test_income_scenario.js
test("calibratedWeeklyDistribution caps at MAX_INCOME_YIELD_ANNUAL", () => {
  // calibration with blended_ratio_used: 11.39 → capped annual ≤ 1.5
});
```

---

## Agent implementation prompt (copy-paste)

```
Fix reverse-split distribution basis mismatch for YieldBOOST income calibration.

Problem: Yahoo etf_distributions.json amounts are retroactively split-adjusted onto post-split $/share, but normalize_events() in scripts/income_schedule.py divides by pre-split issuer NAV. This inflates yield ~6–8× for TSYY/COYY/MTYY (2026-06-02 splits), producing blended_ratio ~4–11× and modeled cash yield ~600–1100%/yr. Cash/share in the UI shows Yahoo restated amounts ($0.24) instead of issuer as-paid (~$0.03).

Scope:
- scripts/income_schedule.py (primary): normalize_events, compute_capture_ratio / build_income_calibration_row
- assets/income_scenario.js: cap calibratedWeeklyDistribution at MAX_INCOME_YIELD_ANNUAL
- index.html: ScenarioDistributionsPanel show amount_economic or basis label
- tests/test_income_schedule.py, tests/test_income_scenario.js
- scripts/audit_distribution_split_basis.py + ci_tick.py hook

Implementation:
1. For discrete_split reverse splits with ex_date < split_boundary: amount_economic = amount / mult; yield_frac = amount_economic / nav_at_ex.
2. Post-split events: no division.
3. When split_boundary within POST_SPLIT_RUN_RATE_LOOKBACK_DAYS and insufficient post-split events for capture ratio, use cross_fund_ratio instead of pre-split median.
4. Export amount_economic and amount_basis in events_recent.
5. Cap scenario modeled yield at 1.5 (150%) in JS and Python.
6. Rebuild dashboard_data.json; verify TSYY blended_ratio_used ≈ 0.65–1.5, modeled yield < 150%, May-29 cash/share ≈ $0.03 or labeled.

Do NOT change price_basis.js TR logic. Do NOT double-adjust post-split amounts.

Acceptance:
- pytest tests/test_income_schedule.py -v
- node --test tests/test_income_scenario.js
- TSYY/COYY/MTYY Scenarios tab: no 1109% header; capture ratio ~0.6–1.5×; pre-split yields ~1–3%/wk
- audit_distribution_split_basis.py passes
```

---

## Verification commands

```bash
python -m pytest tests/test_income_schedule.py -v
node --test tests/test_income_scenario.js
python scripts/build_data.py   # or CI tick subset
python -c "
import json
dd=json.load(open('data/dashboard_data.json'))
for s in ('TSYY','COYY','MTYY'):
    cal=next(r for r in dd['records'] if r['symbol']==s)['income_distribution_calibration']
    print(s, 'ratio', cal['blended_ratio_used'], 'run_rate', cal['run_rate_annual_display'])
"
```

---

## Related docs

- [`reverse-split-fix-prompt.md`](reverse-split-fix-prompt.md) — run-rate / Exp. ETF return (done)
- `AGENTS.md` §13.13 — split TR continuous vs discrete (price path only)
- Commit `cad020b` — Decay tab `etf_adj_close` double-scaling fix

# B4 Pair Report — Acceptance Criteria & Golden Fixtures

**Status:** Phase 0 freeze (2026-07-20)  
**Schema under test:** `bucket4_backtest.v4` / `bucket4_production_pair.v1`

## Golden fixture set

| Role | Symbol | Artifact | Notes |
|---|---|---|---|
| Production short-window | **QBTZ** | [`data/bucket4_pairs/QBTZ.json`](../data/bucket4_pairs/QBTZ.json) | Plan entered `2026-06-09` → `2026-07-13`; `rebalance_log: []` pre-fix; daily has many `rebalance` flags |
| Production longer window | **MSTZ** | [`data/bucket4_pairs/MSTZ.json`](../data/bucket4_pairs/MSTZ.json) | Plan entered `2026-04-13` |
| Production exited | **APLZ** | [`data/bucket4_pairs/APLZ.json`](../data/bucket4_pairs/APLZ.json) | Plan entered `2026-03-13`, latest `2026-04-20` |
| Research-legacy contrast | **SQQQ** | [`data/bucket4_pairs/SQQQ.json`](../data/bucket4_pairs/SQQQ.json) | Long history + populated `rebalance_log` (research shape) |

Book envelope snapshot (do not mutate for acceptance):

| Field | Value (local freeze) |
|---|---|
| `schema` | `bucket4_backtest.v4` |
| `window_start` | `2026-02-27` |
| `window_end` | `2026-07-13` |
| `n_pairs` | 5 (MSTZ, QBTZ, APLZ, CLSZ, SMZ) |
| Archive floor reason | Aggregate book starts 2026-02-27 (golden). PIT screened archives densified inside that window via git backfill; pair drill-down may use Inception research back to listing. |

## Locked product wording

1. **CAGR** = compounds `daily.ret` (sleeve equity), annualized on **calendar** time via `summaryFromReturns`. Not PnL ÷ display Notional. Not based on gross short exposure.
2. **Display Notional** linearly scales `actual_dollar` ledger fields from `notional_basis_usd`.
3. **Gross leg** = \|ETF MV\| + \|und MV\| (both short sides), scaled; can exceed display Notional.
4. **Plan entered** = first production-ledger / plan-membership day. **Not** ETF listing/inception.

## Acceptance checks

### A. Rebalance log

- [x] For any production pair where `daily.rebalance` has truthy flags, the Pair Report rebalance log shows ≥1 row with finite `h` when `h_used` is finite, boolean executed (`yes`/`no`), and fee when `rebalance_fee` is finite (or explicit `—` only when missing). *(daily_derived fallback + ls-algo normalized export)*
- [x] Prefer exported `rebalance_log` with real fields (`source: production_export`) over daily-derived fallback.
- [x] When fallback is used, UI shows: “Derived from daily ledger flags…”.

### B. Layout / formatting

- [x] Title renders `QBTZ vs QBTS` with a visible gap (not `QBTZvs`).
- [x] Toolbar: Notional and Plan entered / Latest / Gate / Model sit in one left-aligned wrapping row (no giant empty middle).
- [x] Pair report tables use `.pair-report-table` column widths; Date left, numerics right.

### C. CAGR / capital basis

- [x] CAGR sublabel mentions sleeve equity CAGR and display notional.
- [x] CAGR tooltip states compounding daily sleeve returns; not PnL÷Notional; not gross shorts.
- [x] Net PnL / gross leg tooltip mentions source sleeve capital (`notional_basis_usd`).

### D. Plan vs inception

- [x] Meta label is **Plan entered** (not bare “Entered”).
- [x] **ETF inception** chip shown when `etf_inception_date` is present (metadata OK without research series).
- [x] History toggle defaults to **Plan path (production)**; Inception research enabled when `inception_research.daily` exists (nested for golden prod pairs).
- [x] B4 Book equity / Production CAGR never switch to inception research.

## Non-goals (acceptance)

- Do not use `--mode research-legacy` as production.
- Do not invent pre-plan membership PnL in the importer.
- Chart Backtest / Drip remain separate research surfaces.

# B4 Archive Extension Ops Track

**Purpose:** Densify honest PIT screener archives inside the production book window, and keep listing→plan history on the labeled **Inception research** path — without inventing membership or extending the aggregate book before the golden floor.

## Book vs pair policy (locked)

| Surface | Start / history |
|---|---|
| **Aggregate B4 sleeve chart** (diagnostic book KPIs) | **`2026-02-27`** golden production start — do **not** extend book `window_start` to `2025-12-28` |
| **B4 Pairs / Pair Report — Plan path (default)** | Production ledger from first plan membership day (≥ book start when in book) |
| **Chart Optimized tab** (`#/chart/SYM/optimized`) | Per-pair **listing → latest** research on `bucket4_pairs/{ETF}.json`. Default path is **`cash_residual_path`** (`scale_to_budget=false`, crash caps + optional h-first + cadence freeze) when nested; toggles to unit-equity **Stabilized** / **Current h**. Rebuild: `python scripts/build_b4_cash_residual_path.py --etfs SYM`. `authoritative: false`; never feeds book PnL |

## Inventory

Run from `ls-algo`:

```bash
python scripts/inventory_b4_screener_archives.py \
  --out ../dashboards/etf-dashboard/docs/b4_archive_inventory.json
```

Latest snapshot fields live in [`docs/b4_archive_inventory.json`](b4_archive_inventory.json) (`git_recoverability`, `book_window_policy`).

| Field | Value |
|---|---|
| Aggregate book start | **`2026-02-27`** |
| Live screened archive floor | **`2025-12-28`** (pre-book; not used for book window) |
| May–Jun screened hole | Closed via `backfill_screened_history.py` from git tip commits (~49 days written) |
| Dec→Feb / Jan | Mostly **permanent** git hole — leave sparse; do not forward-fill |

## Backfill rules

1. Reconstruct missing daily `etf_screened_today.csv` / `proposed_trades.csv` under `ls-algo/data/runs/<date>/` **only** from git tip history (`scripts/backfill_screened_history.py`).
2. **Never** forward-fill the latest screener into missing days.
3. Default is dry-run; `--apply` writes; never overwrite without `--force`.
4. Hash every archive day into the B4 export `input_hashes`.
5. Re-run production at the **book** start:

```bash
python scripts/export_b4_dashboard.py --run-production --start 2026-02-27
```

6. Import fail-closed in etf-dashboard:

```bash
python scripts/build_bucket4_backtest.py --mode production
```

**Invariant:** production import **merge-preserves** `cash_residual_path`, `inception_research`, and `inception_research_stable` on existing pair shards (and hydrates from standalone `data/bucket4_*` dirs when the nest is missing). It must never blank Optimized by overwriting research nests. CI: `python scripts/audit_b4_inception_paths.py --fail-on-missing-nests` (fail-closed on nightly).

7. Refresh pair inception research (does not change book):

```bash
python scripts/build_b4_inception_research.py --out-dir data/bucket4_inception_research
# Also nests inception_research_stable (deadband+slew) by default (--with-stable).
python scripts/audit_b4_inception_paths.py --fail-on-inception-fail
python scripts/audit_b4_inception_paths.py --fail-on-missing-nests
```

8. Refresh cash-residual Optimized paths (does not change book):

```bash
python scripts/build_b4_cash_residual_path.py
# Or targeted: python scripts/build_b4_cash_residual_path.py --etfs CONI,HOOZ,NBIZ
# Nests cash_residual_path onto data/bucket4_pairs/{ETF}.json
# Also runs automatically at the end of build_b4_inception_research.py (--with-cash-residual, default on).
```

If Optimized shows a missing path after a production import, first confirm the nest was preserved (`cash_residual_path` on the pair shard). If standalone research exists under `data/bucket4_cash_residual_path/{ETF}.json` but the nest is empty, re-nest with the rebuild command above (or re-run import after the preserve fix). The SPA also falls back to standalone URLs client-side as defense-in-depth. Without any nest the UI correctly falls back to unit-equity Stabilized / Current h.

Panel sanitize (`scripts/bucket4/bucket4_price_loading.py::sanitize_panel_vs_session_close`) replaces ETF `a_px` when day \|ret\| > 100% **or** early/late `panel/close` median steps >25%, preferring `etf_adj_close` when it removes reverse-split cliffs (APLZ/BEZ/NBIZ) else session `close_price` (QBTZ fabricated scale).

## Remaining gap

ETF listing → first honest `plan_entry_date` stays on the chart **Inception** tab (`scripts/build_b4_inception_research.py` → nested `inception_research` on pair shards).

B4 Pairs book equity / Production CAGR must remain on the plan path only, starting **2026-02-27**.

## Plan-ledger anomalies (ls-algo re-export follow-up)

Dashboard **does not** rewrite production plan ledgers. Pair Report shows a red banner when plan-path sanity fails. As of the Jul 2026 fleet audit (local scan 2026-07-28):

| ETF | Plan path | Ops action |
|---|---|---|
| **CONI** | equity wipe (2026-06-03+) + \|ret\|≈161% | Re-export from ls-algo after price/split repair |
| **DRIP** | equity wipe (2026-06-18) + \|ret\|≈483% | Re-export after panel repair |
| **LITZ** | \|ret\|≈105% on 2026-07-09 | Re-export after panel repair |
| **MSTZ** | \|ret\|≈431% on 2026-06-29 | Re-export after panel repair |
| **SMST** | equity wipe (2026-06-30) + \|ret\|≈506% | Re-export after panel repair |
| **SOXS** | equity wipe (2026-06-18) + \|ret\|≈298% | Re-export after panel repair |
| **TECS** | equity wipe (2026-06-18) + \|ret\|≈367% | Re-export after panel repair |
| **TZA** | equity wipe (2026-06-18) + \|ret\|≈458% | Re-export after panel repair |
| RKLZ | cleared on research nest after panel sanitize | Monitor plan path after re-export |
| SNDQ / BMNZ / IONZ | WARN \|ret\|≈50–73% | Monitor |

Inception research for those names can still be sane on the chart tab after local panel sanitize + rebuild (`scripts/build_b4_inception_research.py --etfs …`).

Re-export + import:

```bash
# ls-algo
python scripts/export_b4_dashboard.py --run-production --run-date YYYY-MM-DD --start 2026-02-27
# etf-dashboard
python scripts/build_bucket4_backtest.py --mode production
```

## Layer A parity (ops)

```bash
python scripts/b4_layer_a_parity.py --etfs QBTZ,MSTZ,CLSZ,APLZ,SMZ
# report: data/_layer_a_parity/report.json
python -m pytest tests/test_b4_layer_a_parity.py -q
python scripts/audit_b4_inception_paths.py
```

Gates: h-definition, realized-h, return corr, enter/cadence/hard_exit Jaccard, equity-norm. Isolation ignores production blacklist exits. Return-corr allows a residual band (`>0.60` when realized-h already matches) for metrics calendar holes (e.g. QBTZ missing `2026-05-26`). Nightly soft-gates Layer A tests + inception path audit (`continue-on-error`).

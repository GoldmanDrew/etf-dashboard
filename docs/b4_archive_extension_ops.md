# B4 Archive Extension Ops Track

**Purpose:** Densify honest PIT screener archives inside the production book window, and keep listing→plan history on the labeled **Inception research** path — without inventing membership or extending the aggregate book before the golden floor.

## Book vs pair policy (locked)

| Surface | Start / history |
|---|---|
| **Aggregate B4 Book** (equity, Production CAGR, book KPIs) | **`2026-02-27`** golden production start — do **not** extend book `window_start` to `2025-12-28` |
| **Pair Report — Plan path** | Production ledger from first plan membership day (≥ book start when in book) |
| **Pair Report — Inception research** | ETF/underlying overlap back toward listing; `authoritative: false`; never feeds book PnL |

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

7. Refresh pair inception research (does not change book):

```bash
python scripts/build_b4_inception_research.py --out-dir data/bucket4_inception_research
```

## Remaining gap

ETF listing → first honest `plan_entry_date` stays on **Inception research** (`scripts/build_b4_inception_research.py` → nested `inception_research` on pair shards).

B4 Book equity / Production CAGR must remain on the plan path only, starting **2026-02-27**.

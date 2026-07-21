# B4 Archive Extension Ops Track

**Purpose:** Move honest production `plan_entry_date` / book `window_start` earlier than the documented golden floor when point-in-time archives exist — without inventing membership.

## Inventory (Phase 4.1)

Run from `ls-algo`:

```bash
python scripts/inventory_b4_screener_archives.py \
  --out ../dashboards/etf-dashboard/docs/b4_archive_inventory.json
```

Latest local inventory (`docs/b4_archive_inventory.json`):

| Field | Value |
|---|---|
| Documented golden start | `2026-02-27` |
| Live screened archive floor | **`2025-12-28`** (57 days) |
| Live plan archive floor | see inventory JSON |
| Opportunity | Archives begin earlier than the documented golden window — extend production `--start` after validation |

## Backfill rules (Phase 4.2)

1. Reconstruct missing daily `etf_screened_today.csv` / `proposed_trades.csv` under the ls-algo runs archive **only** from recoverable git / retained artifacts.
2. **Never** forward-fill the latest screener into missing days.
3. Hash every archive day into the B4 export `input_hashes`.
4. Re-run:

```bash
python scripts/export_b4_dashboard.py --run-production --start <new_floor>
```

5. Import fail-closed in etf-dashboard:

```bash
python scripts/build_bucket4_backtest.py --mode production
```

## Remaining gap (Phase 4.3)

ETF listing → first honest `plan_entry_date` stays on the labeled **Inception research** path (`scripts/build_b4_inception_research.py` → optional `inception_research/{ETF}.json` beside the production replay).

B4 Book equity / Production CAGR must remain on the plan path only.

# Plan: Wire B1 underlying views into ls-algo EOD

**Goal:** Operator scores edited in etf-dashboard become a committed config that **ls-algo reads at EOD** and applies when sizing **Bucket 1 / `core_leveraged`** pairs.

**Status today:** Dashboard-only research overlay (`config/bucket1_underlying_views.yml` → `data/bucket1_underlying_views.json` via `#/views` Save). **Not** in `build_data.py`, GTP, or production replay.

**Sibling plan:** [`bucket1_underlying_views_plan.md`](bucket1_underlying_views_plan.md) (score → `h` ladder semantics).

---

## Locked decisions (2026-07-27)

| # | Decision | Choice |
|---|----------|--------|
| 1 | Production `h` apply mode | **B / `absolute_nonzero`** — score `0` or missing → no overlay (keep delta-matched sizing). Non-zero → absolute ladder `h` as target hedge ratio. |
| 2 | EOD fetch miss | **Warn** — do not abort screener/GTP; log clearly and proceed with no views (or last run-dir file only if explicitly found for that date). |
| 3 | Where ls-algo reads views | **`data/runs/<run_date>/bucket1_underlying_views.yml` only** — EOD archives the fetch there; GTP/loaders read that path. Do **not** treat `ls-algo/config/…` as the runtime SoT. |

---

## 1. Architecture

Mirror **borrow_history**-style pull (dashboard SoT → ls-algo fetches at EOD), but **runtime input is the dated run artifact**, not a live `config/` overlay.

```
┌──────────────── etf-dashboard ─────────────────┐
│  #/views UI  →  POST /api/bucket1-views        │
│       ↓                                        │
│  config/bucket1_underlying_views.yml  (SoT)    │
│       ↓                                        │
│  data/bucket1_underlying_views.json (artifact) │
│       ↓                                        │
│  commit + push to etf-dashboard main           │
└───────────────────────┬────────────────────────┘
                        │ curl (EOD screener job)
                        ▼
┌────────────────── ls-algo ─────────────────────┐
│  Fetch → write ONLY:                           │
│    data/runs/<run_date>/bucket1_underlying_views.yml
│       ↓                                        │
│  generate_trade_plan.py (--run-date …)         │
│    loads views from that run-dir path          │
│       ↓                                        │
│  apply score→h on core_leveraged (mode B)      │
│       ↓                                        │
│  proposed_trades.csv + audit columns           │
└────────────────────────────────────────────────┘
```

| Concern | Choice |
|--------|--------|
| **Operator SoT** | `etf-dashboard/config/bucket1_underlying_views.yml` (UI Save or hand-edit) |
| **ls-algo runtime path** | `data/runs/<run_date>/bucket1_underlying_views.yml` |
| **Fetch miss** | Warn; GTP sees empty views for that date (no abort) |
| **Apply where** | `generate_trade_plan.py` on `core_leveraged` (B1), **not** screener CSV |
| **Do not** | Rewrite `ls-algo/config/bucket1_underlying_views.yml` each EOD as SoT |
| **Do not** | Put views into `etf_screened_today.csv` |
| **Do not** | Wire into B4 cadence / `pair_overrides` |

Optional: keep a **schema stub** under `ls-algo/config/bucket1_underlying_views.example.yml` (docs/ladder defaults only) — never the EOD-consumed file.

---

## 2. Production `h` semantics (mode B — locked)

| Score | Production behavior |
|------:|---------------------|
| missing / `0` | **No overlay** — keep delta-matched `|und|/|ETF| ≈ \|Δ\|` + existing ratio safety |
| `-2…-1`, `+1…+2` | Set **target hedge ratio** = ladder `h` (absolute), clip to `h_min`/`h_max`; size und from ETF notional; ratio-safety uses **view `h`**, not `|Δ|` |

Research UI still shows ladder including `0 → 1.0`; production simply ignores neutrals.

> Chart Backtest `h_bt = \|MV_ETF\|/\|MV_und\|` is the **inverse** of coverage `h`. Later Backtest prefill must convert `h_bt = 1/h_view`.

---

## 3. Phased delivery

### Phase 0 — Contract freeze (½ day)

1. Freeze YAML schema `bucket1_underlying_views.v1`.
2. Hub `bridges.md`: `schema`, `score_to_h`, `h_min`, `h_max`, `views.<UND>.{score,note,updated}`; note run-dir consume path.
3. Decisions in §Locked above are authoritative.
4. Pages Save remains local-only; production requires YAML on `etf-dashboard` main.

### Phase 1 — ls-algo loader (1 day)

**In `quant/ls-algo`:**

1. Add module (e.g. `bucket1_underlying_views.py` or under `scripts/`) with:
   - `h_for_score`, clip, normalize
   - `load_views_yaml(path) → …`
   - `resolve_run_views(run_date, *, runs_root=…) → path = data/runs/<date>/bucket1_underlying_views.yml`
2. `strategy_config.yml` knobs (paths + flags only — **not** embedding the views map):

```yaml
bucket1_views:
  enabled: false
  apply_mode: absolute_nonzero   # locked
  sleeves: [core_leveraged]
  # Resolved as data/runs/<run_date>/bucket1_underlying_views.yml
  run_filename: bucket1_underlying_views.yml
```

3. Unit tests: ladder, clip, missing file → empty, invalid score → 0, resolve path for a run date.

### Phase 2 — EOD fetch → run dir (½–1 day)

**In `eod_pnl_email.yml` `screener` job** (near borrow_history curl):

```bash
RUN_DIR="data/runs/${RUN_DATE}"
mkdir -p "$RUN_DIR"
if curl -fsSL … -o "$RUN_DIR/bucket1_underlying_views.yml" "$VIEWS_URL"; then
  echo "bucket1 views fetched → $RUN_DIR/bucket1_underlying_views.yml"
else
  echo "::warning::bucket1_underlying_views fetch failed; GTP will run with no views"
  rm -f "$RUN_DIR/bucket1_underlying_views.yml"
fi
```

- **Warn only** on miss — do not fail the job.
- Commit the run-dir YAML with other `data/runs/<date>/` artifacts when present.
- **Local:** if file absent, optionally copy from `ETF_DASHBOARD_ROOT/config/…` into the run dir (dev convenience), still reading only from run dir afterward.

### Phase 3 — Apply in `generate_trade_plan.py` (1–2 days)

1. At start of GTP for `--run-date D`, load  
   `data/runs/D/bucket1_underlying_views.yml` (missing → empty + warning log).
2. After `core_leveraged` delta sizing, before final emit:
   - non-zero score → resize `|und| = h × |ETF|`
   - score 0 / missing → skip
3. Audit columns on `proposed_trades.csv`:
   - `b1_view_score`, `b1_view_h`, `b1_view_note`, `b1_view_updated`
   - `b1_view_source_path` (run-dir path or empty)
   - `underlying_target_usd_pre_b1_view` when applied
4. Ratio safety: when view applied, target = `b1_view_h`.

### Phase 4 — Operator loop (dashboard) (½ day)

1. Keep Save → YAML + JSON as today.
2. Ops: after Save, **commit/push** `config/bucket1_underlying_views.yml` so next EOD fetch succeeds.
3. Optional: schema validator on dashboard push.

### Phase 5 — Observability

- EOD email: hedged/unhedged headline + **B1 split** component line; attach
  `hedged_pnl_b1_by_pair.csv` (matched vs OLS `|delta|`; views = unhedged).
- Risk dashboard `hedged_pnl_panel`: B1 pair table + updated definitions.
- After enabling the B1 split in `hedged_pnl.py`, ops must once run
  `python scripts/backfill_hedged_pnl.py` on ls-algo and commit
  `data/ledger/hedged_pnl_history.csv` so YTD is consistent.
- No B4 export changes.

---

## 4. Explicit non-goals (v1)

- Changing screener CSV / `dashboard_data.json` for views.
- Auto-push from GitHub Pages.
- Applying views to B4/B5 or YieldBOOST.
- Rewriting OLS `delta` in the screener.
- Chart Backtest / Drip prefill.
- Maintaining a live `ls-algo/config/bucket1_underlying_views.yml` as EOD input.

---

## 5. Test plan

| Layer | Tests |
|-------|--------|
| Loader | Missing run-dir file → empty; bad score → 0; clip at bounds |
| Path | `resolve_run_views("2026-07-27")` → `data/runs/2026-07-27/bucket1_underlying_views.yml` |
| Apply | NVDA +2 → all core_leveraged NVDA pairs share `h`; score 0 → unchanged |
| Safety | Applied view target not repaired back to `|Δ|` |
| EOD | Fetch miss → warning, no file, GTP continues; fetch hit → file present and loaded |
| Fixture | Golden `proposed_trades` with 1–2 underlyings |

---

## 6. Rollout checklist

1. [x] Lock apply mode **B / absolute_nonzero**
2. [x] Lock fetch miss = **warn**
3. [x] Lock runtime path = **`data/runs/<date>/bucket1_underlying_views.yml`**
4. [x] Land ls-algo loader + path resolve + tests (`enabled: false`)
5. [x] Land EOD fetch → run dir (warn on miss)
6. [x] Land GTP apply + audit columns + ratio-safety interaction
7. [ ] Commit non-empty views YAML on etf-dashboard main (ops)
8. [x] Enable `bucket1_views.enabled: true`
9. [ ] Hub: `bridges.md` + `[PROPOSED]` daily; AGENTS.md when live

---

## 7. Suggested first PR split

1. **ls-algo:** run-dir loader + knobs + unit tests (`enabled: false`).
2. **ls-algo:** EOD curl → `data/runs/<date>/…` (warn on miss).
3. **ls-algo:** GTP apply + audit columns + ratio-safety.
4. **etf-dashboard:** ops note in views plan (“commit YAML for EOD”); optional schema validator.
